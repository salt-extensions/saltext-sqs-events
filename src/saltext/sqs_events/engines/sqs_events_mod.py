"""
Salt engine module
"""

import logging
import time

import salt.utils.event
import salt.utils.json

try:
    import boto3
    from botocore.exceptions import ClientError
    from botocore.exceptions import NoCredentialsError
    from botocore.exceptions import NoRegionError

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


def __virtual__():
    if not HAS_BOTO3:
        return (
            False,
            "Cannot import engine sqs_events_boto3 because the required boto3 module is missing",
        )
    return True


log = logging.getLogger(__name__)


def _get_sqs_client(profile, region=None, key=None, keyid=None):
    """
    Get a boto3 client to SQS.
    Nutzt explizite Keys aus profile/__opts__ oder, falls nicht gesetzt,
    Standard-Credential-Provider-Chain (inkl. IMDSv2).
    """
    session_kwargs = {}
    client_kwargs = {}

    if profile:
        if isinstance(profile, str):
            _profile = __opts__[profile]
        elif isinstance(profile, dict):
            _profile = profile
        else:
            _profile = {}

        key = _profile.get("key", key)
        keyid = _profile.get("keyid", keyid)
        region = _profile.get("region", region)

    if not region:
        region = __opts__.get("sqs.region", "us-east-1")

    session_kwargs["region_name"] = region

    # Wenn key/keyid gesetzt sind, explizit verwenden, sonst Default Chain
    if keyid and key:
        client_kwargs["aws_access_key_id"] = keyid
        client_kwargs["aws_secret_access_key"] = key

    try:
        session = boto3.Session(**session_kwargs)
        client = session.client("sqs", **client_kwargs)
    except (NoCredentialsError, NoRegionError) as exc:
        log.error(
            "Error creating boto3 SQS client for sqs_events engine: %s",
            exc,
        )
        return None

    return client


def _get_queue_url(sqs_client, queue_name, owner_acct_id=None):
    """
    Resolve queue name to URL.
    """
    try:
        params = {"QueueName": queue_name}
        if owner_acct_id:
            params["QueueOwnerAWSAccountId"] = owner_acct_id
        resp = sqs_client.get_queue_url(**params)
        return resp["QueueUrl"]
    except ClientError as exc:
        log.warning(
            "Error getting queue URL for %s (owner_acct_id=%s): %s",
            queue_name,
            owner_acct_id,
            exc,
        )
        return None


def _process_queue(
    sqs_client,
    queue_url,
    q_name,
    fire_master,
    tag="salt/engine/sqs",
    owner_acct_id=None,
    message_format=None,
):
    if not queue_url:
        log.warning(
            "Failure resolving queue URL for: %s, waiting 10 seconds.",
            ":".join([_f for _f in (str(owner_acct_id), q_name) if _f]),
        )
        time.sleep(10)
        return

    try:
        # Long polling wie vorher (20 Sekunden)
        resp = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
        )
    except ClientError as exc:
        log.warning(
            "Error receiving messages from SQS queue %s: %s",
            q_name,
            exc,
        )
        time.sleep(10)
        return

    messages = resp.get("Messages", [])
    for msg in messages:
        body = msg.get("Body", "")
        if message_format == "json":
            try:
                data = salt.utils.json.loads(body)
            except Exception as exc:  # pylint: disable=broad-except
                log.warning(
                    "Failed to decode JSON body from SQS message on queue %s: %s",
                    q_name,
                    exc,
                )
                data = body
            fire_master(tag=tag, data={"message": data})
        else:
            fire_master(tag=tag, data={"message": body})

        # Nachricht löschen, nachdem sie verarbeitet wurde
        try:
            sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=msg["ReceiptHandle"],
            )
        except ClientError as exc:
            log.warning(
                "Failed to delete SQS message from queue %s: %s",
                q_name,
                exc,
            )


def start(queue, profile=None, tag="salt/engine/sqs", owner_acct_id=None):
    """
    Listen to SQS and fire messages on Salt event bus (boto3-Version).
    """
    if __opts__.get("__role") == "master":
        fire_master = salt.utils.event.get_master_event(
            __opts__, __opts__["sock_dir"], listen=False
        ).fire_event
    else:
        fire_master = __salt__["event.send"]

    message_format = __opts__.get("sqs.message_format", None)

    sqs_client = _get_sqs_client(profile)
    if not sqs_client:
        # Falls keine Credentials/Region etc. -> Retry-Schleife
        while not sqs_client:
            log.warning("Failed to create SQS boto3 client, retrying in 10 seconds.")
            time.sleep(10)
            sqs_client = _get_sqs_client(profile)

    queue_url = None

    while True:
        if not queue_url:
            queue_url = _get_queue_url(sqs_client, queue, owner_acct_id=owner_acct_id)

        _process_queue(
            sqs_client,
            queue_url,
            queue,
            fire_master,
            tag=tag,
            owner_acct_id=owner_acct_id,
            message_format=message_format,
        )
