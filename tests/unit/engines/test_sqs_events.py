import pytest

import saltext.sqs_events.engines.sqs_events_mod as sqs_events_engine


@pytest.fixture
def configure_loader_modules():
    module_globals = {
        "__salt__": {"this_does_not_exist.please_replace_it": lambda: True},
    }
    return {
        sqs_events_engine: module_globals,
    }


def test_replace_this_this_with_something_meaningful():
    assert "this_does_not_exist.please_replace_it" in sqs_events_engine.__salt__
    assert sqs_events_engine.__salt__["this_does_not_exist.please_replace_it"]() is True
