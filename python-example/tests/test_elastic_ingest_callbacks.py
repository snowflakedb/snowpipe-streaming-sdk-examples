import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import elastic_ingest_callbacks as callbacks


class Channel:
    def __init__(self):
        self.success_handler = None
        self.error_handler = None
        self.flush_timeouts = []

    def _complete(self, token):
        if self.success_handler is not None:
            self.success_handler(SimpleNamespace(append_tokens=(token,)))

    def append_row(self, row, token):
        self._complete(token)

    def append_rows(self, rows, token):
        self._complete(token)

    def set_success_handler(self, handler):
        self.success_handler = handler

    def set_error_handler(self, handler):
        self.error_handler = handler

    def wait_for_flush(self, timeout_seconds=None):
        self.flush_timeouts.append(timeout_seconds)


class Client:
    def __init__(self, channel):
        self.channel = channel
        self.closes = []

    def get_elastic_channel(self):
        return self.channel

    def close(self, **options):
        self.closes.append(options)


def test_ack_inbox_wait_one_returns_token():
    inbox = callbacks.AckInbox()
    inbox.on_success(SimpleNamespace(append_tokens=("t1",)))

    assert inbox.wait_one(1) == "t1"


def test_ack_inbox_wait_one_raises_handler_error():
    inbox = callbacks.AckInbox()
    inbox.on_error(SimpleNamespace(error=RuntimeError("boom")))

    with pytest.raises(RuntimeError, match="boom"):
        inbox.wait_one(1)


def test_ack_inbox_times_out_without_an_event():
    inbox = callbacks.AckInbox()

    with pytest.raises(TimeoutError):
        inbox.wait_one(0.01)


def test_main_closes_the_client(monkeypatch):
    channel = Channel()
    client = Client(channel)
    monkeypatch.setattr(callbacks, "create_client", lambda: client)

    callbacks.main()

    assert client.closes == [{"wait_for_flush": True, "timeout_seconds": 60}]
    assert channel.flush_timeouts
