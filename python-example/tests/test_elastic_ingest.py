import os
import sys
from concurrent.futures import Future
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import elastic_ingest as ingest


class Channel:
    def __init__(self):
        self.calls = []
        self.success_handler = None
        self.error_handler = None
        self.flush_timeouts = []

    def _future(self):
        future = Future()
        future.set_result(None)
        return future

    def append_row_with_wait(self, row, token):
        self.calls.append(("append_row_with_wait", row, token))
        return self._future()

    def append_rows_with_wait(self, rows, token):
        self.calls.append(("append_rows_with_wait", list(rows), token))
        return self._future()

    def append_row(self, row, token):
        self.calls.append(("append_row", row, token))
        if self.success_handler is not None:
            self.success_handler(SimpleNamespace(append_tokens=(token,)))

    def append_rows(self, rows, token):
        self.calls.append(("append_rows", list(rows), token))
        if self.success_handler is not None:
            self.success_handler(SimpleNamespace(append_tokens=(token,)))

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


def test_connection_properties_uses_profile_without_pat(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_PAT", raising=False)

    assert ingest.connection_properties() is None


def test_connection_properties_builds_pat_settings(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_PAT", "token")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "account")
    monkeypatch.setenv("SNOWFLAKE_URL", "https://account.snowflakecomputing.com")
    monkeypatch.setenv("SNOWFLAKE_ROLE", "role")

    assert ingest.connection_properties() == {
        "authorization_type": "PAT",
        "personal_access_token": "token",
        "account": "account",
        "url": "https://account.snowflakecomputing.com",
        "role": "role",
    }


def test_connection_properties_requires_account_and_url(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_PAT", "token")
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
    monkeypatch.delenv("SNOWFLAKE_URL", raising=False)

    with pytest.raises(ValueError, match="SNOWFLAKE_ACCOUNT"):
        ingest.connection_properties()


def test_sample_row_uses_stable_event_id():
    row = ingest.sample_row(7)

    assert row["EVENT_ID"] == 7
    assert row["C1"] == 7
    assert row["C2"] == "event-7"


def test_main_closes_the_client(monkeypatch):
    channel = Channel()
    client = Client(channel)
    monkeypatch.setattr(ingest, "create_client", lambda: client)

    ingest.main()

    assert client.closes == [{"wait_for_flush": True, "timeout_seconds": 60}]
    assert channel.flush_timeouts
