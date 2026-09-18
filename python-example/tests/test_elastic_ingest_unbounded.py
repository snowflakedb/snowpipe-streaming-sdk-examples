import os
import sys
from concurrent.futures import Future

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import elastic_ingest_unbounded as unbounded


class Channel:
    def __init__(self):
        self.calls = 0

    def append_row_with_wait(self, row, token):
        self.calls += 1
        future = Future()
        future.set_result(None)
        return future


class Client:
    def __init__(self, channel):
        self.channel = channel
        self.closes = []

    def get_elastic_channel(self):
        return self.channel

    def close(self, **options):
        self.closes.append(options)


def test_main_closes_the_client(monkeypatch):
    channel = Channel()
    client = Client(channel)
    monkeypatch.setenv("SNOWFLAKE_TEST_ROWS", "4")
    monkeypatch.setattr(unbounded, "create_client", lambda: client)

    unbounded.main()

    assert channel.calls == 4
    assert client.closes == [{"wait_for_flush": True, "timeout_seconds": 60}]


def test_main_closes_after_interrupt(monkeypatch):
    channel = Channel()
    client = Client(channel)

    def append_row_with_wait(row, token):
        channel.calls += 1
        if channel.calls == 3:
            raise KeyboardInterrupt
        future = Future()
        future.set_result(None)
        return future

    channel.append_row_with_wait = append_row_with_wait
    monkeypatch.setenv("SNOWFLAKE_TEST_ROWS", "10")
    monkeypatch.setattr(unbounded, "create_client", lambda: client)

    unbounded.main()

    assert channel.calls == 3
    assert client.closes == [{"wait_for_flush": True, "timeout_seconds": 60}]
