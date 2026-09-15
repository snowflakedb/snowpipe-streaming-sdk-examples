import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import named_channel_checkpoint_example as named
support = named
from snowflake.ingest.streaming import StreamingIngestError, StreamingIngestErrorCode


def error(code, status):
    return StreamingIngestError(code, "synthetic", status, str(status))


class Channel:
    def __init__(self, committed=0):
        self.committed = committed
        self.calls = []
        self.on_append = None
        self.waits = 0
        self.errors = 0

    def append_row(self, row, token):
        self.calls.append(int(token))
        if self.on_append:
            self.on_append(int(token))
        self.committed = int(token)

    def wait_for_commit(self, predicate, **options):
        self.waits += 1
        assert predicate(str(self.committed))

    def get_channel_status(self):
        return SimpleNamespace(status_code="SUCCESS", rows_error_count=self.errors,
                               latest_committed_offset_token=str(self.committed))


class Session:
    def __init__(self, committed=0):
        self.channel = Channel(committed)
        self.recoveries = 0

    def open(self):
        return self.channel.committed

    def recover(self, failure):
        self.recoveries += 1
        self.channel.on_append = None
        return self.channel.committed


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(support, "backoff", lambda attempt, deadline: support.remaining(deadline))


def test_restart_seeks_after_server_committed_offset():
    session = Session(2)
    source = support.SampleEventSource(5)
    named.run(session, source)
    assert session.channel.calls == [3, 4, 5]
    assert session.channel.waits == 0
    assert source.committed == 5


def test_429_retains_current_event_without_reading_next():
    session = Session()
    source = support.SampleEventSource(3)

    def pressure(offset):
        assert source.next_offset == offset + 1
        session.channel.on_append = None
        raise error(StreamingIngestErrorCode.RECEIVER_SATURATED, 429)

    session.channel.on_append = pressure
    named.run(session, source)
    assert session.channel.calls == [1, 1, 2, 3]
    assert session.recoveries == 0
    assert source.committed == 3


@pytest.mark.parametrize("code", [StreamingIngestErrorCode.INVALID_CHANNEL_ERROR,
                                  StreamingIngestErrorCode.CLOSED_CHANNEL_ERROR])
def test_invalidation_replays_after_server_committed_record(code):
    session = Session()

    def invalidate(offset):
        if offset == 3:
            raise error(code, 409)

    session.channel.on_append = invalidate
    source = support.SampleEventSource(4)
    named.run(session, source)
    assert session.channel.calls == [1, 2, 3, 3, 4]
    assert session.recoveries == 1
    assert source.committed == 4


def test_partial_progress_does_not_wait_or_reopen():
    producer = Session(1)
    source = named.SampleEventSource(3)
    named.collect_progress(producer, 3, source)
    assert source.committed == 1
    assert producer.channel.waits == 0
    assert producer.recoveries == 0


def test_row_errors_prevent_source_handoff():
    session = Session()
    session.channel.errors = 1
    source = support.SampleEventSource(2)
    with pytest.raises(RuntimeError, match="Row errors"):
        named.run(session, source)
    assert source.committed == 0


def test_permanent_failure_preserves_checkpoint():
    session = Session()
    session.channel.on_append = lambda _: (_ for _ in ()).throw(error(StreamingIngestErrorCode.SF_API_AUTH_ERROR, 403))
    source = support.SampleEventSource(3)
    with pytest.raises(StreamingIngestError):
        named.run(session, source)
    assert source.committed == 0
    assert session.channel.calls == [1]


def test_expired_checkpoint_does_not_advance():
    source = support.SampleEventSource(3)
    with pytest.raises(TimeoutError):
        named.remaining(support.time.monotonic() - 1)
    assert source.committed == 0


def test_fully_committed_source_has_no_appends():
    session = Session(3)
    source = support.SampleEventSource(3)
    named.run(session, source)
    assert session.channel.calls == []
    assert source.committed == 3



def test_named_continues_past_progress_check_with_uncommitted_rows(monkeypatch):
    monkeypatch.setattr(named, "CHECKPOINT_ROWS", 2)
    producer = Session()
    source = named.SampleEventSource(5)
    def status():
        accepted = len(producer.channel.calls)
        return SimpleNamespace(status_code="SUCCESS", rows_error_count=0,
                               latest_committed_offset_token=str(accepted if accepted == 5 else 0))
    producer.channel.get_channel_status = status
    named.run(producer, source)
    assert producer.channel.calls == [1, 2, 3, 4, 5]
    assert source.committed == 5


def test_idle_caught_up_source_does_not_expire(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(named.time, "monotonic", lambda: clock[0])
    class Source(named.SampleEventSource):
        def read(self):
            clock[0] += 3600
            return None
    source = Source(0)
    named.run(Session(), source)
    assert source.committed == 0
