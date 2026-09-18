"""Elastic ingest with callbacks instead of Futures.

Same four-call tour as elastic_ingest.py, using append_row / append_rows.
Handlers are the only acknowledgement signal.

They run on the SDK acknowledgement thread. Keep them to a single
queue.SimpleQueue.put: no I/O, no SDK calls, and no lock the ingest thread
also waits on. SimpleQueue.put never blocks, so a full queue cannot deadlock
the ack thread the way Queue.put(maxsize=...) can. The GIL still serializes
Python bytecode — a slow handler delays this channel's acks and other Python
work.

Count and checkpoint on the ingest thread after get(). Do not mutate shared
counters in the handler; i += 1 is not atomic.
"""

from queue import Empty, SimpleQueue
import os
import time

os.environ.setdefault("SS_LOG_LEVEL", "warn")

from elastic_ingest import (
    ACK_TIMEOUT_SECONDS,
    BATCH_COUNT,
    BATCH_SIZE,
    FIRE_AND_FORGET_BATCH_SIZE,
    FIRE_AND_FORGET_ROWS,
    PIPELINE_ROWS,
    create_client,
    sample_row,
)


class AckInbox:
    """Handoff from the SDK ack thread to the ingest thread."""

    def __init__(self) -> None:
        self._events = SimpleQueue()

    def install(self, channel) -> None:
        channel.set_success_handler(self.on_success)
        channel.set_error_handler(self.on_error)

    def on_success(self, detail) -> None:
        for token in detail.append_tokens:
            self._events.put(("ok", token))

    def on_error(self, detail) -> None:
        self._events.put(("err", detail))

    def wait_one(self, timeout: float):
        try:
            kind, payload = self._events.get(timeout=timeout)
        except Empty:
            raise TimeoutError("Timed out waiting for an acknowledgement") from None
        if kind == "err":
            raise payload.error
        return payload

    def wait_n(self, count: int, timeout: float) -> list:
        deadline = time.monotonic() + timeout
        got = []
        for _ in range(count):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Timed out waiting for acknowledgements")
            got.append(self.wait_one(remaining))
        return got

    def raise_pending_errors(self) -> None:
        while True:
            try:
                kind, payload = self._events.get_nowait()
            except Empty:
                return
            if kind == "err":
                raise payload.error


def main() -> None:
    client = create_client()
    try:
        channel = client.get_elastic_channel()
        inbox = AckInbox()
        inbox.install(channel)
        next_id = 0

        # 1. Wait per row — simplest call, and the slow path.
        channel.append_row(sample_row(next_id), f"event-{next_id}")
        inbox.wait_one(ACK_TIMEOUT_SECONDS)
        next_id += 1

        # 2. Recommended: submit every row before waiting. The SDK batches
        # these for transport.
        for event_id in range(next_id, next_id + PIPELINE_ROWS):
            channel.append_row(sample_row(event_id), f"event-{event_id}")
        inbox.wait_n(PIPELINE_ROWS, ACK_TIMEOUT_SECONDS)
        next_id += PIPELINE_ROWS

        # 3. Optional: application batches when you already have a group, or
        # want fewer tokens. Same pipelining; not required for wire efficiency.
        for batch_index in range(BATCH_COUNT):
            rows = [
                sample_row(event_id)
                for event_id in range(next_id, next_id + BATCH_SIZE)
            ]
            channel.append_rows(rows, f"batch-{batch_index}")
            next_id += BATCH_SIZE
        inbox.wait_n(BATCH_COUNT, ACK_TIMEOUT_SECONDS)

        # 4. Fire-and-forget: do not wait on the inbox. wait_for_flush covers
        # these appends; then look for errors the handler already queued.
        for event_id in range(next_id, next_id + FIRE_AND_FORGET_ROWS):
            channel.append_row(sample_row(event_id), f"event-{event_id}")
        next_id += FIRE_AND_FORGET_ROWS
        fire_and_forget_batch = [
            sample_row(event_id)
            for event_id in range(next_id, next_id + FIRE_AND_FORGET_BATCH_SIZE)
        ]
        channel.append_rows(fire_and_forget_batch, "batch-ff")
        next_id += FIRE_AND_FORGET_BATCH_SIZE
        channel.wait_for_flush(timeout_seconds=ACK_TIMEOUT_SECONDS)
        inbox.raise_pending_errors()

        print(f"Durably acknowledged {next_id} rows")
    finally:
        client.close(wait_for_flush=True, timeout_seconds=ACK_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
