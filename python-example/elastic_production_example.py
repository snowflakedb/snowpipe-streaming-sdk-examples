"""Append while capacity permits; retire only confirmed source progress.

The SDK owns batching and transport retries. Caller timeouts keep the original
Future alive. Retain/replay unacknowledged source events across restarts; Elastic
replay may duplicate events, so EVENT_ID must be stable and source-unique.
"""

import logging
import time
from dataclasses import dataclass

from snowflake.ingest.streaming import StreamingIngestError
import os
import random
from snowflake.ingest.streaming import StreamingIngestClient

MAX_PENDING_EVENTS = 100_000
CHECKPOINT_ROWS = 1_000
CHECKPOINT_SECONDS = 5.0
OUTAGE_SECONDS = 30 * 60.0
POLL_SECONDS = 1.0
MAX_ATTEMPTS = 6
INVALIDATION = {"InvalidChannelError", "InvalidClientError", "ClosedChannelError",
                "ClosedElasticChannelError", "ClosedClientError"}
TRANSIENT = {408, 429, 500, 502, 503, 504}


# Start here: create a source, connect, and stream retained events.

def main():
    """Run the sample and close the client, retaining unconfirmed source work on failure."""
    source = SampleEventSource(
        int(os.environ.get("SNOWFLAKE_TEST_ROWS", "10000")),
        int(os.environ.get("SNOWFLAKE_SOURCE_CHECKPOINT", "0")),
    )
    producer = ElasticProducer()
    completed = False
    try:
        producer.open()
        run(producer, source)
        completed = True
        print(f"Durable source checkpoint: {source.committed}; materialization is separate")
    finally:
        if not completed:
            print(f"Stopped. Retain source events after checkpoint {source.committed} for replay")
        producer.close(completed)



def run(producer, source):
    """Append while capacity is available; collect acknowledgements without draining each window."""
    pending = []
    event = None
    exhausted = False
    since_scan = 0
    next_scan = time.monotonic() + CHECKPOINT_SECONDS
    deadline = time.monotonic() + OUTAGE_SECONDS
    while True:
        confirmed_before = source.committed
        if exhausted or event is not None or since_scan >= CHECKPOINT_ROWS or time.monotonic() >= next_scan or len(pending) >= MAX_PENDING_EVENTS:
            collect_progress(producer, pending, source, deadline)
            since_scan = 0
            next_scan = time.monotonic() + CHECKPOINT_SECONDS
        if source.committed > confirmed_before or (not pending and event is None):
            deadline = time.monotonic() + OUTAGE_SECONDS
        if exhausted and not pending:
            return
        remaining(deadline)
        if exhausted or len(pending) >= MAX_PENDING_EVENTS:
            time.sleep(min(POLL_SECONDS, remaining(deadline)))
            continue
        if event is None:
            event = source.read()
        if event is None:
            exhausted = True
            continue
        try:
            # The source keeps its copy; SDK acceptance alone is not source acknowledgement.
            offset, row = event
            pending.append(Pending(event, producer.channel.append_row_with_wait(row, str(offset)),
                                   producer.generation))
            event = None
            since_scan += 1
            if pending[-1].future.done() and pending[-1].future.exception() is not None:
                since_scan = CHECKPOINT_ROWS
        except StreamingIngestError as error:
            if error.http_status_code != 429:
                if not retryable(error):
                    raise
                if error.error_code.value in INVALIDATION:
                    producer.recover(producer.generation)
                pending.append(append_event(producer, event, deadline))
                event = None
            else:
                # Keep this rejected event and collect progress before retrying it.
                backoff(2, deadline)


def collect_progress(producer, pending, source, deadline):
    """Retire only a contiguous confirmed prefix; never wait for an unfinished Future."""
    for index, item in enumerate(pending):
        if not item.future.done():
            continue
        error = item.future.exception()
        if error is not None:
            if not retryable(error) or item.retries >= MAX_ATTEMPTS - 1:
                raise error
            if error.error_code.value in INVALIDATION:
                producer.recover(item.generation)
            backoff(item.retries, deadline)
            replacement = append_event(producer, item.event, deadline)
            replacement.retries = item.retries + 1
            pending[index] = replacement
    confirmed_count = 0
    for item in pending:
        if not item.future.done() or item.future.exception() is not None:
            break
        confirmed_count += 1
    if confirmed_count:
        # Persist source progress before discarding acknowledgement bookkeeping.
        source.acknowledge(pending[confirmed_count - 1].event[0])
        del pending[:confirmed_count]


# Supporting delivery and connection details.

def retryable(error):
    """Identify SDK failures eligible for bounded application retry."""
    return isinstance(error, StreamingIngestError) and (
        error.error_code.value in INVALIDATION or error.http_status_code in TRANSIENT
    )


def remaining(deadline):
    """Return the remaining stalled-progress budget without advancing source progress."""
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise TimeoutError("Stalled-progress deadline exceeded; retain events after the confirmed source checkpoint")
    return seconds


def backoff(attempt, deadline):
    """Wait with capped jitter without exceeding the remaining checkpoint budget."""
    delay = random.uniform(0, min(10.0, 0.25 * 2 ** min(attempt, 6)))
    time.sleep(min(delay, remaining(deadline)))


def create_client():
    """Create a table client using the authentication profile or explicitly configured PAT."""
    properties = None
    if os.environ.get("SNOWFLAKE_PAT"):
        properties = {
            "authorization_type": "PAT",
            "personal_access_token": os.environ["SNOWFLAKE_PAT"],
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "url": os.environ["SNOWFLAKE_URL"],
        }
        if os.environ.get("SNOWFLAKE_ROLE"):
            properties["role"] = os.environ["SNOWFLAKE_ROLE"]
    return StreamingIngestClient.from_table(
        client_name=f"production-{os.getpid()}",
        db_name=os.environ.get("SNOWFLAKE_DATABASE", "MY_DATABASE"),
        schema_name=os.environ.get("SNOWFLAKE_SCHEMA", "MY_SCHEMA"),
        table_name=os.environ.get("SNOWFLAKE_TABLE", "MY_TABLE"),
        profile_json=None if properties else os.environ.get("SNOWFLAKE_PROFILE", "profile.json"),
        properties=properties,
    )


class SampleEventSource:
    """Synthetic input only: no external source and no persisted checkpoint.

    Replace read/acknowledge/seek with your retained source operations.
    """

    def __init__(self, total=10_000, checkpoint=0):
        if not 0 <= checkpoint <= total:
            raise ValueError("Require 0 <= source checkpoint <= total")
        self.total = total
        self.committed = checkpoint
        self.next_offset = checkpoint + 1

    def read(self):
        """Return the next sample event without acknowledging source progress."""
        # Replace this deterministic fixture with reads from your retained source.
        if self.next_offset > self.total:
            return None
        offset = self.next_offset
        self.next_offset += 1
        # Replace this mapping with your target table columns and stable event ID.
        row = {"EVENT_ID": offset, "C1": offset, "C2": f"event-{offset}"}
        return offset, row

    def acknowledge(self, offset):
        """Record confirmed progress; replace with your source's durable commit operation."""
        # In production, persist/commit source progress here before retiring events.
        if not self.committed <= offset <= self.total:
            raise ValueError("Invalid source checkpoint")
        self.committed = offset

    def seek(self, committed):
        """Resume sample reads after confirmed progress; replace with your source seek operation."""
        # Position the retained source strictly after Snowflake committed progress.
        self.acknowledge(committed)
        self.next_offset = committed + 1


@dataclass
class Pending:
    """Track an event, its acknowledgement, and the client generation that submitted it."""
    event: tuple
    future: object
    generation: int
    retries: int = 0


class ElasticProducer:
    """Owns the current SDK client; old failures must not replace a fresh client."""
    def __init__(self, factory=create_client):
        self.factory = factory
        self.client = None
        self.generation = 0

    def open(self):
        """Create a client and obtain its cached Elastic Channel."""
        client = self.factory()
        try:
            self.channel = client.get_elastic_channel()
        except BaseException:
            client.close(wait_for_flush=False, timeout_seconds=0)
            raise
        self.client = client
        self.generation += 1

    def recover(self, generation):
        """Replace the invalid client only if the failure belongs to its current generation."""
        # Several pending appends can fail from the same old client. Replace it once.
        if generation != self.generation:
            return
        self.close(False)
        self.open()

    def close(self, flush):
        """Close the current client; flush only when requested by the caller."""
        if self.client is not None:
            try:
                self.client.close(wait_for_flush=flush, timeout_seconds=30)
            finally:
                self.client = None


def append_event(producer, event, deadline):
    """Submit one event with bounded retry and retain its original acknowledgement Future."""
    for attempt in range(MAX_ATTEMPTS):
        remaining(deadline)
        try:
            source_offset, row = event
            # This SDK call writes the event; the Future confirms durable acceptance.
            return Pending(event, producer.channel.append_row_with_wait(row, str(source_offset)),
                           producer.generation)
        except StreamingIngestError as error:
            if not retryable(error) or attempt == MAX_ATTEMPTS - 1:
                raise
            if error.error_code.value in INVALIDATION:
                producer.recover(producer.generation)
            backoff(attempt, deadline)
    raise RuntimeError("Submission retry budget exhausted")


if __name__ == "__main__":
    main()
