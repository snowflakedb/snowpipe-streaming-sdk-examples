"""Single-writer named-channel producer with source-offset recovery.

Stream individual rows and checkpoint committed offsets, never local submission.
Source events remain replayable until confirmed. SDK backpressure pauses reading;
only SDK invalidation reopens the channel. Do not share channel ownership.
"""

import os
import time

from snowflake.ingest.streaming import StreamingIngestError, StreamingIngestErrorCode
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
    producer = NamedProducer()
    completed = False
    try:
        run(producer, source)
        completed = True
        print(f"Committed source checkpoint: {source.committed}")
    finally:
        if not completed:
            print(f"Stopped. Retain source events after checkpoint {source.committed} for replay")
        producer.close(completed)



def run(producer, source):
    """Append continuously; poll committed progress without waiting for every submitted row."""
    source.seek(producer.open())
    submitted = source.committed
    event = None
    exhausted = False
    retry_attempts = 0
    since_poll = 0
    next_poll = time.monotonic() + CHECKPOINT_SECONDS
    deadline = time.monotonic() + OUTAGE_SECONDS
    while True:
        try:
            outstanding = submitted > source.committed
            if outstanding and (exhausted or event is not None or since_poll >= CHECKPOINT_ROWS
                                or time.monotonic() >= next_poll
                                or submitted - source.committed >= MAX_PENDING_EVENTS):
                previous = source.committed
                collect_progress(producer, submitted, source)
                if source.committed > previous:
                    deadline = time.monotonic() + OUTAGE_SECONDS
                    retry_attempts = 0
                next_poll = time.monotonic() + CHECKPOINT_SECONDS
                since_poll = 0
            if submitted == source.committed and event is None:
                deadline = time.monotonic() + OUTAGE_SECONDS
                if exhausted:
                    return
            remaining(deadline)
            if exhausted or submitted - source.committed >= MAX_PENDING_EVENTS:
                time.sleep(min(POLL_SECONDS, remaining(deadline)))
                continue
            if event is None:
                event = source.read()
            if event is None:
                exhausted = True
                continue
            offset, row = event
            producer.channel.append_row(row, str(offset))
            submitted = offset
            since_poll += 1
            event = None
        except StreamingIngestError as error:
            if not retryable(error):
                raise
            if error.http_status_code != 429:
                retry_attempts += 1
                if retry_attempts >= MAX_ATTEMPTS:
                    raise
            if error.error_code.value in INVALIDATION:
                previous = source.committed
                source.seek(producer.recover(error))
                if source.committed > previous:
                    deadline = time.monotonic() + OUTAGE_SECONDS
                submitted = source.committed
                event = None
                exhausted = False
            backoff(2, deadline)


def collect_progress(producer, submitted, source):
    """Fetch status once and commit the confirmed prefix, checking row health first."""
    status = producer.channel.get_channel_status()
    if status.rows_error_count:
        raise RuntimeError("Row errors require reconciliation before source handoff")
    if status.status_code != "SUCCESS":
        raise StreamingIngestError(StreamingIngestErrorCode.INVALID_CHANNEL_ERROR,
                                   status.status_code, 409, "Conflict")
    committed = min(submitted, parse_offset(status.latest_committed_offset_token))
    if committed > source.committed:
        source.acknowledge(committed)


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

CHANNEL_NAME = os.environ.get("SNOWFLAKE_CHANNEL", "production-source-0")


def parse_offset(token):
    """Decode this sample's numeric source offset; an absent token means no progress."""
    return 0 if token is None else int(token)


class NamedProducer:
    """Owns one stable channel and preserves its server offset during recovery."""
    def __init__(self, factory=create_client):
        self.factory = factory
        self.client = None
        self.channel = None

    def open(self):
        """Open the owned named channel and return its authoritative committed source offset."""
        if self.client is None:
            self.client = self.factory()
        self.channel, status = self.client.open_channel(CHANNEL_NAME)
        if status.rows_error_count:
            raise RuntimeError("Row errors require reconciliation before source handoff")
        return parse_offset(status.latest_committed_offset_token)

    def recover(self, error):
        """Reopen without resetting the server offset, recreating an invalid client if needed."""
        if error.error_code.value == "InvalidClientError":
            self.close(False)
        elif self.channel is not None:
            try:
                self.channel.close(wait_for_flush=False, timeout_seconds=0)
            except StreamingIngestError:
                pass
        try:
            return self.open()
        except StreamingIngestError as reopened:
            if reopened.error_code.value not in {"InvalidClientError", "ClosedClientError"}:
                raise
            self.close(False)
            return self.open()

    def close(self, flush):
        """Close the current client; flush only when requested by the caller."""
        if self.client is not None:
            try:
                self.client.close(wait_for_flush=flush, timeout_seconds=30)
            finally:
                self.client = None


if __name__ == "__main__":
    main()
