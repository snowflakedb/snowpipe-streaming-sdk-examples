"""Elastic ingest: the four append APIs.

Elastic Channels need no channel name, offset token, or recovery config.
Create a table-mode client, get the channel, and append.

The SDK batches rows for transport. Waiting after every append is the slow
path. Pipelining single-row append_row_with_wait calls is the recommended
default for throughput and simplicity.

append_rows is optional: one Future and one append_token for a logical group
when you already have a batch, or to cut Python/FFI call overhead. It does
not replace SDK transport batching.

Fire-and-forget append_row/append_rows return no Future. Success and error
handlers are the only acknowledgement signal; pass your own append_token and
the SDK echoes it back.
"""

import os
import time
from typing import Optional
import uuid

os.environ.setdefault("SS_LOG_LEVEL", "warn")

from snowflake.ingest import streaming


DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "MY_DATABASE")
SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "MY_SCHEMA")
TABLE = os.environ.get("SNOWFLAKE_TABLE", "MY_TABLE")
PROFILE = os.environ.get("SNOWFLAKE_PROFILE", "profile.json")

PIPELINE_ROWS = 10
BATCH_COUNT = 2
BATCH_SIZE = 5
FIRE_AND_FORGET_ROWS = 3
FIRE_AND_FORGET_BATCH_SIZE = 4
ACK_TIMEOUT_SECONDS = 60

Row = dict[str, object]


def connection_properties() -> Optional[dict[str, str]]:
    pat = os.environ.get("SNOWFLAKE_PAT")
    if not pat:
        return None
    if not os.environ.get("SNOWFLAKE_ACCOUNT") or not os.environ.get("SNOWFLAKE_URL"):
        raise ValueError("PAT authentication requires SNOWFLAKE_ACCOUNT and SNOWFLAKE_URL")

    properties = {
        "authorization_type": "PAT",
        "personal_access_token": pat,
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "url": os.environ["SNOWFLAKE_URL"],
    }
    if os.environ.get("SNOWFLAKE_ROLE"):
        properties["role"] = os.environ["SNOWFLAKE_ROLE"]
    return properties


def create_client() -> streaming.StreamingIngestClient:
    properties = connection_properties()
    return streaming.StreamingIngestClient.from_table(
        client_name=f"ingest-{uuid.uuid4()}",
        db_name=DATABASE,
        schema_name=SCHEMA,
        table_name=TABLE,
        profile_json=None if properties else PROFILE,
        properties=properties,
    )


def sample_row(event_id: int) -> Row:
    return {
        "EVENT_ID": event_id,
        "C1": event_id,
        "C2": f"event-{event_id}",
    }


def main() -> None:
    client = create_client()
    try:
        # Elastic Channels belong to their client and are not closed separately.
        channel = client.get_elastic_channel()
        next_id = 0

        # 1. Wait per row — simplest call, and the slow path.
        channel.append_row_with_wait(sample_row(next_id), f"event-{next_id}").result(
            timeout=ACK_TIMEOUT_SECONDS
        )
        next_id += 1

        # 2. Recommended: submit every row before waiting. The SDK batches
        # these for transport.
        pending = []
        for event_id in range(next_id, next_id + PIPELINE_ROWS):
            pending.append(channel.append_row_with_wait(sample_row(event_id), None))
        for future in pending:
            future.result(timeout=ACK_TIMEOUT_SECONDS)
        next_id += PIPELINE_ROWS

        # 3. Optional: application batches when you already have a group, or
        # want fewer Futures and tokens. Same pipelining; not required for
        # wire efficiency.
        pending = []
        for batch_index in range(BATCH_COUNT):
            rows = [
                sample_row(event_id)
                for event_id in range(next_id, next_id + BATCH_SIZE)
            ]
            pending.append(channel.append_rows_with_wait(rows, f"batch-{batch_index}"))
            next_id += BATCH_SIZE
        for future in pending:
            future.result(timeout=ACK_TIMEOUT_SECONDS)

        # 4. Fire-and-forget: no Future. Handlers are the only ack signal.
        # They run on the SDK ack thread — cheap bookkeeping only. The SDK
        # echoes your append_token; it does not assign an offset.
        submitted_at = {}
        latencies = []
        failures = []

        def on_success(detail):
            now = time.monotonic()
            for token in detail.append_tokens:
                latencies.append(now - submitted_at[token])

        def on_error(detail):
            failures.append(detail)

        channel.set_success_handler(on_success)
        channel.set_error_handler(on_error)
        started = time.monotonic()
        callback_rows = 0
        for event_id in range(next_id, next_id + FIRE_AND_FORGET_ROWS):
            token = f"event-{event_id}"
            submitted_at[token] = time.monotonic()
            channel.append_row(sample_row(event_id), token)
        next_id += FIRE_AND_FORGET_ROWS
        callback_rows += FIRE_AND_FORGET_ROWS
        fire_and_forget_batch = [
            sample_row(event_id)
            for event_id in range(next_id, next_id + FIRE_AND_FORGET_BATCH_SIZE)
        ]
        submitted_at["batch-ff"] = time.monotonic()
        channel.append_rows(fire_and_forget_batch, "batch-ff")
        next_id += FIRE_AND_FORGET_BATCH_SIZE
        callback_rows += FIRE_AND_FORGET_BATCH_SIZE
        channel.wait_for_flush(timeout_seconds=ACK_TIMEOUT_SECONDS)
        if failures:
            raise failures[0].error
        elapsed = time.monotonic() - started

        print(f"Durably acknowledged {next_id} rows")
        if latencies and elapsed > 0:
            avg_ack_ms = 1000 * sum(latencies) / len(latencies)
            rps = callback_rows / elapsed
            # Average ack latency can look large. Many appends are in flight, so
            # throughput is not 1 / latency — parallelism carries the rate.
            print(f"Callback path: avg ack latency {avg_ack_ms:.1f} ms, {rps:.0f} rows/s")
    finally:
        client.close(wait_for_flush=True, timeout_seconds=ACK_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
