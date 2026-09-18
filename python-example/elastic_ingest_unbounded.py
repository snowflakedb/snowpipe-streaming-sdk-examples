"""Elastic ingest of a large, uninterrupted row stream.

Create a table-mode client, get the channel, and keep appending. The SDK
batches rows for transport. This program submits many single-row
append_row_with_wait calls before waiting, and only pauses when too many
acknowledgement Futures are outstanding — not after every append.

Ctrl+C stops intake, waits for accepted appends, prints stats, and closes.
"""

from collections import deque
import os
import time

os.environ.setdefault("SS_LOG_LEVEL", "warn")

from elastic_ingest import (
    ACK_TIMEOUT_SECONDS,
    create_client,
    sample_row,
)


DEFAULT_ROWS = 10_000_000
MAX_PENDING_EVENTS = 10_000


def main() -> None:
    total = int(os.environ.get("SNOWFLAKE_TEST_ROWS", str(DEFAULT_ROWS)))
    client = create_client()
    pending = deque()
    acked = 0
    latency_sum = 0.0
    started = time.monotonic()
    wait_on_close = False
    try:
        channel = client.get_elastic_channel()
        try:
            for event_id in range(total):
                pending.append(
                    (
                        time.monotonic(),
                        channel.append_row_with_wait(sample_row(event_id), None),
                    )
                )
                if len(pending) < MAX_PENDING_EVENTS:
                    continue
                pending[0][1].result(timeout=ACK_TIMEOUT_SECONDS)
                while pending and pending[0][1].done():
                    submitted_at, future = pending.popleft()
                    future.result()
                    latency_sum += time.monotonic() - submitted_at
                    acked += 1
        except KeyboardInterrupt:
            pass
        while pending:
            pending[0][1].result(timeout=ACK_TIMEOUT_SECONDS)
            while pending and pending[0][1].done():
                submitted_at, future = pending.popleft()
                future.result()
                latency_sum += time.monotonic() - submitted_at
                acked += 1
        wait_on_close = True
        elapsed = time.monotonic() - started
        print(f"Durably acknowledged {acked} rows")
        if acked and elapsed > 0:
            avg_ack_ms = 1000 * latency_sum / acked
            rps = acked / elapsed
            # Average ack latency can look large. Many appends are in flight, so
            # throughput is not 1 / latency — parallelism carries the rate.
            print(f"avg ack latency {avg_ack_ms:.1f} ms, {rps:.0f} rows/s")
    except KeyboardInterrupt:
        pass
    finally:
        client.close(wait_for_flush=wait_on_close, timeout_seconds=ACK_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
