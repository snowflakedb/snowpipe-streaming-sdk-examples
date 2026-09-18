# Python Snowpipe Streaming SDK Examples

[`elastic_ingest.py`](./elastic_ingest.py) and
[`elastic_ingest_callbacks.py`](./elastic_ingest_callbacks.py) tour the append
APIs (Futures vs handlers). [`elastic_ingest_unbounded.py`](./elastic_ingest_unbounded.py)
keeps that pipelined single-row pattern going for a large default row count.
The SDK requirement is `snowpipe-streaming` **1.8.0 or later**.

## Examples

| Path | File | What it adds |
| --- | --- | --- |
| Elastic ingest | [`elastic_ingest.py`](./elastic_ingest.py) | The four append APIs. Pipelined single-row `append_row_with_wait` is the recommended default; `append_rows` is optional. |
| Elastic ingest (callbacks) | [`elastic_ingest_callbacks.py`](./elastic_ingest_callbacks.py) | Same tour with `append_row` / `append_rows`. Handlers only enqueue; the ingest thread waits. |
| Elastic ingest (unbounded) | [`elastic_ingest_unbounded.py`](./elastic_ingest_unbounded.py) | Pipelined single-row appends at volume (10M rows by default). Ctrl+C drains accepted work and prints ack latency and rows/s. |

The [`monitoring`](./monitoring) directory contains separate monitoring and abort
examples.

## Setup

### Requirements

- Python 3.9 or later
- A Snowflake account with RSA key-pair authentication
- A role allowed to insert into the target table

Install the SDK and test dependency:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt pytest
```

### Target table

```sql
CREATE OR REPLACE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE (
    DATA VARIANT,
    EVENT_ID NUMBER,
    C1 NUMBER,
    C2 VARCHAR,
    TS TIMESTAMP_NTZ
);
```

No `CREATE PIPE` is required. Table-mode clients use the default
`MY_TABLE-STREAMING` pipe.

### Authentication

Create `profile.json` from `profile.json.example`:

```json
{
  "account": "<account_identifier>",
  "user": "your_username",
  "url": "https://<account_identifier>.snowflakecomputing.com:443",
  "private_key_file": "rsa_key.p8",
  "role": "your_role"
}
```

Set object names through the environment or edit their example defaults:

```bash
export SNOWFLAKE_DATABASE=MY_DATABASE
export SNOWFLAKE_SCHEMA=MY_SCHEMA
export SNOWFLAKE_TABLE=MY_TABLE
```

Alternatively, set `SNOWFLAKE_PAT`, `SNOWFLAKE_ACCOUNT`, and `SNOWFLAKE_URL`.
`SNOWFLAKE_ROLE` is optional. The examples pass these values through
`connection_properties()`.

## Run

```bash
python3 elastic_ingest.py
python3 elastic_ingest_callbacks.py
python3 elastic_ingest_unbounded.py
```

Set `SNOWFLAKE_TEST_ROWS` to change the generated row count in the unbounded
example (default 10,000,000). Ctrl+C stops intake, waits for appends already
accepted by the SDK, prints stats, and closes.

## Semantics

### Elastic Channels

An Elastic acknowledgement confirms that Snowflake durably accepted the
append. It does not confirm row validity or immediate table visibility. Check
the target table and its error table separately.

The SDK batches rows for transport. Waiting after every append is the slow
path. Pipelined `append_row_with_wait` — submit many rows, then wait on the
Futures — is the recommended default for throughput and simplicity.
`append_rows` / `append_rows_with_wait` are optional: one Future and one
`append_token` for a logical group when you already have a batch, or to cut
Python/FFI call overhead. They do not replace SDK transport batching.
Fire-and-forget `append_row` / `append_rows` return no Future; success and
error handlers are the only acknowledgement signal, and they echo the
caller-supplied `append_token`. Those handlers run on the SDK acknowledgement
thread: enqueue onto a `queue.SimpleQueue` and return. Do not wait, take locks
the ingest thread also waits on, or call back into the SDK from a handler.
`SimpleQueue.put` never blocks; a bounded `Queue.put` can deadlock the ack
thread. Count on the ingest thread after `get()` — `i += 1` in a handler is
not atomic.

Average ack latency can look large next to rows/s. Many appends are in
flight, so throughput is not `1 / latency`.

Replaying an Elastic append can create a duplicate. Use stable source event IDs
and define downstream reconciliation for your application.

## Tests

The tests use fake SDK clients and do not connect to Snowflake:

```bash
python3 -m pytest tests -q
```

They cover connection properties, callback handoff via `SimpleQueue`, and
clean shutdown after interrupt.

## Additional Resources

- [Elastic Channels overview](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-overview)
- [Elastic Channels getting started](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-getting-started)
- [Elastic Channels best practices](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-best-practices)
- [Snowpipe Streaming SDK on PyPI](https://pypi.org/project/snowpipe-streaming/)
