# Node.js Snowpipe Streaming SDK Examples

[`elastic_ingest.js`](./elastic_ingest.js) and
[`elastic_ingest_callbacks.js`](./elastic_ingest_callbacks.js) tour the append
APIs (Promises vs handlers). [`elastic_ingest_unbounded.js`](./elastic_ingest_unbounded.js)
keeps that pipelined single-row pattern going for a large default row count.
These examples require `snowpipe-streaming` **1.8.0 or later** and Node.js 20
or later.

## Examples

| Path | File | What it adds |
| --- | --- | --- |
| Elastic ingest | [`elastic_ingest.js`](./elastic_ingest.js) | The four append APIs. Pipelined single-row `appendRowWithWait` is the recommended default; `appendRows` is optional. |
| Elastic ingest (callbacks) | [`elastic_ingest_callbacks.js`](./elastic_ingest_callbacks.js) | Same tour with `appendRow` / `appendRows`. Handlers only enqueue; the ingest path waits. |
| Elastic ingest (unbounded) | [`elastic_ingest_unbounded.js`](./elastic_ingest_unbounded.js) | Pipelined single-row appends at volume (10M rows by default). Ctrl+C drains accepted work and prints ack latency and rows/s. |

## Setup

### Requirements

- Node.js 20 or later
- npm
- A Snowflake account with RSA key-pair authentication
- A role allowed to insert into the target table

Install the SDK:

```bash
npm install
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
`connectionProperties()`.

## Run

```bash
npm start
node elastic_ingest_callbacks.js
node elastic_ingest_unbounded.js
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
path. Pipelined `appendRowWithWait` — submit many rows, then `await` the
Promises — is the recommended default for throughput and simplicity.
`appendRows` / `appendRowsWithWait` are optional: one Promise and one
append token for a logical group when you already have a batch, or to cut
JS/FFI call overhead. They do not replace SDK transport batching.
Fire-and-forget `appendRow` / `appendRows` return no Promise; success and
error handlers are the only acknowledgement signal, and they echo the
caller-supplied append token. Those handlers run on the SDK acknowledgement
callback: enqueue a cheap event and return. Do not wait, block the event
loop, or call back into the SDK from a handler. Count on the ingest path
after `waitOne()`.

Average ack latency can look large next to rows/s. Many appends are in
flight, so throughput is not `1 / latency`.

Replaying an Elastic append can create a duplicate. Use stable source event IDs
and define downstream reconciliation for your application.

## Tests

The tests use SDK-shaped fake clients and do not connect to Snowflake:

```bash
npm test
```

They cover connection properties, callback handoff, and clean shutdown after
interrupt.

## Additional Resources

- [Elastic Channels overview](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-overview)
- [Elastic Channels getting started](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-getting-started)
- [Elastic Channels best practices](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-best-practices)
- [Snowpipe Streaming SDK on npm](https://www.npmjs.com/package/snowpipe-streaming)
