# Java Snowpipe Streaming SDK Examples

`ElasticChannelIngest` and `ElasticChannelCallbacks` tour the append APIs
(Futures vs handlers). `ElasticChannelUnbounded` keeps that pipelined
single-row pattern going for a large default row count. These examples
require `snowpipe-streaming` **1.8.0 or later**.

## Examples

| Path | Class | What it adds |
| --- | --- | --- |
| Elastic ingest | `ElasticChannelIngest` | The four append APIs. Pipelined single-row `appendRowWithWait` is the recommended default; `appendRows` is optional. |
| Elastic ingest (callbacks) | `ElasticChannelCallbacks` | Same tour with `appendRow` / `appendRows`. Handlers only enqueue; the ingest thread waits. |
| Elastic ingest (unbounded) | `ElasticChannelUnbounded` | Pipelined single-row appends at volume (10M rows by default). Interrupt drains accepted work and prints ack latency and rows/s. |

The `monitoring` directory contains separate monitoring and abort examples.

## Setup

### Requirements

- Java 11 or later
- Maven 3.6 or later
- A Snowflake account with RSA key-pair authentication
- A role allowed to insert into the target table

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

## Build And Run

```bash
mvn clean package

# ElasticChannelIngest is the default.
mvn exec:java
mvn exec:java -Dexec.mainClass=com.snowflake.example.ElasticChannelCallbacks
mvn exec:java -Dexec.mainClass=com.snowflake.example.ElasticChannelUnbounded
```

Set `SNOWFLAKE_TEST_ROWS` to change the generated row count in the unbounded
example (default 10,000,000). Interrupt the process to stop intake, wait for
appends already accepted by the SDK, print stats, and close.

## Semantics

An Elastic acknowledgement confirms that Snowflake durably accepted the
append. It does not confirm row validity or immediate table visibility. Check
the target table and its error table separately.

The SDK batches rows for transport. Waiting after every append is the slow
path. Pipelined `appendRowWithWait` — submit many rows, then wait on the
Futures — is the recommended default for throughput and simplicity.
`appendRows` / `appendRowsWithWait` are optional: one Future and one
append token for a logical group when you already have a batch, or to cut
call overhead. They do not replace SDK transport batching.
Fire-and-forget `appendRow` / `appendRows` return no Future; success and
error handlers are the only acknowledgement signal, and they echo the
caller-supplied append token. Those handlers run on the SDK acknowledgement
thread: enqueue onto a `BlockingQueue` with `offer` and return. Do not wait,
take locks the ingest thread also waits on, or call back into the SDK from a
handler. `offer` on an unbounded queue never blocks; a bounded `put` can
deadlock the ack thread. Count on the ingest thread after `poll`.

Average ack latency can look large next to rows/s. Many appends are in
flight, so throughput is not `1 / latency`.

Replaying an Elastic append can create a duplicate. Use stable source event IDs
and define downstream reconciliation for your application.

## Tests

The tests use SDK-shaped fake clients and do not connect to Snowflake:

```bash
mvn test
```
