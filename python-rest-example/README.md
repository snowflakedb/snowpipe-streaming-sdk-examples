# Python Elastic Channels REST Example (Production)

This example demonstrates a production-grade way to stream data into an
[Elastic Channel](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-overview)
using the Snowpipe Streaming **REST API** directly, with no SDK dependency.

Most applications should prefer the SDK examples (
[Java](../java-example), [Python](../python-example), [Node.js](../nodejs-example))
for better throughput and simpler error handling. Use the REST API for
lightweight, language-agnostic, or infrastructure-constrained integrations.
For a minimal `curl` walkthrough of the same API, see the
[Elastic Channels REST getting-started tutorial](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-rest-getting-started).

## What this example includes

- **Key-pair JWT generation** and the ingest-host discovery / scoped-token
  exchange flow, with proactive refresh before expiry and reactive refresh
  on an unexpected `401`.
- **NDJSON batching** bounded by row count and byte size (staying under the
  1 MB Elastic payload limit), with gzip compression.
- **Bounded in-flight requests**: submitting rows blocks once too many
  batches are outstanding, instead of growing memory unbounded.
- **Retry with capped exponential backoff and full jitter** for `429`,
  `500`, `503`, and ambiguous network failures (timeouts, connection
  errors), honoring the `Retry-After` header when present.
- **Fail-fast** on permanent `400`, `403`, and `404` errors, and on `401`
  after one forced token refresh.
- **Stable event IDs and `requestId`/`retryCount` reuse** across retries of
  the same rowset, so support and downstream consumers can identify and
  reconcile possible duplicates from at-least-once delivery.
- **Graceful shutdown**: a `SIGINT`/`SIGTERM` handler stops intake, flushes
  the current batch, and waits (with a timeout) for in-flight requests
  before exiting.

## Prerequisites

- Python 3.9 or higher
- A Snowflake user configured for key-pair authentication
- A target database, schema, and table

```sql
ALTER USER MY_USER SET RSA_PUBLIC_KEY='<contents of rsa_key.pub, without header/footer>';

CREATE OR REPLACE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE (
    id VARCHAR,
    c1 NUMBER,
    c2 VARCHAR,
    ts TIMESTAMP_NTZ
);
```

No `CREATE PIPE` is required. The default table endpoint uses an
automatically created pipe named `MY_TABLE-STREAMING`.

## Setup

### 1. Generate an RSA key pair

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

### 2. Install dependencies

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure authentication and target objects

Copy `profile.json.example` to `profile.json` and fill in your values:

```json
{
  "account": "<account_identifier>",
  "user": "your_username",
  "private_key_file": "rsa_key.p8",
  "role": "your_role",
  "database": "MY_DATABASE",
  "schema": "MY_SCHEMA",
  "table": "MY_TABLE"
}
```

If your private key is encrypted, set `PRIVATE_KEY_PASSPHRASE` in the
environment before running the example.

## Run

```bash
python elastic_production.py
```

Send `SIGINT` (Ctrl+C) or `SIGTERM` at any point to trigger a graceful
shutdown: the example stops submitting new rows, flushes the current
batch, and waits for in-flight requests to complete before exiting.

## Using the pipe endpoint instead of the default table endpoint

To stream into a custom pipe (for example, one with in-flight
transformations or an explicit `ON_ERROR` setting) instead of the default
table endpoint, set `PIPE` near the top of `elastic_production.py`:

```python
PIPE = "MY_TABLE-STREAMING"  # or any custom pipe name
```

## Tests

Narrow unit tests cover batching, backoff/jitter math, `Retry-After`
parsing, JWT claim construction, and endpoint URL selection without making
any network calls. The tests import the same third-party libraries as the
example (`requests`, `PyJWT`, `cryptography`), so install the requirements
before running them:

```bash
pip install -r requirements.txt
python -m unittest test_elastic_production.py -v
```

## Troubleshooting

- **HTTP 401 (Unauthorized)**: The example forces one scoped-token refresh
  automatically. If it still fails, verify your key-pair configuration and
  that the public key is registered on the Snowflake user.
- **HTTP 404 (Not Found)**: Verify the database, schema, and table (or
  pipe) names and that they exist in your account.
- **HTTP 429 (Too Many Requests)**: The example retries automatically with
  backoff and jitter. Sustained 429s indicate you should reduce request
  rate or increase batch size.
- **Duplicate rows**: Elastic Channels provide at-least-once delivery.
  Reconcile using the stable `id` field included in every row payload.
- **No rows visible after acknowledgement**: A successful response is a
  durable acknowledgement, not immediate queryability. Allow a few seconds,
  then check `MY_DATABASE.MY_SCHEMA.MY_TABLE__ERRORS` for row-level errors.

## Additional resources

- [Elastic Channels overview](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-overview)
- [Elastic Channels REST getting-started tutorial](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-rest-getting-started)
- [Elastic Channels REST API reference](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-rest-api)
- [Elastic Channels best practices](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-best-practices)
