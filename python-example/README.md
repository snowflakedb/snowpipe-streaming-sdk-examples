# Python Snowpipe Streaming SDK Examples

Examples for ingesting data into Snowflake with the [Snowpipe Streaming](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview) Python SDK.


## Adapt This Example to Your Application

1. **Run the sample unchanged first.** Set your target database/schema/table and authentication profile. The production examples generate synthetic rows with `EVENT_ID NUMBER`, `C1 NUMBER`, and `C2 VARCHAR`; they do not read a broker, file, or API. Set `SNOWFLAKE_TEST_ROWS=1005` to cross the progress-check cadence and exercise final drain. Successful output reports source checkpoint `1005`.
2. **Start reading at `main`, then `run`.** The loop reads a retained event, submits it to the SDK, collects available delivery progress, and acknowledges only confirmed source events. Connection and recovery details appear below that flow in the same file.
3. **Replace `SampleEventSource`.** Replace `read` with your source operation and change the sample row mapping to match your table. Reading must not delete or permanently acknowledge an event. End-of-input (`None`/`null`) stops the example; a temporarily idle live source must instead wait or poll with a bounded, interruptible read.
4. **Implement durable source progress.** Replace `acknowledge` with your source commit/checkpoint operation. The sample stores progress only in memory. Elastic requires retained/replayable events and stable source-unique IDs for duplicate reconciliation. Named channels additionally require `seek` strictly after the server's committed offset and one exclusive owner per stable channel name.
5. **Choose outage and shutdown behavior.** Pausing reads must propagate backpressure to the producer. A push source needs explicit flow control. If events cannot be replayed, persist them before accepting responsibility; the SDK memory buffer is not a disk spool. On shutdown, stop intake and confirm pending progress within your budget; retain anything unconfirmed for restart.
6. **Verify delivery and table results separately.** Elastic acknowledgement confirms durability, not row validity or immediate query visibility. Monitor materialization/error logging separately. Named examples block source handoff on row errors. Do not simply retry schema or authorization failures indefinitely.

### Before Production

- Size the 100,000-pending-event application safety limit for your payloads: an event-count limit is not a byte-memory limit. Validate row sizes and account for the SDK buffer plus retained source data.
- The periodic progress check runs between source reads, not on an independent timer. Integrate bounded reads and cancellation for live sources.
- The 30-minute stalled-progress budget does not cancel SDK management calls or their independent transport retries.
- Test restart, source checkpoint failure, invalidation, and sustained backpressure with your real source. Define storage capacity and overflow behavior before accepting unreplayable events.
- Keep credentials in a secure credential manager and choose a role with only the required privileges. Kafka is not required solely to deliver events to Snowflake.


## Which example should I use?

**Start with Elastic Channels.** They are the default starting point for a new streaming application: one channel per client, Snowflake manages the fan-out, and you never name, open, or close a channel yourself.

| Example | Use it for |
| --- | --- |
| [`elastic_quickstart.py`](./elastic_quickstart.py) | Your first Elastic Channel. Append one row, wait for durability, inspect status, close. |
| [`elastic_production_example.py`](./elastic_production_example.py) | Running Elastic Channels in production: individual row appends, bounded durability checkpoints, retry, recovery, and reconciliation. |
| [`named_channel_checkpoint_example.py`](./named_channel_checkpoint_example.py) | Ordered, strict exactly-once ingestion driven by a replayable source offset (Kafka partition, CDC log sequence number, file offset). |
| [`streaming_ingest_example.py`](./streaming_ingest_example.py) | The original single-file named-channel walkthrough. |
| [`monitoring/`](./monitoring) | Monitoring channel status, tracking offset lag, and aborting on error increase. |

Choose a named channel over an Elastic Channel when you need Snowflake to track a source offset for you so a restart resumes exactly where the last run committed. Otherwise prefer Elastic.

## Prerequisites

- Python 3.9 or higher
- pip
- A Snowflake account with appropriate permissions
- RSA key-pair authentication configured
- `snowpipe-streaming` **1.8.0 or later** for the GA acknowledgement API used here

## Setup

### 1. Generate RSA Key Pair

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Register the public key with your Snowflake user:

```sql
ALTER USER MY_USER SET RSA_PUBLIC_KEY='<contents of rsa_key.pub, without header/footer>';
```

### 2. Create a Snowflake Table

```sql
CREATE OR REPLACE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE (
    DATA VARIANT,
    EVENT_ID NUMBER,
    c1 NUMBER,
    c2 VARCHAR,
    ts TIMESTAMP_NTZ
);
```

No `CREATE PIPE` is needed. Snowflake automatically creates a **default pipe** named `MY_TABLE-STREAMING` when you first use the table for streaming.

### 3. Install Dependencies

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 4. Configure Authentication

Create a `profile.json` file in this directory using `profile.json.example` as a template:

```json
{
  "account": "<account_identifier>",
  "user": "your_username",
  "url": "https://<account_identifier>.snowflakecomputing.com:443",
  "private_key_file": "rsa_key.p8",
  "role": "your_role"
}
```

**Note:** Use `private_key_file` to reference the key file path. For production, use a secure credential manager.

### 5. Update Configuration

Edit the object-name defaults or set the matching `SNOWFLAKE_*` environment variables. The pipe name is derived as `<TABLE>-STREAMING`.

## Run

```bash
python elastic_quickstart.py
python elastic_production_example.py
python named_channel_checkpoint_example.py
```

## Elastic Channels

### Quickstart

`elastic_quickstart.py` is the minimal path:

1. `StreamingIngestClient.from_table(...)` creates a table-mode client against the default pipe.
2. `client.get_elastic_channel()` returns the channel. It is a **cached singleton per client**: calling it again hands back the same channel.
3. `channel.append_row_with_wait(row, append_token)` returns a `Future` that completes on durable acknowledgement.
4. `client.close(wait_for_flush=True, timeout_seconds=60)` flushes and tears down. Elastic Channels have no `close()` of their own; their lifecycle is tied to the client.

The second argument to every append is an **append token**: an opaque id you choose, echoed back to you on success or failure so you can tie an outcome to your own data. Tokens stay in SDK memory until acknowledgement, so keep them small.

### Append variants

| Call | Returns | Use when |
| --- | --- | --- |
| `append_row` / `append_rows` | nothing | Fire-and-forget. The success and error handlers are your **only** signal. |
| `append_row_with_wait` / `append_rows_with_wait` | `Future` | You need to know a specific batch became durable. This is the correctness path. |

### Production concerns

See the [shared retention contract](../README.md#production-retention-contract).
`elastic_production_example.py` submits each event immediately with `append_row_with_wait`, then
collects completed original Futures without waiting for an entire window. It does not batch payloads
or wait for each row. Intake pauses on SDK backpressure or the application pending-event limit. SDK
invalidation recreates the client; a generation check prevents old failures from rebuilding it again.
Only terminal retryable SDK errors are resubmitted, with explicit duplicate risk.

Each example includes its configuration, capped jittered backoff, and deterministic
`SampleEventSource` in the same file. Its checkpoint is in-memory: replace it with the producer's retained source APIs.
PAT mode requires `SNOWFLAKE_PAT`, `SNOWFLAKE_ACCOUNT`, and `SNOWFLAKE_URL`; the optional
`SNOWFLAKE_ROLE` has no administrative default. Otherwise `profile.json` or `SNOWFLAKE_PROFILE` is used.

## Named channels

`named_channel_checkpoint_example.py` shows the asynchronous checkpoint pattern:

1. Open a stable, exclusively owned channel without replacing its server offset, then seek the source after the returned committed offset.
2. Append individual rows with source-offset tokens; SDK buffering handles transport batching.
3. Periodically fetch committed status once and check row errors before handing off confirmed progress. Keep appending between polls; drain at end of input.
4. Retry local backpressure on the same event. On SDK invalidation, reopen and seek again; client invalidation requires a new client.

Offset tokens are opaque to Snowflake; this fixture encodes numeric offsets as strings and compares
them numerically. No lexical ordering assumption or server-side deduplication by arbitrary token value
is made. The source adapter is responsible for replay positioning.

## Tests

The tests drive the actual loops with SDK-shaped failures: immediate submission, paused intake,
late success, shared outage deadlines, partial acknowledgements, backpressure, invalidation, and restart seek.

```bash
pip install -r requirements.txt pytest
python -m pytest tests -v
```

## Logging

```bash
export SS_LOG_LEVEL=info    # More detailed logs
export SS_LOG_LEVEL=debug   # Debug logs
```

The examples default to `warn` to reduce output noise.

## Troubleshooting

- **Connection Issues**: Verify your `profile.json` credentials and network connectivity to Snowflake
- **Permission Errors**: Ensure your role has the necessary privileges on the database, schema, and table
- **Table Not Found**: Verify the table exists; the default pipe is created automatically
- **`get_elastic_channel()` returns a closed channel**: The client is invalidated. Create a new client — re-getting the channel on the same client returns the same channel
- **Appends failing with 429**: You are appending faster than Snowflake is acknowledging. Lower your in-flight bound and back off; see `elastic_production_example.py`
- **`wait_for_commit` never returns**: Your predicate is probably an equality check. Snowflake commits in batches, so use `>=`
- **VARIANT Columns**: Pass data as a Python `dict`, not a JSON string
- **Import Errors**: Install dependencies with `pip install -r requirements.txt`, and confirm `snowpipe-streaming>=1.8.0`

## Additional Resources

- [Elastic Channels overview](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-overview)
- [Elastic Channels getting started](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-getting-started)
- [Elastic Channels best practices](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-best-practices)
- [Elastic Channels error handling](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-error-handling)
- [High-Performance Streaming Overview](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview)
- [Snowpipe Streaming SDK on PyPI](https://pypi.org/project/snowpipe-streaming/)
