# Java Snowpipe Streaming SDK Examples

Examples demonstrating how to use the Snowflake Streaming Ingest SDK in Java to ingest data into Snowflake using the [high-performance architecture](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview).

SDK requirement: `snowpipe-streaming` **1.8.0** or later.


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


## Examples

| File | Description |
|---|---|
| `ElasticQuickstart.java` | **Start here.** One waitable Elastic append, channel status, and client close. |
| `ElasticProducer.java` | Production Elastic Channel producer: individual row appends, bounded durability checkpoints, retry, and client recovery. |
| `NamedChannelCheckpoint.java` | Single-writer named channel: append immediately, poll committed offsets at checkpoints, reopen and seek on invalidation. |
| `StreamingIngestExample.java` | Original named-channel example (retained for reference). |

**Channel mode guidance:**
- Use **Elastic Channels** for most new applications. They scale automatically across concurrent producers and require no offset management.
- Use **named channels** when you need ordered, strictly-exactly-once delivery or source-offset integration (for example, Kafka partition offset tracking).

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
- A Snowflake account with appropriate permissions
- RSA key-pair authentication configured

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

### 2. Create a Target Table

```sql
CREATE OR REPLACE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE (
    DATA VARIANT,
    c1 NUMBER,
    c2 VARCHAR,
    ts  TIMESTAMP_NTZ
);
```

No `CREATE PIPE` is required. The high-performance architecture automatically creates a default pipe named `MY_TABLE-STREAMING` when you first open a channel or call `getElasticChannel`.

Both production examples also expect an `event_id` column for replay identification:

```sql
ALTER TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE ADD COLUMN event_id NUMBER;
```

### 3. Configure Authentication

Copy `profile.json.example` to `profile.json` in the `java-example` directory and fill in your credentials:

```json
{
  "account": "<account_identifier>",
  "user": "your_username",
  "url": "https://<account_identifier>.snowflakecomputing.com:443",
  "private_key_file": "rsa_key.p8",
  "role": "your_role"
}
```

### 4. Update Object Names

Edit the `DATABASE`, `SCHEMA`, and `TABLE` defaults, or set the matching `SNOWFLAKE_*` environment variables.

Each production program includes its own configuration, retry timing, and replay fixture.
No separate production helper library is required. PAT mode reads `SNOWFLAKE_PAT` from the environment and requires explicit
`SNOWFLAKE_ACCOUNT` and `SNOWFLAKE_URL`; `SNOWFLAKE_ROLE` is optional. Otherwise they use `profile.json`
(or `SNOWFLAKE_PROFILE`). No PM account or administrative role is selected by default.

## Production behavior

See the [shared retention contract](../README.md#production-retention-contract).
Each event is submitted before the next source read. Periodic progress collection does not wait
for all submitted events. SDK backpressure and the pending-event safety limit pause intake.
Original Futures remain tracked; only SDK invalidation triggers client/channel recovery.
`SampleEventSource` is regenerable test data with an in-memory checkpoint, not a disk spool.

Configure `SNOWFLAKE_TEST_ROWS` for sample size and `SNOWFLAKE_CHANNEL` for a dedicated named-channel
identity. A repeat named run resumes without resending committed records. Elastic replay uses
`SNOWFLAKE_SOURCE_CHECKPOINT` if supplied; duplicates remain possible.

## Build

```bash
mvn clean package
```

## Run

**Elastic Channel quickstart** (default):

```bash
mvn exec:java
```

**Specific example:**

```bash
mvn exec:java -Dexec.mainClass="com.snowflake.example.ElasticProducer"
mvn exec:java -Dexec.mainClass="com.snowflake.example.NamedChannelCheckpoint"
```

## Test

Unit tests verify checkpointing, error classification, recovery, and offset arithmetic without a live Snowflake connection:

```bash
mvn test
```

## Additional Resources

- [Elastic Channels overview](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview)
- [Elastic Channels getting started](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-getting-started)
- [Best practices](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-best-practices)
- [Snowpipe Streaming SDK on Maven Central](https://repo1.maven.org/maven2/com/snowflake/snowpipe-streaming/)
