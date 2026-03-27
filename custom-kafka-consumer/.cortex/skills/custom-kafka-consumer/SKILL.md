---
name: custom-kafka-consumer
description: "Set up, configure, run, and debug a custom Kafka consumer that streams data into Snowflake via Snowpipe Streaming SDK v2. Use when: building a Kafka-to-Snowflake streaming pipeline, running the CDR demo, or troubleshooting the custom consumer. Triggers: kafka consumer, kafka snowflake, snowpipe streaming kafka, CDR demo, kafka to snowflake, custom consumer, streaming ingest kafka."
---

# Custom Kafka Consumer -- Snowpipe Streaming V2

## Architecture

```
FakeKafkaWriter (CDR generator)
        |  JSON messages
   Apache Kafka (topic, N partitions)
        |  poll()
CustomKafkaConsumer (1:1 partition-to-channel mapping)
        |  appendRow() per record
Snowpipe Streaming V2 (channel per partition)
        |
Snowflake Table (CALL_DETAIL_RECORDS)
```

Key design: each Kafka partition maps to exactly one Snowflake channel. A `ConsumerRebalanceListener` opens/closes channels on rebalance. Kafka offsets are committed only after Snowflake confirms persistence.

## Prerequisites

- Java 17+
- Maven 3.8+
- Homebrew (macOS) for local Kafka, or a remote Kafka broker
- Snowflake account with RSA key-pair auth configured
- `profile.json` with Snowflake connection details (url, account, user, role, private_key_file)

## Workflow

### Step 1: Start Local Kafka

**Goal:** Get a single-node Kafka broker running locally.

**Actions:**

1. **Check** if Kafka is installed:
   ```bash
   brew list kafka 2>/dev/null && echo "INSTALLED" || echo "NOT_INSTALLED"
   ```
   If `NOT_INSTALLED`, run `brew install kafka`.

2. **Check** if Kafka is already running:
   ```bash
   brew services list | grep kafka
   ```
   If status is `started`, skip to topic creation. Otherwise:
   ```bash
   brew services start kafka
   ```

3. **Create** the demo topic with 3 partitions:
   ```bash
   kafka-topics --create --topic test-topic --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
   ```

4. **Verify** the topic:
   ```bash
   kafka-topics --describe --topic test-topic --bootstrap-server localhost:9092
   ```

**Teardown (when done):**
```bash
brew services stop kafka
```

**STOP**: Verify `kafka-topics --list` shows `test-topic` before proceeding.

### Step 2: Set Up Snowflake Objects

**Goal:** Create the target database, schema, and table in Snowflake.

**Actions:**

1. **Execute** the following SQL in Snowflake:
   ```sql
   CREATE OR REPLACE DATABASE TEST_DATABASE;
   CREATE OR REPLACE SCHEMA TEST_SCHEMA;

   CREATE OR REPLACE TABLE CALL_DETAIL_RECORDS (
     record_id          NUMBER,
     caller_number      VARCHAR,
     callee_number      VARCHAR,
     call_type          VARCHAR,
     disposition        VARCHAR,
     call_start         TIMESTAMP_NTZ,
     call_end           TIMESTAMP_NTZ,
     duration_seconds   NUMBER,
     data_usage_mb      FLOAT,
     charge_amount      FLOAT,
     network_type       VARCHAR,
     cell_tower_id      VARCHAR,
     plan_type          VARCHAR,
     roaming            BOOLEAN,
     event_timestamp    TIMESTAMP_LTZ
   );
   ```

2. **Update** `profile.json` in the project root with Snowflake credentials:
   ```json
   {
     "user": "<SNOWFLAKE_USER>",
     "account": "<SNOWFLAKE_ACCOUNT>",
     "url": "<ACCOUNT>.snowflakecomputing.com",
     "private_key_file": "<PATH_TO_RSA_PRIVATE_KEY>",
     "role": "<ROLE>"
   }
   ```

**MANDATORY STOPPING POINT**: Confirm the table exists and `profile.json` is populated before proceeding.

### Step 3: Configure Properties

**Goal:** Set Kafka and Snowflake connection parameters in the config files.

**Ask** the user for:
- Snowflake database name (default: `TEST_DATABASE`)
- Snowflake schema name (default: `TEST_SCHEMA`)
- Kafka bootstrap servers (default: `localhost:9092`)
- Kafka topic name (default: `test-topic`)

**consumer-config.properties:**
```properties
kafka.bootstrap.servers=localhost:9092
kafka.topic=test-topic
kafka.group.id=test-group
kafka.poll.duration.ms=1000
snowflake.channel.name=TEST_CHANNEL
snowflake.database=TEST_DATABASE
snowflake.schema=TEST_SCHEMA
snowflake.table=CALL_DETAIL_RECORDS
snowflake.profile.path=profile.json
max.rows.per.append=100
consumer.thread.count=3
```

**producer-config.properties:**
```properties
kafka.bootstrap.servers=localhost:9092
kafka.topic=test-topic
producer.acks=all
producer.linger.ms=5
producer.batch.size=16384
default.burst.count=100
default.stream.rps=10
```

**STOP**: Confirm config values before writing files.

### Step 4: Build the Project

**Goal:** Compile the Java project with Maven.

```bash
mvn clean compile
```

**If error occurs:**
- `JAVA_HOME` not set or wrong version: Ensure Java 17+ is installed (`java -version`)
- Maven not found: Install via `brew install maven`
- Dependency resolution failure: Check network connectivity and Maven Central access

### Step 5: Run Producer and Consumer

**Goal:** Start the consumer and producer in separate terminals.

**Terminal 1 -- Start the consumer:**
```bash
mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.consumer.Main"
```

**Terminal 2 -- Start the producer:**
```bash
mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.producer.FakeKafkaWriter"
```

**Producer interactive commands:**

| Command | Description |
|---------|-------------|
| `single` | Send one CDR |
| `burst [count]` | Send a batch of CDRs (default 100) |
| `stream [rps]` | Continuous stream at N records/sec (default 10, Enter to stop) |
| `malformed` | Send 5 malformed JSON messages (tests error handling) |
| `nulls` | Send CDRs with null/missing fields |
| `custom [json]` | Send a custom JSON payload |
| `quit` | Exit the producer |

**Stop both with Ctrl+C.**

### Step 6: Verify Data in Snowflake

**Goal:** Confirm records arrived in the target table.

```sql
SELECT COUNT(*) FROM TEST_DATABASE.TEST_SCHEMA.CALL_DETAIL_RECORDS;
SELECT * FROM TEST_DATABASE.TEST_SCHEMA.CALL_DETAIL_RECORDS LIMIT 10;
```

## Key Consumer Patterns (Reference)

### SSv2 Client Initialization

```java
SnowflakeStreamingIngestClient sfClient = SnowflakeStreamingIngestClientFactory
    .builder("TEST_CLIENT", database, schema, table + "-STREAMING")
    .setProperties(sfProps)
    .build();
```

### Channel Open + Offset Recovery

Each partition gets its own channel named `<prefix>_P<partition>`:
```java
OpenChannelResult result = sfClient.openChannel(channelPrefix + "_P" + partition);
SnowflakeStreamingIngestChannel channel = result.getChannel();
String lastToken = channel.getLatestCommittedOffsetToken();
if (lastToken != null) {
    consumer.seek(tp, Long.parseLong(lastToken) + 1);
}
```

### Retry Strategy by HTTP Status

| HTTP Status | Action |
|-------------|--------|
| 401, 403 | Auth error -- fail immediately |
| 409 | Channel invalidated -- close + reopen channel, retry |
| 429, 5xx | Backpressure -- exponential backoff (500ms to 30s), unlimited retries |
| 408 | Timeout -- exponential backoff, max 10 attempts |

### Channel Health Check

Every 30 seconds, poll `channel.getChannelStatus()`. If status is not `SUCCESS`, proactively reopen the channel before the next append fails.

### Kafka Offset Commit

Kafka offsets are committed **only after** Snowflake confirms persistence:
```java
String committedToken = channel.getLatestCommittedOffsetToken();
// Only commit to Kafka if token has advanced since last commit
consumer.commitSync(offsets);
```

## Project Structure

```
custom-kafka-consumer/
├── consumer-config.properties          # Consumer: Kafka + Snowflake target config
├── producer-config.properties          # Producer: Kafka broker + tuning config
├── profile.json                        # Snowflake credentials (gitignored)
├── pom.xml                             # Maven build (snowpipe-streaming 1.3.0, kafka-clients 3.5.1)
└── src/
    ├── main/java/com/snowflake/streaming/
    │   ├── consumer/
    │   │   ├── Main.java               # Entry point, launches N consumer threads
    │   │   ├── CustomKafkaConsumer.java # Kafka -> Snowpipe Streaming consumer (1:1 partition-to-channel)
    │   │   └── Config.java             # Loads consumer-config.properties
    │   └── producer/
    │       ├── FakeKafkaWriter.java     # Interactive CLI CDR generator
    │       └── Config.java             # Loads producer-config.properties
    └── test/java/com/snowflake/streaming/consumer/
        └── CustomKafkaConsumerTest.java # Unit tests (Mockito)
```

## Troubleshooting

**Kafka connection refused:**
- Verify broker is running: `brew services list | grep kafka`
- Check bootstrap server address matches config

**Auth errors (HTTP 401/403):**
- Verify `profile.json` has correct account, user, role, and private_key_file path
- Ensure RSA key-pair is registered with the Snowflake user
- Check the role has INSERT privileges on the target table

**Channel invalidated (HTTP 409):**
- Table schema may have changed -- recreate the table
- Another consumer may be writing to the same channel -- ensure unique channel names
- The consumer automatically reopens channels on 409; check logs for repeated occurrences

**Maven build failures:**
- Wrong Java version: `java -version` must show 17+
- Dependencies not resolving: check network and `~/.m2/settings.xml` proxy config

**No data appearing in Snowflake:**
- Check consumer logs for errors
- Verify Kafka topic has messages: `kafka-console-consumer --topic test-topic --from-beginning --bootstrap-server localhost:9092 --max-messages 5`
- Confirm `consumer-config.properties` database/schema/table match the Snowflake objects

## Stopping Points

- After Step 1: Confirm Kafka is running and topic exists
- After Step 2: Confirm Snowflake table exists and profile.json is ready
- After Step 3: Confirm config values before writing files

## Output

A running demo: fake CDRs flow from Kafka into a Snowflake table via Snowpipe Streaming V2, demonstrating 1:1 partition-to-channel mapping, offset recovery, retry logic, and channel health checks.
