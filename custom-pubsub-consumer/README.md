# Sample Pub/Sub Consumer

Standalone GCP Pub/Sub → Snowflake streaming consumer that demonstrates the Snowpipe Streaming SDK v2. Pulls messages from a Pub/Sub subscription and ingests them into Snowflake in real-time with at-least-once delivery guarantees.

## Prerequisites

- Java 17+
- Maven
- GCP project with Pub/Sub enabled
- `gcloud` CLI authenticated (`gcloud auth application-default login`)
- Snowflake account

## GCP Pub/Sub Setup

If you already have a Pub/Sub topic and subscription, skip this step and update `consumer-config.properties` with your existing values.

To create a test topic/subscription using the gcloud CLI:

```bash
# Create topic
gcloud pubsub topics create ssv2-example-topic --project=YOUR_PROJECT_ID

# Create subscription
gcloud pubsub subscriptions create ssv2-example-sub \
  --topic=ssv2-example-topic \
  --ack-deadline=60 \
  --project=YOUR_PROJECT_ID

# Verify
gcloud pubsub topics list --project=YOUR_PROJECT_ID
gcloud pubsub subscriptions list --project=YOUR_PROJECT_ID
```

To tear down when done:

```bash
gcloud pubsub subscriptions delete ssv2-example-sub --project=YOUR_PROJECT_ID
gcloud pubsub topics delete ssv2-example-topic --project=YOUR_PROJECT_ID
```

## Snowflake Setup

Log into Snowflake and create the target objects. Database, schema, and table names should match `consumer-config.properties`:

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

## Configuration

1. Copy `profile.json.example` to `profile.json` and fill in your Snowflake connection details
2. Update `consumer-config.properties` with your GCP project ID and Pub/Sub subscription name
3. Update `producer-config.properties` with your GCP project ID and Pub/Sub topic name

## Running

```bash
# Produce test records (interactive CLI)
mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.producer.FakePubSubWriter"

# Run the consumer (in a separate terminal)
mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.consumer.Main"

# Stop with Ctrl+C
```

## Verify

```sql
SELECT COUNT(*) FROM TEST_DATABASE.TEST_SCHEMA.CALL_DETAIL_RECORDS;
SELECT * FROM TEST_DATABASE.TEST_SCHEMA.CALL_DETAIL_RECORDS LIMIT 10;
```

## Project Structure

```
├── consumer-config.properties                 # Consumer config (Pub/Sub subscription + Snowflake target)
├── producer-config.properties                 # Producer config (Pub/Sub topic + generator tuning)
├── profile.json.example                       # Snowflake credentials template
├── pom.xml
└── src/
    ├── main/java/com/snowflake/streaming/
    │   ├── consumer/
    │   │   ├── Main.java                # Entry point, launches consumer threads
    │   │   ├── CustomPubSubConsumer.java # Pub/Sub → Snowpipe Streaming consumer
    │   │   └── Config.java              # Config loader
    │   └── producer/
    │       ├── FakePubSubWriter.java    # Test producer, writes CDR records to Pub/Sub
    │       └── Config.java              # Producer config loader
    └── test/java/com/snowflake/streaming/consumer/
        └── CustomPubSubConsumerTest.java  # Unit tests
```

## Key Design Decisions

- **Synchronous pull** — messages are pulled in batches and only acknowledged after Snowflake confirms ingestion
- **One channel per thread** — each consumer thread operates its own Snowflake channel (Pub/Sub doesn't expose partition assignments to consumers)
- **Same CDR data model** — uses the same 15-column call detail record schema as the Kafka example for consistency
- **Same retry strategy** — 401/403 fail immediately, 409 reopens channel, 429/5xx exponential backoff
