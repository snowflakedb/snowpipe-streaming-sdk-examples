# Custom SQS Consumer

Standalone Amazon SQS → Snowflake streaming consumer that demonstrates the Snowpipe Streaming SDK v2. Long-polls messages from an SQS queue and ingests them into Snowflake in real-time with at-least-once delivery guarantees.

## Prerequisites

- Java 17+
- Maven
- AWS account with an SQS queue
- AWS CLI configured (`aws configure` or environment variables)
- Snowflake account

## AWS SQS Setup

If you already have an SQS queue, skip this step and update `consumer-config.properties` with your existing queue URL.

To create a test queue using the AWS CLI:

```
# Create queue
aws sqs create-queue --queue-name ssv2-example-queue --region us-east-1

# Get the queue URL (needed for config)
aws sqs get-queue-url --queue-name ssv2-example-queue --region us-east-1

# Verify
aws sqs list-queues --region us-east-1
```

To tear down when done:

```
aws sqs delete-queue \
  --queue-url https://sqs.us-east-1.amazonaws.com/YOUR_ACCOUNT_ID/ssv2-example-queue
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
2. Update `consumer-config.properties` with your AWS region and SQS queue URL
3. Update `producer-config.properties` with your AWS region and SQS queue URL

## Running

```
# Produce test records (interactive CLI)
mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.producer.FakeSQSWriter"

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
consumer-config.properties                 # Consumer config (SQS queue + Snowflake target)
producer-config.properties                 # Producer config (SQS queue + generator tuning)
profile.json.example                       # Snowflake credentials template
pom.xml
src/
  main/java/com/snowflake/streaming/
    consumer/
      Main.java                            # Entry point, launches consumer threads
      CustomSQSConsumer.java               # SQS -> Snowpipe Streaming consumer
      Config.java                          # Config loader
    producer/
      FakeSQSWriter.java                   # Test producer, writes CDR records to SQS
      Config.java                          # Producer config loader
  test/java/com/snowflake/streaming/consumer/
    CustomSQSConsumerTest.java             # Unit tests
```

## Key Design Decisions

- **Long-polling** — `WaitTimeSeconds=20` reduces empty API calls; each poll blocks up to 20 seconds waiting for messages
- **Batch accumulation** — the consumer accumulates messages across multiple receive calls (SQS max is 10 per call) until `max.rows.per.append` is reached before flushing to Snowflake
- **Delete after confirm** — messages are deleted from SQS only after all `appendRow` calls in the batch succeed, ensuring at-least-once delivery
- **Shutdown interrupt** — `shutdown()` closes the `SqsClient` immediately, breaking any in-flight long-poll (mirrors Kafka's `consumer.wakeup()`)
- **One channel per thread** — each consumer thread operates its own Snowflake channel (SQS Standard queues have no partition concept)
- **Same CDR data model** — uses the same 15-column call detail record schema as the Kafka and Pub/Sub examples for consistency
- **Same retry strategy** — 401/403 fail immediately, 409 reopens channel, 429/5xx exponential backoff (500ms → 30s)
