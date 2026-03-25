# Sample Kafka Consumer

Sample standalone consumer that demonstrates Kafka-to-Snowflake streaming consumer code using the Snowpipe Streaming SDK v2. Demonstrates how to map multiple partitions in a Kafka topic to a channel, tracking offsets and resuming for no data loss

## Prerequisites

- Java 17+
- Maven
- Snowflake account with a streaming pipe

## Kafka Local Setup (macOS with Homebrew). Customize these based on your setup

```bash
# Install Kafka
brew install kafka

# Start Kafka
brew services start kafka

# Create a test topic with 3 partitions
kafka-topics --create --topic test-topic --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092

# Verify the topic was created
kafka-topics --describe --topic test-topic --bootstrap-server localhost:9092
```

To stop Kafka:

```bash
brew services stop kafka
```

## Snowflake Setup - Log into Snowflake to setup following entities. Database, Schema and Table name should match consumer-config.properties entries

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

1. `config.properties` is already setup assuming Kafka is running on port 9092. If you changed above how Kafka is locally running update `config.properties`
2. Update `profile.json` in the project root directory with your Snowflake connection details



## Running

```bash


# Produce test records (runs FakeKafkaWriter at 1 TPS continuously)
mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.producer.FakeKafkaWriter"

# Run the consumer
mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.consumer.Main"

# Stop with Ctrl+C
```

## Verify

```sql
SELECT COUNT(*) FROM TEST_DB.PUBLIC.TEST_TABLE;
SELECT * FROM TEST_DB.PUBLIC.TEST_TABLE LIMIT 10;
```

## Project Structure

```
├── consumer-config.properties                 # Consumer runtime config to read from Kafka and write to Snowpipe Streaming
├── producer-config.properties                 # Producer runtime config to write to Kafka
├── profile.json                               # Snowflake credentials (gitignored)
├── pom.xml
└── src/
    ├── main/java/com/snowflake/streaming/
    │   ├── consumer/
    │   │   ├── Main.java              # Entry point, launches N consumer threads
    │   │   ├── CustomKafkaConsumer.java # Kafka → Snowpipe Streaming consumer
    │   │   └── Config.java            # Config loader
    │   └── producer/
    │       └── FakeKafkaWriter.java   # Test producer, writes CDR records to Kafka
    └── test/java/com/snowflake/streaming/consumer/
        └── CustomKafkaConsumerTest.java  # Unit tests
```
