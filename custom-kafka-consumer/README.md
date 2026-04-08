# Custom Kafka Consumer — Snowpipe Streaming V2

A sample Kafka-to-Snowflake streaming consumer using the Snowpipe Streaming SDK v2. Demonstrates 1:1 partition-to-channel mapping, offset tracking, retry logic, and resume-without-data-loss semantics.

## End-to-End Walkthrough

This example includes a complete end-to-end demo that simulates a telecom cell tower monitoring pipeline:

1. **Streaming Ingestion** — A fake producer generates Call Detail Records (CDRs) into Kafka topics. The custom consumer ingests these records into a Snowflake table using the Snowpipe Streaming High Performance Architecture SDK, with partition-level offset tracking and exactly-once semantics.

2. **ML Prediction** — Once data is flowing, three `SNOWFLAKE.ML.FORECAST` models are trained directly in Snowflake (pure SQL, no Python) to predict tower activity over the next 7 days:
   - **Call drop rate** — which towers are likely to have the highest failure rates
   - **Call volume** — expected daily call load per tower for capacity planning
   - **Data usage** — forecasted bandwidth consumption per tower

3. **Semantic View & Natural Language Queries** — A Snowflake Semantic View is created over the live CDR data and forecast results, enabling natural language queries through Cortex Analyst such as:
   - *"What cell towers will have the highest call drop rate in the next 7 days?"*
   - *"Which towers are critical and need maintenance?"*
   - *"What is the forecasted data usage per tower?"*

To try the full walkthrough, see [SKILL.md](.cortex/skills/custom-kafka-consumer/SKILL.md).

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

Each Kafka partition maps to exactly one Snowflake channel. A `ConsumerRebalanceListener` opens/closes channels on rebalance. Kafka offsets are committed only after Snowflake confirms persistence.

## Key Features

- **Multi-threaded ingestion** — configurable number of consumer threads via `consumer.thread.count`
- **Retry and error handling** — exponential backoff with jitter for 429/5xx, channel reopening on 409, fail-fast on 401/403
- **Smart offset commits** — only commits to Kafka when the Snowflake offset token has advanced
- **Channel health checks** — proactively reopens unhealthy channels every 30 seconds
- **Test producer** — `FakeKafkaWriter` generates synthetic CDR records with interactive commands (single, burst, stream, malformed, etc.)

## Prerequisites

- Java 17+
- Maven 3.8+
- Homebrew (macOS) for local Kafka, or a remote Kafka broker
- Snowflake account with RSA key-pair auth configured

## Quick Start

```bash
# Install and start Kafka
brew install kafka
brew services start kafka

# Create test topic
kafka-topics --create --topic test-topic --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092

# Build
mvn clean compile

# Terminal 1 — Start the consumer
mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.consumer.Main"

# Terminal 2 — Start the producer
mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.producer.FakeKafkaWriter"
```

## Project Structure

```
custom-kafka-consumer/
├── consumer-config.properties          # Consumer: Kafka + Snowflake target config
├── producer-config.properties          # Producer: Kafka broker + tuning config
├── profile.json                        # Snowflake credentials (gitignored)
├── pom.xml
└── src/
    ├── main/java/com/snowflake/streaming/
    │   ├── consumer/
    │   │   ├── Main.java               # Entry point, launches N consumer threads
    │   │   ├── CustomKafkaConsumer.java # Kafka → Snowpipe Streaming consumer
    │   │   └── Config.java             # Loads consumer-config.properties
    │   └── producer/
    │       ├── FakeKafkaWriter.java     # Interactive CLI CDR generator
    │       └── Config.java             # Loads producer-config.properties
    └── test/java/com/snowflake/streaming/consumer/
        └── CustomKafkaConsumerTest.java # Unit tests (Mockito)
```
