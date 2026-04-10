# AGENTS.md

## Project Overview

Example repository demonstrating the **Snowpipe Streaming SDK** (v2) for real-time data ingestion into Snowflake. Contains minimal SDK examples (Java, Python) and advanced integration examples (Kafka, SQS) with real-world patterns.

This is a published reference repo — code should be clean, well-documented, and self-contained per sub-project.

## Repository Structure

```
java-example/              # Minimal Java example (Java 11+, SDK 1.0.1)
  pom.xml
  profile.json.example
  src/main/java/com/snowflake/example/
    StreamingIngestExample.java

python-example/            # Minimal Python example (Python 3.9+)
  requirements.txt
  profile.json.example
  streaming_ingest_example.py

custom-kafka-consumer/     # Kafka → Snowflake consumer (Java 17+, SDK 1.3.0)
  pom.xml
  consumer-config.properties
  producer-config.properties
  profile.json              # Placeholder (gitignored in practice)
  src/main/java/com/snowflake/streaming/
    consumer/               # Main, CustomKafkaConsumer, Config
    producer/               # FakeKafkaWriter (CDR generator), Config
  src/test/java/            # 15 unit tests

custom-sqs-consumer/       # SQS → Snowflake consumer (Java 17+, SDK 1.3.0)
  pom.xml
  consumer-config.properties
  producer-config.properties
  profile.json.example
  src/main/java/com/snowflake/streaming/
    consumer/               # Main, CustomSQSConsumer, Config
    producer/               # FakeSQSWriter (CDR generator), Config
  src/test/java/            # Unit tests

.github/CODEOWNERS         # Owned by @snowflakedb/streaming-ingest
```

## Key Concepts

- **Authentication**: RSA key-pair via `profile.json` (account, user, url, private_key path). Never committed — use `.example` templates.
- **SDK flow**: Create client → open channel → append rows → poll `getLatestCommittedOffsetToken` for commit confirmation → close resources.
- **CDR data model**: Both Kafka and SQS examples use the same 15-column call detail record schema for consistency.
- **Retry strategy** (advanced examples): 401/403 fail immediately, 409 reopens channel, 429/5xx exponential backoff with jitter.

## Build & Run

**Java example**: `cd java-example && mvn compile exec:java`
**Python example**: `cd python-example && pip install -r requirements.txt && python streaming_ingest_example.py`
**Kafka consumer**: `cd custom-kafka-consumer && mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.consumer.Main"`
**Kafka producer**: `cd custom-kafka-consumer && mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.producer.FakeKafkaWriter"`
**SQS consumer**: `cd custom-sqs-consumer && mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.consumer.Main"`
**SQS producer**: `cd custom-sqs-consumer && mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.producer.FakeSQSWriter"`

All require a valid `profile.json` in their respective directories.

## Code Ownership

All files require approval from `@snowflakedb/streaming-ingest` (see `.github/CODEOWNERS`).
