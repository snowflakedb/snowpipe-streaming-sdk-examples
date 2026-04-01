# Snowpipe Streaming SDK Examples

This repository contains examples demonstrating how to use the Snowpipe Streaming SDK to ingest data into Snowflake in real-time.

## Overview

The Snowpipe Streaming SDK enables applications to stream data directly into Snowflake tables with low latency and high throughput. This repository provides practical examples to help you get started with the SDK quickly.

## Examples

This repository contains complete, runnable examples in multiple languages and integration patterns:

### [Java Example](./java-example)
A minimal Maven project demonstrating the Snowpipe Streaming SDK in Java. Streams 100K rows into a Snowflake table. Good starting point for understanding the SDK basics.

### [Python Example](./python-example)
A minimal Python project demonstrating the Snowpipe Streaming SDK in Python. Same workflow as the Java example — streams 100K rows into a Snowflake table.

### [Custom Kafka Consumer](./custom-kafka-consumer)
A standalone Kafka → Snowflake streaming consumer. Demonstrates partition-to-channel mapping, offset recovery, exactly-once delivery semantics, retry logic, and multi-threaded consumption. Includes a CDR data generator for testing.

### [Custom Pub/Sub Consumer](./custom-pubsub-consumer)
A standalone GCP Pub/Sub → Snowflake streaming consumer. Demonstrates synchronous pull-based ingestion with ack-after-confirm semantics, retry logic, and channel health monitoring. Uses the same CDR data model as the Kafka example. Includes a CDR data generator for testing.

## Getting Started

1. Choose your preferred example
2. Navigate to the respective example directory
3. Follow the README instructions in that directory to:
   - Set up your Snowflake table
   - Configure authentication
   - Install dependencies
   - Run the example

## Important Notes

**Dependencies**: The dependency versions used in the examples are for demonstration purposes only. These should be updated appropriately based on the SDK version you are using.

## License

This project is licensed under the CC BY 4.0 license.
