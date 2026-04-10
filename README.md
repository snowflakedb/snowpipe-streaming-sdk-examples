# Snowpipe Streaming SDK Example

This repository contains examples demonstrating how to use the Snowpipe Streaming SDK to ingest data into Snowflake in real-time.

## Overview

The Snowpipe Streaming SDK enables applications to stream data directly into Snowflake tables with low latency and high throughput. This repository provides practical examples to help you get started with the SDK quickly.

## Examples

This repository contains complete, runnable examples in multiple languages:

### [Java Example](./java-example)
A complete Maven project demonstrating the Snowpipe Streaming SDK in Java. Includes:
- Maven build configuration with all required dependencies
- Full example code with proper error handling
- Comprehensive setup instructions
- Sample configuration files

### [Python Example](./python-example)
A complete Python project demonstrating the Snowpipe Streaming SDK in Python. Includes:
- Requirements file with all necessary packages
- Clean, well-documented example code
- Setup instructions with virtual environment
- Sample configuration files

### [Custom Kafka Consumer](./custom-kafka-consumer)
A production-style Java consumer that reads from an Apache Kafka topic and streams records into Snowflake. Demonstrates partition-aware channel management, manual offset commit after Snowflake confirms ingestion, and consumer group rebalance handling.

### [Custom Pub/Sub Consumer](./custom-pubsub-consumer)
A production-style Java consumer that reads from a GCP Pub/Sub subscription and streams records into Snowflake. Demonstrates synchronous pull, batch acknowledgement only after Snowflake confirms ingestion, and per-thread channel management.

### [Custom SQS Consumer](./custom-sqs-consumer)
A production-style Java consumer that reads from an Amazon SQS queue and streams records into Snowflake. Demonstrates long-polling, batch accumulation across multiple receive calls, and delete-after-confirm for at-least-once delivery.

## Getting Started

1. Choose an example that matches your message broker or use case
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
