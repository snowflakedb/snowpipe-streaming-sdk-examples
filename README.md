# Snowpipe Streaming SDK Example

This repository contains examples demonstrating how to use the Snowpipe Streaming SDK to ingest data into Snowflake in real-time.

## Overview

The Snowpipe Streaming SDK enables applications to stream data directly into Snowflake tables with low latency and high throughput. This repository provides practical examples to help you get started with the SDK quickly.

The language examples use **Elastic Channels**: one implicit, Snowflake-managed
channel per pipe, with concurrent producers and durable acknowledgements.

## Choosing SDK vs. REST

- **SDK (Java, Python, Node.js)** — recommended for most applications. Higher throughput and simpler error handling than calling the REST API directly.
- **REST API** — use for lightweight, language-agnostic, or infrastructure-constrained integrations where adding the SDK isn't practical.

## Examples

This repository contains complete, runnable examples in multiple languages:

### [Java Example](./java-example)
A complete Maven project demonstrating the Snowpipe Streaming SDK in Java. Includes:
- An append-API tour (Futures and callbacks) plus a high-volume unbounded ingest example
- Maven build configuration with all required dependencies
- Full example code with proper error handling
- Comprehensive setup instructions
- Sample configuration files
- **[Monitoring & Abort](./java-example/monitoring)** — Monitor channel status, track offset lag, inject errors, and abort on error increase

### [Python Example](./python-example)
A complete Python project demonstrating the Snowpipe Streaming SDK in Python. Includes:
- An append-API tour (Futures and callbacks) plus a high-volume unbounded ingest example
- Requirements file with all necessary packages
- Clean, well-documented example code
- Setup instructions with virtual environment
- Sample configuration files
- **[Monitoring & Abort](./python-example/monitoring)** — Monitor channel status, track offset lag, inject errors, abort on error increase, and optional live matplotlib plotting

### [Node.js Example](./nodejs-example)
A complete Node.js project demonstrating the Snowpipe Streaming SDK in Node.js. Includes:
- An append-API tour (Promises and callbacks) plus a high-volume unbounded ingest example
- npm package configuration with all required dependencies
- Clean, well-documented example code
- Setup instructions
- Sample configuration files

### [Python REST Example](./python-rest-example)
A production-grade example that streams into an Elastic Channel using the Snowpipe Streaming REST API directly, with no SDK dependency. Includes:
- Key-pair JWT generation, ingest-host discovery, and scoped-token exchange with refresh
- Bounded, batched NDJSON append requests with gzip compression
- Retry with capped exponential backoff and full jitter, honoring `Retry-After`
- Stable event IDs and `requestId`/`retryCount` reuse for duplicate reconciliation
- Graceful shutdown and narrow unit tests

## Getting Started

1. Choose your preferred language (Java, Python, or Node.js), or the REST example if you don't want an SDK dependency
2. Navigate to the respective example directory
3. Follow the README instructions in that directory to:
   - Set up your Snowflake table and pipe
   - Configure authentication
   - Install dependencies
   - Run the example

## Important Notes

**SDK version**: All examples require `snowpipe-streaming` **1.8.0** or later. The version numbers in the dependency files (`pom.xml`, `package.json`, `requirements.txt`) reflect the minimum tested version. Pin to the latest published SDK version in production deployments.

## License

This project is licensed under the CC BY 4.0 license.
