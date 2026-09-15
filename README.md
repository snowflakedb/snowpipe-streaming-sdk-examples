# Snowpipe Streaming SDK Example

This repository contains examples demonstrating how to use the Snowpipe Streaming SDK to ingest data into Snowflake in real-time.

## Overview

The Snowpipe Streaming SDK enables applications to stream data directly into Snowflake tables with low latency and high throughput. This repository provides practical examples to help you get started with the SDK quickly.

## Choosing a channel mode

Snowpipe Streaming supports two channel modes:

- **Elastic Channels** (recommended default) — one implicit, Snowflake-managed channel per pipe. Simplest to develop against, with concurrent producers and durable acknowledgements. Use Elastic Channels unless you specifically need the guarantees below.
- **Named channels** — caller-defined channels with offset tokens, for strict exactly-once ingestion, ordering, and source-offset recovery.

Each language example below includes both an Elastic Channels path and a named-channel path.

## Production retention contract

Each production example is self-contained: SDK calls, configuration, retry decisions, and the replay
fixture live in the same file. Both modes append immediately and leave transport batching to the SDK.
They keep appending while the SDK accepts work and collect confirmed progress without draining
every checkpoint. SDK backpressure pauses source intake; a separate 100,000-pending-event safety
limit bounds application bookkeeping and may pause intake before the SDK buffer fills. This is not
a byte-memory limit. Elastic retires only a contiguous acknowledged prefix; named channels fetch
committed status periodically. Unfinished appends are never resubmitted merely because they are slow.
The sample stops after 30 minutes without confirmed source progress while work is outstanding.
The timer resets on confirmed progress, not on successful submissions. Caught-up idle producers
do not expire. At end-of-input, pending work drains under the same stalled-progress policy. These are application settings, not SDK default timeouts. SDK management calls also
have their own transport timeouts and retries; the application deadline does not cancel those calls.

The producer application must retain or be able to replay unacknowledged events. The included
`SampleEventSource` generates fixed sample events; its checkpoint is in-memory and it is **not durable storage**.
Replace `read`, `acknowledge`, and `seek` with your application's retained log, outbox, or source APIs.
For push sources, implement upstream flow control instead of merely ceasing reads. If events cannot be
replayed and must survive a restart, persist them in a bounded producer-local buffer before accepting
responsibility for them. Kafka is not a prerequisite. Host-independent durability requires appropriate
persistent or replicated storage, and any finite buffer needs a capacity/overflow policy.

- Elastic: acknowledge source progress only after every append in the window is durably acknowledged.
  Recreate the client only for SDK invalidation. Terminal retryable SDK failures may be replayed with
  stable source-unique event IDs; even successful SDK internal retries can produce duplicates.
- Named: assign one owner to each stable channel name. Reopen without supplying a replacement offset,
  seek strictly after Snowflake's returned committed offset, and never drop the channel during recovery.
  Schema/row errors require reconciliation rather than automatic source handoff.
- A successful Elastic acknowledgement is not proof of target-table visibility or row validity.
  Check the error table and materialization separately; shared Elastic status counters cannot validate
  one producer's checkpoint.
- On failure, the programs exit nonzero and report the last confirmed source checkpoint. Re-run Elastic
  with `SNOWFLAKE_SOURCE_CHECKPOINT` from your actual persisted source state, or replay conservatively
  with deduplication. Named runs obtain the authoritative offset from Snowflake.

## Choosing SDK vs. REST

- **SDK (Java, Python, Node.js)** — recommended for most applications. Higher throughput and simpler error handling than calling the REST API directly.
- **REST API** — use for lightweight, language-agnostic, or infrastructure-constrained integrations where adding the SDK isn't practical.

## Examples

This repository contains complete, runnable examples in multiple languages:

### [Java Example](./java-example)
A complete Maven project demonstrating the Snowpipe Streaming SDK in Java. Includes:
- Maven build configuration with all required dependencies
- Full example code with proper error handling
- Comprehensive setup instructions
- Sample configuration files
- **[Monitoring & Abort](./java-example/monitoring)** — Monitor channel status, track offset lag, inject errors, and abort on error increase

### [Python Example](./python-example)
A complete Python project demonstrating the Snowpipe Streaming SDK in Python. Includes:
- Requirements file with all necessary packages
- Clean, well-documented example code
- Setup instructions with virtual environment
- Sample configuration files
- **[Monitoring & Abort](./python-example/monitoring)** — Monitor channel status, track offset lag, inject errors, abort on error increase, and optional live matplotlib plotting

### [Node.js Example](./nodejs-example)
A complete Node.js project demonstrating the Snowpipe Streaming SDK in Node.js. Includes:
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
