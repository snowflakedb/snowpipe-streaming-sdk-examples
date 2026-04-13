# Snowpipe Streaming: Monitor & Abort Example (Java)

This example demonstrates how to monitor Snowpipe Streaming channel status in real-time and automatically halt production when server-side errors are detected.

## What It Does

- Streams rows using the Snowpipe Streaming Java SDK (high-performance architecture)
- Polls `getChannelStatus()` during production to track committed offsets, error counts, and processing latency
- Computes **offset lag** (sent - committed) and prints it to console
- **Aborts production** when `getRowsErrorCount()` increases — useful for catching schema mismatches early
- Supports **error injection** via a configurable flag to demonstrate the abort behavior
- Uses [Java Faker](https://github.com/DiUS/java-faker) for realistic test data

> **Note:** The Python version of this example includes live matplotlib plotting of offset lag. This Java version uses console output only.

## Prerequisites

- Java 11+
- Maven 3.6+
- A Snowflake account with the [high-performance architecture](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview) enabled
- RSA key-pair authentication configured (see [parent README](../README.md))

## Setup

1. Create the target table in Snowflake:

```sql
CREATE OR REPLACE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE (
    user_id INTEGER,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone_number VARCHAR(50),
    address VARCHAR(500),
    date_of_birth DATE,
    registration_date TIMESTAMP_NTZ,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

> No `CREATE PIPE` is needed — the default pipe `MY_TABLE-STREAMING` is auto-created.

2. Copy `profile.json.example` to `profile.json` and fill in your credentials:

```bash
cp profile.json.example profile.json
```

3. Build the project:

```bash
mvn clean package
```

## Configuration

Edit the constants at the top of `MonitorAbortExample.java`:

| Constant | Default | Description |
|----------|---------|-------------|
| `TOTAL_ROWS` | `500` | Number of rows to stream |
| `SEND_INTERVAL_MS` | `100` | Delay between row appends (ms) |
| `POLL_INTERVAL_MS` | `500` | How often to poll channel status (ms) |
| `TEST_ERROR_OFFSET` | `-1` | Set to a positive integer to inject a schema error at that offset. `-1` disables error injection |

## Running

### Demo 1: Normal monitoring (no errors)

```bash
mvn exec:java
```

With `TEST_ERROR_OFFSET = -1`, all 500 rows stream successfully. The monitor prints status updates showing lag converging to zero.

### Demo 2: Error injection with abort

Set `TEST_ERROR_OFFSET = 200` (or any positive integer) in `MonitorAbortExample.java`, rebuild, then run:

```bash
mvn clean package
mvn exec:java
```

At offset 200, a row with invalid types is injected. When the monitor detects `getRowsErrorCount()` increasing, it halts production immediately.

## Monitor Output

Each status poll prints:

```
[monitor] committed= 150
[monitor] rows_inserted= 150
[monitor] rows_error_count= 0
[monitor] server_avg_processing_latency= 245
[monitor] last_error_offset_token_upper_bound= null
[monitor] last_error_message= null
[monitor] offset_lag= 50
```

## SSv2 API Notes

This example uses the **high-performance architecture** (SSv2) API:

- `appendRow(row, offsetToken)` — fire-and-forget, validation is server-side
- `getChannelStatus()` — returns `ChannelStatus` with `getRowsErrorCount()`, `getRowsInsertedCount()`, `getLatestOffsetToken()`, `getServerAvgProcessingLatency()`, `getLastErrorMessage()`, etc.
