# Snowpipe Streaming: Monitor & Abort Example (Python)

This example demonstrates how to monitor Snowpipe Streaming channel status in real-time and automatically halt production when server-side errors are detected.

## What It Does

- Streams rows using the Snowpipe Streaming Python SDK (high-performance architecture)
- Polls `get_channel_status()` during production to track committed offsets, error counts, and processing latency
- Computes **offset lag** (sent - committed) and optionally plots it live with matplotlib
- **Aborts production** when `rows_error_count` increases — useful for catching schema mismatches early
- Supports **error injection** via a configurable flag to demonstrate the abort behavior

## Prerequisites

- Python 3.9+
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

3. Install dependencies:

```bash
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

## Configuration

Edit the constants at the top of `monitor_abort_example.py`:

| Constant | Default | Description |
|----------|---------|-------------|
| `TOTAL_ROWS` | `500` | Number of rows to stream |
| `SEND_INTERVAL_MS` | `100` | Delay between row appends (ms) |
| `POLL_MS` | `500` | How often to poll channel status (ms) |
| `TEST_ERROR_OFFSET` | `-1` | Set to a positive integer to inject a schema error at that offset. `-1` disables error injection |
| `ENABLE_PLOTTING` | `False` | Set to `True` for live matplotlib lag plot (falls back gracefully if matplotlib is not installed) |

## Running

### Demo 1: Normal monitoring (no errors)

```bash
python monitor_abort_example.py
```

With `TEST_ERROR_OFFSET = -1`, all 500 rows stream successfully. The monitor prints status updates and the live plot shows offset lag converging to zero.

### Demo 2: Error injection with abort

Set `TEST_ERROR_OFFSET = 200` (or any positive integer) in the script, then run:

```bash
python monitor_abort_example.py
```

At offset 200, a row with invalid types is injected. When the monitor detects `rows_error_count` increasing, it halts production immediately.

## Monitor Output

Each status poll prints:

```
[monitor] committed= 150
[monitor] rows_inserted= 150
[monitor] rows_error_count= 0
[monitor] server_avg_processing_latency= 245
[monitor] last_error_offset_token_upper_bound= None
[monitor] last_error_message= None
[monitor] offset_lag= 50
```
