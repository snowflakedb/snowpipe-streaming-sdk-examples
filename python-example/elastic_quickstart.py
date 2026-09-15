"""
Elastic Channel quickstart for the Snowpipe Streaming SDK (Python).

Elastic Channels are the default starting point for a new streaming
application. You get one channel per client, Snowflake manages the fan-out,
and you never name, open, or close a channel yourself.

If you need ordered, strict exactly-once ingestion driven by a replayable
source offset, use a named channel instead — see
named_channel_checkpoint_example.py.

For a version with retry, backpressure handling, and recovery, see
elastic_production_example.py.

Requires snowpipe-streaming >= 1.8.0.
"""

import os
import uuid

# Set to "info" or "debug" to increase SDK logging detail.
os.environ.setdefault("SS_LOG_LEVEL", "warn")

from snowflake.ingest.streaming import StreamingIngestClient

# Replace these with your Snowflake object names.
DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "MY_DATABASE")
SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "MY_SCHEMA")
TABLE = os.environ.get("SNOWFLAKE_TABLE", "MY_TABLE")

def connection_properties():
    """Use securely injected PAT auth when present, otherwise profile.json."""
    pat = os.environ.get("SNOWFLAKE_PAT")
    if not pat:
        return None
    return {
        "authorization_type": "PAT",
        "personal_access_token": pat,
        "account": os.environ.get("SNOWFLAKE_ACCOUNT", "PM"),
        "url": os.environ.get("SNOWFLAKE_URL", "https://PM.snowflakecomputing.com"),
        "role": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    }


def main():
    """Append one row and wait for durable acknowledgement."""
    client = StreamingIngestClient.from_table(
        client_name=f"elastic-quickstart-{uuid.uuid4()}",
        db_name=DATABASE,
        schema_name=SCHEMA,
        table_name=TABLE,
        profile_json=None if connection_properties() else "profile.json",
        properties=connection_properties(),
    )

    try:
        channel = client.get_elastic_channel()

        row = {
            "DATA": {"event_id": 1, "status": "active"},
            "C1": 1,
            "C2": "example",
        }
        channel.append_row_with_wait(row, "batch-1").result(timeout=60)

        status = channel.get_channel_status()
        print(f"Durably acknowledged; status={status.status_code}, errors={status.rows_error_count}")
    finally:
        client.close(wait_for_flush=True, timeout_seconds=60)


if __name__ == "__main__":
    main()
