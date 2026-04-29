"""
Example demonstrating how to use the Snowflake Streaming Ingest SDK in Python
with the high-performance architecture and default pipe.

The default pipe is automatically created by Snowflake when you first
open a channel. No CREATE PIPE DDL is required. The default pipe name
follows the convention: <TABLE_NAME>-STREAMING
"""

from datetime import datetime
import uuid
import os

# Change Environment Variable SS_LOG_LEVEL="info" to increase logging details
os.environ["SS_LOG_LEVEL"] = "warn"

from snowflake.ingest.streaming import StreamingIngestClient


MAX_ROWS = 100_000

# Replace these with your Snowflake object names
DATABASE = "MY_DATABASE"
SCHEMA = "MY_SCHEMA"
TABLE = "MY_TABLE"

# Default pipe: Snowflake auto-creates this on first channel open
PIPE = f"{TABLE}-STREAMING"


def main():
    """Main function to demonstrate streaming data ingestion."""

    # Create Snowflake Streaming Ingest Client using context manager
    with StreamingIngestClient(
        client_name=f"MY_CLIENT_{uuid.uuid4()}",
        db_name=DATABASE,
        schema_name=SCHEMA,
        pipe_name=PIPE,
        profile_json="profile.json"
    ) as client:

        print("Client created successfully")

        # Open a channel for data ingestion using context manager
        with client.open_channel(f"MY_CHANNEL_{uuid.uuid4()}")[0] as channel:
            print(f"Channel opened: {channel.channel_name}")

            # Ingest rows — column names must match the target table schema.
            # The default pipe uses MATCH_BY_COLUMN_NAME to map fields.
            print(f"Ingesting {MAX_ROWS} rows...")
            for i in range(1, MAX_ROWS + 1):
                row_id = str(i)
                channel.append_row(
                    {
                        "c1": i,
                        "c2": row_id,
                        "ts": datetime.now()
                    },
                    row_id
                )

                if i % 10_000 == 0:
                    print(f"Ingested {i} rows...")

            print("All rows submitted. Waiting for commit...")

            # Wait for all rows to be committed using wait_for_commit.
            # The predicate receives the latest committed offset token
            # (str or None) and should return True when satisfied.
            def all_rows_committed(token):
                return token is not None and int(token) >= MAX_ROWS

            channel.wait_for_commit(all_rows_committed, timeout_seconds=30)

            # Now that data has landed, check the channel status
            status = channel.get_channel_status()
            print(f"All data committed. Channel status:")
            print(f"  Committed offset:   {status.latest_committed_offset_token}")
            print(f"  Rows inserted:      {status.rows_inserted_count}")
            print(f"  Rows errored:       {status.rows_error_count}")
            print(f"  Avg server latency: {status.server_avg_processing_latency}")
            if status.rows_error_count > 0:
                print(f"  Last error:         {status.last_error_message}")

        # Channel automatically closed here
        print("Data ingestion completed")

    # Client automatically closed here


if __name__ == "__main__":
    main()
