"""Send ten rows through an Elastic channel and wait for durable acknowledgements."""

import uuid

from snowflake.ingest.streaming import StreamingIngestClient

# Replace these with your existing target table. Authentication lives in profile.json.
DATABASE = "MY_DATABASE"
SCHEMA = "MY_SCHEMA"
TABLE = "MY_TABLE"


def main():
    """Load profile.json, pipeline sample rows, confirm, and close the client."""
    # The table client uses the default streaming pipe; no CREATE PIPE is needed.
    client = StreamingIngestClient.from_table(
        client_name=f"quickstart-{uuid.uuid4()}",
        db_name=DATABASE,
        schema_name=SCHEMA,
        table_name=TABLE,
        profile_json="profile.json",
    )
    complete = False
    try:
        # Elastic channels are managed by Snowflake, so there is no channel name to choose.
        channel = client.get_elastic_channel()
        pending = []
        for event_id in range(1, 11):
            # Replace sample values with source data; keys match the target columns.
            row = {"C1": event_id, "C2": str(event_id)}
            pending.append(channel.append_row_with_wait(row, None))
        # Submit before waiting so the SDK can batch. None disables callback token reporting.
        # A slow acknowledgement is not a failed append; do not resubmit on a local timeout.
        for acknowledgement in pending:
            acknowledgement.result()
        complete = True
        print("Durably acknowledged 10 rows. Check table contents separately.")
    finally:
        # Always release resources. On failure, retain unconfirmed source data for recovery.
        client.close(wait_for_flush=complete, timeout_seconds=30)


if __name__ == "__main__":
    main()
