# Python Streaming Quickstarts

Two self-contained examples send ten rows using SDK 1.8.0: an Elastic channel
quickstart and a named channel quickstart. Requires Python 3.9+.

## Setup

Create `profile.json` using `profile.json.example`. Configure your account, user,
role, and private key for key-pair authentication. Register the matching public
key on the Snowflake user. Keep credentials and local profiles out of version control.
See [key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth).

Create a table in an existing database/schema using an authorized role:

```sql
CREATE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE (
    C1 NUMBER,
    C2 VARCHAR,
    TS TIMESTAMP_NTZ
);
```

Set `DATABASE`, `SCHEMA`, and `TABLE` at the top of the example you want to run.
Both examples use the default streaming pipe; no CREATE PIPE statement is needed.
Run from this language directory so `profile.json` resolves correctly:

```bash
python -m pip install -r requirements.txt
python elastic_channel_quickstart.py
python named_channel_quickstart.py
```

## Choose a Quickstart

- **Elastic:** generates rows, submits them before waiting, observes durable
  acknowledgements, and closes the client. The SDK batches and retries transport
  operations. No application channel name or source offset is required.
- **Named:** opens a fresh demonstration channel, appends rows with offsets,
  waits for the final committed offset, reports channel status, and closes.
  The timestamp field demonstrates an additional column mapping.

## Errors and Delivery

Failures surface to the caller and resources are closed. The Elastic example does
not impose a short timeout on individual acknowledgements. Its 30-second close
budget is a cleanup limit. The named example has a 30-second commit wait; expiration
does not cancel ingestion or prove failure. Do not blindly replay after a timeout.

Elastic acknowledgements confirm durable buffering, not immediate table visibility
or successful materialization of every row. Check table contents and error logging.
Elastic replay can duplicate data. Keep real source events recoverable until confirmed.

The named quickstart uses a new channel name per run for a clean demonstration.
It is not a restart-recovery or exactly-once source integration recipe. Production
recovery requires stable channel ownership and source replay using the committed
offset returned on reopen. Neither quickstart implements crash recovery.

Advanced continuous ingestion and callback examples are deferred to a separate PR.
