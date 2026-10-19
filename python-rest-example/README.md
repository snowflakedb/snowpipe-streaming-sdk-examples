# Elastic REST Quickstart

Use the SDK when possible; it owns transport batching and retries. This example
shows those responsibilities explicitly for direct REST, without a streaming SDK.

## Setup

Requires Python 3.9+. Create `profile.json` using `profile.json.example`, with your
account, user, role, account URL, and private key path. Register the matching public
key on your Snowflake user. Keep profiles and keys out of version control. For an
encrypted key, inject `PRIVATE_KEY_PASSPHRASE` through your credential manager.

Create a table with `C1 NUMBER` and `C2 VARCHAR`. Set `DATABASE`, `SCHEMA`, and
`TABLE` at the top of `elastic_channel_quickstart.py`. Run from this directory:

```bash
python -m pip install -r requirements.txt
python elastic_channel_quickstart.py
```

## Walkthrough

1. Load the profile and generate ten sample rows with an explicit loop.
2. Discover the ingestion host and exchange a signed JWT for a scoped token.
3. Encode rows as NDJSON, bounded by 5,000 rows and 4,000,000 uncompressed bytes.
4. Gzip each batch. Split at row boundaries if its exact compressed body exceeds
   1,000,000 bytes, below the 4 MB service wire limit. Reject a single oversized row.
5. Send one request at a time. Confirm it before advancing source progress.
6. Send the final partial batch and close the HTTP session.

The ten sample rows normally form one compressed request. For a real source, pass
an iterator into `batch_rows` rather than collecting the entire source into a list.
The one-second flush check runs between source reads; a blocking source requires
its own idle-flush integration. Input limits do not bound total process memory.

## Retries and Delivery

Retries reuse identical compressed bytes and `requestId`, incrementing `retryCount`.
The sample handles connection errors, timeouts, and HTTP 404/408/429/500/502/503/504.
HTTP 404 retries address transient availability loss, not incorrect target names.
Validate the target during setup. Persistent failures stop after eight retries or
the 30-minute batch budget, whichever is reached first. Requests time out at 30 seconds.
Backoff uses jitter and respects `Retry-After` without shortening the requested delay.
HTTP 401 triggers at most one token refresh; other permanent errors fail visibly.
Token discovery/exchange failures stop the run.

Request identity does not guarantee deduplication. Ambiguous responses and replay
can duplicate rows. Keep source events recoverable until acknowledgement. Acknowledgement
confirms durable buffering, not row validity or immediate table visibility. Verify
table contents separately. This quickstart does not implement crash recovery or
signal-driven draining. It is an Elastic example, not a named-channel offset example.

## Validation

Private tests cover compressed bounds, row preservation, retries, and JWT construction.
Live PAT-backed testing of prior versions does not validate this profile-only JWT path.
No credentials or private test harnesses are included in this repository.

## References

- [REST reference](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api)
- [Elastic limitations](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-elastic-channels-limitations)
