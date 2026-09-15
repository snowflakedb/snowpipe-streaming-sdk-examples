/**
 * Single-writer named-channel producer. Append immediately, then checkpoint the
 * committed offset before source handoff. Retain source events for replay;
 * timeout pauses intake without reopening. Do not share channel ownership.
 */
"use strict";

const { createTableClient, StreamingIngestError } = require("snowpipe-streaming");

const MAX_PENDING_EVENTS = 100_000;
const CHECKPOINT_ROWS = 1_000;
const CHECKPOINT_MS = 5_000;
const OUTAGE_MS = 30 * 60_000;
const POLL_MS = 1_000;
const MAX_ATTEMPTS = 6;
const INVALIDATION = new Set([
  "InvalidChannelError", "InvalidClientError", "ClosedChannelError",
  "ClosedElasticChannelError", "ClosedClientError",
]);

// Start here: source, connection, then the streaming loop.

// Run the sample and close the client, retaining unconfirmed source work on failure.
async function main() {
  const source = new SampleEventSource(Number(process.env.SNOWFLAKE_TEST_ROWS || 10_000),
    Number(process.env.SNOWFLAKE_SOURCE_CHECKPOINT || 0));
  const producer = new NamedProducer();
  let completed = false;
  try {
    await run(producer, source);
    completed = true;
    console.log(`Committed source checkpoint: ${source.committed}`);
  } finally {
    if (!completed) console.error(`Stopped. Retain events after checkpoint ${source.committed} for replay`);
    await producer.close(completed);
  }
}

// Stream retained events while collecting confirmed delivery progress.
async function run(producer, source) {
  source.seek(await producer.open());
  let submitted = source.committed;
  let event = null;
  let exhausted = false;
  let failures = 0;
  let sincePoll = 0;
  let nextPoll = performance.now() + CHECKPOINT_MS;
  let deadline = performance.now() + OUTAGE_MS;
  while (true) {
    try {
      if (submitted > source.committed && (exhausted || event !== null || sincePoll >= CHECKPOINT_ROWS
          || performance.now() >= nextPoll || submitted - source.committed >= MAX_PENDING_EVENTS)) {
        const previous = source.committed;
        await collectProgress(producer, submitted, source);
        if (source.committed > previous) { deadline = performance.now() + OUTAGE_MS; failures = 0; }
        nextPoll = performance.now() + CHECKPOINT_MS;
        sincePoll = 0;
      }
      if (submitted === source.committed && event === null) {
        deadline = performance.now() + OUTAGE_MS;
        if (exhausted) return;
      }
      remaining(deadline);
      if (exhausted || submitted - source.committed >= MAX_PENDING_EVENTS) {
        await new Promise((resolve) => setTimeout(resolve, Math.min(POLL_MS, remaining(deadline))));
        continue;
      }
      if (event === null) event = source.read();
      if (event === null) { exhausted = true; continue; }
      producer.channel.appendRow(event.row, String(event.offset));
      submitted = event.offset;
      event = null;
      sincePoll++;
    } catch (error) {
      if (!retryable(error)) throw error;
      if (error.httpStatusCode !== 429 && ++failures >= MAX_ATTEMPTS) throw error;
      if (INVALIDATION.has(error.errorCode)) {
        const previous = source.committed;
        source.seek(await producer.recover(error));
        if (source.committed > previous) deadline = performance.now() + OUTAGE_MS;
        submitted = source.committed;
        event = null;
        exhausted = false;
      }
      await backoff(2, deadline);
    }
  }
}

// Fetch once: a partial committed offset is useful progress, not a reason to block.
async function collectProgress(producer, submitted, source) {
  const status = await producer.channel.getChannelStatus();
  if (status.rowsErrorCount) throw new Error("Reconcile row errors before source handoff");
  if (status.statusCode !== "SUCCESS") {
    throw new StreamingIngestError("InvalidChannelError", status.statusCode, 409, "Conflict");
  }
  const committed = Math.min(submitted, parseOffset(status.latestCommittedOffsetToken));
  if (committed > source.committed) source.acknowledge(committed);
}

// Supporting delivery and connection details.

// Identify SDK failures eligible for bounded application retry.
function retryable(error) {
  return error instanceof StreamingIngestError &&
    (INVALIDATION.has(error.errorCode) || [408, 429, 500, 502, 503, 504].includes(error.httpStatusCode));
}

// Return the remaining stalled-progress budget without advancing source progress.
function remaining(deadline) {
  const millis = deadline - performance.now();
  if (millis <= 0) {
    throw new Error("Stalled-progress deadline exceeded; retain events after the confirmed source checkpoint");
  }
  return millis;
}

// Wait with capped jitter without exceeding the remaining checkpoint budget.
async function backoff(attempt, deadline) {
  const delay = Math.min(remaining(deadline), Math.random() * Math.min(10_000, 250 * 2 ** Math.min(attempt, 6)));
  await new Promise((resolve) => setTimeout(resolve, delay));
}

// Create a table client using the authentication profile or explicitly configured PAT.
async function createClient() {
  let authentication = { profilePath: process.env.SNOWFLAKE_PROFILE || "profile.json" };
  if (process.env.SNOWFLAKE_PAT) {
    if (!process.env.SNOWFLAKE_ACCOUNT || !process.env.SNOWFLAKE_URL) {
      throw new Error("PAT mode requires SNOWFLAKE_ACCOUNT and SNOWFLAKE_URL");
    }
    authentication = { properties: {
      authorization_type: "PAT",
      personal_access_token: process.env.SNOWFLAKE_PAT,
      account: process.env.SNOWFLAKE_ACCOUNT,
      url: process.env.SNOWFLAKE_URL,
      ...(process.env.SNOWFLAKE_ROLE ? { role: process.env.SNOWFLAKE_ROLE } : {}),
    } };
  }
  return createTableClient({
    clientName: `production-${process.pid}`,
    dbName: process.env.SNOWFLAKE_DATABASE || "MY_DATABASE",
    schemaName: process.env.SNOWFLAKE_SCHEMA || "MY_SCHEMA",
    tableName: process.env.SNOWFLAKE_TABLE || "MY_TABLE",
    ...authentication,
  });
}

// Regenerable sample data only; a real source must retain events across restarts.
class SampleEventSource {
  constructor(total = 10_000, checkpoint = 0) {
    if (!Number.isSafeInteger(total) || !Number.isSafeInteger(checkpoint) || checkpoint < 0 || checkpoint > total) {
      throw new Error("Require integer 0 <= source checkpoint <= total");
    }
    this.total = total;
    this.committed = checkpoint;
    this.nextOffset = checkpoint + 1;
  }
  // Return the next sample event without acknowledging source progress.
  read() {
    if (this.nextOffset > this.total) return null;
    const offset = this.nextOffset++;
    // Replace this mapping with your target columns and stable event ID.
    return { offset, row: { EVENT_ID: offset, C1: offset, C2: `event-${offset}` } };
  }
  // Record confirmed progress; replace with your source's durable commit operation.
  acknowledge(offset) {
    // Persist/commit source progress here before retiring real source events.
    if (offset < this.committed || offset > this.total) throw new Error("Invalid source checkpoint");
    this.committed = offset;
  }
  // Resume sample reads after confirmed progress; replace with your source seek operation.
  seek(committed) {
    this.acknowledge(committed);
    this.nextOffset = committed + 1;
  }
}
const CHANNEL = process.env.SNOWFLAKE_CHANNEL || "production-source-0";

// Decode this sample's numeric source offset; an absent token means no progress.
function parseOffset(token) {
  if (token == null) return 0;
  const offset = Number(token);
  if (!Number.isSafeInteger(offset) || offset < 0) throw new Error("Invalid committed source offset");
  return offset;
}

// Own one stable named channel and preserve server progress during recovery.
class NamedProducer {
  constructor(factory = createClient) {
    this.factory = factory;
    this.client = null;
    this.channel = null;
  }
  // Open the owned named channel and return its authoritative committed source offset.
  async open() {
    if (!this.client) this.client = await this.factory();
    const opened = await this.client.openChannel({ name: CHANNEL });
    this.channel = opened.channel;
    if (opened.status.rowsErrorCount) throw new Error("Reconcile row errors before source handoff");
    return parseOffset(opened.status.latestCommittedOffsetToken);
  }
  // Reopen without resetting the server offset, recreating an invalid client if needed.
  async recover(error) {
    if (error.errorCode === "InvalidClientError") {
      await this.close(false);
    } else if (this.channel) {
      await this.channel.close({ waitForFlush: false, timeoutMs: 30_000 }).catch(() => {});
    }
    try {
      return await this.open();
    } catch (reopened) {
      if (!["InvalidClientError", "ClosedClientError"].includes(reopened.errorCode)) throw reopened;
      await this.close(false);
      return this.open();
    }
  }
  // Close the current client; flush only when requested by the caller.
  async close(flush) {
    if (this.client) {
      try {
        await this.client.close({ waitForFlush: flush, timeoutMs: 30_000 });
      } finally {
        this.client = null;
      }
    }
  }
}

if (require.main === module) {
  const keepAlive = setInterval(() => {}, 1_000);
  main().catch((error) => { console.error(error.message); process.exitCode = 1; })
    .finally(() => clearInterval(keepAlive));
}

module.exports = { MAX_PENDING_EVENTS, collectProgress, SampleEventSource, retryable, remaining, backoff, createClient, CHECKPOINT_ROWS, MAX_ATTEMPTS,  NamedProducer, parseOffset, run, main };
