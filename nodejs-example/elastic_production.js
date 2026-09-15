/**
 * Stream events immediately; checkpoint every acknowledgement before source handoff.
 * The SDK owns batching. Caller timeouts keep the original Promise alive.
 * The source fixture regenerates events but does not persist its checkpoint.
 * Replay can duplicate events; production EVENT_IDs must be stable and source-unique.
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
  const producer = new ElasticProducer();
  let completed = false;
  try {
    await producer.open();
    await run(producer, source);
    completed = true;
    console.log(`Durable source checkpoint: ${source.committed}; materialization is separate`);
  } finally {
    if (!completed) console.error(`Stopped. Retain events after checkpoint ${source.committed} for replay`);
    await producer.close(completed);
  }
}

// Stream retained events while collecting confirmed delivery progress.
async function run(producer, source) {
  // Periodic event-loop yields let native SDK completions run during sustained intake.
  const pending = [];
  let event = null;
  let exhausted = false;
  let submittedSinceYield = 0;
  let deadline = performance.now() + OUTAGE_MS;
  while (true) {
    const previous = source.committed;
    if (exhausted || event !== null || submittedSinceYield === 0 || pending.length >= MAX_PENDING_EVENTS) {
      await collectProgress(producer, pending, source, deadline);
    }
    if (source.committed > previous || (!pending.length && event === null)) {
      deadline = performance.now() + OUTAGE_MS;
    }
    if (exhausted && !pending.length) return;
    remaining(deadline);
    if (exhausted || pending.length >= MAX_PENDING_EVENTS) {
      await new Promise((resolve) => setTimeout(resolve, Math.min(POLL_MS, remaining(deadline))));
      continue;
    }
    if (event === null) event = source.read();
    if (event === null) { exhausted = true; continue; }
    // Observe rejection immediately; source ownership remains outside the SDK.
    const item = appendEvent(producer, event);
    await Promise.resolve();
    await Promise.resolve();
    if (item.result?.error?.httpStatusCode === 429) {
      await backoff(2, deadline);
      continue;
    }
    pending.push(item);
    event = null;
    if (++submittedSinceYield >= CHECKPOINT_ROWS) {
      await new Promise((resolve) => setImmediate(resolve));
      submittedSinceYield = 0;
    }
  }
}

// Collect successes without waiting for unresolved acknowledgements.
async function collectProgress(producer, pending, source, deadline) {
  for (let index = 0; index < pending.length; index++) {
    const item = pending[index];
    if (!item.result?.error) continue;
    const error = item.result.error;
    if (!retryable(error) || (item.retries || 0) >= MAX_ATTEMPTS - 1) throw error;
    if (INVALIDATION.has(error.errorCode)) await producer.recover(item.generation);
    await backoff(item.retries || 0, deadline);
    const replacement = appendEvent(producer, item.event);
    replacement.retries = (item.retries || 0) + 1;
    pending[index] = replacement;
  }
  let count = 0;
  while (count < pending.length && pending[count].result?.ok) count++;
  if (count) {
    source.acknowledge(pending[count - 1].event.offset);
    pending.splice(0, count);
  }
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

// Own the current Elastic client and prevent stale failures from replacing a fresh client.
class ElasticProducer {
  constructor(factory = createClient) {
    this.factory = factory;
    this.client = null;
    this.generation = 0;
  }
  // Create a client and obtain its cached Elastic Channel.
  async open() {
    const client = await this.factory();
    try {
      this.channel = await client.getElasticChannel();
    } catch (error) {
      await client.close({ waitForFlush: false, timeoutMs: 30_000 });
      throw error;
    }
    this.client = client;
    this.generation++;
  }
  // Replace the invalid client only if the failure belongs to its current generation.
  async recover(generation) {
    // Old pending failures must not close the replacement client.
    if (generation !== this.generation) return;
    await this.close(false);
    await this.open();
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

// Submit one event and retain its acknowledgement for checkpoint confirmation.
function appendEvent(producer, event) {
  // Observe rejection immediately, even while other events are being read.
  let promise;
  try {
    // This is the Snowflake write; keep its original acknowledgement Promise.
    promise = producer.channel.appendRowWithWait(event.row, String(event.offset));
  } catch (error) {
    promise = Promise.reject(error);
  }
  const item = { event, generation: producer.generation, result: null };
  item.outcome = Promise.resolve(promise)
    .then(() => ({ ok: true }), (error) => ({ error }))
    .then((result) => {
      item.result = result;
      return result;
    });
  return item;
}

if (require.main === module) {
  const keepAlive = setInterval(() => {}, 1_000);
  main().catch((error) => { console.error(error.message); process.exitCode = 1; })
    .finally(() => clearInterval(keepAlive));
}

module.exports = { MAX_PENDING_EVENTS, collectProgress, SampleEventSource, retryable, remaining, backoff, createClient, CHECKPOINT_ROWS, MAX_ATTEMPTS,  ElasticProducer, appendEvent, run, main };
