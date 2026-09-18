#!/usr/bin/env node
/**
 * Elastic ingest: the four append APIs.
 *
 * Elastic Channels need no channel name, offset token, or recovery config.
 * Create a table-mode client, get the channel, and append.
 *
 * The SDK batches rows for transport. Waiting after every append is the slow
 * path. Pipelining single-row appendRowWithWait calls is the recommended
 * default for throughput and simplicity.
 *
 * appendRows is optional: one Promise and one append token for a logical group
 * when you already have a batch, or to cut JS/FFI call overhead. It does not
 * replace SDK transport batching.
 *
 * Fire-and-forget appendRow/appendRows return no Promise. Success and error
 * handlers are the only acknowledgement signal; pass your own append token and
 * the SDK echoes it back.
 */
"use strict";

process.env.SS_LOG_LEVEL ??= "warn";

const { randomUUID } = require("node:crypto");
const streaming = require("snowpipe-streaming");

const PIPELINE_ROWS = 10;
const BATCH_COUNT = 2;
const BATCH_SIZE = 5;
const FIRE_AND_FORGET_ROWS = 3;
const FIRE_AND_FORGET_BATCH_SIZE = 4;
const ACK_TIMEOUT_MS = 60_000;

function connectionProperties() {
  const pat = process.env.SNOWFLAKE_PAT;
  if (!pat) return null;
  if (!process.env.SNOWFLAKE_ACCOUNT || !process.env.SNOWFLAKE_URL) {
    throw new Error("PAT authentication requires SNOWFLAKE_ACCOUNT and SNOWFLAKE_URL");
  }

  const properties = {
    authorization_type: "PAT",
    personal_access_token: pat,
    account: process.env.SNOWFLAKE_ACCOUNT,
    url: process.env.SNOWFLAKE_URL,
  };
  if (process.env.SNOWFLAKE_ROLE) {
    properties.role = process.env.SNOWFLAKE_ROLE;
  }
  return properties;
}

async function createClient() {
  const properties = connectionProperties();
  return streaming.createTableClient({
    clientName: `ingest-${randomUUID()}`,
    dbName: process.env.SNOWFLAKE_DATABASE || "MY_DATABASE",
    schemaName: process.env.SNOWFLAKE_SCHEMA || "MY_SCHEMA",
    tableName: process.env.SNOWFLAKE_TABLE || "MY_TABLE",
    ...(properties ? { properties } : { profilePath: process.env.SNOWFLAKE_PROFILE || "profile.json" }),
  });
}

function sampleRow(eventId) {
  return {
    EVENT_ID: eventId,
    C1: eventId,
    C2: `event-${eventId}`,
  };
}

async function main(clientFactory = createClient) {
  const client = await clientFactory();
  try {
    // Elastic Channels belong to their client and are not closed separately.
    const channel = await client.getElasticChannel();
    let nextId = 0;

    // 1. Wait per row — simplest call, and the slow path.
    await channel.appendRowWithWait(sampleRow(nextId), `event-${nextId}`);
    nextId += 1;

    // 2. Recommended: submit every row before waiting. The SDK batches
    // these for transport.
    const pipelined = [];
    for (let eventId = nextId; eventId < nextId + PIPELINE_ROWS; eventId++) {
      pipelined.push(channel.appendRowWithWait(sampleRow(eventId), null));
    }
    await Promise.all(pipelined);
    nextId += PIPELINE_ROWS;

    // 3. Optional: application batches when you already have a group, or
    // want fewer Promises and tokens. Same pipelining; not required for
    // wire efficiency.
    const batched = [];
    for (let batchIndex = 0; batchIndex < BATCH_COUNT; batchIndex++) {
      const rows = [];
      for (let eventId = nextId; eventId < nextId + BATCH_SIZE; eventId++) {
        rows.push(sampleRow(eventId));
      }
      batched.push(channel.appendRowsWithWait(rows, `batch-${batchIndex}`));
      nextId += BATCH_SIZE;
    }
    await Promise.all(batched);

    // 4. Fire-and-forget: no Promise. Handlers are the only ack signal.
    // They run on the SDK ack callback — cheap bookkeeping only. The SDK
    // echoes your append token; it does not assign an offset.
    const submittedAt = new Map();
    const latencies = [];
    const failures = [];

    channel.setSuccessHandler((detail) => {
      const now = performance.now();
      for (const token of detail.appendTokens) {
        latencies.push(now - submittedAt.get(token));
      }
    });
    channel.setErrorHandler((detail) => {
      failures.push(detail);
    });

    const started = performance.now();
    let callbackRows = 0;
    for (let eventId = nextId; eventId < nextId + FIRE_AND_FORGET_ROWS; eventId++) {
      const token = `event-${eventId}`;
      submittedAt.set(token, performance.now());
      channel.appendRow(sampleRow(eventId), token);
    }
    nextId += FIRE_AND_FORGET_ROWS;
    callbackRows += FIRE_AND_FORGET_ROWS;

    const fireAndForgetBatch = [];
    for (let eventId = nextId; eventId < nextId + FIRE_AND_FORGET_BATCH_SIZE; eventId++) {
      fireAndForgetBatch.push(sampleRow(eventId));
    }
    submittedAt.set("batch-ff", performance.now());
    channel.appendRows(fireAndForgetBatch, "batch-ff");
    nextId += FIRE_AND_FORGET_BATCH_SIZE;
    callbackRows += FIRE_AND_FORGET_BATCH_SIZE;

    await channel.waitForFlush({ timeoutMs: ACK_TIMEOUT_MS });
    if (failures.length) {
      throw failures[0].error;
    }
    const elapsedSec = (performance.now() - started) / 1000;

    console.log(`Durably acknowledged ${nextId} rows`);
    if (latencies.length && elapsedSec > 0) {
      const avgAckMs = latencies.reduce((sum, value) => sum + value, 0) / latencies.length;
      const rps = callbackRows / elapsedSec;
      // Average ack latency can look large. Many appends are in flight, so
      // throughput is not 1 / latency — parallelism carries the rate.
      console.log(`Callback path: avg ack latency ${avgAckMs.toFixed(1)} ms, ${rps.toFixed(0)} rows/s`);
    }
  } finally {
    await client.close({ waitForFlush: true, timeoutMs: ACK_TIMEOUT_MS });
  }
}

if (require.main === module) {
  const keepAlive = setInterval(() => {}, 1_000);
  main()
    .catch((error) => {
      console.error(error.message);
      process.exitCode = 1;
    })
    .finally(() => clearInterval(keepAlive));
}

module.exports = {
  PIPELINE_ROWS,
  BATCH_COUNT,
  BATCH_SIZE,
  FIRE_AND_FORGET_ROWS,
  FIRE_AND_FORGET_BATCH_SIZE,
  ACK_TIMEOUT_MS,
  connectionProperties,
  createClient,
  sampleRow,
  main,
};
