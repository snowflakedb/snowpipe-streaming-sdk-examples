#!/usr/bin/env node
/**
 * Elastic ingest of a large, uninterrupted row stream.
 *
 * Create a table-mode client, get the channel, and keep appending. The SDK
 * batches rows for transport. This program submits many single-row
 * appendRowWithWait calls before waiting, and only pauses when too many
 * acknowledgement Promises are outstanding — not after every append.
 *
 * Ctrl+C (SIGINT) aborts intake through an AbortController. Already-accepted
 * appends are drained, stats are printed, and the client closes.
 */
"use strict";

process.env.SS_LOG_LEVEL ??= "warn";

const { ACK_TIMEOUT_MS, createClient, sampleRow } = require("./elastic_ingest.js");

const DEFAULT_ROWS = 10_000_000;
const MAX_PENDING_EVENTS = 10_000;

function isAbortError(error) {
  return error?.name === "AbortError" || error?.code === "ABORT_ERR";
}

function track(promise, submittedAt) {
  const item = { submittedAt, settled: false, result: null };
  item.outcome = Promise.resolve(promise)
    .then(
      () => ({ ok: true }),
      (error) => ({ ok: false, error }),
    )
    .then((result) => {
      item.settled = true;
      item.result = result;
      return result;
    });
  return item;
}

async function waitAndRemoveConfirmedPrefix(pending) {
  await pending[0].outcome;
  let confirmed = 0;
  let latencySum = 0;
  while (pending[0]?.settled) {
    const item = pending.shift();
    if (!item.result.ok) throw item.result.error;
    latencySum += performance.now() - item.submittedAt;
    confirmed += 1;
  }
  return { confirmed, latencySum };
}

async function main(clientFactory = createClient) {
  const total = Number.parseInt(process.env.SNOWFLAKE_TEST_ROWS ?? String(DEFAULT_ROWS), 10);
  const client = await clientFactory();
  const pending = [];
  let acked = 0;
  let latencySum = 0;
  const started = performance.now();
  let waitOnClose = false;
  const stop = new AbortController();
  const onSigint = () => stop.abort();
  process.on("SIGINT", onSigint);
  try {
    const channel = await client.getElasticChannel();
    try {
      for (let eventId = 0; eventId < total; eventId++) {
        if (stop.signal.aborted) break;
        const submittedAt = performance.now();
        pending.push(track(channel.appendRowWithWait(sampleRow(eventId), null), submittedAt));
        if (pending.length < MAX_PENDING_EVENTS) continue;
        const prefix = await waitAndRemoveConfirmedPrefix(pending);
        acked += prefix.confirmed;
        latencySum += prefix.latencySum;
      }
    } catch (error) {
      if (!isAbortError(error)) throw error;
    }
    while (pending.length) {
      const prefix = await waitAndRemoveConfirmedPrefix(pending);
      acked += prefix.confirmed;
      latencySum += prefix.latencySum;
    }
    waitOnClose = true;
    const elapsedSec = (performance.now() - started) / 1000;
    console.log(`Durably acknowledged ${acked} rows`);
    if (acked && elapsedSec > 0) {
      const avgAckMs = latencySum / acked;
      const rps = acked / elapsedSec;
      // Average ack latency can look large. Many appends are in flight, so
      // throughput is not 1 / latency — parallelism carries the rate.
      console.log(`avg ack latency ${avgAckMs.toFixed(1)} ms, ${rps.toFixed(0)} rows/s`);
    }
  } catch (error) {
    if (!isAbortError(error)) throw error;
  } finally {
    process.removeListener("SIGINT", onSigint);
    await client.close({ waitForFlush: waitOnClose, timeoutMs: ACK_TIMEOUT_MS });
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
  DEFAULT_ROWS,
  MAX_PENDING_EVENTS,
  createClient,
  main,
  sampleRow,
};
