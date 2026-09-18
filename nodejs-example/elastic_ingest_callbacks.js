#!/usr/bin/env node
/**
 * Elastic ingest with callbacks instead of Promises.
 *
 * Same four-call tour as elastic_ingest.js, using appendRow / appendRows.
 * Handlers are the only acknowledgement signal.
 *
 * They run on the SDK acknowledgement callback. Keep them to a cheap
 * enqueue: no I/O, no SDK calls, and no await. A slow handler delays this
 * channel's acks and other work on the event loop.
 *
 * Count and checkpoint on the ingest path after waitOne(). Do not mutate
 * shared counters in the handler.
 */
"use strict";

process.env.SS_LOG_LEVEL ??= "warn";

const {
  ACK_TIMEOUT_MS,
  BATCH_COUNT,
  BATCH_SIZE,
  FIRE_AND_FORGET_BATCH_SIZE,
  FIRE_AND_FORGET_ROWS,
  PIPELINE_ROWS,
  createClient,
  sampleRow,
} = require("./elastic_ingest.js");

class AckInbox {
  constructor() {
    this._events = [];
    this._resolvers = [];
  }

  install(channel) {
    channel.setSuccessHandler((detail) => this.onSuccess(detail));
    channel.setErrorHandler((detail) => this.onError(detail));
  }

  onSuccess(detail) {
    for (const token of detail.appendTokens) {
      this._dispatch({ token });
    }
  }

  onError(detail) {
    this._dispatch({ error: detail.error });
  }

  _dispatch(event) {
    const resolve = this._resolvers.shift();
    if (resolve) {
      resolve(event);
      return;
    }
    this._events.push(event);
  }

  async waitOne(timeoutMs) {
    const event = this._events.length > 0 ? this._events.shift() : await this._waitForEvent(timeoutMs);
    if (event.error) throw event.error;
    return event.token;
  }

  _waitForEvent(timeoutMs) {
    return new Promise((resolve, reject) => {
      let settle;
      const timer = setTimeout(() => {
        const index = this._resolvers.indexOf(settle);
        if (index >= 0) this._resolvers.splice(index, 1);
        const error = new Error("Timed out waiting for an acknowledgement");
        error.name = "TimeoutError";
        reject(error);
      }, timeoutMs);
      settle = (event) => {
        clearTimeout(timer);
        resolve(event);
      };
      this._resolvers.push(settle);
    });
  }

  async waitN(count, timeoutMs) {
    const deadline = performance.now() + timeoutMs;
    const got = [];
    for (let i = 0; i < count; i++) {
      const remaining = deadline - performance.now();
      if (remaining <= 0) {
        const error = new Error("Timed out waiting for acknowledgements");
        error.name = "TimeoutError";
        throw error;
      }
      got.push(await this.waitOne(remaining));
    }
    return got;
  }

  raisePendingErrors() {
    while (this._events.length) {
      const event = this._events.shift();
      if (event.error) throw event.error;
    }
  }
}

async function main(clientFactory = createClient) {
  const client = await clientFactory();
  try {
    const channel = await client.getElasticChannel();
    const inbox = new AckInbox();
    inbox.install(channel);
    let nextId = 0;

    // 1. Wait per row — simplest call, and the slow path.
    channel.appendRow(sampleRow(nextId), `event-${nextId}`);
    await inbox.waitOne(ACK_TIMEOUT_MS);
    nextId += 1;

    // 2. Recommended: submit every row before waiting. The SDK batches
    // these for transport.
    for (let eventId = nextId; eventId < nextId + PIPELINE_ROWS; eventId++) {
      channel.appendRow(sampleRow(eventId), `event-${eventId}`);
    }
    await inbox.waitN(PIPELINE_ROWS, ACK_TIMEOUT_MS);
    nextId += PIPELINE_ROWS;

    // 3. Optional: application batches when you already have a group, or
    // want fewer tokens. Same pipelining; not required for wire efficiency.
    for (let batchIndex = 0; batchIndex < BATCH_COUNT; batchIndex++) {
      const rows = [];
      for (let eventId = nextId; eventId < nextId + BATCH_SIZE; eventId++) {
        rows.push(sampleRow(eventId));
      }
      channel.appendRows(rows, `batch-${batchIndex}`);
      nextId += BATCH_SIZE;
    }
    await inbox.waitN(BATCH_COUNT, ACK_TIMEOUT_MS);

    // 4. Fire-and-forget: do not wait on the inbox. waitForFlush covers
    // these appends; then look for errors the handler already queued.
    for (let eventId = nextId; eventId < nextId + FIRE_AND_FORGET_ROWS; eventId++) {
      channel.appendRow(sampleRow(eventId), `event-${eventId}`);
    }
    nextId += FIRE_AND_FORGET_ROWS;
    const fireAndForgetBatch = [];
    for (let eventId = nextId; eventId < nextId + FIRE_AND_FORGET_BATCH_SIZE; eventId++) {
      fireAndForgetBatch.push(sampleRow(eventId));
    }
    channel.appendRows(fireAndForgetBatch, "batch-ff");
    nextId += FIRE_AND_FORGET_BATCH_SIZE;
    await channel.waitForFlush({ timeoutMs: ACK_TIMEOUT_MS });
    inbox.raisePendingErrors();

    console.log(`Durably acknowledged ${nextId} rows`);
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

module.exports = { AckInbox, main };
