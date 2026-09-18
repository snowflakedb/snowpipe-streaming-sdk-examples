"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const callbacks = require("../elastic_ingest_callbacks.js");

function fakeChannel() {
  return {
    successHandler: null,
    errorHandler: null,
    flushTimeouts: [],
    _complete(token) {
      this.successHandler?.({ appendTokens: [token] });
    },
    appendRow(row, token) {
      this._complete(token);
    },
    appendRows(rows, token) {
      this._complete(token);
    },
    setSuccessHandler(handler) {
      this.successHandler = handler;
    },
    setErrorHandler(handler) {
      this.errorHandler = handler;
    },
    async waitForFlush(options) {
      this.flushTimeouts.push(options);
    },
  };
}

function fakeClient(channel) {
  const closes = [];
  return {
    channel,
    closes,
    getElasticChannel: async () => channel,
    close: async (options) => closes.push(options),
  };
}

test("AckInbox waitOne returns a token", async () => {
  const inbox = new callbacks.AckInbox();
  inbox.onSuccess({ appendTokens: ["t1"] });

  assert.equal(await inbox.waitOne(1_000), "t1");
});

test("AckInbox waitOne rejects on handler error", async () => {
  const inbox = new callbacks.AckInbox();
  inbox.onError({ error: new Error("boom") });

  await assert.rejects(() => inbox.waitOne(1_000), { message: /boom/ });
});

test("AckInbox times out without an event", async () => {
  const inbox = new callbacks.AckInbox();

  await assert.rejects(() => inbox.waitOne(10), { name: "TimeoutError" });
});

test("main closes the client", async () => {
  const channel = fakeChannel();
  const client = fakeClient(channel);

  await callbacks.main(async () => client);

  assert.deepEqual(client.closes, [{ waitForFlush: true, timeoutMs: 60_000 }]);
  assert.ok(channel.flushTimeouts.length);
});
