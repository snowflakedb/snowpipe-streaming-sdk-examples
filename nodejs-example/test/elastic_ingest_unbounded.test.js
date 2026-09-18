"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const unbounded = require("../elastic_ingest_unbounded.js");

function snapshotEnv(keys) {
  const previous = Object.fromEntries(keys.map((key) => [key, process.env[key]]));
  return () => {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  };
}

function fakeChannel() {
  return {
    calls: 0,
    appendRowWithWait(row, token) {
      this.calls += 1;
      return Promise.resolve();
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

test("main closes the client", async () => {
  const channel = fakeChannel();
  const client = fakeClient(channel);
  const restore = snapshotEnv(["SNOWFLAKE_TEST_ROWS"]);
  process.env.SNOWFLAKE_TEST_ROWS = "4";
  try {
    await unbounded.main(async () => client);
  } finally {
    restore();
  }

  assert.equal(channel.calls, 4);
  assert.deepEqual(client.closes, [{ waitForFlush: true, timeoutMs: 60_000 }]);
});

test("main closes after interrupt", async () => {
  const channel = fakeChannel();
  const client = fakeClient(channel);
  channel.appendRowWithWait = (row, token) => {
    channel.calls += 1;
    if (channel.calls === 3) {
      throw new DOMException("This operation was aborted", "AbortError");
    }
    return Promise.resolve();
  };
  const restore = snapshotEnv(["SNOWFLAKE_TEST_ROWS"]);
  process.env.SNOWFLAKE_TEST_ROWS = "10";
  try {
    await unbounded.main(async () => client);
  } finally {
    restore();
  }

  assert.equal(channel.calls, 3);
  assert.deepEqual(client.closes, [{ waitForFlush: true, timeoutMs: 60_000 }]);
});
