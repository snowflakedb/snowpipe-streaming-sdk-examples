"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const ingest = require("../elastic_ingest.js");

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
    calls: [],
    successHandler: null,
    errorHandler: null,
    flushTimeouts: [],
    appendRowWithWait(row, token) {
      this.calls.push(["appendRowWithWait", row, token]);
      return Promise.resolve();
    },
    appendRowsWithWait(rows, token) {
      this.calls.push(["appendRowsWithWait", [...rows], token]);
      return Promise.resolve();
    },
    appendRow(row, token) {
      this.calls.push(["appendRow", row, token]);
      this.successHandler?.({ appendTokens: [token] });
    },
    appendRows(rows, token) {
      this.calls.push(["appendRows", [...rows], token]);
      this.successHandler?.({ appendTokens: [token] });
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

test("connectionProperties uses profile without PAT", () => {
  const restore = snapshotEnv(["SNOWFLAKE_PAT"]);
  delete process.env.SNOWFLAKE_PAT;
  try {
    assert.equal(ingest.connectionProperties(), null);
  } finally {
    restore();
  }
});

test("connectionProperties builds PAT settings", () => {
  const restore = snapshotEnv([
    "SNOWFLAKE_PAT",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_URL",
    "SNOWFLAKE_ROLE",
  ]);
  process.env.SNOWFLAKE_PAT = "token";
  process.env.SNOWFLAKE_ACCOUNT = "account";
  process.env.SNOWFLAKE_URL = "https://account.snowflakecomputing.com";
  process.env.SNOWFLAKE_ROLE = "role";
  try {
    assert.deepEqual(ingest.connectionProperties(), {
      authorization_type: "PAT",
      personal_access_token: "token",
      account: "account",
      url: "https://account.snowflakecomputing.com",
      role: "role",
    });
  } finally {
    restore();
  }
});

test("connectionProperties requires account and url", () => {
  const restore = snapshotEnv(["SNOWFLAKE_PAT", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_URL"]);
  process.env.SNOWFLAKE_PAT = "token";
  delete process.env.SNOWFLAKE_ACCOUNT;
  delete process.env.SNOWFLAKE_URL;
  try {
    assert.throws(() => ingest.connectionProperties(), /SNOWFLAKE_ACCOUNT/);
  } finally {
    restore();
  }
});

test("sampleRow uses a stable event id", () => {
  const row = ingest.sampleRow(7);
  assert.equal(row.EVENT_ID, 7);
  assert.equal(row.C1, 7);
  assert.equal(row.C2, "event-7");
});

test("main closes the client", async () => {
  const channel = fakeChannel();
  const client = fakeClient(channel);

  await ingest.main(async () => client);

  assert.deepEqual(client.closes, [{ waitForFlush: true, timeoutMs: 60_000 }]);
  assert.ok(channel.flushTimeouts.length);
});
