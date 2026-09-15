"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const { StreamingIngestError } = require("snowpipe-streaming");
const elastic = require("../elastic_production.js");
const named = require("../named_channel_checkpoint.js");
const support = elastic;

const error = (code, status) => new StreamingIngestError(code, "synthetic", status, String(status));
const deadline = () => performance.now() + 500;

function elasticClient(outcomes = []) {
  const calls = [];
  const closes = [];
  const channel = {
    appendRowWithWait(row, token) {
      calls.push(token);
      const outcome = outcomes.shift();
      if (outcome instanceof Error) return Promise.reject(outcome);
      return outcome || Promise.resolve();
    },
  };
  return { calls, closes, channel,
    getElasticChannel: async () => channel,
    close: async (options) => { closes.push(options); },
  };
}

test("Elastic appends before reading the next retained event", async () => {
  const client = elasticClient();
  const session = new elastic.ElasticProducer(async () => client);
  await session.open();
  class Source extends support.SampleEventSource {
    read() {
      assert.equal(client.calls.length, this.nextOffset - 1);
      return super.read();
    }
  }
  const source = new Source(3);
  await elastic.run(session, source);
  assert.equal(source.committed, 3);
  assert.deepEqual(client.calls, ["1", "2", "3"]);
});

test("caller polling timeout retains original promise; late success advances checkpoint", async () => {
  let resolve;
  const original = new Promise((done) => { resolve = done; });
  const client = elasticClient([original]);
  const session = new elastic.ElasticProducer(async () => client);
  await session.open();
  const source = new support.SampleEventSource(1);
  const pending = [elastic.appendEvent(session, source.read())];
  await elastic.collectProgress(session, pending, source, deadline());
  assert.equal(pending.length, 1);
  assert.equal(source.committed, 0);
  resolve();
  await new Promise((done) => setImmediate(done));
  await elastic.collectProgress(session, pending, source, deadline());
  assert.deepEqual(client.calls, ["1"]);
  assert.deepEqual(client.closes, []);
  assert.equal(session.generation, 1);
  assert.equal(source.committed, 1);
});

test("out-of-order success cannot acknowledge an earlier gap", async () => {
  const client = elasticClient([new Promise(() => {}), Promise.resolve()]);
  const session = new elastic.ElasticProducer(async () => client);
  await session.open();
  const source = new support.SampleEventSource(2);
  const pending = [elastic.appendEvent(session, source.read()), elastic.appendEvent(session, source.read())];
  await new Promise((done) => setImmediate(done));
  await elastic.collectProgress(session, pending, source, deadline());
  assert.equal(source.committed, 0);
  assert.equal(pending.length, 2);
  assert.deepEqual(client.closes, []);
});

test("intake continues beyond the old checkpoint while ack is pending", async () => {
  let resolve;
  const future = new Promise((done) => { resolve = done; });
  const client = elasticClient([future]);
  const producer = new elastic.ElasticProducer(async () => client);
  await producer.open();
  const source = new elastic.SampleEventSource(1005);
  const read = source.read.bind(source);
  source.read = () => {
    if (client.calls.length === 1005) {
      assert.equal(source.committed, 0);
      resolve();
    }
    return read();
  };
  await elastic.run(producer, source);
  assert.equal(source.committed, 1005);
});

test("429 retries the rejected event on the same client", async (context) => {
  context.mock.method(Math, "random", () => 0);
  const client = elasticClient([error("ReceiverSaturated", 429)]);
  const session = new elastic.ElasticProducer(async () => client);
  await session.open();
  const source = new support.SampleEventSource(1);
  await elastic.run(session, source);
  assert.deepEqual(client.calls, ["1", "1"]);
  assert.deepEqual(client.closes, []);
  assert.equal(source.committed, 1);
});

test("SDK invalidation rebuilds once for a failed generation and skips successful events", async (context) => {
  context.mock.method(Math, "random", () => 0);
  const old = elasticClient([Promise.resolve(), error("InvalidChannelError", 409), error("InvalidClientError", 409)]);
  const fresh = elasticClient();
  const clients = [old, fresh];
  const session = new elastic.ElasticProducer(async () => clients.shift());
  await session.open();
  const source = new support.SampleEventSource(3);
  const pending = Array.from({ length: 3 }, () => elastic.appendEvent(session, source.read()));
  await new Promise((done) => setImmediate(done));
  await elastic.collectProgress(session, pending, source, deadline());
  await new Promise((done) => setImmediate(done));
  await elastic.collectProgress(session, pending, source, deadline());
  assert.deepEqual(fresh.calls, ["2", "3"]);
  assert.equal(old.closes.length, 1);
  assert.equal(session.generation, 2);
  assert.equal(source.committed, 3);
});

for (const status of [400, 401, 403, 404]) {
  test(`permanent ${status} preserves retained source checkpoint`, async () => {
    const client = elasticClient([error("SfApiUserError", status)]);
    const session = new elastic.ElasticProducer(async () => client);
    await session.open();
    const source = new support.SampleEventSource(1);
    await assert.rejects(elastic.run(session, source), (failure) => failure.httpStatusCode === status);
    assert.equal(source.committed, 0);
    assert.deepEqual(client.calls, ["1"]);
  });
}

test("terminal SDK retry exhaustion stops with uncommitted source work", async (context) => {
  context.mock.method(Math, "random", () => 0);
  const client = elasticClient(Array.from({ length: support.MAX_ATTEMPTS }, () => error("HttpRetriesExhaustedError", 503)));
  const session = new elastic.ElasticProducer(async () => client);
  await session.open();
  const source = new support.SampleEventSource(1);
  await assert.rejects(elastic.run(session, source));
  assert.equal(client.calls.length, support.MAX_ATTEMPTS);
  assert.equal(source.committed, 0);
});

function namedSession(committed = 0) {
  const session = { committed, calls: [], recoveries: 0, polls: 0,
    open: async () => session.committed,
    recover: async () => { session.recoveries++; return session.committed; },
  };
  session.channel = {
    appendRow(row, token) {
      session.calls.push(Number(token));
      session.onAppend?.(Number(token));
      session.committed = Number(token);
    },
    async getChannelStatus() {
      session.polls++;
      return { statusCode: "SUCCESS", rowsErrorCount: 0, latestCommittedOffsetToken: String(session.committed) };
    },
  };
  return session;
}

test("named restart seeks strictly after server committed offset", async () => {
  const session = namedSession(2);
  const source = new support.SampleEventSource(5);
  await named.run(session, source);
  assert.deepEqual(session.calls, [3, 4, 5]);
  assert.equal(session.polls, 1);
  assert.equal(source.committed, 5);
});

test("named backpressure retains current event", async (context) => {
  context.mock.method(Math, "random", () => 0);
  const session = namedSession();
  const source = new support.SampleEventSource(3);
  session.onAppend = (offset) => {
    assert.equal(source.nextOffset, offset + 1);
    session.onAppend = null;
    throw error("ReceiverSaturated", 429);
  };
  await named.run(session, source);
  assert.deepEqual(session.calls, [1, 1, 2, 3]);
  assert.equal(session.recoveries, 0);
});

test("named invalidation resumes from server offset without resetting it", async (context) => {
  context.mock.method(Math, "random", () => 0);
  const session = namedSession();
  session.onAppend = (offset) => {
    if (offset === 3) {
      session.onAppend = null;
      throw error("InvalidChannelError", 409);
    }
  };
  const source = new support.SampleEventSource(4);
  await named.run(session, source);
  assert.deepEqual(session.calls, [1, 2, 3, 3, 4]);
  assert.equal(session.recoveries, 1);
  assert.equal(source.committed, 4);
});

test("named delayed status keeps intake paused without reopening", async (context) => {
  context.mock.method(Math, "random", () => 0);
  const session = namedSession();
  const source = new support.SampleEventSource(2);
  let polls = 0;
  session.channel.getChannelStatus = async () => {
    assert.equal(source.committed, 0);
    assert.equal(source.nextOffset, 3);
    return { statusCode: "SUCCESS", rowsErrorCount: 0, latestCommittedOffsetToken: ++polls > 1 ? "2" : "0" };
  };
  await named.run(session, source);
  assert.equal(session.recoveries, 0);
  assert.equal(source.committed, 2);
  assert.deepEqual(session.calls, [1, 2]);
});

test("named closed channel recovers from committed offset", async (context) => {
  context.mock.method(Math, "random", () => 0);
  const session = namedSession();
  session.onAppend = () => {
    session.onAppend = null;
    throw error("ClosedChannelError", 409);
  };
  const source = new support.SampleEventSource(2);
  await named.run(session, source);
  assert.deepEqual(session.calls, [1, 1, 2]);
  assert.equal(session.recoveries, 1);
  assert.equal(source.committed, 2);
});

test("named row errors and permanent failures prevent handoff", async () => {
  const session = namedSession();
  session.channel.getChannelStatus = async () => ({ statusCode: "SUCCESS", rowsErrorCount: 1, latestCommittedOffsetToken: "2" });
  const source = new support.SampleEventSource(2);
  await assert.rejects(named.run(session, source), /row errors/);
  assert.equal(source.committed, 0);
});

test("replay source regenerates stable payload and source checkpoint is explicit", () => {
  const source = new support.SampleEventSource(3);
  source.read();
  const event = source.read();
  assert.deepEqual(new support.SampleEventSource(3, 1).read(), event);
  assert.equal(source.committed, 0);
});


test("production examples require no sibling support module", () => {
  const fs = require("node:fs");
  const path = require("node:path");
  for (const file of ["elastic_production.js", "named_channel_checkpoint.js"]) {
    const code = fs.readFileSync(path.join(__dirname, "..", file), "utf8");
    assert.doesNotMatch(code, /require\(["']\.\//);
  }
});
