"use strict";

const { randomUUID } = require("node:crypto");
const { createTableClient } = require("snowpipe-streaming");

// Replace these with your target table. Authentication lives in profile.json.
const DATABASE = "MY_DATABASE";
const SCHEMA = "MY_SCHEMA";
const TABLE = "MY_TABLE";

// Load the profile, submit sample rows, await acknowledgements, and close.
async function main() {
  // Table clients use the default streaming pipe; no CREATE PIPE is needed.
  const client = await createTableClient({
    clientName: `quickstart-${randomUUID()}`,
    dbName: DATABASE,
    schemaName: SCHEMA,
    tableName: TABLE,
    profilePath: "profile.json",
  });
  let complete = false;
  try {
    // Snowflake manages the Elastic channel; no application channel name is needed.
    const channel = await client.getElasticChannel();
    const pending = [];
    for (let eventId = 1; eventId <= 10; eventId++) {
      // Replace sample values with source data; keys match target columns.
      const row = { C1: eventId, C2: String(eventId) };
      // Attach rejection handlers immediately while submitting without per-row waits.
      // null disables callback token reporting, not this Promise's acknowledgement.
      pending.push(channel.appendRowWithWait(row, null)
        .then(() => null, (error) => error));
    }
    // The SDK batches and retries transport. Slow acknowledgements are not failures.
    const errors = await Promise.all(pending);
    if (errors.some(Boolean)) throw errors.find(Boolean);
    complete = true;
    console.log("Durably acknowledged 10 rows. Check table contents separately.");
  } finally {
    // Always release resources; keep unconfirmed source events recoverable on failure.
    await client.close({ waitForFlush: complete, timeoutMs: 30_000 });
  }
}

// Native SDK Promises alone may not keep Node's event loop alive during ingestion.
const keepAlive = setInterval(() => {}, 1000);
main().catch((error) => { console.error(error); process.exitCode = 1; })
  .finally(() => clearInterval(keepAlive));
