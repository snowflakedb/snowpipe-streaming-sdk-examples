#!/usr/bin/env node
/**
 * Elastic Channels quickstart for Snowpipe Streaming (Node.js).
 *
 * Elastic Channels are the recommended starting point for most applications.
 * Snowflake manages scaling and channel lifecycle; delivery is at-least-once
 * and unordered. For strict exactly-once ingestion with explicit source-offset
 * tracking, see named_channel_checkpoint.js.
 *
 * Requirements: snowpipe-streaming >= 1.8.0, Node.js >= 20
 */

"use strict";

const { createTableClient } = require("snowpipe-streaming");

// Replace these with your Snowflake object names.
const DATABASE = process.env.SNOWFLAKE_DATABASE || "MY_DATABASE";
const SCHEMA = process.env.SNOWFLAKE_SCHEMA || "MY_SCHEMA";
const TABLE = process.env.SNOWFLAKE_TABLE || "MY_TABLE";

async function main() {
  const client = await createTableClient({
    clientName: "elastic-quickstart",
    dbName: DATABASE,
    schemaName: SCHEMA,
    tableName: TABLE,
    ...(process.env.SNOWFLAKE_PAT
      ? {
          properties: {
            authorization_type: "PAT",
            personal_access_token: process.env.SNOWFLAKE_PAT,
            account: process.env.SNOWFLAKE_ACCOUNT || "PM",
            url: process.env.SNOWFLAKE_URL || "https://PM.snowflakecomputing.com",
            role: process.env.SNOWFLAKE_ROLE || "ACCOUNTADMIN",
          },
        }
      : { profilePath: "profile.json" }),
  });

  try {
    const channel = await client.getElasticChannel();

    const row = {
      DATA: { event_id: 1, status: "active" },
      C1: 1,
      C2: "example",
    };
    await channel.appendRowWithWait(row, "batch-1");

    const status = await channel.getChannelStatus();
    console.log(`Durably acknowledged; status=${status.statusCode}, errors=${status.rowsErrorCount}`);
  } finally {
    await client.close({ waitForFlush: true, timeoutMs: 30_000 });
  }
}

// Only run when executed directly, so this file can be required without
// opening a client.
if (require.main === module) {
  const keepAlive = setInterval(() => {}, 1_000);
  main()
    .catch((err) => {
      console.error("Fatal:", err.message);
      process.exitCode = 1;
    })
    .finally(() => clearInterval(keepAlive));
}

module.exports = { main };
