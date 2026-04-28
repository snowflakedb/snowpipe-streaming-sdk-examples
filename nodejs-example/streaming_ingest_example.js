#!/usr/bin/env node
/**
 * Example demonstrating how to use the Snowflake Streaming Ingest SDK in
 * Node.js with the high-performance architecture and default pipe.
 *
 * The default pipe is automatically created by Snowflake when you first
 * open a channel. No CREATE PIPE DDL is required. The default pipe name
 * follows the convention: <TABLE_NAME>-STREAMING
 */

"use strict";

const crypto = require("node:crypto");
const { createClient } = require("snowpipe-streaming");

const MAX_ROWS = 100_000;

// Replace these with your Snowflake object names
const DATABASE = "MY_DATABASE";
const SCHEMA = "MY_SCHEMA";
const TABLE = "MY_TABLE";

// Default pipe: Snowflake auto-creates this on first channel open
const PIPE = `${TABLE}-STREAMING`;

async function main() {
  // Create Snowflake Streaming Ingest Client
  const client = await createClient({
    clientName: `MY_CLIENT_${crypto.randomUUID()}`,
    dbName: DATABASE,
    schemaName: SCHEMA,
    pipeName: PIPE,
    profilePath: "profile.json",
  });

  console.log("Client created successfully");

  try {
    // Open a channel for data ingestion
    const { channel } = await client.openChannel({
      name: `MY_CHANNEL_${crypto.randomUUID()}`,
    });

    console.log(`Channel opened: ${channel.channelName}`);

    try {
      // Ingest rows — column names must match the target table schema.
      // The default pipe uses MATCH_BY_COLUMN_NAME to map fields.
      console.log(`Ingesting ${MAX_ROWS} rows...`);
      for (let i = 1; i <= MAX_ROWS; i++) {
        const rowId = String(i);
        await channel.appendRow(
          {
            c1: i,
            c2: rowId,
            ts: new Date(),
          },
          rowId,
        );

        if (i % 10_000 === 0) {
          console.log(`Ingested ${i} rows...`);
        }
      }

      console.log("All rows submitted. Waiting for ingestion to complete...");

      // Wait for all rows to be committed
      const expectedToken = String(MAX_ROWS);
      await channel.waitForCommit(
        (token) => token !== null && token === expectedToken,
        { timeoutMs: 60_000 },
      );

      // Verify channel status
      const status = await channel.getChannelStatus();
      console.log(
        `Latest committed offset token: ${status.latestCommittedOffsetToken}`,
      );
      console.log("All data committed successfully");
    } finally {
      await channel.close();
    }

    console.log("Data ingestion completed");
  } finally {
    await client.close();
  }
}

main().catch((err) => {
  console.error("Error during data ingestion:", err.message);
  process.exitCode = 1;
});
