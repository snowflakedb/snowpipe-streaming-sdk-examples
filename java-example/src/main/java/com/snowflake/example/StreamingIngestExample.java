package com.snowflake.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Example demonstrating how to use the Snowflake Streaming Ingest SDK
 * with the high-performance architecture and default pipe.
 *
 * The default pipe is automatically created by Snowflake when you first
 * open a channel. No CREATE PIPE DDL is required. The default pipe name
 * follows the convention: {@code <TABLE_NAME>-STREAMING}
 */
public class StreamingIngestExample {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PROFILE_PATH = "profile.json";
    private static final int MAX_ROWS = 100_000;

    // Replace these with your Snowflake object names
    private static final String DATABASE = "MY_DATABASE";
    private static final String SCHEMA = "MY_SCHEMA";
    private static final String TABLE = "MY_TABLE";

    // Default pipe: Snowflake auto-creates this on first channel open
    private static final String PIPE = TABLE + "-STREAMING";

    public static void main(String[] args) {
        try {
            // Load properties from profile.json
            Properties props = new Properties();
            JsonNode jsonNode = MAPPER.readTree(Files.readAllBytes(Paths.get(PROFILE_PATH)));
            jsonNode.fields().forEachRemaining(entry ->
                props.put(entry.getKey(), entry.getValue().asText()));

            // Create Snowflake Streaming Ingest Client using try-with-resources
            try (SnowflakeStreamingIngestClient client = SnowflakeStreamingIngestClientFactory.builder(
                    "MY_CLIENT_" + UUID.randomUUID(),
                    DATABASE,
                    SCHEMA,
                    PIPE)
                    .setProperties(props)
                    .build()) {

                System.out.println("Client created successfully");

                // Open a channel for data ingestion using try-with-resources
                try (SnowflakeStreamingIngestChannel channel = client.openChannel(
                        "MY_CHANNEL_" + UUID.randomUUID(), "0").getChannel()) {

                    System.out.println("Channel opened: " + channel.getChannelName());
                    System.out.println("Ingesting " + MAX_ROWS + " rows...");

                    // Ingest rows — column names must match the target table schema.
                    // The default pipe uses MATCH_BY_COLUMN_NAME to map fields.
                    for (int i = 1; i <= MAX_ROWS; i++) {
                        String rowId = String.valueOf(i);
                        Map<String, Object> row = Map.of(
                            "c1", i,
                            "c2", rowId,
                            "ts", Instant.now().toEpochMilli() / 1000.0
                        );
                        channel.appendRow(row, rowId);

                        if (i % 10_000 == 0) {
                            System.out.println("Ingested " + i + " rows...");
                        }
                    }

                    System.out.println("All rows submitted. Waiting for commit...");

                    // Wait for all rows to be committed using waitForCommit.
                    // The predicate receives the latest committed offset token
                    // (String) and should return true when satisfied.
                    channel.waitForCommit(
                        token -> token != null && Long.parseLong(token) >= MAX_ROWS,
                        Duration.ofSeconds(30)
                    ).get();

                    // Now that data has landed, check the channel status
                    ChannelStatus status = channel.getChannelStatus();
                    System.out.println("All data committed. Channel status:");
                    System.out.println("  Committed offset:   " + status.getLatestOffsetToken());
                    System.out.println("  Rows inserted:      " + status.getRowsInsertedCount());
                    System.out.println("  Rows errored:       " + status.getRowsErrorCount());
                    System.out.println("  Avg server latency: " + status.getServerAvgProcessingLatency());
                    if (status.getRowsErrorCount() > 0) {
                        System.out.println("  Last error:         " + status.getLastErrorMessage());
                    }
                } // Channel automatically closed here

                System.out.println("Data ingestion completed");
            } // Client automatically closed here

        } catch (IOException | InterruptedException | ExecutionException e) {
            System.err.println("Error during data ingestion: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

