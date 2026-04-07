package com.snowflake.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
    private static final int POLL_ATTEMPTS = 30;
    private static final long POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);

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

                    System.out.println("All rows submitted. Waiting for ingestion to complete...");

                    // Wait for ingestion to complete
                    for (int attempt = 1; attempt <= POLL_ATTEMPTS; attempt++) {
                        String latestOffset = channel.getChannelStatus().getLatestOffsetToken();
                        System.out.println("Latest offset token: " + latestOffset);

                        if (latestOffset != null && Integer.parseInt(latestOffset) >= MAX_ROWS) {
                            System.out.println("All data committed successfully");
                            break;
                        }

                        if (attempt == POLL_ATTEMPTS) {
                            throw new RuntimeException("Ingestion failed after all attempts");
                        }

                        Thread.sleep(POLL_INTERVAL_MS);
                    }
                } // Channel automatically closed here

                System.out.println("Data ingestion completed");
            } // Client automatically closed here

        } catch (IOException | InterruptedException e) {
            System.err.println("Error during data ingestion: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

