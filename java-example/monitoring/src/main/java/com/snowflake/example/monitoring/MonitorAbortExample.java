package com.snowflake.example.monitoring;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.UUID;

/**
 * Demonstrates monitoring Snowpipe Streaming channel status in real-time
 * and automatically halting production when server-side errors are detected.
 *
 * <p>This example mirrors the Python monitor_abort_example.py and uses the
 * Snowpipe Streaming high-performance architecture API:
 * <ul>
 *   <li>{@code appendRow()} - fire-and-forget row append (server-side validation)</li>
 *   <li>{@code getChannelStatus()} - rich status with error counts, latency, committed offset</li>
 * </ul>
 *
 * <p>Configuration flags at the top of this class control behavior:
 * <ul>
 *   <li>{@code TEST_ERROR_OFFSET = -1} disables error injection</li>
 *   <li>{@code TEST_ERROR_OFFSET = 200} injects a schema error at offset 200 to trigger abort</li>
 * </ul>
 */
public class MonitorAbortExample {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PROFILE_PATH = "profile.json";

    // Snowflake configuration
    private static final String DATABASE = "MY_DATABASE";
    private static final String SCHEMA = "MY_SCHEMA";
    private static final String TABLE = "MY_TABLE";
    private static final String PIPE = TABLE + "-STREAMING";
    private static final String CHANNEL_NAME = "monitor_abort_channel";

    // Tuning knobs
    private static final int TOTAL_ROWS = 500;
    private static final int SEND_INTERVAL_MS = 100;
    private static final int POLL_INTERVAL_MS = 500;
    private static final int TEST_ERROR_OFFSET = -1; // -1 to disable error injection

    public static void main(String[] args) {
        try {
            // Load properties from profile.json
            Properties props = new Properties();
            JsonNode jsonNode = MAPPER.readTree(Files.readAllBytes(Paths.get(PROFILE_PATH)));
            jsonNode.fields().forEachRemaining(entry ->
                    props.put(entry.getKey(), entry.getValue().asText()));

            try (SnowflakeStreamingIngestClient client = SnowflakeStreamingIngestClientFactory.builder(
                    "MY_CLIENT_" + UUID.randomUUID(),
                    DATABASE,
                    SCHEMA,
                    PIPE)
                    .setProperties(props)
                    .build()) {

                System.out.println("Client created successfully");

                // Open a channel
                SnowflakeStreamingIngestChannel channel =
                        client.openChannel(CHANNEL_NAME, "0").getChannel();
                System.out.println("Channel opened: " + channel.getChannelName());

                // If channel already has committed state, drop and reopen for a fresh run
                ChannelStatus initialStatus = channel.getChannelStatus();
                if (initialStatus.getLatestOffsetToken() != null) {
                    System.out.println("Existing channel state detected; dropping and reopening...");
                    channel.close();
                    client.dropChannel(CHANNEL_NAME);
                    Thread.sleep(500);
                    channel = client.openChannel(CHANNEL_NAME, "0").getChannel();
                    System.out.println("Reopened channel: " + channel.getChannelName());
                    Thread.sleep(500);
                }

                FakeDataGenerator generator = new FakeDataGenerator();
                OffsetTracker tracker = new OffsetTracker();
                long lastPollTime = 0;
                long previousErrorCount = 0;
                boolean firstPoll = true;

                // ---- Producer loop ----
                for (int i = 1; i <= TOTAL_ROWS; i++) {
                    Map<String, Object> row = generator.makeRow(i);

                    // Error injection
                    if (i == TEST_ERROR_OFFSET) {
                        row.put("user_id", "BAD_ID");             // should be INTEGER
                        row.put("date_of_birth", "INVALID_DATE"); // should be DATE
                        row.put("registration_date", "INVALID_TS"); // should be TIMESTAMP_NTZ
                        System.out.println("[producer] Injecting schema error at offset=" + i);
                    }

                    channel.appendRow(row, String.valueOf(i));
                    tracker.updateSent(i);

                    if (i % 10 == 0) {
                        System.out.println("[producer] sent offset=" + i);
                    }

                    // ---- Monitor loop ----
                    long now = System.currentTimeMillis();
                    if (now - lastPollTime >= POLL_INTERVAL_MS) {
                        ChannelStatus status = channel.getChannelStatus();
                        boolean shouldAbort = monitorChannelStatus(
                                status, tracker, previousErrorCount, firstPoll);

                        long currentErrorCount = status.getRowsErrorCount();
                        if (shouldAbort) {
                            break;
                        }
                        previousErrorCount = currentErrorCount;
                        firstPoll = false;
                        lastPollTime = now;
                    }

                    Thread.sleep(SEND_INTERVAL_MS);
                }

                // Wait for commits to catch up
                System.out.println("Waiting for commits to catch up to " + tracker.getSentHighestOffset());
                int maxAttempts = 30;
                for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                    ChannelStatus status = channel.getChannelStatus();
                    String latestOffset = status.getLatestOffsetToken();
                    System.out.println("Latest committed offset: " + latestOffset);

                    try {
                        if (latestOffset != null
                                && Long.parseLong(latestOffset) >= tracker.getSentHighestOffset()) {
                            System.out.println("All data committed successfully.");
                            break;
                        }
                    } catch (NumberFormatException e) {
                        System.out.println("Warning: unexpected offset token format: " + latestOffset);
                    }
                    if (attempt == maxAttempts) {
                        System.out.println("Warning: Timed out waiting for commits.");
                    }
                    Thread.sleep(1000);
                }

                System.out.println("Final committed: "
                        + channel.getChannelStatus().getLatestOffsetToken());

                channel.close();
                // Drop channel for demo cleanup. In production, you typically reopen
                // existing channels to resume from the last committed offset.
                client.dropChannel(CHANNEL_NAME);

            } // client auto-closed

            System.out.println("Monitor-abort demo completed.");

        } catch (IOException | InterruptedException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Log channel status, compute lag, and detect error-count increases.
     * Returns true if production should be aborted.
     */
    private static boolean monitorChannelStatus(
            ChannelStatus status,
            OffsetTracker tracker,
            long previousErrorCount,
            boolean firstPoll) {

        String committedToken = status.getLatestOffsetToken();
        OptionalLong lag = tracker.computeLag(committedToken);

        System.out.println();
        System.out.println("[monitor] committed= " + committedToken);
        System.out.println("[monitor] rows_inserted= " + status.getRowsInsertedCount());
        System.out.println("[monitor] rows_error_count= " + status.getRowsErrorCount());
        System.out.println("[monitor] server_avg_processing_latency= "
                + status.getServerAvgProcessingLatency());
        System.out.println("[monitor] last_error_offset_token_upper_bound= "
                + status.getLastErrorOffsetTokenUpperBound());
        System.out.println("[monitor] last_error_message= " + status.getLastErrorMessage());
        System.out.println("[monitor] offset_lag= " + (lag.isPresent() ? lag.getAsLong() : "N/A"));
        System.out.println();

        if (firstPoll) {
            return false;
        }

        long currentErrorCount = status.getRowsErrorCount();
        if (currentErrorCount > previousErrorCount) {
            System.out.println("[monitor] Error count increased (" + previousErrorCount
                    + " -> " + currentErrorCount + ") - halting production.");
            System.out.println("Monitor requested to halt production - current offset not yet sent= "
                    + (tracker.getSentHighestOffset() + 1));
            return true;
        }
        return false;
    }
}
