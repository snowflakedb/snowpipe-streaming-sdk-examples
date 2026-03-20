package com.snowflake.streaming.producer;

import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Simple direct writer that pushes fake data into Snowpipe Streaming over REST,
 * bypassing Kafka entirely. Runs a single-threaded loop for a hardcoded duration.
 *
 * Run via:
 *   mvn exec:java -Dexec.mainClass="com.snowflake.streaming.producer.DirectStreamWriter"
 */
public class DirectStreamWriter {

    private static final Logger logger = LoggerFactory.getLogger(DirectStreamWriter.class);

    // ---- Snowflake connection ----
    private static final String SF_URL = "SFENGINEERING-SFCTEST0_AWS_US_WEST_2.snowflakecomputing.com";
    private static final String SF_ACCOUNT = "SFCTEST0_AWS_US_WEST_2";
    private static final String SF_USER = "snowpipe_sdk_gh_action";
    private static final String SF_PRIVATE_KEY = "";

    // ---- Snowflake target ----
    private static final String DATABASE = "TEST_DB";
    private static final String SCHEMA = "PUBLIC";
    private static final String TABLE = "TEST_TABLE";
    private static final String PIPE = "TEST_PIPE";
    private static final String CLIENT_NAME = "DIRECT_STREAM_CLIENT";
    private static final String CHANNEL_NAME = "DIRECT_CHANNEL";

    // ---- Writer settings ----
    private static final int BATCH_SIZE = 1000;
    private static final int DURATION_SECONDS = 60;
    private static final int REPORT_INTERVAL_SECONDS = 5;

    public static void main(String[] args) throws Exception {
        new DirectStreamWriter().run();
    }

    public void run() throws Exception {
        logger.info("=== DirectStreamWriter ===");
        logger.info("Target: {}.{}.{}", DATABASE, SCHEMA, TABLE);
        logger.info("Batch size: {}, Duration: {}s", BATCH_SIZE, DURATION_SECONDS);

        Properties sfProps = new Properties();
        sfProps.setProperty("url", SF_URL);
        sfProps.setProperty("account", SF_ACCOUNT);
        sfProps.setProperty("user", SF_USER);
        sfProps.setProperty("private_key", SF_PRIVATE_KEY);

        SnowflakeStreamingIngestClient client = SnowflakeStreamingIngestClientFactory
                .builder(CLIENT_NAME, DATABASE, SCHEMA, PIPE)
                .setProperties(sfProps)
                .build();

        OpenChannelResult result = client.openChannel(CHANNEL_NAME);
        SnowflakeStreamingIngestChannel channel = result.getChannel();
        logger.info("Opened channel: {}", CHANNEL_NAME);

        Instant startTime = Instant.now();
        Instant deadline = startTime.plusSeconds(DURATION_SECONDS);
        long sent = 0;
        long errors = 0;
        long lastReportTime = System.currentTimeMillis();
        long lastReportSent = 0;

        while (Instant.now().isBefore(deadline)) {
            List<Map<String, Object>> rows = new ArrayList<>(BATCH_SIZE);
            for (int i = 0; i < BATCH_SIZE; i++) {
                rows.add(generateRow(sent + i));
            }

            try {
                channel.appendRows(rows, null, null);
                sent += BATCH_SIZE;
            } catch (Exception e) {
                errors++;
                logger.warn("Append failed: {}", e.getMessage());
            }

            long now = System.currentTimeMillis();
            if (now - lastReportTime >= REPORT_INTERVAL_SECONDS * 1000L) {
                long delta = sent - lastReportSent;
                double intervalSec = (now - lastReportTime) / 1000.0;
                double rps = delta / Math.max(intervalSec, 0.001);
                logger.info("[progress] sent={}, errors={}, throughput={} rec/sec",
                        sent, errors, String.format("%.0f", rps));
                lastReportTime = now;
                lastReportSent = sent;
            }
        }

        Duration elapsed = Duration.between(startTime, Instant.now());
        double overallRps = sent * 1000.0 / Math.max(elapsed.toMillis(), 1);

        logger.info("=== Complete ===");
        logger.info("Sent {} records in {}s ({} errors)", sent, elapsed.getSeconds(), errors);
        logger.info("Throughput: {} rec/sec", String.format("%.0f", overallRps));

        try {
            channel.close(true, Duration.ofSeconds(30));
        } catch (Exception e) {
            logger.warn("Error closing channel: {}", e.getMessage());
        }

        try {
            client.close();
        } catch (Exception e) {
            logger.warn("Error closing client: {}", e.getMessage());
        }
    }

    private static Map<String, Object> generateRow(long seq) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("C1", seq);
        row.put("TS", Instant.now().toString());
        return row;
    }
}
