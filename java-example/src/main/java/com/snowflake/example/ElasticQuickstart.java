package com.snowflake.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Quickstart: Elastic Channel waitable appends.
 *
 * Elastic Channels are the recommended starting point for new Snowpipe Streaming
 * applications. They scale automatically across concurrent producers, require no
 * offset management, and deliver durable acknowledgements per batch.
 *
 * This example shows:
 *   - table-mode client construction (getElasticChannel)
 *   - success and error callback registration
 *   - batched waitable appends (appendRowsWithWait)
 *   - flush and close on shutdown
 *
 * A durable ack means Snowflake has persisted the rows — not that they are
 * immediately visible in a query.
 *
 * SDK requirement: snowpipe-streaming 1.8.0 or later.
 */
public class ElasticQuickstart {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PROFILE_PATH = "profile.json";

    // Replace with your Snowflake object names.
    private static final String DATABASE = env("SNOWFLAKE_DATABASE", "MY_DATABASE");
    private static final String SCHEMA   = env("SNOWFLAKE_SCHEMA", "MY_SCHEMA");
    private static final String TABLE    = env("SNOWFLAKE_TABLE", "MY_TABLE");

    public static void main(String[] args) {
        Properties props = loadProfile();

        try (SnowflakeStreamingIngestClient client =
                SnowflakeStreamingIngestClientFactory.tableBuilder(
                        "elastic-quickstart-client", DATABASE, SCHEMA, TABLE)
                        .setProperties(props)
                        .build()) {

            SnowflakeStreamingIngestElasticChannel channel = client.getElasticChannel();

            Map<String, Object> row = Map.of(
                    "DATA", Map.of("event_id", 1, "status", "active"),
                    "C1", 1,
                    "C2", "example");
            channel.appendRowWithWait(row, "batch-1").get();

            var status = channel.getChannelStatus();
            System.out.println("Durably acknowledged; status=" + status.getStatusCode()
                    + ", errors=" + status.getRowsErrorCount());

        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Ingestion failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Properties loadProfile() {
        String pat = System.getenv("SNOWFLAKE_PAT");
        if (pat != null && !pat.isBlank()) {
            Properties props = new Properties();
            props.put("authorization_type", "PAT");
            props.put("personal_access_token", pat);
            props.put("account", env("SNOWFLAKE_ACCOUNT", "PM"));
            props.put("url", env("SNOWFLAKE_URL", "https://PM.snowflakecomputing.com"));
            props.put("role", env("SNOWFLAKE_ROLE", "ACCOUNTADMIN"));
            return props;
        }
        try {
            Properties props = new Properties();
            JsonNode node = MAPPER.readTree(Files.readAllBytes(Paths.get(PROFILE_PATH)));
            node.fields().forEachRemaining(e -> props.put(e.getKey(), e.getValue().asText()));
            return props;
        } catch (IOException e) {
            System.err.println("Cannot read " + PROFILE_PATH + ": " + e.getMessage());
            System.exit(1);
            return null;
        }
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

}
