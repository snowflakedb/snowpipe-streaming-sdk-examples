package com.snowflake.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.ErrorDetail;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Elastic ingest: the four append APIs.
 *
 * <p>Elastic Channels need no channel name, offset token, or recovery config.
 * Create a table-mode client, get the channel, and append.
 *
 * <p>The SDK batches rows for transport. Waiting after every append is the slow
 * path. Pipelining single-row {@code appendRowWithWait} calls is the recommended
 * default for throughput and simplicity.
 *
 * <p>{@code appendRows} is optional: one Future and one append token for a
 * logical group when you already have a batch, or to cut call overhead. It does
 * not replace SDK transport batching.
 *
 * <p>Fire-and-forget {@code appendRow}/{@code appendRows} return no Future.
 * Success and error handlers are the only acknowledgement signal; pass your own
 * append token and the SDK echoes it back.
 */
public class ElasticChannelIngest {
    static final String DATABASE = env("SNOWFLAKE_DATABASE", "MY_DATABASE");
    static final String SCHEMA = env("SNOWFLAKE_SCHEMA", "MY_SCHEMA");
    static final String TABLE = env("SNOWFLAKE_TABLE", "MY_TABLE");
    static final String PROFILE = env("SNOWFLAKE_PROFILE", "profile.json");

    static final int PIPELINE_ROWS = 10;
    static final int BATCH_COUNT = 2;
    static final int BATCH_SIZE = 5;
    static final int FIRE_AND_FORGET_ROWS = 3;
    static final int FIRE_AND_FORGET_BATCH_SIZE = 4;
    static final int ACK_TIMEOUT_SECONDS = 60;

    @FunctionalInterface
    interface ClientFactory {
        SnowflakeStreamingIngestClient create() throws Exception;
    }

    /** Tests replace this to inject a fake client. */
    static ClientFactory clientFactory = ElasticChannelIngest::openClient;

    static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    static Properties connectionProperties() {
        return connectionProperties(System::getenv);
    }

    static Properties connectionProperties(Function<String, String> env) {
        String pat = env.apply("SNOWFLAKE_PAT");
        if (pat == null || pat.isBlank()) {
            return null;
        }
        String account = env.apply("SNOWFLAKE_ACCOUNT");
        String url = env.apply("SNOWFLAKE_URL");
        if (account == null || account.isBlank() || url == null || url.isBlank()) {
            throw new IllegalArgumentException(
                    "PAT authentication requires SNOWFLAKE_ACCOUNT and SNOWFLAKE_URL");
        }
        Properties properties = new Properties();
        properties.setProperty("authorization_type", "PAT");
        properties.setProperty("personal_access_token", pat);
        properties.setProperty("account", account);
        properties.setProperty("url", url);
        String role = env.apply("SNOWFLAKE_ROLE");
        if (role != null && !role.isBlank()) {
            properties.setProperty("role", role);
        }
        return properties;
    }

    static SnowflakeStreamingIngestClient createClient() throws Exception {
        return clientFactory.create();
    }

    static SnowflakeStreamingIngestClient openClient() throws Exception {
        Properties properties = connectionProperties();
        if (properties == null) {
            Properties profileProperties = new Properties();
            JsonNode profile = new ObjectMapper().readTree(Files.readAllBytes(Paths.get(PROFILE)));
            profile.fields().forEachRemaining(
                    entry -> profileProperties.put(entry.getKey(), entry.getValue().asText()));
            properties = profileProperties;
        }
        return SnowflakeStreamingIngestClientFactory.tableBuilder(
                "ingest-" + UUID.randomUUID(), DATABASE, SCHEMA, TABLE)
                .setProperties(properties)
                .build();
    }

    static Map<String, Object> sampleRow(int eventId) {
        return Map.of(
                "EVENT_ID", eventId,
                "C1", eventId,
                "C2", "event-" + eventId);
    }

    public static void main(String[] args) throws Exception {
        SnowflakeStreamingIngestClient client = createClient();
        try {
            // Elastic Channels belong to their client and are not closed separately.
            SnowflakeStreamingIngestElasticChannel channel = client.getElasticChannel();
            int nextId = 0;

            // 1. Wait per row — simplest call, and the slow path.
            channel.appendRowWithWait(sampleRow(nextId), "event-" + nextId)
                    .get(ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            nextId++;

            // 2. Recommended: submit every row before waiting. The SDK batches
            // these for transport.
            List<CompletableFuture<Void>> pending = new ArrayList<>();
            for (int eventId = nextId; eventId < nextId + PIPELINE_ROWS; eventId++) {
                pending.add(channel.appendRowWithWait(sampleRow(eventId), null));
            }
            for (CompletableFuture<Void> future : pending) {
                future.get(ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            nextId += PIPELINE_ROWS;

            // 3. Optional: application batches when you already have a group, or
            // want fewer Futures and tokens. Same pipelining; not required for
            // wire efficiency.
            pending = new ArrayList<>();
            for (int batchIndex = 0; batchIndex < BATCH_COUNT; batchIndex++) {
                List<Map<String, Object>> rows = new ArrayList<>();
                for (int eventId = nextId; eventId < nextId + BATCH_SIZE; eventId++) {
                    rows.add(sampleRow(eventId));
                }
                pending.add(channel.appendRowsWithWait(rows, "batch-" + batchIndex));
                nextId += BATCH_SIZE;
            }
            for (CompletableFuture<Void> future : pending) {
                future.get(ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }

            // 4. Fire-and-forget: no Future. Handlers are the only ack signal.
            // They run on the SDK ack thread — cheap bookkeeping only. The SDK
            // echoes your append token; it does not assign an offset.
            Map<Object, Long> submittedAt = new ConcurrentHashMap<>();
            ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<ErrorDetail> failures = new ConcurrentLinkedQueue<>();

            channel.setSuccessHandler(detail -> {
                long now = System.nanoTime();
                for (Object token : detail.getAppendTokens()) {
                    latencies.add(now - submittedAt.get(token));
                }
            });
            channel.setErrorHandler(failures::add);
            long started = System.nanoTime();
            int callbackRows = 0;
            for (int eventId = nextId; eventId < nextId + FIRE_AND_FORGET_ROWS; eventId++) {
                String token = "event-" + eventId;
                submittedAt.put(token, System.nanoTime());
                channel.appendRow(sampleRow(eventId), token);
            }
            nextId += FIRE_AND_FORGET_ROWS;
            callbackRows += FIRE_AND_FORGET_ROWS;
            List<Map<String, Object>> fireAndForgetBatch = new ArrayList<>();
            for (int eventId = nextId; eventId < nextId + FIRE_AND_FORGET_BATCH_SIZE; eventId++) {
                fireAndForgetBatch.add(sampleRow(eventId));
            }
            submittedAt.put("batch-ff", System.nanoTime());
            channel.appendRows(fireAndForgetBatch, "batch-ff");
            nextId += FIRE_AND_FORGET_BATCH_SIZE;
            callbackRows += FIRE_AND_FORGET_BATCH_SIZE;
            channel.waitForFlush(Duration.ofSeconds(ACK_TIMEOUT_SECONDS))
                    .get(ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            ErrorDetail failure = failures.peek();
            if (failure != null) {
                throw failure.getError();
            }
            double elapsed = (System.nanoTime() - started) / 1_000_000_000.0;

            System.out.println("Durably acknowledged " + nextId + " rows");
            if (!latencies.isEmpty() && elapsed > 0) {
                double sum = 0;
                int count = 0;
                for (long latencyNanos : latencies) {
                    sum += latencyNanos;
                    count++;
                }
                double avgAckMs = 1000.0 * (sum / count) / 1_000_000_000.0;
                double rps = callbackRows / elapsed;
                // Average ack latency can look large. Many appends are in flight, so
                // throughput is not 1 / latency — parallelism carries the rate.
                System.out.printf(
                        "Callback path: avg ack latency %.1f ms, %.0f rows/s%n", avgAckMs, rps);
            }
        } finally {
            client.close(true, Duration.ofSeconds(ACK_TIMEOUT_SECONDS))
                    .get(ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }
}
