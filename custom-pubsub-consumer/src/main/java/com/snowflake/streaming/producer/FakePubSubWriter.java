package com.snowflake.streaming.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Interactive CLI tool for pushing dummy mobile operator call records (CDRs) to GCP Pub/Sub.
 *
 * <p>Uses the same CDR data model as the Kafka example's FakeKafkaWriter for consistency
 * across examples.</p>
 *
 * <p>Run via:</p>
 * <pre>{@code
 * mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.producer.FakePubSubWriter"
 * }</pre>
 */
public class FakePubSubWriter {

    private static final Logger logger = LoggerFactory.getLogger(FakePubSubWriter.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final AtomicLong sequence = new AtomicLong(0);
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String[] CALL_TYPES = {"VOICE", "SMS", "DATA", "MMS", "VOICEMAIL"};
    private static final String[] NETWORKS = {"4G", "5G", "3G", "WIFI", "ROAMING"};
    private static final String[] DISPOSITIONS = {"ANSWERED", "NO_ANSWER", "BUSY", "FAILED", "DROPPED"};
    private static final String[] AREA_CODES = {"212", "310", "415", "512", "617", "702", "808", "904", "303", "206"};
    private static final String[] CELL_TOWERS = {
            "LAX-001", "LAX-002", "NYC-010", "NYC-011", "SFO-005", "SFO-006",
            "AUS-003", "SEA-007", "DEN-004", "MIA-008", "BOS-009", "LAS-012"
    };
    private static final String[] PLANS = {"BASIC", "STANDARD", "PREMIUM", "UNLIMITED", "PREPAID"};

    private final Publisher publisher;
    private final String topicId;
    private final int defaultBurstCount;
    private final int defaultStreamRps;

    public FakePubSubWriter(Config config) throws Exception {
        this.topicId = config.getPubSubTopicId();
        this.defaultBurstCount = config.getDefaultBurstCount();
        this.defaultStreamRps = config.getDefaultStreamRps();

        TopicName topicName = TopicName.of(config.getGcpProjectId(), topicId);
        this.publisher = Publisher.newBuilder(topicName).build();
    }

    public static void main(String[] args) throws Exception {
        String configPath = System.getProperty("config.path", "producer-config.properties");
        Config config = new Config(configPath);
        new FakePubSubWriter(config).runInteractive();
    }

    public void runInteractive() {
        Scanner scanner = new Scanner(System.in);
        printMenu();

        while (true) {
            System.out.print("\n> ");
            String line = scanner.nextLine().trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split("\\s+", 2);
            String cmd = parts[0].toLowerCase();

            try {
                switch (cmd) {
                    case "1":
                    case "single":
                        sendSingle();
                        break;
                    case "2":
                    case "burst":
                        int count = parts.length > 1 ? Integer.parseInt(parts[1]) : defaultBurstCount;
                        sendBurst(count);
                        break;
                    case "3":
                    case "stream":
                        int rps = parts.length > 1 ? Integer.parseInt(parts[1]) : defaultStreamRps;
                        sendContinuousStream(rps, scanner);
                        break;
                    case "4":
                    case "malformed":
                        sendMalformedMessages();
                        break;
                    case "5":
                    case "nulls":
                        sendNullFields();
                        break;
                    case "6":
                    case "custom":
                        sendCustomMessage(parts.length > 1 ? parts[1] : null, scanner);
                        break;
                    case "menu":
                    case "help":
                        printMenu();
                        break;
                    case "quit":
                    case "exit":
                    case "q":
                        System.out.println("Shutting down...");
                        shutdown();
                        return;
                    default:
                        System.out.println("Unknown command. Type 'menu' for options.");
                }
            } catch (Exception e) {
                // Unwrap ExecutionException to get the actual cause
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                if (cause instanceof ApiException) {
                    // GCP config/auth errors — print the clean message, no stack trace
                    System.out.println("Error: " + cause.getMessage());
                    System.out.println("Hint: check gcp.project.id and pubsub.topic.id in producer-config.properties");
                } else {
                    System.out.println("Error: " + e.getMessage());
                    logger.error("Command failed", e);
                }
            }
        }
    }

    private void printMenu() {
        System.out.println();
        System.out.println("=== Fake Pub/Sub Writer — Mobile CDR Generator ===");
        System.out.println("  Topic: " + topicId);
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  1 | single             Send one call record");
        System.out.printf("  2 | burst [count]      Send a burst of CDRs (default %d)%n", defaultBurstCount);
        System.out.printf("  3 | stream [rps]       Continuous CDR stream (default %d/sec, Enter to stop)%n", defaultStreamRps);
        System.out.println("  4 | malformed          Send malformed JSON messages");
        System.out.println("  5 | nulls              Send CDRs with null / missing fields");
        System.out.println("  6 | custom [json]      Send a custom JSON payload");
        System.out.println("  menu                   Show this menu");
        System.out.println("  quit                   Exit");
    }

    private void sendSingle() throws Exception {
        long seq = sequence.incrementAndGet();
        Map<String, Object> cdr = generateCDR(seq);
        publish(mapper.writeValueAsString(cdr));
        System.out.println("Sent 1 CDR (seq=" + seq + ", type=" + cdr.get("call_type") + ")");
    }

    private void sendBurst(int count) throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            long seq = sequence.incrementAndGet();
            Map<String, Object> cdr = generateCDR(seq);
            publish(mapper.writeValueAsString(cdr));
        }
        long elapsed = System.currentTimeMillis() - start;
        System.out.printf("Sent %d CDRs in %d ms (%.0f rec/sec)%n",
                count, elapsed, count * 1000.0 / Math.max(elapsed, 1));
    }

    private void sendContinuousStream(int rps, Scanner scanner) throws Exception {
        long intervalMs = 1000 / Math.max(rps, 1);
        AtomicBoolean streaming = new AtomicBoolean(true);
        AtomicLong sent = new AtomicLong(0);

        Thread streamThread = new Thread(() -> {
            try {
                while (streaming.get()) {
                    long seq = sequence.incrementAndGet();
                    Map<String, Object> cdr = generateCDR(seq);
                    publish(mapper.writeValueAsString(cdr));
                    sent.incrementAndGet();
                    Thread.sleep(intervalMs);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error("Stream error", e);
            }
        }, "pubsub-stream-writer");
        streamThread.setDaemon(true);
        streamThread.start();

        System.out.println("Streaming CDRs at ~" + rps + " rec/sec. Press Enter to stop...");
        scanner.nextLine();
        streaming.set(false);
        streamThread.join(2000);
        System.out.println("Stopped. Sent " + sent.get() + " CDRs.");
    }

    private void sendMalformedMessages() throws Exception {
        publish("not json at all");
        publish("{broken json: ");
        publish("");
        publish("null");
        publish("[1, 2, 3]");
        System.out.println("Sent 5 malformed messages");
    }

    private void sendNullFields() throws Exception {
        long seq = sequence.incrementAndGet();

        Map<String, Object> missingCallee = new LinkedHashMap<>();
        missingCallee.put("record_id", seq);
        missingCallee.put("caller_number", randomPhone());
        publish(mapper.writeValueAsString(missingCallee));

        Map<String, Object> nullDuration = new LinkedHashMap<>();
        nullDuration.put("record_id", seq + 1);
        nullDuration.put("caller_number", randomPhone());
        nullDuration.put("callee_number", randomPhone());
        nullDuration.put("call_type", "VOICE");
        nullDuration.put("duration_seconds", null);
        publish(mapper.writeValueAsString(nullDuration));

        Map<String, Object> allNulls = new LinkedHashMap<>();
        allNulls.put("record_id", null);
        allNulls.put("caller_number", null);
        allNulls.put("callee_number", null);
        allNulls.put("call_type", null);
        publish(mapper.writeValueAsString(allNulls));

        Map<String, Object> empty = new LinkedHashMap<>();
        publish(mapper.writeValueAsString(empty));

        System.out.println("Sent 4 CDRs with null/missing fields");
    }

    private void sendCustomMessage(String json, Scanner scanner) throws Exception {
        if (json == null || json.isEmpty()) {
            System.out.print("Enter JSON payload: ");
            json = scanner.nextLine().trim();
        }
        publish(json);
        System.out.println("Sent custom message");
    }

    private void publish(String data) throws Exception {
        PubsubMessage message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(data))
                .build();
        ApiFuture<String> future = publisher.publish(message);
        String messageId = future.get();
        logger.info("Published message ID={}, size={}B", messageId, data.length());
    }

    private void shutdown() {
        try {
            publisher.shutdown();
            publisher.awaitTermination(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.warn("Error shutting down publisher", e);
        }
    }

    // ---- CDR generation (same schema as Kafka example) ----

    private static Map<String, Object> generateCDR(long seq) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        String callType = pick(CALL_TYPES);
        String disposition = pick(DISPOSITIONS);

        int durationSec = 0;
        double dataUsageMb = 0.0;
        if ("VOICE".equals(callType) || "VOICEMAIL".equals(callType)) {
            durationSec = "ANSWERED".equals(disposition) ? rng.nextInt(5, 3600) : 0;
        } else if ("DATA".equals(callType)) {
            dataUsageMb = Math.round(rng.nextDouble(0.01, 500.0) * 100.0) / 100.0;
        }

        double chargeAmount = Math.round(rng.nextDouble(0.0, 15.0) * 100.0) / 100.0;
        LocalDateTime callStart = LocalDateTime.now(ZoneOffset.UTC)
                .minusSeconds(rng.nextInt(0, 86400));
        LocalDateTime callEnd = callStart.plusSeconds(durationSec);

        Map<String, Object> cdr = new LinkedHashMap<>();
        cdr.put("record_id", seq);
        cdr.put("caller_number", randomPhone());
        cdr.put("callee_number", randomPhone());
        cdr.put("call_type", callType);
        cdr.put("disposition", disposition);
        cdr.put("call_start", callStart.format(TS_FMT));
        cdr.put("call_end", callEnd.format(TS_FMT));
        cdr.put("duration_seconds", durationSec);
        cdr.put("data_usage_mb", dataUsageMb);
        cdr.put("charge_amount", chargeAmount);
        cdr.put("network_type", pick(NETWORKS));
        cdr.put("cell_tower_id", pick(CELL_TOWERS));
        cdr.put("plan_type", pick(PLANS));
        cdr.put("roaming", "ROAMING".equals(cdr.get("network_type")));
        cdr.put("event_timestamp", Instant.now().toString());
        return cdr;
    }

    private static String randomPhone() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        return "+1" + pick(AREA_CODES) + String.format("%07d", rng.nextInt(0, 10_000_000));
    }

    private static String pick(String[] arr) {
        return arr[ThreadLocalRandom.current().nextInt(arr.length)];
    }
}
