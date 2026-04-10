package com.snowflake.streaming.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Interactive CLI tool for pushing dummy mobile operator call records (CDRs) to Amazon SQS.
 *
 * <p>Uses the same CDR data model as the Kafka and Pub/Sub examples for consistency
 * across examples.</p>
 *
 * <p>Run via:</p>
 * <pre>{@code
 * mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.producer.FakeSQSWriter"
 * }</pre>
 *
 * <p><b>AWS credentials</b> are resolved via the default credential chain:
 * environment variables ({@code AWS_ACCESS_KEY_ID} / {@code AWS_SECRET_ACCESS_KEY}),
 * {@code ~/.aws/credentials}, or the instance metadata service.</p>
 */
public class FakeSQSWriter {

    private static final Logger logger = LoggerFactory.getLogger(FakeSQSWriter.class);
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

    private final SqsClient sqsClient;
    private final String queueUrl;
    private final int defaultBurstCount;
    private final int defaultStreamRps;

    public FakeSQSWriter(Config config) {
        this.queueUrl = config.getSqsQueueUrl();
        this.defaultBurstCount = config.getDefaultBurstCount();
        this.defaultStreamRps = config.getDefaultStreamRps();
        this.sqsClient = SqsClient.builder()
                .region(Region.of(config.getAwsRegion()))
                .build();
    }

    public static void main(String[] args) throws Exception {
        String configPath = System.getProperty("config.path", "producer-config.properties");
        Config config = new Config(configPath);
        new FakeSQSWriter(config).runInteractive();
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
            } catch (SqsException e) {
                System.out.println("AWS SQS error: " + e.getMessage());
                System.out.println("Hint: check aws.region and sqs.queue.url in producer-config.properties");
                System.out.println("      Ensure AWS credentials are configured (env vars or ~/.aws/credentials)");
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
                logger.error("Command failed", e);
            }
        }
    }

    private void printMenu() {
        System.out.println();
        System.out.println("=== Fake SQS Writer — Mobile CDR Generator ===");
        System.out.println("  Queue: " + queueUrl);
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
        String msgId = publish(mapper.writeValueAsString(cdr));
        System.out.println("Sent 1 CDR (seq=" + seq + ", type=" + cdr.get("call_type") + ", messageId=" + msgId + ")");
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
        }, "sqs-stream-writer");
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

    private String publish(String body) {
        SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(body)
                .build();
        SendMessageResponse response = sqsClient.sendMessage(request);
        logger.info("Published messageId={}, size={}B", response.messageId(), body.length());
        return response.messageId();
    }

    private void shutdown() {
        try {
            sqsClient.close();
        } catch (Exception e) {
            logger.warn("Error shutting down SQS client", e);
        }
    }

    // ---- CDR generation (same schema as Kafka and Pub/Sub examples) ----

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
