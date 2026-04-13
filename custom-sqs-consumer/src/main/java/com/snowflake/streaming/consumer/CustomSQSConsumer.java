package com.snowflake.streaming.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Amazon SQS → Snowpipe Streaming consumer using synchronous long-polling.
 *
 * <p>Uses a pull-based model so that messages are only deleted from SQS after
 * Snowflake confirms ingestion. Each consumer thread operates its own
 * Snowflake channel.</p>
 *
 * <p>Key design decisions:</p>
 * <ul>
 *   <li>Long-polling (WaitTimeSeconds up to 20) — reduces empty API calls and cost</li>
 *   <li>One Snowflake channel per consumer thread</li>
 *   <li>Batch delete (DeleteMessageBatch) — only after all rows in the batch are appended</li>
 *   <li>Shutdown interrupts the blocking receive call by closing the SqsClient immediately</li>
 *   <li>Same retry strategy as the Pub/Sub consumer: 401/403 fail, 409 reopen, 429/5xx backoff</li>
 *   <li>Periodic channel health checks every 30 seconds</li>
 * </ul>
 *
 * <p><b>At-least-once delivery:</b> if the process crashes after a successful
 * {@code appendRow} but before {@code DeleteMessageBatch}, SQS will redeliver
 * those messages after the visibility timeout expires. The Snowpipe Streaming
 * channel's {@code getLatestCommittedOffsetToken()} is used on restart to skip
 * rows whose offset tokens are already committed.</p>
 */
public class CustomSQSConsumer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CustomSQSConsumer.class);

    private static final int JITTER_BOUND_MS = 200;
    private static final Random jitterRandom = new Random();

    private static final long INITIAL_BACKOFF_MS = 500;
    private static final long MAX_BACKOFF_MS = 30_000;
    private static final int MAX_RETRY_ATTEMPTS_OTHER = 10;

    private static final long HEALTH_CHECK_INTERVAL_MS = 30_000;
    private long lastHealthCheckMs = 0;

    private final String queueUrl;
    private final int maxMessages;
    private final int waitTimeSeconds;
    private final int visibilityTimeoutSeconds;
    private final int maxRowsPerAppend;

    private final SnowflakeStreamingIngestClient sfClient;
    private final String channelName;
    private SnowflakeStreamingIngestChannel channel;

    private final AtomicLong offsetCounter = new AtomicLong(0);
    private final AtomicLong totalRowsProcessed = new AtomicLong(0);
    private volatile boolean running = true;

    private final ObjectMapper objectMapper = new ObjectMapper();

    // Kept as a field so shutdown() can close it to interrupt blocking receiveMessage
    private volatile SqsClient sqsClient;

    // Package-private constructor for unit tests — accepts a pre-built SqsClient
    CustomSQSConsumer(Config config, SnowflakeStreamingIngestClient sfClient,
                      String channelName, SqsClient sqsClient) {
        this.queueUrl = config.getSqsQueueUrl();
        this.maxMessages = config.getSqsMaxMessages();
        this.waitTimeSeconds = config.getSqsWaitTimeSeconds();
        this.visibilityTimeoutSeconds = config.getSqsVisibilityTimeoutSeconds();
        this.maxRowsPerAppend = config.getMaxRowsPerAppend();
        this.sfClient = sfClient;
        this.channelName = channelName;
        this.sqsClient = sqsClient;
    }

    public CustomSQSConsumer(Config config, SnowflakeStreamingIngestClient sfClient,
                             String channelName) {
        this(config, sfClient, channelName,
                SqsClient.builder()
                        .region(Region.of(config.getAwsRegion()))
                        .build());
    }

    @Override
    public void run() {
        MDC.put("consumerName", channelName);

        try {
            // Open Snowflake channel and resume offset counter from last committed token
            OpenChannelResult result = sfClient.openChannel(channelName);
            channel = result.getChannel();
            String lastToken = channel.getLatestCommittedOffsetToken();
            if (lastToken != null) {
                offsetCounter.set(Long.parseLong(lastToken));
            }
            logger.info("Opened channel '{}'. Last committed offset: {}", channelName, lastToken);

            List<Message> pending = new ArrayList<>();

            while (running) {
                checkChannelHealthPeriodically();

                // Accumulate messages across multiple receive calls until we hit maxRowsPerAppend
                // or an empty response (queue drained for this poll cycle).
                ReceiveMessageResponse response = receiveMessages();
                List<Message> batch = response.messages();

                if (batch.isEmpty() && pending.isEmpty()) {
                    // Nothing to process — loop and long-poll again
                    continue;
                }

                pending.addAll(batch);

                // If we haven't hit the append threshold and more messages may be waiting,
                // keep accumulating. An empty poll response signals the queue is drained.
                if (!batch.isEmpty() && pending.size() < maxRowsPerAppend) {
                    continue;
                }

                // Append all pending rows to Snowflake, then batch-delete from SQS
                List<DeleteMessageBatchRequestEntry> deleteEntries = new ArrayList<>();
                int entryIndex = 0;
                long batchStart = System.currentTimeMillis();

                for (Message message : pending) {
                    String body = message.body();
                    Map<String, Object> row = parseRecord(body);
                    String offsetToken = String.valueOf(offsetCounter.incrementAndGet());
                    appendRowWithRetry(row, offsetToken);

                    deleteEntries.add(DeleteMessageBatchRequestEntry.builder()
                            .id(String.valueOf(entryIndex++))
                            .receiptHandle(message.receiptHandle())
                            .build());
                }

                // Delete only after ALL appends in this batch have succeeded
                deleteMessages(deleteEntries);
                long batchMs = System.currentTimeMillis() - batchStart;
                long total = totalRowsProcessed.addAndGet(pending.size());
                logger.info("Batch: {} rows in {}ms ({} rows/sec). Total: {} rows.",
                        pending.size(), batchMs,
                        (int) (pending.size() * 1000.0 / Math.max(batchMs, 1)),
                        total);
                pending.clear();
            }
        } catch (Exception e) {
            if (running) {
                logger.error("Fatal error in consumer loop", e);
            }
            // If !running, the exception was triggered by sqsClient.close() in shutdown() — expected
        } finally {
            cleanup();
            MDC.remove("consumerName");
        }
    }

    private ReceiveMessageResponse receiveMessages() {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(maxMessages)
                .waitTimeSeconds(waitTimeSeconds)
                .visibilityTimeout(visibilityTimeoutSeconds)
                .build();
        return sqsClient.receiveMessage(request);
    }

    private static final int SQS_MAX_BATCH_DELETE_SIZE = 10;

    private void deleteMessages(List<DeleteMessageBatchRequestEntry> entries) {
        if (entries.isEmpty()) return;

        for (int i = 0; i < entries.size(); i += SQS_MAX_BATCH_DELETE_SIZE) {
            List<DeleteMessageBatchRequestEntry> chunk =
                    entries.subList(i, Math.min(i + SQS_MAX_BATCH_DELETE_SIZE, entries.size()));
            DeleteMessageBatchRequest deleteRequest = DeleteMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(chunk)
                    .build();
            sqsClient.deleteMessageBatch(deleteRequest);
        }
    }

    private void appendRowWithRetry(Map<String, Object> row, String offsetToken) {
        long backoffMs = INITIAL_BACKOFF_MS;
        int attempt = 0;

        while (true) {
            attempt++;
            try {
                channel.appendRow(row, offsetToken);
                return;
            } catch (SFException e) {
                int httpStatus = e.getHttpStatusCode();

                if (httpStatus == 401 || httpStatus == 403) {
                    logger.error("Authorization error (HTTP {})", httpStatus, e);
                    throw e;
                }

                if (httpStatus == 409) {
                    logger.warn("Channel invalidated (HTTP 409). Reopening. Attempt {}", attempt);
                    reopenChannel();
                    continue;
                }

                if (httpStatus == 429 || httpStatus >= 500) {
                    logger.warn("Backpressure (HTTP {}). Attempt {}. Backing off {}ms",
                            httpStatus, attempt, backoffMs);
                    sleepWithJitter(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
                    continue;
                }

                if (httpStatus == 408) {
                    if (attempt >= MAX_RETRY_ATTEMPTS_OTHER) {
                        logger.error("Failed after {} attempts. HTTP {}", attempt, httpStatus, e);
                        throw e;
                    }
                    logger.warn("Retryable error (HTTP {}). Attempt {}/{}. Backing off {}ms",
                            httpStatus, attempt, MAX_RETRY_ATTEMPTS_OTHER, backoffMs);
                    sleepWithJitter(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
                    continue;
                }

                logger.error("Non-retryable error (HTTP {})", httpStatus, e);
                throw e;
            }
        }
    }

    private void reopenChannel() {
        long startTime = System.currentTimeMillis();

        if (channel != null && !channel.isClosed()) {
            try {
                channel.close(false, java.time.Duration.ZERO);
            } catch (Exception e) {
                logger.warn("Error closing invalidated channel (swallowed): {}", e.getMessage());
            }
        }

        OpenChannelResult result = sfClient.openChannel(channelName);
        channel = result.getChannel();

        String lastToken = channel.getLatestCommittedOffsetToken();
        if (lastToken != null) {
            offsetCounter.set(Long.parseLong(lastToken));
        }

        long elapsed = System.currentTimeMillis() - startTime;
        logger.info("Channel reopened in {}ms. Last committed offset: {}", elapsed, lastToken);
    }

    private void checkChannelHealthPeriodically() {
        long now = System.currentTimeMillis();
        if (now - lastHealthCheckMs < HEALTH_CHECK_INTERVAL_MS) return;
        lastHealthCheckMs = now;

        if (channel == null) return;

        try {
            ChannelStatus status = channel.getChannelStatus();
            String statusCode = status.getStatusCode();
            if (!"SUCCESS".equals(statusCode)) {
                logger.warn("Channel unhealthy (status={}). Proactively reopening.", statusCode);
                reopenChannel();
            }
        } catch (Exception e) {
            logger.warn("Channel invalid during health check ({}). Reopening.", channelName, e);
            reopenChannel();
        }
    }

    private Map<String, Object> parseRecord(String body) {
        try {
            return objectMapper.readValue(body, new TypeReference<>() {});
        } catch (Exception e) {
            logger.error("Failed to parse SQS message body: {}", body, e);
            return Map.of("raw_data", body);
        }
    }

    private void cleanup() {
        if (channel != null && !channel.isClosed()) {
            try {
                channel.close(true, java.time.Duration.ofSeconds(30));
            } catch (Exception e) {
                logger.warn("Error closing channel", e);
            }
        }
    }

    private void sleepWithJitter(long baseMs) {
        long totalMs = baseMs + jitterRandom.nextInt(JITTER_BOUND_MS);
        try {
            Thread.sleep(totalMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during retry backoff", e);
        }
    }

    /**
     * Signals the consumer to stop and immediately interrupts any blocking
     * {@code receiveMessage} long-poll call by closing the SQS client.
     * This mirrors Kafka's {@code consumer.wakeup()} pattern.
     */
    public void shutdown() {
        running = false;
        SqsClient client = this.sqsClient;
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                logger.warn("Error closing SQS client during shutdown", e);
            }
        }
    }
}
