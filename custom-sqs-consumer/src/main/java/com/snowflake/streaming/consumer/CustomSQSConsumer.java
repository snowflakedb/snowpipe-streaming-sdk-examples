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
 *   <li>Batch delete (DeleteMessageBatch) — only after Snowflake confirms the batch is committed</li>
 *   <li>Shutdown interrupts the blocking receive call by closing the SqsClient immediately</li>
 *   <li>Same retry strategy as the Pub/Sub consumer: 401/403 fail, 409 reopen, 429/5xx backoff</li>
 *   <li>Periodic channel health checks every 30 seconds</li>
 * </ul>
 *
 * <p><b>At-least-once delivery:</b> each batch waits for
 * {@code getLatestCommittedOffsetToken()} to reach the batch's last offset token
 * before deleting messages from SQS. If the process crashes before
 * {@code DeleteMessageBatch}, SQS redelivers those messages after the visibility
 * timeout and the channel's committed offset prevents re-insertion.</p>
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

    private static final long COMMIT_POLL_INTERVAL_MS = 100;
    private static final long COMMIT_TIMEOUT_MS = 60_000;

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
                List<Map<String, Object>> rows = new ArrayList<>(pending.size());
                List<DeleteMessageBatchRequestEntry> deleteEntries = new ArrayList<>(pending.size());
                int entryIndex = 0;
                long batchStart = System.currentTimeMillis();

                for (Message message : pending) {
                    rows.add(parseRecord(message.body()));
                    deleteEntries.add(DeleteMessageBatchRequestEntry.builder()
                            .id(String.valueOf(entryIndex++))
                            .receiptHandle(message.receiptHandle())
                            .build());
                }

                // Single batch call — buffers all rows at once before the SDK flushes
                String lastOffsetToken = appendRowsWithRetry(rows);

                // Wait for Snowflake to confirm the batch is committed before
                // deleting from SQS — this is what makes delivery at-least-once
                waitForCommit(lastOffsetToken);
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

    /**
     * Appends a batch of rows to the Snowflake channel in a single call,
     * with retries for transient errors. Assigns contiguous offset tokens to
     * the batch; on 409 (channel invalidated), reopens the channel and
     * re-assigns tokens from the newly committed position before retrying.
     *
     * @return the last offset token assigned to the batch (used for {@link #waitForCommit})
     */
    private String appendRowsWithRetry(List<Map<String, Object>> rows) {
        long backoffMs = INITIAL_BACKOFF_MS;
        int attempt = 0;

        // Assign contiguous offset token range for this batch
        long base = offsetCounter.getAndAdd(rows.size());
        String firstOffsetToken = String.valueOf(base + 1);
        String lastOffsetToken = String.valueOf(base + rows.size());

        while (true) {
            attempt++;
            try {
                channel.appendRows(rows, firstOffsetToken, lastOffsetToken);
                return lastOffsetToken;
            } catch (SFException e) {
                int httpStatus = e.getHttpStatusCode();

                if (httpStatus == 401 || httpStatus == 403) {
                    logger.error("Authorization error (HTTP {})", httpStatus, e);
                    throw e;
                }

                if (httpStatus == 409) {
                    logger.warn("Channel invalidated (HTTP 409). Reopening. Attempt {}", attempt);
                    reopenChannel();
                    // Reassign offset tokens from the new committed position
                    base = offsetCounter.getAndAdd(rows.size());
                    firstOffsetToken = String.valueOf(base + 1);
                    lastOffsetToken = String.valueOf(base + rows.size());
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

    /**
     * Polls {@code getLatestCommittedOffsetToken()} until the channel confirms
     * the given offset token (or higher) has been committed to Snowflake.
     * Times out after {@link #COMMIT_TIMEOUT_MS} with a warning; delivery is
     * still at-least-once because SQS will redeliver if the process crashes
     * before {@code DeleteMessageBatch} completes.
     */
    private void waitForCommit(String targetOffsetToken) {
        long target = Long.parseLong(targetOffsetToken);
        long deadline = System.currentTimeMillis() + COMMIT_TIMEOUT_MS;

        while (System.currentTimeMillis() < deadline) {
            String committed = channel.getLatestCommittedOffsetToken();
            if (committed != null && Long.parseLong(committed) >= target) {
                return;
            }
            try {
                Thread.sleep(COMMIT_POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for commit", e);
            }
        }

        throw new RuntimeException(
                "Timed out after " + COMMIT_TIMEOUT_MS + "ms waiting for offset " + targetOffsetToken
                        + " to be committed. Aborting batch — SQS messages will be redelivered after visibility timeout.");
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
