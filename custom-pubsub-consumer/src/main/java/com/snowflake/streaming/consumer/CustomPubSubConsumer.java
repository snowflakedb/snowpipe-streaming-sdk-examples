package com.snowflake.streaming.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * GCP Pub/Sub → Snowpipe Streaming consumer using synchronous pull.
 *
 * <p>Uses a pull-based model so that messages are only acknowledged after
 * Snowflake confirms ingestion. Each consumer thread operates its own
 * Snowflake channel.</p>
 *
 * <p>Key design decisions:</p>
 * <ul>
 *   <li>Synchronous pull — full control over ack timing (no ack before Snowflake confirms)</li>
 *   <li>One Snowflake channel per consumer thread</li>
 *   <li>Same retry strategy as the Kafka consumer: 401/403 fail, 409 reopen, 429/5xx backoff</li>
 *   <li>Periodic channel health checks every 30 seconds</li>
 * </ul>
 */
public class CustomPubSubConsumer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CustomPubSubConsumer.class);

    private static final int JITTER_BOUND_MS = 200;
    private static final Random jitterRandom = new Random();

    private static final long INITIAL_BACKOFF_MS = 500;
    private static final long MAX_BACKOFF_MS = 30_000;
    private static final int MAX_RETRY_ATTEMPTS_OTHER = 10;

    private static final long HEALTH_CHECK_INTERVAL_MS = 30_000;
    private long lastHealthCheckMs = 0;

    private final String gcpProjectId;
    private final String subscriptionId;
    private final int maxMessages;

    private final SnowflakeStreamingIngestClient sfClient;
    private final String channelName;
    private SnowflakeStreamingIngestChannel channel;

    private final AtomicLong offsetCounter = new AtomicLong(0);
    private volatile boolean running = true;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public CustomPubSubConsumer(Config config, SnowflakeStreamingIngestClient sfClient,
                                String channelName) {
        this.gcpProjectId = config.getGcpProjectId();
        this.subscriptionId = config.getPubSubSubscriptionId();
        this.maxMessages = config.getMaxRowsPerAppend();
        this.sfClient = sfClient;
        this.channelName = channelName;
    }

    @Override
    public void run() {
        MDC.put("consumerName", channelName);
        String subscriptionPath = ProjectSubscriptionName.format(gcpProjectId, subscriptionId);

        try {
            // Open Snowflake channel
            OpenChannelResult result = sfClient.openChannel(channelName);
            channel = result.getChannel();
            String lastToken = channel.getLatestCommittedOffsetToken();
            if (lastToken != null) {
                offsetCounter.set(Long.parseLong(lastToken));
            }
            logger.info("Opened channel '{}'. Last committed offset: {}", channelName, lastToken);

            // Set up Pub/Sub subscriber stub
            SubscriberStubSettings settings = SubscriberStubSettings.newBuilder().build();

            try (SubscriberStub subscriber = GrpcSubscriberStub.create(settings)) {
                while (running) {
                    checkChannelHealthPeriodically();

                    PullRequest pullRequest = PullRequest.newBuilder()
                            .setSubscription(subscriptionPath)
                            .setMaxMessages(maxMessages)
                            .build();

                    PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
                    List<ReceivedMessage> messages = pullResponse.getReceivedMessagesList();

                    if (messages.isEmpty()) {
                        continue;
                    }

                    logger.debug("Pulled {} messages from {}", messages.size(), subscriptionId);

                    List<String> ackIds = new ArrayList<>();
                    for (ReceivedMessage message : messages) {
                        String data = message.getMessage().getData().toStringUtf8();
                        Map<String, Object> row = parseRecord(data);
                        String offsetToken = String.valueOf(offsetCounter.incrementAndGet());
                        appendRowWithRetry(row, offsetToken);
                        ackIds.add(message.getAckId());
                    }

                    // Ack only after all rows in this batch have been appended
                    ackMessages(subscriber, subscriptionPath, ackIds);
                }
            }
        } catch (Exception e) {
            if (running) {
                logger.error("Fatal error in consumer loop", e);
            }
        } finally {
            cleanup();
            MDC.remove("consumerName");
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

    private void ackMessages(SubscriberStub subscriber, String subscriptionPath,
                             List<String> ackIds) {
        if (ackIds.isEmpty()) return;

        AcknowledgeRequest ackRequest = AcknowledgeRequest.newBuilder()
                .setSubscription(subscriptionPath)
                .addAllAckIds(ackIds)
                .build();
        subscriber.acknowledgeCallable().call(ackRequest);
        logger.debug("Acknowledged {} messages", ackIds.size());
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
            logger.warn("Error checking channel health", e);
        }
    }

    private Map<String, Object> parseRecord(String recordValue) {
        try {
            return objectMapper.readValue(recordValue, new TypeReference<>() {});
        } catch (Exception e) {
            logger.error("Failed to parse record: {}", recordValue, e);
            return Map.of("raw_data", recordValue);
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

    public void shutdown() {
        running = false;
    }
}
