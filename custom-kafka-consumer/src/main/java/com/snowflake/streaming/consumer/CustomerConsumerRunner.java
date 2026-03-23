package com.snowflake.streaming.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * Kafka → Snowpipe Streaming consumer that maps one Kafka partition to one
 * Snowflake channel. Uses Kafka consumer group coordination ({@code subscribe})
 * so that partitions are automatically distributed across nodes. A
 * {@link ConsumerRebalanceListener} opens and closes Snowflake channels on
 * each rebalance to maintain the 1:1 partition-to-channel mapping.
 */
public class CustomerConsumerRunner implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CustomerConsumerRunner.class);

    private static final int JITTER_BOUND_MS = 200;
    private static final Random jitterRandom = new Random();

    private static final long INITIAL_BACKOFF_MS = 500;
    private static final long MAX_BACKOFF_MS = 30_000;
    private static final int MAX_RETRY_ATTEMPTS_OTHER = 10;

    private static final long HEALTH_CHECK_INTERVAL_MS = 30_000;
    private long lastHealthCheckMs = 0;

    private final String kafkaGroupID;
    private final String kafkaTopicName;
    private final Properties kafkaProps;
    private final long consumerPollDuration;

    private final SnowflakeStreamingIngestClient sfClient;
    private final String sfChannelPrefix;

    private final Map<Integer, SnowflakeStreamingIngestChannel> partitionChannels = new HashMap<>();
    private KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public CustomerConsumerRunner(Config config, SnowflakeStreamingIngestClient sfClient) {
        this(config, sfClient, null);
    }

    CustomerConsumerRunner(Config config, SnowflakeStreamingIngestClient sfClient,
                           KafkaConsumer<String, String> consumer) {
        this.kafkaGroupID = config.getKafkaGroupId();
        this.kafkaTopicName = config.getKafkaTopic();
        this.kafkaProps = config.buildKafkaProperties();
        this.consumerPollDuration = config.getConsumerPollDurationMs();
        this.sfClient = sfClient;
        this.sfChannelPrefix = config.getSnowflakeChannelName();
        this.consumer = consumer;
    }

    @Override
    public void run() {
        MDC.put("consumerName", kafkaGroupID);

        if (this.consumer == null) {
            this.consumer = new KafkaConsumer<>(kafkaProps);
        }

        consumer.subscribe(List.of(kafkaTopicName), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                for (TopicPartition tp : partitions) {
                    SnowflakeStreamingIngestChannel channel = partitionChannels.remove(tp.partition());
                    if (channel != null && !channel.isClosed()) {
                        try {
                            channel.close(true, Duration.ofSeconds(30));
                            logger.info("Partition {}: closed channel on revoke", tp.partition());
                        } catch (Exception e) {
                            logger.warn("Partition {}: error closing channel on revoke", tp.partition(), e);
                        }
                    }
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition tp : partitions) {
                    String channelName = sfChannelPrefix + "_P" + tp.partition();
                    OpenChannelResult result = sfClient.openChannel(channelName);
                    SnowflakeStreamingIngestChannel channel = result.getChannel();
                    partitionChannels.put(tp.partition(), channel);

                    String lastToken = channel.getLatestCommittedOffsetToken();
                    logger.info("Opened channel '{}' for partition {}. Last committed offset: {}",
                            channelName, tp.partition(), lastToken);

                    if (lastToken != null) {
                        long offset = Long.parseLong(lastToken);
                        consumer.seek(tp, offset + 1);
                        logger.info("Seeked partition {} to offset {}", tp.partition(), offset + 1);
                    }
                }
            }
        });

        try {
            while (running) {
                checkChannelHealthPeriodically();

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(consumerPollDuration));
                if (records.isEmpty()) continue;

                for (ConsumerRecord<String, String> record : records) {
                    int partition = record.partition();
                    SnowflakeStreamingIngestChannel channel = partitionChannels.get(partition);
                    Map<String, Object> row = parseRecord(record.value());
                    String offsetToken = String.valueOf(record.offset());
                    appendRowWithRetry(channel, partition, row, offsetToken);
                }

                commitKafkaOffsetsAfterSnowflakeConfirm();
            }
        } catch (WakeupException e) {
            logger.info("Consumer loop waking up for shutdown.");
        } catch (Exception e) {
            logger.error("Fatal error in consumer loop", e);
        } finally {
            cleanup();
            MDC.remove("consumerName");
        }
    }

    private void appendRowWithRetry(SnowflakeStreamingIngestChannel channel, int partition,
                                    Map<String, Object> row, String offsetToken) {
        long backoffMs = INITIAL_BACKOFF_MS;
        int attempt = 0;

        while (true) {
            attempt++;
            try {
                channel.appendRows(List.of(row), offsetToken, offsetToken);
                return;
            } catch (SFException e) {
                int httpStatus = e.getHttpStatusCode();

                if (httpStatus == 401 || httpStatus == 403) {
                    logger.error("Partition {}: authorization error (HTTP {})", partition, httpStatus, e);
                    throw e;
                }

                if (httpStatus == 409) {
                    logger.warn("Partition {}: channel invalidated (HTTP 409). Reopening. Attempt {}",
                            partition, attempt);
                    channel = reopenChannelForPartition(partition);
                    continue;
                }

                if (httpStatus == 429 || httpStatus >= 500) {
                    logger.warn("Partition {}: backpressure (HTTP {}). Attempt {}. Backing off {}ms",
                            partition, httpStatus, attempt, backoffMs);
                    sleepWithJitter(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
                    continue;
                }

                if (httpStatus == 408) {
                    if (attempt >= MAX_RETRY_ATTEMPTS_OTHER) {
                        logger.error("Partition {}: failed after {} attempts. HTTP {}",
                                partition, attempt, httpStatus, e);
                        throw e;
                    }
                    logger.warn("Partition {}: retryable error (HTTP {}). Attempt {}/{}. Backing off {}ms",
                            partition, httpStatus, attempt, MAX_RETRY_ATTEMPTS_OTHER, backoffMs);
                    sleepWithJitter(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
                    continue;
                }

                logger.error("Partition {}: non-retryable error (HTTP {})", partition, httpStatus, e);
                throw e;
            }
        }
    }

    private SnowflakeStreamingIngestChannel reopenChannelForPartition(int partition) {
        long startTime = System.currentTimeMillis();
        SnowflakeStreamingIngestChannel oldChannel = partitionChannels.get(partition);

        if (!oldChannel.isClosed()) {
            try {
                oldChannel.close(false, Duration.ZERO);
            } catch (Exception e) {
                logger.warn("Partition {}: error closing invalidated channel (swallowed): {}",
                        partition, e.getMessage());
            }
        }

        String channelName = sfChannelPrefix + "_P" + partition;
        OpenChannelResult result = sfClient.openChannel(channelName);
        SnowflakeStreamingIngestChannel newChannel = result.getChannel();
        partitionChannels.put(partition, newChannel);

        String lastToken = newChannel.getLatestCommittedOffsetToken();
        if (lastToken != null && consumer != null) {
            long offset = Long.parseLong(lastToken);
            TopicPartition tp = new TopicPartition(kafkaTopicName, partition);
            consumer.seek(tp, offset + 1);
            logger.info("Partition {}: after recovery, seeked to offset {}", partition, offset + 1);
        }

        long elapsed = System.currentTimeMillis() - startTime;
        logger.info("Partition {}: channel reopened in {}ms. Last committed offset: {}",
                partition, elapsed, lastToken);
        return newChannel;
    }

    /**
     * Commits Kafka offsets per partition based on each channel's confirmed offset.
     */
    private void commitKafkaOffsetsAfterSnowflakeConfirm() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        for (Map.Entry<Integer, SnowflakeStreamingIngestChannel> entry : partitionChannels.entrySet()) {
            int partition = entry.getKey();
            String committedToken = entry.getValue().getLatestCommittedOffsetToken();
            if (committedToken != null) {
                long sfOffset = Long.parseLong(committedToken);
                TopicPartition tp = new TopicPartition(kafkaTopicName, partition);
                offsets.put(tp, new OffsetAndMetadata(sfOffset + 1));
            }
        }

        if (!offsets.isEmpty()) {
            consumer.commitSync(offsets);
            logger.debug("Committed Kafka offsets for {} partition(s)", offsets.size());
        }
    }

    private void checkChannelHealthPeriodically() {
        long now = System.currentTimeMillis();
        if (now - lastHealthCheckMs < HEALTH_CHECK_INTERVAL_MS) return;
        lastHealthCheckMs = now;

        for (Map.Entry<Integer, SnowflakeStreamingIngestChannel> entry : partitionChannels.entrySet()) {
            int partition = entry.getKey();
            try {
                ChannelStatus status = entry.getValue().getChannelStatus();
                String statusCode = status.getStatusCode();
                if (!"SUCCESS".equals(statusCode)) {
                    logger.warn("Partition {}: channel unhealthy (status={}). Proactively reopening.",
                            partition, statusCode);
                    reopenChannelForPartition(partition);
                }
            } catch (Exception e) {
                logger.warn("Partition {}: error checking channel health", partition, e);
            }
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
        for (Map.Entry<Integer, SnowflakeStreamingIngestChannel> entry : partitionChannels.entrySet()) {
            try {
                if (!entry.getValue().isClosed()) {
                    entry.getValue().close(true, Duration.ofSeconds(30));
                }
            } catch (Exception e) {
                logger.warn("Partition {}: error closing channel", entry.getKey(), e);
            }
        }
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) {
                logger.warn("Error closing Kafka consumer", e);
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
        if (consumer != null) {
            consumer.wakeup();
        }
    }
}
