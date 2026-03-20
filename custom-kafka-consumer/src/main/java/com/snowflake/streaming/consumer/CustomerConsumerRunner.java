package com.snowflake.streaming.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Kafka → Snowpipe Streaming consumer that maps one Kafka partition to one
 * Snowflake channel. At startup it queries the topic's partition count,
 * opens a dedicated channel per partition, and manually assigns all partitions
 * so every record flows through its partition-specific channel.
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
    private final int MAX_ROWS_PER_APPEND;

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
        this.MAX_ROWS_PER_APPEND = config.getMaxRowsPerAppend();
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

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(kafkaTopicName);
        int partitionCount = partitionInfos.size();
        logger.info("Topic '{}' has {} partition(s)", kafkaTopicName, partitionCount);

        List<TopicPartition> topicPartitions = partitionInfos.stream()
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .collect(Collectors.toList());

        consumer.assign(topicPartitions);

        for (TopicPartition tp : topicPartitions) {
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

        Map<Integer, RowSetBuilder> builders = new HashMap<>();
        Map<Integer, Long> firstOffsets = new HashMap<>();
        Map<Integer, Long> lastOffsets = new HashMap<>();
        Map<Integer, Integer> counts = new HashMap<>();

        for (int p : partitionChannels.keySet()) {
            builders.put(p, RowSetBuilder.newBuilder2());
            firstOffsets.put(p, -1L);
            lastOffsets.put(p, -1L);
            counts.put(p, 0);
        }

        try {
            while (running) {
                checkChannelHealthPeriodically();

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(consumerPollDuration));
                if (records.isEmpty()) continue;

                for (ConsumerRecord<String, String> record : records) {
                    int partition = record.partition();
                    RowSetBuilder builder = builders.get(partition);
                    appendRowsetBuilder(builder, record.value());

                    if (firstOffsets.get(partition) == -1L) {
                        firstOffsets.put(partition, record.offset());
                    }
                    lastOffsets.put(partition, record.offset());
                    counts.merge(partition, 1, Integer::sum);

                    if (counts.get(partition) >= MAX_ROWS_PER_APPEND) {
                        flushPartition(partition, builders, firstOffsets, lastOffsets, counts);
                    }
                }

                for (int p : partitionChannels.keySet()) {
                    if (counts.get(p) > 0) {
                        flushPartition(p, builders, firstOffsets, lastOffsets, counts);
                    }
                }

                commitKafkaOffsetsAfterSnowflakeConfirm();
            }
        } catch (WakeupException e) {
            logger.info("Consumer loop waking up for shutdown.");
        } catch (Exception e) {
            logger.error("Fatal error in consumer loop", e);
        } finally {
            for (int p : partitionChannels.keySet()) {
                RowSetBuilder builder = builders.get(p);
                if (builder.rowSetSize() > 0) {
                    try {
                        appendRowsWithRetry(
                                partitionChannels.get(p), p,
                                new ArrayList<>(builder.build()), null, null);
                    } catch (Exception e) {
                        logger.error("Failed to flush remaining rows for partition {} during shutdown", p, e);
                    }
                }
            }
            cleanup();
            MDC.remove("consumerName");
        }
    }

    private void flushPartition(int partition,
                                Map<Integer, RowSetBuilder> builders,
                                Map<Integer, Long> firstOffsets,
                                Map<Integer, Long> lastOffsets,
                                Map<Integer, Integer> counts) {
        RowSetBuilder builder = builders.get(partition);
        SnowflakeStreamingIngestChannel channel = partitionChannels.get(partition);

        appendRowsWithRetry(
                channel, partition,
                new ArrayList<>(builder.build()),
                String.valueOf(firstOffsets.get(partition)),
                String.valueOf(lastOffsets.get(partition)));

        builder.clear();
        firstOffsets.put(partition, -1L);
        lastOffsets.put(partition, -1L);
        counts.put(partition, 0);
    }

    private void appendRowsWithRetry(SnowflakeStreamingIngestChannel channel, int partition,
                                     List<Map<String, Object>> rows,
                                     String startOffset, String endOffset) {
        long backoffMs = INITIAL_BACKOFF_MS;
        int attempt = 0;

        while (true) {
            attempt++;
            try {
                channel.appendRows(rows, startOffset, endOffset);
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

    private void appendRowsetBuilder(RowSetBuilder builder, String recordValue) {
        try {
            Map<String, Object> row = objectMapper.readValue(recordValue, new TypeReference<>() {});
            builder.addRow(row);
        } catch (Exception e) {
            logger.error("Failed to parse record: {}", recordValue, e);
            builder.addRow(Map.of("raw_data", recordValue));
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
