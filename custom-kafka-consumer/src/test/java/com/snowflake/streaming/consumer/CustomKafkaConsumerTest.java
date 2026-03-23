package com.snowflake.streaming.consumer;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CustomKafkaConsumerTest {

    private static final String TOPIC = "test-topic";
    private static final String CHANNEL_PREFIX = "TEST_CHANNEL";

    @Mock private SnowflakeStreamingIngestClient sfClient;
    @Mock private KafkaConsumer<String, String> kafkaConsumer;
    @Mock private SnowflakeStreamingIngestChannel channelP0;
    @Mock private SnowflakeStreamingIngestChannel channelP1;
    @Mock private OpenChannelResult openResultP0;
    @Mock private OpenChannelResult openResultP1;
    @Mock private ChannelStatus healthyStatus;

    private Config config;
    private CustomKafkaConsumer runner;
    private ConsumerRebalanceListener rebalanceListener;

    @BeforeEach
    void setUp() throws Exception {
        config = createTestConfig();

        lenient().when(openResultP0.getChannel()).thenReturn(channelP0);
        lenient().when(openResultP1.getChannel()).thenReturn(channelP1);

        lenient().when(sfClient.openChannel(CHANNEL_PREFIX + "_P0")).thenReturn(openResultP0);
        lenient().when(sfClient.openChannel(CHANNEL_PREFIX + "_P1")).thenReturn(openResultP1);

        lenient().when(channelP0.getLatestCommittedOffsetToken()).thenReturn(null);
        lenient().when(channelP1.getLatestCommittedOffsetToken()).thenReturn(null);
        lenient().when(channelP0.isClosed()).thenReturn(false);
        lenient().when(channelP1.isClosed()).thenReturn(false);

        lenient().when(healthyStatus.getStatusCode()).thenReturn("SUCCESS");
        lenient().when(channelP0.getChannelStatus()).thenReturn(healthyStatus);
        lenient().when(channelP1.getChannelStatus()).thenReturn(healthyStatus);

        doAnswer(inv -> {
            rebalanceListener = inv.getArgument(1);
            return null;
        }).when(kafkaConsumer).subscribe(anyList(), any(ConsumerRebalanceListener.class));

        runner = new CustomKafkaConsumer(config, sfClient, kafkaConsumer);
    }

    // --- Subscribe and rebalance ---

    @Test
    void subscribesToTopicWithRebalanceListener() {
        simulateRebalanceThenShutdown(List.of());

        runner.run();

        verify(kafkaConsumer).subscribe(eq(List.of(TOPIC)), any(ConsumerRebalanceListener.class));
    }

    @Test
    void opensOneChannelPerPartition() {
        simulateRebalanceThenShutdown(List.of(tp(0), tp(1)));

        runner.run();

        verify(sfClient).openChannel(CHANNEL_PREFIX + "_P0");
        verify(sfClient).openChannel(CHANNEL_PREFIX + "_P1");
    }

    @Test
    void seeksToSnowflakeOffsetOnRebalance() {
        when(channelP0.getLatestCommittedOffsetToken()).thenReturn("42");
        simulateRebalanceThenShutdown(List.of(tp(0), tp(1)));

        runner.run();

        verify(kafkaConsumer).seek(tp(0), 43);
        verify(kafkaConsumer, never()).seek(eq(tp(1)), anyLong());
    }

    @Test
    void closesChannelsOnPartitionsRevoked() throws Exception {
        when(kafkaConsumer.poll(any()))
                .thenAnswer(inv -> {
                    rebalanceListener.onPartitionsAssigned(List.of(tp(0), tp(1)));
                    return ConsumerRecords.empty();
                })
                .thenAnswer(inv -> {
                    rebalanceListener.onPartitionsRevoked(List.of(tp(1)));
                    runner.shutdown();
                    return ConsumerRecords.empty();
                });

        runner.run();

        verify(channelP1).close(eq(true), any());
    }

    // --- Record routing ---

    @Test
    void routesRecordsToCorrectPartitionChannel() {
        ConsumerRecords<String, String> batch = buildRecords(
                record(0, 0, "{\"C1\":1}"),
                record(1, 0, "{\"C1\":2}")
        );

        when(kafkaConsumer.poll(any()))
                .thenAnswer(inv -> {
                    rebalanceListener.onPartitionsAssigned(List.of(tp(0), tp(1)));
                    return batch;
                })
                .thenAnswer(inv -> { runner.shutdown(); return ConsumerRecords.empty(); });

        runner.run();

        verify(channelP0).appendRows(anyList(), eq("0"), eq("0"));
        verify(channelP1).appendRows(anyList(), eq("0"), eq("0"));
    }

    @Test
    void insertsEachRowIndividually() {
        ConsumerRecords<String, String> batch = buildRecords(
                record(0, 0, "{\"C1\":1}"),
                record(0, 1, "{\"C1\":2}"),
                record(0, 2, "{\"C1\":3}")
        );

        when(kafkaConsumer.poll(any()))
                .thenAnswer(inv -> {
                    rebalanceListener.onPartitionsAssigned(List.of(tp(0)));
                    return batch;
                })
                .thenAnswer(inv -> { runner.shutdown(); return ConsumerRecords.empty(); });

        runner.run();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Map<String, Object>>> rowsCaptor = ArgumentCaptor.forClass(List.class);
        verify(channelP0, times(3)).appendRows(rowsCaptor.capture(), any(), any());

        for (List<Map<String, Object>> rows : rowsCaptor.getAllValues()) {
            assertEquals(1, rows.size());
        }
    }

    @Test
    void passesCorrectOffsetTokenPerRow() {
        ConsumerRecords<String, String> batch = buildRecords(
                record(0, 5, "{\"C1\":1}"),
                record(0, 6, "{\"C1\":2}")
        );

        when(kafkaConsumer.poll(any()))
                .thenAnswer(inv -> {
                    rebalanceListener.onPartitionsAssigned(List.of(tp(0)));
                    return batch;
                })
                .thenAnswer(inv -> { runner.shutdown(); return ConsumerRecords.empty(); });

        runner.run();

        verify(channelP0).appendRows(anyList(), eq("5"), eq("5"));
        verify(channelP0).appendRows(anyList(), eq("6"), eq("6"));
    }

    // --- Offset commit ---

    @Test
    void commitsKafkaOffsetsAfterSnowflakeConfirm() {
        ConsumerRecords<String, String> batch = buildRecords(
                record(0, 5, "{\"C1\":1}"),
                record(1, 10, "{\"C1\":2}")
        );

        when(channelP0.getLatestCommittedOffsetToken())
                .thenReturn(null)
                .thenReturn("5");
        when(channelP1.getLatestCommittedOffsetToken())
                .thenReturn(null)
                .thenReturn("10");

        when(kafkaConsumer.poll(any()))
                .thenAnswer(inv -> {
                    rebalanceListener.onPartitionsAssigned(List.of(tp(0), tp(1)));
                    return batch;
                })
                .thenAnswer(inv -> { runner.shutdown(); return ConsumerRecords.empty(); });

        runner.run();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor =
                ArgumentCaptor.forClass(Map.class);
        verify(kafkaConsumer, atLeastOnce()).commitSync(captor.capture());

        Map<TopicPartition, OffsetAndMetadata> committed = captor.getValue();
        assertEquals(6, committed.get(tp(0)).offset());
        assertEquals(11, committed.get(tp(1)).offset());
    }

    @Test
    void skipsCommitWhenNoSnowflakeOffsetsConfirmed() {
        simulateRebalanceThenShutdown(List.of(tp(0)));

        runner.run();

        verify(kafkaConsumer, never()).commitSync(anyMap());
    }

    // --- Retry logic ---

    @Test
    void retriesOn429Backpressure() {
        ConsumerRecords<String, String> batch = buildRecords(record(0, 0, "{\"C1\":1}"));

        when(kafkaConsumer.poll(any()))
                .thenAnswer(inv -> {
                    rebalanceListener.onPartitionsAssigned(List.of(tp(0)));
                    return batch;
                })
                .thenAnswer(inv -> { runner.shutdown(); return ConsumerRecords.empty(); });

        doThrow(sfException(429))
                .doNothing()
                .when(channelP0).appendRows(anyList(), any(), any());

        runner.run();

        verify(channelP0, times(2)).appendRows(anyList(), any(), any());
    }

    @Test
    void retriesOn500ServerError() {
        ConsumerRecords<String, String> batch = buildRecords(record(0, 0, "{\"C1\":1}"));

        when(kafkaConsumer.poll(any()))
                .thenAnswer(inv -> {
                    rebalanceListener.onPartitionsAssigned(List.of(tp(0)));
                    return batch;
                })
                .thenAnswer(inv -> { runner.shutdown(); return ConsumerRecords.empty(); });

        doThrow(sfException(503))
                .doNothing()
                .when(channelP0).appendRows(anyList(), any(), any());

        runner.run();

        verify(channelP0, times(2)).appendRows(anyList(), any(), any());
    }

    @Test
    void reopensChannelOn409() throws Exception {
        SnowflakeStreamingIngestChannel newChannel = mock(SnowflakeStreamingIngestChannel.class);
        OpenChannelResult newResult = mock(OpenChannelResult.class);
        when(newResult.getChannel()).thenReturn(newChannel);
        when(newChannel.getLatestCommittedOffsetToken()).thenReturn(null);

        when(sfClient.openChannel(CHANNEL_PREFIX + "_P0"))
                .thenReturn(openResultP0)
                .thenReturn(newResult);

        ConsumerRecords<String, String> batch = buildRecords(record(0, 0, "{\"C1\":1}"));

        when(kafkaConsumer.poll(any()))
                .thenAnswer(inv -> {
                    rebalanceListener.onPartitionsAssigned(List.of(tp(0)));
                    return batch;
                })
                .thenAnswer(inv -> { runner.shutdown(); return ConsumerRecords.empty(); });

        doThrow(sfException(409))
                .when(channelP0).appendRows(anyList(), any(), any());
        doNothing()
                .when(newChannel).appendRows(anyList(), any(), any());

        runner.run();

        verify(channelP0).close(false, Duration.ZERO);
        verify(sfClient, times(2)).openChannel(CHANNEL_PREFIX + "_P0");
        verify(newChannel).appendRows(anyList(), any(), any());
    }

    @Test
    void failsImmediatelyOn401() {
        ConsumerRecords<String, String> batch = buildRecords(record(0, 0, "{\"C1\":1}"));

        when(kafkaConsumer.poll(any()))
                .thenAnswer(inv -> {
                    rebalanceListener.onPartitionsAssigned(List.of(tp(0)));
                    return batch;
                })
                .thenAnswer(inv -> { runner.shutdown(); return ConsumerRecords.empty(); });

        doThrow(sfException(401))
                .when(channelP0).appendRows(anyList(), any(), any());

        runner.run();

        verify(channelP0, times(1)).appendRows(anyList(), any(), any());
    }

    @Test
    void failsImmediatelyOn403() {
        ConsumerRecords<String, String> batch = buildRecords(record(0, 0, "{\"C1\":1}"));

        when(kafkaConsumer.poll(any()))
                .thenAnswer(inv -> {
                    rebalanceListener.onPartitionsAssigned(List.of(tp(0)));
                    return batch;
                })
                .thenAnswer(inv -> { runner.shutdown(); return ConsumerRecords.empty(); });

        doThrow(sfException(403))
                .when(channelP0).appendRows(anyList(), any(), any());

        runner.run();

        verify(channelP0, times(1)).appendRows(anyList(), any(), any());
    }

    // --- Malformed JSON ---

    @Test
    void handlesMalformedJsonGracefully() {
        ConsumerRecords<String, String> batch = buildRecords(record(0, 0, "not-json"));

        when(kafkaConsumer.poll(any()))
                .thenAnswer(inv -> {
                    rebalanceListener.onPartitionsAssigned(List.of(tp(0)));
                    return batch;
                })
                .thenAnswer(inv -> { runner.shutdown(); return ConsumerRecords.empty(); });

        runner.run();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Map<String, Object>>> captor = ArgumentCaptor.forClass(List.class);
        verify(channelP0).appendRows(captor.capture(), any(), any());

        Map<String, Object> row = captor.getValue().get(0);
        assertEquals("not-json", row.get("raw_data"));
    }

    // --- Cleanup ---

    @Test
    void closesAllChannelsAndConsumerOnShutdown() throws Exception {
        simulateRebalanceThenShutdown(List.of(tp(0), tp(1)));

        runner.run();

        verify(channelP0).close(eq(true), any());
        verify(channelP1).close(eq(true), any());
        verify(kafkaConsumer).close();
    }

    @Test
    void shutdownSetsRunningFalseAndWakesConsumer() {
        when(kafkaConsumer.poll(any())).thenAnswer(inv -> {
            rebalanceListener.onPartitionsAssigned(List.of(tp(0)));
            runner.shutdown();
            return ConsumerRecords.empty();
        });

        runner.run();

        verify(kafkaConsumer).wakeup();
    }

    // --- Helpers ---

    private Config createTestConfig() throws Exception {
        java.io.File tmp = java.io.File.createTempFile("test-config", ".properties");
        tmp.deleteOnExit();
        java.util.Properties props = new java.util.Properties();
        props.setProperty("kafka.bootstrap.servers", "localhost:9092");
        props.setProperty("kafka.topic", TOPIC);
        props.setProperty("kafka.group.id", "test-group");
        props.setProperty("kafka.poll.duration.ms", "100");
        props.setProperty("snowflake.channel.name", CHANNEL_PREFIX);
        props.setProperty("snowflake.database", "TEST_DB");
        props.setProperty("snowflake.schema", "PUBLIC");
        props.setProperty("snowflake.table", "TEST_TABLE");
        props.setProperty("snowflake.profile.path", "profile.json");
        props.setProperty("max.rows.per.append", "3");
        try (var out = new java.io.FileOutputStream(tmp)) {
            props.store(out, null);
        }
        return new Config(tmp.getAbsolutePath());
    }

    private void simulateRebalanceThenShutdown(Collection<TopicPartition> partitions) {
        when(kafkaConsumer.poll(any())).thenAnswer(inv -> {
            rebalanceListener.onPartitionsAssigned(partitions);
            runner.shutdown();
            return ConsumerRecords.empty();
        });
    }

    private static TopicPartition tp(int partition) {
        return new TopicPartition(TOPIC, partition);
    }

    private static ConsumerRecord<String, String> record(int partition, long offset, String value) {
        return new ConsumerRecord<>(TOPIC, partition, offset, null, value);
    }

    @SafeVarargs
    private static ConsumerRecords<String, String> buildRecords(ConsumerRecord<String, String>... records) {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> map = new HashMap<>();
        for (ConsumerRecord<String, String> r : records) {
            map.computeIfAbsent(new TopicPartition(r.topic(), r.partition()), k -> new ArrayList<>())
                    .add(r);
        }
        return new ConsumerRecords<>(map);
    }

    private static SFException sfException(int httpStatus) {
        SFException ex = mock(SFException.class);
        when(ex.getHttpStatusCode()).thenReturn(httpStatus);
        lenient().when(ex.getMessage()).thenReturn("Mock SFException HTTP " + httpStatus);
        return ex;
    }
}
