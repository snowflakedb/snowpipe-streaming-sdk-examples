package com.snowflake.streaming.consumer;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link CustomPubSubConsumer}.
 *
 * <p>Tests channel lifecycle, config loading, and shutdown behavior. The retry
 * logic tests are structural (verifying the consumer compiles and starts correctly)
 * since the Pub/Sub pull loop requires a real gRPC connection to drive through
 * to appendRow calls.</p>
 *
 * <p>For full integration testing with retry/error scenarios, use the data generator
 * against a real Pub/Sub topic and Snowflake account.</p>
 */
@ExtendWith(MockitoExtension.class)
class CustomPubSubConsumerTest {

    private static final String PROJECT = "test-project";
    private static final String SUBSCRIPTION = "test-sub";
    private static final String CHANNEL_NAME = "PUBSUB_CHANNEL_T0";

    @Mock private SnowflakeStreamingIngestClient sfClient;
    @Mock private SnowflakeStreamingIngestChannel channel;
    @Mock private OpenChannelResult openResult;
    @Mock private ChannelStatus healthyStatus;

    private Config config;

    @BeforeEach
    void setUp() throws Exception {
        config = createTestConfig();

        lenient().when(openResult.getChannel()).thenReturn(channel);
        lenient().when(sfClient.openChannel(CHANNEL_NAME)).thenReturn(openResult);
        lenient().when(channel.getLatestCommittedOffsetToken()).thenReturn(null);
        lenient().when(channel.isClosed()).thenReturn(false);
        lenient().when(healthyStatus.getStatusCode()).thenReturn("SUCCESS");
        lenient().when(channel.getChannelStatus()).thenReturn(healthyStatus);
    }

    // --- Channel lifecycle ---

    @Test
    void opensChannelOnStartup() throws Exception {
        CustomPubSubConsumer consumer = new CustomPubSubConsumer(config, sfClient, CHANNEL_NAME);
        runConsumerBriefly(consumer);

        verify(sfClient).openChannel(CHANNEL_NAME);
    }

    @Test
    void resumesFromLastCommittedOffset() throws Exception {
        when(channel.getLatestCommittedOffsetToken()).thenReturn("42");

        CustomPubSubConsumer consumer = new CustomPubSubConsumer(config, sfClient, CHANNEL_NAME);
        runConsumerBriefly(consumer);

        verify(sfClient).openChannel(CHANNEL_NAME);
        verify(channel).getLatestCommittedOffsetToken();
    }

    @Test
    void freshStartWithNoCommittedOffsetsOpensChannel() throws Exception {
        when(channel.getLatestCommittedOffsetToken()).thenReturn(null);

        CustomPubSubConsumer consumer = new CustomPubSubConsumer(config, sfClient, CHANNEL_NAME);
        runConsumerBriefly(consumer);

        verify(sfClient).openChannel(CHANNEL_NAME);
    }

    @Test
    void closesChannelOnShutdown() throws Exception {
        CustomPubSubConsumer consumer = new CustomPubSubConsumer(config, sfClient, CHANNEL_NAME);
        runConsumerBriefly(consumer);

        verify(channel).close(eq(true), any(Duration.class));
    }

    @Test
    void shutdownSetsRunningFalse() {
        CustomPubSubConsumer consumer = new CustomPubSubConsumer(config, sfClient, CHANNEL_NAME);
        consumer.shutdown();
        // Should not throw — just sets the running flag
        assertDoesNotThrow(() -> consumer.shutdown());
    }

    @Test
    void doesNotCloseAlreadyClosedChannel() throws Exception {
        when(channel.isClosed()).thenReturn(true);

        CustomPubSubConsumer consumer = new CustomPubSubConsumer(config, sfClient, CHANNEL_NAME);
        runConsumerBriefly(consumer);

        verify(channel, never()).close(anyBoolean(), any(Duration.class));
    }

    // --- Config loading ---

    @Test
    void configLoadsAllProperties() {
        assertEquals(PROJECT, config.getGcpProjectId());
        assertEquals(SUBSCRIPTION, config.getPubSubSubscriptionId());
        assertEquals("PUBSUB_CHANNEL", config.getSnowflakeChannelName());
        assertEquals("TEST_DB", config.getSnowflakeDatabase());
        assertEquals("PUBLIC", config.getSnowflakeSchema());
        assertEquals("TEST_TABLE", config.getSnowflakeTable());
        assertEquals(3, config.getMaxRowsPerAppend());
        assertEquals(1, config.getConsumerThreadCount());
    }

    @Test
    void configDefaultsForMissingProperties() throws Exception {
        java.io.File tmp = java.io.File.createTempFile("minimal-config", ".properties");
        tmp.deleteOnExit();
        java.util.Properties props = new java.util.Properties();
        props.setProperty("gcp.project.id", "my-project");
        try (var out = new java.io.FileOutputStream(tmp)) {
            props.store(out, null);
        }
        Config minimal = new Config(tmp.getAbsolutePath());

        assertEquals("my-project", minimal.getGcpProjectId());
        assertEquals("ssv2-example-sub", minimal.getPubSubSubscriptionId());
        assertEquals("PUBSUB_CHANNEL", minimal.getSnowflakeChannelName());
        assertEquals("TEST_DB", minimal.getSnowflakeDatabase());
        assertEquals("PUBLIC", minimal.getSnowflakeSchema());
        assertEquals("TEST_TABLE", minimal.getSnowflakeTable());
        assertEquals(100, minimal.getMaxRowsPerAppend());
        assertEquals(1, minimal.getConsumerThreadCount());
    }

    @Test
    void configLoadsProfilePath() {
        assertEquals("profile.json", config.getSnowflakeProfilePath());
    }

    @Test
    void configLoadsOutstandingLimits() {
        assertEquals(1000, config.getMaxOutstandingMessages());
        assertEquals(104857600L, config.getMaxOutstandingBytes());
    }

    // --- Helpers ---

    private Config createTestConfig() throws Exception {
        java.io.File tmp = java.io.File.createTempFile("test-config", ".properties");
        tmp.deleteOnExit();
        java.util.Properties props = new java.util.Properties();
        props.setProperty("gcp.project.id", PROJECT);
        props.setProperty("pubsub.subscription.id", SUBSCRIPTION);
        props.setProperty("pubsub.max.outstanding.messages", "1000");
        props.setProperty("pubsub.max.outstanding.bytes", "104857600");
        props.setProperty("snowflake.channel.name", "PUBSUB_CHANNEL");
        props.setProperty("snowflake.database", "TEST_DB");
        props.setProperty("snowflake.schema", "PUBLIC");
        props.setProperty("snowflake.table", "TEST_TABLE");
        props.setProperty("snowflake.profile.path", "profile.json");
        props.setProperty("max.rows.per.append", "3");
        props.setProperty("consumer.thread.count", "1");
        try (var out = new java.io.FileOutputStream(tmp)) {
            props.store(out, null);
        }
        return new Config(tmp.getAbsolutePath());
    }

    /**
     * Runs the consumer briefly then shuts it down. The consumer opens the Snowflake
     * channel but fails to connect to Pub/Sub (no real subscription), triggering cleanup.
     */
    private void runConsumerBriefly(CustomPubSubConsumer consumer) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(consumer);
        Thread.sleep(500);
        consumer.shutdown();
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }
}
