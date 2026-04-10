package com.snowflake.streaming.consumer;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link CustomSQSConsumer}.
 *
 * <p>Tests channel lifecycle, retry behavior, config loading, message processing,
 * and shutdown semantics. Uses Mockito for SQS and Snowflake SDK mocks so no
 * real AWS or Snowflake connection is required.</p>
 */
@ExtendWith(MockitoExtension.class)
class CustomSQSConsumerTest {

    private static final String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";
    private static final String CHANNEL_NAME = "SQS_CHANNEL_T0";

    @Mock private SnowflakeStreamingIngestClient sfClient;
    @Mock private SnowflakeStreamingIngestChannel channel;
    @Mock private OpenChannelResult openResult;
    @Mock private ChannelStatus healthyStatus;
    @Mock private SqsClient sqsClient;

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

        // Default: empty queue so the consumer loop exits cleanly after the channel opens
        lenient().when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(List.of()).build());
        lenient().when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().failed(List.of()).successful(List.of()).build());
    }

    // --- Channel lifecycle ---

    @Test
    void opensChannelOnStartup() throws Exception {
        runConsumerBriefly(buildConsumer());
        verify(sfClient).openChannel(CHANNEL_NAME);
    }

    @Test
    void resumesFromLastCommittedOffset() throws Exception {
        when(channel.getLatestCommittedOffsetToken()).thenReturn("42");
        runConsumerBriefly(buildConsumer());
        verify(channel).getLatestCommittedOffsetToken();
    }

    @Test
    void freshStartWithNoCommittedOffsetOpensChannel() throws Exception {
        when(channel.getLatestCommittedOffsetToken()).thenReturn(null);
        runConsumerBriefly(buildConsumer());
        verify(sfClient).openChannel(CHANNEL_NAME);
    }

    @Test
    void closesChannelWithFlushOnShutdown() throws Exception {
        runConsumerBriefly(buildConsumer());
        verify(channel).close(eq(true), any(Duration.class));
    }

    @Test
    void doesNotCloseAlreadyClosedChannel() throws Exception {
        when(channel.isClosed()).thenReturn(true);
        runConsumerBriefly(buildConsumer());
        verify(channel, never()).close(anyBoolean(), any(Duration.class));
    }

    @Test
    void shutdownSetsRunningFalseAndClosesSqsClient() {
        CustomSQSConsumer consumer = buildConsumer();
        consumer.shutdown();
        verify(sqsClient).close();
        // Idempotent — second call must not throw
        assertDoesNotThrow(consumer::shutdown);
    }

    // --- Message processing ---

    @Test
    void appendRowCalledForEachMessageInBatch() throws Exception {
        String json1 = "{\"record_id\":1,\"caller_number\":\"+12125550001\"}";
        String json2 = "{\"record_id\":2,\"caller_number\":\"+12125550002\"}";

        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder()
                        .messages(
                                Message.builder().body(json1).receiptHandle("rh1").build(),
                                Message.builder().body(json2).receiptHandle("rh2").build())
                        .build())
                .thenReturn(ReceiveMessageResponse.builder().messages(List.of()).build());

        runConsumerBriefly(buildConsumer());

        verify(channel, atLeast(2)).appendRow(anyMap(), anyString());
    }

    @Test
    void deleteMessageBatchCalledAfterSuccessfulAppends() throws Exception {
        String json = "{\"record_id\":1}";
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder()
                        .messages(Message.builder().body(json).receiptHandle("rh-abc").build())
                        .build())
                .thenReturn(ReceiveMessageResponse.builder().messages(List.of()).build());

        runConsumerBriefly(buildConsumer());

        ArgumentCaptor<DeleteMessageBatchRequest> captor =
                ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        verify(sqsClient, atLeastOnce()).deleteMessageBatch(captor.capture());
        DeleteMessageBatchRequest req = captor.getValue();
        assertEquals(QUEUE_URL, req.queueUrl());
        assertEquals("rh-abc", req.entries().get(0).receiptHandle());
    }

    @Test
    void messagesNotDeletedIfAppendRowThrows() throws Exception {
        String json = "{\"record_id\":1}";
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder()
                        .messages(Message.builder().body(json).receiptHandle("rh-xyz").build())
                        .build());

        SFException fatal = new SFException("ERR_AUTH", "unauthorized", 401, "Unauthorized");
        doThrow(fatal).when(channel).appendRow(anyMap(), anyString());

        runConsumerBriefly(buildConsumer());

        verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void malformedJsonFallsBackToRawData() throws Exception {
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder()
                        .messages(Message.builder().body("not json").receiptHandle("rh1").build())
                        .build())
                .thenReturn(ReceiveMessageResponse.builder().messages(List.of()).build());

        runConsumerBriefly(buildConsumer());

        // appendRow should have been called with a map containing "raw_data"
        ArgumentCaptor<java.util.Map<String, Object>> rowCaptor =
                ArgumentCaptor.forClass(java.util.Map.class);
        verify(channel, atLeastOnce()).appendRow(rowCaptor.capture(), anyString());
        assertTrue(rowCaptor.getValue().containsKey("raw_data"));
    }

    // --- Retry logic ---

    @Test
    void retryOn409ReopensChannelAndContinues() throws Exception {
        String json = "{\"record_id\":1}";
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder()
                        .messages(Message.builder().body(json).receiptHandle("rh1").build())
                        .build())
                .thenReturn(ReceiveMessageResponse.builder().messages(List.of()).build());

        OpenChannelResult secondOpenResult = mock(OpenChannelResult.class);
        SnowflakeStreamingIngestChannel newChannel = mock(SnowflakeStreamingIngestChannel.class);
        when(secondOpenResult.getChannel()).thenReturn(newChannel);
        when(newChannel.getLatestCommittedOffsetToken()).thenReturn(null);
        when(newChannel.isClosed()).thenReturn(false);

        // First call throws 409; second open returns newChannel; its appendRow succeeds
        when(sfClient.openChannel(CHANNEL_NAME))
                .thenReturn(openResult)      // initial open
                .thenReturn(secondOpenResult); // reopen after 409
        doThrow(new SFException("ERR_CONFLICT", "channel invalidated", 409, "Conflict"))
                .doNothing()
                .when(channel).appendRow(anyMap(), anyString());

        runConsumerBriefly(buildConsumer());

        // openChannel called twice: once on startup, once for reopen
        verify(sfClient, times(2)).openChannel(CHANNEL_NAME);
    }

    @Test
    void immediateFailOn401() throws Exception {
        String json = "{\"record_id\":1}";
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder()
                        .messages(Message.builder().body(json).receiptHandle("rh1").build())
                        .build());

        doThrow(new SFException("ERR_AUTH", "unauthorized", 401, "Unauthorized")).when(channel).appendRow(anyMap(), anyString());

        // Consumer should exit with error — openChannel called only once (no reopen)
        runConsumerBriefly(buildConsumer());
        verify(sfClient, times(1)).openChannel(CHANNEL_NAME);
    }

    @Test
    void immediateFailOn403() throws Exception {
        String json = "{\"record_id\":1}";
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder()
                        .messages(Message.builder().body(json).receiptHandle("rh1").build())
                        .build());

        doThrow(new SFException("ERR_AUTH", "forbidden", 403, "Forbidden")).when(channel).appendRow(anyMap(), anyString());

        runConsumerBriefly(buildConsumer());
        verify(sfClient, times(1)).openChannel(CHANNEL_NAME);
    }

    // --- Channel health check ---

    @Test
    void unhealthyChannelTriggersReopen() throws Exception {
        ChannelStatus unhealthy = mock(ChannelStatus.class);
        when(unhealthy.getStatusCode()).thenReturn("INVALIDATED");
        when(channel.getChannelStatus()).thenReturn(unhealthy);

        OpenChannelResult secondOpen = mock(OpenChannelResult.class);
        SnowflakeStreamingIngestChannel newChannel = mock(SnowflakeStreamingIngestChannel.class);
        when(secondOpen.getChannel()).thenReturn(newChannel);
        when(newChannel.getLatestCommittedOffsetToken()).thenReturn(null);
        when(newChannel.isClosed()).thenReturn(false);
        when(sfClient.openChannel(CHANNEL_NAME))
                .thenReturn(openResult)
                .thenReturn(secondOpen);

        runConsumerBriefly(buildConsumer());

        verify(sfClient, times(2)).openChannel(CHANNEL_NAME);
    }

    // --- Config ---

    @Test
    void configLoadsAllProperties() {
        assertEquals("us-east-1", config.getAwsRegion());
        assertEquals(QUEUE_URL, config.getSqsQueueUrl());
        assertEquals(10, config.getSqsMaxMessages());
        assertEquals(20, config.getSqsWaitTimeSeconds());
        assertEquals(30, config.getSqsVisibilityTimeoutSeconds());
        assertEquals("SQS_CHANNEL", config.getSnowflakeChannelName());
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
        props.setProperty("sqs.queue.url", QUEUE_URL);
        try (var out = new java.io.FileOutputStream(tmp)) {
            props.store(out, null);
        }
        Config minimal = new Config(tmp.getAbsolutePath());

        assertEquals("us-east-1", minimal.getAwsRegion());
        assertEquals(10, minimal.getSqsMaxMessages());
        assertEquals(20, minimal.getSqsWaitTimeSeconds());
        assertEquals(30, minimal.getSqsVisibilityTimeoutSeconds());
        assertEquals("SQS_CHANNEL", minimal.getSnowflakeChannelName());
        assertEquals("TEST_DB", minimal.getSnowflakeDatabase());
        assertEquals("PUBLIC", minimal.getSnowflakeSchema());
        assertEquals("TEST_TABLE", minimal.getSnowflakeTable());
        assertEquals(50, minimal.getMaxRowsPerAppend());
        assertEquals(1, minimal.getConsumerThreadCount());
    }

    @Test
    void configRejectsInvalidMaxMessages() throws Exception {
        java.io.File tmp = java.io.File.createTempFile("bad-config", ".properties");
        tmp.deleteOnExit();
        java.util.Properties props = new java.util.Properties();
        props.setProperty("sqs.queue.url", QUEUE_URL);
        props.setProperty("sqs.max.messages", "11");
        try (var out = new java.io.FileOutputStream(tmp)) {
            props.store(out, null);
        }
        Config badConfig = new Config(tmp.getAbsolutePath());
        assertThrows(IllegalArgumentException.class, badConfig::getSqsMaxMessages);
    }

    // --- Helpers ---

    private CustomSQSConsumer buildConsumer() {
        return new CustomSQSConsumer(config, sfClient, CHANNEL_NAME, sqsClient);
    }

    private Config createTestConfig() throws Exception {
        java.io.File tmp = java.io.File.createTempFile("test-config", ".properties");
        tmp.deleteOnExit();
        java.util.Properties props = new java.util.Properties();
        props.setProperty("aws.region", "us-east-1");
        props.setProperty("sqs.queue.url", QUEUE_URL);
        props.setProperty("sqs.max.messages", "10");
        props.setProperty("sqs.wait.time.seconds", "20");
        props.setProperty("sqs.visibility.timeout.seconds", "30");
        props.setProperty("snowflake.channel.name", "SQS_CHANNEL");
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
     * Runs the consumer briefly, then shuts it down. The consumer opens the Snowflake
     * channel and polls the mocked SQS client before shutdown() stops the loop.
     */
    private void runConsumerBriefly(CustomSQSConsumer consumer) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(consumer);
        Thread.sleep(300);
        consumer.shutdown();
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }
}
