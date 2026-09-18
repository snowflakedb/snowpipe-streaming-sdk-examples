package com.snowflake.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.snowflake.ingest.streaming.ErrorDetail;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;
import com.snowflake.ingest.streaming.SuccessDetail;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ElasticChannelCallbacksTest {
    @AfterEach
    void restoreClientFactory() {
        ElasticChannelIngest.clientFactory = ElasticChannelIngest::openClient;
    }

    @Test
    void ackInboxWaitOneReturnsToken() throws Exception {
        ElasticChannelCallbacks.AckInbox inbox = new ElasticChannelCallbacks.AckInbox();
        inbox.onSuccess(successDetail("t1"));

        assertEquals("t1", inbox.waitOne(1, TimeUnit.SECONDS));
    }

    @Test
    void ackInboxWaitOneRaisesHandlerError() {
        ElasticChannelCallbacks.AckInbox inbox = new ElasticChannelCallbacks.AckInbox();
        SFException boom = new SFException("Internal", "boom", 500, "boom");
        inbox.onError(errorDetail(boom));

        assertEquals(boom, assertThrows(SFException.class, () -> inbox.waitOne(1, TimeUnit.SECONDS)));
    }

    @Test
    void ackInboxTimesOutWithoutAnEvent() {
        ElasticChannelCallbacks.AckInbox inbox = new ElasticChannelCallbacks.AckInbox();

        assertThrows(TimeoutException.class, () -> inbox.waitOne(10, TimeUnit.MILLISECONDS));
    }

    @Test
    void mainClosesTheClientAfterFlush() throws Exception {
        FakeChannel channel = new FakeChannel();
        FakeClient client = new FakeClient(channel);
        ElasticChannelIngest.clientFactory = client::asSdk;

        ElasticChannelCallbacks.main(new String[0]);

        assertEquals(List.of(true), client.waitForFlushOnClose);
        assertEquals(List.of(Duration.ofSeconds(60)), client.closeTimeouts);
        assertFalse(channel.flushTimeouts.isEmpty());
    }

    static final class FakeChannel {
        final List<Duration> flushTimeouts = new ArrayList<>();
        Consumer<SuccessDetail> successHandler;
        Consumer<ErrorDetail> errorHandler;

        void complete(Object token) {
            if (successHandler != null) {
                successHandler.accept(successDetail(token));
            }
        }

        void appendRow(Map<String, Object> row, Object token) {
            complete(token);
        }

        void appendRows(Iterable<Map<String, Object>> rows, Object token) {
            complete(token);
        }
    }

    static final class FakeClient {
        final FakeChannel channel;
        final List<Boolean> waitForFlushOnClose = new ArrayList<>();
        final List<Duration> closeTimeouts = new ArrayList<>();

        FakeClient(FakeChannel channel) {
            this.channel = channel;
        }

        @SuppressWarnings("unchecked")
        SnowflakeStreamingIngestClient asSdk() {
            SnowflakeStreamingIngestElasticChannel sdkChannel =
                    (SnowflakeStreamingIngestElasticChannel) Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[] {SnowflakeStreamingIngestElasticChannel.class},
                            (proxy, method, args) -> {
                                switch (method.getName()) {
                                    case "appendRow":
                                        channel.appendRow((Map<String, Object>) args[0], args[1]);
                                        return null;
                                    case "appendRows":
                                        channel.appendRows(
                                                (Iterable<Map<String, Object>>) args[0], args[1]);
                                        return null;
                                    case "setSuccessHandler":
                                        channel.successHandler = (Consumer<SuccessDetail>) args[0];
                                        return null;
                                    case "setErrorHandler":
                                        channel.errorHandler = (Consumer<ErrorDetail>) args[0];
                                        return null;
                                    case "waitForFlush":
                                        channel.flushTimeouts.add((Duration) args[0]);
                                        return CompletableFuture.completedFuture(null);
                                    case "toString":
                                        return "fake-elastic-channel";
                                    case "hashCode":
                                        return System.identityHashCode(proxy);
                                    case "equals":
                                        return proxy == args[0];
                                    default:
                                        throw new UnsupportedOperationException(method.getName());
                                }
                            });
            return (SnowflakeStreamingIngestClient) Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[] {SnowflakeStreamingIngestClient.class},
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "getElasticChannel":
                                return sdkChannel;
                            case "close":
                                if (method.getParameterCount() == 0) {
                                    waitForFlushOnClose.add(true);
                                    return null;
                                }
                                waitForFlushOnClose.add((Boolean) args[0]);
                                closeTimeouts.add((Duration) args[1]);
                                return CompletableFuture.completedFuture(null);
                            case "toString":
                                return "fake-client";
                            case "hashCode":
                                return System.identityHashCode(proxy);
                            case "equals":
                                return proxy == args[0];
                            default:
                                throw new UnsupportedOperationException(method.getName());
                        }
                    });
        }
    }

    static SuccessDetail successDetail(Object token) {
        return new SuccessDetail() {
            @Override
            public Iterable<Object> getAppendTokens() {
                return List.of(token);
            }

            @Override
            public String getRequestId() {
                return "req";
            }

            @Override
            public int getRetryCount() {
                return 0;
            }
        };
    }

    static ErrorDetail errorDetail(SFException error) {
        return new ErrorDetail() {
            @Override
            public Iterable<Object> getAppendTokens() {
                return List.of();
            }

            @Override
            public SFException getError() {
                return error;
            }

            @Override
            public String getRequestId() {
                return null;
            }

            @Override
            public int getRetryCount() {
                return 0;
            }
        };
    }
}
