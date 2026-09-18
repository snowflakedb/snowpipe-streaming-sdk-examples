package com.snowflake.example;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ElasticChannelUnboundedTest {
    @AfterEach
    void restoreClientFactory() {
        ElasticChannelIngest.clientFactory = ElasticChannelIngest::openClient;
        ElasticChannelUnbounded.rowCountForTest = null;
        Thread.interrupted();
    }

    @Test
    void mainClosesTheClientAfterFlush() throws Exception {
        FakeChannel channel = new FakeChannel();
        FakeClient client = new FakeClient(channel);
        ElasticChannelIngest.clientFactory = client::asSdk;
        ElasticChannelUnbounded.rowCountForTest = 4L;

        ElasticChannelUnbounded.main(new String[0]);

        assertEquals(4, channel.calls);
        assertEquals(List.of(true), client.waitForFlushOnClose);
        assertEquals(List.of(Duration.ofSeconds(60)), client.closeTimeouts);
    }

    @Test
    void mainClosesAfterInterruptOnThirdAppend() throws Exception {
        FakeChannel channel = new FakeChannel();
        channel.interruptOnCall = 3;
        FakeClient client = new FakeClient(channel);
        ElasticChannelIngest.clientFactory = client::asSdk;
        ElasticChannelUnbounded.rowCountForTest = 10L;

        ElasticChannelUnbounded.main(new String[0]);

        assertEquals(3, channel.calls);
        assertEquals(List.of(true), client.waitForFlushOnClose);
        assertEquals(List.of(Duration.ofSeconds(60)), client.closeTimeouts);
    }

    static final class FakeChannel {
        int calls;
        int interruptOnCall;

        CompletableFuture<Void> appendRowWithWait(Map<String, Object> row, Object token) {
            calls++;
            if (interruptOnCall > 0 && calls == interruptOnCall) {
                Thread.currentThread().interrupt();
            }
            return CompletableFuture.completedFuture(null);
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
                                    case "appendRowWithWait":
                                        return channel.appendRowWithWait(
                                                (Map<String, Object>) args[0], args[1]);
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
}
