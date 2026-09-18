package com.snowflake.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.ingest.streaming.ErrorDetail;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;
import com.snowflake.ingest.streaming.SuccessDetail;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ElasticChannelIngestTest {
    @AfterEach
    void restoreClientFactory() {
        ElasticChannelIngest.clientFactory = ElasticChannelIngest::openClient;
    }

    @Test
    void connectionPropertiesUsesProfileWithoutPat() {
        assertNull(ElasticChannelIngest.connectionProperties(name -> null));
    }

    @Test
    void connectionPropertiesBuildsPatSettings() {
        Map<String, String> env = Map.of(
                "SNOWFLAKE_PAT", "token",
                "SNOWFLAKE_ACCOUNT", "account",
                "SNOWFLAKE_URL", "https://account.snowflakecomputing.com",
                "SNOWFLAKE_ROLE", "role");

        Properties properties = ElasticChannelIngest.connectionProperties(env::get);

        assertEquals("PAT", properties.getProperty("authorization_type"));
        assertEquals("token", properties.getProperty("personal_access_token"));
        assertEquals("account", properties.getProperty("account"));
        assertEquals("https://account.snowflakecomputing.com", properties.getProperty("url"));
        assertEquals("role", properties.getProperty("role"));
    }

    @Test
    void connectionPropertiesRequiresAccountAndUrl() {
        Map<String, String> env = new HashMap<>();
        env.put("SNOWFLAKE_PAT", "token");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> ElasticChannelIngest.connectionProperties(env::get));
        assertTrue(error.getMessage().contains("SNOWFLAKE_ACCOUNT"));
    }

    @Test
    void sampleRowUsesStableEventId() {
        Map<String, Object> row = ElasticChannelIngest.sampleRow(7);

        assertEquals(7, row.get("EVENT_ID"));
        assertEquals(7, row.get("C1"));
        assertEquals("event-7", row.get("C2"));
    }

    @Test
    void mainClosesTheClientAfterFlush() throws Exception {
        FakeChannel channel = new FakeChannel();
        FakeClient client = new FakeClient(channel);
        ElasticChannelIngest.clientFactory = client::asSdk;

        ElasticChannelIngest.main(new String[0]);

        assertEquals(List.of(true), client.waitForFlushOnClose);
        assertEquals(List.of(Duration.ofSeconds(60)), client.closeTimeouts);
        assertFalse(channel.flushTimeouts.isEmpty());
    }

    static final class FakeChannel {
        final List<Duration> flushTimeouts = new ArrayList<>();
        Consumer<SuccessDetail> successHandler;
        Consumer<ErrorDetail> errorHandler;

        CompletableFuture<Void> appendRowWithWait(Map<String, Object> row, Object token) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> appendRowsWithWait(
                Iterable<Map<String, Object>> rows, Object token) {
            return CompletableFuture.completedFuture(null);
        }

        void appendRow(Map<String, Object> row, Object token) {
            if (successHandler != null) {
                successHandler.accept(successDetail(token));
            }
        }

        void appendRows(Iterable<Map<String, Object>> rows, Object token) {
            if (successHandler != null) {
                successHandler.accept(successDetail(token));
            }
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
                                    case "appendRowsWithWait":
                                        return channel.appendRowsWithWait(
                                                (Iterable<Map<String, Object>>) args[0], args[1]);
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
}
