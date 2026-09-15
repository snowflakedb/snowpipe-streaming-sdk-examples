package com.snowflake.example;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ElasticRecoveryContractTest {
    static SFException failure(String code, int status) {
        return new SFException(code, "synthetic", status, String.valueOf(status));
    }
    static long deadline() { return System.nanoTime() + TimeUnit.SECONDS.toNanos(10); }

    static class FakeClient {
        final List<String> calls = new ArrayList<>();
        final List<Object> outcomes = new ArrayList<>();
        int closes;
        FakeClient(Object... outcomes) { this.outcomes.addAll(Arrays.asList(outcomes)); }
        final SnowflakeStreamingIngestElasticChannel channel = (SnowflakeStreamingIngestElasticChannel)
                Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{SnowflakeStreamingIngestElasticChannel.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("appendRowWithWait")) {
                                calls.add((String) args[1]);
                                Object outcome = outcomes.isEmpty() ? CompletableFuture.completedFuture(null) : outcomes.remove(0);
                                if (outcome instanceof SFException) throw (SFException) outcome;
                                return outcome;
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
        final SnowflakeStreamingIngestClient client = (SnowflakeStreamingIngestClient)
                Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{SnowflakeStreamingIngestClient.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("getElasticChannel")) return channel;
                            if (method.getName().equals("close")) { closes++; return CompletableFuture.completedFuture(null); }
                            throw new UnsupportedOperationException(method.getName());
                        });
    }

    @Test
    void appendPrecedesNextReadAndFinalAckCoversAllEvents() throws Exception {
        FakeClient fake = new FakeClient();
        ElasticProducer.Producer session = new ElasticProducer.Producer(() -> fake.client);
        session.open();
        ElasticProducer.SampleEventSource source = new ElasticProducer.SampleEventSource(3, 0) {
            @Override ElasticProducer.Event read() {
                assertEquals(nextOffset - 1, fake.calls.size());
                return super.read();
            }
        };
        ElasticProducer.run(session, source);
        assertEquals(Arrays.asList("1", "2", "3"), fake.calls);
        assertEquals(3, source.committed);
    }

    @Test
    void lateAckDoesNotResubmitOrRecreate() throws Exception {
        CompletableFuture<Void> late = new CompletableFuture<>();
        FakeClient fake = new FakeClient(late);
        ElasticProducer.Producer session = new ElasticProducer.Producer(() -> fake.client);
        session.open();
        ElasticProducer.SampleEventSource source = new ElasticProducer.SampleEventSource(1, 0);
        List<ElasticProducer.Pending> pending = new ArrayList<>();
        pending.add(ElasticProducer.appendEvent(session, source.read(), deadline()));
        ElasticProducer.collectProgress(session, pending, source, deadline());
        assertEquals(0, source.committed);
        late.complete(null);
        ElasticProducer.collectProgress(session, pending, source, deadline());
        assertEquals(1, fake.calls.size());
        assertEquals(0, fake.closes);
        assertEquals(1, source.committed);
    }

    @Test
    void outageDeadlineDoesNotAdvanceOrCancelPendingWork() throws Exception {
        FakeClient fake = new FakeClient();
        ElasticProducer.Producer session = new ElasticProducer.Producer(() -> fake.client);
        session.open();
        CompletableFuture<Void> waiting = new CompletableFuture<>();
        ElasticProducer.SampleEventSource source = new ElasticProducer.SampleEventSource(1, 0);
        List<ElasticProducer.Pending> pending = new ArrayList<>();
        pending.add(new ElasticProducer.Pending(source.read(), waiting, session.generation));
        assertThrows(TimeoutException.class,
                () -> ElasticProducer.remaining(System.nanoTime() - 1));
        assertEquals(0, source.committed);
        assertFalse(waiting.isCancelled());
        assertEquals(1, pending.size());
    }

    @Test
    void backpressureMidstreamPreservesPriorFutures() throws Exception {
        FakeClient fake = new FakeClient(CompletableFuture.completedFuture(null), failure("ReceiverSaturated", 429));
        ElasticProducer.Producer session = new ElasticProducer.Producer(() -> fake.client);
        session.open();
        ElasticProducer.SampleEventSource source = new ElasticProducer.SampleEventSource(2, 0);
        ElasticProducer.run(session, source);
        assertEquals(Arrays.asList("1", "2", "2"), fake.calls);
        assertEquals(0, fake.closes);
        assertEquals(2, source.committed);
    }

    @Test
    void invalidationSkipsAcknowledgedRowsAndRebuildsOncePerGeneration() throws Exception {
        FakeClient old = new FakeClient(CompletableFuture.completedFuture(null),
                CompletableFuture.failedFuture(failure("InvalidChannelError", 409)),
                CompletableFuture.failedFuture(failure("InvalidClientError", 409)));
        FakeClient fresh = new FakeClient();
        AtomicInteger builds = new AtomicInteger();
        ElasticProducer.Producer session = new ElasticProducer.Producer(() -> builds.getAndIncrement() == 0 ? old.client : fresh.client);
        session.open();
        ElasticProducer.SampleEventSource source = new ElasticProducer.SampleEventSource(3, 0);
        List<ElasticProducer.Pending> pending = new ArrayList<>();
        for (int index = 0; index < 3; index++) pending.add(ElasticProducer.appendEvent(session, source.read(), deadline()));
        ElasticProducer.collectProgress(session, pending, source, deadline());
        assertEquals(Arrays.asList("2", "3"), fresh.calls);
        assertEquals(2, builds.get());
        assertEquals(1, old.closes);
        assertEquals(3, source.committed);
    }

    @Test
    void permanentFailurePreservesSourceCheckpoint() throws Exception {
        FakeClient fake = new FakeClient(CompletableFuture.failedFuture(failure("SfApiUserError", 403)));
        ElasticProducer.Producer session = new ElasticProducer.Producer(() -> fake.client);
        session.open();
        ElasticProducer.SampleEventSource source = new ElasticProducer.SampleEventSource(2, 0);
        assertThrows(SFException.class, () -> ElasticProducer.run(session, source));
        assertEquals(0, source.committed);
        assertEquals(Arrays.asList("1"), fake.calls);
    }

    static ChannelStatus status(long offset, long errors) {
        return new ChannelStatus("DB", "SCHEMA", "PIPE", "CHANNEL", "SUCCESS", String.valueOf(offset),
                Instant.EPOCH, offset, offset, errors, null, null, null, null, Instant.EPOCH);
    }

    static class NamedFake {
        long committed;
        int opens;
        int closes;
        long errors;
        boolean failThird;
        String invalidationCode = "InvalidChannelError";
        boolean backpressure;
        final List<Long> calls = new ArrayList<>();
        NamedFake(long committed) { this.committed = committed; }
        final SnowflakeStreamingIngestChannel channel = (SnowflakeStreamingIngestChannel)
                Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{SnowflakeStreamingIngestChannel.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("appendRow")) {
                                long offset = Long.parseLong((String) args[1]);
                                calls.add(offset);
                                if (backpressure) { backpressure = false; throw failure("ReceiverSaturated", 429); }
                                if (failThird && offset == 3) { failThird = false; throw failure(invalidationCode, 409); }
                                committed = offset;
                                return null;
                            }
                            if (method.getName().equals("getChannelStatus")) return status(committed, errors);
                            if (method.getName().equals("close")) { closes++; return null; }
                            throw new UnsupportedOperationException(method.getName());
                        });
        final SnowflakeStreamingIngestClient client = (SnowflakeStreamingIngestClient)
                Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{SnowflakeStreamingIngestClient.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("openChannel")) {
                                assertEquals(1, args.length, "Never overwrite a server offset on reopen");
                                opens++;
                                return new OpenChannelResult(channel, status(committed, errors));
                            }
                            if (method.getName().equals("close")) return CompletableFuture.completedFuture(null);
                            throw new UnsupportedOperationException(method.getName());
                        });
    }

    @Test
    void namedRestartSeeksAfterServerOffset() throws Exception {
        NamedFake fake = new NamedFake(2);
        NamedChannelCheckpoint.SampleEventSource source = new NamedChannelCheckpoint.SampleEventSource(5, 0);
        NamedChannelCheckpoint.run(new NamedChannelCheckpoint.Producer(() -> fake.client), source);
        assertEquals(Arrays.asList(3L, 4L, 5L), fake.calls);
        assertEquals(5, source.committed);
        assertEquals(1, fake.opens);
    }

    @Test
    void namedInvalidationReplaysOnlyBeyondCommittedOffset() throws Exception {
        NamedFake fake = new NamedFake(0);
        fake.failThird = true;
        NamedChannelCheckpoint.SampleEventSource source = new NamedChannelCheckpoint.SampleEventSource(4, 0);
        NamedChannelCheckpoint.run(new NamedChannelCheckpoint.Producer(() -> fake.client), source);
        assertEquals(Arrays.asList(1L, 2L, 3L, 3L, 4L), fake.calls);
        assertEquals(2, fake.opens);
        assertEquals(1, fake.closes);
        assertEquals(4, source.committed);
    }

    @Test
    void namedBackpressureRetriesCurrentEventWithoutReopen() throws Exception {
        NamedFake fake = new NamedFake(0);
        fake.backpressure = true;
        NamedChannelCheckpoint.SampleEventSource source = new NamedChannelCheckpoint.SampleEventSource(2, 0);
        NamedChannelCheckpoint.run(new NamedChannelCheckpoint.Producer(() -> fake.client), source);
        assertEquals(Arrays.asList(1L, 1L, 2L), fake.calls);
        assertEquals(1, fake.opens);
        assertEquals(2, source.committed);
    }

    @Test
    void namedClosedChannelRecoversFromCommittedOffset() throws Exception {
        NamedFake fake = new NamedFake(0);
        fake.failThird = true;
        fake.invalidationCode = "ClosedChannelError";
        NamedChannelCheckpoint.SampleEventSource source = new NamedChannelCheckpoint.SampleEventSource(4, 0);
        NamedChannelCheckpoint.run(new NamedChannelCheckpoint.Producer(() -> fake.client), source);
        assertEquals(Arrays.asList(1L, 2L, 3L, 3L, 4L), fake.calls);
        assertEquals(2, fake.opens);
        assertEquals(4, source.committed);
    }

    @Test
    void namedRowErrorsPreventSourceHandoff() {
        NamedFake fake = new NamedFake(0);
        fake.errors = 1;
        NamedChannelCheckpoint.SampleEventSource source = new NamedChannelCheckpoint.SampleEventSource(2, 0);
        assertThrows(IllegalStateException.class,
                () -> NamedChannelCheckpoint.run(new NamedChannelCheckpoint.Producer(() -> fake.client), source));
        assertEquals(0, source.committed);
    }
}
