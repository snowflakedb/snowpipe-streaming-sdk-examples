package com.snowflake.example;

import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Append immediately; checkpoint all acknowledgements before source handoff.
 * The SDK owns batching. Caller timeouts keep the original Future alive.
 * Retain source events across restarts; Elastic replay may duplicate them.
 */
public class ElasticProducer {
    static final int MAX_PENDING_EVENTS = 100_000;
    static final int CHECKPOINT_ROWS = 1000;
    static final long CHECKPOINT_NANOS = TimeUnit.SECONDS.toNanos(5);
    static final long OUTAGE_NANOS = TimeUnit.MINUTES.toNanos(30);
    static final int MAX_ATTEMPTS = 6;

    // Start here: create a source, connect, and stream retained events.
    public static void main(String[] args) throws Exception {
        SampleEventSource source = new SampleEventSource(
                Long.parseLong(env("SNOWFLAKE_TEST_ROWS", "10000")),
                Long.parseLong(env("SNOWFLAKE_SOURCE_CHECKPOINT", "0")));
        Producer producer = new Producer(ElasticProducer::createClient);
        boolean completed = false;
        try {
            producer.open();
            run(producer, source);
            completed = true;
            System.out.println("Durable source checkpoint: " + source.committed + "; materialization is separate");
        } finally {
            if (!completed) System.err.println("Retain source events after checkpoint " + source.committed);
            producer.close(completed);
        }
    }

    /** Stream retained events while collecting confirmed delivery progress. */
    static void run(Producer producer, SampleEventSource source) throws Exception {
        List<Pending> pending = new ArrayList<>();
        Event event = null;
        boolean exhausted = false;
        int sinceScan = 0;
        long nextScan = System.nanoTime() + CHECKPOINT_NANOS;
        long deadline = System.nanoTime() + OUTAGE_NANOS;
        while (true) {
            long previous = source.committed;
            if (exhausted || event != null || sinceScan >= CHECKPOINT_ROWS || System.nanoTime() >= nextScan
                    || pending.size() >= MAX_PENDING_EVENTS) {
                collectProgress(producer, pending, source, deadline);
                sinceScan = 0;
                nextScan = System.nanoTime() + CHECKPOINT_NANOS;
            }
            if (source.committed > previous || (pending.isEmpty() && event == null)) {
                deadline = System.nanoTime() + OUTAGE_NANOS;
            }
            if (exhausted && pending.isEmpty()) return;
            remaining(deadline);
            if (exhausted || pending.size() >= MAX_PENDING_EVENTS) {
                TimeUnit.NANOSECONDS.sleep(Math.min(TimeUnit.SECONDS.toNanos(1), remaining(deadline)));
                continue;
            }
            if (event == null) event = source.read();
            if (event == null) { exhausted = true; continue; }
            try {
                // SDK acceptance does not retire the retained source event.
                pending.add(new Pending(event, producer.channel.appendRowWithWait(event.row,
                        String.valueOf(event.offset)), producer.generation));
                event = null;
                sinceScan++;
                if (pending.get(pending.size() - 1).future.isCompletedExceptionally()) sinceScan = CHECKPOINT_ROWS;
            } catch (SFException error) {
                if (error.getHttpStatusCode() == 429) {
                    backoff(2, deadline);
                } else {
                    if (!retryable(error)) throw error;
                    if (invalidation(error)) producer.recover(producer.generation);
                    pending.add(appendEvent(producer, event, deadline));
                    event = null;
                }
            }
        }
    }

    /** Collect completed outcomes without waiting for unfinished acknowledgements. */
    static void collectProgress(Producer producer, List<Pending> pending, SampleEventSource source,
                                long deadline) throws Exception {
        for (int index = 0; index < pending.size(); index++) {
            Pending item = pending.get(index);
            if (!item.future.isDone()) continue;
            try {
                item.future.get();
            } catch (ExecutionException failure) {
                if (!(failure.getCause() instanceof SFException)) throw failure;
                SFException error = (SFException) failure.getCause();
                if (!retryable(error) || item.retries >= MAX_ATTEMPTS - 1) throw error;
                if (invalidation(error)) producer.recover(item.generation);
                backoff(item.retries, deadline);
                Pending replacement = appendEvent(producer, item.event, deadline);
                replacement.retries = item.retries + 1;
                pending.set(index, replacement);
            }
        }
        int count = 0;
        while (count < pending.size() && pending.get(count).future.isDone()
                && !pending.get(count).future.isCompletedExceptionally()) count++;
        if (count > 0) {
            source.acknowledge(pending.get(count - 1).event.offset);
            pending.subList(0, count).clear();
        }
    }

    // Supporting delivery and connection details.
    static boolean invalidation(SFException error) {
        String code = error.getErrorCodeName();
        return "InvalidChannelError".equals(code) || "InvalidClientError".equals(code)
                || "ClosedChannelError".equals(code)
                || "ClosedElasticChannelError".equals(code) || "ClosedClientError".equals(code);
    }

    /** Identify SDK failures eligible for bounded application retry. */
    static boolean retryable(SFException error) {
        int status = error.getHttpStatusCode();
        return invalidation(error) || status == 408 || status == 429
                || status == 500 || status == 502 || status == 503 || status == 504;
    }

    /** Return the remaining stalled-progress budget without advancing source progress. */
    static long remaining(long deadline) throws TimeoutException {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) {
            throw new TimeoutException("Stalled-progress deadline exceeded; retain events after source checkpoint for replay");
        }
        return nanos;
    }

    /** Wait with capped jitter without exceeding the remaining checkpoint budget. */
    static void backoff(int attempt, long deadline) throws Exception {
        long cap = Math.min(10000, 250L << Math.min(attempt, 6));
        long delay = TimeUnit.MILLISECONDS.toNanos(ThreadLocalRandom.current().nextLong(cap + 1));
        TimeUnit.NANOSECONDS.sleep(Math.min(delay, remaining(deadline)));
    }

    /** Read an optional setting, using the fallback for missing or blank values. */
    static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    /** Create a table client using the authentication profile or explicitly configured PAT. */
    static SnowflakeStreamingIngestClient createClient() throws Exception {
        Properties properties = new Properties();
        String pat = System.getenv("SNOWFLAKE_PAT");
        if (pat != null && !pat.isBlank()) {
            String account = System.getenv("SNOWFLAKE_ACCOUNT");
            String url = System.getenv("SNOWFLAKE_URL");
            if (account == null || url == null) {
                throw new IllegalArgumentException("PAT mode requires SNOWFLAKE_ACCOUNT and SNOWFLAKE_URL");
            }
            properties.put("authorization_type", "PAT");
            properties.put("personal_access_token", pat);
            properties.put("account", account);
            properties.put("url", url);
            if (System.getenv("SNOWFLAKE_ROLE") != null) {
                properties.put("role", System.getenv("SNOWFLAKE_ROLE"));
            }
        } else {
            JsonNode profile = new ObjectMapper().readTree(Files.readAllBytes(
                    Paths.get(env("SNOWFLAKE_PROFILE", "profile.json"))));
            profile.fields().forEachRemaining(entry -> properties.put(entry.getKey(), entry.getValue().asText()));
        }
        return SnowflakeStreamingIngestClientFactory.tableBuilder(
                "production-" + ProcessHandle.current().pid(), env("SNOWFLAKE_DATABASE", "MY_DATABASE"),
                env("SNOWFLAKE_SCHEMA", "MY_SCHEMA"), env("SNOWFLAKE_TABLE", "MY_TABLE"))
                .setProperties(properties).build();
    }

    /** Pair a stable sample source offset with the row sent to Snowflake. */
    static class Event {
        final long offset;
        final Map<String, Object> row;
        Event(long offset) {
            this.offset = offset;
            // Replace this mapping with your table columns and stable event ID.
            this.row = Map.of("EVENT_ID", offset, "C1", offset, "C2", "event-" + offset);
        }
    }

    /** Synthetic input only. Replace read/acknowledge/seek with retained source operations. */
    static class SampleEventSource {
        final long total;
        long committed;
        long nextOffset;
        SampleEventSource(long total, long checkpoint) {
            if (checkpoint < 0 || checkpoint > total) throw new IllegalArgumentException("Invalid checkpoint");
            this.total = total;
            this.committed = checkpoint;
            this.nextOffset = checkpoint + 1;
        }
        /** Return the next sample event without acknowledging source progress. */
        Event read() { return nextOffset > total ? null : new Event(nextOffset++); }
        /** Record confirmed progress; replace with your source's durable commit operation. */
        void acknowledge(long offset) {
            // Persist source progress before retiring real source events.
            if (offset < committed || offset > total) throw new IllegalArgumentException("Invalid checkpoint");
            committed = offset;
        }
        /** Resume sample reads after confirmed progress; replace with your source seek operation. */
        void seek(long offset) {
            acknowledge(offset);
            nextOffset = offset + 1;
        }
    }


    interface ClientFactory { SnowflakeStreamingIngestClient create() throws Exception; }

    /** Track an event, its acknowledgement, and the client generation that submitted it. */
    static class Pending {
        final Event event;
        final CompletableFuture<Void> future;
        final int generation;
        int retries;
        Pending(Event event, CompletableFuture<Void> future, int generation) {
            this.event = event;
            this.future = future;
            this.generation = generation;
        }
    }

    /** Own the SDK client and channel state needed for recovery. */
    static class Producer {
        final ClientFactory factory;
        SnowflakeStreamingIngestClient client;
        SnowflakeStreamingIngestElasticChannel channel;
        int generation;
        Producer(ClientFactory factory) { this.factory = factory; }
        /** Create a client and obtain its cached Elastic Channel. */
        void open() throws Exception {
            SnowflakeStreamingIngestClient fresh = factory.create();
            try {
                channel = fresh.getElasticChannel();
            } catch (RuntimeException error) {
                fresh.close(false, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
                throw error;
            }
            client = fresh;
            generation++;
        }
        /** Replace the invalid client only if the failure belongs to its current generation. */
        void recover(int failedGeneration) throws Exception {
            // Several failures from one old client must trigger only one replacement.
            if (generation != failedGeneration) return;
            close(false);
            open();
        }
        /** Close the current client; flush only when requested by the caller. */
        void close(boolean flush) throws Exception {
            if (client == null) return;
            try {
                client.close(flush, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            } finally {
                client = null;
            }
        }
    }

    /** Submit one event and retain its acknowledgement for checkpoint confirmation. */
    static Pending appendEvent(Producer producer, Event event, long deadline) throws Exception {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            remaining(deadline);
            try {
                // This SDK call writes the row; preserve the returned Future.
                return new Pending(event, producer.channel.appendRowWithWait(event.row, String.valueOf(event.offset)),
                        producer.generation);
            } catch (SFException error) {
                if (!retryable(error) || attempt == MAX_ATTEMPTS - 1) throw error;
                if (invalidation(error)) producer.recover(producer.generation);
                backoff(attempt, deadline);
            }
        }
        throw new IllegalStateException("Submission retry budget exhausted");
    }

}
