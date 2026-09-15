package com.snowflake.example;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
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

/**
 * Single-writer named-channel producer. Stream rows immediately and retain source
 * events until their committed offset is confirmed. Outage pauses intake; only
 * SDK invalidation reopens. Do not share ownership of the same channel.
 */
public class NamedChannelCheckpoint {
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
        Producer producer = new Producer(NamedChannelCheckpoint::createClient);
        boolean completed = false;
        try {
            run(producer, source);
            completed = true;
            System.out.println("Committed source checkpoint: " + source.committed);
        } finally {
            if (!completed) System.err.println("Retain source events after checkpoint " + source.committed);
            producer.close(completed);
        }
    }

    /** Stream retained events while collecting confirmed delivery progress. */
    static void run(Producer producer, SampleEventSource source) throws Exception {
        source.seek(producer.open());
        long submitted = source.committed;
        Event event = null;
        boolean exhausted = false;
        int failures = 0;
        int sincePoll = 0;
        long nextPoll = System.nanoTime() + CHECKPOINT_NANOS;
        long deadline = System.nanoTime() + OUTAGE_NANOS;
        while (true) {
            try {
                if (submitted > source.committed && (exhausted || event != null || sincePoll >= CHECKPOINT_ROWS
                        || System.nanoTime() >= nextPoll || submitted - source.committed >= MAX_PENDING_EVENTS)) {
                    long previous = source.committed;
                    collectProgress(producer, submitted, source);
                    if (source.committed > previous) { deadline = System.nanoTime() + OUTAGE_NANOS; failures = 0; }
                    nextPoll = System.nanoTime() + CHECKPOINT_NANOS;
                    sincePoll = 0;
                }
                if (submitted == source.committed && event == null) {
                    deadline = System.nanoTime() + OUTAGE_NANOS;
                    if (exhausted) return;
                }
                remaining(deadline);
                if (exhausted || submitted - source.committed >= MAX_PENDING_EVENTS) {
                    TimeUnit.NANOSECONDS.sleep(Math.min(TimeUnit.SECONDS.toNanos(1), remaining(deadline)));
                    continue;
                }
                if (event == null) event = source.read();
                if (event == null) { exhausted = true; continue; }
                producer.channel.appendRow(event.row, String.valueOf(event.offset));
                submitted = event.offset;
                event = null;
                sincePoll++;
            } catch (SFException error) {
                if (!retryable(error)) throw error;
                if (error.getHttpStatusCode() != 429 && ++failures >= MAX_ATTEMPTS) throw error;
                if (invalidation(error)) {
                    long previous = source.committed;
                    source.seek(producer.recover(error));
                    if (source.committed > previous) deadline = System.nanoTime() + OUTAGE_NANOS;
                    submitted = source.committed;
                    event = null;
                    exhausted = false;
                }
                backoff(2, deadline);
            }
        }
    }

    /** Fetch committed progress once, checking row health before source handoff. */
    static void collectProgress(Producer producer, long submitted, SampleEventSource source) {
        ChannelStatus status = producer.channel.getChannelStatus();
        if (status.getRowsErrorCount() > 0) throw new IllegalStateException("Reconcile row errors before handoff");
        if (!"SUCCESS".equals(status.getStatusCode())) {
            throw new SFException("InvalidChannelError", status.getStatusCode(), 409, "Conflict");
        }
        long committed = Math.min(submitted, parseOffset(status.getLatestCommittedOffsetToken()));
        if (committed > source.committed) source.acknowledge(committed);
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

    static final String CHANNEL_NAME = env("SNOWFLAKE_CHANNEL", "production-source-0");

    /** Decode this sample's numeric source offset; an absent token means no progress. */
    static long parseOffset(String token) { return token == null ? 0 : Long.parseLong(token); }

    /** Own the SDK client and channel state needed for recovery. */
    static class Producer {
        final ClientFactory factory;
        SnowflakeStreamingIngestClient client;
        SnowflakeStreamingIngestChannel channel;
        Producer(ClientFactory factory) { this.factory = factory; }
        /** Open the owned named channel and return its authoritative committed source offset. */
        long open() throws Exception {
            if (client == null) client = factory.create();
            OpenChannelResult opened = client.openChannel(CHANNEL_NAME);
            channel = opened.getChannel();
            if (opened.getChannelStatus().getRowsErrorCount() > 0) {
                throw new IllegalStateException("Reconcile row errors before source handoff");
            }
            return parseOffset(opened.getChannelStatus().getLatestCommittedOffsetToken());
        }
        /** Reopen without resetting the server offset, recreating an invalid client if needed. */
        long recover(SFException error) throws Exception {
            if ("InvalidClientError".equals(error.getErrorCodeName())) {
                close(false);
            } else if (channel != null) {
                try {
                    channel.close(false, Duration.ofSeconds(30));
                } catch (SFException alreadyInvalid) {
                    // Reopen the named channel without dropping its committed offset.
                }
            }
            try {
                return open();
            } catch (SFException reopened) {
                if (!"InvalidClientError".equals(reopened.getErrorCodeName())
                        && !"ClosedClientError".equals(reopened.getErrorCodeName())) throw reopened;
                close(false);
                return open();
            }
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

}
