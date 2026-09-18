package com.snowflake.example;

import com.snowflake.ingest.streaming.ErrorDetail;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;
import com.snowflake.ingest.streaming.SuccessDetail;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Elastic ingest with callbacks instead of Futures.
 *
 * <p>Same four-call tour as {@link ElasticChannelIngest}, using {@code appendRow}
 * / {@code appendRows}. Handlers are the only acknowledgement signal.
 *
 * <p>They run on the SDK acknowledgement thread. Keep them to a single
 * {@link BlockingQueue#offer}: no I/O, no SDK calls, and no lock the ingest
 * thread also waits on. {@code offer} on an unbounded queue never blocks, so a
 * full queue cannot deadlock the ack thread the way {@code put} on a bounded
 * queue can.
 *
 * <p>Count and checkpoint on the ingest thread after {@code poll}. Do not
 * mutate shared counters in the handler; {@code i++} is not atomic across
 * threads.
 */
public class ElasticChannelCallbacks {
    /**
     * Handoff from the SDK ack thread to the ingest thread.
     *
     * <p>Handlers only enqueue a cheap event. The ingest thread waits and
     * counts.
     */
    static final class AckInbox {
        private final BlockingQueue<AckEvent> events = new LinkedBlockingQueue<>();

        void install(SnowflakeStreamingIngestElasticChannel channel) {
            channel.setSuccessHandler(this::onSuccess);
            channel.setErrorHandler(this::onError);
        }

        void onSuccess(SuccessDetail detail) {
            for (Object token : detail.getAppendTokens()) {
                events.offer(AckEvent.ok(token));
            }
        }

        void onError(ErrorDetail detail) {
            events.offer(AckEvent.err(detail));
        }

        Object waitOne(long timeout, TimeUnit unit)
                throws InterruptedException, TimeoutException {
            AckEvent event = events.poll(timeout, unit);
            if (event == null) {
                throw new TimeoutException("Timed out waiting for an acknowledgement");
            }
            if (event.error != null) {
                throw event.error.getError();
            }
            return event.token;
        }

        List<Object> waitN(int count, long timeout, TimeUnit unit)
                throws InterruptedException, TimeoutException {
            long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
            List<Object> got = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    throw new TimeoutException("Timed out waiting for acknowledgements");
                }
                got.add(waitOne(remaining, TimeUnit.NANOSECONDS));
            }
            return got;
        }

        void raisePendingErrors() {
            AckEvent event;
            while ((event = events.poll()) != null) {
                if (event.error != null) {
                    throw event.error.getError();
                }
            }
        }
    }

    private static final class AckEvent {
        final Object token;
        final ErrorDetail error;

        static AckEvent ok(Object token) {
            return new AckEvent(token, null);
        }

        static AckEvent err(ErrorDetail error) {
            return new AckEvent(null, error);
        }

        private AckEvent(Object token, ErrorDetail error) {
            this.token = token;
            this.error = error;
        }
    }

    public static void main(String[] args) throws Exception {
        SnowflakeStreamingIngestClient client = ElasticChannelIngest.createClient();
        try {
            SnowflakeStreamingIngestElasticChannel channel = client.getElasticChannel();
            AckInbox inbox = new AckInbox();
            inbox.install(channel);
            int nextId = 0;

            // 1. Wait per row — simplest call, and the slow path.
            channel.appendRow(
                    ElasticChannelIngest.sampleRow(nextId), "event-" + nextId);
            inbox.waitOne(
                    ElasticChannelIngest.ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            nextId++;

            // 2. Recommended: submit every row before waiting. The SDK batches
            // these for transport.
            for (int eventId = nextId;
                    eventId < nextId + ElasticChannelIngest.PIPELINE_ROWS;
                    eventId++) {
                channel.appendRow(
                        ElasticChannelIngest.sampleRow(eventId), "event-" + eventId);
            }
            inbox.waitN(
                    ElasticChannelIngest.PIPELINE_ROWS,
                    ElasticChannelIngest.ACK_TIMEOUT_SECONDS,
                    TimeUnit.SECONDS);
            nextId += ElasticChannelIngest.PIPELINE_ROWS;

            // 3. Optional: application batches when you already have a group, or
            // want fewer tokens. Same pipelining; not required for wire efficiency.
            for (int batchIndex = 0; batchIndex < ElasticChannelIngest.BATCH_COUNT; batchIndex++) {
                List<Map<String, Object>> rows = new ArrayList<>();
                for (int eventId = nextId;
                        eventId < nextId + ElasticChannelIngest.BATCH_SIZE;
                        eventId++) {
                    rows.add(ElasticChannelIngest.sampleRow(eventId));
                }
                channel.appendRows(rows, "batch-" + batchIndex);
                nextId += ElasticChannelIngest.BATCH_SIZE;
            }
            inbox.waitN(
                    ElasticChannelIngest.BATCH_COUNT,
                    ElasticChannelIngest.ACK_TIMEOUT_SECONDS,
                    TimeUnit.SECONDS);

            // 4. Fire-and-forget: do not wait on the inbox. waitForFlush covers
            // these appends; then look for errors the handler already queued.
            for (int eventId = nextId;
                    eventId < nextId + ElasticChannelIngest.FIRE_AND_FORGET_ROWS;
                    eventId++) {
                channel.appendRow(
                        ElasticChannelIngest.sampleRow(eventId), "event-" + eventId);
            }
            nextId += ElasticChannelIngest.FIRE_AND_FORGET_ROWS;
            List<Map<String, Object>> fireAndForgetBatch = new ArrayList<>();
            for (int eventId = nextId;
                    eventId < nextId + ElasticChannelIngest.FIRE_AND_FORGET_BATCH_SIZE;
                    eventId++) {
                fireAndForgetBatch.add(ElasticChannelIngest.sampleRow(eventId));
            }
            channel.appendRows(fireAndForgetBatch, "batch-ff");
            nextId += ElasticChannelIngest.FIRE_AND_FORGET_BATCH_SIZE;
            channel.waitForFlush(Duration.ofSeconds(ElasticChannelIngest.ACK_TIMEOUT_SECONDS))
                    .get(ElasticChannelIngest.ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            inbox.raisePendingErrors();

            System.out.println("Durably acknowledged " + nextId + " rows");
        } finally {
            client.close(
                    true, Duration.ofSeconds(ElasticChannelIngest.ACK_TIMEOUT_SECONDS))
                    .get(ElasticChannelIngest.ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }
}
