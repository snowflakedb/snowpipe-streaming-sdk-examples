package com.snowflake.example;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Elastic ingest of a large, uninterrupted row stream.
 *
 * <p>Create a table-mode client, get the channel, and keep appending. The SDK
 * batches rows for transport. This program submits many single-row
 * {@code appendRowWithWait} calls before waiting, and only pauses when too many
 * acknowledgement Futures are outstanding — not after every append.
 *
 * <p>Thread interrupt (Ctrl+C in environments that interrupt the main thread)
 * stops intake, waits for accepted appends, prints stats, and closes.
 */
public class ElasticChannelUnbounded {
    static final long DEFAULT_ROWS = 10_000_000L;
    static final int MAX_PENDING_EVENTS = 10_000;

    /** Tests set this instead of {@code SNOWFLAKE_TEST_ROWS}. */
    static Long rowCountForTest;

    static final class InFlight {
        final long submittedAtNanos;
        final CompletableFuture<Void> ack;

        InFlight(long submittedAtNanos, CompletableFuture<Void> ack) {
            this.submittedAtNanos = submittedAtNanos;
            this.ack = ack;
        }
    }

    static long totalRows() {
        if (rowCountForTest != null) {
            return rowCountForTest;
        }
        return Long.parseLong(
                ElasticChannelIngest.env("SNOWFLAKE_TEST_ROWS", String.valueOf(DEFAULT_ROWS)));
    }

    static void throwIfInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }
    }

    static void drainConfirmedPrefix(Deque<InFlight> pending, LatencyStats stats)
            throws Exception {
        pending.peekFirst().ack.get(
                ElasticChannelIngest.ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        while (!pending.isEmpty() && pending.peekFirst().ack.isDone()) {
            InFlight item = pending.removeFirst();
            item.ack.get();
            stats.latencySumNanos += System.nanoTime() - item.submittedAtNanos;
            stats.acked++;
        }
    }

    static final class LatencyStats {
        long acked;
        long latencySumNanos;
    }

    public static void main(String[] args) throws Exception {
        long total = totalRows();
        SnowflakeStreamingIngestClient client = ElasticChannelIngest.createClient();
        Deque<InFlight> pending = new ArrayDeque<>();
        LatencyStats stats = new LatencyStats();
        long started = System.nanoTime();
        boolean waitOnClose = false;
        try {
            SnowflakeStreamingIngestElasticChannel channel = client.getElasticChannel();
            try {
                for (int eventId = 0; eventId < total; eventId++) {
                    throwIfInterrupted();
                    pending.addLast(new InFlight(
                            System.nanoTime(),
                            channel.appendRowWithWait(
                                    ElasticChannelIngest.sampleRow(eventId), null)));
                    throwIfInterrupted();
                    if (pending.size() < MAX_PENDING_EVENTS) {
                        continue;
                    }
                    drainConfirmedPrefix(pending, stats);
                }
            } catch (InterruptedException ignored) {
                // Stop intake. Drain work already accepted by the SDK.
                Thread.interrupted();
            }
            while (!pending.isEmpty()) {
                drainConfirmedPrefix(pending, stats);
            }
            waitOnClose = true;
            double elapsed = (System.nanoTime() - started) / 1_000_000_000.0;
            System.out.println("Durably acknowledged " + stats.acked + " rows");
            if (stats.acked > 0 && elapsed > 0) {
                double avgAckMs = 1000.0 * (stats.latencySumNanos / (double) stats.acked)
                        / 1_000_000_000.0;
                double rps = stats.acked / elapsed;
                // Average ack latency can look large. Many appends are in flight, so
                // throughput is not 1 / latency — parallelism carries the rate.
                System.out.printf("avg ack latency %.1f ms, %.0f rows/s%n", avgAckMs, rps);
            }
        } catch (InterruptedException ignored) {
            Thread.interrupted();
        } finally {
            client.close(
                    waitOnClose, Duration.ofSeconds(ElasticChannelIngest.ACK_TIMEOUT_SECONDS))
                    .get(ElasticChannelIngest.ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }
}
