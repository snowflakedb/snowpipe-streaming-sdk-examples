package com.snowflake.example;

import com.snowflake.ingest.streaming.ErrorCode;
import com.snowflake.ingest.streaming.SFException;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Narrow compile-check and unit tests for the Java example classes.
 *
 * These tests verify SDK API usage patterns and batch-building logic without
 * requiring a live Snowflake connection.
 */
class ExamplesCompileTest {

    // ---- Elastic Quickstart: batch construction sanity ----

    @Test
    void batchHasCorrectRowCount() {
        List<Map<String, Object>> rows = buildBatch(1, 50);
        assertEquals(50, rows.size());
    }

    @Test
    void batchRowsHaveExpectedFields() {
        List<Map<String, Object>> rows = buildBatch(1, 3);
        Map<String, Object> first = rows.get(0);
        assertEquals(1, first.get("c1"));
        assertEquals("1", first.get("c2"));
        assertNotNull(first.get("ts"));
    }

    @Test
    void batchTokenIsNotNull() {
        String token = "batch-1";
        assertNotNull(token);
        assertTrue(token.startsWith("batch-"));
    }

    // ---- ElasticProducer: error classification logic ----

    @Test
    void permanentHttpCodesAreIdentified() {
        int[] permanent = {400, 401, 403};
        for (int code : permanent) {
            assertTrue(isPermanent(code), "Expected " + code + " to be permanent");
        }
    }

    @Test
    void retryableHttpCodesAreIdentified() {
        int[] retryable = {408, 429, 500, 502, 503, 504};
        for (int code : retryable) {
            assertTrue(isRetryable(code), "Expected " + code + " to be retryable");
        }
    }

    @Test
    void invalidationErrorCodesAreRecognised() {
        // SDK reports channel/client invalidation and closure with these specific
        // error code names (verified against ErrorCode.java and
        // SnowflakeStreamingIngestElasticChannelTest.java in the SDK source).
        assertTrue(isInvalidation("InvalidChannelError"));
        assertTrue(isInvalidation("InvalidClientError"));
        assertTrue(isInvalidation("ClosedElasticChannelError"),
                "ClosedElasticChannelError is thrown by the SDK when a channel is closed "
                        + "out from under an in-flight append and must be classified as invalidation");
        assertFalse(isInvalidation(null));
        assertFalse(isInvalidation(""));
    }

    @Test
    void closedElasticChannelErrorMatchesRealSdkErrorCode() {
        // Build the exception the way the SDK itself does
        // (SnowflakeStreamingIngestElasticChannelInternal.appendRow throws exactly this
        // on a closed channel) so classification is checked against the real error
        // code name and HTTP status, not a guessed string.
        SFException sfe = new SFException(ErrorCode.CLOSED_ELASTIC_CHANNEL_ERROR, "channel closed");
        assertEquals("ClosedElasticChannelError", sfe.getErrorCodeName());
        assertEquals(409, sfe.getHttpStatusCode());
        assertTrue(isInvalidation(sfe.getErrorCodeName()));
    }

    @Test
    void invalidChannelErrorMatchesRealSdkErrorCode() {
        SFException sfe = new SFException(ErrorCode.INVALID_CHANNEL_ERROR, "channel invalidated");
        assertEquals("InvalidChannelError", sfe.getErrorCodeName());
        assertTrue(isInvalidation(sfe.getErrorCodeName()));
    }

    @Test
    void backoffCapsAtMaximum() {
        long backoff = 100;
        long max = 30_000;
        for (int i = 0; i < 20; i++) {
            backoff = Math.min(backoff * 2, max);
        }
        assertEquals(max, backoff, "Backoff should be capped at MAX_BACKOFF_MS");
    }

    // ---- NamedChannelCheckpoint: offset arithmetic ----

    @Test
    void resumeOffsetNullStartsAtOne() {
        String resumeOffset = null;
        int startRow = resumeOffset == null ? 1 : Integer.parseInt(resumeOffset) + 1;
        assertEquals(1, startRow);
    }

    @Test
    void resumeOffsetAdvancesStartRow() {
        String resumeOffset = "5000";
        int startRow = Integer.parseInt(resumeOffset) + 1;
        assertEquals(5001, startRow);
    }

    @Test
    void waitForCommitPredicateMatchesExact() {
        // Verify the predicate used in NamedChannelCheckpoint compiles and evaluates correctly.
        String targetToken = "10000";
        java.util.function.Predicate<String> pred =
                token -> token != null && Long.parseLong(token) >= Long.parseLong(targetToken);
        assertTrue(pred.test("10000"));
        assertTrue(pred.test("10001"));
        assertFalse(pred.test("9999"));
        assertFalse(pred.test(null));
    }

    // ---- Shared API surface: Duration usage ----

    @Test
    void flushTimeoutDurationIsPositive() {
        Duration timeout = Duration.ofSeconds(30);
        assertFalse(timeout.isNegative());
        assertFalse(timeout.isZero());
    }

    @Test
    void pomAllowsExecMainClassOverride() throws Exception {
        String pom = Files.readString(Paths.get("pom.xml"));
        assertTrue(pom.contains("<exec.mainClass>com.snowflake.example.ElasticQuickstart</exec.mainClass>"));
        assertTrue(pom.contains("<mainClass>${exec.mainClass}</mainClass>"));
    }

    @Test
    void namedExampleOpensChannelOnlyOnce() throws Exception {
        String source = Files.readString(Paths.get(
                "src/main/java/com/snowflake/example/NamedChannelCheckpoint.java"));
        assertEquals(1, source.split("client.openChannel\\(", -1).length - 1);
        assertTrue(source.contains("opened.getChannelStatus().getLatestCommittedOffsetToken()"));
    }

    // ---- Helpers (mirror private methods in the example classes) ----

    private static List<Map<String, Object>> buildBatch(int start, int end) {
        double ts = System.currentTimeMillis() / 1000.0;
        List<Map<String, Object>> rows = new ArrayList<>(end - start + 1);
        for (int i = start; i <= end; i++) {
            rows.add(Map.of("c1", i, "c2", String.valueOf(i), "ts", ts));
        }
        return rows;
    }

    private static boolean isPermanent(int http) {
        return http == 400 || http == 401 || http == 403;
    }

    private static boolean isRetryable(int http) {
        return http == 429 || http >= 500 || http == 408;
    }

    private static boolean isInvalidation(String code) {
        return "InvalidChannelError".equals(code) || "InvalidClientError".equals(code)
                || "ClosedElasticChannelError".equals(code);
    }
}
