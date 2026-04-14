package com.snowflake.example.monitoring;

import java.util.OptionalLong;

/**
 * Tracks the highest sent offset and computes lag against the
 * server-side committed offset.
 */
public class OffsetTracker {
    private long sentHighestOffset = 0;

    public void updateSent(long offset) {
        if (offset > sentHighestOffset) {
            sentHighestOffset = offset;
        }
    }

    public long getSentHighestOffset() {
        return sentHighestOffset;
    }

    /**
     * Compute lag = sentHighestOffset - committed.
     * Returns empty if committedToken is null or unparseable.
     */
    public OptionalLong computeLag(String committedToken) {
        if (committedToken == null) {
            return OptionalLong.empty();
        }
        try {
            long committed = Long.parseLong(committedToken);
            return OptionalLong.of(sentHighestOffset - committed);
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
    }
}
