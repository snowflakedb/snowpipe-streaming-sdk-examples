package com.snowflake.streaming.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Mimics the customer's RowSetBuilder — an in-memory buffer that accumulates
 * rows (Map<String, Object>) and returns them as a batch for appendRows().
 */
public class RowSetBuilder {

    private final List<Map<String, Object>> rows = new ArrayList<>();

    private RowSetBuilder() {}

    public static RowSetBuilder newBuilder2() {
        return new RowSetBuilder();
    }

    public void addRow(Map<String, Object> row) {
        rows.add(row);
    }

    public List<Map<String, Object>> build() {
        return new ArrayList<>(rows);
    }

    public void clear() {
        rows.clear();
    }

    public int rowSetSize() {
        return rows.size();
    }
}
