package com.snowflake.streaming.consumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Loads configuration from consumer-config.properties and exposes typed accessors.
 */
public class Config {

    private final Properties props;

    public Config(String path) throws IOException {
        props = new Properties();
        try (FileInputStream fis = new FileInputStream(path)) {
            props.load(fis);
        }
    }

    public String getAwsRegion() {
        return props.getProperty("aws.region", "us-east-1");
    }

    public String getSqsQueueUrl() {
        return props.getProperty("sqs.queue.url");
    }

    /**
     * Max messages per SQS ReceiveMessage call. SQS hard limit is 10.
     */
    public int getSqsMaxMessages() {
        int val = Integer.parseInt(props.getProperty("sqs.max.messages", "10"));
        if (val < 1 || val > 10) {
            throw new IllegalArgumentException("sqs.max.messages must be between 1 and 10, got: " + val);
        }
        return val;
    }

    /**
     * Long-poll wait time in seconds (0 = short poll, up to 20 = long poll).
     * Long polling reduces empty responses and lowers cost.
     */
    public int getSqsWaitTimeSeconds() {
        return Integer.parseInt(props.getProperty("sqs.wait.time.seconds", "20"));
    }

    /**
     * Visibility timeout in seconds. Messages invisible to other consumers during processing.
     * Must be longer than the time needed to process a full batch.
     */
    public int getSqsVisibilityTimeoutSeconds() {
        return Integer.parseInt(props.getProperty("sqs.visibility.timeout.seconds", "30"));
    }

    public String getSnowflakeChannelName() {
        return props.getProperty("snowflake.channel.name", "SQS_CHANNEL");
    }

    public String getSnowflakeDatabase() {
        return props.getProperty("snowflake.database", "TEST_DB");
    }

    public String getSnowflakeSchema() {
        return props.getProperty("snowflake.schema", "PUBLIC");
    }

    public String getSnowflakeTable() {
        return props.getProperty("snowflake.table", "TEST_TABLE");
    }

    public String getSnowflakeProfilePath() {
        return props.getProperty("snowflake.profile.path", "profile.json");
    }

    public int getMaxRowsPerAppend() {
        return Integer.parseInt(props.getProperty("max.rows.per.append", "50"));
    }

    public int getConsumerThreadCount() {
        return Integer.parseInt(props.getProperty("consumer.thread.count", "1"));
    }
}
