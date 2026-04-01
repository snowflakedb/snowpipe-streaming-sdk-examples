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

    public String getGcpProjectId() {
        return props.getProperty("gcp.project.id");
    }

    public String getPubSubSubscriptionId() {
        return props.getProperty("pubsub.subscription.id", "ssv2-example-sub");
    }

    public int getMaxOutstandingMessages() {
        return Integer.parseInt(props.getProperty("pubsub.max.outstanding.messages", "1000"));
    }

    public long getMaxOutstandingBytes() {
        return Long.parseLong(props.getProperty("pubsub.max.outstanding.bytes", "104857600"));
    }

    public String getSnowflakeChannelName() {
        return props.getProperty("snowflake.channel.name", "PUBSUB_CHANNEL");
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
        return Integer.parseInt(props.getProperty("max.rows.per.append", "100"));
    }

    public int getConsumerThreadCount() {
        return Integer.parseInt(props.getProperty("consumer.thread.count", "1"));
    }
}
