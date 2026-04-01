package com.snowflake.streaming.producer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Loads producer configuration from producer-config.properties.
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

    public String getPubSubTopicId() {
        return props.getProperty("pubsub.topic.id", "ssv2-example-topic");
    }

    public int getDefaultBurstCount() {
        return Integer.parseInt(props.getProperty("default.burst.count", "100"));
    }

    public int getDefaultStreamRps() {
        return Integer.parseInt(props.getProperty("default.stream.rps", "10"));
    }
}
