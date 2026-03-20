package com.snowflake.streaming.consumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Loads configuration from consumer-config.properties and builds Kafka consumer properties.
 */
public class Config {

    private final Properties props;

    public Config(String path) throws IOException {
        props = new Properties();
        try (FileInputStream fis = new FileInputStream(path)) {
            props.load(fis);
        }
    }

    public String getKafkaBootstrapServers() {
        return props.getProperty("kafka.bootstrap.servers", "localhost:9092");
    }

    public String getKafkaTopic() {
        return props.getProperty("kafka.topic", "test-topic");
    }

    public String getKafkaGroupId() {
        return props.getProperty("kafka.group.id", "test-group");
    }

    public long getConsumerPollDurationMs() {
        return Long.parseLong(props.getProperty("kafka.poll.duration.ms", "1000"));
    }

    public String getSnowflakeChannelName() {
        return props.getProperty("snowflake.channel.name", "TEST_CHANNEL");
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

    public String getSnowflakePipe() {
        return props.getProperty("snowflake.pipe", "TEST_PIPE");
    }

    public String getSnowflakeProfilePath() {
        return props.getProperty("snowflake.profile.path", "profile.json");
    }

    public int getMaxRowsPerAppend() {
        return Integer.parseInt(props.getProperty("max.rows.per.append", "100"));
    }

    /**
     * Builds Kafka consumer properties with sensible defaults.
     */
    public Properties buildKafkaProperties() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", getKafkaBootstrapServers());
        kafkaProps.put("group.id", getKafkaGroupId());
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("enable.auto.commit", "false"); // We commit manually after Snowflake confirms
        kafkaProps.put("auto.offset.reset", "earliest");
        kafkaProps.put("max.poll.records", String.valueOf(getMaxRowsPerAppend()));
        return kafkaProps;
    }
}
