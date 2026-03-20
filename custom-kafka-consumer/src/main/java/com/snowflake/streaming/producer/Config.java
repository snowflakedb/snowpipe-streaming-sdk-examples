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

    public String getKafkaBootstrapServers() {
        return props.getProperty("kafka.bootstrap.servers", "localhost:9092");
    }

    public String getKafkaTopic() {
        return props.getProperty("kafka.topic", "test-topic");
    }

    public String getProducerAcks() {
        return props.getProperty("producer.acks", "all");
    }

    public String getProducerLingerMs() {
        return props.getProperty("producer.linger.ms", "5");
    }

    public String getProducerBatchSize() {
        return props.getProperty("producer.batch.size", "16384");
    }

    public int getDefaultBurstCount() {
        return Integer.parseInt(props.getProperty("default.burst.count", "100"));
    }

    public int getDefaultStreamRps() {
        return Integer.parseInt(props.getProperty("default.stream.rps", "10"));
    }

    /**
     * Builds Kafka producer properties from config values.
     */
    public Properties buildKafkaProducerProperties() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", getKafkaBootstrapServers());
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("acks", getProducerAcks());
        kafkaProps.put("linger.ms", getProducerLingerMs());
        kafkaProps.put("batch.size", getProducerBatchSize());
        return kafkaProps;
    }
}
