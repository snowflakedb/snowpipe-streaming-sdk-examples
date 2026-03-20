package com.snowflake.streaming.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;

/**
 * Simple Kafka producer that pushes JSON test messages for validating the consumer.
 */
public class TestProducer {

    private static final Logger logger = LoggerFactory.getLogger(TestProducer.class);

    private final String bootstrapServers;
    private final String topic;

    public TestProducer(Config config) {
        this.bootstrapServers = config.getKafkaBootstrapServers();
        this.topic = config.getKafkaTopic();
    }

    public void produce(int count) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");

        ObjectMapper mapper = new ObjectMapper();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                // Matches table schema: TEST_TABLE (C1 NUMBER, TS TIMESTAMP_LTZ)
                Map<String, Object> payload = Map.of(
                        "C1", i,
                        "TS", Instant.now().toString()
                );

                String json = mapper.writeValueAsString(payload);
                String key = "key-" + (i % 10);

                producer.send(new ProducerRecord<>(topic, key, json), (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to send record", exception);
                    }
                });

                if ((i + 1) % 100 == 0) {
                    logger.info("Produced {} / {} records", i + 1, count);
                }
            }
            producer.flush();
            logger.info("Done. Produced {} records to topic '{}'", count, topic);
        } catch (Exception e) {
            logger.error("Producer error", e);
        }
    }
}
