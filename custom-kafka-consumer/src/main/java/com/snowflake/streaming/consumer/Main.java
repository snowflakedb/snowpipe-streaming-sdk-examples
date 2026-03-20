package com.snowflake.streaming.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Entry point. Supports two modes:
 *   java -cp ... Main produce [count]   — push test records to Kafka
 *   java -cp ... Main consume           — run the consumer
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        String configPath = System.getProperty("config.path", "consumer-config.properties");
        Config config = new Config(configPath);


        logger.info("Starting consumer. Topic={}, Group={}, Channel={}",
                config.getKafkaTopic(), config.getKafkaGroupId(), config.getSnowflakeChannelName());

        Properties sfProps = loadSnowflakeProfile(config.getSnowflakeProfilePath());
        SnowflakeStreamingIngestClient sfClient = SnowflakeStreamingIngestClientFactory.builder(
                        "TEST_CLIENT",
                        config.getSnowflakeDatabase(),
                        config.getSnowflakeSchema(),
                        "CALL_DETAIL_RECORDS-STREAMING")
                .setProperties(sfProps)
                .build();


        logger.info("DB Name: {}", sfClient.getDBName());
        logger.info("Pipe Name: {}", sfClient.getPipeName());

        CustomerConsumerRunner runner = new CustomerConsumerRunner(config, sfClient);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered");
            runner.shutdown();
        }));
        runner.run();

        // Cleanup client
        try {
            sfClient.close();
        } catch (Exception e) {
            logger.warn("Error closing Snowflake client", e);
        }
    }

    /**
     * Loads Snowflake connection profile from a JSON file.
     * Expected keys: url, account, user, role, private_key (or private_key_file), warehouse
     */
    private static Properties loadSnowflakeProfile(String path) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        @SuppressWarnings("unchecked")
        Map<String, Object> profile = mapper.readValue(new FileInputStream(path), Map.class);

        Properties props = new Properties();
        profile.forEach((k, v) -> props.setProperty(k, String.valueOf(v)));
        return props;
    }
}
