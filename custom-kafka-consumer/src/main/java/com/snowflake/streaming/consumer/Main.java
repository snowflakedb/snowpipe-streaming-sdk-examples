package com.snowflake.streaming.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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


        int threadCount = config.getConsumerThreadCount();
        logger.info("Starting {} consumer thread(s). Topic={}, Group={}, Channel={}",
                threadCount, config.getKafkaTopic(), config.getKafkaGroupId(), config.getSnowflakeChannelName());

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

        List<CustomKafkaConsumer> runners = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            CustomKafkaConsumer runner = new CustomKafkaConsumer(config, sfClient);
            runners.add(runner);
            executor.submit(runner);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered");
            runners.forEach(CustomKafkaConsumer::shutdown);
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    logger.warn("Executor did not terminate in 30s, forcing shutdown");
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }));

        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            logger.info("Main thread interrupted");
            Thread.currentThread().interrupt();
        } finally {
            try {
                sfClient.close();
            } catch (Exception e) {
                logger.warn("Error closing Snowflake client", e);
            }
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
