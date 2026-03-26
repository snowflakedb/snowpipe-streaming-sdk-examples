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
 * Entry point for the Custom Kafka Consumer application.
 *
 * <p>Launches one or more {@link CustomKafkaConsumer} threads that read from a Kafka topic
 * and ingest records into Snowflake via the Snowpipe Streaming SDK. The number of consumer
 * threads is controlled by the {@code consumer.thread.count} property in the config file.</p>
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>Loads consumer configuration from a properties file (default: {@code consumer-config.properties},
 *       overridable via {@code -Dconfig.path=...}).</li>
 *   <li>Loads Snowflake connection credentials from a JSON profile file.</li>
 *   <li>Creates a shared {@link SnowflakeStreamingIngestClient} used by all consumer threads.</li>
 *   <li>Submits {@code consumer.thread.count} {@link CustomKafkaConsumer} instances to a
 *       fixed-size thread pool. Each instance independently subscribes to the Kafka topic
 *       and participates in consumer-group rebalancing.</li>
 *   <li>Registers a JVM shutdown hook that gracefully stops all consumers, waits up to 30 seconds
 *       for the executor to drain, and closes the Snowflake client.</li>
 * </ol>
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * mvn compile exec:java                          # uses default consumer-config.properties
 * mvn compile exec:java -Dconfig.path=my.props   # custom config path
 * }</pre>
 *
 * @see CustomKafkaConsumer
 * @see Config
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    /**
     * Bootstraps the application: loads config, creates the Snowflake client,
     * launches consumer threads, and blocks until shutdown.
     *
     * @param args not used; configuration is loaded from the properties file
     * @throws Exception if config loading, Snowflake client creation, or profile parsing fails
     */
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
                        config.getSnowflakeTable()+"-STREAMING")
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
     * Loads Snowflake connection properties from a JSON profile file.
     *
     * <p>The JSON file should contain a flat object with string values. Expected keys include:
     * {@code url}, {@code account}, {@code user}, {@code role}, {@code private_key}
     * (or {@code private_key_file}), and {@code warehouse}.</p>
     *
     * @param path filesystem path to the JSON profile file
     * @return a {@link Properties} object populated with the profile entries
     * @throws Exception if the file cannot be read or parsed
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
