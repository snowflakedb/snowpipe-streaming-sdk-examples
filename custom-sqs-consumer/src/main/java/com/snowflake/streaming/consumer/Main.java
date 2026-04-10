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
 * Entry point for the Custom SQS Consumer application.
 *
 * <p>Launches one or more {@link CustomSQSConsumer} threads that pull messages from
 * an Amazon SQS queue and ingest records into Snowflake via the Snowpipe
 * Streaming SDK.</p>
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.consumer.Main"
 * mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.consumer.Main" -Dconfig.path=my.props
 * }</pre>
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        String configPath = System.getProperty("config.path", "consumer-config.properties");
        Config config = new Config(configPath);

        int threadCount = config.getConsumerThreadCount();
        logger.info("Starting {} consumer thread(s). Region={}, Queue={}, Channel={}",
                threadCount, config.getAwsRegion(), config.getSqsQueueUrl(),
                config.getSnowflakeChannelName());

        Properties sfProps = loadSnowflakeProfile(config.getSnowflakeProfilePath());
        SnowflakeStreamingIngestClient sfClient = SnowflakeStreamingIngestClientFactory.builder(
                        "SQS_CLIENT",
                        config.getSnowflakeDatabase(),
                        config.getSnowflakeSchema(),
                        config.getSnowflakeTable() + "-STREAMING")
                .setProperties(sfProps)
                .build();

        logger.info("DB Name: {}", sfClient.getDBName());
        logger.info("Pipe Name: {}", sfClient.getPipeName());

        List<CustomSQSConsumer> runners = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            String channelName = config.getSnowflakeChannelName() + "_T" + i;
            CustomSQSConsumer runner = new CustomSQSConsumer(config, sfClient, channelName);
            runners.add(runner);
            executor.submit(runner);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered");
            runners.forEach(CustomSQSConsumer::shutdown);
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

    private static Properties loadSnowflakeProfile(String path) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        @SuppressWarnings("unchecked")
        Map<String, Object> profile = mapper.readValue(new FileInputStream(path), Map.class);

        Properties props = new Properties();
        profile.forEach((k, v) -> props.setProperty(k, String.valueOf(v)));
        return props;
    }
}
