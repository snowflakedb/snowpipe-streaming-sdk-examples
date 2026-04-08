---
name: custom-kafka-consumer
description: "Set up, configure, run, and debug a custom Kafka consumer that streams data into Snowflake via Snowpipe Streaming SDK v2. Use when: building a Kafka-to-Snowflake streaming pipeline, running the CDR demo, or troubleshooting the custom consumer. Triggers: kafka consumer, kafka snowflake, snowpipe streaming kafka, CDR demo, kafka to snowflake, custom consumer, streaming ingest kafka."
---

# Custom Kafka Consumer -- Snowpipe Streaming V2

## Architecture

```
FakeKafkaWriter (CDR generator)
        |  JSON messages
   Apache Kafka (topic, N partitions)
        |  poll()
CustomKafkaConsumer (1:1 partition-to-channel mapping)
        |  appendRow() per record
Snowpipe Streaming V2 (channel per partition)
        |
Snowflake Table (CALL_DETAIL_RECORDS)
```

Key design: each Kafka partition maps to exactly one Snowflake channel. A `ConsumerRebalanceListener` opens/closes channels on rebalance. Kafka offsets are committed only after Snowflake confirms persistence.

## Prerequisites

- Java 17+
- Maven 3.8+
- Homebrew (macOS) for local Kafka, or a remote Kafka broker
- Snowflake account with RSA key-pair auth configured
- `profile.json` with Snowflake connection details (url, account, user, role, private_key_file)

## Workflow

### Step 1: Start Local Kafka

**Goal:** Get a single-node Kafka broker running locally.

**Actions:**

1. **Check** if Kafka is installed:
   ```bash
   brew list kafka 2>/dev/null && echo "INSTALLED" || echo "NOT_INSTALLED"
   ```
   If `NOT_INSTALLED`, run `brew install kafka`.

2. **Check** if Kafka is already running:
   ```bash
   brew services list | grep kafka
   ```
   If status is `started`, skip to topic creation. Otherwise:
   ```bash
   brew services start kafka
   ```

3. **Create** the demo topic with 3 partitions:
   ```bash
   kafka-topics --create --topic test-topic --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
   ```

4. **Verify** the topic:
   ```bash
   kafka-topics --describe --topic test-topic --bootstrap-server localhost:9092
   ```

**Teardown (when done):**
```bash
brew services stop kafka
```

**STOP**: Verify `kafka-topics --list` shows `test-topic` before proceeding.

### Step 2: Set Up Snowflake Objects

**Goal:** Create the target database, schema, and table in Snowflake.

**Actions:**

1. **Execute** the following SQL in Snowflake:
   ```sql
   CREATE OR REPLACE DATABASE TEST_DATABASE;
   CREATE OR REPLACE SCHEMA TEST_SCHEMA;

   CREATE OR REPLACE TABLE CALL_DETAIL_RECORDS (
     record_id          NUMBER,
     caller_number      VARCHAR,
     callee_number      VARCHAR,
     call_type          VARCHAR,
     disposition        VARCHAR,
     call_start         TIMESTAMP_NTZ,
     call_end           TIMESTAMP_NTZ,
     duration_seconds   NUMBER,
     data_usage_mb      FLOAT,
     charge_amount      FLOAT,
     network_type       VARCHAR,
     cell_tower_id      VARCHAR,
     plan_type          VARCHAR,
     roaming            BOOLEAN,
     event_timestamp    TIMESTAMP_LTZ
   );
   ```

2. **Update** `profile.json` in the project root with Snowflake credentials:
   ```json
   {
     "user": "<SNOWFLAKE_USER>",
     "account": "<SNOWFLAKE_ACCOUNT>",
     "url": "<ACCOUNT>.snowflakecomputing.com",
     "private_key_file": "<PATH_TO_RSA_PRIVATE_KEY>",
     "role": "<ROLE>"
   }
   ```

**MANDATORY STOPPING POINT**: Confirm the table exists and `profile.json` is populated before proceeding.

### Step 3: Configure Properties

**Goal:** Set Kafka and Snowflake connection parameters in the config files.

**Ask** the user for:
- Snowflake database name (default: `TEST_DATABASE`)
- Snowflake schema name (default: `TEST_SCHEMA`)
- Kafka bootstrap servers (default: `localhost:9092`)
- Kafka topic name (default: `test-topic`)

**consumer-config.properties:**
```properties
kafka.bootstrap.servers=localhost:9092
kafka.topic=test-topic
kafka.group.id=test-group
kafka.poll.duration.ms=1000
snowflake.channel.name=TEST_CHANNEL
snowflake.database=TEST_DATABASE
snowflake.schema=TEST_SCHEMA
snowflake.table=CALL_DETAIL_RECORDS
snowflake.profile.path=profile.json
max.rows.per.append=100
consumer.thread.count=3
```

**producer-config.properties:**
```properties
kafka.bootstrap.servers=localhost:9092
kafka.topic=test-topic
producer.acks=all
producer.linger.ms=5
producer.batch.size=16384
default.burst.count=100
default.stream.rps=10
```

**STOP**: Confirm config values before writing files.

### Step 4: Build the Project

**Goal:** Compile the Java project with Maven.

```bash
mvn clean compile
```

**If error occurs:**
- `JAVA_HOME` not set or wrong version: Ensure Java 17+ is installed (`java -version`)
- Maven not found: Install via `brew install maven`
- Dependency resolution failure: Check network connectivity and Maven Central access

### Step 5: Run Producer and Consumer

**Goal:** Start the consumer and producer in separate terminals.

**Terminal 1 -- Start the consumer:**
```bash
mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.consumer.Main"
```

**Terminal 2 -- Start the producer:**
```bash
mvn compile exec:java -Dexec.mainClass="com.snowflake.streaming.producer.FakeKafkaWriter"
```

Once the producer starts, type `stream 20` to continuously send CDR records at 20 records/sec. Leave both terminals running — the data is needed for Steps 6 and 7.

**Producer interactive commands:**

| Command | Description |
|---------|-------------|
| `single` | Send one CDR |
| `burst [count]` | Send a batch of CDRs (default 100) |
| `stream [rps]` | Continuous stream at N records/sec (default 10, Enter to stop) |
| `malformed` | Send 5 malformed JSON messages (tests error handling) |
| `nulls` | Send CDRs with null/missing fields |
| `custom [json]` | Send a custom JSON payload |
| `quit` | Exit the producer |

**Stop streaming with Enter. Stop both processes with Ctrl+C.**

### Step 6: Verify Data in Snowflake

**Goal:** Confirm records arrived in the target table.

```sql
SELECT COUNT(*) FROM TEST_DATABASE.TEST_SCHEMA.CALL_DETAIL_RECORDS;
SELECT * FROM TEST_DATABASE.TEST_SCHEMA.CALL_DETAIL_RECORDS LIMIT 10;
```

### Step 7: Predictive Maintenance with Snowflake ML FORECAST

**Goal:** Use `SNOWFLAKE.ML.FORECAST` to predict call drop rates per tower over the next 7 days and flag towers that need repair — no Python, pure SQL.

**MANDATORY STOPPING POINT — wait for at least 300 rows before proceeding.**

Poll until the table has enough data (re-run until count ≥ 300):

```sql
SELECT COUNT(*) AS row_count,
       IFF(COUNT(*) >= 300, 'READY — proceed to Step 7', 'NOT READY — keep streaming') AS status
FROM TEST_DATABASE.TEST_SCHEMA.CALL_DETAIL_RECORDS;
```

If not ready, wait ~30 seconds (the producer from Step 5 is still streaming) and re-check. Repeat until status shows `READY`.

---

The CDR data streamed in Steps 5–6 is the input. Create a daily aggregation view per tower (exactly 3 columns — extra columns are treated as exogenous features and break inference):

```sql
CREATE OR REPLACE VIEW TEST_DATABASE.TEST_SCHEMA.TOWER_DROP_RATE_TS AS
SELECT
  cell_tower_id                                           AS tower_id,
  DATE_TRUNC('day', event_timestamp)::TIMESTAMP_NTZ      AS ts,
  COUNT_IF(disposition IN ('DROPPED','FAILED')) / COUNT(*) AS drop_rate
FROM TEST_DATABASE.TEST_SCHEMA.CALL_DETAIL_RECORDS
GROUP BY 1, 2;
```

#### 7a: Train the FORECAST Model

```sql
USE DATABASE TEST_DATABASE;
USE SCHEMA TEST_SCHEMA;

CREATE OR REPLACE SNOWFLAKE.ML.FORECAST tower_drop_forecast (
  INPUT_DATA   => SYSTEM$REFERENCE('VIEW', 'TEST_DATABASE.TEST_SCHEMA.TOWER_DROP_RATE_TS'),
  SERIES_COLNAME    => 'TOWER_ID',
  TIMESTAMP_COLNAME => 'TS',
  TARGET_COLNAME    => 'DROP_RATE'
);
```

Training time: ~24 seconds for 360 rows × 12 towers on an X-Small warehouse. Duration varies with warehouse size.

> **Important:** Always use a view/table with exactly 3 columns: series, timestamp, target.
> Extra columns are treated as exogenous features and cause `FORECAST` to require future values at prediction time.

#### 7b: Run 7-Day Predictions

```sql
CREATE OR REPLACE TABLE TEST_DATABASE.TEST_SCHEMA.TOWER_DROP_FORECAST_RESULTS AS
SELECT * FROM TABLE(
  tower_drop_forecast!FORECAST(
    FORECASTING_PERIODS => 7,
    CONFIG_OBJECT => {'prediction_interval': 0.9}
  )
);
```

#### 7c: Identify Towers That Need Repair

```sql
SELECT
  series                              AS tower_id,
  ROUND(AVG(forecast), 3)             AS avg_7day_drop_rate,
  ROUND(MAX(forecast), 3)             AS peak_drop_rate,
  COUNT_IF(forecast >= 0.40)          AS critical_days,
  CASE
    WHEN AVG(forecast) >= 0.40 THEN 'CRITICAL — dispatch now'
    WHEN AVG(forecast) >= 0.20 THEN 'AT RISK — schedule this week'
    ELSE 'HEALTHY — monitor'
  END AS maintenance_recommendation
FROM TEST_DATABASE.TEST_SCHEMA.TOWER_DROP_FORECAST_RESULTS
GROUP BY series
ORDER BY avg_7day_drop_rate DESC;
```

**Expected output (thresholds: CRITICAL ≥ 40%, AT RISK ≥ 20%):**

| Tower | Avg 7-Day Drop Rate | Recommendation |
|-------|-------------------|----------------|
| LAS-012 | ~93% | CRITICAL — dispatch now |
| SEA-007 | ~80% | CRITICAL — dispatch now |
| AUS-003 | ~58% | CRITICAL — dispatch now |
| MIA-008 | ~42% | CRITICAL — dispatch now |
| All others | < 10% | HEALTHY — monitor |

**Retrain timing:** `DROP` is instant; `CREATE OR REPLACE` retrains in ~24s on this dataset — safe for live demos.

#### 7d: Call Volume Forecast

Predict daily call volume per tower to support capacity planning.

```sql
CREATE OR REPLACE VIEW TEST_DATABASE.TEST_SCHEMA.TOWER_CALL_VOLUME_TS AS
SELECT
  cell_tower_id                                         AS tower_id,
  DATE_TRUNC('day', event_timestamp)::TIMESTAMP_NTZ     AS ts,
  COUNT(*)                                              AS call_volume
FROM TEST_DATABASE.TEST_SCHEMA.CALL_DETAIL_RECORDS
GROUP BY 1, 2;
```

```sql
USE DATABASE TEST_DATABASE;
USE SCHEMA TEST_SCHEMA;

CREATE OR REPLACE SNOWFLAKE.ML.FORECAST tower_call_volume_forecast (
  INPUT_DATA        => SYSTEM$REFERENCE('VIEW', 'TEST_DATABASE.TEST_SCHEMA.TOWER_CALL_VOLUME_TS'),
  SERIES_COLNAME    => 'TOWER_ID',
  TIMESTAMP_COLNAME => 'TS',
  TARGET_COLNAME    => 'CALL_VOLUME'
);
```

```sql
CREATE OR REPLACE TABLE TEST_DATABASE.TEST_SCHEMA.TOWER_CALL_VOLUME_FORECAST_RESULTS AS
SELECT * FROM TABLE(
  tower_call_volume_forecast!FORECAST(
    FORECASTING_PERIODS => 7,
    CONFIG_OBJECT => {'prediction_interval': 0.9}
  )
);
```

Inspect results:

```sql
SELECT series AS tower_id,
       ROUND(AVG(forecast), 0) AS avg_daily_calls,
       ROUND(MAX(forecast), 0) AS peak_daily_calls
FROM TEST_DATABASE.TEST_SCHEMA.TOWER_CALL_VOLUME_FORECAST_RESULTS
GROUP BY series
ORDER BY avg_daily_calls DESC;
```

#### 7e: Data Usage Forecast

Predict daily data usage (MB) per tower for bandwidth planning.

```sql
CREATE OR REPLACE VIEW TEST_DATABASE.TEST_SCHEMA.TOWER_DATA_USAGE_TS AS
SELECT
  cell_tower_id                                         AS tower_id,
  DATE_TRUNC('day', event_timestamp)::TIMESTAMP_NTZ     AS ts,
  SUM(data_usage_mb)                                    AS total_data_usage_mb
FROM TEST_DATABASE.TEST_SCHEMA.CALL_DETAIL_RECORDS
GROUP BY 1, 2;
```

```sql
USE DATABASE TEST_DATABASE;
USE SCHEMA TEST_SCHEMA;

CREATE OR REPLACE SNOWFLAKE.ML.FORECAST tower_data_usage_forecast (
  INPUT_DATA        => SYSTEM$REFERENCE('VIEW', 'TEST_DATABASE.TEST_SCHEMA.TOWER_DATA_USAGE_TS'),
  SERIES_COLNAME    => 'TOWER_ID',
  TIMESTAMP_COLNAME => 'TS',
  TARGET_COLNAME    => 'TOTAL_DATA_USAGE_MB'
);
```

```sql
CREATE OR REPLACE TABLE TEST_DATABASE.TEST_SCHEMA.TOWER_DATA_USAGE_FORECAST_RESULTS AS
SELECT * FROM TABLE(
  tower_data_usage_forecast!FORECAST(
    FORECASTING_PERIODS => 7,
    CONFIG_OBJECT => {'prediction_interval': 0.9}
  )
);
```

Inspect results:

```sql
SELECT series AS tower_id,
       ROUND(AVG(forecast), 1) AS avg_daily_data_mb,
       ROUND(MAX(forecast), 1) AS peak_daily_data_mb
FROM TEST_DATABASE.TEST_SCHEMA.TOWER_DATA_USAGE_FORECAST_RESULTS
GROUP BY series
ORDER BY avg_daily_data_mb DESC;
```

#### 7f: Create Semantic View for Natural Language Queries

First create a unified forecast view joining all 3 result tables (avoids column name conflicts in the semantic view):

```sql
CREATE OR REPLACE VIEW TEST_DATABASE.TEST_SCHEMA.TOWER_ALL_FORECASTS AS
SELECT
  f.SERIES::VARCHAR                 AS tower_id,
  f.TS                               AS forecast_date,
  f.FORECAST                         AS fault_rate_forecast,
  f.LOWER_BOUND                      AS fault_rate_lb,
  f.UPPER_BOUND                      AS fault_rate_ub,
  v.FORECAST                         AS call_volume_forecast,
  v.LOWER_BOUND                      AS call_volume_lb,
  v.UPPER_BOUND                      AS call_volume_ub,
  d.FORECAST                         AS data_usage_forecast_mb,
  d.LOWER_BOUND                      AS data_usage_lb,
  d.UPPER_BOUND                      AS data_usage_ub
FROM TEST_DATABASE.TEST_SCHEMA.TOWER_DROP_FORECAST_RESULTS f
JOIN TEST_DATABASE.TEST_SCHEMA.TOWER_CALL_VOLUME_FORECAST_RESULTS v
  ON f.SERIES = v.SERIES AND f.TS = v.TS
JOIN TEST_DATABASE.TEST_SCHEMA.TOWER_DATA_USAGE_FORECAST_RESULTS d
  ON f.SERIES = d.SERIES AND f.TS = d.TS;
```

> **Note:** All 3 FORECAST result tables have the same column name `FORECAST`. Joining them into a single view with distinct aliases (`fault_rate_forecast`, `call_volume_forecast`, `data_usage_forecast_mb`) avoids identifier conflicts in the semantic view DDL.

Now create the semantic view over the CDR table and the unified forecast view:

```sql
CREATE OR REPLACE SEMANTIC VIEW TEST_DATABASE.TEST_SCHEMA.TOWER_MAINTENANCE_ANALYTICS
  TABLES (
    TEST_DATABASE.TEST_SCHEMA.CALL_DETAIL_RECORDS,
    TEST_DATABASE.TEST_SCHEMA.TOWER_ALL_FORECASTS
  )
  FACTS (
    CALL_DETAIL_RECORDS.DATA_USAGE_MB AS DATA_USAGE_MB,
    CALL_DETAIL_RECORDS.CHARGE_AMOUNT AS CHARGE_AMOUNT,
    TOWER_ALL_FORECASTS.FAULT_RATE_FORECAST AS FAULT_RATE_FORECAST COMMENT='Predicted call fault (drop) rate (0.0 to 1.0)',
    TOWER_ALL_FORECASTS.FAULT_RATE_LB AS FAULT_RATE_LB COMMENT='Lower bound of 90% prediction interval for fault rate',
    TOWER_ALL_FORECASTS.FAULT_RATE_UB AS FAULT_RATE_UB COMMENT='Upper bound of 90% prediction interval for fault rate',
    TOWER_ALL_FORECASTS.CALL_VOLUME_FORECAST AS CALL_VOLUME_FORECAST COMMENT='Predicted number of calls per day',
    TOWER_ALL_FORECASTS.CALL_VOLUME_LB AS CALL_VOLUME_LB COMMENT='Lower bound of 90% prediction interval for call volume',
    TOWER_ALL_FORECASTS.CALL_VOLUME_UB AS CALL_VOLUME_UB COMMENT='Upper bound of 90% prediction interval for call volume',
    TOWER_ALL_FORECASTS.DATA_USAGE_FORECAST_MB AS DATA_USAGE_FORECAST_MB COMMENT='Predicted total data usage in MB per day',
    TOWER_ALL_FORECASTS.DATA_USAGE_LB AS DATA_USAGE_LB COMMENT='Lower bound of 90% prediction interval for data usage',
    TOWER_ALL_FORECASTS.DATA_USAGE_UB AS DATA_USAGE_UB COMMENT='Upper bound of 90% prediction interval for data usage'
  )
  DIMENSIONS (
    CALL_DETAIL_RECORDS.RECORD_ID AS RECORD_ID,
    CALL_DETAIL_RECORDS.CALLER_NUMBER AS CALLER_NUMBER,
    CALL_DETAIL_RECORDS.CALLEE_NUMBER AS CALLEE_NUMBER,
    CALL_DETAIL_RECORDS.CALL_TYPE AS CALL_TYPE,
    CALL_DETAIL_RECORDS.DISPOSITION AS DISPOSITION,
    CALL_DETAIL_RECORDS.DURATION_SECONDS AS DURATION_SECONDS,
    CALL_DETAIL_RECORDS.NETWORK_TYPE AS NETWORK_TYPE,
    CALL_DETAIL_RECORDS.CELL_TOWER_ID AS CELL_TOWER_ID COMMENT='Unique identifier for each cell tower (e.g. LAS-012, SEA-007)',
    CALL_DETAIL_RECORDS.PLAN_TYPE AS PLAN_TYPE,
    CALL_DETAIL_RECORDS.ROAMING AS ROAMING,
    CALL_DETAIL_RECORDS.CALL_START AS CALL_START,
    CALL_DETAIL_RECORDS.CALL_END AS CALL_END,
    CALL_DETAIL_RECORDS.EVENT_TIMESTAMP AS EVENT_TIMESTAMP,
    TOWER_ALL_FORECASTS.TOWER_ID AS TOWER_ID WITH SYNONYMS=('tower_id','tower','series') COMMENT='Cell tower identifier (e.g. LAS-012, SEA-007)',
    TOWER_ALL_FORECASTS.FORECAST_DATE AS FORECAST_DATE WITH SYNONYMS=('forecast_date','date','ts') COMMENT='Forecast date'
  )
  METRICS (
    CALL_DETAIL_RECORDS.TOTAL_CALLS AS COUNT(*) COMMENT='Total number of calls',
    CALL_DETAIL_RECORDS.DROPPED_CALLS AS SUM(CASE WHEN DISPOSITION IN ('DROPPED', 'FAILED') THEN 1 ELSE 0 END) COMMENT='Number of dropped or failed calls',
    CALL_DETAIL_RECORDS.CALL_FAULT_RATE AS SUM(CASE WHEN DISPOSITION IN ('DROPPED', 'FAILED') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) COMMENT='Fraction of calls that were dropped or failed (0.0 to 1.0)',
    CALL_DETAIL_RECORDS.AVG_CALL_DURATION_SECONDS AS AVG(DURATION_SECONDS) COMMENT='Average call duration in seconds',
    CALL_DETAIL_RECORDS.TOTAL_REVENUE AS SUM(CHARGE_AMOUNT) COMMENT='Total charge amount across all calls',
    TOWER_ALL_FORECASTS.AVG_FORECAST_FAULT_RATE AS AVG(FAULT_RATE_FORECAST) COMMENT='Average forecasted fault rate across the horizon',
    TOWER_ALL_FORECASTS.PEAK_FORECAST_FAULT_RATE AS MAX(FAULT_RATE_FORECAST) COMMENT='Highest forecasted fault rate in the horizon',
    TOWER_ALL_FORECASTS.CRITICAL_DAYS AS SUM(CASE WHEN FAULT_RATE_FORECAST >= 0.40 THEN 1 ELSE 0 END) COMMENT='Days where fault rate >= 40%',
    TOWER_ALL_FORECASTS.AVG_FORECASTED_CALL_VOLUME AS AVG(CALL_VOLUME_FORECAST) COMMENT='Average forecasted daily call volume',
    TOWER_ALL_FORECASTS.PEAK_FORECASTED_CALL_VOLUME AS MAX(CALL_VOLUME_FORECAST) COMMENT='Highest forecasted daily call volume',
    TOWER_ALL_FORECASTS.AVG_FORECASTED_DATA_USAGE_MB AS AVG(DATA_USAGE_FORECAST_MB) COMMENT='Average forecasted daily data usage in MB',
    TOWER_ALL_FORECASTS.PEAK_FORECASTED_DATA_USAGE_MB AS MAX(DATA_USAGE_FORECAST_MB) COMMENT='Highest forecasted daily data usage in MB'
  )
  COMMENT='Predictive maintenance analytics for cell tower network. Combines live CDR data with 7-day ML forecasts for fault rate, call volume, and data usage per tower. Identifies CRITICAL towers (fault rate >= 40%) for immediate dispatch and supports capacity planning.'
  AI_VERIFIED_QUERIES (
    WHICH_TOWERS_NEED_MAINTENANCE AS (
      QUESTION 'Which towers need maintenance based on their forecasted call drop rates?'
      SQL 'SELECT tower_id,
             ROUND(AVG(fault_rate_forecast), 3) AS avg_7day_fault_rate,
             ROUND(MAX(fault_rate_forecast), 3) AS peak_fault_rate,
             COUNT_IF(fault_rate_forecast >= 0.40) AS critical_days,
             CASE
               WHEN AVG(fault_rate_forecast) >= 0.40 THEN ''CRITICAL''
               WHEN AVG(fault_rate_forecast) >= 0.20 THEN ''AT RISK''
               ELSE ''HEALTHY''
             END AS maintenance_status
           FROM tower_all_forecasts
           GROUP BY tower_id
           ORDER BY avg_7day_fault_rate DESC'
    ),
    CRITICAL_TOWERS AS (
      QUESTION 'Which towers are critical?'
      SQL 'SELECT tower_id
           FROM tower_all_forecasts
           GROUP BY tower_id
           HAVING AVG(fault_rate_forecast) >= 0.40
           ORDER BY AVG(fault_rate_forecast) DESC'
    ),
    CALL_VOLUME_FORECAST_PER_TOWER AS (
      QUESTION 'What is the forecasted call volume for each tower over the next 7 days?'
      SQL 'SELECT tower_id,
             ROUND(AVG(call_volume_forecast), 0) AS avg_daily_calls,
             ROUND(MAX(call_volume_forecast), 0) AS peak_daily_calls,
             ROUND(MIN(call_volume_forecast), 0) AS min_daily_calls
           FROM tower_all_forecasts
           GROUP BY tower_id
           ORDER BY avg_daily_calls DESC'
    ),
    DATA_USAGE_FORECAST_PER_TOWER AS (
      QUESTION 'What is the forecasted data usage for each tower over the next 7 days?'
      SQL 'SELECT tower_id,
             ROUND(AVG(data_usage_forecast_mb), 1) AS avg_daily_data_mb,
             ROUND(MAX(data_usage_forecast_mb), 1) AS peak_daily_data_mb,
             ROUND(SUM(data_usage_forecast_mb), 1) AS total_7day_data_mb
           FROM tower_all_forecasts
           GROUP BY tower_id
           ORDER BY avg_daily_data_mb DESC'
    ),
    CALL_FAULT_RATE_PER_TOWER AS (
      QUESTION 'What is the call drop rate for each tower?'
      SQL 'SELECT cell_tower_id,
             COUNT(*) AS total_calls,
             COUNT_IF(disposition IN (''DROPPED'', ''FAILED'')) AS dropped_calls,
             ROUND(COUNT_IF(disposition IN (''DROPPED'', ''FAILED'')) / COUNT(*), 3) AS fault_rate
           FROM call_detail_records
           GROUP BY cell_tower_id
           ORDER BY fault_rate DESC'
    ),
    SEVEN_DAY_ALL_FORECASTS AS (
      QUESTION 'What is the 7-day forecast for each tower including drop rate, call volume, and data usage?'
      SQL 'SELECT tower_id,
             forecast_date,
             ROUND(fault_rate_forecast, 3) AS predicted_fault_rate,
             ROUND(call_volume_forecast, 0) AS predicted_calls,
             ROUND(data_usage_forecast_mb, 1) AS predicted_data_mb,
             CASE
               WHEN fault_rate_forecast >= 0.40 THEN ''CRITICAL''
               WHEN fault_rate_forecast >= 0.20 THEN ''AT RISK''
               ELSE ''HEALTHY''
             END AS maintenance_status
           FROM tower_all_forecasts
           ORDER BY tower_id, forecast_date'
    )
  );
```

**Sample natural language questions to ask Cortex Analyst:**
- "Which towers need maintenance?"
- "Which towers are critical?"
- "What is the 7-day forecast per tower?"
- "What is the call drop rate for each tower?"
- "What is the forecasted call volume for each tower?"
- "What is the forecasted data usage per tower?"
- "What is the load on towers for the next 7 days?"

---

## Key Consumer Patterns (Reference)

### SSv2 Client Initialization

```java
SnowflakeStreamingIngestClient sfClient = SnowflakeStreamingIngestClientFactory
    .builder("TEST_CLIENT", database, schema, table + "-STREAMING")
    .setProperties(sfProps)
    .build();
```

### Channel Open + Offset Recovery

Each partition gets its own channel named `<prefix>_P<partition>`:
```java
OpenChannelResult result = sfClient.openChannel(channelPrefix + "_P" + partition);
SnowflakeStreamingIngestChannel channel = result.getChannel();
String lastToken = channel.getLatestCommittedOffsetToken();
if (lastToken != null) {
    consumer.seek(tp, Long.parseLong(lastToken) + 1);
}
```

### Retry Strategy by HTTP Status

| HTTP Status | Action |
|-------------|--------|
| 401, 403 | Auth error -- fail immediately |
| 409 | Channel invalidated -- close + reopen channel, retry |
| 429, 5xx | Backpressure -- exponential backoff (500ms to 30s), unlimited retries |
| 408 | Timeout -- exponential backoff, max 10 attempts |

### Channel Health Check

Every 30 seconds, poll `channel.getChannelStatus()`. If status is not `SUCCESS`, proactively reopen the channel before the next append fails.

### Kafka Offset Commit

Kafka offsets are committed **only after** Snowflake confirms persistence:
```java
String committedToken = channel.getLatestCommittedOffsetToken();
// Only commit to Kafka if token has advanced since last commit
consumer.commitSync(offsets);
```

## Project Structure

```
custom-kafka-consumer/
├── consumer-config.properties          # Consumer: Kafka + Snowflake target config
├── producer-config.properties          # Producer: Kafka broker + tuning config
├── profile.json                        # Snowflake credentials (gitignored)
├── pom.xml                             # Maven build (snowpipe-streaming 1.3.0, kafka-clients 3.5.1)
└── src/
    ├── main/java/com/snowflake/streaming/
    │   ├── consumer/
    │   │   ├── Main.java               # Entry point, launches N consumer threads
    │   │   ├── CustomKafkaConsumer.java # Kafka -> Snowpipe Streaming consumer (1:1 partition-to-channel)
    │   │   └── Config.java             # Loads consumer-config.properties
    │   └── producer/
    │       ├── FakeKafkaWriter.java     # Interactive CLI CDR generator
    │       └── Config.java             # Loads producer-config.properties
    └── test/java/com/snowflake/streaming/consumer/
        └── CustomKafkaConsumerTest.java # Unit tests (Mockito)
```

## Troubleshooting

**Kafka connection refused:**
- Verify broker is running: `brew services list | grep kafka`
- Check bootstrap server address matches config

**Auth errors (HTTP 401/403):**
- Verify `profile.json` has correct account, user, role, and private_key_file path
- Ensure RSA key-pair is registered with the Snowflake user
- Check the role has INSERT privileges on the target table

**Channel invalidated (HTTP 409):**
- Table schema may have changed -- recreate the table
- Another consumer may be writing to the same channel -- ensure unique channel names
- The consumer automatically reopens channels on 409; check logs for repeated occurrences

**Maven build failures:**
- Wrong Java version: `java -version` must show 17+
- Dependencies not resolving: check network and `~/.m2/settings.xml` proxy config

**No data appearing in Snowflake:**
- Check consumer logs for errors
- Verify Kafka topic has messages: `kafka-console-consumer --topic test-topic --from-beginning --bootstrap-server localhost:9092 --max-messages 5`
- Confirm `consumer-config.properties` database/schema/table match the Snowflake objects

### Step 8: Stop Demo and Tear Down Resources

**Ask the user:** "The demo is complete. Would you like to tear down all resources?"

- If **yes** → run the teardown below.
- If **no** → leave everything running; remind them of what's still active.

#### Tear Down Kafka

Stop the broker and delete the demo topic:

```bash
kafka-topics --delete --topic test-topic --bootstrap-server localhost:9092
brew services stop kafka
```

#### Tear Down Snowflake Objects

```sql
USE DATABASE TEST_DATABASE;
USE SCHEMA TEST_SCHEMA;

DROP SEMANTIC VIEW IF EXISTS TEST_DATABASE.TEST_SCHEMA.TOWER_MAINTENANCE_ANALYTICS;
DROP SNOWFLAKE.ML.FORECAST IF EXISTS tower_drop_forecast;
DROP SNOWFLAKE.ML.FORECAST IF EXISTS tower_call_volume_forecast;
DROP SNOWFLAKE.ML.FORECAST IF EXISTS tower_data_usage_forecast;
DROP TABLE IF EXISTS TEST_DATABASE.TEST_SCHEMA.TOWER_DROP_FORECAST_RESULTS;
DROP TABLE IF EXISTS TEST_DATABASE.TEST_SCHEMA.TOWER_CALL_VOLUME_FORECAST_RESULTS;
DROP TABLE IF EXISTS TEST_DATABASE.TEST_SCHEMA.TOWER_DATA_USAGE_FORECAST_RESULTS;
DROP TABLE IF EXISTS TEST_DATABASE.TEST_SCHEMA.CALL_DETAIL_RECORDS_HISTORY;
DROP VIEW  IF EXISTS TEST_DATABASE.TEST_SCHEMA.TOWER_DROP_RATE_TS;
DROP VIEW  IF EXISTS TEST_DATABASE.TEST_SCHEMA.TOWER_ALL_FORECASTS;
DROP VIEW  IF EXISTS TEST_DATABASE.TEST_SCHEMA.TOWER_CALL_VOLUME_TS;
DROP VIEW  IF EXISTS TEST_DATABASE.TEST_SCHEMA.TOWER_DATA_USAGE_TS;
DROP TABLE IF EXISTS TEST_DATABASE.TEST_SCHEMA.CALL_DETAIL_RECORDS;
DROP SCHEMA IF EXISTS TEST_DATABASE.TEST_SCHEMA;
DROP DATABASE IF EXISTS TEST_DATABASE;
```

#### What Remains Active (if user skips teardown)

| Resource | Location | How to stop |
|----------|----------|-------------|
| Kafka broker | localhost:9092 | `brew services stop kafka` |
| Consumer process | Terminal 1 | Ctrl+C |
| Producer process | Terminal 2 | `quit` then Ctrl+C |
| Snowflake objects | TEST_DATABASE.TEST_SCHEMA | Run DROP statements above |

## Stopping Points

- After Step 1: Confirm Kafka is running and topic exists
- After Step 2: Confirm Snowflake table exists and profile.json is ready
- After Step 3: Confirm config values before writing files
- After Step 7 gate: Confirm `CALL_DETAIL_RECORDS` has ≥ 300 rows before training the models
- After Step 8: Confirm whether user wants to tear down before executing any DROP statements

## Output

A full end-to-end demo:
1. Fake CDRs flow from Kafka into a Snowflake table via Snowpipe Streaming V2, demonstrating 1:1 partition-to-channel mapping, offset recovery, retry logic, and channel health checks.
2. Three `SNOWFLAKE.ML.FORECAST` models predict call drop rate, call volume, and data usage per tower for the next 7 days — flagging CRITICAL towers for immediate maintenance dispatch and supporting capacity planning.
3. All resources cleanly torn down (or left running for further exploration).
