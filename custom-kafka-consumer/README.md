# Test Consumer

Test harness for validating a customer's Kafka-to-Snowflake streaming consumer code. Uses the Snowpipe Streaming SDK v2 (`com.snowflake:snowpipe-streaming:1.1.0`).

## Prerequisites

- Java 17+
- Maven
- Docker (for local Kafka)
- Snowflake account with a streaming pipe

## Snowflake Setup

```sql
CREATE OR REPLACE DATABASE TEST_DB;
CREATE OR REPLACE TABLE TEST_TABLE (C1 NUMBER, TS TIMESTAMP_LTZ);
CREATE OR REPLACE PIPE TEST_PIPE AS COPY INTO TEST_TABLE
  FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

## Configuration

1. Edit `config.properties` with your Kafka and Snowflake settings.
2. Create or update `profile.json` with your Snowflake connection details in project root directory:

```json
{
  "url": "https://<account>.snowflakecomputing.com:443",
  "account": "<account>",
  "user": "<user>",
  "role": "<role>",
  "private_key": "<base64-encoded-private-key>"
}
```

## Running

```bash
# Start local Kafka
docker-compose up -d

# Produce test records
mvn compile exec:java -Dexec.args="produce 1000"

# Run the consumer
mvn compile exec:java

# Stop with Ctrl+C
```

## Verify

```sql
SELECT COUNT(*) FROM TEST_DB.PUBLIC.TEST_TABLE;
SELECT * FROM TEST_DB.PUBLIC.TEST_TABLE LIMIT 10;
```

## Project Structure

```
├── config.properties                          # Runtime config
├── profile.json                               # Snowflake credentials (gitignored)
├── docker-compose.yml                         # Local Kafka + Zookeeper
├── customer-fixes.java                        # Fixed customer code (reference)
├── pom.xml
└── src/main/java/com/snowflake/sai/consumer/
    ├── Main.java                  # Entry point (produce / consume)
    ├── CustomerConsumerRunner.java # Customer code with fixes applied
    ├── RowSetBuilder.java         # Mock of customer's row buffer
    ├── TestProducer.java          # Pushes test JSON to Kafka
    └── Config.java                # Config loader
```
