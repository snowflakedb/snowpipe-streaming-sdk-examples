# Node.js Snowpipe Streaming SDK Example

This example demonstrates how to use the Snowflake Streaming Ingest SDK in Node.js to ingest data into Snowflake in real-time using the [high-performance architecture](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview) and default pipe.

## Prerequisites

- Node.js 20 or higher
- npm (Node.js package manager)
- A Snowflake account with appropriate permissions
- RSA key-pair authentication configured

## Setup

### 1. Generate RSA Key Pair

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Register the public key with your Snowflake user:

```sql
ALTER USER MY_USER SET RSA_PUBLIC_KEY='<contents of rsa_key.pub, without header/footer>';
```

### 2. Create a Snowflake Table

Create a target table in your Snowflake account:

```sql
CREATE OR REPLACE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE (
    c1 NUMBER,
    c2 VARCHAR,
    ts TIMESTAMP_NTZ
);
```

No `CREATE PIPE` is needed — the high-performance architecture automatically creates a **default pipe** named `MY_TABLE-STREAMING` when you first open a channel.

### 3. Install Dependencies

```bash
npm install
```

### 4. Configure Authentication

Create a `profile.json` file in the `nodejs-example` directory using `profile.json.example` as a template:

```json
{
  "account": "<account_identifier>",
  "user": "your_username",
  "url": "https://<account_identifier>.snowflakecomputing.com:443",
  "private_key_file": "rsa_key.p8",
  "role": "your_role"
}
```

**Note:** Use `private_key_file` to reference the key file path. For production, consider using a secure credential manager.

### 5. Update Configuration

Edit `streaming_ingest_example.js` and update the constants at the top of the file:

- `DATABASE` - Your database name
- `SCHEMA` - Your schema name
- `TABLE` - Your table name (the pipe name is derived automatically as `<TABLE>-STREAMING`)

## Run

```bash
npm start
```

Or directly:

```bash
node streaming_ingest_example.js
```

## What the Example Does

1. **Creates a Streaming Ingest Client** - Connects to Snowflake using credentials from `profile.json`
2. **Opens a Channel** - Creates a channel on the default pipe (`MY_TABLE-STREAMING`)
3. **Ingests Data** - Streams 100,000 rows with columns matched by name (MATCH_BY_COLUMN_NAME):
   - `c1`: Integer counter
   - `c2`: String representation of the counter
   - `ts`: Current timestamp
4. **Waits for Completion** - Uses `waitForCommit` to block until all data is committed, then prints the channel status
5. **Closes Resources** - Properly closes the channel and client via try/finally blocks

## Expected Output

```
Client created successfully
Channel opened: MY_CHANNEL_<uuid>
Ingesting 100000 rows...
Ingested 10000 rows...
Ingested 20000 rows...
...
All rows submitted. Waiting for ingestion to complete...
Latest committed offset token: 100000
All data committed successfully
Data ingestion completed
```

## Troubleshooting

- **Connection Issues**: Verify your `profile.json` credentials and network connectivity to Snowflake
- **Permission Errors**: Ensure your role has the necessary privileges on the database, schema, and table
- **Table Not Found**: Verify the table exists — the default pipe is created automatically
- **VARIANT Columns**: If using VARIANT columns, pass data as a plain JavaScript object, not a JSON string
- **Node.js Version**: Ensure you are running Node.js 20 or higher (`node --version`)

## Additional Resources

- [High-Performance Streaming Overview](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview)
- [Getting Started Guide](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-getting-started)
- [Snowpipe Streaming SDK on npm](https://www.npmjs.com/package/snowpipe-streaming)
