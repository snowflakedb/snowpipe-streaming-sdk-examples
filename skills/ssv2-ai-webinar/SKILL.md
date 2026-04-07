---
name: ssv2-AI-webinar
description: "End-to-end SSv2 + AI demo for webinars. Sets up Snowpipe Streaming V2 with background data generation, deploys a live Streamlit dashboard, then layers on a Semantic View and Cortex Agent so the presenter can do natural-language queries on live-streaming data in Snowsight. Triggers: ssv2 ai webinar, ssv2 webinar demo, ssv2 ai demo, streaming ai demo, snowpipe streaming webinar, ssv2 cortex agent demo, ssv2 semantic view demo."
---

<!-- Copyright (c) 2026 Snowflake Inc. Licensed under Apache 2.0. See LICENSE. -->

## When to use

Use this skill when the user wants to:

- Run a webinar or live demo showing Snowpipe Streaming V2 + Cortex AI together
- Demo real-time streaming data with natural-language querying via Cortex Agent
- Set up an end-to-end pipeline: streaming ingestion -> live dashboard -> semantic view -> Cortex Agent

This is **not** for a quick SSv2 tryout (use `ssv2-quickstart` for that). This skill is specifically for a **presentation-ready demo** that showcases streaming + AI.

## What this skill provides

A fully automated, presentation-ready demo pipeline:

1. **Streaming setup + dashboard** (Steps 0-5) — Same as SSv2 Quickstart: platform detection, RSA keys, Snowflake objects, Python venv. Step 5 deploys the Streamlit dashboard AND starts background streaming **in parallel**
2. **Semantic View** (Step 6) — Creates a semantic view on the streaming table via direct SQL with rich synonyms, computed dimensions, and 8 metrics
3. **Cortex Agent** (Step 7) — Creates a Cortex Agent via direct SQL with `cortex_analyst_text_to_sql` pointed at the semantic view
4. **Showcase queries** (Step 8) — Runs 4 live queries against the semantic view to prove everything works with real streaming data
5. **Presenter handoff** (Step 9) — Clear instructions for the live demo portion in Snowsight
6. **Cleanup** (Step 10) — Drops agent, semantic view, and all quickstart objects

## Critical concepts

### Default pipe naming convention

The high-performance architecture automatically creates a default pipe when data is first ingested into a table. **No `CREATE PIPE` SQL is required.**

The Python SDK references the default pipe using this naming convention:

```
<TABLE_NAME>-streaming
```

**Important:** Use a **hyphen** (`-`), not an underscore (`_`).

### Background streaming

Unlike the quickstart (which blocks for N minutes), this skill runs the streaming script **in the background** for 30 minutes. This lets the presenter immediately move on to building the AI layer while data keeps flowing. The wow factor: re-ask the same question later and the numbers have changed.

### Direct SQL for speed

Steps 6 and 7 use direct `CREATE SEMANTIC VIEW` and `CREATE AGENT` SQL rather than invoking the bundled `semantic-view` and `cortex-agent` skills. This is because we know the exact table schema (it's always the same 12 columns) and want maximum speed for a live demo. The semantic view and agent are created in ~2 seconds each instead of minutes.

## Instructions

**IMPORTANT EXECUTION GUIDELINES:**

1. **Announce each step clearly** — Before executing each step, print a clear header like "**Step X -- [Step Name]**" so the user knows exactly where they are.

2. **Batch commands aggressively to minimize permission prompts** — Same batching rules as the quickstart:
   - **Step 1:** Run platform checks (Bash) and Snowflake context query (SQL) as **two parallel tool calls**
   - **Step 2:** Run both `openssl genrsa...` and the pubkey extraction in **one single Bash call**
   - **Step 3:** Run all CREATE/GRANT statements in **one single SQL call**
   - **Steps 4a:** Write `profile.json` and `ssv2_demo.py` as **two parallel FileWrite calls**
   - **Step 5 parallel launch:** CREATE STAGE (SQL), sed template to streamlit_app.py (Bash), AND start streaming (Bash with `run_in_background=true`) as **three parallel tool calls**

3. **Use parallel tool calls** whenever operations are independent.

4. **Log all SQL to `ssv2_demo_sql.log`** — Every SQL execution gets appended to a local log file with step headers and timestamps.

5. **Do NOT ask about demo duration** — This skill always runs streaming for 30 minutes in the background. Skip that question from the quickstart preferences.

---

### Step 0 -- Confirm intent

**Before doing anything**, confirm the user wants to run the full webinar demo:

> "This will run the **SSv2 AI Webinar Demo** -- a fully automated pipeline that:
> - Creates a demo database, schema, table, user, and role in Snowflake
> - Generates RSA keys and a Python virtual environment locally
> - Deploys a live Streamlit dashboard to monitor data in real-time
> - Starts streaming fake user data **in the background** (runs for 30 min)
> - Creates a **Semantic View** on the streaming data for natural-language querying
> - Creates a **Cortex Agent** you can interact with in Snowsight
>
> Setup takes ~5 minutes. After that, you drive the live demo in Snowsight. Want to proceed?"

If the user says no, stop gracefully.

---

### Step 1 -- Detect platform, check Python, and gather Snowflake context

**Purpose:** Verify system requirements and understand the current Snowflake context.

**Execute these two tool calls in parallel:**

**Tool call 1 -- Bash** (platform checks + initialize SQL log):
```bash
echo "=== OS ==="; uname -s 2>/dev/null || echo "WINDOWS"; echo "=== Python ==="; python3 --version 2>/dev/null || python --version 2>/dev/null || echo "NOT FOUND"; echo "=== Working Directory ==="; pwd; echo "=== Home Directory ==="; echo $HOME; echo "-- SSV2 AI Webinar SQL Log" > ssv2_demo_sql.log; echo "-- Generated by Cortex Code SSV2 AI Webinar Skill" >> ssv2_demo_sql.log; echo "" >> ssv2_demo_sql.log
```

**Tool call 2 -- SQL** (Snowflake context):
```sql
SELECT
    CURRENT_USER()       AS current_user,
    CURRENT_ROLE()       AS current_role,
    CURRENT_DATABASE()   AS current_database,
    CURRENT_SCHEMA()     AS current_schema,
    CURRENT_WAREHOUSE()  AS current_warehouse,
    CURRENT_ACCOUNT()    AS current_account;
```

Interpret the OS result:
- `Darwin` -> **macOS**
- `Linux` -> **Linux** (also check glibc >= 2.26: `ldd --version 2>&1 | head -1`)
- `WINDOWS` or `MINGW*` or `MSYS*` or `CYGWIN*` -> **Windows** (experimental -- warn user, recommend WSL2)

**Error handling -- stop if:**
- Python not found or below 3.9
- Linux glibc below 2.26
- `CURRENT_WAREHOUSE()` is NULL

Store the working Python command (`python3` or `python`) and all Snowflake context values.

#### 1b. Ask the user for preferences

Ask the user:
- Which **database** and **schema** to use (default: `SSV2_WEBINAR_DB.SSV2_SCHEMA`)
- A **table name** for the landing table (default: `SSV2_WEBINAR_USERS`)
- Whether to **deploy a Streamlit dashboard** (default: yes)

Store the dashboard preference as a boolean (`DEPLOY_STREAMLIT`). If the user declines, Step 5 will skip the dashboard deployment and only start the background streaming.

**Do NOT ask about demo duration** -- this skill always streams for 30 minutes in the background.

---

### Step 2 -- Generate RSA key-pair

**Purpose:** The Snowpipe Streaming SDK uses key-pair (JWT) authentication.

**IMPORTANT — NEVER display private or public key content to the user.** All key material must stay hidden from the conversation output.

Run key generation in **one single Bash call** with all output suppressed:

```bash
openssl genrsa 2048 2>/dev/null | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt 2>/dev/null && chmod 600 rsa_key.p8 && openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub 2>/dev/null && echo "RSA key-pair generated successfully (rsa_key.p8 + rsa_key.pub)"
```

Then **silently read** `rsa_key.pub` using the Read tool. **Do NOT echo, cat, or print the key content in any Bash command or in your response to the user.** Internally strip the `-----BEGIN PUBLIC KEY-----` and `-----END PUBLIC KEY-----` header/footer lines and join the remaining lines into a single string. Store that value for use in Step 3's `ALTER USER ... SET RSA_PUBLIC_KEY` statement.

**What to tell the user:** "RSA key-pair generated." — nothing more. Do not show any key material.

---

### Step 3 -- Create Snowflake objects, demo user, and register RSA key

**Purpose:** Create all Snowflake objects in one SQL call.

**IMPORTANT — Key material in SQL:** The `ALTER USER ... SET RSA_PUBLIC_KEY` statement contains the public key value. When logging this SQL to `ssv2_demo_sql.log`, replace the actual key value with `<REDACTED>`. When presenting SQL output to the user, do NOT display the full ALTER USER statement — just confirm it succeeded.

```sql
CREATE DATABASE IF NOT EXISTS <DATABASE>;
CREATE SCHEMA IF NOT EXISTS <DATABASE>.<SCHEMA>;
USE DATABASE <DATABASE>;
USE SCHEMA <SCHEMA>;
CREATE OR REPLACE TABLE <DATABASE>.<SCHEMA>.<TABLE_NAME> (
    user_id              INTEGER,
    first_name           VARCHAR(100),
    last_name            VARCHAR(100),
    email                VARCHAR(255),
    phone_number         VARCHAR(50),
    address              VARCHAR(500),
    date_of_birth        DATE,
    registration_date    TIMESTAMP_NTZ,
    city                 VARCHAR(100),
    state                VARCHAR(100),
    country              VARCHAR(100),
    order_amount         NUMBER(10,2)
);
CREATE ROLE IF NOT EXISTS SSV2_DEMO_ROLE;
CREATE USER IF NOT EXISTS SSV2_DEMO_USER DEFAULT_ROLE = SSV2_DEMO_ROLE;
GRANT ROLE SSV2_DEMO_ROLE TO USER SSV2_DEMO_USER;
ALTER USER SSV2_DEMO_USER SET RSA_PUBLIC_KEY = '<PUBK_VALUE>';
GRANT USAGE ON DATABASE <DATABASE> TO ROLE SSV2_DEMO_ROLE;
GRANT USAGE ON SCHEMA <DATABASE>.<SCHEMA> TO ROLE SSV2_DEMO_ROLE;
GRANT USAGE ON WAREHOUSE <WAREHOUSE> TO ROLE SSV2_DEMO_ROLE;
GRANT OWNERSHIP ON TABLE <DATABASE>.<SCHEMA>.<TABLE_NAME> TO ROLE SSV2_DEMO_ROLE COPY CURRENT GRANTS;
GRANT SELECT ON TABLE <DATABASE>.<SCHEMA>.<TABLE_NAME> TO ROLE <CURRENT_ROLE>;
DESC USER SSV2_DEMO_USER;
```

Look for `RSA_PUBLIC_KEY` in the DESC USER output to confirm it was set.

---

### Step 4 -- Write config files, demo script, create Python venv

#### 4a. Write profile.json and ssv2_demo.py in parallel

**FileWrite 1 -- profile.json:**

```json
{
    "user": "SSV2_DEMO_USER",
    "account": "<ACCOUNT_IDENTIFIER>",
    "url": "https://<ACCOUNT_IDENTIFIER>.snowflakecomputing.com:443",
    "private_key_file": "rsa_key.p8",
    "role": "SSV2_DEMO_ROLE"
}
```

**FileWrite 2 -- ssv2_demo.py:**

Use the demo script from the **"Demo script reference"** section below. Key difference from quickstart: `DEMO_MINUTES = 30` (runs for 30 minutes to keep data flowing throughout the webinar).

#### 4b. Create Python virtual environment and install dependencies

```bash
<PYTHON_CMD> -m venv ssv2_venv && source ssv2_venv/bin/activate && pip install --upgrade pip && pip install snowpipe-streaming faker && python -c "from snowflake.ingest.streaming import StreamingIngestClient; print('SDK OK')" && python -c "from faker import Faker; print('Faker OK')"
```

---

### Demo script reference (ssv2_demo.py)

```python
import time
import os
import random
from faker import Faker

os.environ["SS_LOG_LEVEL"] = "warn"
from snowflake.ingest.streaming import StreamingIngestClient

fake = Faker()

# --- Configuration (auto-populated by Cortex Code) ---
BATCH_SIZE = 5
DEMO_MINUTES = 30  # Runs in background for the full webinar
NUM_BATCHES = DEMO_MINUTES * 120  # 120 batches per minute (5 rows every 0.5s)
DATABASE = "<DATABASE>"
SCHEMA   = "<SCHEMA>"

# Default auto-created pipe: <TABLE_NAME>-streaming (hyphen, not underscore)
PIPE     = "<TABLE_NAME>-streaming"

PROFILE_JSON_PATH = "profile.json"

# --- Initialize Streaming Client ---
print(f"Connecting to Snowflake...")
print(f"  Database: {DATABASE}")
print(f"  Schema:   {SCHEMA}")
print(f"  Pipe:     {PIPE} (default auto-created pipe)")

try:
    client = StreamingIngestClient(
        "SSV2_WEBINAR_CLIENT",
        DATABASE,
        SCHEMA,
        PIPE,
        profile_json=PROFILE_JSON_PATH,
        properties=None,
    )
except Exception as e:
    print(f"\n[ERROR] Failed to create StreamingIngestClient:")
    print(f"  {e}")
    raise SystemExit(1)

# --- Open channel ---
print(f"\nOpening channel...")
try:
    channel, status = client.open_channel("SSV2_WEBINAR_CHANNEL")
    print(f"  Channel: {status.channel_name}")
    print(f"  Status:  {status.status_code}")
except Exception as e:
    print(f"\n[ERROR] Failed to open channel:")
    print(f"  {e}")
    client.close()
    raise SystemExit(1)

# --- Stream fake user data in batches ---
total_rows = BATCH_SIZE * NUM_BATCHES
print(f"\nStreaming {total_rows} rows ({NUM_BATCHES} batches of {BATCH_SIZE}) over ~{DEMO_MINUTES} minute(s)...")
print(f"Data is flowing in the background. Move on to the AI setup!\n")
errors = []
row_id = 0
for batch in range(1, NUM_BATCHES + 1):
    for _ in range(BATCH_SIZE):
        row_id += 1
        row = {
            "user_id": row_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "address": fake.address().replace("\n", ", "),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
            "registration_date": fake.date_time_this_year().isoformat(),
            "city": fake.city(),
            "state": fake.state(),
            "country": fake.country(),
            "order_amount": round(random.uniform(1, 100), 2),
        }
        try:
            channel.append_row(row, offset_token=str(row_id))
        except Exception as e:
            errors.append((row_id, str(e)))
            if len(errors) >= 5:
                print(f"\n[ERROR] Too many row errors ({len(errors)}). Stopping.")
                break
    if len(errors) >= 5:
        break

    if batch % 120 == 0:  # Progress update every minute
        print(f"  [producer] batch {batch}/{NUM_BATCHES} (row {row_id})")

    time.sleep(0.5)

if errors:
    print(f"\n[WARNING] {len(errors)} rows failed to send:")
    for offset, err in errors[:5]:
        print(f"  Row {offset}: {err}")

# --- Wait for all data to be committed ---
print(f"\n[producer] Waiting for commits to reach offset {row_id}...")
try:
    channel.wait_for_commit(
        lambda token: token is not None and int(token) >= row_id,
        timeout_seconds=120
    )
    print("All rows committed.")
except Exception as e:
    print(f"\n[WARNING] Commit wait timed out or failed: {e}")
    print("Some rows may still be in flight. Check the table directly.")

# --- Display channel status ---
s = channel.get_channel_status()
print(f"\nChannel status:")
print(f"  Channel:              {s.channel_name}")
print(f"  Committed offset:     {s.latest_committed_offset_token}")
print(f"  Rows inserted:        {s.rows_inserted_count}")
print(f"  Rows errored:         {s.rows_error_count}")
print(f"  Avg server latency:   {s.server_avg_processing_latency}")
print(f"  Last error message:   {s.last_error_message}")

# --- Cleanup ---
print(f"\nFinal committed offset: {channel.get_latest_committed_offset_token()}")
channel.close()
client.close()
print("\nStreaming complete! (Background process ending)")
```

---

### Step 5 -- Deploy Streamlit dashboard AND start streaming (in parallel)

**Purpose:** Start the background streaming script, and optionally deploy the live monitoring dashboard. If `DEPLOY_STREAMLIT` is false, skip straight to starting the streaming script only.

#### If DEPLOY_STREAMLIT is false:

Run **only** the streaming script in the background:

```bash
source ssv2_venv/bin/activate && python ssv2_demo.py
```

Use `run_in_background=true`. Tell the user:

> "Data is now streaming in the background (~10 rows/second for 30 minutes). Skipping dashboard -- moving on to the AI layer."

**Immediately proceed to Step 6.**

#### If DEPLOY_STREAMLIT is true (default):

##### 5a. Check for pre-baked Streamlit template

The template lives at `~/.snowflake/cortex/skills/ssv2-AI-webinar/streamlit_app.py.template`. Check if it exists using the Read tool. If it exists, proceed to 5b. If it doesn't exist, fall back to writing the full Streamlit app inline (see the Templates section at the end of this skill for the full source).

##### 5b. Create stage, prepare Streamlit app, AND start streaming -- all in parallel

**Execute these three tool calls in parallel:**

**Tool call 1 -- SQL** (create stage):
```sql
CREATE STAGE IF NOT EXISTS <DATABASE>.<SCHEMA>.SSV2_STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);
```

**Tool call 2 -- Bash** (copy template and substitute placeholders):
```bash
sed -e 's/{{DATABASE}}/<DATABASE>/g' -e 's/{{SCHEMA}}/<SCHEMA>/g' -e 's/{{TABLE_NAME}}/<TABLE_NAME>/g' ~/.snowflake/cortex/skills/ssv2-AI-webinar/streamlit_app.py.template > <LOCAL_PATH>/streamlit_app.py
```

Replace `<DATABASE>`, `<SCHEMA>`, `<TABLE_NAME>` in the sed command with the actual values collected in Step 1.

**Tool call 3 -- Bash** (start streaming in background with `run_in_background=true`):
```bash
source ssv2_venv/bin/activate && python ssv2_demo.py
```

**IMPORTANT:** Use the Bash tool with `run_in_background=true` for tool call 3 so it does not block. The script will run for 30 minutes.

##### 5c. Upload to stage

After 5b completes (stage created + streamlit file prepared), upload the file:

```bash
snow stage copy <LOCAL_PATH>/streamlit_app.py @<DATABASE>.<SCHEMA>.SSV2_STREAMLIT_STAGE --overwrite
```

##### 5d. Create Streamlit app and verify

```sql
CREATE OR REPLACE STREAMLIT <DATABASE>.<SCHEMA>.SSV2_STREAMING_MONITOR
    ROOT_LOCATION = '@<DATABASE>.<SCHEMA>.SSV2_STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = '<WAREHOUSE>';
SHOW STREAMLITS IN SCHEMA <DATABASE>.<SCHEMA>;
```

##### 5e. Present dashboard to user

> "Your real-time streaming dashboard is ready and data is already streaming in the background!
>
> **To access the dashboard:**
> 1. Open Snowsight in your browser
> 2. Navigate to **Projects > Streamlit** in the left sidebar
> 3. Find and click **SSV2_STREAMING_MONITOR**
>
> You should see rows appearing within 5-10 seconds. While you explore the dashboard, I'll set up the AI layer."

**Do NOT wait for the user to confirm the dashboard loaded.** Immediately proceed to Step 6.

---

### Step 6 -- Create Semantic View (direct SQL)

**Purpose:** Create a semantic view on the streaming table so Cortex Analyst can answer natural-language questions about the live data. We use direct SQL here (not the semantic-view skill) because we know the exact table schema and want maximum speed for the live demo.

#### 6a. Wait for data

First, verify that some data has landed:

```sql
SELECT COUNT(*) AS row_count FROM <DATABASE>.<SCHEMA>.<TABLE_NAME>;
```

If `row_count` is 0, wait 10 seconds and retry (up to 3 times). If still 0, warn the user and troubleshoot.

#### 6b. Create the semantic view

Run this **single SQL call**:

```sql
CREATE OR REPLACE SEMANTIC VIEW <DATABASE>.<SCHEMA>.SSV2_STREAMING_ANALYTICS

  TABLES (
    users AS <DATABASE>.<SCHEMA>.<TABLE_NAME>
      PRIMARY KEY (user_id)
      WITH SYNONYMS = ('registrations', 'customers', 'orders')
      COMMENT = 'Real-time user registrations and orders streaming in via Snowpipe Streaming V2'
  )

  FACTS (
    users.order_amount AS order_amount
      COMMENT = 'Dollar amount of the order placed at registration',
    users.user_id_value AS user_id
      COMMENT = 'Unique identifier for each registered user'
  )

  DIMENSIONS (
    users.first_name AS first_name
      COMMENT = 'First name of the registered user',
    users.last_name AS last_name
      COMMENT = 'Last name of the registered user',
    users.email AS email
      COMMENT = 'Email address of the registered user',
    users.phone_number AS phone_number
      COMMENT = 'Phone number of the registered user',
    users.city AS city
      WITH SYNONYMS = ('town')
      COMMENT = 'City where the user is located',
    users.state AS state
      WITH SYNONYMS = ('province', 'region')
      COMMENT = 'State or province where the user is located',
    users.country AS country
      WITH SYNONYMS = ('nation')
      COMMENT = 'Country where the user is located',
    users.registration_date AS registration_date
      WITH SYNONYMS = ('signup date', 'sign up date', 'registered at')
      COMMENT = 'Timestamp when the user registered',
    users.registration_day AS DATE_TRUNC('day', registration_date)
      WITH SYNONYMS = ('signup day')
      COMMENT = 'Day the user registered (date only)',
    users.registration_hour AS DATE_TRUNC('hour', registration_date)
      COMMENT = 'Hour the user registered',
    users.date_of_birth AS date_of_birth
      WITH SYNONYMS = ('birthday', 'dob', 'birth date')
      COMMENT = 'Date of birth of the user',
    users.age AS DATEDIFF('year', date_of_birth, CURRENT_DATE())
      WITH SYNONYMS = ('years old')
      COMMENT = 'Current age of the user in years',
    users.age_group AS
      CASE
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 25 THEN '18-24'
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 35 THEN '25-34'
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 45 THEN '35-44'
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 55 THEN '45-54'
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 65 THEN '55-64'
        ELSE '65+'
      END
      WITH SYNONYMS = ('age bracket', 'age range', 'age decade', 'demographic')
      COMMENT = 'Age group bucket for the user'
  )

  METRICS (
    users.total_users AS COUNT(users.user_id_value)
      WITH SYNONYMS = ('user count', 'number of users', 'registrations', 'how many users')
      COMMENT = 'Total number of registered users',
    users.total_revenue AS SUM(users.order_amount)
      WITH SYNONYMS = ('total sales', 'total orders', 'revenue', 'sales', 'income')
      COMMENT = 'Total revenue from all orders',
    users.avg_order_value AS AVG(users.order_amount)
      WITH SYNONYMS = ('average order', 'average spend', 'avg order size', 'mean order')
      COMMENT = 'Average order amount per user',
    users.max_order AS MAX(users.order_amount)
      WITH SYNONYMS = ('largest order', 'biggest order', 'highest order')
      COMMENT = 'Largest single order amount',
    users.min_order AS MIN(users.order_amount)
      WITH SYNONYMS = ('smallest order', 'lowest order')
      COMMENT = 'Smallest single order amount',
    users.unique_countries AS COUNT(DISTINCT users.country)
      WITH SYNONYMS = ('number of countries', 'country count', 'how many countries')
      COMMENT = 'Number of distinct countries represented',
    users.unique_cities AS COUNT(DISTINCT users.city)
      WITH SYNONYMS = ('number of cities', 'city count')
      COMMENT = 'Number of distinct cities represented',
    users.unique_states AS COUNT(DISTINCT users.state)
      WITH SYNONYMS = ('number of states', 'state count')
      COMMENT = 'Number of distinct states represented'
  )

  COMMENT = 'Semantic view for real-time streaming user registration and order data ingested via Snowpipe Streaming V2. Data arrives continuously -- counts and totals grow over time.'
  AI_SQL_GENERATION 'This data represents real-time user registrations with associated orders streaming via Snowpipe Streaming V2. Each row is one user with one order. The data is continuously growing as new rows arrive every few seconds. When asked about trends over time, use the registration_date dimension. When asked about demographics, use the age_group dimension. When asked about geography, use country, state, or city dimensions. For revenue questions, use the total_revenue metric (SUM of order_amount). For user count questions, use the total_users metric (COUNT of user_id). For average order questions, use the avg_order_value metric. The order_amount column stores dollar amounts as NUMBER(10,2). Always use metric and dimension names defined in this semantic view rather than raw column names.';
```

#### 6c. Verify the semantic view

```sql
SHOW SEMANTIC VIEWS IN SCHEMA <DATABASE>.<SCHEMA>;
```

Tell the user:

> "Semantic view `SSV2_STREAMING_ANALYTICS` created. Cortex Analyst can now answer natural-language questions about the streaming data."

---

### Step 7 -- Create Cortex Agent (direct SQL)

**Purpose:** Create a Cortex Agent that uses the semantic view to answer natural-language questions. We use direct `CREATE AGENT` SQL for speed.

Run this **single SQL call**:

```sql
CREATE OR REPLACE AGENT <DATABASE>.<SCHEMA>.SSV2_STREAMING_AGENT
  COMMENT = 'AI agent for exploring real-time streaming user and order data via Snowpipe Streaming V2'
  FROM SPECIFICATION $$
models:
  orchestration: auto
instructions:
  system: >
    You are a streaming data analyst. You help explore real-time user registration
    and e-commerce order data that is being ingested via Snowpipe Streaming V2.
    The data includes user demographics (name, email, city, state, country, date of birth),
    registration timestamps, and order amounts. New data arrives every few seconds,
    so counts and totals are continuously growing.
  response: >
    Answer questions about the streaming user and order data concisely.
    Include relevant numbers and trends. When showing geographic data,
    mention both the top entries and the total count. Format currency with
    dollar signs and two decimal places. Data is streaming in real-time,
    so counts and totals are continuously growing.
tools:
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: StreamingAnalytics
      description: >
        Query real-time streaming user registration and order data.
        Use this tool for questions about users, revenue, orders,
        geographic distribution, demographics, and trends over time.
tool_resources:
  StreamingAnalytics:
    semantic_view: "<DATABASE>.<SCHEMA>.SSV2_STREAMING_ANALYTICS"
$$;
```

**IMPORTANT:** Replace `<DATABASE>`, `<SCHEMA>` in the `semantic_view` value inside the YAML spec with the actual database and schema names (fully qualified).

#### 7b. Verify the agent

```sql
SHOW AGENTS IN SCHEMA <DATABASE>.<SCHEMA>;
```

Tell the user:

> "Cortex Agent `SSV2_STREAMING_AGENT` created and pointed at the semantic view. You can interact with it in Snowsight or ask questions here."

---

### Step 8 -- Run showcase queries on the semantic view

**Purpose:** Prove everything works by running 3-4 live queries against the semantic view. The presenter sees real results from the streaming data right in the terminal -- instant "wow" moment.

#### 8a. Total users and revenue

```sql
SELECT * FROM SEMANTIC_VIEW(
    <DATABASE>.<SCHEMA>.SSV2_STREAMING_ANALYTICS
    METRICS users.total_users, users.total_revenue, users.avg_order_value
);
```

Tell the user the results. Note that these numbers are growing in real-time.

#### 8b. Revenue by country (top 5)

```sql
SELECT * FROM SEMANTIC_VIEW(
    <DATABASE>.<SCHEMA>.SSV2_STREAMING_ANALYTICS
    DIMENSIONS users.country
    METRICS users.total_revenue, users.total_users
)
ORDER BY total_revenue DESC
LIMIT 5;
```

#### 8c. Breakdown by age group

```sql
SELECT * FROM SEMANTIC_VIEW(
    <DATABASE>.<SCHEMA>.SSV2_STREAMING_ANALYTICS
    DIMENSIONS users.age_group
    METRICS users.total_users, users.avg_order_value
)
ORDER BY avg_order_value DESC;
```

#### 8d. Registration trend over time

```sql
SELECT * FROM SEMANTIC_VIEW(
    <DATABASE>.<SCHEMA>.SSV2_STREAMING_ANALYTICS
    DIMENSIONS users.registration_hour
    METRICS users.total_users, users.total_revenue
)
ORDER BY registration_hour;
```

After running the queries, tell the user:

> "All 4 queries returned live results from streaming data. The semantic view and agent are working.
>
> **Try re-running any of these in a minute -- the numbers will have changed because data is still streaming in.**
>
> Ready for the live demo portion?"

---

### Step 9 -- Presenter handoff

**Purpose:** Everything is set up. Hand control to the presenter for the live demo.

**Present this to the user:**

> **Setup Complete! Your SSv2 AI Webinar Demo is Ready.**
>
> You now have:
> - **Streaming pipeline** -- Data flowing into `<DATABASE>.<SCHEMA>.<TABLE_NAME>` (~10 rows/second)
> - **Live dashboard** -- Streamlit app `SSV2_STREAMING_MONITOR` showing real-time metrics
> - **Semantic view** -- `SSV2_STREAMING_ANALYTICS` for natural-language querying
> - **Cortex Agent** -- `SSV2_STREAMING_AGENT` ready for conversational analytics
>
> **Demo flow for your presentation:**
>
> 1. **Show the Streamlit dashboard** -- Point out rows arriving in real-time, cumulative revenue growing, countries populating
>
> 2. **Open Cortex Agent in Snowsight:**
>    - Navigate to **AI & ML > Cortex Agents** (or **Snowflake Intelligence > Agents**)
>    - Find and open **SSV2_STREAMING_AGENT**
>    - Try questions like "How many users have registered?" or "What is revenue by country?"
>
> 3. **Key demo moments:**
>    - Ask "How many users have registered so far?" -- note the number
>    - Show a geographic breakdown: "What's the total revenue by country?"
>    - Wait 30-60 seconds, re-ask "How many users now?" -- the number has grown! (live data)
>    - Try an ad-lib question: "What's the average order value for users in California?"
>
> 4. **Toggle between dashboard and agent** to show the full picture: real-time monitoring + AI-powered analysis on the same live data
>
> **The streaming script runs for ~30 minutes.** You have plenty of time for the demo.
>
> When you're done, say "clean up" and I'll tear everything down.

---

### Step 10 -- Cleanup

**Purpose:** Remove all demo assets from Snowflake.

#### 10a. Stop background streaming

First, check if the streaming script is still running. If it is, kill the background process.

#### 10b. Ask about cleanup

> "The demo is complete! How would you like to handle cleanup?
>
> 1. **Clean up Snowflake objects, keep local files** *(default)* -- Drops agent, semantic view, demo user, role, Streamlit app, stage, database/schema. Keeps local files for review.
> 2. **Clean up everything** -- Drops Snowflake objects AND deletes local files.
> 3. **Keep everything** -- Leave all objects and files in place."

#### 10c. Execute cleanup

**If option 1 (default) or option 2**, run the Snowflake cleanup SQL:

```sql
-- AI layer
DROP AGENT IF EXISTS <DATABASE>.<SCHEMA>.SSV2_STREAMING_AGENT;
DROP SEMANTIC VIEW IF EXISTS <DATABASE>.<SCHEMA>.SSV2_STREAMING_ANALYTICS;
-- Streamlit app and stage
DROP STREAMLIT IF EXISTS <DATABASE>.<SCHEMA>.SSV2_STREAMING_MONITOR;
DROP STAGE IF EXISTS <DATABASE>.<SCHEMA>.SSV2_STREAMLIT_STAGE;
-- Schema cascade drops table and auto-created pipe
DROP SCHEMA IF EXISTS <DATABASE>.<SCHEMA>;
-- Demo user and role
DROP USER IF EXISTS SSV2_DEMO_USER;
DROP ROLE IF EXISTS SSV2_DEMO_ROLE;
-- Database (only if created for this demo)
DROP DATABASE IF EXISTS <DATABASE>;
```

**If option 2**, also run local cleanup:

```bash
deactivate 2>/dev/null; rm -rf ssv2_venv; rm -f rsa_key.p8 rsa_key.pub profile.json ssv2_demo.py streamlit_app.py ssv2_demo_sql.log
```

**After cleanup**, summarize what was removed.

---

## Best practices

- **Never persist private keys in chat or logs.** The `rsa_key.p8` file stays local and is cleaned up after.
- **Use `private_key_file` in profile.json** (path to `.p8` file), not inline key content.
- **Default pipe naming uses a hyphen.** Convention: `<TABLE_NAME>-streaming`.
- **Background streaming is key to the demo flow.** Never wait for the streaming script to complete before moving on.
- **Rich synonyms on the semantic view** make ad-lib NL queries work on the first try during the live demo.
- **Showcase queries in Step 8** demonstrate the semantic view capabilities before handing off to the presenter.

## Examples

**User**: "ssv2 ai webinar"
-> Run full flow Steps 0-9. Set up streaming, dashboard, semantic view, agent, and run showcase queries.

**User**: "clean up" (after demo)
-> Run Step 10 cleanup.

**User**: "I just want the quickstart without the AI stuff"
-> Redirect to the `ssv2-quickstart` skill instead.

## Templates

### Table DDL

```sql
CREATE OR REPLACE TABLE {{DATABASE}}.{{SCHEMA}}.{{TABLE_NAME}} (
    user_id              INTEGER,
    first_name           VARCHAR(100),
    last_name            VARCHAR(100),
    email                VARCHAR(255),
    phone_number         VARCHAR(50),
    address              VARCHAR(500),
    date_of_birth        DATE,
    registration_date    TIMESTAMP_NTZ,
    city                 VARCHAR(100),
    state                VARCHAR(100),
    country              VARCHAR(100),
    order_amount         NUMBER(10,2)
);
```

### Default pipe reference

No `CREATE PIPE` needed. The SDK references:
```
{{TABLE_NAME}}-streaming
```