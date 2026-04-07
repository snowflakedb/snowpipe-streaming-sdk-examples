---
name: ssv2-quickstart
description: "Automated quick-start for Snowpipe Streaming V2 (high-performance architecture). Detects your OS (macOS/Linux/Windows), verifies Python, sets up a virtual environment, creates a landing table, configures RSA key-pair auth, streams fake user data via the default auto-created pipe, and deploys a real-time Streamlit in Snowflake dashboard so you can watch rows arrive live. Triggers: ssv2 quickstart, snowpipe streaming quickstart, snowpipe streaming demo, demo snowpipe streaming, try snowpipe streaming."
---

<!-- Copyright (c) 2026 Snowflake Inc. Licensed under Apache 2.0. See LICENSE. -->

## When to use

Use this skill when the user wants to:

- Try out or demo Snowpipe Streaming V2 (high-performance architecture) with minimal setup
- Set up an end-to-end streaming ingestion pipeline quickly
- Generate fake/sample data and stream it into a Snowflake table
- Learn how Snowpipe Streaming V2 works with the Python SDK

## What this skill provides

A fully automated, zero-to-streaming pipeline:

1. **Platform detection & context** — detects macOS / Linux / Windows, verifies Python 3.9+, gathers Snowflake context (all in parallel); asks user for preferences
2. **RSA key-pair generation** — generates a fresh 2048-bit RSA key-pair (`rsa_key.p8` / `rsa_key.pub`) and extracts the public key body (single Bash call)
3. **Snowflake object setup + demo user** — creates database, schema, landing table, a dedicated demo user with RSA key, and grants (single SQL call)
4. **Config files, demo script, and Python venv** — writes `profile.json` and `ssv2_demo.py` in parallel, then creates an isolated venv with `snowpipe-streaming` and `faker`
5. **Real-time Streamlit dashboard** — deploys a Streamlit in Snowflake app with stage creation, file upload, and app creation (minimal SQL calls)
6. **Streaming demo execution** — runs the Python demo script that generates fake data via the **default auto-created pipe** (no explicit `CREATE PIPE` needed)
7. **Results summary** — queries the landing table to confirm rows arrived and presents metrics
8. **Cleanup (optional)** — removes all Snowflake assets including demo user/role and local files (one SQL + one Bash call in parallel)

## Critical concepts

### Default pipe naming convention

The high-performance architecture automatically creates a default pipe when data is first ingested into a table. **No `CREATE PIPE` SQL is required.**

The Python SDK references the default pipe using this naming convention:

```
<TABLE_NAME>-streaming
```

**Important:** Use a **hyphen** (`-`), not an underscore (`_`). Examples:
- Table `SSV2_QUICKSTART_USERS` → Pipe name: `SSV2_QUICKSTART_USERS-streaming`
- Table `MY_EVENTS` → Pipe name: `MY_EVENTS-streaming`

### High-performance vs Classic architecture

The high-performance architecture (V2) is the recommended path. Unlike classic, it uses a PIPE object for data ingestion. The default pipe is auto-created at ingest time — no explicit `CREATE PIPE` is needed for straightforward use cases.

### Authentication

The Python SDK uses **key-pair (JWT) authentication**. The `profile.json` file references a `private_key_file` path (not inline key content). OAuth is only available in SDK 2.0.3+.

### Supported platforms

- **macOS** (ARM64) — fully supported
- **Linux** (x86_64, ARM64) — fully supported (requires glibc >= 2.26)
- **Windows** (x86_64) — **experimental**. The skill's shell commands target Unix. Windows users should use WSL2 or Git Bash. If Windows is detected, warn the user and offer to continue at their own risk.

## Instructions

**IMPORTANT EXECUTION GUIDELINES:**

1. **Announce each step clearly** — Before executing each step, print a clear header like "**Step X — [Step Name]**" so the user knows exactly where they are in the process.

2. **Batch commands aggressively to minimize permission prompts** — The user should see as few permission dialogs as possible. Specific batching rules:
   - **Step 1:** Run platform checks (Bash) and Snowflake context query (SQL) as **two parallel tool calls** — one Bash, one SQL
   - **Step 2:** Run both `openssl genrsa...` and the pubkey extraction in **one single Bash call**
   - **Step 3:** Run CREATE DATABASE, CREATE SCHEMA, CREATE TABLE, CREATE USER, ALTER USER SET RSA_PUBLIC_KEY, GRANTs, DESC USER in **one single SQL call**
   - **Steps 4a:** Write `profile.json` and `ssv2_demo.py` as **two parallel FileWrite calls**
   - **Step 5 Streamlit deployment:** Run CREATE STAGE in SQL. Then write `streamlit_app.py` locally and upload it to the stage in **one Bash call**. Then run CREATE STREAMLIT + SHOW STREAMLITS in **one SQL call**.
   - **Step 8 cleanup:** Run all DROP statements in **one single SQL call**. Local files are preserved by default.

3. **Always ask about cleanup at the end** — After the demo completes and results are summarized, ask the user how they want to handle cleanup. Default is to clean up Snowflake objects and keep local files.

4. **Use parallel tool calls** — When operations are independent (e.g., writing local files while running SQL), execute them in parallel.

5. **Log all SQL to `ssv2_demo_sql.log`** — Every time you execute SQL via `SnowflakeSqlExecute`, **immediately append** the SQL to a local file called `ssv2_demo_sql.log` using a Bash call (can run in parallel with the next step). Format each entry with a step header and timestamp:
   ```
   -- ============================================================
   -- Step N — <Step Title>
   -- Executed: <YYYY-MM-DD HH:MM:SS>
   -- ============================================================
   <THE SQL>

   ```
   This gives the user a complete record of every SQL statement run during the demo. The file is **not** deleted during cleanup so the user can review it afterward.

Follow each step in order. Use `SnowflakeSqlExecute` for all SQL operations and `Bash` for all shell operations.

---

### Step 0 — Confirm intent

**Before doing anything**, confirm the user wants to run the full quickstart:

> "This will run the **SSv2 Quickstart** — a fully automated demo that:
> - Creates a demo database, schema, table, user, and role in Snowflake
> - Generates RSA keys and a Python virtual environment locally
> - Streams fake user data into Snowflake via the Snowpipe Streaming SDK
> - Deploys a live Streamlit dashboard to monitor data arriving in real-time
> - Cleans up all Snowflake objects at the end (local files are preserved)
>
> The whole demo takes ~5 minutes. Want to proceed?"

If the user says no, or if they were just asking a question about SSv2 (e.g., "what is ssv2?", "how does snowpipe streaming work?"), answer their question directly without running the quickstart.

---

### Step 1 — Detect platform, check Python, and gather Snowflake context

**Purpose:** Verify system requirements and understand the current Snowflake context.

**Execute these two tool calls in parallel:**

**Tool call 1 — Bash** (platform checks + initialize SQL log):
```bash
echo "=== OS ==="; uname -s 2>/dev/null || echo "WINDOWS"; echo "=== Python ==="; python3 --version 2>/dev/null || python --version 2>/dev/null || echo "NOT FOUND"; echo "=== Working Directory ==="; pwd; echo "=== Home Directory ==="; echo $HOME; echo "-- SSV2 Quickstart SQL Log" > ssv2_demo_sql.log; echo "-- Generated by Cortex Code SSV2 Quickstart Skill" >> ssv2_demo_sql.log; echo "" >> ssv2_demo_sql.log
```

**Tool call 2 — SQL** (Snowflake context):
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
- `Darwin` → **macOS**
- `Linux` → **Linux** (also need glibc >= 2.26 check: `ldd --version 2>&1 | head -1`)
- `WINDOWS` or `MINGW*` or `MSYS*` or `CYGWIN*` → **Windows** (experimental — see below)

**Error handling — stop if:**
- Python not found or below 3.9 → tell user to install Python 3.9+
- Linux glibc below 2.26 → tell user to upgrade
- Working directory is `$HOME`, `/`, or drive root → tell user to `mkdir -p ~/ssv2-quickstart && cd ~/ssv2-quickstart`
- `CURRENT_WAREHOUSE()` is NULL → tell user to `USE WAREHOUSE <name>;`
- **Windows detected** → warn the user:
  > "Windows support for this skill is **experimental**. The shell commands are written for macOS/Linux. You can continue if you're running under **WSL2** or **Git Bash**, but native CMD/PowerShell may hit issues. Would you like to proceed?"
  If the user declines, stop gracefully.

Store the working Python command (`python3` or `python`) and all Snowflake context values.

#### 1b. Ask the user for preferences

Ask the user:
- Which **database** and **schema** to use (default: `SSV2_QUICKSTART_DB.SSV2_SCHEMA`, or use existing)
- A **table name** for the landing table (default: `SSV2_QUICKSTART_USERS`)
- How many **minutes** to run the streaming demo (default: `3`, minimum: `1`, maximum: `10`)

**Note on demo user:** This skill creates a dedicated demo user (`SSV2_DEMO_USER`) for streaming authentication. This avoids overwriting any existing RSA key-pair on the current user. The demo user is always cleaned up at the end.

---

### Step 2 — Generate RSA key-pair

**Purpose:** The Snowpipe Streaming SDK uses key-pair (JWT) authentication. Generate an RSA key-pair and extract the public key body.

Run key generation and extraction in **one single Bash call**:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt && chmod 600 rsa_key.p8 && openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub && echo "=== PUBLIC KEY BODY ===" && cat ./rsa_key.pub | grep -v KEY- | tr -d '\012' && echo
```

**Error handling — OpenSSL not found:**

If `openssl` command fails:

> "OpenSSL is required to generate RSA keys but was not found on your system.
>
> Install OpenSSL:
> - macOS: `brew install openssl`
> - Linux: `sudo apt-get install openssl` or `sudo yum install openssl`
> - Windows: Install via `winget install OpenSSL` or download from the official OpenSSL wiki"

Capture the public key body output (the base64 string after `=== PUBLIC KEY BODY ===`) for use in the next step.

---

### Step 3 — Create Snowflake objects, demo user, and register RSA key

**Purpose:** Create the database, schema, landing table, a dedicated demo user with the RSA public key, and the necessary grants — all in one SQL call to minimize prompts.

**Why a demo user?** The Snowpipe Streaming SDK requires RSA key-pair auth. Rather than overwriting any existing RSA key on the current user (which could break their existing workflows), we create a short-lived demo user `SSV2_DEMO_USER` with a dedicated role. This user is always cleaned up at the end.

Run **all of these in one single SQL call** (multi-statement):

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

**Why GRANT OWNERSHIP on the table?** The default auto-created pipe is Snowflake-managed and tied to the table. The role that streams data must own the table to ensure full access to the default pipe (which is auto-created on first ingest). The DB and schema remain owned by the primary role so the user can still see and query the table under their own role. After transferring ownership, we grant SELECT back to the primary role so the Streamlit dashboard (which runs under the primary role) can read the table.

Look for `RSA_PUBLIC_KEY` in the DESC USER output to confirm it was set.

**No `CREATE PIPE` is needed.** The high-performance architecture will auto-create a default pipe the first time data is streamed into this table. The Python SDK references it as: `<TABLE_NAME>-streaming`

**Error handling:**
- Privilege errors on CREATE DATABASE/SCHEMA → suggest using existing DB/schema or switching to SYSADMIN
- CREATE USER fails → requires ACCOUNTADMIN or USERADMIN. If the user's role cannot create users, inform them and offer to fall back to their current user (with a warning that their existing RSA key will be overwritten)
- GRANT fails → check role has MANAGE GRANTS or is SECURITYADMIN/ACCOUNTADMIN

---

### Step 4 — Write profile.json and demo script, create Python venv

**Purpose:** Write the SDK configuration file and demo script, then set up the Python environment. These are independent operations that should be parallelized.

#### 4a. Write profile.json and ssv2_demo.py in parallel

**Execute these two FileWrite calls in parallel:**

**FileWrite 1 — profile.json:**

```json
{
    "user": "SSV2_DEMO_USER",
    "account": "<ACCOUNT_IDENTIFIER>",
    "url": "https://<ACCOUNT_IDENTIFIER>.snowflakecomputing.com:443",
    "private_key_file": "rsa_key.p8",
    "role": "SSV2_DEMO_ROLE"
}
```

Where:
- `user` — always `SSV2_DEMO_USER` (the dedicated demo user created in Step 3)
- `<ACCOUNT_IDENTIFIER>` — derived from `CURRENT_ACCOUNT()` (e.g., `xy12345` or `org-account`)
- `role` — always `SSV2_DEMO_ROLE` (the dedicated demo role created in Step 3)

**Error handling — account URL format:**

If the account uses PrivateLink or org-based URLs, the URL format may differ:
- Standard: `https://<account>.snowflakecomputing.com:443`
- Org-based: `https://<org>-<account>.snowflakecomputing.com:443`
- PrivateLink: `https://<account>.privatelink.snowflakecomputing.com:443`

Ask the user if they're using a non-standard URL format.

**FileWrite 2 — ssv2_demo.py:** (see Step 4b below for full content)

#### 4b. Demo script content (ssv2_demo.py)

Write `ssv2_demo.py` with the following content (interpolate user's chosen values):

Write `ssv2_demo.py` using the template in the **"Demo script reference"** section below (between Steps 4 and 5). Interpolate the user's chosen DATABASE, SCHEMA, TABLE_NAME, and DEMO_MINUTES values into the placeholders.

#### 4c. Create Python virtual environment and install dependencies

**Purpose:** Create an isolated Python environment to install the Snowpipe Streaming SDK and Faker.

#### macOS / Linux

Run the full setup in one command:

```bash
<PYTHON_CMD> -m venv ssv2_venv && source ssv2_venv/bin/activate && pip install --upgrade pip && pip install snowpipe-streaming faker && python -c "from snowflake.ingest.streaming import StreamingIngestClient; print('SDK OK')" && python -c "from faker import Faker; print('Faker OK')"
```

#### Windows (experimental — WSL2 / Git Bash recommended)

If the user is on Windows and chose to proceed, use the macOS/Linux command above inside WSL2 or Git Bash. For native CMD as a fallback:

```cmd
<PYTHON_CMD> -m venv ssv2_venv && ssv2_venv\Scripts\activate.bat && pip install --upgrade pip && pip install snowpipe-streaming faker && python -c "from snowflake.ingest.streaming import StreamingIngestClient; print('SDK OK')" && python -c "from faker import Faker; print('Faker OK')"
```

**Error handling — import fails:**

If either import fails:

> "Package installation failed. Common fixes:
> 1. Ensure the virtual environment is activated: `source ssv2_venv/bin/activate`
> 2. Retry installation: `pip install --force-reinstall snowpipe-streaming faker`
> 3. Check Python version: `python --version` (must be 3.9+)
> 4. On Linux, verify glibc >= 2.26: `ldd --version`"

---

### Demo script reference (ssv2_demo.py)

**Note:** This script is written to disk in Step 4a (parallel with profile.json). The content below is the full template — interpolate `<DATABASE>`, `<SCHEMA>`, `<TABLE_NAME>`, and `<DEMO_MINUTES>` with the user's chosen values.

**Security note:** The private key (`rsa_key.p8`) is unencrypted for demo simplicity. In production, use an encrypted key with a passphrase or a secrets manager.

This script runs **locally on your computer** and uses the Snowpipe Streaming SDK to stream data directly into Snowflake. It generates fake user data with order amounts and sends batches of 5 rows every 0.5 seconds.

**Architecture:** Your local Python script → Snowpipe Streaming SDK → Snowflake (cloud)

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
DEMO_MINUTES = <DEMO_MINUTES>  # User-specified duration (1-10 minutes)
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
        "SSV2_QUICKSTART_CLIENT",
        DATABASE,
        SCHEMA,
        PIPE,
        profile_json=PROFILE_JSON_PATH,
        properties=None,
    )
except Exception as e:
    print(f"\n[ERROR] Failed to create StreamingIngestClient:")
    print(f"  {e}")
    print(f"\nTroubleshooting:")
    print(f"  1. Verify profile.json exists and has correct values")
    print(f"  2. Check that rsa_key.p8 exists in the same directory")
    print(f"  3. Verify the public key is registered: DESC USER <your_user>;")
    print(f"  4. Ensure network allows outbound HTTPS to Snowflake")
    raise SystemExit(1)

# --- Open channel ---
print(f"\nOpening channel...")
try:
    channel, status = client.open_channel("SSV2_QUICKSTART_CHANNEL")
    print(f"  Channel: {status.channel_name}")
    print(f"  Status:  {status.status_code}")
    print(f"  Latest committed offset: {status.latest_committed_offset_token}")
except Exception as e:
    print(f"\n[ERROR] Failed to open channel:")
    print(f"  {e}")
    print(f"\nTroubleshooting:")
    print(f"  1. Verify the table exists: SELECT * FROM {DATABASE}.{SCHEMA}.<TABLE> LIMIT 1;")
    print(f"  2. Check role has INSERT privilege on the table")
    print(f"  3. Verify database and schema names are correct")
    client.close()
    raise SystemExit(1)

# --- Stream fake user data in batches ---
total_rows = BATCH_SIZE * NUM_BATCHES
print(f"\nStreaming {total_rows} rows ({NUM_BATCHES} batches of {BATCH_SIZE}) over ~{DEMO_MINUTES} minute(s)...")
print(f"Watch your Streamlit dashboard to see data arrive in real-time!\n")
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
    
    if batch % 60 == 0:  # Progress update every 30 seconds
        print(f"  [producer] batch {batch}/{NUM_BATCHES} (row {row_id})")
    
    time.sleep(0.5)  # 0.5 second delay between batches

if errors:
    print(f"\n[WARNING] {len(errors)} rows failed to send:")
    for offset, err in errors[:5]:
        print(f"  Row {offset}: {err}")

# --- Wait for all data to be committed ---
print(f"\n[producer] Waiting for commits to reach offset {total_rows}...")
try:
    channel.wait_for_commit(
        lambda token: token is not None and int(token) >= total_rows,
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
print("\nDemo complete!")
```

---

### Step 5 — Deploy a real-time Streamlit dashboard in Snowflake

**Purpose:** Deploy a live monitoring dashboard that runs **in the Snowflake cloud**. While your local Python script streams data, this cloud-hosted dashboard auto-refreshes every 2 seconds so you can watch data arrive in real-time from anywhere. No local Streamlit install needed.

**Architecture:** Local streaming script → Snowflake table ← Cloud dashboard (Streamlit in Snowflake)

#### 5a. Create stage and write Streamlit app locally

**Execute these in parallel:**

**Tool call 1 — SQL** (create stage):
```sql
CREATE STAGE IF NOT EXISTS <DATABASE>.<SCHEMA>.SSV2_STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);
```

**Tool call 2 — FileWrite** (write `streamlit_app.py` locally):

```python
import streamlit as st
import time

st.set_page_config(page_title="SSV2 Streaming Monitor", layout="wide")

conn = st.connection("snowflake")

DATABASE = "<DATABASE>"
SCHEMA   = "<SCHEMA>"
TABLE    = "<TABLE_NAME>"
REFRESH_INTERVAL = 2

st.title("Snowpipe Streaming V2 — Live Monitor")
st.caption(f"Reading from `{DATABASE}.{SCHEMA}.{TABLE}` · refreshes every {REFRESH_INTERVAL}s")

try:
    metrics_df = conn.query(
        f"""SELECT COUNT(*) AS total_rows, 
                   COALESCE(SUM(order_amount), 0) AS total_revenue
            FROM {DATABASE}.{SCHEMA}.{TABLE}""",
        ttl=0,
    )
    total_rows = metrics_df["TOTAL_ROWS"].iloc[0] if len(metrics_df) > 0 else 0
    total_revenue = metrics_df["TOTAL_REVENUE"].iloc[0] if len(metrics_df) > 0 else 0
except Exception as e:
    st.error(f"Error querying table: {e}")
    total_rows = 0
    total_revenue = 0

if total_rows > 0:
    latest_df = conn.query(
        f"""SELECT MAX(user_id) AS latest_id,
                   COUNT(DISTINCT country) AS unique_countries
            FROM {DATABASE}.{SCHEMA}.{TABLE}""",
        ttl=0,
    )
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Rows", f"{total_rows:,}")
    col2.metric("Revenue Total", f"${total_revenue:,.2f}")
    col3.metric("Latest User ID", latest_df["LATEST_ID"].iloc[0])
    col4.metric("Unique Countries", latest_df["UNIQUE_COUNTRIES"].iloc[0])
else:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Rows", "0")
    col2.metric("Revenue Total", "$0.00")
    col3.metric("Latest User ID", "—")
    col4.metric("Unique Countries", "—")
    st.info("Waiting for data... Start the streaming demo to see rows appear.")

st.subheader("Most Recent Records")
if total_rows > 0:
    recent_df = conn.query(
        f"""SELECT user_id, first_name, last_name, email, country, order_amount
            FROM {DATABASE}.{SCHEMA}.{TABLE}
            ORDER BY user_id DESC
            LIMIT 20""",
        ttl=0,
    )
    st.dataframe(recent_df, use_container_width=True, hide_index=True)
else:
    st.write("No data yet.")

if total_rows > 0:
    st.subheader("Revenue Over Time")
    time_df = conn.query(
        f"""SELECT 
                DATE_TRUNC('second', registration_date) AS time_bucket,
                SUM(SUM(order_amount)) OVER (ORDER BY DATE_TRUNC('second', registration_date)) AS cumulative_revenue
            FROM {DATABASE}.{SCHEMA}.{TABLE}
            GROUP BY time_bucket
            ORDER BY time_bucket""",
        ttl=0,
    )
    st.line_chart(time_df.set_index("TIME_BUCKET"), y="CUMULATIVE_REVENUE", height=300)

    st.subheader("Top 10 Countries by Revenue")
    country_df = conn.query(
        f"""SELECT country, SUM(order_amount) AS revenue
            FROM {DATABASE}.{SCHEMA}.{TABLE}
            GROUP BY country
            ORDER BY revenue DESC
            LIMIT 10""",
        ttl=0,
    )
    st.dataframe(country_df, use_container_width=True, hide_index=True)

time.sleep(REFRESH_INTERVAL)
st.rerun()
```

#### 5b. Upload to stage

After both 5a calls complete, upload the file:

```bash
snow stage copy <LOCAL_PATH>/streamlit_app.py @<DATABASE>.<SCHEMA>.SSV2_STREAMLIT_STAGE --overwrite
```

**Error handling — upload fails:**

If the upload fails:
> "Failed to upload Streamlit app to stage. Common causes:
> 1. Stage doesn't exist — verify with `SHOW STAGES IN SCHEMA <DATABASE>.<SCHEMA>;`
> 2. Insufficient privileges — need WRITE access to the stage
> 3. File path is incorrect — ensure `streamlit_app.py` exists locally"

#### 5c. Create Streamlit app and verify

Run **one single SQL call** with both CREATE STREAMLIT and SHOW STREAMLITS:

```sql
CREATE OR REPLACE STREAMLIT <DATABASE>.<SCHEMA>.SSV2_STREAMING_MONITOR
    ROOT_LOCATION = '@<DATABASE>.<SCHEMA>.SSV2_STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = '<WAREHOUSE>';
SHOW STREAMLITS IN SCHEMA <DATABASE>.<SCHEMA>;
```

**Error handling — Streamlit creation fails:**

If CREATE STREAMLIT fails:

> "Failed to create Streamlit app. Common causes:
> 1. No warehouse specified — `QUERY_WAREHOUSE` is required
> 2. File not found in stage — run `LIST @<STAGE>;` to verify
> 3. Insufficient privileges — need CREATE STREAMLIT privilege on schema
> 4. Streamlit not enabled — contact your Snowflake admin"

#### 5d. Grant access (if needed)

If other users need to view the dashboard:

```sql
GRANT USAGE ON STREAMLIT <DATABASE>.<SCHEMA>.SSV2_STREAMING_MONITOR TO ROLE <ROLE>;
```

#### 5e. Present dashboard to user

**Present this to the user:**

> "Your real-time streaming dashboard is ready!
>
> **To access the dashboard:**
> 1. Open Snowsight in your browser
> 2. Navigate to **Projects > Streamlit** in the left sidebar
> 3. Find and click **SSV2_STREAMING_MONITOR** in the list
>
> *(Note: Direct URL links may not work — use the Projects > Streamlit navigation instead.)*
>
> Keep it open while the demo runs — you'll see rows appear as they're ingested.
> The dashboard auto-refreshes every 2 seconds."

**IMPORTANT:** Wait for the user to confirm the Streamlit dashboard has fully loaded before proceeding to Step 6. The dashboard should display "Waiting for data..." with zero metrics. This ensures they can watch the data arrive in real time.

---

### Step 6 — Run the streaming demo

**Purpose:** This is the exciting part! You'll run a **local Python script on your computer** that streams data directly into Snowflake's cloud. Meanwhile, your **cloud-hosted Streamlit dashboard** displays the data as it arrives — demonstrating real-time ingestion from local to cloud.

**What's happening:**
- **Local (your computer):** Python script generating and streaming fake user data
- **Cloud (Snowflake):** Receiving data via Snowpipe Streaming, storing in table, displaying on live dashboard

Data typically takes 5-10 seconds to start appearing in the dashboard after the script begins streaming.

**After the demo completes:** We'll summarize the results (rows inserted, revenue generated, errors) and then optionally clean up the Snowflake assets created during this quickstart.

Run the demo:

```bash
source ssv2_venv/bin/activate && python ssv2_demo.py
```

**Note:** Data typically takes 5-10 seconds to start appearing in the dashboard after the script begins streaming.

The user should have the Streamlit dashboard open (from Step 5e) to watch data arrive in real time.

**Error handling — common issues:**

| Error | Cause | Fix |
|-------|-------|-----|
| `Connection refused` | Account URL incorrect | Verify `url` in profile.json matches your Snowflake account |
| `Authentication failed` | Public key not registered | Run `DESC USER <USER>;` and check RSA_PUBLIC_KEY is set |
| `Table not found` | Wrong database/schema/table | Verify objects exist and role has access |
| `ModuleNotFoundError` | Venv not activated | Run `source ssv2_venv/bin/activate` |
| `Permission denied` | Role lacks INSERT | Grant INSERT on table to role |

---

### Step 7 — Results Summary

**Purpose:** Summarize the streaming demo results and confirm data landed successfully.

Run the summary query:

```sql
SELECT 
    COUNT(*) AS total_rows,
    SUM(order_amount) AS total_revenue,
    COUNT(DISTINCT country) AS unique_countries,
    MIN(registration_date) AS first_record,
    MAX(registration_date) AS last_record
FROM <DATABASE>.<SCHEMA>.<TABLE_NAME>;
```

**Present the results to the user:**

> **Demo Complete!**
>
> | Metric | Value |
> |--------|-------|
> | **Rows Streamed** | X,XXX |
> | **Total Revenue** | $XX,XXX.XX |
> | **Unique Countries** | XXX |
> | **Errors** | X |
> | **Avg Server Latency** | X.XX seconds |
>
> **What happened:**
> - **Local:** Python script on your computer generated X,XXX fake user records with order amounts
> - **Cloud:** Snowflake received data via Snowpipe Streaming and displayed it live on the dashboard

**Error handling — row count is zero:**

If no rows appear:

> "No data found in the table. Troubleshooting:
> 1. Check the Python script output for errors
> 2. Verify the channel committed successfully (look for 'All rows committed')
> 3. Wait 30-60 seconds — there may be ingestion latency
> 4. Check `SHOW PIPES;` to see if the default pipe was created
> 5. Query `SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY` for pipe activity"

---

### Step 8 — Cleanup (optional)

**Purpose:** Remove demo assets from Snowflake to avoid clutter and ongoing costs.

#### 8a. Check if the streaming demo is still running

Before cleanup, verify the Python demo script has completed. If the script is still running (you'll see ongoing output in the terminal), ask the user:

> "The streaming demo appears to still be running. Would you like me to stop it before cleanup?"

If yes, terminate the background process or ask the user to press `Ctrl+C` in the terminal running the script.

#### 8b. Ask the user about cleanup

**Ask the user** (default: clean up Snowflake objects, keep local files):

> "The demo is complete! How would you like to handle cleanup?
>
> 1. **Clean up Snowflake objects, keep local files** *(default)* — Drops demo user, role, table, Streamlit app, stage, database/schema from Snowflake. Keeps all local files so you can review the code and SQL log.
> 2. **Clean up everything** — Drops Snowflake objects AND deletes local files.
> 3. **Keep everything** — Leave all Snowflake objects and local files in place."

#### 8c. Execute cleanup

**If option 1 (default) or option 2**, run the Snowflake cleanup SQL:

```sql
-- Streamlit app and stage first (they live in the schema)
DROP STREAMLIT IF EXISTS <DATABASE>.<SCHEMA>.SSV2_STREAMING_MONITOR;
DROP STAGE IF EXISTS <DATABASE>.<SCHEMA>.SSV2_STREAMLIT_STAGE;
-- Drop schema with CASCADE — this drops the table (owned by demo role) and its auto-created pipe
DROP SCHEMA IF EXISTS <DATABASE>.<SCHEMA>;
-- Demo user and role
DROP USER IF EXISTS SSV2_DEMO_USER;
DROP ROLE IF EXISTS SSV2_DEMO_ROLE;
-- Database (only if created for this demo)
DROP DATABASE IF EXISTS <DATABASE>;
```

**If option 2**, also run local cleanup in parallel:

```bash
deactivate 2>/dev/null; rm -rf ssv2_venv; rm -f rsa_key.p8 rsa_key.pub profile.json ssv2_demo.py streamlit_app.py ssv2_demo_sql.log
```

**After cleanup**, remind the user what was done:

- **Option 1:** "Snowflake objects cleaned up. Local files preserved in your working directory: `ssv2_demo_sql.log`, `ssv2_demo.py`, `streamlit_app.py`, `profile.json`, `rsa_key.p8`, `rsa_key.pub`, `ssv2_venv/`. To delete locally: `rm -rf ssv2_venv rsa_key.p8 rsa_key.pub profile.json ssv2_demo.py streamlit_app.py ssv2_demo_sql.log`"
- **Option 2:** "All Snowflake objects and local files cleaned up."
- **Option 3:** "Everything left in place. Snowflake objects: `<DATABASE>.<SCHEMA>.<TABLE_NAME>`, demo user `SSV2_DEMO_USER`, Streamlit app `SSV2_STREAMING_MONITOR`. Local files in your working directory."

---

## Best practices

- **Never persist private keys in chat or logs.** The private key file `rsa_key.p8` should stay local and be cleaned up after the demo.
- **Use `private_key_file` in profile.json** (path to the `.p8` file), not inline key content.
- **Use the user's current context** (database, schema, role, warehouse) unless they explicitly override it.
- **Confirm before replacing objects.** Always ask before running `CREATE OR REPLACE`.
- **Handle errors gracefully.** If a step fails, explain what went wrong and offer to retry.
- **Respect row counts.** For large row counts (>10,000), warn that it may take longer.
- **Default pipe naming uses a hyphen.** The convention is `<TABLE_NAME>-streaming`.
- **VARIANT columns must receive native objects.** Pass Python dicts, not JSON strings.

---

## Examples

**User**: "I want to try Snowpipe Streaming V2"
→ Run full flow Steps 1-8. Deploy Streamlit dashboard before streaming demo.

**User**: "Set up streaming into my existing table EVENTS"
→ Skip DB/schema creation in Step 3. Adjust column mappings. Use default pipe `EVENTS-streaming`.

**User**: "Just generate the profile.json, I already have the table"
→ Execute Steps 1, 2, and the profile.json portion of Step 4 only.

**User**: "I'm on Windows"
→ Warn that Windows support is experimental. Recommend WSL2 or Git Bash. If they proceed, use the CMD fallback commands.

---

## Templates

### profile.json

```json
{
    "user": "SSV2_DEMO_USER",
    "account": "{{ACCOUNT_IDENTIFIER}}",
    "url": "https://{{ACCOUNT_IDENTIFIER}}.snowflakecomputing.com:443",
    "private_key_file": "rsa_key.p8",
    "role": "SSV2_DEMO_ROLE"
}
```

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

### Streamlit deployment

```sql
CREATE STAGE IF NOT EXISTS {{DATABASE}}.{{SCHEMA}}.SSV2_STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- After uploading streamlit_app.py:
CREATE OR REPLACE STREAMLIT {{DATABASE}}.{{SCHEMA}}.SSV2_STREAMING_MONITOR
    ROOT_LOCATION = '@{{DATABASE}}.{{SCHEMA}}.SSV2_STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = '{{WAREHOUSE}}';
```
