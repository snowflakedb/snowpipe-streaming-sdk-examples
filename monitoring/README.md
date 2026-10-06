# Snowpipe Streaming monitoring dashboard

Create a Streamlit in Snowflake app to monitor ingestion from your event table. Start with recorded streaming activity by target table, then select a table to investigate its throughput, processing latency, channels, and errors. Pipe identity is shown inline; selecting a pipe is optional.

## Create the app in Snowsight

You need only **`streamlit_app.py` and `pyproject.toml`**. The CLI template is optional.

1. Follow the prerequisites and event-collection setup in [Monitor Snowpipe Streaming](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-event-table-telemetry).
2. Use an app execution role with permission to query the event source and use the required compute resources. `SNOWFLAKE.EVENTS_VIEWER` gives access to the default view, not a custom event table. For shared deployments, use a dedicated least-privilege owner role, not `ACCOUNTADMIN`.
3. In Snowsight Workspaces, create a Streamlit app with a compute pool and query warehouse. Workspace apps use the container runtime. If your UI offers a runtime choice, select **Run on container**. Use Streamlit **1.53.1 or later** with Python 3.11.
4. Upload `streamlit_app.py` and `pyproject.toml`. Before running, attach an approved package source. Where available, select `snowflake.snowpark.pypi_shared_repository` in the app settings; your role needs access through `SNOWFLAKE.PYPI_REPOSITORY_USER`. Alternatively, attach an administrator-approved PyPI external access integration. Creating an integration without attaching it to the app is not sufficient. See [Dependency management](https://docs.snowflake.com/en/developer-guide/streamlit/app-development/dependency-management).
5. Run the app and select **Load / refresh** to browse recorded streaming tables in the account event destination. No target table is required. Optionally search for a **Database** and choose a **Schema** to narrow the overview. Selecting a database resolves its effective event destination, respecting a database override before the account setting. For the built-in destination, the app queries `SNOWFLAKE.TELEMETRY.EVENTS_VIEW`. Use **Advanced** for a governed source override or direct manual table lookup.
6. Send a fresh batch through Snowpipe Streaming, choose a time range that includes it, and select **Load / refresh**. Allow time for telemetry to arrive. Existing target-table rows do not generate new streaming events, and enabling collection does not backfill old events. No event data is queried before submission.

Database search waits for a 350-millisecond pause in typing and requests at most five prefix matches from Snowflake. There is no database lookup for empty input, no full database list, and no separate prefix field. Keep typing to narrow the matches. Unquoted prefixes are uppercased and matched literally, so `_` isn't a wildcard. Use SQL double quotes for mixed-case or special database names, such as `"My.Database"`. If suggestions are unavailable, you can still enter a full name and press Enter.

The app discovers schemas only after a database is selected. It does not enumerate event tables or read target-table rows. Metadata uses the app's execution role, not each viewer's privileges. The live database search uses an inline Streamlit v2 component in the same Python file, with no additional package, CDN, or external network request.

The landing view covers **one accessible event source and the selected window**, not every streaming pipeline in an account. Database-level event destinations elsewhere are not automatically combined. Only tables with recorded, fully attributed events appear in the list; missing telemetry is not zero ingestion. Events without complete target names are counted separately and included in overview summary metrics when they match the scope. Select a database to inspect its overridden destination.

**Advanced** contains optional controls:

- **Enter schema and table manually:** Optional direct lookup, including a target with no recorded events. Use exact stored names without SQL quotes. Schema lists are limited to 1,000 objects per lookup.
- **Override event source:** Enter a fully qualified custom governed view or known event table. Source names support double-quoted components for spaces, mixed case, and embedded dots. If automatic destination lookup fails, the app asks for an override instead of guessing.
- **Channel:** Leave blank to include all channels for the selected table.
- **Show error messages:** Off by default because messages can contain sensitive data.
- **Refresh metadata:** Reload object lists and destination settings after changes. Metadata is cached only within the viewer's session for 60 seconds, with at most 32 lookups retained.

The event source is the monitoring destination, not the table receiving streamed rows. A small caption shows the resolved source. The app does not change routing, enable collection, or grant privileges. If destination lookup is unavailable, verify it as described in [Event table setup](https://docs.snowflake.com/en/developer-guide/logging-tracing/event-table-setting-up). Metadata lookup failures show sanitized diagnostics. Changing the target, time range, source, or error-message setting clears the displayed results; select **Load / refresh** to query the new selection.

## Using the dashboard

- **Streaming tables:** Summary metrics cover the full search/filter scope, not just the displayed page. A full-width table shows fully qualified targets, ingested rows, average rows/second over the selected window, rejected rows/rate, p95 processing time, error-event counts, and last recorded commit. Server-side literal table-name search and 100-row pages keep results bounded. Default order prioritizes rejected rows, error events, then volume. Error-only targets remain visible with unavailable volume rather than zero.
- **Table to channels:** Select a table row to open its metrics and channels directly, without selecting a pipe. **Back to streaming tables** returns to the overview. Changing scope or refreshing clears stale row selections.
- **Pipeline overview:** Five headline metrics show reported rows, rows/second in the latest complete interval, p95 processing time, rejected rows, and rejected-row rate. The header shows the selected window, last refresh, last recorded commit, and latest returned event.
- **Throughput and processing latency:** Side-by-side charts have compact legends above the plot and short unit/coverage captions below. They show ingested/rejected rows per second and p50/p95 latency. Rates exclude partial intervals at the window edges, which are shaded. Missing intervals remain gaps, not zeros. Detailed caveats are under **Metric definitions and limitations**.
- **Channels:** Up to 100 channel/pipe pairs are prioritized by rejected rows, error-event count, then p95 latency; a hint appears when more exist. Each row shows **Reported channel** and **Reported pipe**. Missing pipe identity is **Not reported**, not a default pipe. Names alone do not establish default/named pipe type. A **Pipe filter** optionally narrows the view, with a separate choice for records lacking pipe identity. Attribution coverage reports commits and measured latency separately. Click a channel row to scope charts/errors to that exact channel/pipe pair, or type an exact channel name to include that name across the current pipe scope. Clear focus to restore the parent scope. Average rows/second uses the full selected window.
- **Errors and diagnostics:** The top 50 event-type/error-code groups show recorded occurrences, known affected channels, and first/last seen times. **Recent error details** contains at most 100 events. Raw error messages are queried only when explicitly enabled under Advanced. **Channel lifecycle** shows OPEN/DROP events separately.
- **Refresh:** Manual refresh is the default. **Auto-refresh every 30 seconds** refreshes the active view after the first manual load. It is off by default because queries incur cost; slow queries can delay the next refresh. An overview load runs two sequential SELECTs; table detail runs six, including pipe choices. Pipe or channel drill-down runs five additional SELECTs over the same window. Hidden views are not queried.

Results persist while you interact with the loaded view, in this viewer's session only. A refresh failure clears old and partial results rather than showing stale totals as current. Snapshots are not stored in a shared result cache.

## Optional: Deploy with Snowflake CLI

With Snowflake CLI 3.14 or later, copy `snowflake.yml.example` to `snowflake.yml`, replace every placeholder with your account's settings, and run `snow streamlit deploy` from this folder. The template uses an approved external access integration for package installation. Do not commit the filled-in manifest or credentials.

## Access and sharing

The app uses the standard `st.connection("snowflake")` connection. Deployed apps query with [owner's rights](https://docs.snowflake.com/en/developer-guide/streamlit/object-management/owners-rights), not each viewer's table privileges. No caller grants are required by this connection. Queries disable Streamlit's shared query-result caching; displayed snapshots are kept only in the viewer's session. This example is intended for Streamlit in Snowflake, not a publicly hosted local server.

Sharing the app can expose telemetry and object names that viewers cannot query or list directly. Because viewers can change the source and target filters, grant the app owner access only to telemetry and metadata every intended viewer may see, preferably using a dedicated governed view for telemetry. Filters and the hidden-by-default error-message option are not security controls. Do not share an `ACCOUNTADMIN`-owned app. Keep privileged Workspace tests private and verify the intended visibility before deployment.

## Troubleshooting

- **Failed to retrieve packages / PyPI DNS error:** Check that the package repository or approved external access integration is attached to this development app. After changing settings, restart the app. Package installation happens before dashboard code runs.
- **No matching events:** Verify the event source, target names, effective `LOG_EVENT_LEVEL`, recent streaming activity, and time range. Run a query from the monitoring guide to distinguish missing telemetry from an app-access issue.
- **Telemetry could not be loaded:** Use the diagnostic line beneath the message to identify the failing query stage, exception type, error code, SQLSTATE, and query ID when available. Inspect the query ID in Snowflake Query History. The app deliberately does not display raw database errors or SQL text.
- **An error mentions restricted caller rights or caller grants:** Check that you are using this version's `st.connection("snowflake")`, not the earlier `snowflake-callers-rights` example. That is a different access model requiring extra grants, even when the viewer has an administrative role. Do not grant access to managed system roles merely to work around the error.

Telemetry queries use `?` bind placeholders required by Streamlit's Snowflake connection. Metadata commands use a validated identifier-quoting helper and short-lived cursors. Do not replace binds with `%s` or insert unescaped user input into SQL.

## Interpreting the dashboard

- Error rate is rejected rows divided by parsed rows. Missing data or zero parsed rows displays **N/A** (unavailable), not a healthy zero.
- Latency measures processing inside Snowflake, not total source-to-query time. p50/p95 are approximate percentiles of available event measurements, not row-weighted percentiles. Missing measurements are excluded.
- Latest rows/second uses only the latest complete interval (1, 5, or 15 minutes for the 1-, 6-, or 24-hour window). If that interval has no measurements, it is **N/A** rather than reusing an older interval or reporting zero.
- The last recorded commit is bounded by the selected window. Its age is not source-data freshness, backlog, or proof of a stalled pipeline. Late delivery can affect even complete intervals.
- Byte totals are omitted because request-level bytes can repeat across channel events.
- OPEN/DROP events are operations, not active-channel counts. Row-error events might not include every rejected row.
- Error summaries count telemetry occurrences, not rejected rows. Channel counts exclude missing or empty channel names. Target filters also exclude error events without matching object attributes.
- **Reported channel** displays the telemetry's `channel_name`, not an inferred channel type. Elastic ingestion can expose server-managed identifiers. The app does not classify UUID-like names as Elastic or merge them into one logical `ELASTIC` row without an authoritative mapping.
- Telemetry queries are limited to at most 24 hours with 30-second requested timeouts. The overview scans the selected source across targets, so its query cost can exceed a single-table query; a row limit does not bound scanned data. Narrow database/schema/time filters and configure warehouse/resource policies independently. Sequential queries are not an atomic snapshot: late-arriving events can cause differences between panels. SQL, container compute, and telemetry storage can incur costs.

This example has been exercised in a private Workspace against fresh Snowpipe Streaming telemetry using the standard Snowflake connection. Shared deployment and intended viewer visibility still require validation in your account. This is an example, not a supported product or health guarantee. For SDK channel-status examples, see [Python](../python-example/monitoring) or [Java](../java-example/monitoring).
