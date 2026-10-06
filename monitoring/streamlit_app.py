"""Snowpipe Streaming monitoring for Streamlit in Snowflake."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import re
from time import monotonic
import altair as alt
import pandas as pd
import streamlit as st


EVENT_VIEW = 'SNOWFLAKE.TELEMETRY.EVENTS_VIEW'
RANGES = {'Last hour': (1, 60), 'Last 6 hours': (6, 300), 'Last 24 hours': (24, 900)}
IDENTIFIER_PATTERN = r'(?:[A-Za-z_][A-Za-z0-9_$]*|"(?:[^"\x00-\x1f]|"")+")'
SOURCE_PATTERN = re.compile(rf'({IDENTIFIER_PATTERN})\.({IDENTIFIER_PATTERN})\.({IDENTIFIER_PATTERN})\Z')
METADATA_LIMIT = 1000
METADATA_TTL = 60


DATABASE_SEARCH_HTML = '''
<label for="database-search">Database</label>
<input id="database-search" role="combobox" aria-autocomplete="list" aria-expanded="false"
  aria-controls="database-matches" autocomplete="off" spellcheck="false" maxlength="520"
  placeholder="Type a database name" />
<div id="database-matches" role="listbox" aria-label="Matching databases" hidden></div>
<small id="database-hint" role="status" aria-live="polite"></small>
'''
DATABASE_SEARCH_CSS = '''
label { display: block; margin-bottom: .4rem; font-size: .875rem; }
input { box-sizing: border-box; width: 100%; min-width: 0; padding: .6rem .75rem;
  font: inherit; color: var(--st-text-color); background: var(--st-secondary-background-color);
  border: 1px solid var(--st-border-color, #888); border-radius: .5rem; }
input:focus { outline: 2px solid var(--st-primary-color); outline-offset: -2px; }
[role=listbox] { margin-top: .25rem; border: 1px solid var(--st-border-color, #888);
  border-radius: .5rem; overflow: hidden; }
button { display: block; width: 100%; text-align: left; padding: .5rem .75rem;
  color: var(--st-text-color); background: var(--st-background-color); border: 0;
  font: inherit; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; cursor: pointer; }
button:hover, button[aria-selected=true] { background: var(--st-secondary-background-color); }
small { display: block; margin-top: .3rem; font-size: .8rem; opacity: .7; }
'''
DATABASE_SEARCH_JS = '''
export default function({parentElement, data, setStateValue}) {
  const input = parentElement.querySelector('input');
  const list = parentElement.querySelector('[role=listbox]');
  const hint = parentElement.querySelector('small');
  if (!input._initialized) {
    input.value = data.text;
    input._revision = data.revision;
    input._committed = data.committed;
    input._initialized = true;
  }
  let active = -1;
  const hide = () => {
    list.hidden = true;
    input.setAttribute('aria-expanded', 'false');
    input.removeAttribute('aria-activedescendant');
    active = -1;
  };
  const emit = committed => {
    clearTimeout(input._timer);
    setStateValue('value', {text: input.value, committed, revision: input._revision});
  };
  const choose = value => {
    input.value = value;
    input._revision += 1;
    input._committed = true;
    hide();
    hint.textContent = '';
    emit(true);
  };
  list.replaceChildren();
  // Ignore results from an older search when the user has already typed more.
  if (data.revision === input._revision && data.text === input.value) {
    for (const [index, option] of data.options.entries()) {
      const button = document.createElement('button');
      button.type = 'button';
      button.id = `database-match-${index}`;
      button.setAttribute('role', 'option');
      button.setAttribute('aria-selected', 'false');
      button.tabIndex = -1;
      button.textContent = option.label;
      button.title = option.label;
      button.onmousedown = event => event.preventDefault();
      button.onclick = () => choose(option.value);
      list.appendChild(button);
    }
    list.hidden = data.committed || !data.options.length;
    input.setAttribute('aria-expanded', String(!list.hidden));
    hint.textContent = data.hint;
  } else {
    hide();
    hint.textContent = 'Searching...';
  }
  input.oninput = event => {
    input._revision += 1;
    input._committed = false;
    hide();
    hint.textContent = input.value.trim() ? 'Searching...' : '';
    clearTimeout(input._timer);
    if (!event.isComposing) input._timer = setTimeout(() => emit(false), 350);
  };
  input.oncompositionend = () => {
    clearTimeout(input._timer);
    input._timer = setTimeout(() => emit(false), 350);
  };
  input.onkeydown = event => {
    if (event.isComposing) return;
    const options = [...list.children];
    if (['ArrowDown', 'ArrowUp'].includes(event.key) && options.length && !list.hidden) {
      event.preventDefault();
      active = (active + (event.key === 'ArrowDown' ? 1 : -1) + options.length) % options.length;
      options.forEach((option, index) => option.setAttribute('aria-selected', String(index === active)));
      input.setAttribute('aria-activedescendant', options[active].id);
    } else if (event.key === 'Enter') {
      event.preventDefault();
      if (active >= 0 && !list.hidden) options[active].click();
      else choose(input.value);
    } else if (event.key === 'Escape') {
      hide();
    }
  };
  // A new response can arrive during the debounce; preserve the newer input.
  if (data.revision !== input._revision && data.text !== input.value) {
    clearTimeout(input._timer);
    input._timer = setTimeout(() => emit(input._committed), 350);
  }
  return () => {
    clearTimeout(input._timer);
    input.oninput = input.onkeydown = input.oncompositionend = null;
  };
}
'''


def sql_identifier(name):
    if not isinstance(name, str) or not name or len(name) > 255 or any(ord(char) < 32 for char in name):
        raise ValueError('Object names must contain 1 to 255 characters without control characters.')
    return name if re.fullmatch(r'[A-Z_][A-Z0-9_$]*', name) else '"' + name.replace('"', '""') + '"'


def source_parts(source):
    match = SOURCE_PATTERN.fullmatch(source)
    if not match:
        raise ValueError('Event source must be a fully qualified name: DATABASE.SCHEMA.TABLE_OR_VIEW.')
    parts = [part[1:-1].replace('""', '"') if part.startswith('"') else part.upper() for part in match.groups()]
    for part in parts:
        sql_identifier(part)
    return parts


def database_name(value):
    value = value.strip()
    if not re.fullmatch(IDENTIFIER_PATTERN, value):
        raise ValueError('Enter a full database name, not a prefix. Use double quotes for mixed-case or special names.')
    name = value[1:-1].replace('""', '"') if value.startswith('"') else value.upper()
    sql_identifier(name)
    return name


def database_search_prefix(value):
    value = value.strip()
    if not value:
        return ''
    if value.startswith('"'):
        raw = value[1:-1] if value.endswith('"') and len(value) > 1 else value[1:]
        if re.search(r'(?<!")"(?!")', raw):
            raise ValueError('Escape quotes in a database name by doubling them.')
        prefix = raw.replace('""', '"')
    elif re.fullmatch(r'[A-Za-z_][A-Za-z0-9_$]*', value):
        prefix = value.upper()
    else:
        raise ValueError('Use a database name or prefix. Double-quote mixed-case or special names.')
    if prefix:
        sql_identifier(prefix)
    return prefix


@dataclass(frozen=True)
class Scope:
    source: str
    database: str
    schema: str
    table: str
    start: datetime
    end: datetime
    bucket_seconds: int
    channel: str = ''
    pipe: str | None = None

    def __post_init__(self):
        source_parts(self.source)
        for name in ('database', 'schema', 'table'):
            value = getattr(self, name)
            if value != value.strip() or len(value) > 255:
                raise ValueError(f'Enter the {name} name exactly as stored, without surrounding spaces.')
        if self.table and not all((self.database, self.schema)) or self.schema and not self.database:
            raise ValueError('A table requires its database and schema; a schema requires its database.')
        if (self.channel or self.pipe is not None) and not self.table:
            raise ValueError('Select a table before a pipe or channel.')
        if self.pipe is not None and len(self.pipe) > 255:
            raise ValueError('Pipe name is too long.')
        if len(self.channel) > 1024:
            raise ValueError('Channel name is too long.')
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError('Time bounds must include a timezone.')
        if not timedelta(0) < self.end - self.start <= timedelta(hours=24):
            raise ValueError('Select a time window of at most 24 hours.')
        if self.bucket_seconds not in {60, 300, 900}:
            raise ValueError('Unsupported time bucket.')


def make_scope(source, database, schema, table, range_name, channel='', now=None):
    if range_name not in RANGES:
        raise ValueError('Select a supported time range.')
    hours, bucket = RANGES[range_name]
    end = now or datetime.now(timezone.utc)
    return Scope(source, database, schema, table, end - timedelta(hours=hours), end, bucket, channel)


def _where(scope):
    clause = """FROM IDENTIFIER(?)
WHERE RECORD_TYPE = 'EVENT'
  AND SCOPE['name']::STRING = 'snow.snowpipe.streaming'
  AND TIMESTAMP >= ? AND TIMESTAMP < ?"""
    # Event-table timestamps are UTC TIMESTAMP_NTZ, independent of the session timezone.
    params = [scope.source,
              scope.start.astimezone(timezone.utc).replace(tzinfo=None),
              scope.end.astimezone(timezone.utc).replace(tzinfo=None)]
    for attribute, value in (('database', scope.database), ('schema', scope.schema), ('table', scope.table)):
        if value:
            clause += f"\n  AND RESOURCE_ATTRIBUTES['snow.{attribute}.name']::STRING = ?"
            params.append(value)
    if scope.pipe is not None:
        clause += "\n  AND COALESCE(RESOURCE_ATTRIBUTES['snow.pipe.name']::STRING, '') = ?"
        params.append(scope.pipe)
    if scope.channel:
        clause += "\n  AND VALUE['channel_name']::STRING = ?"
        params.append(scope.channel)
    return clause, params


def queries(scope, include_messages=False):
    where, params = _where(scope)
    summary = """SELECT
  COUNT(*) AS event_count,
  COUNT_IF(RECORD['name']::STRING = 'commit') AS commit_events,
  COUNT_IF(RECORD['name']::STRING = 'latency') AS latency_events,
  SUM(IFF(RECORD['name']::STRING = 'commit', VALUE['row_count']::NUMBER, NULL)) AS rows_ingested,
  SUM(IFF(RECORD['name']::STRING = 'commit', VALUE['rows_parsed']::NUMBER, NULL)) AS rows_parsed,
  SUM(IFF(RECORD['name']::STRING = 'commit', VALUE['error_count']::NUMBER, NULL)) AS errors,
  COUNT(IFF(RECORD['name']::STRING = 'latency', VALUE['total_latency_ms']::FLOAT, NULL)) AS measured_samples,
  AVG(IFF(RECORD['name']::STRING = 'latency', VALUE['total_latency_ms']::FLOAT, NULL)) AS avg_latency_ms,
  APPROX_PERCENTILE(IFF(RECORD['name']::STRING = 'latency', VALUE['total_latency_ms']::FLOAT, NULL), 0.5) AS p50_latency_ms,
  APPROX_PERCENTILE(IFF(RECORD['name']::STRING = 'latency', VALUE['total_latency_ms']::FLOAT, NULL), 0.95) AS p95_latency_ms,
  MAX(IFF(RECORD['name']::STRING = 'commit', TIMESTAMP, NULL)) AS latest_commit,
  COUNT_IF(RECORD['name']::STRING = 'commit' AND NULLIF(RESOURCE_ATTRIBUTES['snow.pipe.name']::STRING, '') IS NOT NULL) AS pipe_commit_events,
  COUNT(IFF(RECORD['name']::STRING = 'latency' AND NULLIF(RESOURCE_ATTRIBUTES['snow.pipe.name']::STRING, '') IS NOT NULL, VALUE['total_latency_ms']::FLOAT, NULL)) AS pipe_latency_samples,
  COUNT_IF(RECORD['name']::STRING IN ('row_error', 'channel_error')) AS error_events,
  MAX(TIMESTAMP) AS latest_event
""" + where
    timeline = """SELECT TIME_SLICE(TIMESTAMP::TIMESTAMP_NTZ, ?, 'SECOND') AS bucket,
  COUNT_IF(RECORD['name']::STRING = 'commit') AS commit_events,
  SUM(IFF(RECORD['name']::STRING = 'commit', VALUE['row_count']::NUMBER, NULL)) AS rows_ingested,
  SUM(IFF(RECORD['name']::STRING = 'commit', VALUE['error_count']::NUMBER, NULL)) AS errors,
  COUNT_IF(RECORD['name']::STRING = 'latency') AS latency_events,
  COUNT(IFF(RECORD['name']::STRING = 'latency', VALUE['total_latency_ms']::FLOAT, NULL)) AS measured_samples,
  APPROX_PERCENTILE(IFF(RECORD['name']::STRING = 'latency', VALUE['total_latency_ms']::FLOAT, NULL), 0.5) AS p50_latency_ms,
  APPROX_PERCENTILE(IFF(RECORD['name']::STRING = 'latency', VALUE['total_latency_ms']::FLOAT, NULL), 0.95) AS p95_latency_ms,
  COUNT_IF(RECORD['name']::STRING = 'channel_lifecycle' AND VALUE['event_type']::STRING = 'OPEN') AS opens,
  COUNT_IF(RECORD['name']::STRING = 'channel_lifecycle' AND VALUE['event_type']::STRING = 'DROP') AS drops
""" + where + '\nGROUP BY 1 ORDER BY 1'
    channels = """SELECT VALUE['channel_name']::STRING AS channel_name,
  COALESCE(RESOURCE_ATTRIBUTES['snow.pipe.name']::STRING, '') AS pipe_name,
  SUM(IFF(RECORD['name']::STRING = 'commit', VALUE['row_count']::NUMBER, NULL)) AS rows_ingested,
  SUM(IFF(RECORD['name']::STRING = 'commit', VALUE['rows_parsed']::NUMBER, NULL)) AS rows_parsed,
  SUM(IFF(RECORD['name']::STRING = 'commit', VALUE['error_count']::NUMBER, NULL)) AS errors,
  COUNT_IF(RECORD['name']::STRING IN ('row_error', 'channel_error')) AS error_events,
  COUNT(IFF(RECORD['name']::STRING = 'latency', VALUE['total_latency_ms']::FLOAT, NULL)) AS measured_samples,
  APPROX_PERCENTILE(IFF(RECORD['name']::STRING = 'latency', VALUE['total_latency_ms']::FLOAT, NULL), 0.95) AS p95_latency_ms,
  MAX(IFF(RECORD['name']::STRING = 'commit', TIMESTAMP, NULL)) AS latest_commit
""" + where + '\nGROUP BY 1, 2 ORDER BY errors DESC NULLS LAST, error_events DESC, p95_latency_ms DESC NULLS LAST, channel_name NULLS LAST, pipe_name NULLS LAST LIMIT 101'
    error_groups = """SELECT RECORD['name']::STRING AS event_name,
  VALUE['error_code']::STRING AS error_code,
  COUNT(*) AS event_count,
  COUNT(DISTINCT IFF(NULLIF(VALUE['channel_name']::STRING, '') IS NOT NULL,
    ARRAY_CONSTRUCT(COALESCE(RESOURCE_ATTRIBUTES['snow.pipe.name']::STRING, ''), VALUE['channel_name']::STRING), NULL)) AS affected_channels,
  MIN(TIMESTAMP) AS first_seen, MAX(TIMESTAMP) AS last_seen
""" + where + "\n  AND RECORD['name']::STRING IN ('row_error', 'channel_error')\nGROUP BY 1, 2 ORDER BY event_count DESC, event_name, error_code LIMIT 50"
    message_column = ", LEFT(VALUE['error_message']::STRING, 1000) AS error_message" if include_messages else ''
    errors = """SELECT TIMESTAMP AS event_time, RECORD['name']::STRING AS event_name,
  VALUE['channel_name']::STRING AS channel_name,
  RESOURCE_ATTRIBUTES['snow.pipe.name']::STRING AS pipe_name,
  VALUE['error_type']::STRING AS error_type,
  VALUE['error_code']::STRING AS error_code""" + message_column + '\n' + where + "\n  AND RECORD['name']::STRING IN ('row_error', 'channel_error')\nORDER BY TIMESTAMP DESC LIMIT 100"
    return {
        'summary': (summary, list(params)),
        'timeline': (timeline, [scope.bucket_seconds, *params]),
        'channels': (channels, list(params)),
        'error_groups': (error_groups, list(params)),
        'errors': (errors, list(params)),
    }


def pipe_query(scope):
    where, params = _where(scope)
    return ("SELECT DISTINCT COALESCE(RESOURCE_ATTRIBUTES['snow.pipe.name']::STRING, '') AS pipe_name\n" +
            where + '\nORDER BY 1 LIMIT 1001', params)


def overview_queries(scope, search='', page=0):
    if scope.table or scope.channel or scope.pipe is not None:
        raise ValueError('The overview must not select a table, pipe, or channel.')
    if not isinstance(page, int) or not 0 <= page <= 10000 or len(search) > 255:
        raise ValueError('Invalid table search or page.')
    where, params = _where(scope)
    # Search is literal substring matching, not SQL wildcard matching.
    if search:
        where += "\n  AND CONTAINS(UPPER(RESOURCE_ATTRIBUTES['snow.table.name']::STRING), UPPER(?))"
        params.append(search)
    base = """WITH activity AS (
SELECT RESOURCE_ATTRIBUTES['snow.database.name']::STRING AS database_name,
  RESOURCE_ATTRIBUTES['snow.schema.name']::STRING AS schema_name,
  RESOURCE_ATTRIBUTES['snow.table.name']::STRING AS table_name,
  RECORD['name']::STRING AS event_name, TIMESTAMP AS event_time,
  IFF(RECORD['name']::STRING = 'commit', VALUE['row_count']::NUMBER, NULL) AS rows_ingested,
  IFF(RECORD['name']::STRING = 'commit', VALUE['rows_parsed']::NUMBER, NULL) AS rows_parsed,
  IFF(RECORD['name']::STRING = 'commit', VALUE['error_count']::NUMBER, NULL) AS errors,
  IFF(RECORD['name']::STRING = 'latency', VALUE['total_latency_ms']::FLOAT, NULL) AS latency_ms,
  NULLIF(RESOURCE_ATTRIBUTES['snow.pipe.name']::STRING, '') AS pipe_name
""" + where + "\n)\n"
    summary = base + """SELECT COUNT(*) AS event_count,
  COUNT_IF(event_name = 'commit') AS commit_events,
  SUM(rows_ingested) AS rows_ingested, SUM(rows_parsed) AS rows_parsed, SUM(errors) AS errors,
  APPROX_PERCENTILE(latency_ms, 0.95) AS p95_latency_ms,
  COUNT(latency_ms) AS measured_samples,
  COUNT_IF(event_name IN ('row_error', 'channel_error')) AS error_events,
  COUNT_IF(NULLIF(database_name, '') IS NULL OR NULLIF(schema_name, '') IS NULL OR NULLIF(table_name, '') IS NULL) AS unattributed_events
FROM activity"""
    tables = base + """, targets AS (
SELECT database_name, schema_name, table_name, COUNT(*) AS event_count,
  SUM(rows_ingested) AS rows_ingested, SUM(rows_parsed) AS rows_parsed, SUM(errors) AS errors,
  COUNT_IF(event_name IN ('row_error', 'channel_error')) AS error_events,
  APPROX_PERCENTILE(latency_ms, 0.95) AS p95_latency_ms, COUNT(latency_ms) AS measured_samples,
  MAX(IFF(event_name = 'commit', event_time, NULL)) AS latest_commit
FROM activity
WHERE NULLIF(database_name, '') IS NOT NULL AND NULLIF(schema_name, '') IS NOT NULL AND NULLIF(table_name, '') IS NOT NULL
GROUP BY 1, 2, 3
)
SELECT *, COUNT(*) OVER () AS total_tables FROM targets
ORDER BY errors DESC NULLS LAST, error_events DESC, rows_ingested DESC NULLS LAST,
  database_name, schema_name, table_name
LIMIT 100 OFFSET ?"""
    return {'summary': (summary, list(params)), 'tables': (tables, [*params, page * 100])}


def number(value):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return float(result) if result.is_finite() else None


def error_rate(errors, parsed):
    numerator, denominator = number(errors), number(parsed)
    if numerator is None or denominator is None or denominator <= 0 or not 0 <= numerator <= denominator:
        return None
    return 100 * numerator / denominator


def display(value, suffix='', decimals=0):
    numeric = number(value)
    return 'Unavailable' if numeric is None else f'{numeric:,.{decimals}f}{suffix}'


def collection_state(summary):
    if not number(summary.get('event_count')):
        return 'No matching events. Check the source view, target names, time range, permissions, and LOG_EVENT_LEVEL. This is not evidence of healthy ingestion.'
    if not number(summary.get('commit_events')):
        return 'Events exist, but no commit events were returned. Ingestion totals and error rate are unavailable. Check INFO-level collection and pipeline activity.'
    return None


def safe_diagnostic(error, stage):
    fields = [f'Stage: {stage}']
    for label, value, pattern in (
        ('Type', type(error).__name__, r'[A-Za-z_][A-Za-z0-9_]{0,79}'),
        ('Code', str(getattr(error, 'errno', '')), r'\d{1,10}'),
        ('SQLSTATE', str(getattr(error, 'sqlstate', '')), r'[A-Z0-9]{5}'),
        ('Query ID', str(getattr(error, 'sfqid', '')), r'[a-fA-F0-9-]{36}'),
    ):
        if re.fullmatch(pattern, value):
            fields.append(f'{label}: {value}')
    return ' | '.join(fields)


def discover(connection, kind, database='', schema='', prefix=''):
    try:
        row_limit = METADATA_LIMIT
        limit = f' LIMIT {METADATA_LIMIT}'
        if kind == 'database suggestions':
            if not prefix:
                return []
            sql_identifier(prefix)
            literal = prefix.replace('\\', '\\\\').replace("'", "''")
            row_limit = 5
            sql = f"SHOW TERSE DATABASES STARTS WITH '{literal}' LIMIT 5"
        elif kind == 'schemas':
            sql = f'SHOW TERSE SCHEMAS IN DATABASE {sql_identifier(database)}' + limit
        elif kind == 'tables':
            sql = f'SHOW TABLES IN SCHEMA {sql_identifier(database)}.{sql_identifier(schema)}' + limit
        elif kind == 'event destination':
            scope = f'DATABASE {sql_identifier(database)}' if database else 'ACCOUNT'
            sql = f"SHOW PARAMETERS LIKE 'EVENT_TABLE' IN {scope}"
        else:
            raise ValueError('Unsupported metadata lookup.')
    except ValueError as error:
        st.warning(str(error))
        return None

    # Keep metadata within this viewer's session, never in a shared data cache.
    cache = st.session_state.setdefault('_metadata', {})
    cache_key = (id(connection), sql)
    now = monotonic()
    if cache_key not in cache or now - cache[cache_key][0] >= METADATA_TTL:
        rows, diagnostic = [], None
        try:
            # SHOW results aren't always Arrow-compatible; fetch rows with a short-lived cursor.
            with connection.cursor() as cursor:
                cursor.execute(sql, timeout=30)
                columns = [column[0].lower() for column in cursor.description]
                keep = {'name', 'database_name', 'schema_name', 'is_event', 'key', 'value', 'level'}
                rows = [
                    {key: value for key, value in zip(columns, row) if key in keep}
                    for row in cursor.fetchmany(row_limit)
                ]
                if kind == 'database suggestions':
                    # Some accounts include discoverable imported databases despite SHOW's prefix.
                    rows = [row for row in rows if str(row.get('name', '')).startswith(prefix)]
        except Exception as error:
            diagnostic = safe_diagnostic(error, kind)
        if len(cache) >= 32:
            cache.pop(next(iter(cache)))
        cache[cache_key] = (now, rows, diagnostic)

    _, rows, diagnostic = cache[cache_key]
    if diagnostic:
        st.warning(f'Could not discover {kind}. Use Advanced to enter names manually or refresh metadata.')
        st.caption(diagnostic)
        return None
    if kind == 'database suggestions':
        # Recheck cached results too, including sessions opened before filtering was added.
        names = sorted({row['name'] for row in rows if isinstance(row.get('name'), str) and row['name'].startswith(prefix)})
        return [{'name': name} for name in names[:5]]
    if len(rows) >= METADATA_LIMIT:
        st.caption('Not all names are listed. Use Advanced to enter a missing schema or table.')
    return rows


def object_names(rows):
    return sorted({row['name'] for row in rows or [] if row.get('name') and row.get('is_event') != 'Y'}, key=lambda name: (name.casefold(), name))


def suggested_source(account_rows, database_rows):
    # A failed database lookup cannot rule out an override; don't guess the account destination.
    if database_rows is None:
        return ''
    for rows in (database_rows, account_rows):
        for row in rows or []:
            if str(row.get('key', '')).upper() == 'EVENT_TABLE' and row.get('value'):
                try:
                    parts = source_parts(row['value'])
                except ValueError:
                    return ''
                if parts == ['SNOWFLAKE', 'TELEMETRY', 'EVENTS']:
                    return EVENT_VIEW
                return '.'.join(sql_identifier(part) for part in parts)
    return ''


def reset_children(context_key, context, children):
    if st.session_state.get(context_key) != context:
        for key in children:
            st.session_state.pop(key, None)
        st.session_state[context_key] = context


def database_search(connection, component):
    state = st.session_state.get('database_search', {}).get('value') or {}
    text = state.get('text', '')
    committed = state.get('committed') is True
    revision = state.get('revision', 0)
    if not isinstance(text, str) or len(text) > 520 or not isinstance(revision, int):
        st.error('Invalid database selection. Clear the search and try again.')
        return ''
    matches, selected, hint = [], '', ''
    try:
        if committed and text.strip():
            selected = database_name(text)
        elif text.strip():
            prefix = database_search_prefix(text)
            matches = object_names(discover(connection, 'database suggestions', prefix=prefix))[:5] if prefix else []
            hint = 'Top 5 matches. Keep typing to narrow.' if len(matches) == 5 else 'Select a match or press Enter to use the full name.'
            if not matches:
                hint = 'No suggestions. Enter a full name and press Enter.'
    except ValueError as error:
        hint = str(error)
    component(
        key='database_search',
        data={'text': text, 'committed': committed, 'revision': revision, 'hint': hint,
              'options': [{'label': name, 'value': sql_identifier(name)} for name in matches]},
        default={'value': {'text': text, 'committed': committed, 'revision': revision}},
        on_value_change=lambda: None,
    )
    return selected


def timeline_frame(frame, scope):
    columns = ['commit_events', 'rows_ingested', 'errors', 'latency_events', 'measured_samples', 'p50_latency_ms', 'p95_latency_ms', 'opens', 'drops']
    first = pd.Timestamp(scope.start).floor(f'{scope.bucket_seconds}s')
    last = pd.Timestamp(scope.end).floor(f'{scope.bucket_seconds}s')
    if last == pd.Timestamp(scope.end):
        last -= pd.Timedelta(seconds=scope.bucket_seconds)
    grid = pd.date_range(first, last, freq=f'{scope.bucket_seconds}s', name='bucket')
    if frame.empty:
        chart = pd.DataFrame(index=grid, columns=columns, dtype=float)
    else:
        chart = frame.copy()
        chart['bucket'] = pd.to_datetime(chart['bucket'], utc=True)
        for column in columns:
            chart[column] = chart[column].map(number)
        chart = chart.set_index('bucket').reindex(grid)
    chart['complete'] = (chart.index >= pd.Timestamp(scope.start)) & (chart.index + pd.Timedelta(seconds=scope.bucket_seconds) <= pd.Timestamp(scope.end))
    # Never turn an absent interval into zero. Partial edge intervals aren't comparable rates.
    chart['rows_per_second'] = (chart['rows_ingested'] / scope.bucket_seconds).where(chart['complete'])
    chart['rejected_per_second'] = (chart['errors'] / scope.bucket_seconds).where(chart['complete'])
    return chart


def latest_rate(chart):
    complete = chart[chart['complete']]
    if complete.empty:
        return None, None
    return number(complete.iloc[-1]['rows_per_second']), complete.index[-1]


def utc_text(value):
    if value is None or pd.isna(value):
        return 'Unavailable'
    return pd.to_datetime(value, utc=True).strftime('%Y-%m-%d %H:%M:%S UTC')


def headline(value, latency=False, decimals=0):
    numeric = number(value)
    if numeric is None:
        return 'N/A'
    if latency:
        return f'{numeric / 1000:,.2f} s' if numeric >= 1000 else f'{numeric:,.0f} ms'
    for scale, suffix in ((1e12, 'T'), (1e9, 'B'), (1e6, 'M'), (1e3, 'K')):
        if abs(numeric) >= scale:
            return f'{numeric / scale:,.2f}{suffix}'
    return f'{numeric:,.{decimals}f}'


def channel_frame(frame, scope):
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    for column in ('rows_ingested', 'rows_parsed', 'errors', 'error_events', 'measured_samples', 'p95_latency_ms'):
        result[column] = result[column].map(number)
    result['rows_per_second'] = result['rows_ingested'] / (scope.end - scope.start).total_seconds()
    result['error_rate'] = [error_rate(errors, parsed) for errors, parsed in zip(result['errors'], result['rows_parsed'])]
    result['latest_commit'] = pd.to_datetime(result['latest_commit'], utc=True)
    return result


def load_snapshot(connection, scope, include_messages):
    results = {}
    stage = 'initialization'
    try:
        for stage, (sql, params) in queries(scope, include_messages).items():
            frame = connection.query(sql, params=params, ttl=0, timeout=30)
            frame.columns = [column.lower() for column in frame.columns]
            results[stage] = frame
    except Exception as error:
        st.error('Telemetry could not be loaded. No partial or previously loaded totals are displayed. Check source access and compute availability using Query History.')
        st.caption(safe_diagnostic(error, stage))
        return None
    return {'scope': scope, 'results': results, 'loaded_at': datetime.now(timezone.utc)}


def trend_chart(frame, columns, labels, title, scope):
    plot = frame.reset_index()[['bucket', *columns]].rename(columns=dict(zip(columns, labels)))
    plot = plot.melt('bucket', var_name='Series', value_name='Value')
    start, end = pd.Timestamp(scope.start), pd.Timestamp(scope.end)
    ranges = []
    for bucket in frame.index[~frame['complete']]:
        ranges.append({'start': max(bucket, start), 'end': min(bucket + pd.Timedelta(seconds=scope.bucket_seconds), end)})
    lines = alt.Chart(plot).mark_line(point=alt.OverlayMarkDef(size=22), strokeWidth=2).encode(
        x=alt.X('bucket:T', title=None, scale=alt.Scale(type='utc', domain=[start.isoformat(), end.isoformat()]), axis=alt.Axis(labelExpr="utcFormat(datum.value, '%H:%M')", tickCount=5, labelFontSize=11, grid=False)),
        y=alt.Y('Value:Q', title=None, axis=alt.Axis(tickCount=5, labelExpr="abs(datum.value) >= 1000 ? format(datum.value, '.3~s') : format(datum.value, '.3~g')", labelFontSize=11)),
        color=alt.Color('Series:N', scale=alt.Scale(domain=labels, range=['#29B5E8', '#FF9F36']), legend=alt.Legend(title=None, orient='top', direction='horizontal', columns=2, symbolType='stroke', labelFontSize=12, offset=8, padding=0)),
        tooltip=[alt.Tooltip('bucket:T', title='Interval start (UTC)', format='%Y-%m-%d %H:%M', formatType='utc'), 'Series:N', alt.Tooltip('Value:Q', format=',.2f')],
    )
    if ranges:
        edge = alt.Chart(pd.DataFrame(ranges)).mark_rect(color='#808080', opacity=0.12).encode(x=alt.X('start:T', scale=alt.Scale(type='utc')), x2='end:T')
        lines = edge + lines
    st.altair_chart(lines.properties(height=310, width='container'), width='stretch', height=350)


def render_snapshot(snapshot, selected_channel):
    scope, results = snapshot['scope'], snapshot['results']
    summary = results['summary'].iloc[0].to_dict() if not results['summary'].empty else {}
    chart = timeline_frame(results['timeline'], scope)
    state = collection_state(summary)
    if state:
        st.warning(state)
    st.subheader('Table streaming metrics' if not selected_channel else 'Channel overview')
    st.caption(f'{scope.database}.{scope.schema}.{scope.table}' + (f' / {selected_channel}' if selected_channel else '') + f' · Refreshed {utc_text(snapshot["loaded_at"])}')

    rate, bucket = latest_rate(chart)
    with st.container(horizontal=True):
        st.metric('Rows ingested', headline(summary.get('rows_ingested')), border=True, width=175, help=f'Exact reported total: {display(summary.get("rows_ingested"))}')
        st.metric('Latest rows / sec', headline(rate, decimals=2), border=True, width=175, help='Latest complete interval only. Missing telemetry is unavailable, not zero.')
        st.metric('p95 processing time', headline(summary.get('p95_latency_ms'), latency=True), border=True, width=175, help='Approximate 95th percentile of recorded server-side latency measurements. Not source-to-query latency.')
        st.metric('Rejected rows', headline(summary.get('errors')), border=True, width=175, help=f'Exact reported total: {display(summary.get("errors"))}')
        st.metric('Row error rate', display(error_rate(summary.get('errors'), summary.get('rows_parsed')), '%', 2).replace('Unavailable', 'N/A'), border=True, width=175)
    interval_text = utc_text(bucket) if bucket is not None else 'Unavailable'
    with st.expander('Measurement details'):
        st.caption(f'Window: {utc_text(scope.start)} to {utc_text(scope.end)}')
        st.caption(f'Last recorded commit: {utc_text(summary.get("latest_commit"))} | Latest event: {utc_text(summary.get("latest_event"))}')
        st.caption(f'Rate interval: {interval_text} ({scope.bucket_seconds // 60} minutes). N/A means no usable measurement, not zero. Commit timestamps reflect telemetry, not source-data freshness.')

    with st.container(horizontal=True):
        with st.container(border=True, width='stretch', key='throughput_chart_card'):
            st.subheader('Throughput')
            if chart['rows_ingested'].notna().any():
                trend_chart(chart, ['rows_per_second', 'rejected_per_second'], ['Ingested', 'Rejected'], 'Rows / second', scope)
            else:
                st.info('No commit measurements in this window.')
            st.caption(f'Rows/s · {scope.bucket_seconds // 60}-min intervals · UTC · shaded = partial')
        with st.container(border=True, width='stretch', key='latency_chart_card'):
            st.subheader('Processing latency')
            if chart['p95_latency_ms'].notna().any():
                trend_chart(chart, ['p50_latency_ms', 'p95_latency_ms'], ['p50', 'p95'], 'Milliseconds', scope)
            else:
                st.info('No latency measurements in this window.')
            st.caption(f'Milliseconds · UTC · {display(summary.get("measured_samples"))} / {display(summary.get("latency_events"))} events measured')

    return results


def render_channels(snapshot):
    frame = channel_frame(snapshot['results']['channels'], snapshot['scope'])
    st.subheader('Channels')
    if frame.empty:
        st.info('No channel activity returned.')
        return
    if len(frame) > 100:
        st.info('Showing the first 100 channel/pipe pairs. Narrow the pipe filter or enter an exact channel to investigate another pair.')
    frame = frame.head(100).copy()
    if 'pipe_name' not in frame:
        frame['pipe_name'] = ''
    shown = frame[['channel_name', 'pipe_name', 'rows_ingested', 'rows_per_second', 'errors', 'error_rate', 'p95_latency_ms', 'measured_samples', 'latest_commit', 'error_events']].copy()
    shown['pipe_name'] = shown['pipe_name'].replace('', 'Not reported')
    event = st.dataframe(shown, hide_index=True, on_select='rerun', selection_mode='single-row', key='channel_table', column_config={
        'channel_name': st.column_config.TextColumn('Reported channel', help='Raw channel_name from telemetry. Elastic ingestion can expose server-managed identifiers; this is not a channel-type label.'),
        'pipe_name': st.column_config.TextColumn('Reported pipe', help='Exact pipe name when present. Missing identity is Not reported, not a default pipe. Default versus named is not inferred.'),
        'rows_ingested': st.column_config.NumberColumn('Rows ingested', format='localized'),
        'rows_per_second': st.column_config.NumberColumn('Avg rows / sec', format='%.2f', help='Reported rows divided by the entire selected window; not instantaneous throughput.'),
        'errors': st.column_config.NumberColumn('Rejected rows', format='localized'),
        'error_rate': st.column_config.NumberColumn('Rejected %', format='%.2f'),
        'p95_latency_ms': st.column_config.NumberColumn('p95 (ms)', format='%.0f'),
        'measured_samples': st.column_config.NumberColumn('Latency samples'),
        'latest_commit': st.column_config.DatetimeColumn('Last commit (UTC)', format='YYYY-MM-DD HH:mm:ss'),
        'error_events': st.column_config.NumberColumn('Error events'),
    })
    selection = list(event.selection.rows)
    previous = st.session_state.get('_channel_selection', [])
    st.session_state['_channel_selection'] = selection
    if previous and not selection:
        st.session_state['_pending_focus'] = ''
        st.session_state['_selected_channel_pipe'] = None
        st.rerun(scope='fragment')
    if selection != previous and selection and 0 <= selection[0] < len(frame):
        chosen = frame.iloc[selection[0]]['channel_name']
        if isinstance(chosen, str) and chosen:
            st.session_state['_pending_focus'] = chosen
            st.session_state['_selected_channel_pipe'] = frame.iloc[selection[0]]['pipe_name']
            st.rerun(scope='fragment')


def render_errors(results, include_messages):
    st.subheader('Errors and diagnostics')
    groups = results['error_groups']
    if groups.empty:
        st.info('No matching error events returned. This does not establish pipeline health.')
    else:
        groups = groups.copy()
        for column in ('event_count', 'affected_channels'):
            groups[column] = groups[column].map(number)
        for column in ('first_seen', 'last_seen'):
            groups[column] = pd.to_datetime(groups[column], utc=True)
        st.dataframe(groups, hide_index=True, column_config={
            'event_name': 'Event type', 'error_code': 'Error code', 'event_count': 'Events',
            'affected_channels': 'Known channels', 'first_seen': 'First seen (UTC)', 'last_seen': 'Last seen (UTC)',
        })
        st.caption('Top 50 event-type/error-code groups across the selected window. Unknown channel names are not counted as known channels.')
    with st.expander('Recent error details', expanded=not results['errors'].empty):
        if include_messages:
            st.warning('Error messages can contain sensitive data. Do not publish these results.')
        if not results['errors'].empty:
            errors = results['errors'].copy()
            errors['event_time'] = pd.to_datetime(errors['event_time'], utc=True)
            st.dataframe(errors, hide_index=True)
            st.caption('Most recent 100 recorded error events; not an exhaustive rejected-row list.')
        else:
            st.caption('No recent error details for this selection.')
    with st.expander('Channel lifecycle'):
        activity = results['timeline']
        if not activity.empty:
            activity = activity.copy()
            activity['bucket'] = pd.to_datetime(activity['bucket'], utc=True)
            activity = activity.set_index('bucket')[['opens', 'drops']].apply(lambda column: column.map(number))
            st.bar_chart(activity, height=180)
        st.caption('Recorded OPEN/DROP operations, not active-channel counts. No offset-based recovery decisions are made here.')


def dashboard_panel(connection, context, range_name, include_messages, submitted, auto_refresh):
    source, database, schema, table, _, channel, _ = context
    if st.session_state.get('_loaded_context') != context:
        st.info('Select Load / refresh to retry loading telemetry.')
        return
    if st.button('Back to streaming tables', key='back_to_tables'):
        st.session_state['_pending_target'] = ('', '', '')
        st.rerun()
    snapshot = st.session_state.get('_overview_snapshot')
    due = auto_refresh and snapshot is not None and (datetime.now(timezone.utc) - snapshot['scope'].end).total_seconds() >= 30
    if submitted or snapshot is None or due:
        drill_end = st.session_state.pop('_drill_end', None)
        scope = make_scope(source, database, schema, table, range_name, channel, now=drill_end)
        with st.spinner('Refreshing telemetry'):
            snapshot = load_snapshot(connection, scope, include_messages)
            if snapshot is not None:
                try:
                    sql, params = pipe_query(scope)
                    pipes = connection.query(sql, params=params, ttl=0, timeout=30)
                    pipes.columns = [column.lower() for column in pipes.columns]
                    snapshot['pipes'] = pipes
                except Exception as error:
                    st.error('Pipe metadata could not be loaded. No partial totals are displayed.')
                    st.caption(safe_diagnostic(error, 'pipes'))
                    snapshot = None
        st.session_state.pop('_channel_snapshot', None)
        st.session_state.pop('channel_table', None)
        st.session_state.pop('_channel_selection', None)
        st.session_state.pop('_selected_channel_pipe', None)
        st.session_state.pop('_pipe_snapshot', None)
        st.session_state.pop('focus_channel', None)
        st.session_state.pop('_last_focus', None)
        if snapshot is None:
            st.session_state.pop('_overview_snapshot', None)
            st.session_state.pop('_loaded_context', None)
            return
        st.session_state['_overview_snapshot'] = snapshot
        st.session_state['_refresh_requested'] = False

    row_focus = '_pending_focus' in st.session_state
    if row_focus:
        st.session_state['focus_channel'] = st.session_state.pop('_pending_focus')
    pipe_names = snapshot.get('pipes', pd.DataFrame()).get('pipe_name', pd.Series(dtype=str)).tolist()
    options = [None, *pipe_names[:1000]]
    if st.session_state.get('pipe_filter') not in options:
        st.session_state['pipe_filter'] = None
    pipe = st.selectbox('Pipe filter', options, key='pipe_filter', format_func=lambda name: 'All reported pipes' if name is None else name or 'Not reported')
    if len(pipe_names) > 1000:
        st.warning('Pipe list is limited to 1,000 entries.')
    if st.session_state.get('_active_pipe') != pipe:
        st.session_state['_active_pipe'] = pipe
        for key in ('focus_channel', '_selected_channel_pipe', '_channel_selection', 'channel_table', '_channel_snapshot'):
            st.session_state.pop(key, None)
    scope = snapshot['scope']
    pipe_snapshot = snapshot
    if pipe is not None:
        cached = st.session_state.get('_pipe_snapshot')
        cache_key = (pipe, scope.end)
        if cached is None or cached[0] != cache_key:
            pipe_scope = Scope(scope.source, scope.database, scope.schema, scope.table, scope.start, scope.end, scope.bucket_seconds, scope.channel, pipe)
            with st.spinner('Loading pipe-filtered activity'):
                pipe_snapshot = load_snapshot(connection, pipe_scope, include_messages)
            if pipe_snapshot is None:
                st.session_state.pop('_overview_snapshot', None)
                st.session_state.pop('_loaded_context', None)
                return
            st.session_state['_pipe_snapshot'] = (cache_key, pipe_snapshot)
        else:
            pipe_snapshot = cached[1]
    summary = snapshot['results']['summary'].iloc[0].to_dict() if not snapshot['results']['summary'].empty else {}
    focus = st.text_input('Focus channel', key='focus_channel', placeholder='All channels', help='Click a channel row below, or enter an exact channel name. Clear to return to the whole table.')
    if len(focus) > 1024:
        st.error('Channel name is too long.')
        return
    if st.session_state.get('_last_focus') != focus:
        if not row_focus:
            st.session_state['_selected_channel_pipe'] = None
        st.session_state['_last_focus'] = focus
    selected_pipe = st.session_state.get('_selected_channel_pipe') if focus else None
    detail_pipe = selected_pipe if selected_pipe is not None else pipe
    details = pipe_snapshot
    if focus and focus != pipe_snapshot['scope'].channel:
        key = (focus, detail_pipe, snapshot['scope'].end)
        cached = st.session_state.get('_channel_snapshot')
        if cached is None or cached[0] != key:
            scope = snapshot['scope']
            detail_scope = Scope(scope.source, scope.database, scope.schema, scope.table, scope.start, scope.end, scope.bucket_seconds, focus, detail_pipe)
            with st.spinner('Loading channel detail'):
                details = load_snapshot(connection, detail_scope, include_messages)
            if details is None:
                st.session_state.pop('_overview_snapshot', None)
                st.session_state.pop('_channel_snapshot', None)
                st.session_state.pop('_loaded_context', None)
                return
            st.session_state['_channel_snapshot'] = (key, details)
        else:
            details = cached[1]
    results = render_snapshot(details, focus or channel)
    if details['scope'].pipe is not None:
        st.caption(f'Reported pipe: {details["scope"].pipe or "Not reported"}')
    render_channels(pipe_snapshot)
    render_errors(results, include_messages)
    with st.expander('Metric definitions and limitations'):
        st.caption(f'Pipe attribution: {display(summary.get("pipe_commit_events"))} / {display(summary.get("commit_events"))} commit events; {display(summary.get("pipe_latency_samples"))} / {display(summary.get("measured_samples"))} latency measurements. Missing pipe identity is retained as Not reported.')
        st.caption('Channel sort: rejected rows, error events, then p95 latency descending. Select a row to investigate its channel/pipe pair. Channel types are not inferred from names.')
        st.markdown('''
- Error rate is rejected rows divided by parsed rows. Zero or inconsistent denominators produce **N/A**.
- Latency is recorded server-side ingestion processing, not source-to-query latency; p50/p95 are approximate sample percentiles, not row-weighted.
- Last recorded commit is limited to the selected window. Missing telemetry and gaps do not prove an outage or healthy ingestion.
- Partial boundary intervals are shaded and excluded from rate metrics. Complete intervals can still receive late telemetry.
- Chart gaps mean no returned measurement, not zero. Latency coverage counts measurements versus recorded latency events, not all source requests.
- Channel names are raw telemetry identifiers. The app does not infer Elastic versus Named mode from a UUID or merge distinct identifiers into one ELASTIC row.
- Bytes, backlog, source lag, and active-channel counts are not derived from these events.
- A table refresh runs six sequential read-only queries, including pipe choices; pipe or channel drill-down runs five more. No partial results are shown on query failure.
- Snapshots are retained only in this viewer's session, until the target/settings change, a refresh fails, or the session ends. Auto-refresh is optional and incurs query costs.
''')


def tables_panel(connection, scope, context, submitted, auto_refresh):
    st.subheader('Streaming tables')
    search = st.text_input('Search recorded table names', key='table_search', max_chars=255,
                           placeholder='Search tables', label_visibility='collapsed', width=320,
                           help='Filter recorded table names in the selected event source and window.')
    if st.session_state.get('_table_search') != search:
        st.session_state['_table_search'] = search
        st.session_state['table_page'] = 0
        for state_key in list(st.session_state):
            if state_key.startswith('tables_'):
                st.session_state.pop(state_key, None)
    page = int(st.session_state.get('table_page', 0))
    key = (context, search, page)
    snapshot = st.session_state.get('_tables_snapshot')
    due = auto_refresh and snapshot is not None and (datetime.now(timezone.utc) - snapshot['scope'].end).total_seconds() >= 30
    if submitted or snapshot is None or snapshot['key'] != key or due:
        if not submitted and st.session_state.get('_loaded_context') != context:
            st.info('Select Load / refresh to query recorded streaming activity.')
            return
        results = {}
        if snapshot is not None and snapshot['key'][0] == context and not submitted and not due:
            scope = snapshot['scope']
        try:
            for stage, (sql, params) in overview_queries(scope, search, page).items():
                frame = connection.query(sql, params=params, ttl=0, timeout=30)
                frame.columns = [column.lower() for column in frame.columns]
                results[stage] = frame
        except Exception as error:
            st.session_state.pop('_tables_snapshot', None)
            st.session_state.pop('_loaded_context', None)
            st.error('Table activity could not be loaded. No old or partial totals are displayed.')
            st.caption(safe_diagnostic(error, stage))
            return
        snapshot = {'scope': scope, 'results': results, 'key': key}
        st.session_state['_tables_snapshot'] = snapshot
    scope = snapshot['scope']
    st.caption(f'{scope.start:%H:%M}–{scope.end:%H:%M} UTC · Recorded activity')
    summary = snapshot['results']['summary'].iloc[0].to_dict() if not snapshot['results']['summary'].empty else {}
    state = collection_state(summary)
    if state:
        st.warning(state)
    with st.container(horizontal=True):
        st.metric('Rows ingested', headline(summary.get('rows_ingested')), border=True)
        st.metric('Avg rows / sec', headline(number(summary.get('rows_ingested')) / (scope.end - scope.start).total_seconds() if number(summary.get('rows_ingested')) is not None else None, decimals=2), border=True)
        st.metric('Rejected rows', headline(summary.get('errors')), border=True)
        st.metric('Row error rate', display(error_rate(summary.get('errors'), summary.get('rows_parsed')), '%', 2), border=True)
        st.metric('p95 processing time', headline(summary.get('p95_latency_ms'), latency=True), border=True)
    with st.expander('Coverage and metric details'):
        st.caption(f'Source: {scope.source} | {utc_text(scope.start)} to {utc_text(scope.end)}')
        st.caption('Tables with recorded activity in this event source and window, not an inventory of all configured pipelines. Database-level destinations elsewhere are not included.')
        st.caption('Summary covers the search/filter scope, not just this page. Rates average the entire window; latency is server-side processing, not source-to-query time.')
        if search:
            st.caption('Table-name search excludes events without a table name; it cannot assess their attribution coverage.')
    if number(summary.get('unattributed_events')):
        st.warning(f'{display(summary["unattributed_events"])} events lack a full target identity. They are included in summary metrics but cannot appear as a target table.')
    frame = snapshot['results']['tables'].copy()
    if frame.empty:
        st.info('No table activity returned on this page. Missing telemetry does not establish zero ingestion or pipeline health.')
        if page and st.button('Return to first page'):
            st.session_state['table_page'] = 0
            st.rerun(scope='fragment')
        return
    total = int(frame['total_tables'].iloc[0])
    frame['target_table'] = [ '.'.join(sql_identifier(value) for value in row) for row in frame[['database_name', 'schema_name', 'table_name']].itertuples(index=False, name=None)]
    frame = channel_frame(frame, scope)
    shown = frame[['target_table', 'rows_ingested', 'rows_per_second', 'errors', 'error_rate', 'p95_latency_ms', 'latest_commit', 'error_events']]
    event = st.dataframe(shown, hide_index=True, on_select='rerun', selection_mode='single-row', key=f'tables_{page}', column_config={
        'target_table': st.column_config.TextColumn('Target table'),
        'rows_ingested': st.column_config.NumberColumn('Ingested rows', format='localized'),
        'rows_per_second': st.column_config.NumberColumn('Avg rows / sec', format='%.2f'),
        'errors': st.column_config.NumberColumn('Rejected rows', format='localized'),
        'error_rate': st.column_config.NumberColumn('Rejected %', format='%.2f'),
        'p95_latency_ms': st.column_config.NumberColumn('p95 (ms)', format='%.0f'),
        'latest_commit': st.column_config.DatetimeColumn('Last recorded commit (UTC)'),
        'error_events': st.column_config.NumberColumn('Error events'),
    })
    st.caption(f'{page * 100 + 1}–{page * 100 + len(frame)} of {total} tables. Select a row to open table metrics and channels.')
    if event.selection.rows:
        row = frame.iloc[event.selection.rows[0]]
        st.session_state['_pending_target'] = (row['database_name'], row['schema_name'], row['table_name'])
        st.session_state['_drill_end'] = scope.end
        st.rerun()
    with st.container(horizontal=True):
        if st.button('Previous page', disabled=page == 0):
            st.session_state['table_page'] = page - 1
            st.rerun(scope='fragment')
        if st.button('Next page', disabled=(page + 1) * 100 >= total):
            st.session_state['table_page'] = page + 1
            st.rerun(scope='fragment')


st.set_page_config(page_title='Snowpipe Streaming monitoring', layout='wide')
st.subheader('Snowpipe Streaming monitoring')
database_component = st.components.v2.component(
    'streaming_database_search', html=DATABASE_SEARCH_HTML, css=DATABASE_SEARCH_CSS, js=DATABASE_SEARCH_JS,
)

try:
    connection = st.connection('snowflake')
except Exception as error:
    st.error('Connection unavailable. Check the Streamlit runtime and execution-role configuration. See README.')
    st.caption(safe_diagnostic(error, 'connection'))
    st.stop()

with st.sidebar:
    st.subheader('Monitoring scope')
    database = database_search(connection, database_component)
    manual_target = st.session_state.get('manual_target', False)
    reset_children('_target_mode', manual_target, ('schema', 'table', 'channel'))
    reset_children('_database', database, ('schema', 'table', 'channel'))
    pending = st.session_state.pop('_pending_target', None)
    if pending is not None:
        st.session_state['_selected_target'] = pending if pending[2] else None
        if not pending[2]:
            st.session_state['table'] = ''
        st.session_state['_refresh_requested'] = True
        for key in list(st.session_state):
            if key.startswith('tables_'):
                st.session_state.pop(key, None)
        for key in ('_overview_snapshot', '_channel_snapshot', '_pipe_snapshot', 'focus_channel', 'pipe_filter', '_active_pipe', '_selected_channel_pipe', '_last_focus', 'channel_table', '_channel_selection'):
            st.session_state.pop(key, None)
    st.caption('Leave database and schema empty to browse activity in the account event destination.')
    if manual_target:
        schema = st.text_input('Schema', key='schema', disabled=not database)
    else:
        schemas = object_names(discover(connection, 'schemas', database)) if database else []
        schema = st.selectbox('Schema', schemas, index=None, key='schema', disabled=not schemas, placeholder='All schemas')
        if database and not schemas:
            st.caption('No schemas listed. Check the database name or enter names under Advanced.')

    if st.session_state.get('_navigation_scope') != (database, schema, manual_target) and pending is None:
        st.session_state['_selected_target'] = None
    st.session_state['_navigation_scope'] = (database, schema, manual_target)
    reset_children('_schema', (database, schema), ('table', 'channel'))
    if manual_target:
        table = st.text_input('Table', key='table', disabled=not (database and schema))
    else:
        table = ''
    reset_children('_table', (database, schema, table), ('channel',))

    source = ''
    if database:
        database_destination = discover(connection, 'event destination', database)
        source = suggested_source([], database_destination)
        if database_destination is not None and not any(row.get('value') for row in database_destination):
            source = suggested_source(discover(connection, 'event destination'), database_destination)
    else:
        source = suggested_source(discover(connection, 'event destination'), [])

    time_range = st.selectbox('Time range', list(RANGES))
    auto_refresh = st.checkbox('Auto-refresh every 30 seconds', value=False, help='Off by default. Refreshes the active view every 30 seconds after loading; queries incur costs and slow queries can delay refreshes.')
    with st.expander('Advanced'):
        st.checkbox('Enter schema and table manually', key='manual_target', help='Optional direct table lookup. Use exact stored names; no telemetry is inferred when activity is missing.')
        override_source = st.checkbox('Override event source', key='override_source', help='Use a custom governed view or a known destination when automatic detection is unavailable.')
        if override_source:
            source = st.text_input('Event table or view', key='manual_source', placeholder='DATABASE.SCHEMA.TABLE_OR_VIEW')
        channel = st.text_input('Channel (optional)', key='channel')
        include_messages = st.checkbox('Show error messages', value=False, help='Messages can contain customer data. Leave hidden when presenting or sharing screenshots.')
        if st.button('Refresh metadata'):
            st.session_state.pop('_metadata', None)
            st.rerun()

    if source:
        st.caption(f'Monitoring source: `{source}`' + (' (manual override)' if override_source else ' (automatically detected)'))
    else:
        st.info('Could not determine the event source. Set an override under Advanced.')
    submitted = st.button('Load / refresh', icon=':material/refresh:', disabled=not source)

if st.session_state.get('_view_version') != 'tables_channels_v1':
    st.session_state.pop('_loaded_context', None)
    for key in ('_overview_snapshot', '_tables_snapshot', '_channel_snapshot', '_pipe_snapshot'):
        st.session_state.pop(key, None)
    st.session_state['_view_version'] = 'tables_channels_v1'
selected_target = st.session_state.get('_selected_target')
target_database, target_schema, target_table = selected_target or (database or '', schema or '', table or '')
context = (source, target_database, target_schema, target_table, time_range, channel if target_table else '', include_messages)
overview_context = (source, database or '', schema or '', time_range, include_messages)
if st.session_state.get('_overview_context') != overview_context:
    st.session_state['_overview_context'] = overview_context
    st.session_state['table_page'] = 0
    for key in list(st.session_state):
        if key.startswith('tables_'):
            st.session_state.pop(key, None)
if st.session_state.get('_loaded_context') != context:
    for key in ('_overview_snapshot', '_channel_snapshot', '_pipe_snapshot', 'channel_table', '_channel_selection', 'focus_channel', '_pending_focus', '_selected_channel_pipe', '_active_pipe', 'pipe_filter', '_last_focus', '_loaded_context'):
        st.session_state.pop(key, None)
if submitted or st.session_state.get('_refresh_requested'):
    try:
        make_scope(source, target_database, target_schema, target_table, time_range, channel if target_table else '')
    except ValueError as error:
        st.error(str(error))
        st.stop()
    st.session_state['_loaded_context'] = context
    st.session_state['_refresh_requested'] = True
    if not target_table:
        for key in list(st.session_state):
            if key.startswith('tables_'):
                st.session_state.pop(key, None)
if st.session_state.get('_loaded_context') != context:
    st.info('Select Load / refresh to browse recorded streaming tables. No target table selection is required.')
    st.stop()


@st.fragment(run_every=30 if auto_refresh else None)
def live_dashboard():
    refresh_requested = st.session_state.pop('_refresh_requested', False)
    if target_table:
        dashboard_panel(connection, context, time_range, include_messages, refresh_requested, auto_refresh)
    else:
        scope = make_scope(source, target_database, target_schema, '', time_range)
        tables_panel(connection, scope, context, refresh_requested, auto_refresh)


live_dashboard()
