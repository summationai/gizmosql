# Automatic materialization pilot

The optional `scripts/analytics_materializer.py` worker ranks recurring DuckLake queries from GizmoSQL session instrumentation. It builds results in a private DuckDB database and checks for source commits each poll. With query reuse enabled, GizmoSQL can serve a fresh artifact to an eligible Flight SQL request.

## Configure the pilot

Use one PostgreSQL-backed DuckLake catalog, one trusted GizmoSQL username, and an explicit table allowlist. Run one worker per state directory. Use a separate operating-system identity for the worker when GizmoSQL users can read files on the server. Give the worker read-only access to source data and a private state directory. Keep the instrumentation export private because it contains SQL text.

1. Install the worker dependencies from the repository root.

   ```sh
   python3 -m pip install -r scripts/requirements-analytics.txt
   ```

2. Use existing [session instrumentation](session_instrumentation.md) and export the query history as described below. The GizmoSQL CLI's direct query path produces eligible executions. Use the same catalog alias on the server and worker.

3. Create a private worker initialization file with the DuckLake extension and a restricted source attachment. A local DuckDB secret named `analytics_source` should point to the same PostgreSQL-backed metadata and data location as the server. Use `READ_ONLY` even if the worker’s credentials already enforce read-only access.

   ```sql
   LOAD postgres;
   LOAD ducklake;
   ATTACH 'ducklake:analytics_source' AS lake (READ_ONLY);
   ATTACH 'host=metadata-reader dbname=ducklake' AS lake_metadata (TYPE POSTGRES, READ_ONLY);
   ```

4. Run the worker.

   ```sh
   python3 scripts/analytics_materializer.py \
     --instrumentation-history /path/to/query-history.parquet \
     --source-init-sql /path/to/worker-source.sql \
     --source-table lake.main.orders \
     --metadata-catalog lake_metadata \
     --source-epoch orders-catalog-v1 \
     --user analytics_pilot \
     --state-dir /path/to/private-state
   ```

Repeat `--source-table` for other approved tables in the same catalog. Change `--source-epoch` after a restore or catalog replacement. Use `--once` for a single pass and a meaningful exit status if a build fails. For a small demo, pass `--min-runs 2 --min-duration-ms 0` and run the following twice as `analytics_pilot`.

```sql
SELECT SUM(total) AS gross FROM lake.main.orders WHERE region = 'EU';
```

For view queries, add `--source-view lake.main.my_view` for each approved view, including any intermediate views. Approve every leaf table with `--source-table`. View bodies can project and alias columns, use simple filters, and join approved relations. Use fully qualified names throughout. The worker rejects cycles and incomplete lineage.

If the metadata database contains several DuckLake schemas, set `--metadata-schema` to the schema of this catalog. Use an explicit PostgreSQL metadata attachment when DuckLake hides its internal metadata catalog. Pass the explicit alias via `--metadata-catalog`.

## Export query history

Use a read-only connection to the existing instrumentation store. Export completed executions for the relevant cluster, time range, and users. For a continuous worker, write a new Parquet file in the private directory and atomically rename it over the previous export. Run the export on your telemetry replica when available. These inputs exclude raw bind values and query profiles.

Run this example from a local DuckDB connection that can read the instrumentation tables. Replace the catalog, schema, and time window as needed. The default instrumentation catalog is `_gizmosql_instr`.

```sql
COPY (
    SELECT e.execution_id, e.execution_start_time, e.duration_ms, e.status,
           st.sql_text, st.flight_method, st.is_internal,
           s.session_id, s.instance_id, s.username,
           coalesce(e.bind_parameters, '[]') <> '[]' AS has_binds,
           e.materialization_hit
    FROM _gizmosql_instr.main.sql_executions e
    JOIN _gizmosql_instr.main.sql_statements st USING (statement_id)
    JOIN _gizmosql_instr.main.sessions s USING (session_id)
    WHERE e.status = 'success'
      AND e.execution_start_time >= current_timestamp - INTERVAL '1 hour'
      AND st.is_internal = false
      AND st.flight_method IN ('DoGetStatement', 'CreatePreparedStatement')
) TO '/path/to/query-history.next.parquet' (FORMAT PARQUET);
```

When the server version predates cache-hit instrumentation, export `false AS materialization_hit`. For the offline advisor, match `--history-days` to the actual export coverage. Use a PostgreSQL instrumentation replica or an existing offline export when available. For the default file-backed store on a running server, an administrator can run the inner `SELECT` over Flight SQL and write the result to Parquet on the client. Open the database file directly only after the writer releases its lock. Without instrumentation, the worker's `--query-log` and advisor's `--history` still accept legacy JSONL.

## Enable automatic query reuse

Use the stable DuckDB 1.5 channel for server reuse. Queue limits, session timeouts, and enabled DuckDB profiling use normal source execution. Built-in execution instrumentation records cache hits with `materialization_hit = true`. Pass `--publish` to the worker. It publishes bounded Arrow IPC files and exact-query manifests under the private state directory. Give the server process read access to that directory and put it outside DuckDB's allowed SQL filesystem paths. The server reads artifacts through C++ without attaching the worker database to DuckDB. Run the updated worker once to republish older manifests in format 2.

Use a read-only DuckLake attachment on the GizmoSQL serving process. Load all required extensions and attach the source and read-only PostgreSQL metadata under the same aliases as the worker, then seal the server configuration in its initialization SQL.

```sql
-- If the catalog uses local Parquet files, allow just its data directory.
SET allowed_directories = ['/path/to/source-data/'];
SET enable_external_access = false;
SET lock_configuration = true;
```

Point `GIZMOSQL_ANALYTICS_CACHE_CONFIG` to a JSON file that identifies the worker scope.

```json
{
  "state_dir": "/path/to/private-state",
  "catalog": "lake",
  "source_epoch": "orders-catalog-v1",
  "user": "analytics_pilot"
}
```

Use the same source, settings, catalog incarnation and username in both processes. Source writes can come from another process connected to the DuckLake catalog. The pilot serves the most frequently observed exact SQL spelling for each selected query, with integer or decimal output and an artifact size up to 8 MiB. Scope is direct, autocommit Flight SQL without a query timeout or catalog-scoped token. Other requests execute source SQL.

The server binds and authorizes the original query before checking the artifact. It checks function resolution and settings in the caller's session. It verifies the current DuckLake snapshot with a live snapshot ID read and a PostgreSQL lookup of the published snapshot's metadata row. Snapshot history stays on the worker. The metadata catalog alias and schema must match on the two attachments so the server can look up the same metadata row.

Any commit advances the snapshot and sends reads to the source until the worker verifies or rebuilds the artifact. An unavailable or invalid artifact also causes source execution. A hit also writes an `Analytics materialization hit` log event for diagnostics.

## Inspect builds and refreshes

Run a one-shot pass after the repeated queries. After it exits, open `private-state/artifacts.duckdb` in read-only mode and inspect the inventory. Each row’s `name` identifies the physical result table, `source_table` shows its first direct relation, and `snapshot_id` records the source snapshot used for the last build. The checked snapshot can advance independently when changes touched only unrelated objects.

```sql
SELECT m.name, m.source_table, m.snapshot_id AS built_at_snapshot,
       s.snapshot_id AS checked_through_snapshot, m.refreshed_at
FROM materializations m
JOIN materialization_refresh_state s USING (name);

SELECT dependent, source_relation, source_kind, source_id
FROM materialization_dependencies
ORDER BY dependent, source_relation;
```

Commit a change to `lake.main.orders` from another connection and run another pass. Its dependent artifacts should have newer build snapshots and updated rows. An unrelated commit advances only the checked snapshot. A rolled-back write leaves both unchanged. Each rebuild commits its result, lineage and snapshot records together. If refreshing fails, the worker keeps the last successful result in its private database for inspection and withdraws the served artifact.

## Pilot behavior

- Admission uses successful direct Flight SQL source executions. The supported SQL subset uses fully qualified allowlisted relations, named `COUNT`, `SUM`, `MIN`, or `MAX` results, optional simple grouping, column-to-literal filters and inner or left equality joins. The worker parses and reserializes accepted queries before executing them.
- Each pass uses up to 4,096 recent matching instrumentation executions. The defaults are a one-hour window, at least three source executions, at least 100 ms average source execution time, 30-second polls, eight materializations, and a 100,000-row limit per artifact. Candidates rank by total observed source duration. Remaining slots can retain existing artifacts based on recent cache hits, even after their source execution records age out.
- The worker records table and view edges using DuckLake object IDs and UUIDs. A change to a transitive source or its view definition rebuilds the affected artifacts. Missing, expired or unrecognized changeset history triggers a conservative rebuild when lineage can be resolved. Existing artifacts acquire lineage on their next successful refresh.
- The state directory stores result rows, admitted SQL definitions and a private key used to derive artifact IDs. Keep that directory and the source initialization file restricted. The worker runs in a separate DuckDB instance. Stop it or use one-shot runs when inspecting the artifact file because DuckDB holds the writer lock while the worker is active.

## Benchmark optimizations offline

The replay harness takes a local Parquet export and a trusted workload file. It loads the export into a scratch DuckDB instance and locks external access before running SQL from the workload. It does not connect to GizmoSQL or publish artifacts to the live materialization cache.

```sh
python3 scripts/analytics_replay.py \
  --export /path/to/cube-slice.parquet \
  --workload scripts/examples/analytics_replay_quality.json \
  --reports /path/to/private-replay-reports
```

Use the export's snapshot ID in the workload. `baseline` is a list of original `SELECT` queries over `data`. `cases` supplies named parameter values, and each query's `bind` lists them in SQL placeholder order. Add a candidate using one of these forms:

- `sort_by` builds a sorted copy and reruns the baseline against it.
- `build_sql` builds a derived table from `source`. Candidate queries read the derived table as `data`.
- `queries` alone runs a rewrite against the original data. A query's `outputs` maps each logical result ID to its projected columns. This lets a single batched query match multiple baseline results.

The harness alternates baseline and candidate executions. It checks column types, labels, duplicate counts, and values on every run. Set `"ordered": true` when row order matters. Reports record the export checksum, median and p90 latency, build time, and the number of reads needed to recover the build cost at the observed median savings. `faster_on_replay` requires both median and p90 improvement in every case. Results describe the supplied cases and in-process DuckDB timings. Validate candidate performance on Flight SQL and verify live snapshot rules separately before using one in production.

### Consume replay results over HTTP

```sh
python3 scripts/analytics_replay_api.py --reports /path/to/private-replay-reports
```

The API listens on loopback at port 8765 by default:

- `GET /v1/recommendations` returns recommendations from the last completed advisor run, with its `run_id` and UTC `last_run_at`.
- `GET /v1/runs` lists completed runs.
- `GET /v1/runs/{run_id}` returns run details, rejection status, and benchmark evidence.
- `GET /v1/runs/{run_id}/candidates/{candidate_id}` returns one candidate's results.

Consumers can use those results to rank proposals or add their own SQL candidates to a workload file and rerun the harness. Use `--include-workload` when trusted local consumers need the SQL and parameters to reproduce a run. Reports omit query results and workspaces by default. Keep the report directory private. The HTTP API serves saved reports and accepts no SQL or execution requests.

### Run the optimization advisor

Run the advisor as an offline job with a Parquet export, a GizmoSQL instrumentation export, and an explicit mapping from the logical relation in the history to the local export. Claude Code must be installed and signed in. The advisor sends only aggregate workload and benchmark statistics to the configured model, with agent tools disabled.

```sh
python3 scripts/analytics_advisor.py \
  --export /path/to/cube-slice.parquet --snapshot orders-cube-archive-v1 \
  --instrumentation-history /path/to/query-history.parquet \
  --relation main.orders_cube --history-days 30 \
  --changes /path/to/source-changes.json \
  --as-of 2026-01-04 --as-of 2025-06-30 \
  --reports /path/to/private-replay-reports
```

The advisor counts successful direct and prepared executions within the supplied coverage window. It uses session and instance IDs to identify compatible batches and excludes cache hits from source-cost estimates. The advisor discovers recurring daily `SUM` queries that group by `STRFTIME(date, '%Y-%m-%d')`, including queries with one string equality filter. It tests grouping by the native `DATE` and applying the format after aggregation. When no daily SUM shape recurs, it checks for `COUNT(*) - COUNT(column)` and `COUNT(*)` queries sharing a date range. Relative dates use the explicit archive anchors for replay.

The agent ranks bounded templates for sorting, a daily rollup, and fusing compatible checks. Local code generates the candidate SQL. The offline harness checks complete results, median and p90 latency, and build cost. The agent reviews that evidence together with observed volume, source execution latency, and churn. The deterministic gate requires replay improvement and a minimum estimated daily payoff before model advice can mark a candidate worth following up.

Provide optional change evidence as JSON with `relation`, timezone-aware `start` and `end`, `commits`, and `basis` of `observed` or `archived_proxy`. The estimate charges a full rebuild per changed-table commit and amortizes the initial artifact build over 30 days. Missing or archived churn cannot qualify a persistent artifact when the payoff otherwise passes. Adjust the default 1,000 ms per day threshold with `--min-net-ms-per-day`. These estimates use sampled source timings and local replay ratios, so they are for prioritization.

Use `GET /v1/recommendations` for the latest conclusions and payoff estimates in the report directory. A replay without advisor output does not replace the current recommendations. The run and candidate endpoints retain verification and latency measurements for inspection.

After result verification, a daily SUM recommendation includes `rewrite.queries` with parameterized SQL, parameter types, and positional bind order. Bind the same `anchor` date wherever it appears in `bind`. The contract identifies the relation and archive snapshot used for verification. The API omits observed literal values from these templates.

A verified daily rollup includes a `view` contract with a stable name, source relation, column schema, backing-table SQL, view SQL, and parameterized query examples with output mappings. Agents can discover it in the latest recommendations and use the query template after the consumer installs the view in an authorized catalog. The backing table holds precomputed rows. The view gives callers a stable SQL name.

Use a contract marked `suggested` for further evaluation. A `deferred` contract keeps the benchmark evidence available for inspection. Consumers own installation, access control, and refresh. Check that the installed backing table covers the source snapshot before using it in a query. The advisor and HTTP API keep these contracts separate from live schema and query execution.
