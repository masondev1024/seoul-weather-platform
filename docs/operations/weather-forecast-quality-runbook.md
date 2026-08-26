# Weather Forecast Quality Gold Runbook

## Purpose and boundary

This runbook operates the internal forecast-quality products:

- `gold_weather_forecast_quality_grid_score`
- `gold_weather_forecast_quality_hourly`
- `gold_weather_forecast_quality_daily`

Their run-versioned histories and success manifest live only in the personal
R2-backed Iceberg catalog. They are **not** D1 tables, Worker routes, public
products, K-Skill artifacts, or inputs to `weather_serving_snapshot_refresh`.
The existing collection, transform, D1 export, and freshness DAGs continue
independently when quality work fails.

## Checked-in safety defaults

- `weather_forecast_quality_daily`: no checked-in schedule and paused on
  creation.
- `weather_forecast_quality_backfill`: no schedule and paused on creation.
- `ASK_SEOUL_WEATHER_QUALITY_DAG_SCHEDULE` is empty in
  `docker-compose.local.yml` and `.env.example`.
- Candidate build, manifest publication, and published-view build serialize via
  the existing Weather heavy pool with priority `10`.
- Each dbt task has a 15-minute execution bound and `TRINO_DBT_QUERY_MAX_RUN_TIME=15m`.
  A retry is allowed only through the shared dbt failure classifier, with a
  bounded exponential backoff. The DAG-run deadline is 45 minutes.

These settings prevent quality evaluation from becoming a second collection
pipeline or a route around the local Trino OOM guard. They do not grant
permission to change an already-running Airflow deployment.

## Data contract

The scheduled job evaluates the last seven complete KST dates, ending
yesterday. One run is pinned by:

- `evaluation_run_id` and `evaluation_as_of`;
- forecast Bronze `load_date` from window start minus four days through
  window end minus one day;
- canonical 80-grid, D-1/D-2/D-3 vintage windows;
- complete 80-grid / 640-row-hour KMA observation truth slots; and
- policy versions for truth, vintage, evidence, and POP threshold.

Candidate histories are invisible to the three product views until all
candidate dbt builds pass. Only then does `publish_quality_manifest` append an
idempotent `SUCCESS` record. A failed candidate can remain in Iceberg for
forensics but is not selected by published views.

## Preflight: read-only checks

Before any deployment or activation approval, run only secretless checks:

```bash
uv run --no-project --python /opt/homebrew/bin/python3.11 \
  --with pytest==9.0.3 --with jsonschema==4.26.0 --with PyYAML==6.0.2 \
  python -m pytest dags/domains/weather/tests/test_weather_forecast_quality_dags.py \
  dags/domains/weather/tests/test_weather_quality_runtime.py \
  dags/domains/weather/tests/test_weather_quality_publication.py \
  tests/forecast_quality dbt/domains/traffic_weather/tests/weather -q

DBT_LOG_PATH=/tmp/weather-quality-dbt/logs \
DBT_PACKAGES_INSTALL_PATH=/tmp/weather-quality-dbt/dbt_packages \
DBT_TARGET_PATH=/tmp/weather-quality-dbt/target \
dbt parse --project-dir dbt/domains/traffic_weather \
  --profiles-dir dbt/domains/traffic_weather --target ci --no-partial-parse
```

Render Compose configuration only; do not start or recreate services. Confirm
the quality schedule remains blank in every Airflow service.

## Separate activation approval gate

Activation requires a separate explicit approval that names:

1. target commit and affected Airflow services;
2. existing running or queued DAGs and the heavy-pool drain plan;
3. exact pause/drain/deploy/health/rollback sequence;
4. expected Trino/R2 write scope; and
5. a one-date shadow evaluation date.

Do not unpause the daily DAG merely because this code is merged. Do not set the
schedule until shadow evidence has been reviewed.

## Shadow run acceptance evidence

For a manually approved one-date backfill, use the exact confirmation token
`BACKFILL_ONE_KST_DATE`. A run is acceptable only when all of the following are
recorded without credentials or raw payloads:

- DAG/task states and elapsed time are successful and below their bounds;
- one 80-grid expected population is present for every valid hour and horizon;
- counts by `matched`, missing, invalid, and incompatible state reconcile;
- daily/hourly sufficient statistics reconcile to grid scores;
- `evidence_state`, truth revision counts, and provisional-truth limitation are
  visible;
- Trino scan/peak-memory evidence is available, or the missing measurement is
  stated explicitly; and
- no D1 publication, Worker change, or serving asset event occurred.

Coverage below 80%, fewer than 30 matched rows, or provisional truth is not
silently converted into a normal public accuracy claim. It remains an explicit
insufficient/degraded internal cohort.

## Failure and recovery

| Symptom | Action | Why |
|---|---|---|
| Trino timeout/OOM/queue deadline | Let the quality run fail; inspect pool/query evidence; do not increase concurrency | Serving and collection have higher operational priority. |
| 429/network transient | Shared classifier may retry within the bounded task and DAG deadlines | Retry is safe only for transient failure classes. |
| Duplicate grain, noncanonical grid, incompatible unit, reconciliation failure | Fail closed; fix data contract or source condition; do not retry as transport | A retry cannot correct semantic data. |
| Partial observation hour or missing vintage | Retain explicit diagnostic rows and evidence state; use the seven-day repair next run | Missing is evidence, not dry weather or a substitute vintage. |
| Need to repair an older day | Request approval and run exactly one KST date through manual backfill | Open-ended history scans are prohibited. |

Rollback is logical: pause the quality DAGs and remove the local schedule
override. Existing product views continue selecting their last successful
manifest rows; no serving rollback is needed because quality never published to
D1. Do not delete R2/Iceberg files during incident response without a separate
data-retention approval.
