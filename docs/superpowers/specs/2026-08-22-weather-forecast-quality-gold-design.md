# Weather forecast-quality Gold pipeline design

## Status and decision

This design adds an internal, reproducible forecast-quality data product to the
personal Seoul Weather lakehouse. It evaluates the existing short-range forecast
history against the hourly KMA observation Bronze table and stores analytical
products only in R2-backed Iceberg Gold.

The product is not a serving product. It must not be registered in the D1
catalog, exported to D1, exposed by the Worker, or added to the current public
serving asset. The initial release evaluates temperature and precipitation for
the D-1, D-2, and D-3 forecast vintages in a daily batch.

## Goals

1. Measure how the forecast for the same valid hour changed in usefulness from
   D-3 to D-2 to D-1.
2. Preserve grid-level forecast-versus-observation evidence so every aggregate
   can be reproduced and audited.
3. Produce hourly and daily Seoul-wide cohorts with explicit sample, coverage,
   exclusion, truth-revision, and evidence-state metadata.
4. Re-evaluate the previous KST day and a bounded seven-day repair window so
   late or revised observations are incorporated idempotently.
5. Keep Trino work partition-pruned, serialized, bounded, observable, and safe
   for the local 5 GiB Trino process.

## Non-goals

- No D1, Worker, chatbot, agent, or public serving publication.
- No claim that near-real-time KMA observations are provider-final truth.
- No ultra-short-range or mid-term forecast evaluation in version 1.
- No humidity, wind, snow-depth, or precipitation-amount scoring in version 1.
- No station interpolation, ASOS comparison, or historical observation adapter.
- No automatic full-history rebuild or unbounded dbt full refresh.
- No fabricated backfill for periods without complete observation coverage.

## Considered approaches

### A. SQL/dbt incremental evaluation in Trino — selected

Trino reads partition-bounded Iceberg forecast and observation data, dbt owns the
normalization and evaluation models, and Airflow orchestrates a daily selector.
This avoids materializing the evaluation population in the Airflow process and
fits the existing lakehouse, manifest, test, and run-metrics patterns. The main
risk is semantic drift between the existing Python quality kernel and SQL; a
fixture-backed parity test is therefore mandatory.

### B. Python quality-kernel batch

This directly reuses `weather_quality`, but it requires moving a growing data
population through the scheduler or a separate worker, implementing bulk
Iceberg writes, and managing process memory. It is retained for deterministic
contract fixtures and parity checking, not selected as the production engine.

### C. Hourly asset-triggered evaluation

This provides fresher metrics but increases Trino contention, produces metrics
before truth has settled, and complicates late-data repair. It conflicts with
the chosen reliability and cost goals and is not selected.

## Architecture and data flow

```text
Partitioned Iceberg Bronze short-range forecasts
             +
Iceberg Bronze hourly observations
             |
             v
silver_weather_quality_forecast_vintage
             +
silver_kma_observation_truth
             |
             v
silver_weather_forecast_observation_match
             |
             +-------------------------------+
             v                               v
run-versioned Iceberg Gold histories         |
             |                               |
             v                               |
quality publication manifest                 |
             |                               |
             v                               |
three latest-published Gold product views <--+
```

The production calculation is SQL-first. The Python kernel remains the semantic
oracle for contracts, selection boundaries, metric formulas, and deterministic
test fixtures.

## Input contracts

### Forecast input

The source is the publishable subset of `bronze_kma_vilage_fcst`, normalized by
the dedicated `silver_weather_quality_forecast_vintage` model. The existing
general `silver_kma_vilage_fcst` is deliberately not the physical source for
this daily job: live read-only verification on 2026-08-22 showed that relation
was unpartitioned, with 11,862,400 rows and 64,979,033 bytes across two files.
Scanning it every day would turn a logical seven-day predicate into a growing
R2 full-table read.

The Bronze source is partitioned by `load_date`. The quality model scans only
the load-date range capable of producing D-1/D-2/D-3 forecasts for the requested
seven valid dates, then writes a dedicated `day(valid_at)`-partitioned Silver
relation. This isolates the quality product without forcing a risky full rebuild
or repartitioning of the current serving Silver table. Version 1 maps:

| KMA category | Quality variable | Value kind | Unit |
|---|---|---|---|
| `TMP` | `temperature_air_2m` | continuous | `degC` |
| `POP` | `precipitation_occurrence` | probability | `1` after dividing 0–100 by 100 |
| `PTY` | `precipitation_occurrence_category` | categorical wet/dry | `category` |

Forecast rows must have a valid numeric or versioned categorical representation,
an aware `issued_at`, an aware `forecast_at`, a canonical Seoul grid, and a
source revision. Invalid values are retained as excluded audit rows; they never
become zero, dry, or a successful match.

### Observation input

The source is `bronze_kma_ultra_srt_ncst`. Only revision-scoped observation
slots that passed the exact 80-grid/640-category manifest gate are eligible.
Version 1 maps:

| KMA category | Quality variable | Derivation |
|---|---|---|
| `T1H` | `temperature_air_2m` | finite Celsius value |
| `PTY` + `RN1` | `precipitation_occurrence` | wet when `PTY != 0` or `RN1 > 0` |
| `PTY` + `RN1` | `precipitation_occurrence_category` | `wet` or `dry` from the same complete pair |

Missing or invalid `PTY`/`RN1` never means dry. The normalized truth grain is:

```text
(grid_id, observed_at, variable, truth_revision)
```

The latest revision visible at `evaluation_as_of` wins. All near-real-time
observations remain explicitly provisional because the endpoint provides no
provider-final revision timestamp. They may be used for this internal analysis,
but every affected cohort has `evidence_state=degraded` and a limitation stating
that provider finality is unconfirmed. A future historical adapter may replace
the selected truth and cause the bounded repair window to recompute the scores.

This production behavior is versioned separately from the six-hour live-claim
fixture policy. It does not weaken the AI-claim gate because no AI claim or
serving publication is part of this product.

## Vintage selection and temporal semantics

All timestamps are stored as UTC instants and KST is used only to derive the
evaluation date and scheduler boundaries. Forecast and truth match only when
`forecast_at = observed_at` and the grid and variable agree.

For each grid, variable, and valid hour, the selected vintage is the latest
forecast issued in the inclusive window:

```text
D-1: [valid_at - 27 hours, valid_at - 24 hours]
D-2: [valid_at - 51 hours, valid_at - 48 hours]
D-3: [valid_at - 75 hours, valid_at - 72 hours]
```

A missing window produces an explicit `missing_vintage` audit row. It is never
filled with a newer or older issue. When multiple rows share the winning issue,
source revision and source identity provide a deterministic tiebreaker. Any
conflicting semantic duplicate fails the run.

## Model contracts

### `silver_weather_quality_forecast_vintage`

- Materialization: incremental Iceberg merge.
- Source pruning: bounded `bronze_kma_vilage_fcst.load_date` predicates plus the
  existing publishable collection manifest.
- Partition: `day(valid_at)`.
- Grain: `(grid_id, valid_at, variable, issued_at, source_revision)`.
- Purpose: normalize only TMP, POP, and PTY into the versioned quality contract
  while preserving issue and source revisions.
- It does not modify or replace the existing serving-owned
  `silver_kma_vilage_fcst` relation.

### `silver_kma_observation_truth`

- Materialization: incremental Iceberg merge.
- Partition: `day(observed_at)`.
- Grain: `(grid_id, observed_at, variable, truth_revision)`.
- Purpose: normalize complete immutable observation revisions without losing
  provenance or provisional truth quality.
- Required lineage: source ID, revision, collection time, evaluation visibility,
  manifest run, and payload hash.

### `silver_weather_forecast_observation_match`

- Materialization: incremental Iceberg merge over a bounded KST date window.
- Partition: `day(valid_at)`.
- Grain: `(grid_id, valid_at, variable, vintage_label)`.
- Emits one auditable outcome per expected forecast series: `matched`,
  `missing_vintage`, `missing_truth`, `invalid_forecast`, `invalid_truth`, or
  `incompatible_contract`.
- Stores selected forecast issue/revision, selected truth revision/quality,
  values, continuous error components, probability error components, and
  categorical contingency components.
- Deterministic rows are updated when a selected revision changes; the model
  never appends duplicate successful scores for the same business key.

### Atomic analytical publication

Each Gold build writes run-versioned candidate rows carrying
`evaluation_run_id`, `evaluation_as_of`, and the evaluated KST date. Candidate
rows are invisible through the analytical product views until all three grains
pass their model and reconciliation tests. The final Airflow publication task
then appends one immutable `SUCCESS` record to the quality publication manifest.

The three product relations select only the newest successful run for each KST
evaluation date. A failed or timed-out run can therefore leave candidate files
without exposing a partially refreshed date. Weekly maintenance may remove
failed candidates and superseded successful candidates after a 14-day rollback
window. The publication manifest is internal Iceberg metadata and is never a D1
catalog entry.

### `gold_weather_forecast_quality_grid_score`

- Business-ready grid diagnostic view over the published run-versioned Iceberg
  history.
- Grain: `(grid_id, valid_at, variable, vintage_label)`.
- Contains the selected evidence and row-level score components required to
  reproduce aggregate metrics.
- Contains no raw API body, credential, signed URL, or unrestricted request
  parameter payload.

### `gold_weather_forecast_quality_hourly`

- Business-ready hourly view over the published run-versioned Iceberg history.
- Grain: `(valid_at, variable, vintage_label)`.
- Expected population is the canonical 80-grid Seoul universe.
- Publishes sample count, expected count, matched coverage, exclusion counts,
  evidence state, truth source/revision counts, and metric values.
- Coverage below 80%, sample count below 30, or a failed canonical-universe
  check yields `insufficient_evidence`; it cannot appear as a normal metric.

### `gold_weather_forecast_quality_daily`

- Business-ready daily view over the published run-versioned Iceberg history.
- Grain: `(evaluation_date_kst, variable, vintage_label)`.
- Aggregates directly from row-level sufficient statistics. It must not average
  hourly RMSE, ratios, F1, Brier scores, or calibration errors.
- Stores the same evidence metadata plus the observed valid-time range.
- Temperature metrics: MAE, RMSE, and signed bias.
- POP metrics: Brier score, ten-bin calibration sufficient statistics, and
  expected calibration error.
- Thresholded precipitation metrics: true/false positives/negatives, accuracy,
  precision, recall, F1, and positive prevalence. The v1 POP threshold is 0.5
  and is stored with a versioned policy identifier.

All metric rows declare metric family, unit, direction, policy versions, and
nullability. Undefined zero-denominator metrics remain null rather than zero.

## Incremental and reprocessing policy

The scheduled run evaluates the previous complete KST day and recomputes exactly
seven complete KST dates ending on that previous day. It never includes the
current partial KST date. Forecast Bronze reads require the bounded `load_date`
range needed by the D-3 lower cutoff; downstream quality Silver reads require
`day(valid_at)` bounds; observation reads require `day(observed_at)` bounds. A
model that omits any applicable bounded source predicate fails its contract
test.

The merge key is the declared model grain. Re-running the same window with the
same source revisions is a no-op. A changed selected revision updates the same
business key and all aggregates reconcile to the updated grid scores.

Dates older than seven days are processed only by the manual backfill DAG. A
backfill invocation accepts one KST date, uses the same selectors and tests, and
runs with `max_active_runs=1`. Multi-day or open-ended backfill input is rejected
before Trino work begins.

## Orchestration

### Scheduled DAG

- DAG ID: `weather_forecast_quality_daily`.
- Intended local schedule: `5 3 * * *` in `Asia/Seoul`.
- Checked-in public default: no schedule and paused on creation.
- Activation: local ignored runtime overlay after separate operational approval.
- `catchup=False`, `max_active_runs=1`, DAG-run timeout 20 minutes.
- All dbt run/test tasks use the shared `trino_weather_heavy` one-slot pool.
- The quality dbt session has a 15-minute Trino query run limit and the task
  process has an equal execution timeout. A timeout cancels the query and fails
  the run; partial aggregates are not marked publishable.
- The quality DAG writes the `SUCCESS` publication manifest record and emits its
  own internal Gold-ready asset only after all model and reconciliation tests
  pass. No existing serving asset is emitted.

At 03:05 KST, the hourly serving refresh that began at 03:00 has priority in the
same heavy lane. Quality work waits rather than overlaps. The bounded query limit
keeps it from occupying the lane across the 03:45 observation cycle. If the lane
is unavailable long enough to exceed the DAG deadline, the quality run fails and
the observation/serving pipelines continue independently.

### Manual backfill DAG

- DAG ID: `weather_forecast_quality_backfill`.
- No schedule; paused on creation.
- Requires an explicit single KST date and confirmation token.
- Uses the same one-slot pool, query limit, model contracts, and publication
  gate as the scheduled run.
- Never calls KMA and never mutates Raw or Bronze.

## Failure, recovery, and isolation

- Missing/partial observation slots produce excluded rows and insufficient
  evidence, not dry weather or fabricated truth.
- Contract conflicts, noncanonical grids, duplicate business keys, impossible
  metric bounds, or aggregate reconciliation errors fail closed.
- A failed quality run does not block forecast collection, observation
  collection, the existing Gold serving snapshot, D1 export, or the Worker.
- Retries are safe because every model is deterministic for source revisions,
  evaluation window, policy versions, and `evaluation_as_of`.
- The scheduled DAG retries only transient Trino/object-store failures with
  bounded exponential backoff. Data-contract failures are not retried.
- Seven-day repair is the automatic recovery path. Older repair requires a
  one-date manual backfill.
- Iceberg maintenance compacts these tables under the existing bounded weekly
  maintenance policy; quality processing never performs inline global compaction.

## Observability and operational evidence

Each run records:

- evaluation window and `evaluation_as_of`;
- input forecast, observation, matched, excluded, and output row counts;
- counts by exclusion reason and evidence state;
- selected truth revision count and provisional-cohort count;
- dbt/Trino wall time, retry count, and timeout state;
- per-model processed input bytes when available from Trino query metadata;
- query peak memory when available from Trino query metadata;
- policy versions and the internal Gold asset revision.

The host-side shadow and activation reports, rather than the Airflow container,
record Docker-level Trino peak memory, OOM state, and restart deltas.

Alerts are required for run failure, timeout, canonical grid drift, zero matched
rows when eligible input exists, coverage below 80%, aggregate reconciliation
failure, or three consecutive missing/insufficient daily partitions. A degraded
cohort caused solely by provisional truth is recorded as a limitation, not an
operational failure.

## Data quality and test strategy

### Unit and contract tests

- Forecast category/value mapping and invalid-value handling.
- Observation T1H and PTY/RN1 truth mapping.
- Inclusive D-1/D-2/D-3 boundaries and forbidden substitution.
- Revision visibility, deterministic tiebreaking, and conflict rejection.
- Continuous, probability, calibration, and categorical hand calculations.
- Evidence-state sample and coverage boundaries.
- UTC/KST boundary behavior, including month/year and DST-independent KST dates.

### dbt data tests

- Unique and non-null declared grains for every model.
- Exactly the canonical 80-grid population for complete hourly cohorts.
- Accepted variables, vintages, states, units, truth qualities, and policy IDs.
- Metric range, nullability, and zero-denominator rules.
- Hourly and daily metrics reconcile to grid-level sufficient statistics.
- Source and target partition bounds remain inside the requested repair window.
- The quality forecast model reads only bounded Bronze `load_date` partitions
  and never uses the unpartitioned serving Silver as its physical source.
- No internal quality model carries D1 serving metadata or appears in a D1
  selector.

### Integration and adversarial tests

- Deterministic synthetic 80-grid fixture produces SQL metrics equal to the
  Python quality kernel.
- One missing grid, one duplicate, one revised truth, missing PTY/RN1, missing
  D-3, late collection, and conflicting revisions all exercise fail/degrade
  behavior.
- A repeated run is idempotent; a revised truth changes only affected keys and
  reconciled aggregates.
- A forced slow query hits the timeout without leaving a running Trino query or
  a publishable partial partition.
- DagBag remains import-clean with secretless defaults and both new DAGs paused.
- Existing forecast, observation, serving, D1, and Worker regression suites pass.

## Rollout gates

1. Implement contracts and failing tests before models.
2. Pass Python/SQL parity and the full repository suite.
3. Render the local Compose/DagBag with the quality schedule still disabled.
4. Run a read-only query plan and verify both source partition bounds.
5. Run one manual single-date shadow evaluation without publishing an asset.
6. Verify row counts, metric reconciliation, Trino peak memory, scan size, and
   absence of serving/D1 changes.
7. Present the shadow evidence and exact operational diff for separate approval.
8. Only then enable the ignored local schedule overlay and unpause the scheduled
   DAG.

## Acceptance criteria

- Three analytical Gold product views exist with the declared grains, backed by
  run-versioned R2/Iceberg histories and a success publication manifest, with no
  D1 metadata.
- A dedicated partitioned quality-forecast Silver model is sourced from bounded
  publishable Bronze partitions without rebuilding the current serving Silver.
- The daily model compares D-1, D-2, and D-3 for temperature and precipitation.
- Every aggregate is reproducible from the grid-score product.
- The last seven KST dates are idempotently repairable and older dates require a
  one-day manual backfill.
- All incomplete, sparse, provisional, revised, and conflicting data states are
  explicit and tested.
- Both new DAGs are inert in public configuration and isolated from serving.
- Quality work is serialized through the existing Weather heavy pool and bounded
  by the declared time and partition limits.
- Full repository, dbt, DagBag, provenance, secret, and runtime-contract checks
  pass before any operational activation.
