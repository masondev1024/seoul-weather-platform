# Forecast quality evidence architecture

## Implementation status (2026-08-23)

The internal R2/Iceberg implementation now exists as a paused-by-default daily
DAG and a one-KST-date manual backfill DAG. It evaluates the bounded seven-day
repair window with `observation-truth-policy/v2-internal`,
`forecast-vintage-cutoff/v1`, `metric-evidence-gate/v1`, and
`pop-threshold-0.5/v1`. The checked-in Compose schedule remains empty, so no
quality DAG has been unpaused, triggered, or allowed to write R2/Iceberg.

The detailed executable design and rollout plan are
[`2026-08-22-weather-forecast-quality-gold-design.md`](../superpowers/specs/2026-08-22-weather-forecast-quality-gold-design.md)
and
[`2026-08-23-weather-forecast-quality-gold.md`](../superpowers/plans/2026-08-23-weather-forecast-quality-gold.md).
The operational activation and rollback steps are in
[`weather-forecast-quality-runbook.md`](../operations/weather-forecast-quality-runbook.md).

## What this slice proves

The repository can already compare the latest short-range forecast issue with the previous issue. That is revision analysis, not forecast accuracy. This slice adds a separate, deterministic contract for comparing forecast vintages with observation truth without claiming that synthetic fixtures describe current Seoul weather performance.

The executable reference path is:

```text
80-grid reference universe
  + ForecastVintage(issued_at, valid_at, value)
  + ObservationTruth(observed_at, truth_as_of, evaluation_as_of)
  -> point-in-time vintage and truth selection
  -> matched row scores
  -> cohort metrics and calibration
  -> AI evidence envelope
```

It performs no API call, Docker operation, scheduler change, Trino query, R2/D1 write, or Worker deployment.

## Canonical grains and time axes

Milestone 1 is grid-only. A forecast identity is:

```text
product_family × grid_id(nx, ny) × variable × issued_at × valid_at
```

An observation identity is:

```text
truth_source × truth_revision × grid_id(nx, ny) × variable × observed_at
```

The canonical universe is the 80 unique `seoul_bbox` rows in `dags/domains/weather/config/seoul_kma_grids.csv`. The evaluator accepts only a validated `GridUniverse` whose exact IDs, coordinates, count, scope, and population revision match that versioned artifact; 79, 81, or ad hoc 80-grid inputs fail closed. A deliberate grid change therefore requires an explicit population-revision and contract update. The grid universe must not be mixed with the 427 administrative-place mapping. A future place adapter must publish separate cohorts and population revision.

`issued_at` is when a forecast was released. `valid_at` is the target time. `collected_at` is lineage/freshness evidence and is never used to assign a forecast vintage.

## Point-in-time policies

`forecast-vintage-cutoff/v1` evaluates what was knowable at three fixed cutoffs. For horizon `H` in 72, 48, and 24 hours, the evaluator selects the greatest `issued_at` inside the inclusive window:

```text
[valid_at - H - 3 hours, valid_at - H]
```

The three-hour tolerance matches the short-range issue cadence. If the requested window is empty, the output records a gap. It never substitutes D-2 for D-3 or D-1 for D-2.

`observation-truth-policy/v2-internal` requires an explicit `evaluation_as_of`.
Revisions or collections after that instant are invisible to the run. The latest
visible revision is selected per truth source, grid, variable, and observation
time; conflicting values at the selected revision fail closed. The KMA endpoint
does not provide a provider-final revision timestamp, so every eligible
near-real-time observation remains explicitly `provisional`: it may support
internal analysis but makes the cohort `degraded`, never a final public claim.

These policies prevent future data leakage in model evaluation and make backtests reproducible.

## Metrics and evidence gates

Metric families remain separate:

- Continuous temperature: MAE, RMSE, and signed bias. Lower MAE/RMSE is better; zero bias is the target.
- Precipitation probability: Brier score plus fixed reliability bins. Lower Brier score is better.
- Thresholded precipitation occurrence: TP, FP, TN, FN, precision, recall, F1, accuracy, and positive prevalence. This diagnostic must not replace the probability score.
- Categorical precipitation occurrence: the same confusion-matrix family is available as a first-class `occurrence`/`none` contract, independent of POP thresholding.

`pop-threshold-0.5/v1` fixes the POP occurrence threshold at 0.5 while the
ten-bin calibration output remains separate. Empty bins stay visible with zero
count and null means. This avoids the common error of hiding unsupported
probability ranges.

`metric-evidence-gate/v1` requires at least 30 matches and at least 80% matched coverage per cohort. Both boundaries are inclusive. Below either gate, the cohort is `insufficient_evidence`; provisional truth makes an otherwise passing cohort `degraded`. A zero-match cohort remains a schema-valid diagnostic with empty metrics and explicit exclusion counts; it can never be presented as a scored result. Counts, denominator, coverage, gate version, truth revision, evaluation time, limitations, and per-metric unit/direction metadata travel with every emitted metric.

The AI claim helper refuses a universal “weather accuracy” answer. Consumers must name a cohort and metric. In particular, thresholded POP accuracy can look perfect while Brier score and calibration still reveal weak probability quality.

## Reference artifact

Generate or verify the synthetic reference artifact with:

```bash
python -m weather_quality.cli \
  --grid-csv dags/domains/weather/config/seoul_kma_grids.csv \
  --scenario contracts/weather-forecast-quality/fixtures/reference-scenario-v1.json \
  --output contracts/weather-forecast-quality/fixtures/reference-evidence-v1.json

python -m weather_quality.cli \
  --grid-csv dags/domains/weather/config/seoul_kma_grids.csv \
  --scenario contracts/weather-forecast-quality/fixtures/reference-scenario-v1.json \
  --output contracts/weather-forecast-quality/fixtures/reference-evidence-v1.json \
  --check
```

The fixture deliberately makes D-1 better than D-2 and D-3. It is a hand-checkable test oracle, not measured Seoul performance.

## Future lakehouse integration

### Bronze

- Keep forecast and observation payloads immutable with request/run manifests, payload hashes, source revision, collection time, and raw object lineage.
- Give each API family its own source contract. Do not add ultra-short or mid-term fields to `kma_vilage_fcst` rows by nullable accumulation.
- Observation-source licensing, redistribution rights, station/grid mapping, and quality flags must be approved before collection is enabled.

### Silver

- Preserve all forecast vintages at the canonical identity; do not reduce to the latest two issues before evaluation.
- Normalize observation revisions separately and retain `truth_as_of`, quality, and collection time.
- Partition forecasts by `day(valid_at)` and observations by `day(observed_at)`. Use bounded valid-time predicates before joining, then filter issue/revision time. This keeps Trino scans partition-pruned.
- Merge idempotently on canonical identities. Reject conflicting same-revision truth instead of last-write-wins.

### Gold

- Recompute only affected valid-time partitions plus an explicit late-truth repair window.
- Aggregate matched rows by product family, variable, vintage policy, spatial universe revision, and evaluation window.
- Publish metrics only after evidence gates pass; keep insufficient cohorts for diagnostics without exposing them as reliable claims.

### Serving and agents

- Serve the versioned evidence envelope, not raw unqualified metric values.
- Bind pagination/cache identity to `evidence_revision`, normalized query, and cohort identity.
- Require an explicit metric for agent claims and return limitations, sample count, coverage, truth state, and as-of time.
- Keep the current Worker/D1 publication path unchanged until a later deployment review approves a new product.

## Product-family seams

- `short_range`: fixture execution enabled; future production adapter can read the existing full-history Silver model rather than the two-issue serving mart.
- `ultra_short`: contract-only. It may reuse the KMA grid axis, but cadence, horizons, categories, and truth matching require a separate source adapter.
- `mid_term`: contract-only. KMA mid-term products are not assumed to use the 80-grid grain; a regional spatial adapter and separate cohorts are required.

No future adapter may silently coerce a regional or place product into the 80-grid cohort.

## Failure, recovery, and observability

- Duplicate identities: reject the batch and report the identity class.
- Missing vintage: record a label-specific gap; do not substitute.
- Future truth: exclude and count it for the current `evaluation_as_of`.
- Late final truth: rerun the affected valid-time partition and issue a new evidence revision.
- Stale/rejected truth: exclude it from the numerator and expose the reason in denominator diagnostics.
- Partial coverage: retain diagnostics; fail the AI evidence gate below 80%.
- Schema change: version the adapter and evidence contract; fail closed on incompatible kinds or units.

Production observability should track selected/missing vintage counts, visible/excluded truth revisions by reason, match coverage, evidence-state counts, metric drift by lead label, calibration-bin population, late-repair volume, partition scan bytes, Trino peak memory, and publication age. Trino filesystem cache can reduce repeated object-store reads, but it does not replace partition predicates or bounded incremental recomputation.
