# KMA observation pipeline predeployment plan

## Decision state

`IMPLEMENTED AND REPOSITORY-VERIFIED / NOT DEPLOYED OR ACTIVATED`

The repository now contains the `getUltraSrtNcst` adapter, bounded HTTP policy,
shared physical-attempt ledger, immutable 80-grid Raw landing, dedicated Bronze
table, and paused-by-default hourly DAG. Compose examples keep the rollout guard
false and schedule empty. No Docker/Airflow service was recreated, no DAG was
unpaused or triggered, and no live KMA, R2, Iceberg, D1, or Worker mutation was
performed in this implementation phase.

## Proposed data flow

```text
KMA getUltraSrtNcst (80 grids, one hourly slot)
  -> immutable per-grid raw response + slot request ledger in personal R2
  -> exact 80-grid / 640-category manifest gate
  -> dedicated Iceberg Bronze observation table
  -> Silver observation revisions (temperature + precipitation occurrence)
  -> point-in-time D-3/D-2/D-1 quality cohorts
  -> evidence-gated Gold product
  -> D1/Worker only when publication gates pass
```

Forecast and observation remain separate through Bronze. A join happens only in
the bounded quality evaluation layer on grid, variable, and valid/observed time.

## Proposed orchestration

### DAG inventory

| DAG | Default state | Proposed schedule | Responsibility |
|---|---|---|---|
| `weather_ultra_srt_ncst_bronze` | implemented; `schedule=None`; paused | `45 * * * *`, `Asia/Seoul` after approval | reserve quota, fetch missing grids, land Raw, validate 80-grid manifest, load/verify Bronze |
| `weather_forecast_quality_transform` | paused | asset-triggered after complete observation Bronze and eligible forecast partitions | build Silver truth revisions and affected quality cohorts |
| `weather_forecast_quality_backfill` | paused/manual-only | none | quota-aware bounded historical repair by explicit slot range |

Only `weather_ultra_srt_ncst_bronze` is implemented in the current milestone.
The quality transform and backfill rows describe later independently approved
milestones; they are not created, scheduled, or activated by the hourly Bronze
work.

Minute 45 gives the hourly observation a publication buffer and reduces overlap
with the existing short-range forecast collection at minute 20 on its eight issue
hours. The 15-minute normal target then ends around the next hour boundary, while
the 40-minute hard deadline still leaves 20 minutes before the following scheduled
cycle. The schedule remains configurable and must be re-evaluated from at least a
week of source-latency telemetry before unpause.

The six task execution ceilings are 1 + 1 + 20 + 6 + 5 + 1 = 34 minutes. This
leaves six minutes inside the 40-minute DAG deadline for scheduling transitions
and bounded pool wait. Exhausting that slack fails the cycle closed; the durable
Raw checkpoint lets the next approved run request only missing grids.

### Atomic slot contract

A slot is publishable only when:

- the canonical grid revision is exactly the pinned Seoul 80-grid universe;
- all 80 grid requests have success code `00` for the requested slot;
- every grid has exactly the eight versioned categories;
- payload hashes and raw-object write acknowledgements are present;
- the request ledger and manifest totals are 80 responses and 640 category rows.

Partial slots remain `incomplete` and trigger missing-grid reconciliation. They do
not produce Bronze completeness, Silver truth, quality metrics, or D1 output.

### Retry and quota policy

- The schema-v1 SQLite ledger reserves every physical request attempt atomically
  before network I/O. Forecast internal retries and observation retries use the
  same KST-day ledger.
- Nominal logical traffic is 2,560 requests/day: 1,920 observation requests
  (`80 × 24`) plus 640 short-range forecast requests (`80 × 8`). Retries make
  physical traffic higher, so the enforced default ceiling is 7,500 attempts,
  below the provider's 10,000/day development quota.
- Retry only bounded transport timeout, connection reset, HTTP 5xx, and explicitly
  classified per-second throttle responses with jittered backoff.
- Do not automatically retry invalid credential, malformed payload, context
  mismatch, duplicate category, or daily quota exhaustion.
- Reconciliation requests only missing grids. Never replay all 80 grids because
  one grid failed.
- Backfill and any additional KMA collection cannot be enabled from the same
  quota pool until their worst-case budget is approved.

The shared `kma_api_requests` Airflow pool has one slot. If forecast and
observation schedules or retries overlap, only one KMA landing task can issue
requests. Within observation, the default one-request-per-second limiter,
deadline-aware exponential backoff, and cycle-wide throttle circuit bound 429
amplification. A success does not erase earlier throttle evidence in that cycle.

## Storage and table contracts

### Raw R2

Use a dedicated immutable prefix, for example:

```text
raw/weather_observation/kma_ultra_srt_ncst/
  observed_date=YYYY-MM-DD/observed_hour=HH/
  nx=NN/ny=NN/<payload_sha256>.json
```

The complete slot manifest is written only after all grid objects are durable.
Raw objects are audit/recovery evidence and are not the recurring Trino query
surface.

### Iceberg Bronze

- Dedicated table; never nullable-append observation columns to forecast Bronze.
- Identity: source × grid × observed_at × category × source_revision.
- Merge/idempotency: identical revision is a no-op; conflicting content at an
  existing identity fails closed.
- The bounded pre-append lookup and verification both constrain source,
  observed slot, and the `observed_at` partition. Verification joins the exact
  incoming 80-grid revision set, so a later Airflow run can verify a storage
  no-op without duplicating rows.
- Partition: `day(observed_at)`. Do not partition by 80 grids or hourly slot,
  which would create small-file and metadata pressure.
- Compact small Bronze files on a bounded cadence after ingestion, not per grid.

### Silver and Gold

- Silver keeps every visible truth revision, quality state, as-of time, raw hash,
  and manifest lineage.
- Quality joins must first constrain `day(valid_at)` and `day(observed_at)`, then
  apply forecast issue and truth-revision cutoffs.
- Incremental recomputation covers only affected observed days plus a versioned
  late-revision repair window.
- Gold publishes metric family, lead cohort, sample count, denominator, coverage,
  evidence state, source revision, and evaluation as-of. No universal accuracy
  field is allowed.

## Local Trino and internet-cost controls

- When the rollout guard is enabled, observation load/verify and all existing
  heavy Weather DAGs use the same `trino_weather_heavy` one-slot pool. Trino's
  resource group also has `hardConcurrencyLimit=1`, so scheduler and engine both
  prevent heavy-query overlap under the 5 GiB envelope.
- Require partition predicates in every incremental model and test them in dbt.
- Split temperature, precipitation probability, and categorical aggregation into
  bounded phases instead of one wide all-history join.
- Record input rows, scanned bytes, wall time, peak memory, spill, and affected
  partitions for every run; stop promotion when the 5 GiB local Trino envelope is
  approached.
- Keep Trino filesystem cache enabled for repeatedly read recent Iceberg/Parquet
  objects. Cache is a read optimization only: it does not reduce KMA calls and
  does not excuse unbounded scans.
- Read raw JSON once into Bronze; all downstream recomputation reads compact
  columnar Iceberg data with projection and partition pruning.
- Reconcile only missing grids and compact per slot/day so the laptop does not
  repeatedly download thousands of tiny raw objects.

## Observability and data-quality gates

Required operational signals:

- API calls used/remaining by source and purpose;
- source latency bucket, HTTP/business error class, retry count;
- grids expected/landed/valid, category rows expected/valid, incomplete slot age;
- raw/manifest hash mismatch and duplicate revision count;
- Bronze/Silver freshness, affected partitions, scanned bytes, Trino peak memory;
- forecast/truth matched coverage, missing D-3/D-2/D-1 vintages, provisional/final
  truth counts, metric drift, and evidence-state distribution;
- D1 publication age and last evidence revision.

Alerting should distinguish upstream delay, quota exhaustion, invalid credentials,
schema drift, partial Seoul coverage, Trino resource pressure, and publication
staleness. Each class has a different operator action.

## Deployment and rollback sequence

Before any execution, present a fresh read-only report containing the candidate
commit, dirty-tree state, currently running/queued DAG runs, current Trino memory,
exact Compose services to recreate, current DAG pause states, and personal R2/D1
target fingerprints without secret values.

Controlled sequence after a new explicit deployment approval:

1. capture a fresh read-only runtime inventory and keep the new DAG paused;
2. initialize the persistent schema-v1 ledger explicitly with
   `PYTHONPATH=/opt/airflow/dags:/opt/airflow/dags/domains/weather python -m weather_ingest.kma_coordination init-ledger`;
3. import/read back `kma_api_requests=1` and `trino_weather_heavy=1` pools;
4. change only the private runtime environment to shared guards `true` and the
   proposed `45 * * * *` schedule, while keeping the DAG paused;
5. deploy only the exact Airflow code services, then verify health, DagBag,
   ledger schema, pools, dbt parse, contracts, and Trino memory with no data write;
6. after a separate write approval, run one manual single-slot 80-grid canary
   with D1/Worker publication absent from the DAG;
7. validate Raw manifest, Bronze 640/80/8 counts and hashes, observed-at partition
   pruning, scan bytes, retry/resume behavior, and cycle duration;
8. only after a separate scheduling approval, unpause
   `weather_ultra_srt_ncst_bronze` and observe at least one full operating window;
9. implement quality transforms later, and enable D1/Worker only after evidence
   maturity and a separate publication approval.

Rollback pauses only the new DAGs, drains their runs, and restores the prior
Airflow code artifact. Dedicated raw/table namespaces prevent rollback from
touching the existing forecast pipeline. Immutable raw evidence is retained; no
automatic delete or table drop is part of rollback.

## Approval checklist

- [ ] Exact implementation diff and candidate commit reviewed
- [ ] Exact Airflow services and Compose command reported
- [ ] Existing running/queued work and drain window reported
- [ ] Personal KMA/R2/catalog target metadata verified without secrets
- [ ] Quota ceiling, concurrency, retry matrix, and missing-grid repair tested
- [ ] 80-grid/640-row atomicity test green
- [ ] dbt partition-pruning and bounded incremental tests green
- [ ] Local Trino memory/scan acceptance thresholds agreed
- [ ] Raw/Bronze write approved separately from D1/Worker publication
- [ ] Rollback command and preserved data namespaces verified
- [ ] Explicit user approval recorded before pause/restart/unpause/trigger/write
