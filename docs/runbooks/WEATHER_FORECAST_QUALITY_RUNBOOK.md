# Weather Forecast-Quality Gold Runbook

## Operating intent

This internal R2/Iceberg quality product evaluates seven complete KST dates ending yesterday.
It never includes today's partial data. The graph reads stored Bronze data only: it makes
zero new KMA API calls, creates no Raw collection files, and writes nothing to D1 or Worker-facing serving products.

The daily DAG is inert in source control. Its checked-in schedule is blank and it is paused on creation.
After separate Airflow rollout approval, the intended schedule is `03:05 KST` (`5 3 * * *`) through
`ASK_SEOUL_WEATHER_QUALITY_DAG_SCHEDULE`. The manual backfill DAG has no schedule.

| DAG | Purpose | Activation rule |
|---|---|---|
| `weather_forecast_quality_daily` | Seven-date repair/evaluation window | Paused by default; schedule only after approval |
| `weather_forecast_quality_backfill` | One past complete KST date | Manual only with confirmation |

Both DAGs use one slot of `trino_weather_heavy`, priority 10, a 15m task/query ceiling, and a 20m DAG-run ceiling.
They do not run concurrently with other heavy Weather Trino work. This serializes Iceberg operations and prevents an
overlap from increasing Trino OOM risk.

## Data and metric contract

The candidate reads bounded `load_date` partitions of `weather_bronze.kma_vilage_fcst` and bounded
`day(observed_at)` partitions of `weather_bronze.kma_ultra_srt_ncst`. It compares TMP, POP, and PTY for 80 grids.

| Horizon | Inclusive issue-time window |
|---|---|
| D-1 | `[valid_at - 27h, valid_at - 24h]` |
| D-2 | `[valid_at - 51h, valid_at - 48h]` |
| D-3 | `[valid_at - 75h, valid_at - 72h]` |

TMP uses continuous error statistics. POP is percent-to-probability normalized and positive at `>= 0.5`.
PTY uses categorical occurrence accuracy. Version-1 observation truth is **provisional**; an otherwise-complete
provisional cohort is **degraded**, not provider-final. A metric is `insufficient_evidence` when it has fewer than
30 samples or less than 80 percent coverage.

Each successful seven-date run contains 120,960 grid rows, 504 hourly rows, and 21 daily rows.
A one-date replay contains 17,280, 72, and 3. The manifest is the visibility gate: it is `RUNNING` before dbt and
becomes `SUCCESS` only after exact count reconciliation and expected-population checks for grid, hourly, and daily keys.
A partial or failed candidate is not exposed in a latest view.

## Standard daily verification

Perform these checks only after Airflow deployment/activation approval. They are run-state and storage read checks;
they do not authorize an unpause or a live query by themselves.

1. Confirm one or fewer active quality DAG run and one `trino_weather_heavy` pool slot.
2. Confirm the frozen window contains seven complete KST dates ending yesterday and a fixed `evaluation_as_of`.
3. Confirm the manifest reaches exactly one `SUCCESS` for the evaluation identity; a failed candidate is `FAILED`.
4. Reconcile grid, hourly, and daily counts against 120,960, 504, and 21.
5. Alert on rising `missing_vintage`, `missing_truth`, `invalid_forecast`, `invalid_truth`, or `incompatible_contract`.
6. Confirm D1 publication logs, Worker deployments, and public serving assets have no quality-product change.

## One-date replay

Use `weather_forecast_quality_backfill` for exactly one complete past KST date. Supply:

```text
backfill_date=YYYY-MM-DD
confirmation=BACKFILL_ONE_KST_DATE
```

Ranges, today, future dates, missing dates, and wrong confirmation fail before dbt. Do not delete history to make a
replay succeed. Resolve an identity conflict or terminal manifest state before a controlled replay.

## Failure response and rollback

| Symptom | Recovery |
|---|---|
| `missing_vintage` increases | Verify bounded Forecast Bronze/manifest arrival and replay the date only after source repair. |
| `missing_truth` or `degraded` | Preserve the result, alert, and wait for eligible truth; never infer dry weather. |
| `incompatible_contract` | Keep quality disabled, repair the dbt contract, then replay. |
| Count mismatch or interrupted build | Keep the manifest non-successful; inspect run-scoped history and retry only after correction. |
| Trino OOM or timeout | Inspect partition filters and query plan; retain the one-slot pool rather than raising concurrency. |
| Stale `RUNNING` manifest | Reconcile the run identity and record a terminal failure before replay. |

Rollback disables the quality schedule and leaves quality latest views unused. It never requires D1 deletion or Worker rollback.
Docker recreation, Airflow pause/unpause, triggering, backfill, and live R2/Trino operation remain separately approval-gated.

## Shadow-run handoff

Before activation, report the target commit, affected Airflow services, current writer states, Trino memory headroom,
and `trino_weather_heavy` pool state. The expected quality-run effect is zero new KMA API calls, zero D1 writes,
zero Worker writes, and R2/Iceberg writes only for quality Silver/Gold histories and the publication manifest.

Stop for approval before Docker recreation, DAG activation, trigger, backfill, or live storage operations. If approved,
run one controlled daily candidate, validate manifest/count/evidence gates, and retain the unchanged public serving path.
