# KMA observation-truth source contract

## Current status

The repository implements and tests the KMA `getUltraSrtNcst` source adapter,
versioned fixture/schema, physical-attempt budget, bounded retry/circuit policy,
immutable 80-grid Raw landing, dedicated Iceberg Bronze table, and the
paused-by-default `weather_ultra_srt_ncst_bronze` Airflow DAG. It also retains the
one-request credential-safe smoke command and forecast-quality `ObservationTruth`
mapping.

This remains **not deployed and not activated**. The public Compose defaults keep
the shared rollout guard false and the observation schedule empty. No live API
request, R2/Iceberg write, DAG unpause, dbt quality model, D1 write, or Worker
publication was performed while implementing this increment.

## Source decision

Primary operational truth is the Korea Meteorological Administration's
`VilageFcstInfoService_2.0/getUltraSrtNcst` endpoint:

- official catalog: <https://www.data.go.kr/data/15084084/openapi.do?recommendDataYn=Y>
- official KMA API Hub reference: <https://apihub.kma.go.kr/apiList.do?seqApi=10>
- source ID: `kma_ultra_srt_ncst`
- spatial grain: the same KMA 5 km grid used by the short-range forecast
- development quota documented by the catalog: 10,000 requests/day
- public-data license: Korea Open Government License Type 1 (attribution)

This source avoids pretending that a few station observations represent all 80
Seoul grid cells. ASOS remains a future independent audit and historical source;
using it at grid grain requires a separately versioned station metadata,
interpolation, and uncertainty contract. KMA's monthly historical ultra-short
observation files are a better candidate for same-grid backfill and will be
evaluated separately.

## Source and truth semantics

The source response must contain exactly one row for each versioned category:

| category | source meaning | normalized unit | quality use now |
|---|---|---|---|
| `T1H` | air temperature | `degC` | `temperature_air_2m` |
| `RN1` | one-hour precipitation | `mm` | precipitation occurrence |
| `PTY` | precipitation type | `code` | precipitation occurrence |
| `REH` | relative humidity | `percent` | retained |
| `UUU` | east-west wind component | `m/s` | retained |
| `VVV` | north-south wind component | `m/s` | retained |
| `VEC` | wind direction | `degree` | retained |
| `WSD` | wind speed | `m/s` | retained |

`precipitation_occurrence` is true when `PTY != 0` or `RN1 > 0`. Both inputs
must be present and valid; missing input never becomes a dry observation.

The response `baseDate + baseTime` is interpreted in `Asia/Seoul` and normalized
to UTC as `observed_at`. The API does not expose a provider revision timestamp,
so the fetch time becomes both `truth_as_of` and `collected_at`. Revision identity
is `kma_ultra_srt_ncst:<payload_sha256>`.

Near-real-time rows enter the quality kernel as `provisional`, not `final`,
because the provider can revise observations. A later historical/reconciliation
adapter may emit a final revision. This distinction prevents an AI consumer from
presenting a fresh operational observation as immutable ground truth.

## Validation and failure behavior

The adapter fails closed on:

- invalid UTF-8, JSON, response envelope, result code, or row count;
- response date/time/grid that differs from the requested context;
- missing, duplicate, or unversioned categories;
- provider sentinels, non-finite numbers, invalid PTY codes, or invalid physical
  domains for precipitation, humidity, wind direction, and wind speed;
- collection timestamps before the observed slot;
- malformed grid identity or payload hash.

The smoke makes one request and performs no retry. The scheduled runtime uses its
own bounded policy: it retries only transport failures and HTTP 500/502/503/504,
honors a valid numeric `Retry-After`, and applies bounded jittered backoff. HTTP
429 or provider result code 23 opens a cycle-wide circuit; authentication,
permission, schema, context, and daily-exhaustion failures are not retried. Every
physical request attempt is reserved in the shared ledger immediately before
network I/O.

## Quota, completeness, and recovery design

Nominal daily requests are:

```text
observation: 80 grids × 24 hourly slots = 1,920
short range: 80 grids × 8 issue cycles =   640
combined nominal                         = 2,560 requests/day
```

That is 25.6% of the documented 10,000-request development quota before retry,
backfill, or manual repair. The 2,560 figure is the logical success-path request
count, not an upper bound on physical HTTP attempts. The implemented shared
SQLite ledger defaults to a fail-closed 7,500 physical-attempt ceiling, leaving
2,500 attempts of provider headroom. Both forecast internal retries and
observation retries reserve against that same KST-day ledger. A production slot is complete only
when all 80 grids contain all eight categories: 640 category rows. Partial slots
remain quarantined and reconcilable; they are not quality truth and do not reach
Gold or D1.

Recovery is idempotent at source × grid × observed slot × payload revision.
Repeated equal payloads collapse to the same revision, while a changed payload is
retained as a new immutable revision. Reconciliation should request only missing
grids, remain inside the daily budget, and close the slot atomically after the
80-grid completeness check.

## Credential-safe smoke

Fixture validation is network-free:

```bash
python -m tools.kma_observation_smoke \
  --grid-id kma_60_127 \
  --base-date 20260822 \
  --base-time 1400 \
  --fixture contracts/weather-observation/fixtures/kma-ultra-srt-ncst-v1.json
```

For a live request, provide `KMA_SERVICE_KEY` in the process environment and omit
`--fixture`. The command prints only source/grid/slot, HTTP and business status,
category names/count, a latency bucket, the full payload SHA-256, and validation
status. It never prints the key, full secret-bearing URL, raw response, or
observation values, and it writes no file.

## Lakehouse activation gate

Implemented but still inert:

1. hourly paused-by-default DAG with a 40-minute deadline and `max_active_runs=1`;
2. shared one-slot KMA API pool and 7,500/day physical-attempt ledger;
3. immutable per-grid Raw objects, durable missing-grid resume, and exact
   80-grid/640-category manifest gate;
4. dedicated `bronze_kma_ultra_srt_ncst` table partitioned by
   `day(observed_at)`, bounded novel-revision append/no-op semantics, and exact
   revision-scoped verification;
5. shared one-slot Weather Trino pool so observation load/verify cannot overlap
   the existing heavy Weather transforms when the rollout guard is enabled.

Still later milestones, not part of activation:

1. Silver truth revisions and bounded incremental forecast-vs-truth evaluation;
2. scan-byte, source-latency, late-revision, Trino peak-memory, and evidence-state
   production telemetry/alerts;
3. D1/Worker publication after sample, coverage, and evidence maturity gates.

Trino filesystem cache helps repeated reads of recent Iceberg objects, but does
not reduce KMA API calls and never replaces partition pruning. The evaluation
design must keep `day(valid_at)`/`day(observed_at)` predicates and bounded repair
windows so a local 5 GiB Trino process does not rescan full history.
