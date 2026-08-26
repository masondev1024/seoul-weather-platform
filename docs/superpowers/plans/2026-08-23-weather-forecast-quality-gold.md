# Weather 예보 품질 Gold 구현 계획

> 사람용 안내: 예보 발표본과 실황을 비교하는 내부 분석 제품의 세부 작업표다. 전체
> 흐름은 한국어로 이해하고, 체크박스·모델명·설정 키는 자동 검사를 위해 원문을 유지한다.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**목표:** D1·Worker·기존 제공 경로를 바꾸지 않고, R2/Iceberg 안에서 단기예보 D-1·D-2·D-3와
완전한 시간별 실황을 매일 다시 계산할 수 있는 품질 파이프라인을 만든다.

**구조:** Python 실행 계약으로 KST 7일 평가 범위를 고정한다. `dbt`는 날짜 범위가 제한된
예보 Bronze와 파티션된 실황 Bronze를 전용 품질 Silver로 읽고, 실행 버전별 격자·시간·일
Gold 이력을 만든다. 성공 manifest에 있는 날짜만 보이며, 기본 멈춤 상태의 Airflow DAG 두
개가 매일 계산과 한 날짜 backfill을 기존 Trino 한 자리에서 맡는다.

**사용 기술:** Python 3.11, Airflow 3.2.2, dbt-core 1.10.22, dbt-trino 1.10.2, Trino 482,
Iceberg v2, Cloudflare R2 Data Catalog, pytest, JSON Schema.

**상세 설계:** `docs/superpowers/specs/2026-08-22-weather-forecast-quality-gold-design.md`

## Global Constraints

- D1, Worker, serving contracts, D1 selectors, and `WEATHER_GOLD_PUBLICATION_READY_ASSET` remain unchanged.
- The existing unpartitioned `iceberg.weather.silver_kma_vilage_fcst` is not an input and is not rebuilt or repartitioned.
- Forecast input is publishable `weather_bronze.kma_vilage_fcst` with a bounded `load_date` predicate.
- Observation input is `weather_bronze.kma_ultra_srt_ncst` with a bounded `day(observed_at)` predicate.
- The scheduled window is exactly seven complete KST dates ending yesterday; the current partial date is forbidden.
- D-1/D-2/D-3 selection windows are inclusive `[valid_at-27h,-24h]`, `[valid_at-51h,-48h]`, and `[valid_at-75h,-72h]`.
- Version 1 evaluates `TMP`, `POP`, and `PTY`; POP is divided by 100 and classified positive at `>= 0.5`.
- Complete hourly cohorts use the canonical 80-grid Seoul universe; 25-grid assumptions are forbidden.
- Provisional observation truth forces `evidence_state=degraded`; it is never labeled provider-final.
- Evidence is `insufficient_evidence` when `sample_count < 30` or `matched_coverage < 0.80`.
- Trino tasks use `trino_weather_heavy`, one slot, priority 10, a 15-minute task/query ceiling, and a 20-minute DAG-run ceiling.
- Checked-in quality schedules are blank and both DAGs are paused on creation.
- No automatic full refresh, unbounded date input, multi-day backfill, or live KMA call is permitted.
- Commit steps are checkpoints only. Do not execute commit, push, PR, Docker recreation, R2 write, or DAG activation without separate approval.

## File ownership map

- `dags/domains/weather/weather_quality_runtime.py`: Airflow-free window and variable contract.
- `dags/domains/weather/weather_quality_publication.py`: Trino manifest DDL and success publication.
- `dags/domains/weather/weather_forecast_quality_daily.py`: inert daily orchestration.
- `dags/domains/weather/weather_forecast_quality_backfill.py`: one-date manual orchestration.
- `dbt/domains/traffic_weather/models/weather/quality/silver/`: forecast, truth, and match models.
- `dbt/domains/traffic_weather/models/weather/quality/gold/`: histories and latest-success views.
- `dbt/domains/traffic_weather/tests/weather/quality/`: SQL reconciliation and bounds tests.
- `dags/domains/weather/tests/test_weather_quality_*.py`: runtime, manifest, and DAG tests.
- `tests/forecast_quality/test_production_sql_parity.py`: Python/SQL semantic parity.

---

### Task 1: Freeze the production evaluation window

**Files:**
- Create: `dags/domains/weather/weather_quality_runtime.py`
- Create: `dags/domains/weather/tests/test_weather_quality_runtime.py`

**Interfaces:**
- Produces: `QualityEvaluationWindow`, `resolve_daily_quality_window()`, `resolve_backfill_quality_window()`, `quality_schedule()`, `as_dbt_vars()`.
- Consumes: aware datetimes, safe Airflow run IDs, one ISO KST date, and confirmation `BACKFILL_ONE_KST_DATE`.

- [ ] **Step 1: Write failing daily-window tests**

```python
def test_daily_window_is_seven_complete_kst_dates():
    now = datetime(2026, 8, 22, 3, 5, tzinfo=ZoneInfo("Asia/Seoul"))
    window = resolve_daily_quality_window(now=now, run_id="scheduled__quality")
    assert window.window_start_date == date(2026, 8, 15)
    assert window.window_end_date == date(2026, 8, 21)
    assert window.forecast_load_start_date == date(2026, 8, 11)
    assert window.forecast_load_end_date == date(2026, 8, 20)
    assert window.evaluation_as_of == datetime(2026, 8, 21, 18, 5, tzinfo=timezone.utc)


def test_backfill_rejects_range_or_wrong_confirmation():
    with pytest.raises(QualityWindowError, match="single KST date"):
        resolve_backfill_quality_window(
            backfill_date="2026-08-20/2026-08-21",
            confirmation="BACKFILL_ONE_KST_DATE",
            now=datetime.now(timezone.utc),
            run_id="manual__bad",
        )
```

- [ ] **Step 2: Verify RED**

```bash
.venv/bin/python -m pytest dags/domains/weather/tests/test_weather_quality_runtime.py -q
```

Expected: collection fails because `weather_quality_runtime` does not exist.

- [ ] **Step 3: Implement the immutable contract**

```python
QUALITY_REPAIR_DAYS = 7
FORECAST_LOAD_LOOKBACK_DAYS = 4
QUALITY_TRUTH_POLICY_VERSION = "observation-truth-policy/v2-internal"
QUALITY_VINTAGE_POLICY_VERSION = "forecast-vintage-cutoff/v1"
QUALITY_EVIDENCE_POLICY_VERSION = "metric-evidence-gate/v1"
QUALITY_POP_POLICY_VERSION = "pop-threshold-0.5/v1"
QUALITY_BACKFILL_CONFIRMATION = "BACKFILL_ONE_KST_DATE"
QUALITY_SCHEDULE_ENV = "ASK_SEOUL_WEATHER_QUALITY_DAG_SCHEDULE"


@dataclass(frozen=True, slots=True)
class QualityEvaluationWindow:
    evaluation_run_id: str
    evaluation_as_of: datetime
    window_start_date: date
    window_end_date: date
    forecast_load_start_date: date
    forecast_load_end_date: date

    def as_dbt_vars(self) -> dict[str, str]:
        return {
            "weather_quality_run_id": self.evaluation_run_id,
            "weather_quality_evaluation_as_of": self.evaluation_as_of.isoformat(),
            "weather_quality_window_start_date": self.window_start_date.isoformat(),
            "weather_quality_window_end_date": self.window_end_date.isoformat(),
            "weather_quality_forecast_load_start_date": self.forecast_load_start_date.isoformat(),
            "weather_quality_forecast_load_end_date": self.forecast_load_end_date.isoformat(),
            "weather_quality_truth_policy_version": QUALITY_TRUTH_POLICY_VERSION,
        }
```

`resolve_daily_quality_window()` converts `now` to KST, ends yesterday, starts six days earlier, and derives forecast load dates `start-4` through `end-1`. `quality_schedule()` returns `os.getenv(QUALITY_SCHEDULE_ENV, "").strip() or None`.

- [ ] **Step 4: Add adversarial cases**

Cover naive timestamps, unsafe/blank run IDs, invalid/future/current backfill dates, year boundaries, stable dbt-var keys, and redacted exception messages.

- [ ] **Step 5: Verify GREEN**

```bash
.venv/bin/python -m pytest dags/domains/weather/tests/test_weather_quality_runtime.py -q
```

- [ ] **Step 6: Commit checkpoint after approval**

```bash
git add dags/domains/weather/weather_quality_runtime.py dags/domains/weather/tests/test_weather_quality_runtime.py
git commit -m "feat: define weather quality evaluation window"
```

---

### Task 2: Add strict dbt inputs, sources, selectors, and query limit

**Files:**
- Create: `dbt/domains/traffic_weather/macros/weather/weather_quality_contract.sql`
- Modify: `dbt/domains/traffic_weather/models/weather/sources.yml`
- Modify: `dbt/domains/traffic_weather/selectors.yml`
- Modify: `dbt/domains/traffic_weather/dbt_project.yml`
- Modify: `dbt/domains/traffic_weather/profiles.yml`
- Create: `dbt/domains/traffic_weather/tests/weather/test_weather_quality_model_contract.py`

**Interfaces:**
- Consumes: keys from `QualityEvaluationWindow.as_dbt_vars()`.
- Produces: strict `weather_quality_*()` macros, observation/manifest sources, `ask_seoul_weather_quality_candidate`, and a scoped session limit.

- [ ] **Step 1: Write failing source/selector/macro tests**

```python
def test_quality_sources_and_selectors_are_internal():
    names = source_table_names(SOURCES)
    assert {"kma_vilage_fcst", "kma_ultra_srt_ncst", "quality_publication_manifest"} <= names
    selector = selector_block(SELECTORS, "ask_seoul_weather_quality_candidate")
    assert "ask_seoul_weather_d1_public_products" not in selector
    assert "ask_seoul_weather_serving_snapshot_refresh" not in selector
```

Assert the macro requires all runtime vars, contains all policy IDs, validates ISO values, and has no `current_date` fallback.

- [ ] **Step 2: Verify RED**

```bash
.venv/bin/python -m pytest dbt/domains/traffic_weather/tests/weather/test_weather_quality_model_contract.py -q
```

- [ ] **Step 3: Implement fail-closed macros**

```sql
{% macro weather_quality_required_date(name) -%}
  {%- set value = var(name, '') | string | trim -%}
  {%- if not modules.re.fullmatch('^[0-9]{4}-[0-9]{2}-[0-9]{2}$', value) -%}
    {{ exceptions.raise_compiler_error(name ~ ' must be an ISO date') }}
  {%- endif -%}
  cast('{{ value }}' as date)
{%- endmacro %}

{% macro weather_quality_evidence_state(sample_count, expected_count, provisional) -%}
case
  when {{ sample_count }} < 30
    or cast({{ sample_count }} as double) / nullif({{ expected_count }}, 0) < 0.80
    then 'insufficient_evidence'
  when {{ provisional }} then 'degraded'
  else 'sufficient'
end
{%- endmacro %}
```

Add required date/timestamp/run-ID macros, exact policy constants, and a singular test that rejects start/end inversions.

- [ ] **Step 4: Add sources and isolated selectors**

```yaml
  - name: weather_quality_control
    database: "{{ target.database }}"
    schema: "{{ env_var('WEATHER_SCHEMA', 'weather') }}"
    tables:
      - name: quality_publication_manifest
        identifier: weather_forecast_quality_publication_manifest
```

Add `kma_ultra_srt_ncst` to `weather_bronze`. Add candidate and published selectors containing only quality models/tests.

- [ ] **Step 5: Add the quality-only session override hook**

Add to every dbt target:

```yaml
session_properties:
  query_max_run_time: "{{ env_var('TRINO_DBT_QUERY_MAX_RUN_TIME', '2h') }}"
```

Normal tasks retain `2h`; only the quality DAG injects `15m`.

- [ ] **Step 6: Parse and test**

```bash
DBT_TARGET=ci TRINO_HOST=127.0.0.1 .venv/bin/dbt parse --project-dir dbt/domains/traffic_weather --profiles-dir dbt/domains/traffic_weather
.venv/bin/python -m pytest dbt/domains/traffic_weather/tests/weather/test_weather_quality_model_contract.py -q
```

- [ ] **Step 7: Commit checkpoint after approval**

```bash
git add dbt/domains/traffic_weather/macros/weather/weather_quality_contract.sql dbt/domains/traffic_weather/models/weather/sources.yml dbt/domains/traffic_weather/selectors.yml dbt/domains/traffic_weather/dbt_project.yml dbt/domains/traffic_weather/profiles.yml dbt/domains/traffic_weather/tests/weather/test_weather_quality_model_contract.py
git commit -m "feat: add bounded weather quality dbt contract"
```

---

### Task 3: Build the partitioned quality forecast Silver

**Files:**
- Create: `dbt/domains/traffic_weather/models/weather/quality/silver/silver_weather_quality_forecast_vintage.sql`
- Create: `dbt/domains/traffic_weather/models/weather/quality/silver/_quality_silver.yml`
- Create: `dbt/domains/traffic_weather/tests/weather/quality/assert_quality_forecast_vintage_unique.sql`
- Modify: `dbt/domains/traffic_weather/tests/weather/test_weather_quality_model_contract.py`

**Interfaces:**
- Consumes: publishable Forecast Bronze, manifest, coverage grid, bounded load dates.
- Produces: `(grid_id,valid_at,variable,issued_at,source_revision)` partitioned by `day(valid_at)`.

- [ ] **Step 1: Write failing physical-source assertions**

```python
def test_quality_forecast_reads_partitioned_bronze_not_serving_silver():
    sql = FORECAST_VINTAGE.read_text(encoding="utf-8")
    assert "source('weather_bronze', 'kma_vilage_fcst')" in sql
    assert "source('weather_bronze', 'collection_run_manifest')" in sql
    assert "ref('silver_kma_vilage_fcst')" not in sql
    assert "load_date >=" in sql and "load_date <=" in sql
    assert "ARRAY['day(valid_at)']" in sql
```

- [ ] **Step 2: Verify RED**

Run the single test; expect a missing model.

- [ ] **Step 3: Implement bounded normalization**

Use incremental merge, the five-column unique key, `on_schema_change='fail'`, and Iceberg property `partitioning=ARRAY['day(valid_at)']`. The source must include:

```sql
where bronze.load_date >= cast({{ weather_quality_forecast_load_start_date() }} as varchar)
  and bronze.load_date <= cast({{ weather_quality_forecast_load_end_date() }} as varchar)
```

Join latest manifest state on `dag_run_id`; require `SUCCESS` and `is_publishable`. Normalize `TMP→temperature_air_2m`, `POP→precipitation_occurrence`, `PTY→precipitation_occurrence_category`. Reject POP outside 0–100; map supported PTY zero to `dry`, nonzero to `wet`, and preserve invalid status. Use `payload_hash` as source revision.

- [ ] **Step 4: Add schema/data tests**

Declare all columns; test canonical grid, accepted variable/value-kind/unit, valid issue/valid timestamps, and exact grain uniqueness.

- [ ] **Step 5: Parse and test**

```bash
.venv/bin/python -m pytest dbt/domains/traffic_weather/tests/weather/test_weather_quality_model_contract.py -q
DBT_TARGET=ci TRINO_HOST=127.0.0.1 .venv/bin/dbt parse --project-dir dbt/domains/traffic_weather --profiles-dir dbt/domains/traffic_weather
```

- [ ] **Step 6: Commit checkpoint after approval**

```bash
git add dbt/domains/traffic_weather/models/weather/quality/silver dbt/domains/traffic_weather/tests/weather/quality dbt/domains/traffic_weather/tests/weather/test_weather_quality_model_contract.py
git commit -m "feat: normalize bounded weather forecast vintages"
```

---

### Task 4: Build provisional observation truth Silver

**Files:**
- Create: `dbt/domains/traffic_weather/models/weather/quality/silver/silver_kma_observation_truth.sql`
- Modify: `dbt/domains/traffic_weather/models/weather/quality/silver/_quality_silver.yml`
- Create: `dbt/domains/traffic_weather/tests/weather/quality/assert_quality_observation_truth_unique.sql`
- Create: `dbt/domains/traffic_weather/tests/weather/quality/assert_quality_observation_truth_complete_hours.sql`
- Modify: `dbt/domains/traffic_weather/tests/weather/test_weather_quality_model_contract.py`

**Interfaces:**
- Consumes: bounded `weather_bronze.kma_ultra_srt_ncst` revisions for the seven complete evaluation dates.
- Produces: one deterministic provisional truth row per `(grid_id, observed_at, variable, truth_revision)` with value, quality state, and provenance.

- [ ] **Step 1: Write failing source and semantics tests**

```python
def test_observation_truth_is_bounded_and_preserves_missingness():
    sql = OBSERVATION_TRUTH.read_text(encoding="utf-8")
    assert "source('weather_bronze', 'kma_ultra_srt_ncst')" in sql
    assert "source('weather_bronze', 'collection_run_manifest')" in sql
    assert "observed_at >=" in sql and "observed_at <" in sql
    assert "category in ('T1H', 'PTY', 'RN1')" in sql
    assert "coalesce(rn1" not in sql.lower()
    assert "'provisional'" in sql
```

Add static assertions for `partitioning=ARRAY['day(observed_at)']`, source-revision preservation, and a fail-closed invalid-observation state.

- [ ] **Step 2: Verify RED**

```bash
.venv/bin/python -m pytest dbt/domains/traffic_weather/tests/weather/test_weather_quality_model_contract.py -q
```

Expected: the observation-truth model and its contract are missing.

- [ ] **Step 3: Implement deterministic revision-aware truth**

Read only `[window_start_date 00:00 KST, window_end_date + 1 day 00:00 KST)`. Join the latest publishable observation manifest state and require the exact 80-grid/640-category gate. Pivot `T1H`, `PTY`, and `RN1` per grid/hour/revision, then emit long-form rows for `temperature_air_2m`, `precipitation_occurrence`, and `precipitation_occurrence_category`. Preserve `collected_at`, manifest/run provenance, raw values, and payload/source hashes.

```sql
case
  when try_cast(t1h_value as double) is null then 'invalid_truth'
  else 'provisional'
end as temperature_truth_status,
case
  when try_cast(pty_value as integer) = 0 and try_cast(rn1_value as double) = 0 then false
  when try_cast(pty_value as integer) > 0 or try_cast(rn1_value as double) > 0 then true
  when pty_value is null or rn1_value is null then null
  else null
end as precipitation_observed
```

Do not turn absent precipitation fields into dry weather. Rank duplicate inputs deterministically by `collected_at desc, source_revision desc, source_run_id desc` and retain revision columns for later audit.

- [ ] **Step 4: Add schema and data-quality tests**

Test exact grain uniqueness, canonical Seoul grid membership, complete-hour coverage reporting, accepted truth statuses, non-null provenance, and no current-day rows. The completeness test reports gaps; publication gates decide whether gaps are fatal.

- [ ] **Step 5: Parse and verify GREEN**

```bash
.venv/bin/python -m pytest dbt/domains/traffic_weather/tests/weather/test_weather_quality_model_contract.py -q
DBT_TARGET=ci TRINO_HOST=127.0.0.1 .venv/bin/dbt parse --project-dir dbt/domains/traffic_weather --profiles-dir dbt/domains/traffic_weather
```

- [ ] **Step 6: Commit checkpoint after approval**

```bash
git add dbt/domains/traffic_weather/models/weather/quality/silver dbt/domains/traffic_weather/tests/weather/quality dbt/domains/traffic_weather/tests/weather/test_weather_quality_model_contract.py
git commit -m "feat: model provisional weather observation truth"
```

---

### Task 5: Match D-1/D-2/D-3 vintages and build the grid-score product

**Files:**
- Create: `dbt/domains/traffic_weather/models/weather/quality/silver/silver_weather_forecast_observation_match.sql`
- Modify: `dbt/domains/traffic_weather/models/weather/quality/silver/_quality_silver.yml`
- Create: `dbt/domains/traffic_weather/models/weather/quality/gold/gold_weather_forecast_quality_grid_score_history.sql`
- Create: `dbt/domains/traffic_weather/models/weather/quality/gold/gold_weather_forecast_quality_grid_score.sql`
- Create: `dbt/domains/traffic_weather/models/weather/quality/gold/_quality_gold.yml`
- Create: `dbt/domains/traffic_weather/tests/weather/quality/assert_quality_match_unique.sql`
- Create: `dbt/domains/traffic_weather/tests/weather/quality/assert_quality_grid_score_reconciles.sql`
- Create: `tests/forecast_quality/test_production_sql_parity.py`

**Interfaces:**
- Consumes: forecast vintages, provisional truth, evaluation run ID, exact inclusive vintage windows.
- Produces: auditable grid-hour scores for every expected horizon/variable and a latest-success product view.

- [ ] **Step 1: Write failing boundary and deterministic-selection tests**

```python
@pytest.mark.parametrize(
    ("horizon", "lower_hours", "upper_hours"),
    [("D-1", 27, 24), ("D-2", 51, 48), ("D-3", 75, 72)],
)
def test_vintage_windows_are_inclusive(horizon, lower_hours, upper_hours):
    contract = production_sql_contract()
    assert contract.window(horizon) == InclusiveWindow(lower_hours, upper_hours)
```

Add fixtures with candidates exactly on both boundaries, just outside them, duplicate issue timestamps with different revisions, missing vintage, missing truth, and invalid values.

- [ ] **Step 2: Verify RED**

```bash
.venv/bin/python -m pytest tests/forecast_quality/test_production_sql_parity.py -q
```

- [ ] **Step 3: Implement the expected population and vintage match**

Generate the expected population from truth grid-hours crossed with `D-1/D-2/D-3` and the applicable variables. Left join candidates in the inclusive interval and select exactly one with:

```sql
row_number() over (
  partition by evaluation_run_id, grid_id, valid_at, variable, forecast_horizon
  order by issued_at desc, source_revision desc, source_run_id desc
) as candidate_rank
```

Emit one of `matched`, `missing_vintage`, `missing_truth`, `invalid_forecast`, `invalid_truth`, or `incompatible_contract`; never silently drop an expected row.

- [ ] **Step 4: Implement score components, not only final metrics**

For temperature store signed error, absolute error, and squared error. For precipitation store probability, observed binary label, Brier component, predicted-positive flag at `>= 0.5`, and `tp/fp/tn/fn` indicators. Preserve issue/valid/observed timestamps, source revisions, policy versions, and run ID.

- [ ] **Step 5: Implement history plus atomic product view**

`silver_weather_forecast_observation_match` and the history are partitioned by `day(valid_at)`. `gold_weather_forecast_quality_grid_score_history` is incremental and keyed by the evaluation run plus analytical grain. `gold_weather_forecast_quality_grid_score` is a view that resolves exactly one newest `SUCCESS` run per evaluation date, then joins its history rows:

```sql
with successful_candidates as (
  select evaluation_date_kst, evaluation_run_id,
         row_number() over (
           partition by evaluation_date_kst
           order by evaluation_as_of desc, published_at desc, evaluation_run_id desc
         ) as publication_rank
  from successful_quality_runs_expanded_to_evaluation_dates
)
select history.*
from {{ ref('gold_weather_forecast_quality_grid_score_history') }} history
join successful_candidates publication
  on publication.evaluation_run_id = history.evaluation_run_id
 and publication.evaluation_date_kst = history.evaluation_date_kst
 and publication.publication_rank = 1
```

This product contains analytical provenance only; it must not depend on D1 or serving metadata.

- [ ] **Step 6: Add uniqueness, reconciliation, and parity tests**

Assert expected count equals matched plus every explicit failure state, no duplicate grain exists, all matched continuous rows have score components, all matched binary rows have Brier/confusion components, and Python oracle fixtures match SQL semantics.

- [ ] **Step 7: Verify GREEN**

```bash
.venv/bin/python -m pytest tests/forecast_quality/test_production_sql_parity.py dbt/domains/traffic_weather/tests/weather/test_weather_quality_model_contract.py -q
DBT_TARGET=ci TRINO_HOST=127.0.0.1 .venv/bin/dbt parse --project-dir dbt/domains/traffic_weather --profiles-dir dbt/domains/traffic_weather
```

- [ ] **Step 8: Commit checkpoint after approval**

```bash
git add dbt/domains/traffic_weather/models/weather/quality tests/forecast_quality dbt/domains/traffic_weather/tests/weather/quality
git commit -m "feat: score weather forecast vintages against observations"
```

---

### Task 6: Build hourly and daily quality aggregates from score components

**Files:**
- Create: `dbt/domains/traffic_weather/models/weather/quality/gold/gold_weather_forecast_quality_hourly_history.sql`
- Create: `dbt/domains/traffic_weather/models/weather/quality/gold/gold_weather_forecast_quality_hourly.sql`
- Create: `dbt/domains/traffic_weather/models/weather/quality/gold/gold_weather_forecast_quality_daily_history.sql`
- Create: `dbt/domains/traffic_weather/models/weather/quality/gold/gold_weather_forecast_quality_daily.sql`
- Modify: `dbt/domains/traffic_weather/models/weather/quality/gold/_quality_gold.yml`
- Create: `dbt/domains/traffic_weather/tests/weather/quality/assert_quality_hourly_reconciles.sql`
- Create: `dbt/domains/traffic_weather/tests/weather/quality/assert_quality_daily_reconciles.sql`
- Modify: `tests/forecast_quality/test_production_sql_parity.py`

**Interfaces:**
- Consumes: grid-score history only.
- Produces: exact hourly/daily aggregate histories and latest-success views with evidence state.

- [ ] **Step 1: Add failing metric fixtures**

Use a minimal deterministic fixture:

```python
temperature = [(12.0, 10.0), (14.0, 15.0)]
precipitation = [(0.8, True), (0.2, False)]
```

Assert temperature `MAE=1.5`, `RMSE=sqrt(2.5)`, `bias=0.5`; precipitation `Brier=0.04`, confusion counts `tp=1, tn=1`; and all zero-denominator rates are `NULL`, never zero.

- [ ] **Step 2: Verify RED**

```bash
.venv/bin/python -m pytest tests/forecast_quality/test_production_sql_parity.py -q
```

- [ ] **Step 3: Aggregate directly from grid score components**

Calculate `sample_count`, `expected_count`, `matched_coverage`, MAE, RMSE, bias, Brier, ten-bin ECE, confusion counts, precision, recall, and F1 from the underlying component sums. Daily metrics must aggregate grid-score components directly; do not average hourly MAE, RMSE, Brier, ECE, precision, recall, or F1.

```sql
sqrt(sum(squared_error) / nullif(count_if(match_status = 'matched'), 0)) as rmse,
sum(true_positive) / nullif(sum(true_positive) + sum(false_positive), 0) as precision
```

- [ ] **Step 4: Apply the evidence contract**

Use the shared macro so fewer than 30 samples or coverage below 0.80 yields `insufficient_evidence`; otherwise provisional truth yields `degraded`. Store thresholds and policy revisions alongside every row.

- [ ] **Step 5: Add atomic product views and reconciliation tests**

Each public analytical view joins the success manifest exactly as the grid-score view does. Tests reconcile child component sums to hourly/daily totals and ensure all three views expose the same successful run set.

- [ ] **Step 6: Parse and verify GREEN**

```bash
.venv/bin/python -m pytest tests/forecast_quality/test_production_sql_parity.py dbt/domains/traffic_weather/tests/weather/test_weather_quality_model_contract.py -q
DBT_TARGET=ci TRINO_HOST=127.0.0.1 .venv/bin/dbt parse --project-dir dbt/domains/traffic_weather --profiles-dir dbt/domains/traffic_weather
```

- [ ] **Step 7: Commit checkpoint after approval**

```bash
git add dbt/domains/traffic_weather/models/weather/quality/gold dbt/domains/traffic_weather/tests/weather/quality tests/forecast_quality/test_production_sql_parity.py
git commit -m "feat: aggregate weather forecast quality products"
```

---

### Task 7: Publish quality runs atomically through an Iceberg manifest

**Files:**
- Create: `dags/domains/weather/weather_quality_publication.py`
- Create: `dags/domains/weather/tests/test_weather_quality_publication.py`
- Modify: `dags/domains/weather/weather_iceberg_maintenance.py`
- Modify: `dags/domains/weather/tests/test_weather_iceberg_maintenance.py`

**Interfaces:**
- Consumes: one evaluation window, candidate history counts, and a Trino connection.
- Produces: idempotent `RUNNING`, `SUCCESS`, or `FAILED` rows in `weather_forecast_quality_publication_manifest`.

- [ ] **Step 1: Write failing manifest tests**

```python
def test_success_requires_current_run_reconciliation():
    client = RecordingTrinoClient(candidate_count=1600, expected_count=1680)
    with pytest.raises(QualityPublicationError, match="candidate count mismatch"):
        publish_quality_success(client, quality_window())
    assert not client.contains_status("SUCCESS")


def test_replaying_same_success_is_idempotent():
    client = RecordingTrinoClient(existing_success=quality_window())
    publish_quality_success(client, quality_window())
    assert client.success_insert_count == 0
```

Cover conflicting run/date reuse, unsafe identifiers, zero expected rows, missing product counts, failed-run recording, and sanitized exception messages.

- [ ] **Step 2: Verify RED**

```bash
.venv/bin/python -m pytest dags/domains/weather/tests/test_weather_quality_publication.py -q
```

- [ ] **Step 3: Implement fail-closed manifest DDL and state transitions**

The manifest stores:

```text
evaluation_run_id, dag_id, evaluation_as_of,
window_start_date, window_end_date, status,
expected_grid_count, grid_score_count, hourly_count, daily_count,
truth_policy_version, vintage_policy_version,
evidence_policy_version, pop_policy_version, published_at
```

Create the table idempotently. Insert `RUNNING`, reconcile counts only for the current run and bounded window, then atomically expose it by merging one immutable `SUCCESS` row. A replay with identical metadata is a no-op; a conflicting success raises an error. `FAILED` is diagnostic and never makes a Gold view visible.

- [ ] **Step 4: Register maintenance for physical quality tables**

Add only the three quality Silver tables, three Gold history tables, and publication manifest to retention/optimization metadata. Product views are excluded. Keep snapshots long enough for replay/audit and use tighter data-file optimization thresholds than the serving products because daily increments are small.

- [ ] **Step 5: Verify GREEN and maintenance isolation**

```bash
.venv/bin/python -m pytest dags/domains/weather/tests/test_weather_quality_publication.py dags/domains/weather/tests/test_weather_iceberg_maintenance.py -q
```

- [ ] **Step 6: Commit checkpoint after approval**

```bash
git add dags/domains/weather/weather_quality_publication.py dags/domains/weather/tests/test_weather_quality_publication.py dags/domains/weather/weather_iceberg_maintenance.py dags/domains/weather/tests/test_weather_iceberg_maintenance.py
git commit -m "feat: publish weather quality runs atomically"
```

---

### Task 8: Orchestrate inert daily and one-date backfill DAGs

**Files:**
- Create: `dags/domains/weather/weather_forecast_quality_daily.py`
- Create: `dags/domains/weather/weather_forecast_quality_backfill.py`
- Create: `dags/domains/weather/tests/test_weather_forecast_quality_dags.py`
- Modify: `dags/domains/weather/weather_dbt_runtime.py`
- Modify: `dags/domains/weather/tests/test_weather_dbt_runtime.py`
- Modify: `dags/executors/airflow_weather_dbt_run.sh` only if required by the tested env contract
- Modify: `dags/domains/weather/weather_assets.py`
- Modify: `dags/domains/weather/tests/test_weather_assets.py`
- Modify: `docker-compose.local.yml`
- Modify: `scripts/validate_local_runtime.py`
- Modify: `tests/runtime/test_local_runtime_validation.py`

**Interfaces:**
- Daily DAG: blank schedule by default; intended schedule after approval is `5 3 * * *` KST.
- Backfill DAG: manual only, exactly one ISO KST date plus `BACKFILL_ONE_KST_DATE` confirmation.
- Both DAGs use `trino_weather_heavy`, priority 10, max one active run, 15-minute task/query limits, and a 20-minute DAG-run limit.

- [ ] **Step 1: Write failing DagBag and resource-contract tests**

```python
def test_quality_dags_are_inert_by_default(dagbag):
    daily = dagbag.get_dag("weather_forecast_quality_daily")
    backfill = dagbag.get_dag("weather_forecast_quality_backfill")
    assert daily.schedule is None
    assert backfill.schedule is None
    assert daily.is_paused_upon_creation is True
    assert backfill.is_paused_upon_creation is True
    assert daily.max_active_runs == backfill.max_active_runs == 1
```

Assert every Trino/dbt task uses the shared heavy pool and priority 10, has an execution timeout no greater than 15 minutes, and neither DAG references D1 selectors, Worker routes, serving snapshot refresh, or a KMA HTTP client.

- [ ] **Step 2: Verify RED**

```bash
.venv/bin/python -m pytest dags/domains/weather/tests/test_weather_forecast_quality_dags.py -q
```

- [ ] **Step 3: Safely extend the existing dbt runtime**

Extend `run_weather_dbt_phase()` with optional dbt vars and an allowlisted task environment. Permit only `TRINO_DBT_QUERY_MAX_RUN_TIME=15m` for this path; reject arbitrary environment injection. Preserve byte-for-byte behavior for existing collection, serving, D1, and maintenance callers.

- [ ] **Step 4: Implement the daily DAG**

```text
validate runtime and shared guard
  -> resolve seven-date evaluation window
  -> ensure manifest / mark RUNNING
  -> dbt deps from local cache
  -> dbt build ask_seoul_weather_quality_candidate
  -> reconcile current-run counts
  -> publish SUCCESS
  -> emit internal quality-ready asset
  -> record bounded metrics / teardown
```

Use `catchup=False`, `max_active_runs=1`, no task retries around non-idempotent publication, and narrow retry/backoff only around retryable transport/Trino failures. On failure, record `FAILED` best-effort without masking the original exception.

- [ ] **Step 5: Implement the manual one-date backfill DAG**

Require a single `backfill_date` and exact confirmation. Derive the necessary forecast load range, give the run a unique replay-safe ID, and execute the same selector/publication contract. Reject arrays, ranges, current/future dates, and more than one day.

- [ ] **Step 6: Add an internal-only asset**

Add `iceberg://weather/gold/forecast-quality-ready`. Do not reuse `WEATHER_GOLD_PUBLICATION_READY_ASSET`; no serving DAG may subscribe to the quality asset in version 1.

- [ ] **Step 7: Thread the blank schedule through local Compose**

Add `ASK_SEOUL_WEATHER_QUALITY_DAG_SCHEDULE: ${ASK_SEOUL_WEATHER_QUALITY_DAG_SCHEDULE:-}` to all five Airflow services in `docker-compose.local.yml`. Extend the validator to prove the variable exists everywhere and defaults blank. Do not recreate Docker in this task.

- [ ] **Step 8: Verify DAG and local-runtime contracts**

```bash
.venv/bin/python -m pytest dags/domains/weather/tests/test_weather_forecast_quality_dags.py dags/domains/weather/tests/test_weather_dbt_runtime.py dags/domains/weather/tests/test_weather_assets.py tests/runtime/test_local_runtime_validation.py -q
docker compose -f docker-compose.local.yml config >/tmp/weather-quality-compose.rendered.yml
```

- [ ] **Step 9: Commit checkpoint after approval**

```bash
git add dags/domains/weather docker-compose.local.yml scripts/validate_local_runtime.py tests/runtime/test_local_runtime_validation.py dags/executors/airflow_weather_dbt_run.sh
git commit -m "feat: orchestrate bounded weather quality evaluation"
```

---

### Task 9: Document operations, prove isolation, and prepare a shadow-run handoff

**Files:**
- Modify: `docs/architecture/README.md`
- Modify: `docs/runbooks/LOCAL_PIPELINE_RUNBOOK.md`
- Create: `docs/runbooks/WEATHER_FORECAST_QUALITY_RUNBOOK.md`
- Modify: `README.md`
- Modify: `docs/provenance/source-provenance.json`
- Modify: `docs/provenance/source-provenance.schema.json` only if the existing schema cannot express the new internal products
- Include after approval: `docs/superpowers/specs/2026-08-22-weather-forecast-quality-gold-design.md`
- Include after approval: `docs/superpowers/plans/2026-08-23-weather-forecast-quality-gold.md`

**Interfaces:**
- Runbook readers receive metric definitions, exact vintage boundaries, provisional-truth limitations, replay procedure, SLOs, alerts, and rollback steps.
- Provenance identifies KMA sources, Bronze partitions, quality Silver lineage, Gold history/view boundaries, and the no-D1/no-Worker contract.

- [ ] **Step 1: Write failing documentation contract tests**

Extend the existing documentation/provenance tests to require the three exact inclusive windows, seven complete KST dates, `03:05 KST` intended schedule, `15m/20m` ceilings, one-date backfill confirmation, and explicit `provisional/degraded` language.

- [ ] **Step 2: Update architecture and runbooks**

Document failure scenarios and recovery: missing forecast vintage, incomplete observations, schema incompatibility, count mismatch, interrupted dbt build, failed success publication, replay conflict, OOM/query timeout, and stale manifest. State that a failed candidate run remains invisible and that rollback means disabling the quality schedule/view exposure without touching serving.

- [ ] **Step 3: Refresh and validate provenance**

```bash
.venv/bin/python scripts/refresh_source_provenance.py --write
.venv/bin/python scripts/validate_source_provenance.py
```

Inspect the diff to ensure no local path, secret, credential, account identifier, or runtime-only value entered public metadata.

- [ ] **Step 4: Run repository verification**

```bash
.venv/bin/python -m pytest tests/forecast_quality dags/domains/weather/tests dbt/domains/traffic_weather/tests/weather tests/runtime -q
DBT_TARGET=ci TRINO_HOST=127.0.0.1 .venv/bin/dbt parse --project-dir dbt/domains/traffic_weather --profiles-dir dbt/domains/traffic_weather
.venv/bin/python scripts/validate_serving_contract.py
.venv/bin/python scripts/validate_local_runtime.py --compose-file docker-compose.local.yml
.venv/bin/python scripts/check_repository_policy.py
git diff --check
```

- [ ] **Step 5: Run security and secret checks**

Use the repository's pinned secret scanner and inspect every added environment-variable name. Verify there are no credential values, `.env` contents, R2 account identifiers, D1 IDs, KMA keys, or local absolute paths in tracked files.

- [ ] **Step 6: Prove bounded physical reads without writing data**

Compile the quality models with a representative seven-date window, then run read-only Trino `EXPLAIN (TYPE IO)` for the source scans. Evidence must show `load_date` partition pruning for Forecast Bronze and `day(observed_at)` pruning for Observation Bronze. Reject the release if compiled SQL touches unpartitioned `silver_kma_vilage_fcst` or scans outside the computed window.

- [ ] **Step 7: Prepare, but do not execute, the operational rollout**

Present a shadow-run checklist containing exact Docker services affected, memory/headroom snapshot, pool state, intended schedule, expected request count (`0` new KMA calls), expected R2 writes, success queries, and rollback commands. Stop before Docker recreation, R2 writes, DAG activation, commit, push, or PR and request separate approval.

- [ ] **Step 8: Commit and push only after explicit approval**

```bash
git add README.md docs dags dbt tests scripts docker-compose.local.yml
git commit -m "feat: add R2-only weather forecast quality products"
git push origin main
```

Do not run these commands until the user approves the verified diff and rollout evidence.

---

## Final verification checklist

- [ ] All newly added targeted tests pass from a clean process.
- [ ] Full repository policy, provenance, dbt parse, DagBag, local Compose, and serving-isolation checks pass.
- [ ] Forecast and observation source reads are proven partition-bounded by compiled SQL and Trino IO explain.
- [ ] Candidate row counts reconcile before a success manifest row is visible.
- [ ] The quality path makes zero KMA API calls and zero D1/Worker writes.
- [ ] Existing collection and serving DAG schedules, selectors, assets, pools, and priorities are unchanged.
- [ ] No Docker service has been recreated and no quality DAG has been activated during implementation.
- [ ] No commit, push, PR, R2 mutation, or production activation occurs without its explicit approval gate.
