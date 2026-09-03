"""Airflow-free publication gate for weather forecast-quality Gold history.

The dbt quality models write run-versioned history tables. This module owns the
small control-table contract that makes a run visible to the published quality
views only after all candidate history tables reconcile for the exact runtime
window. It intentionally has no Airflow or Trino connection dependency.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from weather_quality_runtime import (
    QUALITY_EVIDENCE_POLICY_VERSION,
    QUALITY_POP_POLICY_VERSION,
    QUALITY_TRUTH_POLICY_VERSION,
    QUALITY_VINTAGE_POLICY_VERSION,
    QualityEvaluationWindow,
)


DEFAULT_CATALOG = "iceberg"
DEFAULT_SCHEMA = "weather"
MANIFEST_TABLE_NAME = "weather_forecast_quality_publication_manifest"
DAG_ID = "weather_quality_publication"

EXPECTED_GRID_COUNT = 80
QUALITY_VARIABLE_COUNT = 3
QUALITY_FORECAST_HORIZON_COUNT = 3
HOURS_PER_EVALUATION_DATE = 24

RUNNING = "RUNNING"
SUCCESS = "SUCCESS"
FAILED = "FAILED"
TERMINAL_STATUSES = frozenset({SUCCESS, FAILED})

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class QualityPublicationError(RuntimeError):
    """Raised for fail-closed quality publication contract violations."""


@dataclass(frozen=True, slots=True)
class QualityPublicationTarget:
    catalog: str = DEFAULT_CATALOG
    schema: str = DEFAULT_SCHEMA

    @property
    def manifest_relation(self) -> str:
        return qualified_relation(self.catalog, self.schema, MANIFEST_TABLE_NAME)


@dataclass(frozen=True, slots=True)
class ExpectedQualityCounts:
    expected_grid_count: int
    grid_score_count: int
    hourly_count: int
    daily_count: int


@dataclass(frozen=True, slots=True)
class CandidateQualityCounts:
    expected_grid_count: int
    grid_score_count: int
    hourly_count: int
    daily_count: int
    grid_population_mismatch_count: int = 0
    hourly_population_mismatch_count: int = 0
    daily_population_mismatch_count: int = 0


@dataclass(frozen=True, slots=True)
class QualityPublicationResult:
    evaluation_run_id: str
    status: str
    action: str
    counts: CandidateQualityCounts | None = None


def qualified_relation(catalog: str, schema: str, table: str) -> str:
    return ".".join(
        (
            _safe_identifier(catalog, kind="catalog"),
            _safe_identifier(schema, kind="schema"),
            _safe_identifier(table, kind="table"),
        )
    )


def expected_counts_for_window(window: QualityEvaluationWindow) -> ExpectedQualityCounts:
    day_count = _validated_day_count(window)
    return ExpectedQualityCounts(
        expected_grid_count=EXPECTED_GRID_COUNT,
        grid_score_count=(
            day_count
            * EXPECTED_GRID_COUNT
            * HOURS_PER_EVALUATION_DATE
            * QUALITY_VARIABLE_COUNT
            * QUALITY_FORECAST_HORIZON_COUNT
        ),
        hourly_count=day_count * HOURS_PER_EVALUATION_DATE * QUALITY_FORECAST_HORIZON_COUNT,
        daily_count=day_count * QUALITY_FORECAST_HORIZON_COUNT,
    )


def ensure_manifest_table(cursor: Any, target: QualityPublicationTarget | None = None) -> None:
    target = _validated_target(target)
    _execute(cursor, _manifest_ddl(target), stage="ensure_manifest_table")


def begin_quality_publication(
    cursor: Any,
    *,
    window: QualityEvaluationWindow,
    target: QualityPublicationTarget | None = None,
    dag_id: str = DAG_ID,
    published_at: datetime | None = None,
) -> QualityPublicationResult:
    """Create the manifest if needed and mark the exact evaluation run RUNNING."""

    target = _validated_target(target)
    safe_dag_id = _safe_text(dag_id, field="dag_id")
    published_at = _published_at(published_at)
    expected = expected_counts_for_window(window)

    ensure_manifest_table(cursor, target)
    existing_rows = _existing_manifest_rows(cursor, target=target, window=window)
    replay = _classify_existing_rows(
        existing_rows,
        window=window,
        dag_id=safe_dag_id,
        expected=expected,
    )
    if replay is not None:
        return replay

    _merge_running(
        cursor,
        target=target,
        window=window,
        dag_id=safe_dag_id,
        expected=expected,
        published_at=published_at,
    )
    return QualityPublicationResult(
        evaluation_run_id=window.evaluation_run_id,
        status=RUNNING,
        action="running",
        counts=None,
    )


def publish_quality_success(
    cursor: Any,
    *,
    window: QualityEvaluationWindow,
    target: QualityPublicationTarget | None = None,
    dag_id: str = DAG_ID,
    published_at: datetime | None = None,
) -> QualityPublicationResult:
    """Publish a reconciled quality run, or return no-op for exact SUCCESS replay."""

    target = _validated_target(target)
    safe_dag_id = _safe_text(dag_id, field="dag_id")
    published_at = _published_at(published_at)
    expected = expected_counts_for_window(window)

    ensure_manifest_table(cursor, target)
    existing_rows = _existing_manifest_rows(cursor, target=target, window=window)
    replay = _classify_existing_rows(
        existing_rows,
        window=window,
        dag_id=safe_dag_id,
        expected=expected,
    )
    if replay is not None:
        return replay

    _merge_running(
        cursor,
        target=target,
        window=window,
        dag_id=safe_dag_id,
        expected=expected,
        published_at=published_at,
    )

    candidate = _candidate_counts(cursor, target=target, window=window)
    _require_reconciled_counts(candidate, expected)

    _publish_success(
        cursor,
        target=target,
        window=window,
        dag_id=safe_dag_id,
        expected=expected,
        counts=candidate,
        published_at=published_at,
    )
    _confirm_published_success(
        cursor,
        target=target,
        window=window,
        dag_id=safe_dag_id,
        expected=expected,
    )
    return QualityPublicationResult(
        evaluation_run_id=window.evaluation_run_id,
        status=SUCCESS,
        action="published",
        counts=candidate,
    )


def record_failed_publication(
    cursor: Any,
    *,
    window: QualityEvaluationWindow,
    target: QualityPublicationTarget | None = None,
    dag_id: str = DAG_ID,
    published_at: datetime | None = None,
) -> QualityPublicationResult:
    """Record a diagnostic FAILED marker without persisting raw failure details."""

    target = _validated_target(target)
    safe_dag_id = _safe_text(dag_id, field="dag_id")
    published_at = _published_at(published_at)
    ensure_manifest_table(cursor, target)
    _execute(
        cursor,
        _merge_manifest_sql(
            target=target,
            window=window,
            dag_id=safe_dag_id,
            status=FAILED,
            expected=expected_counts_for_window(window),
            counts=None,
            published_at=published_at,
        ),
        stage="record_failed_publication",
    )
    return QualityPublicationResult(
        evaluation_run_id=window.evaluation_run_id,
        status=FAILED,
        action="failed_diagnostic_recorded",
        counts=None,
    )


def _validated_target(target: QualityPublicationTarget | None) -> QualityPublicationTarget:
    target = target or QualityPublicationTarget()
    _safe_identifier(target.catalog, kind="catalog")
    _safe_identifier(target.schema, kind="schema")
    return target


def _safe_identifier(value: str, *, kind: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER_RE.fullmatch(value):
        raise QualityPublicationError(f"unsafe quality publication {kind} identifier")
    return value


def _safe_text(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise QualityPublicationError(f"unsafe quality publication {field}")
    if len(value) > 200 or any(ord(ch) < 32 for ch in value):
        raise QualityPublicationError(f"unsafe quality publication {field}")
    return value


def _validated_day_count(window: QualityEvaluationWindow) -> int:
    if window.window_end_date < window.window_start_date:
        raise QualityPublicationError("quality publication window is invalid")
    day_count = (window.window_end_date - window.window_start_date).days + 1
    if day_count not in {1, 7}:
        raise QualityPublicationError(
            "quality publication requires exactly one or seven KST evaluation dates"
        )
    return day_count


def _published_at(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None or value.utcoffset() is None:
        raise QualityPublicationError("quality publication published_at must be timezone-aware")
    return value.astimezone(timezone.utc)


def _manifest_ddl(target: QualityPublicationTarget) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {target.manifest_relation} (
    evaluation_run_id varchar,
    dag_id varchar,
    evaluation_as_of timestamp(6),
    window_start_date date,
    window_end_date date,
    status varchar,
    expected_grid_count bigint,
    grid_score_count bigint,
    hourly_count bigint,
    daily_count bigint,
    truth_policy_version varchar,
    vintage_policy_version varchar,
    evidence_policy_version varchar,
    pop_policy_version varchar,
    published_at timestamp(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(window_end_date)']
)
""".strip()


def _existing_manifest_rows(
    cursor: Any,
    *,
    target: QualityPublicationTarget,
    window: QualityEvaluationWindow,
) -> list[Any]:
    sql = f"""
SELECT
    evaluation_run_id,
    dag_id,
    evaluation_as_of,
    window_start_date,
    window_end_date,
    status,
    expected_grid_count,
    grid_score_count,
    hourly_count,
    daily_count,
    truth_policy_version,
    vintage_policy_version,
    evidence_policy_version,
    pop_policy_version
FROM {target.manifest_relation}
WHERE evaluation_run_id = {sql_literal(window.evaluation_run_id)}
   OR (
        status = 'SUCCESS'
        AND evaluation_as_of = {sql_literal(window.evaluation_as_of)}
        AND evaluation_run_id <> {sql_literal(window.evaluation_run_id)}
   )
ORDER BY evaluation_run_id
""".strip()
    _execute(cursor, sql, stage="read_manifest")
    return list(cursor.fetchall())


def _classify_existing_rows(
    rows: Sequence[Any],
    *,
    window: QualityEvaluationWindow,
    dag_id: str,
    expected: ExpectedQualityCounts,
) -> QualityPublicationResult | None:
    for row in rows:
        row_run_id = _row_get(row, "evaluation_run_id", 0)
        status = _row_get(row, "status", 5)
        if row_run_id != window.evaluation_run_id:
            if status == SUCCESS:
                raise QualityPublicationError(
                    "quality publication evaluation identity already has a successful run"
                )
            continue

        if not _row_matches_contract(row, window=window, dag_id=dag_id):
            raise QualityPublicationError(
                "quality publication run id conflicts with existing metadata"
            )
        if status == RUNNING:
            return None
        if status == SUCCESS:
            counts = _counts_from_manifest_row(row)
            if counts != CandidateQualityCounts(
                expected_grid_count=expected.expected_grid_count,
                grid_score_count=expected.grid_score_count,
                hourly_count=expected.hourly_count,
                daily_count=expected.daily_count,
            ):
                raise QualityPublicationError(
                    "quality publication run id conflicts with existing counts"
                )
            return QualityPublicationResult(
                evaluation_run_id=window.evaluation_run_id,
                status=SUCCESS,
                action="replay_noop",
                counts=counts,
            )
        if status in TERMINAL_STATUSES:
            raise QualityPublicationError(
                "quality publication run id already has terminal status"
            )
        raise QualityPublicationError("quality publication run id has unsafe status")
    return None


def _row_matches_contract(row: Any, *, window: QualityEvaluationWindow, dag_id: str) -> bool:
    return (
        _row_get(row, "dag_id", 1) == dag_id
        and _as_iso_timestamp(_row_get(row, "evaluation_as_of", 2))
        == _as_iso_timestamp(window.evaluation_as_of)
        and _as_iso_date(_row_get(row, "window_start_date", 3))
        == window.window_start_date.isoformat()
        and _as_iso_date(_row_get(row, "window_end_date", 4))
        == window.window_end_date.isoformat()
        and _row_get(row, "truth_policy_version", 10) == QUALITY_TRUTH_POLICY_VERSION
        and _row_get(row, "vintage_policy_version", 11) == QUALITY_VINTAGE_POLICY_VERSION
        and _row_get(row, "evidence_policy_version", 12) == QUALITY_EVIDENCE_POLICY_VERSION
        and _row_get(row, "pop_policy_version", 13) == QUALITY_POP_POLICY_VERSION
    )


def _counts_from_manifest_row(row: Any) -> CandidateQualityCounts:
    return CandidateQualityCounts(
        expected_grid_count=int(_row_get(row, "expected_grid_count", 6)),
        grid_score_count=int(_row_get(row, "grid_score_count", 7)),
        hourly_count=int(_row_get(row, "hourly_count", 8)),
        daily_count=int(_row_get(row, "daily_count", 9)),
    )


def _merge_running(
    cursor: Any,
    *,
    target: QualityPublicationTarget,
    window: QualityEvaluationWindow,
    dag_id: str,
    expected: ExpectedQualityCounts,
    published_at: datetime,
) -> None:
    _execute(
        cursor,
        _merge_manifest_sql(
            target=target,
            window=window,
            dag_id=dag_id,
            status=RUNNING,
            expected=expected,
            counts=None,
            published_at=published_at,
        ),
        stage="merge_running",
    )


def _publish_success(
    cursor: Any,
    *,
    target: QualityPublicationTarget,
    window: QualityEvaluationWindow,
    dag_id: str,
    expected: ExpectedQualityCounts,
    counts: CandidateQualityCounts,
    published_at: datetime,
) -> None:
    _execute(
        cursor,
        _merge_manifest_sql(
            target=target,
            window=window,
            dag_id=dag_id,
            status=SUCCESS,
            expected=expected,
            counts=counts,
            published_at=published_at,
        ),
        stage="publish_success",
    )


def _confirm_published_success(
    cursor: Any,
    *,
    target: QualityPublicationTarget,
    window: QualityEvaluationWindow,
    dag_id: str,
    expected: ExpectedQualityCounts,
) -> None:
    rows = _existing_manifest_rows(cursor, target=target, window=window)
    current_successes = [
        row
        for row in rows
        if _row_get(row, "evaluation_run_id", 0) == window.evaluation_run_id
        and _row_get(row, "status", 5) == SUCCESS
    ]
    if len(current_successes) != 1:
        raise QualityPublicationError(
            "quality publication success manifest invariant failed"
        )
    row = current_successes[0]
    if not _row_matches_contract(row, window=window, dag_id=dag_id):
        raise QualityPublicationError(
            "quality publication success manifest invariant failed"
        )
    expected_counts = CandidateQualityCounts(
        expected_grid_count=expected.expected_grid_count,
        grid_score_count=expected.grid_score_count,
        hourly_count=expected.hourly_count,
        daily_count=expected.daily_count,
    )
    if _counts_from_manifest_row(row) != expected_counts:
        raise QualityPublicationError(
            "quality publication success manifest invariant failed"
        )


def _merge_manifest_sql(
    *,
    target: QualityPublicationTarget,
    window: QualityEvaluationWindow,
    dag_id: str,
    status: str,
    expected: ExpectedQualityCounts,
    counts: CandidateQualityCounts | None,
    published_at: datetime,
) -> str:
    grid_score_count = counts.grid_score_count if counts is not None else None
    hourly_count = counts.hourly_count if counts is not None else None
    daily_count = counts.daily_count if counts is not None else None
    values = (
        sql_literal(window.evaluation_run_id),
        sql_literal(dag_id),
        sql_literal(window.evaluation_as_of),
        sql_literal(window.window_start_date),
        sql_literal(window.window_end_date),
        sql_literal(status),
        sql_literal(expected.expected_grid_count),
        sql_literal(grid_score_count),
        sql_literal(hourly_count),
        sql_literal(daily_count),
        sql_literal(QUALITY_TRUTH_POLICY_VERSION),
        sql_literal(QUALITY_VINTAGE_POLICY_VERSION),
        sql_literal(QUALITY_EVIDENCE_POLICY_VERSION),
        sql_literal(QUALITY_POP_POLICY_VERSION),
        sql_literal(published_at),
    )
    columns = """
evaluation_run_id,
dag_id,
evaluation_as_of,
window_start_date,
window_end_date,
status,
expected_grid_count,
grid_score_count,
hourly_count,
daily_count,
truth_policy_version,
vintage_policy_version,
evidence_policy_version,
pop_policy_version,
published_at
""".replace("\n", " ").strip()
    assignments = """
status = source.status,
grid_score_count = source.grid_score_count,
hourly_count = source.hourly_count,
daily_count = source.daily_count,
published_at = source.published_at
""".replace("\n", " ").strip()
    column_names = [column.strip() for column in columns.split(",")]
    source_values = ", ".join(f"source.{column}" for column in column_names)
    return f"""
MERGE INTO {target.manifest_relation} AS target
USING (
    VALUES ({", ".join(values)})
) AS source ({columns})
ON target.evaluation_as_of = source.evaluation_as_of
WHEN MATCHED
    AND target.evaluation_run_id = source.evaluation_run_id
    AND target.status = 'RUNNING'
THEN UPDATE SET {assignments}
WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({source_values})
""".strip()


def _candidate_counts(
    cursor: Any,
    *,
    target: QualityPublicationTarget,
    window: QualityEvaluationWindow,
) -> CandidateQualityCounts:
    grid_history = qualified_relation(
        target.catalog,
        target.schema,
        "gold_weather_forecast_quality_grid_score_history",
    )
    hourly_history = qualified_relation(
        target.catalog,
        target.schema,
        "gold_weather_forecast_quality_hourly_history",
    )
    daily_history = qualified_relation(
        target.catalog,
        target.schema,
        "gold_weather_forecast_quality_daily_history",
    )
    coverage_grid = qualified_relation(
        target.catalog,
        target.schema,
        "dim_weather_coverage_grid",
    )
    run_id = sql_literal(window.evaluation_run_id)
    start_date = sql_literal(window.window_start_date)
    end_date = sql_literal(window.window_end_date)
    valid_at_start = f"TIMESTAMP {sql_literal(f'{window.window_start_date.isoformat()} 00:00:00')}"
    valid_at_end = f"TIMESTAMP {sql_literal(f'{(window.window_end_date + timedelta(days=1)).isoformat()} 00:00:00')}"
    date_scoped_filter = (
        f"evaluation_run_id = {run_id} "
        f"AND evaluation_date_kst BETWEEN {start_date} AND {end_date}"
    )
    valid_at_scoped_filter = (
        f"{date_scoped_filter} "
        f"AND valid_at >= {valid_at_start} "
        f"AND valid_at < {valid_at_end}"
    )
    sql = f"""
WITH expected_dates AS (
    SELECT evaluation_date_kst
    FROM UNNEST(sequence({start_date}, {end_date})) AS dates(evaluation_date_kst)
),
expected_hours AS (
    SELECT
        expected_dates.evaluation_date_kst,
        cast(expected_dates.evaluation_date_kst as timestamp(6))
            + hour_offset * interval '1' hour AS valid_at
    FROM expected_dates
    CROSS JOIN UNNEST(sequence(0, 23)) AS hours(hour_offset)
),
expected_variables AS (
    SELECT variable
    FROM (
        VALUES
            ('temperature_air_2m'),
            ('precipitation_occurrence'),
            ('precipitation_occurrence_category')
    ) AS variables(variable)
),
expected_horizons AS (
    SELECT forecast_horizon
    FROM (VALUES ('D-1'), ('D-2'), ('D-3')) AS horizons(forecast_horizon)
),
expected_grid_population AS (
    SELECT
        expected_hours.evaluation_date_kst,
        expected_hours.valid_at,
        coverage_grid.grid_id,
        expected_variables.variable,
        expected_horizons.forecast_horizon
    FROM expected_hours
    CROSS JOIN {coverage_grid} AS coverage_grid
    CROSS JOIN expected_variables
    CROSS JOIN expected_horizons
),
candidate_grid_population AS (
    SELECT
        evaluation_date_kst,
        valid_at,
        grid_id,
        variable,
        forecast_horizon
    FROM {grid_history}
    WHERE {valid_at_scoped_filter}
),
grid_population_validation AS (
    SELECT
        count(DISTINCT candidate.grid_id) AS expected_grid_count,
        count(candidate.grid_id) AS grid_score_count,
        count_if(expected.grid_id IS NULL OR candidate.grid_id IS NULL) AS mismatch_count
    FROM expected_grid_population AS expected
    FULL OUTER JOIN candidate_grid_population AS candidate
        ON expected.evaluation_date_kst = candidate.evaluation_date_kst
       AND expected.valid_at = candidate.valid_at
       AND expected.grid_id = candidate.grid_id
       AND expected.variable = candidate.variable
       AND expected.forecast_horizon = candidate.forecast_horizon
),
expected_hourly_population AS (
    SELECT
        expected_hours.evaluation_date_kst,
        expected_hours.valid_at,
        expected_horizons.forecast_horizon
    FROM expected_hours
    CROSS JOIN expected_horizons
),
candidate_hourly_population AS (
    SELECT
        evaluation_date_kst,
        valid_at,
        forecast_horizon
    FROM {hourly_history}
    WHERE {valid_at_scoped_filter}
),
hourly_population_validation AS (
    SELECT
        count(candidate.valid_at) AS hourly_count,
        count_if(expected.valid_at IS NULL OR candidate.valid_at IS NULL) AS mismatch_count
    FROM expected_hourly_population AS expected
    FULL OUTER JOIN candidate_hourly_population AS candidate
        ON expected.evaluation_date_kst = candidate.evaluation_date_kst
       AND expected.valid_at = candidate.valid_at
       AND expected.forecast_horizon = candidate.forecast_horizon
),
expected_daily_population AS (
    SELECT
        expected_dates.evaluation_date_kst,
        expected_horizons.forecast_horizon
    FROM expected_dates
    CROSS JOIN expected_horizons
),
candidate_daily_population AS (
    SELECT
        evaluation_date_kst,
        forecast_horizon
    FROM {daily_history}
    WHERE {date_scoped_filter}
),
daily_population_validation AS (
    SELECT
        count(candidate.evaluation_date_kst) AS daily_count,
        count_if(
            expected.evaluation_date_kst IS NULL
            OR candidate.evaluation_date_kst IS NULL
        ) AS mismatch_count
    FROM expected_daily_population AS expected
    FULL OUTER JOIN candidate_daily_population AS candidate
        ON expected.evaluation_date_kst = candidate.evaluation_date_kst
       AND expected.forecast_horizon = candidate.forecast_horizon
)
SELECT
    grid_population_validation.expected_grid_count,
    grid_population_validation.grid_score_count,
    hourly_population_validation.hourly_count,
    daily_population_validation.daily_count,
    grid_population_validation.mismatch_count AS grid_population_mismatch_count,
    hourly_population_validation.mismatch_count AS hourly_population_mismatch_count,
    daily_population_validation.mismatch_count AS daily_population_mismatch_count
FROM grid_population_validation
CROSS JOIN hourly_population_validation
CROSS JOIN daily_population_validation
""".strip()
    _execute(cursor, sql, stage="read_candidate_counts")
    rows = list(cursor.fetchall())
    if len(rows) != 1:
        raise QualityPublicationError("quality publication candidate counts are missing")
    row = rows[0]
    return CandidateQualityCounts(
        expected_grid_count=int(_row_get(row, "expected_grid_count", 0) or 0),
        grid_score_count=int(_row_get(row, "grid_score_count", 1) or 0),
        hourly_count=int(_row_get(row, "hourly_count", 2) or 0),
        daily_count=int(_row_get(row, "daily_count", 3) or 0),
        grid_population_mismatch_count=int(
            _row_get(row, "grid_population_mismatch_count", 4) or 0
        ),
        hourly_population_mismatch_count=int(
            _row_get(row, "hourly_population_mismatch_count", 5) or 0
        ),
        daily_population_mismatch_count=int(
            _row_get(row, "daily_population_mismatch_count", 6) or 0
        ),
    )


def _require_reconciled_counts(
    candidate: CandidateQualityCounts,
    expected: ExpectedQualityCounts,
) -> None:
    if (
        expected.expected_grid_count <= 0
        or expected.grid_score_count <= 0
        or expected.hourly_count <= 0
        or expected.daily_count <= 0
    ):
        raise QualityPublicationError("quality publication expected counts must be nonzero")
    if (
        candidate.expected_grid_count != expected.expected_grid_count
        or candidate.grid_score_count != expected.grid_score_count
        or candidate.hourly_count != expected.hourly_count
        or candidate.daily_count != expected.daily_count
    ):
        raise QualityPublicationError("quality publication candidate counts do not reconcile")
    if (
        candidate.grid_population_mismatch_count != 0
        or candidate.hourly_population_mismatch_count != 0
        or candidate.daily_population_mismatch_count != 0
    ):
        raise QualityPublicationError(
            "quality publication candidate population does not reconcile"
        )


def _execute(cursor: Any, sql: str, *, stage: str) -> None:
    failed = False
    try:
        cursor.execute(sql)
    except Exception:  # noqa: BLE001 - sanitize connector failures
        failed = True
    if failed:
        raise QualityPublicationError(
            f"quality publication SQL execution failed during {stage}"
        )


def _row_get(row: Any, name: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row.get(name)
    return row[index]


def _as_iso_date(value: Any) -> str:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _as_iso_timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.replace(tzinfo=timezone.utc).isoformat()
    return str(value).replace(" ", "T")


def sql_literal(value: str | int | date | datetime | None) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        raise QualityPublicationError("quality publication does not literalize booleans")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise QualityPublicationError("quality publication timestamp must be timezone-aware")
        return f"TIMESTAMP {sql_literal(value.astimezone(timezone.utc).replace(tzinfo=None).isoformat(sep=' '))}"
    if isinstance(value, date):
        return f"DATE {sql_literal(value.isoformat())}"
    if isinstance(value, str):
        _safe_text(value, field="literal")
        return "'" + value.replace("'", "''") + "'"
    raise QualityPublicationError("quality publication cannot literalize value")
