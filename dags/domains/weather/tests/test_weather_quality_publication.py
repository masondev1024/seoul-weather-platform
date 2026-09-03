from __future__ import annotations

import sys
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_quality_publication import (  # noqa: E402
    DAG_ID,
    EXPECTED_GRID_COUNT,
    FAILED,
    MANIFEST_TABLE_NAME,
    SUCCESS,
    CandidateQualityCounts,
    QualityPublicationError,
    QualityPublicationTarget,
    begin_quality_publication,
    ensure_manifest_table,
    expected_counts_for_window,
    publish_quality_success,
    record_failed_publication,
)
from weather_quality_runtime import (  # noqa: E402
    QUALITY_EVIDENCE_POLICY_VERSION,
    QUALITY_POP_POLICY_VERSION,
    QUALITY_TRUTH_POLICY_VERSION,
    QUALITY_VINTAGE_POLICY_VERSION,
    QualityEvaluationWindow,
    resolve_backfill_quality_window,
    resolve_daily_quality_window,
)


KST = ZoneInfo("Asia/Seoul")
PUBLISHED_AT = datetime(2026, 8, 22, 0, 5, tzinfo=timezone.utc)


class FakeCursor:
    def __init__(
        self,
        *,
        existing_rows=None,
        manifest_reads=None,
        candidate_counts=None,
        fail_on=None,
    ):
        self.existing_rows = list(existing_rows or [])
        self.manifest_reads = [list(rows) for rows in manifest_reads or []]
        self.candidate_counts = candidate_counts
        self.fail_on = fail_on
        self.statements = []
        self._last_result = []

    def execute(self, statement):
        self.statements.append(statement)
        if self.fail_on and self.fail_on in statement:
            raise RuntimeError("SELECT * FROM secrets WHERE password='raw-secret'")
        normalized = " ".join(statement.split())
        if f"FROM iceberg.weather.{MANIFEST_TABLE_NAME}" in normalized and normalized.startswith("SELECT"):
            self._last_result = self.manifest_reads.pop(0) if self.manifest_reads else self.existing_rows
        elif "gold_weather_forecast_quality_grid_score_history" in normalized and normalized.startswith(("SELECT", "WITH")):
            self._last_result = [self.candidate_counts] if self.candidate_counts is not None else []
        else:
            self._last_result = []

    def fetchall(self):
        return self._last_result


def _daily_window(run_id="scheduled__quality"):
    return resolve_daily_quality_window(
        now=datetime(2026, 8, 22, 3, 5, tzinfo=KST),
        run_id=run_id,
    )


def _backfill_window(run_id="manual__2026_08_20"):
    return resolve_backfill_quality_window(
        backfill_date="2026-08-20",
        confirmation="BACKFILL_ONE_KST_DATE",
        now=datetime(2026, 8, 22, 3, 5, tzinfo=KST),
        run_id=run_id,
    )


def _expected_candidate(window):
    expected = expected_counts_for_window(window)
    return {
        "expected_grid_count": expected.expected_grid_count,
        "grid_score_count": expected.grid_score_count,
        "hourly_count": expected.hourly_count,
        "daily_count": expected.daily_count,
    }


def _success_manifest_row(window):
    counts = _expected_candidate(window)
    return {
        "evaluation_run_id": window.evaluation_run_id,
        "dag_id": DAG_ID,
        "evaluation_as_of": window.evaluation_as_of,
        "window_start_date": window.window_start_date,
        "window_end_date": window.window_end_date,
        "status": SUCCESS,
        "expected_grid_count": counts["expected_grid_count"],
        "grid_score_count": counts["grid_score_count"],
        "hourly_count": counts["hourly_count"],
        "daily_count": counts["daily_count"],
        "truth_policy_version": QUALITY_TRUTH_POLICY_VERSION,
        "vintage_policy_version": QUALITY_VINTAGE_POLICY_VERSION,
        "evidence_policy_version": QUALITY_EVIDENCE_POLICY_VERSION,
        "pop_policy_version": QUALITY_POP_POLICY_VERSION,
    }


def test_expected_count_formulas_for_daily_and_backfill_windows():
    daily = expected_counts_for_window(_daily_window())
    assert daily.expected_grid_count == EXPECTED_GRID_COUNT == 80
    assert daily.grid_score_count == 7 * 80 * 24 * 3 * 3
    assert daily.hourly_count == 7 * 24 * 3
    assert daily.daily_count == 7 * 3

    backfill = expected_counts_for_window(_backfill_window())
    assert backfill.grid_score_count == 1 * 80 * 24 * 3 * 3
    assert backfill.hourly_count == 1 * 24 * 3
    assert backfill.daily_count == 1 * 3


def test_manifest_ddl_is_idempotent_and_only_targets_iceberg_manifest():
    cursor = FakeCursor()
    ensure_manifest_table(cursor)

    ddl = cursor.statements[-1]
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS")
    assert f"iceberg.weather.{MANIFEST_TABLE_NAME}" in ddl
    for field in [
        "evaluation_run_id",
        "dag_id",
        "evaluation_as_of",
        "window_start_date",
        "window_end_date",
        "status",
        "expected_grid_count",
        "grid_score_count",
        "hourly_count",
        "daily_count",
        "truth_policy_version",
        "vintage_policy_version",
        "evidence_policy_version",
        "pop_policy_version",
        "published_at",
    ]:
        assert field in ddl


def test_publish_success_merges_running_reconciles_current_run_and_marks_success():
    window = _daily_window()
    cursor = FakeCursor(
        manifest_reads=[[], [_success_manifest_row(window)]],
        candidate_counts=_expected_candidate(window),
    )

    result = publish_quality_success(cursor, window=window, published_at=PUBLISHED_AT)

    assert result.status == SUCCESS
    assert result.action == "published"
    assert result.counts == CandidateQualityCounts(**_expected_candidate(window))
    merged = [s for s in cursor.statements if s.startswith("MERGE INTO")]
    assert len(merged) == 2
    assert "'RUNNING'" in merged[0]
    assert "'SUCCESS'" in merged[1]
    assert "ON target.evaluation_as_of = source.evaluation_as_of" in merged[0]
    count_sql = next(
        statement
        for statement in cursor.statements
        if "grid_population_validation" in statement
    )
    assert f"evaluation_run_id = '{window.evaluation_run_id}'" in count_sql
    assert "evaluation_date_kst BETWEEN DATE '2026-08-15' AND DATE '2026-08-21'" in count_sql
    assert "gold_weather_forecast_quality_grid_score_history" in count_sql
    assert "gold_weather_forecast_quality_hourly_history" in count_sql
    assert "gold_weather_forecast_quality_daily_history" in count_sql


def test_begin_publication_marks_running_before_dbt_without_candidate_reads():
    window = _daily_window()
    cursor = FakeCursor()

    result = begin_quality_publication(cursor, window=window, published_at=PUBLISHED_AT)

    assert result.status == "RUNNING"
    assert result.action == "running"
    merged = [s for s in cursor.statements if s.startswith("MERGE INTO")]
    assert len(merged) == 1
    assert "'RUNNING'" in merged[0]
    assert not any("gold_weather_forecast_quality_grid_score_history" in s for s in cursor.statements)


def test_candidate_mismatch_fails_closed_without_success():
    window = _daily_window()
    bad_counts = _expected_candidate(window)
    bad_counts["hourly_count"] -= 1
    cursor = FakeCursor(candidate_counts=bad_counts)

    with pytest.raises(QualityPublicationError, match="candidate counts do not reconcile"):
        publish_quality_success(cursor, window=window, published_at=PUBLISHED_AT)

    success_merges = [s for s in cursor.statements if s.startswith("MERGE INTO") and "'SUCCESS'" in s]
    assert success_merges == []


def test_candidate_population_gap_fails_closed_even_when_aggregate_counts_match():
    window = _daily_window()
    candidate = _expected_candidate(window)
    candidate["grid_population_mismatch_count"] = 1
    cursor = FakeCursor(
        manifest_reads=[[], [_success_manifest_row(window)]],
        candidate_counts=candidate,
    )

    with pytest.raises(QualityPublicationError, match="candidate population"):
        publish_quality_success(cursor, window=window, published_at=PUBLISHED_AT)

    candidate_sql = next(
        statement
        for statement in cursor.statements
        if "gold_weather_forecast_quality_grid_score_history" in statement
        and statement.startswith("WITH")
    )
    assert "expected_grid_population" in candidate_sql
    assert "grid_population_mismatch_count" in candidate_sql
    for key in (
        "evaluation_date_kst",
        "valid_at",
        "grid_id",
        "variable",
        "forecast_horizon",
    ):
        assert f"expected.{key} = candidate.{key}" in candidate_sql
    assert "valid_at >= TIMESTAMP '2026-08-15 00:00:00'" in candidate_sql
    assert "valid_at < TIMESTAMP '2026-08-22 00:00:00'" in candidate_sql
    assert candidate_sql.count("FROM iceberg.weather.gold_weather_forecast_quality_grid_score_history") == 1
    assert candidate_sql.count("FROM iceberg.weather.gold_weather_forecast_quality_hourly_history") == 1
    assert candidate_sql.count("FROM iceberg.weather.gold_weather_forecast_quality_daily_history") == 1
    assert "count_if(expected.grid_id IS NULL OR candidate.grid_id IS NULL)" in candidate_sql
    assert "WHERE expected.grid_id IS NULL OR candidate.grid_id IS NULL" not in candidate_sql
    success_merges = [
        statement
        for statement in cursor.statements
        if statement.startswith("MERGE INTO") and "'SUCCESS'" in statement
    ]
    assert success_merges == []


def _grid_population_validation_result(candidate_grid_id: str) -> tuple[int, int, int]:
    connection = sqlite3.connect(":memory:")
    try:
        result = connection.execute(
            """
            WITH expected_population (
                evaluation_date_kst, valid_at, grid_id, variable, forecast_horizon
            ) AS (
                VALUES
                    ('2026-08-15', '2026-08-15 00:00:00', 'kma_60_127', 'temperature_air_2m', 'D-1'),
                    ('2026-08-15', '2026-08-15 00:00:00', 'kma_60_128', 'temperature_air_2m', 'D-1')
            ),
            candidate_population (
                evaluation_date_kst, valid_at, grid_id, variable, forecast_horizon
            ) AS (
                VALUES
                    ('2026-08-15', '2026-08-15 00:00:00', 'kma_60_127', 'temperature_air_2m', 'D-1'),
                    ('2026-08-15', '2026-08-15 00:00:00', ?, 'temperature_air_2m', 'D-1')
            )
            SELECT
                count(DISTINCT candidate.grid_id),
                count(candidate.grid_id),
                count(*) FILTER (
                    WHERE expected.grid_id IS NULL OR candidate.grid_id IS NULL
                )
            FROM expected_population AS expected
            FULL OUTER JOIN candidate_population AS candidate
                ON expected.evaluation_date_kst = candidate.evaluation_date_kst
               AND expected.valid_at = candidate.valid_at
               AND expected.grid_id = candidate.grid_id
               AND expected.variable = candidate.variable
               AND expected.forecast_horizon = candidate.forecast_horizon
            """,
            (candidate_grid_id,),
        ).fetchone()
    finally:
        connection.close()

    return result


def test_equal_grid_aggregate_with_missing_and_extra_key_has_symmetric_difference():
    expected_grid_count, grid_score_count, mismatch_count = _grid_population_validation_result(
        "unexpected_grid"
    )

    assert expected_grid_count == 2
    assert grid_score_count == 2
    assert mismatch_count == 2


def test_perfect_grid_population_keeps_full_counts_and_has_no_mismatch():
    expected_grid_count, grid_score_count, mismatch_count = _grid_population_validation_result(
        "kma_60_128"
    )

    assert expected_grid_count == 2
    assert grid_score_count == 2
    assert mismatch_count == 0


def test_exact_success_replay_is_noop_without_extra_success():
    window = _daily_window()
    cursor = FakeCursor(existing_rows=[_success_manifest_row(window)])

    result = publish_quality_success(cursor, window=window, published_at=PUBLISHED_AT)

    assert result.action == "replay_noop"
    assert result.status == SUCCESS
    assert not any(s.startswith("MERGE INTO") for s in cursor.statements)


def test_exact_success_replay_accepts_naive_utc_wall_clock_manifest_timestamp():
    window = _daily_window()
    existing = _success_manifest_row(window)
    existing["evaluation_as_of"] = window.evaluation_as_of.replace(tzinfo=None)
    cursor = FakeCursor(existing_rows=[existing])

    result = publish_quality_success(cursor, window=window, published_at=PUBLISHED_AT)

    assert result.action == "replay_noop"
    assert result.status == SUCCESS
    assert not any(s.startswith("MERGE INTO") for s in cursor.statements)


def test_conflicting_same_run_metadata_or_same_evaluation_identity_raises():
    window = _daily_window()
    conflicting = _success_manifest_row(window)
    conflicting["dag_id"] = "different_dag"

    with pytest.raises(QualityPublicationError, match="conflicts"):
        publish_quality_success(
            FakeCursor(existing_rows=[conflicting]),
            window=window,
            published_at=PUBLISHED_AT,
        )

    reused = _success_manifest_row(window)
    reused["evaluation_run_id"] = "scheduled__other_quality"
    with pytest.raises(QualityPublicationError, match="evaluation identity"):
        publish_quality_success(
            FakeCursor(existing_rows=[reused]),
            window=window,
            published_at=PUBLISHED_AT,
        )


def test_overlapping_prior_repair_window_does_not_block_next_daily_success():
    window = _daily_window()
    prior_window = resolve_daily_quality_window(
        now=datetime(2026, 8, 21, 3, 5, tzinfo=KST),
        run_id="scheduled__prior_quality",
    )
    cursor = FakeCursor(
        manifest_reads=[[], [_success_manifest_row(window)]],
        candidate_counts=_expected_candidate(window),
    )

    result = publish_quality_success(cursor, window=window, published_at=PUBLISHED_AT)

    assert prior_window.window_end_date == date(2026, 8, 20)
    assert prior_window.window_start_date <= window.window_start_date <= prior_window.window_end_date
    assert result.status == SUCCESS
    manifest_sql = next(s for s in cursor.statements if s.startswith("SELECT") and MANIFEST_TABLE_NAME in s)
    assert "evaluation_as_of" in manifest_sql
    assert "window_start_date <=" not in manifest_sql


def test_post_success_manifest_invariant_fails_closed_on_concurrent_identity_collision():
    window = _daily_window()
    colliding = _success_manifest_row(window)
    colliding["evaluation_run_id"] = "scheduled__collision_quality"
    cursor = FakeCursor(
        manifest_reads=[[], [colliding]],
        candidate_counts=_expected_candidate(window),
    )

    with pytest.raises(QualityPublicationError, match="success manifest invariant"):
        publish_quality_success(cursor, window=window, published_at=PUBLISHED_AT)

    success_merge = next(
        s for s in cursor.statements if s.startswith("MERGE INTO") and "'SUCCESS'" in s
    )
    assert "ON target.evaluation_as_of = source.evaluation_as_of" in success_merge
    assert "target.evaluation_run_id = source.evaluation_run_id" in success_merge


def test_unsafe_identifiers_and_bad_runtime_window_are_rejected():
    window = _daily_window()
    with pytest.raises(QualityPublicationError, match="catalog identifier"):
        publish_quality_success(
            FakeCursor(candidate_counts=_expected_candidate(window)),
            window=window,
            target=QualityPublicationTarget(catalog="iceberg;drop", schema="weather"),
            published_at=PUBLISHED_AT,
        )

    bad_window = QualityEvaluationWindow(
        evaluation_run_id="manual__bad_window",
        evaluation_as_of=window.evaluation_as_of,
        window_start_date=date(2026, 8, 1),
        window_end_date=date(2026, 8, 2),
        forecast_load_start_date=date(2026, 7, 28),
        forecast_load_end_date=date(2026, 8, 1),
    )
    with pytest.raises(QualityPublicationError, match="exactly one or seven"):
        publish_quality_success(
            FakeCursor(candidate_counts=_expected_candidate(window)),
            window=bad_window,
            published_at=PUBLISHED_AT,
        )


def test_zero_or_missing_candidate_counts_do_not_publish_success():
    window = _daily_window()
    zero_counts = _expected_candidate(window)
    zero_counts["expected_grid_count"] = 0
    with pytest.raises(QualityPublicationError, match="candidate counts do not reconcile"):
        publish_quality_success(
            FakeCursor(candidate_counts=zero_counts),
            window=window,
            published_at=PUBLISHED_AT,
        )

    with pytest.raises(QualityPublicationError, match="candidate counts are missing"):
        publish_quality_success(
            FakeCursor(candidate_counts=None),
            window=window,
            published_at=PUBLISHED_AT,
        )


def test_failed_diagnostic_records_only_status_without_raw_error_detail():
    window = _backfill_window()
    cursor = FakeCursor()

    result = record_failed_publication(cursor, window=window, published_at=PUBLISHED_AT)

    assert result.status == FAILED
    failed_merge = next(s for s in cursor.statements if s.startswith("MERGE INTO"))
    assert "'FAILED'" in failed_merge
    assert "raw-secret" not in failed_merge
    assert "password" not in failed_merge
    assert "error" not in failed_merge.lower()


def test_connector_errors_are_sanitized_without_sql_or_credentials():
    window = _daily_window()
    cursor = FakeCursor(
        candidate_counts=_expected_candidate(window),
        fail_on="grid_population_validation",
    )

    with pytest.raises(QualityPublicationError) as exc_info:
        publish_quality_success(cursor, window=window, published_at=PUBLISHED_AT)

    message = str(exc_info.value)
    assert "read_candidate_counts" in message
    assert "raw-secret" not in message
    assert "password" not in message
    assert "SELECT" not in message
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


def test_publication_module_does_not_reference_d1_worker_or_serving_publishers():
    source = Path(__file__).resolve().parents[1].joinpath("weather_quality_publication.py").read_text()

    assert "D1" not in source
    assert "Worker" not in source
    assert "serving" not in source.lower()
