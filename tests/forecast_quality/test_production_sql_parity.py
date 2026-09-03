from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import re

import pytest


ROOT = Path(__file__).resolve().parents[2]
QUALITY_MODELS = ROOT / "dbt/domains/traffic_weather/models/weather/quality"
MATCH_SQL = QUALITY_MODELS / "silver/silver_weather_forecast_observation_match.sql"
HISTORY_SQL = QUALITY_MODELS / "gold/gold_weather_forecast_quality_grid_score_history.sql"
VIEW_SQL = QUALITY_MODELS / "gold/gold_weather_forecast_quality_grid_score.sql"
HOURLY_HISTORY_SQL = (
    QUALITY_MODELS / "gold/gold_weather_forecast_quality_hourly_history.sql"
)
HOURLY_VIEW_SQL = QUALITY_MODELS / "gold/gold_weather_forecast_quality_hourly.sql"
DAILY_HISTORY_SQL = (
    QUALITY_MODELS / "gold/gold_weather_forecast_quality_daily_history.sql"
)
DAILY_VIEW_SQL = QUALITY_MODELS / "gold/gold_weather_forecast_quality_daily.sql"
MATCH_TEST_SQL = (
    ROOT
    / "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_match_unique.sql"
)
RECONCILE_TEST_SQL = (
    ROOT
    / "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_grid_score_reconciles.sql"
)
HOURLY_RECONCILE_TEST_SQL = (
    ROOT
    / "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_hourly_reconciles.sql"
)
DAILY_RECONCILE_TEST_SQL = (
    ROOT
    / "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_daily_reconciles.sql"
)


@dataclass(frozen=True)
class InclusiveWindow:
    lower_hours: int
    upper_hours: int


@dataclass(frozen=True)
class Candidate:
    horizon: str
    issued_hours_before_valid: float
    source_revision: str
    source_run_id: str
    value: float | str | None
    collected_at: datetime = datetime(2026, 8, 21, 17, 0)
    manifest_event_at_utc: datetime = datetime(2026, 8, 21, 17, 5)
    value_status: str = "valid"
    value_kind: str = "continuous"
    unit: str = "degC"


@dataclass(frozen=True)
class Truth:
    value: float | str | bool | None
    truth_status: str = "provisional"
    value_kind: str = "continuous"
    unit: str = "degC"


@dataclass(frozen=True)
class MatchCase:
    variable: str
    candidate: Candidate | None
    truth: Truth | None


CANONICAL_CONTRACTS = {
    "temperature_air_2m": ("continuous", "continuous", "degC"),
    "precipitation_occurrence": ("probability", "binary", "1"),
    "precipitation_occurrence_category": ("categorical", "categorical", "category"),
}


class ProductionSqlContract:
    def __init__(self, sql_path: Path = MATCH_SQL) -> None:
        self.sql_path = sql_path
        self.sql = sql_path.read_text(encoding="utf-8")

    def window(self, horizon: str) -> InclusiveWindow:
        match = re.search(
            rf"'{re.escape(horizon)}'\s+as\s+forecast_horizon,\s+"
            r"(?P<lower>\d+)\s+as\s+lower_hours_before_valid,\s+"
            r"(?P<upper>\d+)\s+as\s+upper_hours_before_valid",
            self.sql,
        )
        assert match, f"{horizon} vintage window is not declared in production SQL"
        return InclusiveWindow(int(match.group("lower")), int(match.group("upper")))


def classify(case: MatchCase) -> str:
    if case.truth is None:
        return "missing_truth"
    if case.candidate is None:
        return "missing_vintage"
    forecast_kind, truth_kind, unit = CANONICAL_CONTRACTS[case.variable]
    if (
        case.candidate.value_kind != forecast_kind
        or case.truth.value_kind != truth_kind
        or case.candidate.unit != unit
        or case.truth.unit != unit
    ):
        return "incompatible_contract"
    if case.candidate.value_status != "valid":
        return "invalid_forecast"
    if case.truth.truth_status == "invalid_truth":
        return "invalid_truth"
    return "matched"


def visible_forecast_candidates(
    candidates: list[Candidate],
    evaluation_as_of: datetime,
) -> list[Candidate]:
    return [
        candidate
        for candidate in candidates
        if candidate.collected_at <= evaluation_as_of
        and candidate.manifest_event_at_utc <= evaluation_as_of
    ]


def continuous_components(forecast: float, observed: float) -> dict[str, float]:
    error = forecast - observed
    return {
        "forecast_value": forecast,
        "observed_value": observed,
        "error": error,
        "absolute_error": abs(error),
        "squared_error": error * error,
    }


def probability_components(probability: float, observed: bool) -> dict[str, float | bool | int]:
    observed_num = 1 if observed else 0
    predicted = probability >= 0.5
    return {
        "forecast_probability": probability,
        "observed_occurrence": observed,
        "brier_component": (probability - observed_num) ** 2,
        "predicted_occurrence": predicted,
        "true_positive": int(predicted and observed),
        "false_positive": int(predicted and not observed),
        "true_negative": int((not predicted) and (not observed)),
        "false_negative": int((not predicted) and observed),
    }


def ratio_or_none(numerator: float, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def aggregate_quality_components(
    temperatures: list[tuple[float, float]],
    probabilities: list[tuple[float, bool]],
) -> dict[str, float | int | None]:
    temperature_errors = [forecast - observed for forecast, observed in temperatures]
    pop_components = [
        probability_components(probability, observed)
        for probability, observed in probabilities
    ]
    true_positive = sum(int(component["true_positive"]) for component in pop_components)
    false_positive = sum(int(component["false_positive"]) for component in pop_components)
    true_negative = sum(int(component["true_negative"]) for component in pop_components)
    false_negative = sum(int(component["false_negative"]) for component in pop_components)
    brier_sum = sum(float(component["brier_component"]) for component in pop_components)

    return {
        "temperature_sample_count": len(temperature_errors),
        "temperature_mae": ratio_or_none(sum(abs(error) for error in temperature_errors), len(temperature_errors)),
        "temperature_rmse": (sum(error * error for error in temperature_errors) / len(temperature_errors)) ** 0.5
        if temperature_errors
        else None,
        "temperature_bias": ratio_or_none(sum(temperature_errors), len(temperature_errors)),
        "precipitation_sample_count": len(pop_components),
        "precipitation_brier_score": ratio_or_none(brier_sum, len(pop_components)),
        "precipitation_true_positive_count": true_positive,
        "precipitation_false_positive_count": false_positive,
        "precipitation_true_negative_count": true_negative,
        "precipitation_false_negative_count": false_negative,
        "precipitation_precision": ratio_or_none(true_positive, true_positive + false_positive),
        "precipitation_recall": ratio_or_none(true_positive, true_positive + false_negative),
        "precipitation_f1": ratio_or_none(2 * true_positive, 2 * true_positive + false_positive + false_negative),
    }


@pytest.mark.parametrize(
    ("horizon", "lower_hours", "upper_hours"),
    [("D-1", 27, 24), ("D-2", 51, 48), ("D-3", 75, 72)],
)
def test_vintage_windows_are_inclusive(
    horizon: str, lower_hours: int, upper_hours: int
) -> None:
    contract = ProductionSqlContract()

    assert contract.window(horizon) == InclusiveWindow(lower_hours, upper_hours)
    assert (
        "between expected_population.valid_at - interval '1' hour * expected_population.lower_hours_before_valid"
        in contract.sql
    )
    assert (
        "and expected_population.valid_at - interval '1' hour * expected_population.upper_hours_before_valid"
        in contract.sql
    )


def test_vintage_candidate_selection_uses_inclusive_bounds_and_deterministic_tiebreak() -> None:
    valid_at = datetime(2026, 8, 20, 12)
    candidates = [
        Candidate("D-1", 27, "r1", "run-a", 19.0),
        Candidate("D-1", 24, "r1", "run-b", 20.0),
        Candidate("D-1", 23.999, "outside-newer", "run-z", 99.0),
        Candidate("D-1", 24, "r2", "run-a", 21.0),
        Candidate("D-1", 24, "r2", "run-c", 22.0),
    ]
    window = InclusiveWindow(27, 24)
    in_window = [
        candidate
        for candidate in candidates
        if valid_at - timedelta(hours=window.lower_hours)
        <= valid_at - timedelta(hours=candidate.issued_hours_before_valid)
        <= valid_at - timedelta(hours=window.upper_hours)
    ]
    selected = sorted(
        in_window,
        key=lambda candidate: (
            valid_at - timedelta(hours=candidate.issued_hours_before_valid),
            candidate.source_revision,
            candidate.source_run_id,
        ),
        reverse=True,
    )[0]

    assert selected.value == 22.0

    sql = ProductionSqlContract().sql
    assert "row_number() over" in sql
    assert (
        "order by issued_at desc, source_revision desc, source_run_id desc"
        in " ".join(sql.split())
    )
    assert "candidate_rank = 1" in sql


def test_vintage_candidate_selection_excludes_forecast_evidence_not_visible_as_of() -> None:
    evaluation_as_of = datetime(2026, 8, 21, 18, 5)
    valid_at = datetime(2026, 8, 20, 12)
    window = InclusiveWindow(27, 24)
    candidates = [
        Candidate(
            "D-1",
            24,
            "r1",
            "run-a",
            20.0,
            collected_at=datetime(2026, 8, 21, 17, 0),
            manifest_event_at_utc=datetime(2026, 8, 21, 17, 5),
        ),
        Candidate(
            "D-1",
            24,
            "r2",
            "run-b",
            99.0,
            collected_at=datetime(2026, 8, 21, 18, 6),
            manifest_event_at_utc=datetime(2026, 8, 21, 18, 4),
        ),
        Candidate(
            "D-1",
            24,
            "r3",
            "run-c",
            100.0,
            collected_at=datetime(2026, 8, 21, 18, 4),
            manifest_event_at_utc=datetime(2026, 8, 21, 18, 6),
        ),
    ]

    in_window = [
        candidate
        for candidate in visible_forecast_candidates(candidates, evaluation_as_of)
        if valid_at - timedelta(hours=window.lower_hours)
        <= valid_at - timedelta(hours=candidate.issued_hours_before_valid)
        <= valid_at - timedelta(hours=window.upper_hours)
    ]
    selected = sorted(
        in_window,
        key=lambda candidate: (
            valid_at - timedelta(hours=candidate.issued_hours_before_valid),
            candidate.source_revision,
            candidate.source_run_id,
        ),
        reverse=True,
    )[0]

    assert selected.value == 20.0

    sql = ProductionSqlContract().sql
    normalized = " ".join(sql.split())
    assert "forecast_scope.collected_at <= cast({{ evaluation_as_of }} as timestamp(6))" in normalized
    assert (
        "forecast_scope.manifest_event_at_utc <= cast({{ evaluation_as_of }} as timestamp(6))"
        in normalized
    )


def test_match_state_oracle_covers_every_explicit_failure_state_and_no_fallback() -> None:
    cases = [
        MatchCase("temperature_air_2m", Candidate("D-1", 24, "r1", "run", 12.0), Truth(10.0)),
        MatchCase("temperature_air_2m", None, Truth(10.0)),
        MatchCase("temperature_air_2m", Candidate("D-1", 24, "r1", "run", 12.0), None),
        MatchCase("temperature_air_2m", Candidate("D-1", 24, "r1", "run", None, value_status="invalid"), Truth(10.0)),
        MatchCase("temperature_air_2m", Candidate("D-1", 24, "r1", "run", 12.0), Truth(None, "invalid_truth")),
        MatchCase("precipitation_occurrence_category", Candidate("D-1", 24, "r1", "run", "wet", value_kind="categorical", unit="category"), Truth(True, value_kind="binary", unit="1")),
    ]

    assert [classify(case) for case in cases] == [
        "matched",
        "missing_vintage",
        "missing_truth",
        "invalid_forecast",
        "invalid_truth",
        "incompatible_contract",
    ]

    sql = ProductionSqlContract().sql
    for state in [
        "matched",
        "missing_vintage",
        "missing_truth",
        "invalid_forecast",
        "invalid_truth",
        "incompatible_contract",
    ]:
        assert f"'{state}'" in sql
    assert "else 'matched'" not in sql.lower()


def test_match_state_oracle_is_variable_aware_for_pop_contract() -> None:
    matched_pop = MatchCase(
        "precipitation_occurrence",
        Candidate("D-1", 24, "r1", "run", 0.7, value_kind="probability", unit="1"),
        Truth(True, value_kind="binary", unit="1"),
    )
    invalid_pop = MatchCase(
        "precipitation_occurrence",
        Candidate("D-1", 24, "r1", "run", 0.7, value_kind="continuous", unit="degC"),
        Truth(True, value_kind="binary", unit="1"),
    )

    assert classify(matched_pop) == "matched"
    assert classify(invalid_pop) == "incompatible_contract"

    sql = ProductionSqlContract().sql
    normalized = " ".join(sql.split())
    assert (
        "expected_population.variable = 'precipitation_occurrence' "
        "and selected_forecast.value_kind = 'probability' "
        "and selected_truth.value_kind = 'binary' "
        "and selected_forecast.unit = '1' "
        "and selected_truth.unit = '1'"
    ) in normalized


def test_expected_population_is_independent_of_truth_rows_and_keeps_missing_truth_observable() -> None:
    sql = ProductionSqlContract().sql

    assert "from {{ ref('dim_weather_coverage_grid') }}" in sql
    assert "sequence(0, 23)" in sql
    assert "cross join canonical_variables" in sql
    assert "cross join vintage_windows" in sql
    assert "left join selected_truth" in sql
    assert "left join selected_forecast" in sql
    assert "select distinct observed_at" not in sql
    assert "from {{ ref('silver_kma_observation_truth') }} as expected" not in sql


def test_score_components_match_hand_derived_temperature_pop_and_pty_contracts() -> None:
    assert continuous_components(12.0, 10.0) == {
        "forecast_value": 12.0,
        "observed_value": 10.0,
        "error": 2.0,
        "absolute_error": 2.0,
        "squared_error": 4.0,
    }
    assert probability_components(0.5, False) == {
        "forecast_probability": 0.5,
        "observed_occurrence": False,
        "brier_component": 0.25,
        "predicted_occurrence": True,
        "true_positive": 0,
        "false_positive": 1,
        "true_negative": 0,
        "false_negative": 0,
    }

    sql = ProductionSqlContract().sql
    for component in [
        "temperature_error",
        "temperature_absolute_error",
        "temperature_squared_error",
        "forecast_probability",
        "observed_occurrence",
        "brier_component",
        "predicted_occurrence",
        "true_positive",
        "false_positive",
        "true_negative",
        "false_negative",
        "categorical_forecast",
        "categorical_observed",
        "categorical_match",
    ]:
        assert component in sql
    assert "forecast_probability >= 0.5" in sql


def test_gold_history_and_view_are_manifest_gated_latest_success_without_serving_dependencies() -> None:
    history = HISTORY_SQL.read_text(encoding="utf-8")
    view = VIEW_SQL.read_text(encoding="utf-8")

    assert "materialized='incremental'" in history
    assert "partitioning" in history and "day(valid_at)" in history
    assert "evaluation_run_id" in history
    assert "expected_count" in history
    assert "matched_count" in history
    assert "missing_truth_count" in history
    assert "missing_vintage_count" in history
    assert "evaluation_as_of as history_loaded_at" in history
    assert "current_timestamp as history_loaded_at" not in history

    assert "source('weather_quality_control', 'quality_publication_manifest')" in view
    assert "publication_status = 'SUCCESS'" in view
    assert "partition by evaluation_date_kst" in view
    assert "order by evaluation_as_of desc, published_at desc, evaluation_run_id desc" in " ".join(view.split())
    assert "publication_rank = 1" in view
    assert "serving" not in (history + view).lower()
    assert "d1" not in (history + view).lower()


def test_quality_sql_data_tests_protect_expected_grain_and_reconciliation() -> None:
    unique = MATCH_TEST_SQL.read_text(encoding="utf-8")
    reconcile = RECONCILE_TEST_SQL.read_text(encoding="utf-8")

    for grain_column in [
        "evaluation_run_id",
        "grid_id",
        "valid_at",
        "variable",
        "forecast_horizon",
    ]:
        assert grain_column in unique

    assert "count(*) > 1" in unique
    assert "expected_count" in reconcile
    assert "matched_count" in reconcile
    assert "missing_vintage_count" in reconcile
    assert "missing_truth_count" in reconcile
    assert "invalid_forecast_count" in reconcile
    assert "invalid_truth_count" in reconcile
    assert "incompatible_contract_count" in reconcile


def test_aggregate_metrics_use_sufficient_statistics_and_null_zero_denominators() -> None:
    metrics = aggregate_quality_components(
        temperatures=[(12.0, 10.0), (14.0, 15.0)],
        probabilities=[(0.8, True), (0.2, False)],
    )

    assert metrics["temperature_mae"] == pytest.approx(1.5)
    assert metrics["temperature_rmse"] == pytest.approx(2.5**0.5)
    assert metrics["temperature_bias"] == pytest.approx(0.5)
    assert metrics["precipitation_brier_score"] == pytest.approx(0.04)
    assert metrics["precipitation_true_positive_count"] == 1
    assert metrics["precipitation_true_negative_count"] == 1
    assert metrics["precipitation_precision"] == pytest.approx(1.0)
    assert metrics["precipitation_recall"] == pytest.approx(1.0)
    assert metrics["precipitation_f1"] == pytest.approx(1.0)

    assert ratio_or_none(1.0, 0) is None


def test_hourly_and_daily_products_are_manifest_gated_and_compute_rates_from_components() -> None:
    hourly_history = HOURLY_HISTORY_SQL.read_text(encoding="utf-8")
    hourly_view = HOURLY_VIEW_SQL.read_text(encoding="utf-8")
    daily_history = DAILY_HISTORY_SQL.read_text(encoding="utf-8")
    daily_view = DAILY_VIEW_SQL.read_text(encoding="utf-8")

    for history in [hourly_history, daily_history]:
        assert "materialized='incremental'" in history
        assert "gold_weather_forecast_quality_grid_score_history" in history
        assert "temperature_absolute_error_sum / nullif(base.temperature_sample_count, 0) as temperature_mae" in history
        assert "sqrt(base.temperature_squared_error_sum / nullif(base.temperature_sample_count, 0)) as temperature_rmse" in history
        assert "base.temperature_error_sum / nullif(base.temperature_sample_count, 0) as temperature_bias" in history
        assert "base.precipitation_brier_sum / nullif(base.precipitation_sample_count, 0) as precipitation_brier_score" in history
        assert "precipitation_ece_10bin" in history
        assert "weather_quality_evidence_state" in history
        assert "30 as evidence_min_sample_count" in history
        assert "0.80 as evidence_min_matched_coverage" in history
        assert " nullif(" in history
        assert "serving" not in history.lower()
        assert "d1" not in history.lower()

    assert "partitioning" in hourly_history and "day(valid_at)" in hourly_history
    assert "partitioning" in daily_history and "day(evaluation_date_kst)" in daily_history
    assert "from {{ ref('gold_weather_forecast_quality_hourly_history') }}" not in daily_history

    for view in [hourly_view, daily_view]:
        assert "source('weather_quality_control', 'quality_publication_manifest')" in view
        assert "publication_status = 'SUCCESS'" in view
        assert "partition by evaluation_date_kst" in view
        assert "order by evaluation_as_of desc, published_at desc, evaluation_run_id desc" in " ".join(view.split())
        assert "publication_rank = 1" in view
        assert "serving" not in view.lower()
        assert "d1" not in view.lower()


def test_hourly_and_daily_reconciliation_tests_cover_components_and_views() -> None:
    hourly = HOURLY_RECONCILE_TEST_SQL.read_text(encoding="utf-8")
    daily = DAILY_RECONCILE_TEST_SQL.read_text(encoding="utf-8")

    for sql in [hourly, daily]:
        assert "gold_weather_forecast_quality_grid_score_history" in sql
        assert "gold_weather_forecast_quality" in sql
        assert "temperature_squared_error_sum" in sql
        assert "precipitation_brier_sum" in sql
        assert "precipitation_true_positive_count" in sql
        assert "pty_correct_count" in sql
        assert "missing_from_view" in sql
        assert "unexpected_in_view" in sql
        assert "publication_rank = 1" in sql
        for component in [
            "true_positive",
            "false_positive",
            "true_negative",
            "false_negative",
        ]:
            assert f"coalesce(sum({component}), 0)" in sql

    assert "gold_weather_forecast_quality_hourly_history" in daily
