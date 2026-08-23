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
MATCH_TEST_SQL = (
    ROOT
    / "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_match_unique.sql"
)
RECONCILE_TEST_SQL = (
    ROOT
    / "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_grid_score_reconciles.sql"
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
    forecast_kind: str
    truth_kind: str
    forecast_unit: str
    truth_unit: str
    candidate: Candidate | None
    truth: Truth | None


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
    if case.forecast_kind != case.truth_kind or case.forecast_unit != case.truth_unit:
        return "incompatible_contract"
    if case.candidate.value_status != "valid":
        return "invalid_forecast"
    if case.truth.truth_status == "invalid_truth":
        return "invalid_truth"
    return "matched"


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


@pytest.mark.parametrize(
    ("horizon", "lower_hours", "upper_hours"),
    [("D-1", 27, 24), ("D-2", 51, 48), ("D-3", 75, 72)],
)
def test_vintage_windows_are_inclusive(
    horizon: str, lower_hours: int, upper_hours: int
) -> None:
    contract = ProductionSqlContract()

    assert contract.window(horizon) == InclusiveWindow(lower_hours, upper_hours)
    assert "between valid_at - interval '1' hour * lower_hours_before_valid" in contract.sql
    assert "and valid_at - interval '1' hour * upper_hours_before_valid" in contract.sql


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


def test_match_state_oracle_covers_every_explicit_failure_state_and_no_fallback() -> None:
    cases = [
        MatchCase("temperature_air_2m", "continuous", "continuous", "degC", "degC", Candidate("D-1", 24, "r1", "run", 12.0), Truth(10.0)),
        MatchCase("temperature_air_2m", "continuous", "continuous", "degC", "degC", None, Truth(10.0)),
        MatchCase("temperature_air_2m", "continuous", "continuous", "degC", "degC", Candidate("D-1", 24, "r1", "run", 12.0), None),
        MatchCase("temperature_air_2m", "continuous", "continuous", "degC", "degC", Candidate("D-1", 24, "r1", "run", None, "invalid"), Truth(10.0)),
        MatchCase("temperature_air_2m", "continuous", "continuous", "degC", "degC", Candidate("D-1", 24, "r1", "run", 12.0), Truth(None, "invalid_truth")),
        MatchCase("precipitation_occurrence_category", "categorical", "binary", "category", "1", Candidate("D-1", 24, "r1", "run", "wet", value_kind="categorical", unit="category"), Truth(True, value_kind="binary", unit="1")),
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
