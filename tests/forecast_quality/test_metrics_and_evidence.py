from __future__ import annotations

import math

import pytest

from weather_quality.evidence import ClaimError, build_ai_claim, evidence_state
from weather_quality.evaluation import evaluate_forecast_quality
from weather_quality.grid_universe import load_canonical_grid_universe
from weather_quality.metrics import (
    categorical_metrics,
    continuous_metrics,
    continuous_row_score,
    probability_metrics,
    probability_row_score,
)
from weather_quality.models import ContractError, ForecastVintage, ObservationTruth, TruthQuality


def test_continuous_metrics_are_hand_computable() -> None:
    metrics = continuous_metrics([(10.0, 8.0), (12.0, 13.0), (8.0, 8.0)])

    assert metrics["sample_count"] == 3
    assert metrics["mae"] == pytest.approx(1.0)
    assert metrics["rmse"] == pytest.approx(math.sqrt(5 / 3))
    assert metrics["bias"] == pytest.approx(1 / 3)


def test_row_scores_preserve_auditable_metric_components() -> None:
    assert continuous_row_score(12.0, 10.0) == {
        "forecast_value": 12.0,
        "observed_value": 10.0,
        "error": 2.0,
        "absolute_error": 2.0,
        "squared_error": 4.0,
    }
    assert probability_row_score(0.8, True) == {
        "forecast_probability": 0.8,
        "observed_occurrence": True,
        "brier_component": 0.04,
        "predicted_occurrence": True,
        "classification_outcome": "true_positive",
    }


def test_probability_calibration_has_fixed_boundary_and_empty_bin_contract() -> None:
    pairs = [
        (0.0, 0),
        (0.1, 0),
        (0.2, 0),
        (0.3, 0),
        (0.4, 0),
        (0.5, 1),
        (0.6, 1),
        (0.7, 1),
        (0.8, 1),
        (0.9, 1),
        (0.999999, 1),
        (1.0, 1),
    ]

    metrics = probability_metrics(pairs)
    bins = metrics["calibration_bins"]

    assert len(bins) == 10
    assert [item["sample_count"] for item in bins] == [1, 1, 1, 1, 1, 1, 1, 1, 1, 3]
    assert bins[0]["lower_inclusive"] is True
    assert bins[0]["upper_inclusive"] is False
    assert bins[-1]["upper_inclusive"] is True

    empty = probability_metrics([(0.0, 0)])["calibration_bins"][1]
    assert empty["sample_count"] == 0
    assert empty["mean_forecast_probability"] is None
    assert empty["observed_frequency"] is None
    assert empty["weighted_absolute_calibration_gap"] == 0.0


def test_probability_and_thresholded_categorical_scores_stay_separate() -> None:
    probability = probability_metrics([(0.8, 1), (0.8, 0), (0.2, 0), (0.2, 1)])
    categorical = categorical_metrics(
        [(True, True), (True, False), (False, False), (False, True)]
    )

    assert probability["metric_family"] == "probabilistic"
    assert probability["brier_score"] == pytest.approx(0.34)
    assert categorical == {
        "metric_family": "categorical",
        "sample_count": 4,
        "true_positive": 1,
        "false_positive": 1,
        "true_negative": 1,
        "false_negative": 1,
        "precision": 0.5,
        "recall": 0.5,
        "f1": 0.5,
        "accuracy": 0.5,
        "positive_prevalence": 0.5,
    }


def test_categorical_zero_denominators_are_explicit_nulls() -> None:
    metrics = categorical_metrics([(False, False), (False, False)])

    assert metrics["precision"] is None
    assert metrics["recall"] is None
    assert metrics["f1"] is None
    assert metrics["positive_prevalence"] == 0.0


@pytest.mark.parametrize(
    ("sample_count", "expected_count", "expected_state"),
    [(29, 29, "insufficient_evidence"), (30, 30, "sufficient"), (30, 38, "insufficient_evidence"), (32, 40, "sufficient")],
)
def test_evidence_gate_has_inclusive_sample_and_coverage_boundaries(
    sample_count: int, expected_count: int, expected_state: str
) -> None:
    assert (
        evidence_state(
            sample_count=sample_count,
            expected_count=expected_count,
            contains_provisional_truth=False,
        )
        == expected_state
    )


def test_ai_claim_refuses_universal_accuracy_and_sparse_cohort() -> None:
    cohort = {
        "cohort_id": "c1",
        "variable": "precipitation_occurrence",
        "evidence_state": "sufficient",
        "sample_count": 80,
        "expected_count": 80,
        "matched_coverage": 1.0,
        "metrics": {
            "probabilistic": {"brier_score": 0.04},
            "categorical": {"accuracy": 0.9},
        },
        "metric_metadata": {
            "probabilistic": {
                "brier_score": {"unit": "probability_squared", "direction": "lower_is_better"}
            },
            "categorical": {
                "accuracy": {"unit": "ratio", "direction": "higher_is_better"}
            },
        },
        "limitations": [],
    }

    with pytest.raises(ClaimError, match="metric_scope_required"):
        build_ai_claim(cohort, requested_metric="accuracy")

    sparse = {**cohort, "evidence_state": "insufficient_evidence", "sample_count": 12}
    with pytest.raises(ClaimError, match="insufficient_evidence"):
        build_ai_claim(sparse, requested_metric="brier_score")

    missing_state = {key: value for key, value in cohort.items() if key != "evidence_state"}
    with pytest.raises(ClaimError, match="evidence_state_invalid"):
        build_ai_claim(missing_state, requested_metric="brier_score")

    claim = build_ai_claim(cohort, requested_metric="brier_score")
    assert claim["metric_name"] == "brier_score"
    assert claim["metric_direction"] == "lower_is_better"
    assert claim["sample_count"] == 80
    assert "accuracy_pct" not in claim


def test_evaluator_rejects_ambiguous_multiple_truth_sources() -> None:
    from datetime import datetime, timedelta, timezone
    from pathlib import Path

    valid_at = datetime(2026, 8, 20, 12, tzinfo=timezone.utc)
    forecast = ForecastVintage(
        product_family="short_range",
        grid_id="kma_60_127",
        nx=60,
        ny=127,
        variable="temperature_air_2m",
        value_kind="continuous",
        value=20.0,
        unit="degC",
        issued_at=valid_at - timedelta(hours=24),
        valid_at=valid_at,
        source_id="kma_vilage_fcst",
        source_revision="fixture-v1",
    )
    observations = [
        ObservationTruth(
            grid_id="kma_60_127",
            nx=60,
            ny=127,
            variable="temperature_air_2m",
            value_kind="continuous",
            value=19.0,
            unit="degC",
            observed_at=valid_at,
            truth_source=source,
            truth_revision="r1",
            truth_as_of=valid_at + timedelta(hours=1),
            collected_at=valid_at + timedelta(hours=1),
            quality=TruthQuality.FINAL,
        )
        for source in ("truth_a", "truth_b")
    ]

    with pytest.raises(ContractError, match="multiple truth sources"):
        evaluate_forecast_quality(
            [forecast],
            observations,
            grid_universe=load_canonical_grid_universe(
                Path(__file__).resolve().parents[2]
                / "dags/domains/weather/config/seoul_kma_grids.csv"
            ),
            evaluation_as_of=valid_at + timedelta(hours=2),
        )
