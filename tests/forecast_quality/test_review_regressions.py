from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from weather_quality.evaluation import evaluate_forecast_quality
from weather_quality.fixture import build_reference_evidence
from weather_quality.grid_universe import (
    CANONICAL_SEOUL_GRID_REVISION,
    CANONICAL_SEOUL_GRID_SCOPE,
    GridCell,
    GridUniverse,
    load_canonical_grid_universe,
)
from weather_quality.metrics import categorical_metrics
from weather_quality.models import ContractError, ForecastVintage, ObservationTruth, TruthQuality


ROOT = Path(__file__).resolve().parents[2]
GRID_CSV = ROOT / "dags/domains/weather/config/seoul_kma_grids.csv"
SCENARIO = ROOT / "contracts/weather-forecast-quality/fixtures/reference-scenario-v1.json"
EVIDENCE_SCHEMA = ROOT / "contracts/weather-forecast-quality/schema/evidence-v1.schema.json"
UTC = timezone.utc
VALID_AT = datetime(2026, 8, 20, 3, tzinfo=UTC)


def _forecast(
    cell: GridCell,
    *,
    variable: str = "temperature_air_2m",
    value_kind: str = "continuous",
    value: float | str = 20.0,
    unit: str = "degC",
) -> ForecastVintage:
    return ForecastVintage(
        product_family="short_range",
        grid_id=cell.grid_id,
        nx=cell.nx,
        ny=cell.ny,
        variable=variable,
        value_kind=value_kind,
        value=value,
        unit=unit,
        issued_at=VALID_AT - timedelta(hours=24),
        valid_at=VALID_AT,
        source_id="synthetic_short_range_fixture",
        source_revision="fixture-v1",
    )


def _truth(
    cell: GridCell,
    *,
    variable: str = "temperature_air_2m",
    value_kind: str = "continuous",
    value: float | str = 19.0,
    unit: str = "degC",
) -> ObservationTruth:
    return ObservationTruth(
        grid_id=cell.grid_id,
        nx=cell.nx,
        ny=cell.ny,
        variable=variable,
        value_kind=value_kind,
        value=value,
        unit=unit,
        observed_at=VALID_AT,
        truth_source="synthetic_observation_fixture",
        truth_revision="fixture-truth-v1",
        truth_as_of=VALID_AT + timedelta(hours=1),
        collected_at=VALID_AT + timedelta(hours=1),
        quality=TruthQuality.FINAL,
    )


@pytest.mark.parametrize("mutation", ["missing", "extra", "noncanonical"])
def test_evaluator_universe_rejects_79_81_and_ad_hoc_80_grid_sets(mutation: str) -> None:
    canonical = load_canonical_grid_universe(GRID_CSV)
    cells = list(canonical.cells)
    if mutation == "missing":
        cells.pop()
    elif mutation == "extra":
        cells.append(GridCell(grid_id="kma_99_99", nx=99, ny=99))
    else:
        cells[-1] = GridCell(grid_id="kma_98_98", nx=98, ny=98)

    with pytest.raises(ContractError, match="canonical Seoul KMA grid universe"):
        GridUniverse(
            scope=CANONICAL_SEOUL_GRID_SCOPE,
            cells=tuple(cells),
            population_revision=CANONICAL_SEOUL_GRID_REVISION,
        )


def test_categorical_occurrence_is_a_first_class_evaluation_path() -> None:
    universe = load_canonical_grid_universe(GRID_CSV)
    forecasts: list[ForecastVintage] = []
    observations: list[ObservationTruth] = []
    outcomes = (
        ("occurrence", "occurrence"),
        ("occurrence", "none"),
        ("none", "none"),
        ("none", "occurrence"),
    )
    for index, cell in enumerate(universe.cells):
        predicted, observed = outcomes[index % len(outcomes)]
        forecasts.append(
            _forecast(
                cell,
                variable="precipitation_occurrence_category",
                value_kind="categorical",
                value=predicted,
                unit="category",
            )
        )
        observations.append(
            _truth(
                cell,
                variable="precipitation_occurrence_category",
                value_kind="categorical",
                value=observed,
                unit="category",
            )
        )

    result = evaluate_forecast_quality(
        forecasts,
        observations,
        grid_universe=universe,
        evaluation_as_of=VALID_AT + timedelta(hours=2),
    )

    cohort = result["cohorts"][0]
    assert cohort["metric_family"] == "categorical"
    assert cohort["metrics"]["categorical"] == categorical_metrics(
        [outcomes[index % len(outcomes)] for index in range(80)],
        positive_label="occurrence",
    )
    assert cohort["metrics"]["categorical"]["true_positive"] == 20
    assert cohort["metrics"]["categorical"]["false_positive"] == 20
    assert cohort["metrics"]["categorical"]["true_negative"] == 20
    assert cohort["metrics"]["categorical"]["false_negative"] == 20


def test_every_published_metric_has_unit_and_direction_metadata() -> None:
    evidence = build_reference_evidence(GRID_CSV, SCENARIO)

    for cohort in evidence["cohorts"]:
        metadata = cohort["metric_metadata"]
        for family, metrics in cohort["metrics"].items():
            aggregate_names = {
                name
                for name in metrics
                if name not in {"metric_family", "sample_count", "calibration_bins"}
            }
            assert aggregate_names == set(metadata[family])
            assert all(
                definition["unit"] and definition["direction"]
                for definition in metadata[family].values()
            )


def test_equivalent_evaluation_instants_produce_identical_utc_evidence() -> None:
    universe = load_canonical_grid_universe(GRID_CSV)
    cell = universe.cells[0]
    forecast = _forecast(cell)
    truth = _truth(cell)
    utc_as_of = VALID_AT + timedelta(hours=2)
    kst_as_of = utc_as_of.astimezone(timezone(timedelta(hours=9)))

    utc_result = evaluate_forecast_quality(
        [forecast], [truth], grid_universe=universe, evaluation_as_of=utc_as_of
    )
    kst_result = evaluate_forecast_quality(
        [forecast], [truth], grid_universe=universe, evaluation_as_of=kst_as_of
    )

    assert utc_result == kst_result
    assert utc_result["evaluation_as_of"].endswith("Z")


def test_zero_match_sparse_cohort_remains_schema_valid_and_explicit() -> None:
    universe = load_canonical_grid_universe(GRID_CSV)
    evaluation = evaluate_forecast_quality(
        [_forecast(universe.cells[0])],
        [],
        grid_universe=universe,
        evaluation_as_of=VALID_AT + timedelta(hours=2),
    )
    sparse = evaluation["cohorts"][0]
    assert sparse["sample_count"] == 0
    assert sparse["evidence_state"] == "insufficient_evidence"
    assert sparse["metrics"] == {}
    assert sparse["excluded_counts"] == {"missing_or_ineligible_truth": 1}

    envelope = build_reference_evidence(GRID_CSV, SCENARIO)
    envelope["cohorts"] = [sparse]
    envelope["evidence_revision"] = "0" * 64
    schema = json.loads(EVIDENCE_SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(envelope)


def test_fully_empty_forecast_input_fails_before_evidence_emission() -> None:
    universe = load_canonical_grid_universe(GRID_CSV)

    with pytest.raises(ContractError, match="at least one forecast record"):
        evaluate_forecast_quality(
            [],
            [],
            grid_universe=universe,
            evaluation_as_of=VALID_AT + timedelta(hours=2),
        )
