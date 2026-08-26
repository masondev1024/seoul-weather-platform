from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from weather_quality.evaluation import evaluate_forecast_quality
from weather_quality.grid_universe import load_canonical_grid_universe
from weather_quality.models import ContractError, ForecastVintage, ObservationTruth, TruthQuality


ADAPTERS = (
    {"product_family": "short_range", "status": "fixture_enabled"},
    {"product_family": "ultra_short", "status": "contract_only_disabled"},
    {"product_family": "mid_term", "status": "contract_only_disabled"},
)


def canonical_json_bytes(value: Mapping[str, object]) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _timestamp(raw: object, field: str) -> datetime:
    if not isinstance(raw, str):
        raise ContractError(f"{field} must be an ISO-8601 string")
    try:
        value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractError(f"{field} must be an ISO-8601 timestamp") from exc
    if value.tzinfo is None or value.utcoffset() is None:
        raise ContractError(f"{field} must be timezone-aware")
    return value


def _load_scenario(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractError(f"cannot read reference scenario: {path}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != "weather-forecast-quality-scenario/v1":
        raise ContractError("reference scenario schema version is invalid")
    return payload


def _scenario_text(payload: Mapping[str, object], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{field} must be a non-empty string")
    return value.strip()


def build_reference_evidence(grid_csv: Path, scenario_path: Path) -> dict[str, object]:
    scenario = _load_scenario(scenario_path)
    grid_universe = load_canonical_grid_universe(grid_csv)
    valid_at = _timestamp(scenario.get("valid_at"), "valid_at")
    evaluation_as_of = _timestamp(scenario.get("evaluation_as_of"), "evaluation_as_of")
    truth_as_of = _timestamp(scenario.get("truth_as_of"), "truth_as_of")
    collected_at = _timestamp(scenario.get("collected_at"), "collected_at")
    product_family = _scenario_text(scenario, "product_family")
    if product_family != "short_range":
        raise ContractError("reference scenario product_family must be short_range")
    source_revision = _scenario_text(scenario, "source_revision")

    forecast_rows: list[ForecastVintage] = []
    truth_rows: list[ObservationTruth] = []
    vintage_errors = {"D-3": 3.0, "D-2": 2.0, "D-1": 1.0}
    vintage_probabilities = {"D-3": 0.6, "D-2": 0.7, "D-1": 0.8}
    vintage_hours = {"D-3": 72, "D-2": 48, "D-1": 24}

    for index, cell in enumerate(grid_universe.cells):
        grid_id, nx, ny = cell.grid_id, cell.nx, cell.ny
        observed_temperature = 18.0 + (index % 8) * 0.5
        precipitation_observed = index % 2 == 0
        precipitation_category = "occurrence" if precipitation_observed else "none"
        truth_rows.extend(
            (
                ObservationTruth(
                    grid_id=grid_id,
                    nx=nx,
                    ny=ny,
                    variable="temperature_air_2m",
                    value_kind="continuous",
                    value=observed_temperature,
                    unit="degC",
                    observed_at=valid_at,
                    truth_source="synthetic_observation_fixture",
                    truth_revision="fixture-truth-v1",
                    truth_as_of=truth_as_of,
                    collected_at=collected_at,
                    quality=TruthQuality.FINAL,
                ),
                ObservationTruth(
                    grid_id=grid_id,
                    nx=nx,
                    ny=ny,
                    variable="precipitation_occurrence",
                    value_kind="binary",
                    value=precipitation_observed,
                    unit="1",
                    observed_at=valid_at,
                    truth_source="synthetic_observation_fixture",
                    truth_revision="fixture-truth-v1",
                    truth_as_of=truth_as_of,
                    collected_at=collected_at,
                    quality=TruthQuality.FINAL,
                ),
                ObservationTruth(
                    grid_id=grid_id,
                    nx=nx,
                    ny=ny,
                    variable="precipitation_occurrence_category",
                    value_kind="categorical",
                    value=precipitation_category,
                    unit="category",
                    observed_at=valid_at,
                    truth_source="synthetic_observation_fixture",
                    truth_revision="fixture-truth-v1",
                    truth_as_of=truth_as_of,
                    collected_at=collected_at,
                    quality=TruthQuality.FINAL,
                ),
            )
        )
        for vintage_label in ("D-3", "D-2", "D-1"):
            probability_skill = vintage_probabilities[vintage_label]
            should_flip_category = (
                vintage_label == "D-3" and index % 4 in {0, 1}
            ) or (
                vintage_label == "D-2" and index % 8 in {0, 1}
            )
            category_forecast = (
                "none" if precipitation_category == "occurrence" else "occurrence"
            ) if should_flip_category else precipitation_category
            forecast_rows.extend(
                (
                    ForecastVintage(
                        product_family=product_family,
                        grid_id=grid_id,
                        nx=nx,
                        ny=ny,
                        variable="temperature_air_2m",
                        value_kind="continuous",
                        value=observed_temperature + vintage_errors[vintage_label],
                        unit="degC",
                        issued_at=valid_at - timedelta(hours=vintage_hours[vintage_label]),
                        valid_at=valid_at,
                        source_id="synthetic_short_range_fixture",
                        source_revision=source_revision,
                    ),
                    ForecastVintage(
                        product_family=product_family,
                        grid_id=grid_id,
                        nx=nx,
                        ny=ny,
                        variable="precipitation_occurrence",
                        value_kind="probability",
                        value=probability_skill if precipitation_observed else 1.0 - probability_skill,
                        unit="1",
                        issued_at=valid_at - timedelta(hours=vintage_hours[vintage_label]),
                        valid_at=valid_at,
                        source_id="synthetic_short_range_fixture",
                        source_revision=source_revision,
                    ),
                    ForecastVintage(
                        product_family=product_family,
                        grid_id=grid_id,
                        nx=nx,
                        ny=ny,
                        variable="precipitation_occurrence_category",
                        value_kind="categorical",
                        value=category_forecast,
                        unit="category",
                        issued_at=valid_at - timedelta(hours=vintage_hours[vintage_label]),
                        valid_at=valid_at,
                        source_id="synthetic_short_range_fixture",
                        source_revision=source_revision,
                    ),
                )
            )

    evaluation = evaluate_forecast_quality(
        forecast_rows,
        truth_rows,
        grid_universe=grid_universe,
        evaluation_as_of=evaluation_as_of,
    )
    evidence: dict[str, object] = {
        "schema_version": "weather-forecast-quality-evidence/v1",
        "fixture_notice": "synthetic_reference_only_not_current_seoul_accuracy",
        "evaluation_as_of": evaluation["evaluation_as_of"],
        "policy_versions": {
            "vintage": evaluation["vintage_policy_version"],
            "truth": evaluation["truth_policy_version"],
            "evidence_gate": evaluation["evidence_gate_version"],
            "calibration": "pop-calibration/v1",
        },
        "universe": evaluation["universe"],
        "adapters": list(ADAPTERS),
        "cohorts": evaluation["cohorts"],
        "vintage_gap_count": evaluation["vintage_gap_count"],
        "vintage_gap_counts": evaluation["vintage_gap_counts"],
        "truth_excluded_counts": evaluation["truth_excluded_counts"],
        "limitations": [
            "This deterministic fixture validates scoring and evidence semantics only.",
            "It does not measure current or historical Seoul forecast accuracy.",
            "Live observation, ultra-short, and mid-term adapters remain disabled.",
        ],
    }
    evidence["evidence_revision"] = hashlib.sha256(canonical_json_bytes(evidence)).hexdigest()
    return evidence
