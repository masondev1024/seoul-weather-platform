from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Iterable

from weather_quality.evidence import EVIDENCE_GATE_VERSION, evidence_state
from weather_quality.grid_universe import GridUniverse
from weather_quality.metrics import categorical_metrics, continuous_metrics, probability_metrics
from weather_quality.models import ContractError, ForecastVintage, ObservationTruth, TruthQuality
from weather_quality.selection import (
    SelectedVintage,
    TRUTH_POLICY_VERSION,
    VINTAGE_POLICY_VERSION,
    resolve_observation_truth,
    select_forecast_vintages,
)


EvaluationGroupKey = tuple[str, str, str, datetime, str, str]


def _cohort_id(parts: tuple[object, ...]) -> str:
    payload = json.dumps([str(part) for part in parts], separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()[:24]


def _safe_identity(parts: tuple[object, ...]) -> str:
    payload = json.dumps([str(part) for part in parts], separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()[:16]


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _metric_metadata(metric_family: str, unit: str) -> dict[str, object]:
    categorical = {
        "true_positive": {"unit": "count", "direction": "descriptive"},
        "false_positive": {"unit": "count", "direction": "lower_is_better"},
        "true_negative": {"unit": "count", "direction": "descriptive"},
        "false_negative": {"unit": "count", "direction": "lower_is_better"},
        "precision": {"unit": "ratio", "direction": "higher_is_better"},
        "recall": {"unit": "ratio", "direction": "higher_is_better"},
        "f1": {"unit": "ratio", "direction": "higher_is_better"},
        "accuracy": {"unit": "ratio", "direction": "higher_is_better"},
        "positive_prevalence": {"unit": "ratio", "direction": "descriptive"},
    }
    if metric_family == "continuous":
        return {
            "continuous": {
                "mae": {"unit": unit, "direction": "lower_is_better"},
                "rmse": {"unit": unit, "direction": "lower_is_better"},
                "bias": {"unit": unit, "direction": "zero_is_best"},
            }
        }
    if metric_family == "probabilistic":
        return {
            "probabilistic": {
                "brier_score": {
                    "unit": "probability_squared",
                    "direction": "lower_is_better",
                },
                "expected_calibration_error": {
                    "unit": "ratio",
                    "direction": "lower_is_better",
                },
            },
            "categorical": categorical,
        }
    if metric_family == "categorical":
        return {"categorical": categorical}
    raise ContractError(f"unsupported metric family: {metric_family}")


def evaluate_forecast_quality(
    forecasts: Iterable[ForecastVintage],
    observations: Iterable[ObservationTruth],
    *,
    grid_universe: GridUniverse,
    evaluation_as_of: datetime,
) -> dict[str, object]:
    if not isinstance(grid_universe, GridUniverse):
        raise ContractError("grid_universe must be a validated canonical Seoul KMA GridUniverse")
    if (
        not isinstance(evaluation_as_of, datetime)
        or evaluation_as_of.tzinfo is None
        or evaluation_as_of.utcoffset() is None
    ):
        raise ContractError("evaluation_as_of must be timezone-aware")
    evaluation_as_of = evaluation_as_of.astimezone(timezone.utc)
    grids = grid_universe.grid_ids
    forecast_rows = tuple(forecasts)
    if not forecast_rows:
        raise ContractError("forecast evaluation requires at least one forecast record")
    selected_forecasts = select_forecast_vintages(forecast_rows)
    resolved_truth = resolve_observation_truth(observations, evaluation_as_of=evaluation_as_of)
    sources_by_match_key: dict[tuple[object, ...], set[str]] = defaultdict(set)
    for row in resolved_truth.selected:
        sources_by_match_key[(row.grid_id, row.variable, row.observed_at)].add(row.truth_source)
    for match_key, sources in sources_by_match_key.items():
        if len(sources) > 1:
            raise ContractError(
                "multiple truth sources require an explicit source-selection policy; "
                f"identity_fingerprint={_safe_identity(match_key)}"
            )
    truth_index = {
        (row.grid_id, row.variable, row.observed_at): row for row in resolved_truth.selected
    }

    groups: dict[EvaluationGroupKey, list[SelectedVintage]] = defaultdict(list)
    for item in selected_forecasts.selected:
        row = item.forecast
        if row.grid_id not in grids:
            raise ContractError(f"forecast grid outside canonical universe: {row.grid_id}")
        groups[(row.product_family, row.variable, item.vintage_label, row.valid_at, row.value_kind, row.unit)].append(item)

    cohorts: list[dict[str, object]] = []
    for key in sorted(groups, key=lambda item: tuple(str(value) for value in item)):
        product_family, variable, vintage_label, valid_at, value_kind, unit = key
        matched_pairs: list[tuple[object, object]] = []
        excluded: Counter[str] = Counter()
        contains_provisional = False
        truth_revisions: set[str] = set()
        truth_sources: set[str] = set()
        for item in groups[key]:
            forecast = item.forecast
            truth = truth_index.get((forecast.grid_id, forecast.variable, forecast.valid_at))
            if truth is None:
                excluded["missing_or_ineligible_truth"] += 1
                continue
            if truth.unit != forecast.unit:
                excluded["incompatible_unit"] += 1
                continue
            if value_kind == "continuous" and truth.value_kind != "continuous":
                excluded["incompatible_value_kind"] += 1
                continue
            if value_kind == "probability" and truth.value_kind != "binary":
                excluded["incompatible_value_kind"] += 1
                continue
            if value_kind == "categorical" and truth.value_kind != "categorical":
                excluded["incompatible_value_kind"] += 1
                continue
            matched_pairs.append((forecast.value, truth.value))
            contains_provisional = contains_provisional or truth.quality is TruthQuality.PROVISIONAL
            truth_revisions.add(truth.truth_revision)
            truth_sources.add(truth.truth_source)

        sample_count = len(matched_pairs)
        expected_count = len(grids)
        state = evidence_state(
            sample_count=sample_count,
            expected_count=expected_count,
            contains_provisional_truth=contains_provisional,
        )
        metric_family_by_value_kind = {
            "continuous": "continuous",
            "probability": "probabilistic",
            "categorical": "categorical",
        }
        try:
            metric_family = metric_family_by_value_kind[value_kind]
        except KeyError as exc:
            raise ContractError(f"unsupported evaluation value_kind: {value_kind}") from exc
        metrics: dict[str, object] = {}
        if matched_pairs:
            if value_kind == "continuous":
                metrics["continuous"] = continuous_metrics(
                    [(float(forecast), float(truth)) for forecast, truth in matched_pairs]
                )
            elif value_kind == "probability":
                probability_pairs = [
                    (float(forecast), int(truth)) for forecast, truth in matched_pairs
                ]
                metrics["probabilistic"] = probability_metrics(probability_pairs)
                metrics["categorical"] = categorical_metrics(
                    [(forecast >= 0.5, bool(truth)) for forecast, truth in probability_pairs]
                )
            elif value_kind == "categorical":
                if variable != "precipitation_occurrence_category":
                    raise ContractError(
                        "categorical evaluation is restricted to precipitation_occurrence_category"
                    )
                categorical_pairs = [
                    (str(forecast), str(truth)) for forecast, truth in matched_pairs
                ]
                labels = {
                    value for pair in categorical_pairs for value in pair
                }
                if not labels.issubset({"occurrence", "none"}):
                    raise ContractError(
                        "categorical occurrence values must be occurrence or none"
                    )
                metrics["categorical"] = categorical_metrics(
                    categorical_pairs, positive_label="occurrence"
                )

        limitations = [
            "Synthetic fixtures prove contract behavior, not current Seoul forecast accuracy.",
            "Metrics are valid only for the declared product, variable, vintage, grid universe, and evaluation window.",
        ]
        if contains_provisional:
            limitations.append(
                "Eligible provisional truth is present; evidence is degraded until final truth arrives."
            )
        if state == "insufficient_evidence":
            limitations.append("The cohort is below metric-evidence-gate/v1 sample or coverage thresholds.")

        cohort_key = (
            product_family,
            variable,
            vintage_label,
            valid_at,
            metric_family,
            unit,
        )
        cohorts.append(
            {
                "cohort_id": _cohort_id(cohort_key),
                "product_family": product_family,
                "variable": variable,
                "vintage_label": vintage_label,
                "valid_from": _iso(valid_at),
                "valid_to": _iso(valid_at),
                "metric_family": metric_family,
                "unit": unit,
                "grid_scope": "seoul_kma_80",
                "population_revision": grid_universe.population_revision,
                "expected_count": expected_count,
                "sample_count": sample_count,
                "matched_coverage": sample_count / expected_count,
                "excluded_counts": dict(sorted(excluded.items())),
                "evidence_state": state,
                "evidence_gate_version": EVIDENCE_GATE_VERSION,
                "truth_policy_version": TRUTH_POLICY_VERSION,
                "truth_sources": sorted(truth_sources),
                "truth_revisions": sorted(truth_revisions),
                "metrics": metrics,
                "metric_metadata": _metric_metadata(metric_family, unit),
                "limitations": limitations,
            }
        )

    gap_counts = Counter(gap.vintage_label for gap in selected_forecasts.gaps)
    return {
        "evaluation_as_of": _iso(evaluation_as_of),
        "universe": grid_universe.as_evidence(),
        "vintage_policy_version": VINTAGE_POLICY_VERSION,
        "truth_policy_version": TRUTH_POLICY_VERSION,
        "evidence_gate_version": EVIDENCE_GATE_VERSION,
        "cohorts": cohorts,
        "vintage_gap_count": len(selected_forecasts.gaps),
        "vintage_gap_counts": {
            label: gap_counts.get(label, 0) for label in ("D-1", "D-2", "D-3")
        },
        "truth_excluded_counts": resolved_truth.excluded_counts,
    }
