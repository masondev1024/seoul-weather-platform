from __future__ import annotations

from typing import Mapping


MIN_SAMPLE_COUNT = 30
MIN_MATCHED_COVERAGE = 0.80
EVIDENCE_GATE_VERSION = "metric-evidence-gate/v1"


class ClaimError(ValueError):
    """Raised when an AI claim would overstate the available evidence."""


def evidence_state(
    *, sample_count: int, expected_count: int, contains_provisional_truth: bool
) -> str:
    coverage = sample_count / expected_count if expected_count > 0 else 0.0
    if sample_count < MIN_SAMPLE_COUNT or coverage < MIN_MATCHED_COVERAGE:
        return "insufficient_evidence"
    return "degraded" if contains_provisional_truth else "sufficient"


def build_ai_claim(cohort: Mapping[str, object], *, requested_metric: str) -> dict[str, object]:
    state = str(cohort.get("evidence_state"))
    if state not in {"sufficient", "degraded", "insufficient_evidence"}:
        raise ClaimError("evidence_state_invalid")
    if state == "insufficient_evidence":
        raise ClaimError("insufficient_evidence: cohort does not meet sample and coverage gates")
    metrics = cohort.get("metrics")
    if not isinstance(metrics, Mapping):
        raise ClaimError("metric_evidence_missing")
    if requested_metric == "accuracy" and "probabilistic" in metrics:
        raise ClaimError(
            "metric_scope_required: choose Brier score or thresholded categorical accuracy"
        )

    metric_family = None
    metric_value = None
    for family, values in metrics.items():
        if isinstance(values, Mapping) and requested_metric in values:
            metric_family = str(family)
            metric_value = values[requested_metric]
            break
    if metric_family is None:
        raise ClaimError(f"unknown_metric: {requested_metric}")
    metadata = cohort.get("metric_metadata")
    if not isinstance(metadata, Mapping):
        raise ClaimError("metric_metadata_missing")
    family_metadata = metadata.get(metric_family)
    if not isinstance(family_metadata, Mapping):
        raise ClaimError("metric_family_metadata_missing")
    metric_definition = family_metadata.get(requested_metric)
    if not isinstance(metric_definition, Mapping):
        raise ClaimError("metric_definition_missing")
    direction = metric_definition.get("direction")
    unit = metric_definition.get("unit")
    if not isinstance(direction, str) or not direction or not isinstance(unit, str) or not unit:
        raise ClaimError("metric_definition_invalid")
    return {
        "schema_version": "weather-forecast-quality-claim/v1",
        "cohort_id": cohort.get("cohort_id"),
        "metric_family": metric_family,
        "metric_name": requested_metric,
        "metric_value": metric_value,
        "metric_direction": direction,
        "metric_unit": unit,
        "sample_count": cohort.get("sample_count"),
        "expected_count": cohort.get("expected_count"),
        "matched_coverage": cohort.get("matched_coverage"),
        "evidence_state": state,
        "limitations": cohort.get("limitations", []),
    }
