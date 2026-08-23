from __future__ import annotations

from math import sqrt
from typing import Iterable

from weather_quality.models import ContractError


def _clean(value: float) -> float:
    return round(value, 12)


def continuous_row_score(forecast: float, observed: float) -> dict[str, float]:
    forecast_value = float(forecast)
    observed_value = float(observed)
    error = forecast_value - observed_value
    return {
        "forecast_value": forecast_value,
        "observed_value": observed_value,
        "error": _clean(error),
        "absolute_error": _clean(abs(error)),
        "squared_error": _clean(error * error),
    }


def probability_row_score(
    forecast_probability: float, observed_occurrence: int | bool
) -> dict[str, object]:
    probability = float(forecast_probability)
    observed = bool(observed_occurrence)
    if not 0.0 <= probability <= 1.0:
        raise ContractError("forecast probability must be between 0 and 1")
    if observed_occurrence not in (0, 1, False, True):
        raise ContractError("probability truth must be binary")
    predicted = probability >= 0.5
    if predicted and observed:
        outcome = "true_positive"
    elif predicted:
        outcome = "false_positive"
    elif observed:
        outcome = "false_negative"
    else:
        outcome = "true_negative"
    return {
        "forecast_probability": probability,
        "observed_occurrence": observed,
        "brier_component": _clean((probability - int(observed)) ** 2),
        "predicted_occurrence": predicted,
        "classification_outcome": outcome,
    }


def continuous_metrics(pairs: Iterable[tuple[float, float]]) -> dict[str, object]:
    rows = tuple((float(forecast), float(observed)) for forecast, observed in pairs)
    if not rows:
        raise ContractError("continuous metrics require at least one matched pair")
    scores = [continuous_row_score(forecast, observed) for forecast, observed in rows]
    count = len(scores)
    return {
        "metric_family": "continuous",
        "sample_count": count,
        "mae": _clean(sum(score["absolute_error"] for score in scores) / count),
        "rmse": _clean(sqrt(sum(score["squared_error"] for score in scores) / count)),
        "bias": _clean(sum(score["error"] for score in scores) / count),
    }


def probability_metrics(pairs: Iterable[tuple[float, int | bool]]) -> dict[str, object]:
    rows = tuple((float(probability), int(observed)) for probability, observed in pairs)
    if not rows:
        raise ContractError("probability metrics require at least one matched pair")
    for probability, observed in rows:
        if not 0.0 <= probability <= 1.0:
            raise ContractError("forecast probability must be between 0 and 1")
        if observed not in (0, 1):
            raise ContractError("probability truth must be binary")

    count = len(rows)
    buckets: list[list[tuple[float, int]]] = [[] for _ in range(10)]
    for probability, observed in rows:
        buckets[min(int(probability * 10), 9)].append((probability, observed))

    calibration_bins: list[dict[str, object]] = []
    expected_calibration_error = 0.0
    for index, bucket in enumerate(buckets):
        lower = round(index / 10, 1)
        upper = round((index + 1) / 10, 1)
        if bucket:
            mean_probability = sum(value for value, _ in bucket) / len(bucket)
            observed_frequency = sum(value for _, value in bucket) / len(bucket)
            mean_probability = _clean(mean_probability)
            observed_frequency = _clean(observed_frequency)
            weighted_gap = _clean(
                abs(mean_probability - observed_frequency) * len(bucket) / count
            )
            expected_calibration_error += weighted_gap
        else:
            mean_probability = None
            observed_frequency = None
            weighted_gap = 0.0
        calibration_bins.append(
            {
                "bin_index": index,
                "lower_bound": lower,
                "upper_bound": upper,
                "lower_inclusive": True,
                "upper_inclusive": index == 9,
                "sample_count": len(bucket),
                "mean_forecast_probability": mean_probability,
                "observed_frequency": observed_frequency,
                "weighted_absolute_calibration_gap": weighted_gap,
            }
        )

    row_scores = [probability_row_score(forecast, observed) for forecast, observed in rows]
    return {
        "metric_family": "probabilistic",
        "sample_count": count,
        "brier_score": _clean(
            sum(float(score["brier_component"]) for score in row_scores) / count
        ),
        "expected_calibration_error": _clean(expected_calibration_error),
        "calibration_bins": calibration_bins,
    }


def _safe_ratio(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def categorical_metrics(
    pairs: Iterable[tuple[bool | str, bool | str]],
    *,
    positive_label: bool | str = True,
) -> dict[str, object]:
    rows = tuple(pairs)
    if not rows:
        raise ContractError("categorical metrics require at least one matched pair")
    if not isinstance(positive_label, (bool, str)) or (
        isinstance(positive_label, str) and not positive_label.strip()
    ):
        raise ContractError("categorical positive_label must be a boolean or non-empty string")
    for predicted, observed in rows:
        if type(predicted) is not type(positive_label) or type(observed) is not type(positive_label):
            raise ContractError("categorical values must have the same type as positive_label")
        if isinstance(predicted, str) and (not predicted.strip() or not str(observed).strip()):
            raise ContractError("categorical values must be non-empty strings")

    binary_rows = tuple(
        (predicted == positive_label, observed == positive_label)
        for predicted, observed in rows
    )
    true_positive = sum(predicted and observed for predicted, observed in binary_rows)
    false_positive = sum(predicted and not observed for predicted, observed in binary_rows)
    true_negative = sum(not predicted and not observed for predicted, observed in binary_rows)
    false_negative = sum(not predicted and observed for predicted, observed in binary_rows)
    precision = _safe_ratio(true_positive, true_positive + false_positive)
    recall = _safe_ratio(true_positive, true_positive + false_negative)
    f1 = (
        None
        if precision is None or recall is None or precision + recall == 0
        else 2 * precision * recall / (precision + recall)
    )
    count = len(rows)
    return {
        "metric_family": "categorical",
        "sample_count": count,
        "true_positive": true_positive,
        "false_positive": false_positive,
        "true_negative": true_negative,
        "false_negative": false_negative,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": (true_positive + true_negative) / count,
        "positive_prevalence": (true_positive + false_negative) / count,
    }
