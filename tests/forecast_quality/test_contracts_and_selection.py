from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from weather_quality.models import (
    ContractError,
    ForecastVintage,
    ObservationTruth,
    TruthQuality,
)
from weather_quality.selection import resolve_observation_truth, select_forecast_vintages


UTC = timezone.utc
VALID_AT = datetime(2026, 8, 20, 12, tzinfo=UTC)
EVALUATION_AS_OF = VALID_AT + timedelta(hours=7)


def forecast(*, hours_before: float, value: float = 20.0) -> ForecastVintage:
    return ForecastVintage(
        product_family="short_range",
        grid_id="kma_60_127",
        nx=60,
        ny=127,
        variable="temperature_air_2m",
        value_kind="continuous",
        value=value,
        unit="degC",
        issued_at=VALID_AT - timedelta(hours=hours_before),
        valid_at=VALID_AT,
        source_id="kma_vilage_fcst",
        source_revision="fixture-v1",
    )


def truth(
    *,
    value: float,
    truth_as_of: datetime,
    collected_at: datetime | None = None,
    revision: str = "r1",
    quality: TruthQuality = TruthQuality.FINAL,
) -> ObservationTruth:
    return ObservationTruth(
        grid_id="kma_60_127",
        nx=60,
        ny=127,
        variable="temperature_air_2m",
        value_kind="continuous",
        value=value,
        unit="degC",
        observed_at=VALID_AT,
        truth_source="synthetic_observation_fixture",
        truth_revision=revision,
        truth_as_of=truth_as_of,
        collected_at=collected_at or truth_as_of,
        quality=quality,
    )


def test_contract_rejects_naive_timestamps_and_invalid_probability() -> None:
    with pytest.raises(ContractError, match="timezone-aware"):
        ForecastVintage(
            product_family="short_range",
            grid_id="kma_60_127",
            nx=60,
            ny=127,
            variable="temperature_air_2m",
            value_kind="continuous",
            value=20.0,
            unit="degC",
            issued_at=datetime(2026, 8, 19, 12),
            valid_at=VALID_AT,
            source_id="kma_vilage_fcst",
            source_revision="fixture-v1",
        )

    with pytest.raises(ContractError, match="probability"):
        ForecastVintage(
            product_family="short_range",
            grid_id="kma_60_127",
            nx=60,
            ny=127,
            variable="precipitation_occurrence",
            value_kind="probability",
            value=1.01,
            unit="1",
            issued_at=VALID_AT - timedelta(hours=24),
            valid_at=VALID_AT,
            source_id="kma_vilage_fcst",
            source_revision="fixture-v1",
        )


def test_duplicate_error_has_safe_identity_fingerprint_without_lineage_value() -> None:
    original = forecast(hours_before=72)
    duplicate = replace(original, source_revision="must-not-appear-in-error")

    with pytest.raises(ContractError, match="identity_fingerprint=") as caught:
        select_forecast_vintages([original, duplicate])

    assert "must-not-appear-in-error" not in str(caught.value)


@pytest.mark.parametrize(
    ("hours_before", "expected_label"),
    [(75, "D-3"), (72, "D-3"), (51, "D-2"), (48, "D-2"), (27, "D-1"), (24, "D-1")],
)
def test_vintage_cutoff_includes_exact_three_hour_window_boundaries(
    hours_before: int, expected_label: str
) -> None:
    result = select_forecast_vintages([forecast(hours_before=hours_before)])

    assert result.selected[0].vintage_label == expected_label


def test_vintage_selects_latest_candidate_and_never_substitutes_missing_label() -> None:
    result = select_forecast_vintages(
        [
            forecast(hours_before=75, value=18.0),
            forecast(hours_before=73, value=19.0),
            forecast(hours_before=48, value=20.0),
            forecast(hours_before=24, value=21.0),
        ]
    )

    selected = {item.vintage_label: item.forecast.value for item in result.selected}
    assert selected == {"D-3": 19.0, "D-2": 20.0, "D-1": 21.0}

    missing = select_forecast_vintages(
        [forecast(hours_before=76), forecast(hours_before=48), forecast(hours_before=24)]
    )
    assert "D-3" not in {item.vintage_label for item in missing.selected}
    assert any(gap.vintage_label == "D-3" for gap in missing.gaps)


def test_vintage_selection_fails_closed_on_unit_drift_within_series() -> None:
    celsius = forecast(hours_before=72)
    fahrenheit = ForecastVintage(
        product_family=celsius.product_family,
        grid_id=celsius.grid_id,
        nx=celsius.nx,
        ny=celsius.ny,
        variable=celsius.variable,
        value_kind=celsius.value_kind,
        value=68.0,
        unit="degF",
        issued_at=VALID_AT - timedelta(hours=48),
        valid_at=VALID_AT,
        source_id=celsius.source_id,
        source_revision=celsius.source_revision,
    )

    with pytest.raises(ContractError, match="incompatible value kind or unit"):
        select_forecast_vintages([celsius, fahrenheit])


def test_truth_resolution_prevents_future_revision_and_collection_leakage() -> None:
    earlier = truth(value=20.0, truth_as_of=VALID_AT + timedelta(hours=1), revision="r1")
    future_revision = truth(
        value=19.0,
        truth_as_of=EVALUATION_AS_OF + timedelta(seconds=1),
        revision="r2",
    )
    future_collection = truth(
        value=18.0,
        truth_as_of=VALID_AT + timedelta(hours=2),
        collected_at=EVALUATION_AS_OF + timedelta(seconds=1),
        revision="r3",
    )

    resolved = resolve_observation_truth(
        [earlier, future_revision, future_collection], evaluation_as_of=EVALUATION_AS_OF
    )

    assert resolved.selected == (earlier,)
    assert resolved.excluded_counts == {"future_truth_revision": 1, "future_collection": 1}


def test_truth_resolution_fails_closed_on_same_as_of_conflict() -> None:
    same_as_of = VALID_AT + timedelta(hours=1)

    with pytest.raises(ContractError, match="conflicting truth revisions"):
        resolve_observation_truth(
            [
                truth(value=20.0, truth_as_of=same_as_of, revision="r1"),
                truth(value=21.0, truth_as_of=same_as_of, revision="r2"),
            ],
            evaluation_as_of=EVALUATION_AS_OF,
        )


def test_later_evaluation_selects_the_late_final_revision() -> None:
    provisional = truth(
        value=20.0,
        truth_as_of=VALID_AT + timedelta(hours=1),
        revision="r1",
        quality=TruthQuality.PROVISIONAL,
    )
    final = truth(
        value=19.5,
        truth_as_of=VALID_AT + timedelta(hours=8),
        revision="r2",
        quality=TruthQuality.FINAL,
    )

    early = resolve_observation_truth(
        [provisional, final], evaluation_as_of=VALID_AT + timedelta(hours=2)
    )
    late = resolve_observation_truth(
        [provisional, final], evaluation_as_of=VALID_AT + timedelta(hours=9)
    )

    assert early.selected == (provisional,)
    assert early.degraded is True
    assert late.selected == (final,)
    assert late.degraded is False


def test_provisional_truth_is_degraded_through_six_hours_then_stale() -> None:
    provisional = truth(
        value=20.0,
        truth_as_of=VALID_AT + timedelta(hours=1),
        quality=TruthQuality.PROVISIONAL,
    )

    exact = resolve_observation_truth(
        [provisional], evaluation_as_of=VALID_AT + timedelta(hours=6)
    )
    after = resolve_observation_truth(
        [provisional], evaluation_as_of=VALID_AT + timedelta(hours=6, seconds=1)
    )

    assert exact.selected == (provisional,)
    assert exact.degraded is True
    assert after.selected == ()
    assert after.excluded_counts == {"stale_provisional_truth": 1}
