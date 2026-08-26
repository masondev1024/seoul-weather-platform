from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "dags/domains/weather"))

from weather_ingest.kma_observation import (  # noqa: E402
    REQUIRED_CATEGORIES,
    KmaObservationRecord,
)
from weather_quality.kma_observation import to_observation_truth  # noqa: E402
from weather_quality.models import ContractError, TruthQuality  # noqa: E402


UTC = timezone.utc


def _records(*, pty: int = 0, rn1: float = 0.0, payload_hash: str = "a" * 64):
    values = {
        "T1H": 27.4,
        "RN1": rn1,
        "UUU": -0.8,
        "VVV": 1.1,
        "REH": 68.0,
        "PTY": pty,
        "VEC": 324.0,
        "WSD": 1.4,
    }
    observed_at = datetime(2026, 8, 22, 5, 0, tzinfo=UTC)
    collected_at = datetime(2026, 8, 22, 5, 15, tzinfo=UTC)
    revision = f"kma_ultra_srt_ncst:{payload_hash}"
    return tuple(
        KmaObservationRecord(
            source_id="kma_ultra_srt_ncst",
            grid_id="kma_60_127",
            nx=60,
            ny=127,
            observed_at=observed_at,
            category=category,
            value=values[category],
            unit={
                "T1H": "degC", "RN1": "mm", "UUU": "m/s", "VVV": "m/s",
                "REH": "percent", "PTY": "code", "VEC": "degree", "WSD": "m/s",
            }[category],
            collected_at=collected_at,
            payload_sha256=payload_hash,
            source_revision=revision,
        )
        for category in REQUIRED_CATEGORIES
    )


def test_quality_adapter_maps_temperature_and_dry_precipitation_truth() -> None:
    temperature, precipitation = to_observation_truth(_records())

    assert (temperature.variable, temperature.value_kind, temperature.unit,
            temperature.value) == ("temperature_air_2m", "continuous", "degC", 27.4)
    assert (precipitation.variable, precipitation.value_kind, precipitation.unit,
            precipitation.value) == ("precipitation_occurrence", "binary", "1", False)
    assert temperature.observed_at == datetime(2026, 8, 22, 5, 0, tzinfo=UTC)
    assert temperature.truth_as_of == datetime(2026, 8, 22, 5, 15, tzinfo=UTC)
    assert temperature.quality is TruthQuality.PROVISIONAL


@pytest.mark.parametrize(("pty", "rn1"), [(1, 0.0), (0, 0.1), (7, 0.0)])
def test_quality_adapter_uses_both_pty_and_rn1_for_occurrence(pty: int, rn1: float) -> None:
    _, precipitation = to_observation_truth(_records(pty=pty, rn1=rn1))
    assert precipitation.value is True


def test_quality_adapter_fails_closed_on_partial_or_ambiguous_sets() -> None:
    records = _records()
    with pytest.raises(ContractError):
        to_observation_truth(records[:-1])
    with pytest.raises(ContractError):
        to_observation_truth((*records, records[0]))


def test_revision_is_stable_for_same_payload_and_changes_with_payload_hash() -> None:
    first = to_observation_truth(_records(payload_hash="a" * 64))[0]
    same = to_observation_truth(_records(payload_hash="a" * 64))[0]
    changed = to_observation_truth(_records(payload_hash="b" * 64))[0]

    assert first.truth_revision == same.truth_revision
    assert changed.truth_revision != first.truth_revision
