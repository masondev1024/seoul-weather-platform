"""Adapter from normalized KMA source records to forecast-quality truth."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Protocol

from weather_quality.models import ContractError, ObservationTruth, TruthQuality


SOURCE_ID = "kma_ultra_srt_ncst"
REQUIRED_CATEGORIES = frozenset({"T1H", "RN1", "UUU", "VVV", "REH", "PTY", "VEC", "WSD"})


class ObservationSourceRecord(Protocol):
    source_id: str
    grid_id: str
    nx: int
    ny: int
    observed_at: datetime
    category: str
    value: float | int
    unit: str
    collected_at: datetime
    payload_sha256: str
    source_revision: str


def to_observation_truth(
    records: Iterable[ObservationSourceRecord],
) -> tuple[ObservationTruth, ObservationTruth]:
    """Build provisional truth rows from one complete grid/slot snapshot.

    The near-real-time endpoint can be revised, so its first snapshot is
    deliberately provisional. A future archived-source adapter may promote a
    reconciled observation to ``FINAL`` without changing this source contract.
    """

    rows = tuple(records)
    if len(rows) != len(REQUIRED_CATEGORIES):
        raise ContractError("KMA observation truth requires exactly eight source categories")
    by_category: dict[str, ObservationSourceRecord] = {}
    for row in rows:
        if row.category in by_category:
            raise ContractError(f"duplicate KMA observation truth category: {row.category}")
        by_category[row.category] = row
    if set(by_category) != REQUIRED_CATEGORIES:
        raise ContractError("KMA observation truth category set is incomplete or unversioned")

    first = rows[0]
    common_identity = (
        first.source_id,
        first.grid_id,
        first.nx,
        first.ny,
        first.observed_at,
        first.collected_at,
        first.payload_sha256,
        first.source_revision,
    )
    for row in rows:
        if (
            row.source_id,
            row.grid_id,
            row.nx,
            row.ny,
            row.observed_at,
            row.collected_at,
            row.payload_sha256,
            row.source_revision,
        ) != common_identity:
            raise ContractError("KMA observation truth records do not share one source snapshot")
    if first.source_id != SOURCE_ID:
        raise ContractError("KMA observation truth source is invalid")
    if by_category["T1H"].unit != "degC" or by_category["RN1"].unit != "mm":
        raise ContractError("KMA observation truth source units are invalid")
    if by_category["PTY"].unit != "code":
        raise ContractError("KMA observation PTY source unit is invalid")

    temperature_value = float(by_category["T1H"].value)
    pty_value = int(by_category["PTY"].value)
    rn1_value = float(by_category["RN1"].value)
    common = {
        "grid_id": first.grid_id,
        "nx": first.nx,
        "ny": first.ny,
        "observed_at": first.observed_at,
        "truth_source": first.source_id,
        "truth_revision": first.source_revision,
        "truth_as_of": first.collected_at,
        "collected_at": first.collected_at,
        "quality": TruthQuality.PROVISIONAL,
    }
    return (
        ObservationTruth(
            **common,
            variable="temperature_air_2m",
            value_kind="continuous",
            value=temperature_value,
            unit="degC",
        ),
        ObservationTruth(
            **common,
            variable="precipitation_occurrence",
            value_kind="binary",
            value=pty_value != 0 or rn1_value > 0,
            unit="1",
        ),
    )


__all__ = ["ObservationSourceRecord", "to_observation_truth"]
