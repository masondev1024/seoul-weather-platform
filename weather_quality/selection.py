from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Protocol, TypeAlias

from weather_quality.models import ContractError, ForecastVintage, ObservationTruth, TruthQuality


VINTAGE_POLICY_VERSION = "forecast-vintage-cutoff/v1"
TRUTH_POLICY_VERSION = "observation-truth-policy/v1"
VINTAGE_HOURS = (("D-3", 72), ("D-2", 48), ("D-1", 24))
VINTAGE_LOOKBACK = timedelta(hours=3)
PROVISIONAL_TRUTH_MAX_AGE = timedelta(hours=6)
ForecastGroupKey: TypeAlias = tuple[str, str, str, datetime, str, str]


class IdentifiedRecord(Protocol):
    @property
    def identity(self) -> tuple[object, ...]: ...


def _identity_fingerprint(identity: tuple[object, ...]) -> str:
    return hashlib.sha256(repr(identity).encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True, slots=True)
class SelectedVintage:
    vintage_label: str
    forecast: ForecastVintage


@dataclass(frozen=True, slots=True)
class VintageGap:
    product_family: str
    grid_id: str
    variable: str
    valid_at: datetime
    vintage_label: str
    reason: str = "missing_vintage"


@dataclass(frozen=True, slots=True)
class VintageSelection:
    selected: tuple[SelectedVintage, ...]
    gaps: tuple[VintageGap, ...]


@dataclass(frozen=True, slots=True)
class TruthResolution:
    selected: tuple[ObservationTruth, ...]
    excluded_counts: dict[str, int]
    degraded: bool


def _reject_duplicate_identities(
    records: Iterable[IdentifiedRecord], identity_name: str
) -> None:
    seen: set[tuple[object, ...]] = set()
    for record in records:
        identity = record.identity
        if identity in seen:
            raise ContractError(
                f"duplicate {identity_name} identity; "
                f"identity_fingerprint={_identity_fingerprint(identity)}"
            )
        seen.add(identity)


def select_forecast_vintages(forecasts: Iterable[ForecastVintage]) -> VintageSelection:
    rows = tuple(forecasts)
    _reject_duplicate_identities(rows, "forecast")
    series_contracts: dict[tuple[object, ...], set[tuple[str, str]]] = defaultdict(set)
    for row in rows:
        series_contracts[(row.product_family, row.grid_id, row.variable, row.valid_at)].add(
            (row.value_kind, row.unit)
        )
    for series_identity, contracts in series_contracts.items():
        if len(contracts) > 1:
            raise ContractError(
                "forecast series has incompatible value kind or unit; "
                f"identity_fingerprint={_identity_fingerprint(series_identity)}"
            )
    groups: dict[ForecastGroupKey, list[ForecastVintage]] = defaultdict(list)
    for row in rows:
        groups[
            (
                row.product_family,
                row.grid_id,
                row.variable,
                row.valid_at,
                row.value_kind,
                row.unit,
            )
        ].append(row)

    selected: list[SelectedVintage] = []
    gaps: list[VintageGap] = []
    for group_key in sorted(groups, key=lambda item: tuple(str(value) for value in item)):
        product_family, grid_id, variable, valid_at, _, _ = group_key
        candidates = groups[group_key]
        for label, horizon_hours in VINTAGE_HOURS:
            cutoff = valid_at - timedelta(hours=horizon_hours)
            eligible = [
                row for row in candidates if cutoff - VINTAGE_LOOKBACK <= row.issued_at <= cutoff
            ]
            if not eligible:
                gaps.append(
                    VintageGap(
                        product_family=str(product_family),
                        grid_id=str(grid_id),
                        variable=str(variable),
                        valid_at=valid_at,
                        vintage_label=label,
                    )
                )
                continue
            winner = max(eligible, key=lambda row: (row.issued_at, row.source_revision, row.source_id))
            selected.append(SelectedVintage(label, winner))

    selected.sort(
        key=lambda item: (
            item.forecast.product_family,
            item.forecast.variable,
            item.forecast.valid_at,
            item.vintage_label,
            item.forecast.grid_id,
        )
    )
    gaps.sort(
        key=lambda item: (
            item.product_family,
            item.variable,
            item.valid_at,
            item.vintage_label,
            item.grid_id,
        )
    )
    return VintageSelection(tuple(selected), tuple(gaps))


def resolve_observation_truth(
    observations: Iterable[ObservationTruth], *, evaluation_as_of: datetime
) -> TruthResolution:
    if evaluation_as_of.tzinfo is None or evaluation_as_of.utcoffset() is None:
        raise ContractError("evaluation_as_of must be timezone-aware")
    rows = tuple(observations)
    _reject_duplicate_identities(rows, "observation truth")
    excluded: Counter[str] = Counter()
    visible: list[ObservationTruth] = []
    for row in rows:
        if row.truth_as_of > evaluation_as_of:
            excluded["future_truth_revision"] += 1
        elif row.collected_at > evaluation_as_of:
            excluded["future_collection"] += 1
        else:
            visible.append(row)

    groups: dict[tuple[object, ...], list[ObservationTruth]] = defaultdict(list)
    for row in visible:
        groups[(row.truth_source, row.grid_id, row.variable, row.observed_at)].append(row)

    selected: list[ObservationTruth] = []
    degraded = False
    for key in sorted(groups, key=lambda item: tuple(str(value) for value in item)):
        candidates = groups[key]
        latest_as_of = max(row.truth_as_of for row in candidates)
        latest = [row for row in candidates if row.truth_as_of == latest_as_of]
        semantic_values = {(row.value_kind, row.value, row.unit, row.quality) for row in latest}
        if len(semantic_values) > 1:
            raise ContractError(
                "conflicting truth revisions at selected truth_as_of; "
                f"identity_fingerprint={_identity_fingerprint(key)}"
            )
        winner = max(latest, key=lambda row: row.truth_revision)
        if winner.quality is TruthQuality.REJECTED:
            excluded["rejected_truth"] += 1
            continue
        if (
            winner.quality is TruthQuality.PROVISIONAL
            and evaluation_as_of > winner.observed_at + PROVISIONAL_TRUTH_MAX_AGE
        ):
            excluded["stale_provisional_truth"] += 1
            continue
        if winner.quality is TruthQuality.PROVISIONAL:
            degraded = True
        selected.append(winner)

    selected.sort(key=lambda row: (row.truth_source, row.variable, row.observed_at, row.grid_id))
    return TruthResolution(tuple(selected), dict(sorted(excluded.items())), degraded)
