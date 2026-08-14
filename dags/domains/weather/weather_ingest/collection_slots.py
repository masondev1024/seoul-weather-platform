"""Pure expected-slot and terminal-outcome contracts for KMA issue cycles."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone

from common.collection_slots.contract import CollectionOutcome, ExpectedSlot
from weather_ingest.kma import KST, normalize_kma_base_datetime
from weather_ingest.landing import KmaGrid


WEATHER_COLLECTION_CONTRACT_ID = "weather.vilage_fcst.v1"
WEATHER_COLLECTION_SCHEDULE_VERSION = "weather.vilage_fcst.issue_cycle.v1"
WEATHER_COLLECTION_SOURCE_ID = "kma_vilage_fcst"
WEATHER_COLLECTION_DECLARED_BY = "weather_vilage_fcst_bronze"
WEATHER_RAW_REPLAY_EVIDENCE_CODE = "raw_manifest_verified"
WEATHER_HISTORICAL_EVIDENCE_CODE = "weather_apihub_historical_range"
_KMA_RAW_OBJECT_KEY = re.compile(
    r"/nx=(?P<nx>\d+)/ny=(?P<ny>\d+)/"
    r"\d{8}T\d{6}KST_base-(?P<base_date>\d{8})(?P<base_time>\d{4})_"
    r"[^/]+\.json$"
)


class WeatherCollectionSlotError(ValueError):
    """KMA expected-slot or terminal-outcome evidence is invalid."""


def weather_issue_at_kst(base_date: object, base_time: object) -> datetime:
    """Return the timezone-aware KST issue time for one KMA base cycle."""
    try:
        normalized_date, normalized_time = normalize_kma_base_datetime(
            base_date,
            base_time,
        )
        return datetime.strptime(
            normalized_date + normalized_time,
            "%Y%m%d%H%M",
        ).replace(tzinfo=KST)
    except ValueError as exc:
        raise WeatherCollectionSlotError(str(exc)) from exc


def weather_vilage_fcst_slots(
    base_date: object,
    base_time: object,
    grids: Iterable[KmaGrid],
    *,
    recovery_boundary: datetime | str,
) -> tuple[ExpectedSlot, ...]:
    """Plan one expected slot per configured KMA grid for an issue cycle."""
    issue_at_kst = weather_issue_at_kst(base_date, base_time)
    unique_grids: list[tuple[str, int, int]] = []
    seen_grid_keys: set[tuple[int, int]] = set()
    for grid in grids:
        try:
            place_id = str(grid.place_id).strip()
            nx = int(grid.nx)
            ny = int(grid.ny)
        except (AttributeError, TypeError, ValueError) as exc:
            raise WeatherCollectionSlotError("KMA grid identity is invalid") from exc
        if not place_id or nx < 1 or ny < 1:
            raise WeatherCollectionSlotError("KMA grid identity is invalid")
        grid_key = (nx, ny)
        if grid_key in seen_grid_keys:
            raise WeatherCollectionSlotError(
                f"KMA issue cycle contains duplicate grid: nx={nx}, ny={ny}"
            )
        seen_grid_keys.add(grid_key)
        unique_grids.append((place_id, nx, ny))
    if not unique_grids:
        raise WeatherCollectionSlotError("KMA issue cycle requires at least one grid")

    return tuple(
        ExpectedSlot.create(
            contract_version="v1",
            domain="weather",
            collection_contract_id=WEATHER_COLLECTION_CONTRACT_ID,
            source_id=WEATHER_COLLECTION_SOURCE_ID,
            collection_slot_at=issue_at_kst,
            scheduled_at=issue_at_kst,
            deadline_at=issue_at_kst + timedelta(minutes=60),
            grain={"place_id": place_id, "nx": nx, "ny": ny},
            schedule_version=WEATHER_COLLECTION_SCHEDULE_VERSION,
            is_scheduled=True,
            recovery_boundary_type="weather_apihub_historical_earliest_issued_at",
            recovery_boundary=_as_utc(recovery_boundary, field="recovery_boundary").isoformat(),
            declared_at=issue_at_kst,
            declared_by=WEATHER_COLLECTION_DECLARED_BY,
        )
        for place_id, nx, ny in unique_grids
    )


def weather_collection_success_outcomes(
    slots: Iterable[ExpectedSlot],
    *,
    raw_manifest_key: object,
    raw_objects: Iterable[Mapping[str, object]],
    verified_rows: object,
    event_at: datetime | str,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
) -> tuple[CollectionOutcome, ...]:
    """Build terminal success evidence only from exact verified raw-grid inputs."""
    planned_slots = _slots(slots)
    manifest_key = _required_text(raw_manifest_key, field="raw_manifest_key")
    total_verified_rows = _non_negative_count(verified_rows, field="verified_rows")

    summaries: dict[tuple[str, int, int], dict[str, object]] = {}
    raw_keys: set[str] = set()
    for raw_object in raw_objects:
        if not isinstance(raw_object, Mapping):
            raise WeatherCollectionSlotError("KMA raw object must be an object")
        raw_object_key = _required_text(
            raw_object.get("raw_object_key"),
            field="raw_object_key",
        )
        if raw_object_key in raw_keys:
            raise WeatherCollectionSlotError(
                f"KMA success evidence contains duplicate raw_object_key: {raw_object_key}"
            )
        raw_keys.add(raw_object_key)

        place_id = _required_text(raw_object.get("place_id"), field="place_id")
        nx = _positive_integer(raw_object.get("nx"), field="nx")
        ny = _positive_integer(raw_object.get("ny"), field="ny")
        issue_at = weather_issue_at_kst(
            raw_object.get("base_date"),
            raw_object.get("base_time"),
        )
        row_count = _non_negative_count(raw_object.get("row_count"), field="row_count")
        grid_key = (place_id, nx, ny)
        summary = summaries.setdefault(
            grid_key,
            {"row_count": 0, "raw_object_count": 0, "issue_at": issue_at},
        )
        if summary["issue_at"] != issue_at:
            raise WeatherCollectionSlotError(
                f"KMA raw objects disagree on issue cycle for grid={grid_key}"
            )
        summary["row_count"] = int(summary["row_count"]) + row_count
        summary["raw_object_count"] = int(summary["raw_object_count"]) + 1

    if not raw_keys:
        raise WeatherCollectionSlotError("KMA success evidence requires raw objects")
    if sum(int(summary["row_count"]) for summary in summaries.values()) != total_verified_rows:
        raise WeatherCollectionSlotError(
            "KMA success evidence verified_rows does not match raw row_count"
        )

    expected_grid_keys = {
        _slot_grid_key(slot): slot
        for slot in planned_slots
    }
    if set(summaries) != set(expected_grid_keys):
        raise WeatherCollectionSlotError(
            "KMA success evidence grid set does not match planned slots"
        )
    if any(int(summary["row_count"]) == 0 for summary in summaries.values()):
        raise WeatherCollectionSlotError(
            "KMA success evidence requires forecast rows for every planned grid"
        )

    outcomes: list[CollectionOutcome] = []
    for slot in planned_slots:
        grid_key = _slot_grid_key(slot)
        summary = summaries[grid_key]
        issue_at = summary["issue_at"]
        assert isinstance(issue_at, datetime)
        if _as_utc(issue_at, field="raw issue cycle").isoformat() != slot.collection_slot_at:
            raise WeatherCollectionSlotError(
                "KMA success evidence issue cycle does not match planned slot"
            )
        row_count = int(summary["row_count"])
        outcomes.append(
            CollectionOutcome.create(
                expected_slot_id=slot.expected_slot_id,
                collection_state="observed",
                recovery_state="not_required",
                recovery_class="none",
                event_at=event_at,
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_id,
                raw_manifest_key=manifest_key,
                raw_object_count=int(summary["raw_object_count"]),
                row_count=row_count,
                source_result_code="00",
            )
        )
    return tuple(outcomes)


def weather_collection_failure_outcomes(
    slots: Iterable[ExpectedSlot],
    *,
    raw_manifest_key: object | None,
    raw_object_count: object | None,
    raw_manifest_verified: bool,
    event_at: datetime | str,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
) -> tuple[CollectionOutcome, ...]:
    """Classify a failed KMA issue cycle without treating diagnostic raw as replayable."""
    planned_slots = _slots(slots)
    if not isinstance(raw_manifest_verified, bool):
        raise WeatherCollectionSlotError("raw_manifest_verified must be a bool")
    manifest_key = (
        _required_text(raw_manifest_key, field="raw_manifest_key")
        if raw_manifest_verified
        else None
    )
    manifest_object_count = (
        _non_negative_count(raw_object_count, field="raw_object_count")
        if raw_manifest_verified
        else None
    )

    outcomes: list[CollectionOutcome] = []
    for slot in planned_slots:
        if raw_manifest_verified:
            recovery_state = "pending"
            recovery_class = "raw_replay"
            recovery_evidence_code = WEATHER_RAW_REPLAY_EVIDENCE_CODE
        elif _as_utc(slot.collection_slot_at, field="collection_slot_at") >= _as_utc(
            slot.recovery_boundary,
            field="recovery_boundary",
        ):
            recovery_state = "pending"
            recovery_class = "historical_query"
            recovery_evidence_code = WEATHER_HISTORICAL_EVIDENCE_CODE
        else:
            recovery_state = "unrecoverable"
            recovery_class = "none"
            recovery_evidence_code = None
        outcomes.append(
            CollectionOutcome.create(
                expected_slot_id=slot.expected_slot_id,
                collection_state="collection_failed",
                recovery_state=recovery_state,
                recovery_class=recovery_class,
                gap_reason_code="collection_failed",
                event_at=event_at,
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_id,
                raw_manifest_key=manifest_key,
                raw_object_count=manifest_object_count,
                recovery_evidence_code=recovery_evidence_code,
            )
        )
    return tuple(outcomes)


def weather_manifest_covers_planned_slots(
    object_keys: Iterable[object],
    slots: Iterable[ExpectedSlot],
) -> bool:
    """Return true only for one issue cycle covering every planned grid."""
    planned_slots = _slots(slots)
    planned_by_grid = {
        _slot_grid_key(slot): slot
        for slot in planned_slots
    }
    if len(planned_by_grid) != len(planned_slots):
        raise WeatherCollectionSlotError("Weather collection slots contain duplicate grids")

    keys = tuple(object_keys)
    if not keys or any(not isinstance(key, str) or not key for key in keys):
        return False
    if len(keys) != len(set(keys)):
        return False

    issue_at: str | None = None
    covered_grids: set[tuple[str, int, int]] = set()
    for key in keys:
        assert isinstance(key, str)
        match = _KMA_RAW_OBJECT_KEY.search(key)
        if match is None:
            return False
        current_issue_at = _as_utc(
            weather_issue_at_kst(match.group("base_date"), match.group("base_time")),
            field="raw issue cycle",
        ).isoformat()
        if issue_at is not None and issue_at != current_issue_at:
            return False
        issue_at = current_issue_at
        grid = (int(match.group("nx")), int(match.group("ny")))
        matched_slots = [
            slot_grid
            for slot_grid, slot in planned_by_grid.items()
            if slot_grid[1:] == grid
        ]
        if len(matched_slots) != 1:
            return False
        covered_grids.add(matched_slots[0])

    if issue_at is None:
        return False
    if any(slot.collection_slot_at != issue_at for slot in planned_slots):
        return False
    return covered_grids == set(planned_by_grid)


def _slots(slots: Iterable[ExpectedSlot]) -> tuple[ExpectedSlot, ...]:
    planned_slots = tuple(slots)
    if not planned_slots:
        raise WeatherCollectionSlotError("Weather collection outcome requires slots")
    if not all(isinstance(slot, ExpectedSlot) for slot in planned_slots):
        raise WeatherCollectionSlotError("Weather collection slots must be ExpectedSlot")
    if len({slot.expected_slot_id for slot in planned_slots}) != len(planned_slots):
        raise WeatherCollectionSlotError("Weather collection slots contain duplicate ids")
    return planned_slots


def _slot_grid_key(slot: ExpectedSlot) -> tuple[str, int, int]:
    grain = dict(slot.grain)
    return (
        _required_text(grain.get("place_id"), field="slot place_id"),
        _positive_integer(grain.get("nx"), field="slot nx"),
        _positive_integer(grain.get("ny"), field="slot ny"),
    )


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WeatherCollectionSlotError(f"{field} must be a non-empty string")
    return value


def _positive_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise WeatherCollectionSlotError(f"{field} must be a positive integer")
    return value


def _non_negative_count(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise WeatherCollectionSlotError(f"{field} must be a non-negative integer")
    return value


def _as_utc(value: datetime | str, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise WeatherCollectionSlotError(
                f"{field} must be an ISO timestamp"
            ) from exc
    else:
        raise WeatherCollectionSlotError(
            f"{field} must be a datetime or ISO timestamp"
        )
    if parsed.tzinfo is None:
        raise WeatherCollectionSlotError(f"{field} must include timezone")
    return parsed.astimezone(timezone.utc)


__all__ = [
    "WEATHER_COLLECTION_CONTRACT_ID",
    "WEATHER_COLLECTION_DECLARED_BY",
    "WEATHER_COLLECTION_SCHEDULE_VERSION",
    "WEATHER_COLLECTION_SOURCE_ID",
    "WEATHER_HISTORICAL_EVIDENCE_CODE",
    "WEATHER_RAW_REPLAY_EVIDENCE_CODE",
    "WeatherCollectionSlotError",
    "weather_collection_failure_outcomes",
    "weather_collection_success_outcomes",
    "weather_manifest_covers_planned_slots",
    "weather_issue_at_kst",
    "weather_vilage_fcst_slots",
]
