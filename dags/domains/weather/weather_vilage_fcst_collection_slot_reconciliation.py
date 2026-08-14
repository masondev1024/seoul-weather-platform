"""Paused, API-free reconciliation for Weather KMA collection slots."""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)
DOMAINS_DIR = os.path.dirname(DAG_DIR)
if DOMAINS_DIR not in sys.path:
    sys.path.insert(0, DOMAINS_DIR)
DAGS_ROOT_DIR = os.path.dirname(DOMAINS_DIR)
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.collection_slots import (  # noqa: E402
    CollectionOutcome,
    DueSlotReconciler,
    ExpectedSlot,
    parse_activation_at,
    require_policy_boundary,
)
from common.collection_slots.receipts import CollectionSlotReceipts  # noqa: E402
from common.raw_manifest import validate_raw_manifest  # noqa: E402
from weather_ingest.collection_slots import (  # noqa: E402
    WEATHER_HISTORICAL_EVIDENCE_CODE,
    WEATHER_RAW_REPLAY_EVIDENCE_CODE,
    WEATHER_COLLECTION_SOURCE_ID,
    weather_issue_at_kst,
    weather_manifest_covers_planned_slots,
    weather_vilage_fcst_slots,
)
from weather_ingest.common.runtime import raw_prefix  # noqa: E402
from weather_ingest.kma import KMA_BASE_TIMES, KST, load_kma_grids  # noqa: E402
from weather_ingest.landing import KmaGrid  # noqa: E402
from weather_ingest.runtime import (  # noqa: E402
    build_weather_collection_slot_storage,
)


DAG_ID = "weather_vilage_fcst_collection_slot_reconciliation"
TASK_ID = "reconcile_due_weather_collection_slots"
_ACTIVATION_ENV = "ASK_SEOUL_COLLECTION_SLOT_ACTIVATION_AT"
_HISTORICAL_BOUNDARY_ENV = (
    "ASK_SEOUL_WEATHER_API_HUB_HISTORICAL_EARLIEST_ISSUED_AT"
)
_RECONCILIATION_SCHEDULE = "20 3,6,9,12,15,18,21,0 * * *"
_KMA_RAW_OBJECT_KEY = re.compile(
    r"/nx=(?P<nx>\d+)/ny=(?P<ny>\d+)/"
    r"\d{8}T\d{6}KST_base-(?P<base_date>\d{8})(?P<base_time>\d{4})_"
    r"[^/]+\.json$"
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def weather_missing_outcome(
    slot: ExpectedSlot,
    *,
    raw_manifest_key: object | None,
    raw_object_count: object | None,
    raw_manifest_verified: bool,
    event_at: datetime | str,
    dag_id: str,
    dag_run_id: str,
) -> CollectionOutcome:
    if not isinstance(raw_manifest_verified, bool):
        raise ValueError("raw_manifest_verified must be a bool")
    if raw_manifest_verified:
        if not isinstance(raw_manifest_key, str) or not raw_manifest_key:
            raise ValueError("verified Weather raw replay requires manifest_key")
        if (
            isinstance(raw_object_count, bool)
            or not isinstance(raw_object_count, int)
            or raw_object_count < 1
        ):
            raise ValueError("verified Weather raw replay requires object count")
        recovery_state = "pending"
        recovery_class = "raw_replay"
        recovery_evidence_code = WEATHER_RAW_REPLAY_EVIDENCE_CODE
        manifest_key = raw_manifest_key
        manifest_object_count = raw_object_count
    elif _slot_is_in_historical_range(slot):
        recovery_state = "pending"
        recovery_class = "historical_query"
        recovery_evidence_code = WEATHER_HISTORICAL_EVIDENCE_CODE
        manifest_key = None
        manifest_object_count = None
    else:
        recovery_state = "unrecoverable"
        recovery_class = "none"
        recovery_evidence_code = None
        manifest_key = None
        manifest_object_count = None
    return CollectionOutcome.create(
        expected_slot_id=slot.expected_slot_id,
        collection_state="missing_unknown",
        recovery_state=recovery_state,
        recovery_class=recovery_class,
        gap_reason_code="missed_collection",
        event_at=event_at,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=TASK_ID,
        raw_manifest_key=manifest_key,
        raw_object_count=manifest_object_count,
        recovery_evidence_code=recovery_evidence_code,
    )


def _slot_is_in_historical_range(slot: ExpectedSlot) -> bool:
    return _aware_utc(slot.collection_slot_at) >= _aware_utc(slot.recovery_boundary)


def _aware_utc(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError("timestamp must be an ISO timestamp")
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return parsed.astimezone(timezone.utc)


def weather_raw_manifest_evidence_by_slot(
    storage,
    slots: tuple[ExpectedSlot, ...],
) -> dict[str, dict[str, object]]:
    """Map a verified complete KMA manifest to its exact planned grid slots.

    A manifest is recovery evidence only when it describes every planned grid of
    exactly one issue cycle.  Diagnostic objects, subset backfills, malformed
    manifests, and ambiguous retry manifests are not promoted to raw replay.
    """
    slots_by_issue: dict[str, dict[tuple[int, int], ExpectedSlot]] = {}
    for slot in slots:
        grain = dict(slot.grain)
        nx = grain.get("nx")
        ny = grain.get("ny")
        if isinstance(nx, bool) or not isinstance(nx, int) or nx < 1:
            raise ValueError("Weather expected slot nx is invalid")
        if isinstance(ny, bool) or not isinstance(ny, int) or ny < 1:
            raise ValueError("Weather expected slot ny is invalid")
        grid_slots = slots_by_issue.setdefault(slot.collection_slot_at, {})
        if (nx, ny) in grid_slots:
            raise ValueError("Weather expected slots contain duplicate grid identity")
        grid_slots[(nx, ny)] = slot

    evidence_by_slot: dict[str, dict[str, object]] = {}
    manifest_prefix = (
        f"{raw_prefix().rstrip('/')}/weather/{WEATHER_COLLECTION_SOURCE_ID}/"
    )
    for manifest_key in storage.list_keys(manifest_prefix):
        if not manifest_key.endswith("/_manifest.json"):
            continue
        try:
            manifest = storage.read_json(manifest_key)
            matching_slots = _slots_proven_by_manifest(manifest, slots_by_issue)
        except (FileNotFoundError, TypeError, ValueError):
            continue
        if matching_slots is None:
            continue
        for slot, object_count in matching_slots:
            evidence = {
                "raw_manifest_key": manifest_key,
                "raw_object_count": object_count,
                "raw_manifest_verified": True,
            }
            previous = evidence_by_slot.get(slot.expected_slot_id)
            if previous is not None and previous != evidence:
                raise ValueError(
                    "conflicting Weather raw replay evidence for "
                    f"expected_slot_id={slot.expected_slot_id}"
                )
            evidence_by_slot[slot.expected_slot_id] = evidence
    return evidence_by_slot


def _slots_proven_by_manifest(
    manifest: object,
    slots_by_issue: Mapping[str, Mapping[tuple[int, int], ExpectedSlot]],
) -> list[tuple[ExpectedSlot, int]] | None:
    if not isinstance(manifest, Mapping):
        raise ValueError("KMA raw manifest must be an object")
    run_id = manifest.get("run_id")
    object_keys_value = manifest.get("object_keys")
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("KMA raw manifest run_id is invalid")
    if not isinstance(object_keys_value, list) or not object_keys_value:
        raise ValueError("KMA raw manifest object_keys are invalid")
    object_keys = [
        key
        for key in object_keys_value
        if isinstance(key, str) and key
    ]
    if len(object_keys) != len(object_keys_value) or len(object_keys) != len(
        set(object_keys)
    ):
        raise ValueError("KMA raw manifest object_keys are invalid")
    validate_raw_manifest(
        manifest,
        run_id=run_id,
        dataset=WEATHER_COLLECTION_SOURCE_ID,
        object_keys=object_keys,
    )

    for issue_at, expected_slots in slots_by_issue.items():
        expected = tuple(expected_slots.values())
        if not weather_manifest_covers_planned_slots(object_keys, expected):
            continue
        counts_by_grid: dict[tuple[int, int], int] = {}
        for raw_object_key in object_keys:
            match = _KMA_RAW_OBJECT_KEY.search(raw_object_key)
            if match is None:
                raise ValueError("KMA raw manifest object key is invalid")
            grid = (int(match.group("nx")), int(match.group("ny")))
            counts_by_grid[grid] = counts_by_grid.get(grid, 0) + 1
        return [
            (expected_slots[grid], counts_by_grid[grid])
            for grid in sorted(counts_by_grid)
        ]
    return None


def _unique_slots(slots: list[ExpectedSlot]) -> tuple[ExpectedSlot, ...]:
    by_id = {slot.expected_slot_id: slot for slot in slots}
    return tuple(
        sorted(
            by_id.values(),
            key=lambda slot: (slot.collection_slot_at, slot.expected_slot_id),
        )
    )


def weather_reconciliation_candidate_slots(
    context: Mapping[str, object],
    *,
    recovery_boundary: datetime | str,
) -> tuple[ExpectedSlot, ...]:
    """Declare the KMA issue cycle whose 60-minute grace just elapsed."""
    interval_end = _reconciliation_interval_end(context).astimezone(KST)
    issue_at = interval_end - timedelta(minutes=80)
    base_date = issue_at.strftime("%Y%m%d")
    base_time = issue_at.strftime("%H%M")
    if base_time not in KMA_BASE_TIMES:
        return ()
    grids = tuple(
        KmaGrid(str(grid["place_id"]), int(grid["nx"]), int(grid["ny"]))
        for grid in load_kma_grids()
    )
    return weather_vilage_fcst_slots(
        base_date,
        base_time,
        grids,
        recovery_boundary=recovery_boundary,
    )


def _reconciliation_interval_end(context: Mapping[str, object]) -> datetime:
    candidate = context.get("data_interval_end") or context.get("logical_date")
    if isinstance(candidate, datetime) and candidate.tzinfo is not None:
        return candidate.astimezone(timezone.utc)
    return utc_now()


def reconcile_due_weather_collection_slots(**context) -> dict[str, int]:
    activation_at = parse_activation_at(os.environ.get(_ACTIVATION_ENV))
    if activation_at is None:
        return {"declared": 0, "not_due": 0, "already_terminal": 0, "finalized": 0}
    recovery_boundary = require_policy_boundary(
        os.environ.get(_HISTORICAL_BOUNDARY_ENV),
        _HISTORICAL_BOUNDARY_ENV,
    )
    storage = build_weather_collection_slot_storage()
    receipts = CollectionSlotReceipts(storage)
    now = utc_now()
    slot_reader = DueSlotReconciler(
        storage=storage,
        receipts=receipts,
        clock=lambda: now,
        outcome_factory=lambda _slot: None,
    )
    existing_slots = slot_reader.existing_slots(
        domain="weather",
        source_id=WEATHER_COLLECTION_SOURCE_ID,
    )
    candidates = _unique_slots(
        [
            *existing_slots,
            *weather_reconciliation_candidate_slots(
                context,
                recovery_boundary=recovery_boundary,
            ),
        ]
    )
    due_slots = tuple(
        slot
        for slot in candidates
        if _aware_utc(slot.deadline_at) <= now
    )
    raw_evidence_by_slot = weather_raw_manifest_evidence_by_slot(
        storage,
        due_slots,
    )
    reconciler = DueSlotReconciler(
        storage=storage,
        receipts=receipts,
        clock=lambda: now,
        outcome_factory=lambda slot: weather_missing_outcome(
            slot,
            **raw_evidence_by_slot.get(
                slot.expected_slot_id,
                {
                    "raw_manifest_key": None,
                    "raw_object_count": None,
                    "raw_manifest_verified": False,
                },
            ),
            event_at=now,
            dag_id=DAG_ID,
            dag_run_id=str(context["run_id"]),
        ),
    )
    return reconciler.run(candidates, activation_at=activation_at).as_dict()


with DAG(
    dag_id=DAG_ID,
    description="Settle declared Weather KMA slots without KMA or API Hub requests.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=_RECONCILIATION_SCHEDULE,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["ask_seoul", "weather", "collection-slot", "reconciliation", "control"],
) as dag:
    reconcile = PythonOperator(
        task_id=TASK_ID,
        python_callable=reconcile_due_weather_collection_slots,
    )
