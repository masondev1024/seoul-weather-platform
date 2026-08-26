"""Build bounded recovery candidates from validated Weather slot receipts.

The adapter is read-only.  It consumes the ``ReceiptBatch`` produced by
``CollectionSlotMaterializer`` and intentionally omits raw object names and
other storage identifiers from the planner input.  A candidate represents one
KMA issue cycle (the unit that a future recollect/replay DAG can process
atomically), not an individual grid trigger.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone

from common.collection_slots.contract import ExpectedSlot
from common.collection_slots.materializer import (
    CollectionSlotMaterializer,
    ReceiptBatch,
)
from common.recovery.planner import RecoveryCandidate, RecoveryPlannerError


TERMINAL_COLLECTION_STATES = frozenset(
    {"observed", "source_empty_valid", "not_scheduled"}
)


class RecoveryCandidateEvidenceError(ValueError):
    """Receipt evidence cannot be converted into a safe recovery candidate."""


def read_weather_recovery_candidates(
    storage: object,
    *,
    now: datetime,
    active_run_ids: Mapping[str, str] | None = None,
) -> tuple[RecoveryCandidate, ...]:
    """Read and validate receipt evidence without invoking any source or sink.

    ``storage`` must implement the read-only methods required by
    :class:`CollectionSlotMaterializer`.  The materializer validates every
    receipt before this adapter groups it.
    """
    batch = CollectionSlotMaterializer(storage, object()).read_batch()  # type: ignore[arg-type]
    return candidates_from_receipts(batch, now=now, active_run_ids=active_run_ids)


def candidates_from_receipts(
    batch: ReceiptBatch,
    *,
    now: datetime,
    active_run_ids: Mapping[str, str] | None = None,
) -> tuple[RecoveryCandidate, ...]:
    """Return one candidate per pending source issue cycle.

    A complete raw replay requires the same manifest identity on every pending
    grid.  Historical recollect is allowed only when every pending grid has the
    explicit historical-query recovery class.  Anything else is left for an
    operator rather than guessed.
    """
    normalized_now = _aware_utc(now, field="now")
    expected_by_id = _expected_slots(batch.expected)
    events_by_slot: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for event in batch.events:
        expected_slot_id = _required_text(event.get("expected_slot_id"), "expected_slot_id")
        if expected_slot_id not in expected_by_id:
            raise RecoveryCandidateEvidenceError(
                "event references unknown expected slot"
            )
        events_by_slot[expected_slot_id].append(event)

    groups: dict[tuple[str, str, str], list[tuple[ExpectedSlot, Mapping[str, object] | None]]] = defaultdict(list)
    for expected_slot_id, slot in expected_by_id.items():
        events = events_by_slot.get(expected_slot_id, [])
        if _has_terminal_event(events):
            continue
        latest = _latest_event(events)
        groups[(slot.domain, slot.source_id, slot.collection_slot_at)].append((slot, latest))

    candidates: list[RecoveryCandidate] = []
    for (domain, source_id, slot_key), entries in sorted(groups.items()):
        candidates.append(
            _candidate_for_group(
                domain=domain,
                source_id=source_id,
                slot_key=slot_key,
                entries=entries,
                now=normalized_now,
                active_run_ids=active_run_ids or {},
            )
        )
    return tuple(candidates)


def _candidate_for_group(
    *,
    domain: str,
    source_id: str,
    slot_key: str,
    entries: Sequence[tuple[ExpectedSlot, Mapping[str, object] | None]],
    now: datetime,
    active_run_ids: Mapping[str, str],
) -> RecoveryCandidate:
    if not entries:
        raise RecoveryCandidateEvidenceError("recovery group must not be empty")
    first_slot = entries[0][0]
    for slot, _event in entries[1:]:
        if (
            slot.deadline_at != first_slot.deadline_at
            or slot.recovery_boundary != first_slot.recovery_boundary
        ):
            raise RecoveryCandidateEvidenceError(
                "one issue cycle contains inconsistent deadline or recovery boundary"
            )

    slot_ids = tuple(sorted(slot.expected_slot_id for slot, _event in entries))
    latest_events = [event for _slot, event in entries]
    raw_manifest_keys = {
        str(event.get("raw_manifest_key"))
        for event in latest_events
        if event is not None
        and event.get("recovery_state") == "pending"
        and event.get("recovery_class") == "raw_replay"
        and isinstance(event.get("raw_manifest_key"), str)
        and str(event.get("raw_manifest_key")).strip()
    }
    raw_manifest_verified = len(raw_manifest_keys) == 1 and all(
        event is not None
        and event.get("recovery_state") == "pending"
        and event.get("recovery_class") == "raw_replay"
        and isinstance(event.get("raw_manifest_key"), str)
        and str(event.get("raw_manifest_key")).strip()
        for event in latest_events
    )
    historical_query_allowed = all(
        event is not None
        and event.get("recovery_state") == "pending"
        and event.get("recovery_class") == "historical_query"
        and event.get("recovery_evidence_code")
        for event in latest_events
    )
    latest_failure = max(
        (event for event in latest_events if event is not None),
        key=lambda event: _aware_utc(event.get("event_at"), field="event_at"),
        default=None,
    )
    failure_code = (
        str(latest_failure.get("gap_reason_code"))
        if latest_failure is not None and latest_failure.get("gap_reason_code")
        else None
    )
    normal_run_id = active_run_ids.get(domain)
    return RecoveryCandidate(
        domain=domain,
        source_id=source_id,
        slot_key=slot_key,
        slot_ids=slot_ids,
        scheduled_at=_parse_timestamp(first_slot.scheduled_at, "scheduled_at"),
        deadline_at=_parse_timestamp(first_slot.deadline_at, "deadline_at"),
        recovery_boundary=_parse_timestamp(
            first_slot.recovery_boundary,
            "recovery_boundary",
        ),
        expected_count=len(slot_ids),
        covered_count=len(slot_ids) if raw_manifest_verified else 0,
        raw_manifest_verified=raw_manifest_verified,
        historical_query_allowed=historical_query_allowed,
        normal_run_active=normal_run_id is not None,
        normal_run_id=normal_run_id,
        last_failure_code=failure_code,
    )


def _expected_slots(documents: Iterable[Mapping[str, object]]) -> dict[str, ExpectedSlot]:
    slots: dict[str, ExpectedSlot] = {}
    for document in documents:
        slot = _expected_slot(document)
        previous = slots.get(slot.expected_slot_id)
        if previous is not None and previous != slot:
            raise RecoveryCandidateEvidenceError("conflicting expected slot evidence")
        slots[slot.expected_slot_id] = slot
    return slots


def _expected_slot(document: Mapping[str, object]) -> ExpectedSlot:
    grain_json = document.get("grain_json")
    if not isinstance(grain_json, str):
        raise RecoveryCandidateEvidenceError("expected receipt grain_json is invalid")
    try:
        grain = json.loads(grain_json)
    except json.JSONDecodeError as exc:
        raise RecoveryCandidateEvidenceError("expected receipt grain_json is invalid") from exc
    if not isinstance(grain, Mapping):
        raise RecoveryCandidateEvidenceError("expected receipt grain_json is invalid")
    try:
        slot = ExpectedSlot.create(
            contract_version=document.get("contract_version"),
            domain=document.get("domain"),
            collection_contract_id=document.get("collection_contract_id"),
            source_id=document.get("source_id"),
            collection_slot_at=document.get("collection_slot_at"),
            scheduled_at=document.get("scheduled_at"),
            deadline_at=document.get("deadline_at"),
            grain=grain,
            schedule_version=document.get("schedule_version"),
            is_scheduled=document.get("is_scheduled"),
            recovery_boundary_type=document.get("recovery_boundary_type"),
            recovery_boundary=document.get("recovery_boundary"),
            declared_at=document.get("declared_at"),
            declared_by=document.get("declared_by"),
        )
    except (TypeError, ValueError) as exc:
        raise RecoveryCandidateEvidenceError("expected receipt contract is invalid") from exc
    if slot.expected_slot_id != document.get("expected_slot_id"):
        raise RecoveryCandidateEvidenceError("expected slot identity mismatch")
    if slot.domain != "weather":
        raise RecoveryCandidateEvidenceError("recovery coordinator only accepts Weather slots")
    return slot


def _has_terminal_event(events: Iterable[Mapping[str, object]]) -> bool:
    return any(
        event.get("collection_state") in TERMINAL_COLLECTION_STATES
        or event.get("recovery_state") == "recovered"
        for event in events
    )


def _latest_event(
    events: Sequence[Mapping[str, object]],
) -> Mapping[str, object] | None:
    if not events:
        return None
    return max(
        events,
        key=lambda event: _aware_utc(event.get("event_at"), field="event_at"),
    )


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RecoveryCandidateEvidenceError(f"{field} is invalid")
    return value


def _parse_timestamp(value: object, field: str) -> datetime:
    return _aware_utc(value, field=field)


def _aware_utc(value: object, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise RecoveryCandidateEvidenceError(f"{field} is invalid") from exc
    else:
        raise RecoveryCandidateEvidenceError(f"{field} is invalid")
    if parsed.tzinfo is None:
        raise RecoveryCandidateEvidenceError(f"{field} must include timezone")
    return parsed.astimezone(timezone.utc)


__all__ = [
    "RecoveryCandidateEvidenceError",
    "candidates_from_receipts",
    "read_weather_recovery_candidates",
]
