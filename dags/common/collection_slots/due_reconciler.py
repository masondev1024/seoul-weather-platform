"""Declare due collection slots and finalize missed slots without source I/O."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from common.collection_slots.activation import is_slot_active
from common.collection_slots.contract import CollectionOutcome, ExpectedSlot
from common.collection_slots.materializer import (
    EVENT_RECEIPTS_PREFIX,
    EXPECTED_RECEIPTS_PREFIX,
    MaterializationError,
    _document,
    _expected,
    _event,
)
from common.collection_slots.receipts import CollectionSlotReceipts


class DueSlotStorage(Protocol):
    def list_keys(self, prefix: str) -> list[str]: ...

    def read_json(self, key: str) -> object: ...

    def write_json_if_absent(self, key: str, value: object) -> bool: ...


class DueSlotReconciliationError(ValueError):
    """Due-slot reconciliation encountered invalid receipt or policy evidence."""


OutcomeFactory = Callable[[ExpectedSlot], CollectionOutcome]


@dataclass(frozen=True)
class DueSlotReconciliationResult:
    declared: int
    not_due: int
    already_terminal: int
    finalized: int

    def as_dict(self) -> dict[str, int]:
        return {
            "declared": self.declared,
            "not_due": self.not_due,
            "already_terminal": self.already_terminal,
            "finalized": self.finalized,
        }


class DueSlotReconciler:
    """Reconcile exact expected slots using only immutable R2 receipts."""

    def __init__(
        self,
        *,
        storage: DueSlotStorage,
        receipts: CollectionSlotReceipts,
        clock: Callable[[], datetime],
        outcome_factory: OutcomeFactory,
    ) -> None:
        self._storage = storage
        self._clock = clock
        self._outcome_factory = outcome_factory
        self._receipts = receipts

    def run(
        self,
        candidates: Iterable[ExpectedSlot],
        *,
        activation_at: datetime | None,
    ) -> DueSlotReconciliationResult:
        declared = 0
        not_due = 0
        already_terminal = 0
        finalized = 0
        now = _aware_utc(self._clock(), field="clock")

        for slot in candidates:
            if not is_slot_active(slot.collection_slot_at, activation_at):
                continue

            self._receipts.record_expected(slot)
            declared += 1

            if _aware_utc(slot.deadline_at, field="deadline_at") > now:
                not_due += 1
                continue

            if self._has_terminal_event(slot.expected_slot_id):
                already_terminal += 1
                continue

            outcome = self._outcome_factory(slot)
            self._validate_missing_outcome(outcome, slot)
            self._receipts.record_outcome(outcome)
            finalized += 1

        return DueSlotReconciliationResult(
            declared=declared,
            not_due=not_due,
            already_terminal=already_terminal,
            finalized=finalized,
        )

    def existing_slots(
        self,
        *,
        domain: str,
        source_id: str,
    ) -> tuple[ExpectedSlot, ...]:
        """Return validated already-declared slots for one domain/source scope."""
        if not domain or not source_id:
            raise DueSlotReconciliationError("domain and source_id are required")
        by_id: dict[str, ExpectedSlot] = {}
        for key in self._storage.list_keys(EXPECTED_RECEIPTS_PREFIX):
            if not key.endswith(".json"):
                continue
            try:
                slot = _expected_slot(
                    self._storage.read_json(key),
                    key=key,
                )
            except MaterializationError as exc:
                raise DueSlotReconciliationError(str(exc)) from exc
            if slot.domain != domain or slot.source_id != source_id:
                continue
            previous = by_id.get(slot.expected_slot_id)
            if previous is not None and previous != slot:
                raise DueSlotReconciliationError(
                    f"expected receipt conflict: {slot.expected_slot_id}"
                )
            by_id[slot.expected_slot_id] = slot
        return tuple(
            sorted(
                by_id.values(),
                key=lambda slot: (slot.collection_slot_at, slot.expected_slot_id),
            )
        )

    def _has_terminal_event(self, expected_slot_id: str) -> bool:
        prefix = f"{EVENT_RECEIPTS_PREFIX}expected_slot_id={expected_slot_id}/"
        has_terminal_event = False
        for key in self._storage.list_keys(prefix):
            if not key.endswith(".json"):
                continue
            try:
                event = _event(_document(self._storage.read_json(key), key=key), key=key)
            except MaterializationError as exc:
                raise DueSlotReconciliationError(str(exc)) from exc
            if str(event["expected_slot_id"]) != expected_slot_id:
                raise DueSlotReconciliationError(
                    f"event receipt expected_slot_id mismatch: {key}"
                )
            has_terminal_event = True
        return has_terminal_event

    @staticmethod
    def _validate_missing_outcome(
        outcome: CollectionOutcome,
        slot: ExpectedSlot,
    ) -> None:
        if not isinstance(outcome, CollectionOutcome):
            raise DueSlotReconciliationError("outcome_factory must return CollectionOutcome")
        if outcome.expected_slot_id != slot.expected_slot_id:
            raise DueSlotReconciliationError(
                "outcome_factory returned different expected_slot_id"
            )
        if outcome.collection_state != "missing_unknown":
            raise DueSlotReconciliationError(
                "outcome_factory must return collection_state=missing_unknown"
            )
        if (
            outcome.recovery_state in {"pending", "recovered"}
            and outcome.recovery_evidence_code is None
        ):
            raise DueSlotReconciliationError(
                "outcome_factory must return recovery_evidence_code for pending or recovered recovery"
            )
        if (
            outcome.recovery_state in {"pending", "recovered"}
            and outcome.recovery_class == "raw_replay"
            and outcome.recovery_evidence_code != "raw_manifest_verified"
        ):
            raise DueSlotReconciliationError(
                "raw_replay recovery must use recovery_evidence_code=raw_manifest_verified"
            )


def _aware_utc(raw: datetime | str, *, field: str) -> datetime:
    if isinstance(raw, datetime):
        parsed = raw
    elif isinstance(raw, str):
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise DueSlotReconciliationError(f"{field} must be an ISO timestamp") from exc
    else:
        raise DueSlotReconciliationError(f"{field} must be a datetime or ISO timestamp")
    if parsed.tzinfo is None:
        raise DueSlotReconciliationError(f"{field} must include timezone")
    return parsed.astimezone(timezone.utc)


def _expected_slot(value: object, *, key: str) -> ExpectedSlot:
    normalized = _expected(_document(value, key=key), key=key)
    try:
        grain = json.loads(str(normalized["grain_json"]))
        if not isinstance(grain, dict):
            raise TypeError("grain_json must be an object")
        return ExpectedSlot.create(
            contract_version=normalized["contract_version"],
            domain=normalized["domain"],
            collection_contract_id=normalized["collection_contract_id"],
            source_id=normalized["source_id"],
            collection_slot_at=normalized["collection_slot_at"],
            scheduled_at=normalized["scheduled_at"],
            deadline_at=normalized["deadline_at"],
            grain=grain,
            schedule_version=normalized["schedule_version"],
            is_scheduled=normalized["is_scheduled"],
            recovery_boundary_type=normalized["recovery_boundary_type"],
            recovery_boundary=normalized["recovery_boundary"],
            declared_at=normalized["declared_at"],
            declared_by=normalized["declared_by"],
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise MaterializationError(
            f"expected receipt contract is invalid: {key}"
        ) from exc


__all__ = [
    "DueSlotReconciliationError",
    "DueSlotReconciliationResult",
    "DueSlotReconciler",
    "OutcomeFactory",
]
