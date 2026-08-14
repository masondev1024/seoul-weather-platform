"""Write-once R2 receipts for expected collection slots and outcome events."""

from __future__ import annotations

from typing import Protocol

from common.collection_slots.contract import (
    CollectionOutcome,
    ExpectedSlot,
    canonical_json,
)
from common.ops.contract import ControlSubtype, OpsCategory, ops_key


RECEIPT_DOMAIN = "collection_slots"
RECEIPT_VERSION = "v1"


class ConditionalJsonStorage(Protocol):
    def write_json_if_absent(self, key: str, value: object) -> bool: ...

    def read_json(self, key: str) -> object: ...


class ReceiptConflictError(ValueError):
    """A deterministic receipt key already contains different evidence."""


class CollectionSlotReceipts:
    """Persist idempotent expected and append-only outcome receipt documents."""

    def __init__(self, storage: ConditionalJsonStorage) -> None:
        self._storage = storage

    def expected_key(self, slot: ExpectedSlot) -> str:
        return ops_key(
            OpsCategory.CONTROL,
            control=ControlSubtype.STATE,
            domain=RECEIPT_DOMAIN,
            subpath=(
                RECEIPT_VERSION,
                "expected",
                f"domain={slot.domain}",
                f"source_id={slot.source_id}",
                f"slot_date={slot.collection_slot_at[:10]}",
            ),
            filename=f"{slot.expected_slot_id}.json",
        )

    def outcome_key(self, outcome: CollectionOutcome) -> str:
        return ops_key(
            OpsCategory.CONTROL,
            control=ControlSubtype.STATE,
            domain=RECEIPT_DOMAIN,
            subpath=(
                RECEIPT_VERSION,
                "events",
                f"expected_slot_id={outcome.expected_slot_id}",
            ),
            filename=f"{outcome.event_id}.json",
        )

    def record_expected(self, slot: ExpectedSlot) -> str:
        key = self.expected_key(slot)
        self._write_idempotently(
            key,
            slot.to_document(),
            identity_field="expected_slot_id",
        )
        return key

    def record_outcome(self, outcome: CollectionOutcome) -> str:
        key = self.outcome_key(outcome)
        self._write_idempotently(
            key,
            outcome.to_document(),
            identity_field="event_id",
        )
        return key

    def _write_idempotently(
        self,
        key: str,
        document: dict[str, object],
        *,
        identity_field: str,
    ) -> None:
        if self._storage.write_json_if_absent(key, document):
            return
        existing = self._storage.read_json(key)
        if canonical_json(existing) != canonical_json(document):
            raise ReceiptConflictError(f"{identity_field} conflict: {key}")


__all__ = [
    "CollectionSlotReceipts",
    "RECEIPT_DOMAIN",
    "RECEIPT_VERSION",
    "ReceiptConflictError",
]
