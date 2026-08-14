"""Validate collection-slot receipts before writing them to Iceberg.

The collector writes immutable JSON receipts to R2.  This module is deliberately
storage/sink agnostic so the receipt contract can be tested without a warehouse
and the batch DAG can fail closed before opening an Iceberg transaction.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from common.collection_slots.contract import (
    CollectionOutcome,
    ExpectedSlot,
    canonical_json,
)
from common.ops.contract import ControlSubtype, OpsCategory, category_prefix


RECEIPT_DOMAIN = "collection_slots"
RECEIPT_VERSION = "v1"
_RECEIPT_ROOT = category_prefix(
    OpsCategory.CONTROL,
    control=ControlSubtype.STATE,
    domain=RECEIPT_DOMAIN,
)
EXPECTED_RECEIPTS_PREFIX = f"{_RECEIPT_ROOT}{RECEIPT_VERSION}/expected/"
EVENT_RECEIPTS_PREFIX = f"{_RECEIPT_ROOT}{RECEIPT_VERSION}/events/"


class ReceiptStorage(Protocol):
    def list_keys(self, prefix: str) -> list[str]: ...

    def read_json(self, key: str) -> object: ...


class CollectionSlotSink(Protocol):
    def write_expected(self, rows: list[dict[str, object]]) -> int: ...

    def write_events(self, rows: list[dict[str, object]]) -> int: ...


class MaterializationError(ValueError):
    """Receipt input is invalid or internally inconsistent."""


@dataclass(frozen=True)
class ReceiptBatch:
    expected: tuple[dict[str, object], ...]
    events: tuple[dict[str, object], ...]


def _document(value: object, *, key: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise MaterializationError(f"receipt document must be an object: {key}")
    try:
        return dict(value)
    except (TypeError, ValueError) as exc:
        raise MaterializationError(f"receipt document is malformed: {key}") from exc


def _required(document: Mapping[str, object], fields: tuple[str, ...], *, key: str) -> None:
    missing = [field for field in fields if field not in document]
    if missing:
        raise MaterializationError(
            f"receipt missing fields ({', '.join(missing)}): {key}"
        )


def _expected(document: Mapping[str, object], *, key: str) -> dict[str, object]:
    _required(
        document,
        (
            "contract_version",
            "expected_slot_id",
            "domain",
            "collection_contract_id",
            "source_id",
            "collection_slot_at",
            "grain_key",
            "grain_json",
            "schedule_version",
            "scheduled_at",
            "deadline_at",
            "is_scheduled",
            "recovery_boundary_type",
            "recovery_boundary",
            "declared_at",
            "declared_by",
        ),
        key=key,
    )
    grain_json = document["grain_json"]
    if not isinstance(grain_json, str):
        raise MaterializationError(f"grain_json must be a string: {key}")
    try:
        grain = json.loads(grain_json)
    except json.JSONDecodeError as exc:
        raise MaterializationError(f"grain_json is invalid JSON: {key}") from exc
    if not isinstance(grain, Mapping):
        raise MaterializationError(f"grain_json must contain an object: {key}")

    try:
        slot = ExpectedSlot.create(
            contract_version=document["contract_version"],
            domain=document["domain"],
            collection_contract_id=document["collection_contract_id"],
            source_id=document["source_id"],
            collection_slot_at=document["collection_slot_at"],
            scheduled_at=document["scheduled_at"],
            deadline_at=document["deadline_at"],
            grain=grain,
            schedule_version=document["schedule_version"],
            is_scheduled=document["is_scheduled"],
            recovery_boundary_type=document["recovery_boundary_type"],
            recovery_boundary=document["recovery_boundary"],
            declared_at=document["declared_at"],
            declared_by=document["declared_by"],
        )
    except (TypeError, ValueError) as exc:
        raise MaterializationError(f"expected receipt contract is invalid: {key}") from exc
    if slot.expected_slot_id != document["expected_slot_id"]:
        raise MaterializationError(f"expected_slot_id identity mismatch: {key}")
    if slot.grain_key != document["grain_key"]:
        raise MaterializationError(f"grain_key identity mismatch: {key}")
    return slot.to_document()


def _event(document: Mapping[str, object], *, key: str) -> dict[str, object]:
    _required(
        document,
        (
            "event_id",
            "expected_slot_id",
            "event_type",
            "collection_state",
            "recovery_state",
            "recovery_class",
            "gap_reason_code",
            "dag_id",
            "dag_run_id",
            "task_id",
            "raw_manifest_key",
            "raw_object_count",
            "row_count",
            "source_result_code",
            "recovery_run_id",
            "recovered_at",
            "event_at",
        ),
        key=key,
    )
    try:
        outcome = CollectionOutcome.create(
            expected_slot_id=document["expected_slot_id"],
            event_type=document["event_type"],
            collection_state=document["collection_state"],
            recovery_state=document["recovery_state"],
            recovery_class=document["recovery_class"],
            gap_reason_code=document["gap_reason_code"],
            dag_id=document["dag_id"],
            dag_run_id=document["dag_run_id"],
            task_id=document["task_id"],
            raw_manifest_key=document["raw_manifest_key"],
            raw_object_count=document["raw_object_count"],
            row_count=document["row_count"],
            source_result_code=document["source_result_code"],
            recovery_run_id=document["recovery_run_id"],
            recovered_at=document["recovered_at"],
            recovery_evidence_code=document.get("recovery_evidence_code"),
            event_at=document["event_at"],
        )
    except (TypeError, ValueError) as exc:
        raise MaterializationError(f"event receipt contract is invalid: {key}") from exc
    if outcome.event_id != document["event_id"]:
        raise MaterializationError(f"event_id identity mismatch: {key}")
    return outcome.to_document()


def _deduplicate(
    documents: list[dict[str, object]],
    *,
    identity_field: str,
) -> list[dict[str, object]]:
    by_identity: dict[str, dict[str, object]] = {}
    for document in documents:
        identity = str(document[identity_field])
        previous = by_identity.get(identity)
        if previous is not None and canonical_json(previous) != canonical_json(document):
            raise MaterializationError(f"{identity_field} conflicting duplicate: {identity}")
        by_identity[identity] = document
    return [by_identity[key] for key in sorted(by_identity)]


class CollectionSlotMaterializer:
    """Read, validate, and hand off one R2 receipt batch to an idempotent sink."""

    def __init__(self, storage: ReceiptStorage, sink: CollectionSlotSink) -> None:
        self._storage = storage
        self._sink = sink

    def read_batch(self) -> ReceiptBatch:
        expected = _deduplicate(
            [
                _expected(self._document(self._storage.read_json(key), key=key), key=key)
                for key in self._storage.list_keys(EXPECTED_RECEIPTS_PREFIX)
                if key.endswith(".json")
            ],
            identity_field="expected_slot_id",
        )
        expected_ids = {str(row["expected_slot_id"]) for row in expected}
        events = _deduplicate(
            [
                _event(self._document(self._storage.read_json(key), key=key), key=key)
                for key in self._storage.list_keys(EVENT_RECEIPTS_PREFIX)
                if key.endswith(".json")
            ],
            identity_field="event_id",
        )
        unknown = sorted(
            {
                str(row["expected_slot_id"])
                for row in events
                if str(row["expected_slot_id"]) not in expected_ids
            }
        )
        if unknown:
            raise MaterializationError(
                "event references unknown expected_slot_id: " + ", ".join(unknown)
            )
        return ReceiptBatch(tuple(expected), tuple(events))

    def run(self) -> dict[str, int]:
        batch = self.read_batch()
        expected_count = self._sink.write_expected(list(batch.expected))
        event_count = self._sink.write_events(list(batch.events))
        return {"expected": int(expected_count), "events": int(event_count)}

    @staticmethod
    def _document(value: object, *, key: str) -> dict[str, object]:
        return _document(value, key=key)


__all__ = [
    "CollectionSlotMaterializer",
    "EVENT_RECEIPTS_PREFIX",
    "EXPECTED_RECEIPTS_PREFIX",
    "MaterializationError",
    "ReceiptBatch",
]
