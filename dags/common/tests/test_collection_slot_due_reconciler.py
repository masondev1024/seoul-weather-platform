from __future__ import annotations

from datetime import datetime, timezone

import pytest

from common.collection_slots.contract import CollectionOutcome, ExpectedSlot
from common.collection_slots.due_reconciler import (
    DueSlotReconciliationError,
    DueSlotReconciler,
)
from common.collection_slots.materializer import (
    EVENT_RECEIPTS_PREFIX,
    EXPECTED_RECEIPTS_PREFIX,
)
from common.collection_slots.receipts import CollectionSlotReceipts


class StrictStorage:
    def __init__(self, documents: dict[str, object] | None = None) -> None:
        self.documents = documents or {}
        self.calls: list[str] = []

    def list_keys(self, prefix: str) -> list[str]:
        self.calls.append("list_keys")
        return sorted(key for key in self.documents if key.startswith(prefix))

    def read_json(self, key: str) -> object:
        self.calls.append("read_json")
        return self.documents[key]

    def write_json_if_absent(self, key: str, value: object) -> bool:
        self.calls.append("write_json_if_absent")
        if key in self.documents:
            return False
        self.documents[key] = value
        return True


def _slot(slot_at: str = "2026-08-08T00:05:00+00:00") -> ExpectedSlot:
    return ExpectedSlot.create(
        contract_version="v1",
        domain="traffic",
        collection_contract_id="traffic.incident.v1",
        source_id="seoul_traffic_incident",
        collection_slot_at=slot_at,
        scheduled_at=slot_at,
        deadline_at="2026-08-08T00:20:00+00:00",
        grain={"source_id": "seoul_traffic_incident"},
        schedule_version="traffic-incident-5m-v1",
        is_scheduled=True,
        recovery_boundary_type="raw_retention",
        recovery_boundary="2026-08-01T00:00:00+00:00",
        declared_at=slot_at,
        declared_by="traffic_incident_landing",
    )


def _missing(slot: ExpectedSlot) -> CollectionOutcome:
    return CollectionOutcome.create(
        expected_slot_id=slot.expected_slot_id,
        collection_state="missing_unknown",
        recovery_state="pending",
        recovery_class="raw_replay",
        gap_reason_code="due_slot_reconciler",
        recovery_evidence_code="raw_manifest_verified",
        event_at="2026-08-08T00:25:00+00:00",
        dag_id="traffic_incident_bronze",
        dag_run_id="manual__2026-08-08T00:25:00+00:00",
        task_id="reconcile_due_collection_slots",
    )


def _observed(slot: ExpectedSlot) -> CollectionOutcome:
    return CollectionOutcome.create(
        expected_slot_id=slot.expected_slot_id,
        collection_state="observed",
        recovery_state="not_required",
        recovery_class="none",
        event_at="2026-08-08T00:08:00+00:00",
        dag_id="traffic_incident_bronze",
        dag_run_id="scheduled__2026-08-08T00:05:00+00:00",
        task_id="materialize_pending_traffic_incident_snapshots",
        raw_manifest_key="raw/traffic/manifest.json",
        raw_object_count=1,
        row_count=1,
        source_result_code="INFO-000",
    )


def test_disabled_and_pre_activation_slots_are_not_mutated_or_counted():
    storage = StrictStorage()
    slot = _slot()
    reconciler = DueSlotReconciler(
        storage=storage,
        receipts=CollectionSlotReceipts(storage),
        clock=lambda: datetime(2026, 8, 8, 0, 25, tzinfo=timezone.utc),
        outcome_factory=lambda _slot: _missing(_slot),
    )

    disabled = reconciler.run([slot], activation_at=None)
    before_activation = reconciler.run(
        [slot],
        activation_at=datetime(2026, 8, 8, 0, 6, tzinfo=timezone.utc),
    )

    assert disabled.as_dict() == {
        "declared": 0,
        "not_due": 0,
        "already_terminal": 0,
        "finalized": 0,
    }
    assert before_activation.as_dict() == disabled.as_dict()
    assert storage.documents == {}
    assert storage.calls == []


def test_active_slot_is_declared_but_not_finalized_before_deadline():
    storage = StrictStorage()
    slot = _slot()
    factory_calls = 0

    def factory(_slot: ExpectedSlot) -> CollectionOutcome:
        nonlocal factory_calls
        factory_calls += 1
        return _missing(_slot)

    result = DueSlotReconciler(
        storage=storage,
        receipts=CollectionSlotReceipts(storage),
        clock=lambda: datetime(2026, 8, 8, 0, 19, 59, tzinfo=timezone.utc),
        outcome_factory=factory,
    ).run([slot], activation_at=datetime(2026, 8, 8, 0, 0, tzinfo=timezone.utc))

    assert result.as_dict() == {
        "declared": 1,
        "not_due": 1,
        "already_terminal": 0,
        "finalized": 0,
    }
    assert factory_calls == 0
    assert list(storage.documents.values()) == [slot.to_document()]


def test_existing_slots_reads_only_matching_valid_expected_receipts():
    traffic_slot = _slot()
    weather_slot = ExpectedSlot.create(
        contract_version="v1",
        domain="weather",
        collection_contract_id="weather.vilage_fcst.v1",
        source_id="kma_vilage_fcst",
        collection_slot_at="2026-08-08T00:00:00+00:00",
        scheduled_at="2026-08-08T00:00:00+00:00",
        deadline_at="2026-08-08T01:00:00+00:00",
        grain={"place_id": "jongno", "nx": 60, "ny": 127},
        schedule_version="weather-v1",
        is_scheduled=True,
        recovery_boundary_type="historical",
        recovery_boundary="2026-08-01T00:00:00+00:00",
        declared_at="2026-08-08T00:00:00+00:00",
        declared_by="weather_vilage_fcst_bronze",
    )
    storage = StrictStorage(
        {
            f"{EXPECTED_RECEIPTS_PREFIX}{traffic_slot.expected_slot_id}.json": traffic_slot.to_document(),
            f"{EXPECTED_RECEIPTS_PREFIX}{weather_slot.expected_slot_id}.json": weather_slot.to_document(),
        }
    )
    reconciler = DueSlotReconciler(
        storage=storage,
        receipts=CollectionSlotReceipts(storage),
        clock=lambda: datetime(2026, 8, 8, 0, 25, tzinfo=timezone.utc),
        outcome_factory=_missing,
    )

    slots = reconciler.existing_slots(
        domain="traffic",
        source_id="seoul_traffic_incident",
    )

    assert slots == (traffic_slot,)
    assert storage.calls == ["list_keys", "read_json", "read_json"]


def test_existing_valid_event_skips_missing_finalization_after_expected_is_declared():
    slot = _slot()
    event = _observed(slot)
    storage = StrictStorage(
        {
            f"{EVENT_RECEIPTS_PREFIX}expected_slot_id={slot.expected_slot_id}/{event.event_id}.json": event.to_document(),
        }
    )

    result = DueSlotReconciler(
        storage=storage,
        receipts=CollectionSlotReceipts(storage),
        clock=lambda: datetime(2026, 8, 8, 0, 25, tzinfo=timezone.utc),
        outcome_factory=lambda _slot: pytest.fail("factory must not be called"),
    ).run([slot], activation_at=datetime(2026, 8, 8, 0, 0, tzinfo=timezone.utc))

    assert result.as_dict() == {
        "declared": 1,
        "not_due": 0,
        "already_terminal": 1,
        "finalized": 0,
    }
    assert slot.to_document() in storage.documents.values()
    assert event.to_document() in storage.documents.values()


def test_after_deadline_writes_exactly_one_policy_selected_missing_unknown_outcome():
    storage = StrictStorage()
    slot = _slot()

    result = DueSlotReconciler(
        storage=storage,
        receipts=CollectionSlotReceipts(storage),
        clock=lambda: datetime(2026, 8, 8, 0, 25, tzinfo=timezone.utc),
        outcome_factory=_missing,
    ).run([slot], activation_at=datetime(2026, 8, 8, 0, 0, tzinfo=timezone.utc))

    receipts = CollectionSlotReceipts(storage)
    assert result.as_dict() == {
        "declared": 1,
        "not_due": 0,
        "already_terminal": 0,
        "finalized": 1,
    }
    assert storage.documents[receipts.expected_key(slot)] == slot.to_document()
    assert storage.documents[receipts.outcome_key(_missing(slot))] == _missing(slot).to_document()
    assert set(storage.calls) <= {"list_keys", "read_json", "write_json_if_absent"}


def test_invalid_or_mismatched_event_receipt_fails_closed():
    slot = _slot()
    event = _observed(slot)
    mismatched = dict(event.to_document(), expected_slot_id="other")
    storage = StrictStorage(
        {
            f"{EVENT_RECEIPTS_PREFIX}expected_slot_id={slot.expected_slot_id}/{event.event_id}.json": mismatched,
        }
    )

    with pytest.raises(DueSlotReconciliationError, match="event"):
        DueSlotReconciler(
            storage=storage,
            receipts=CollectionSlotReceipts(storage),
            clock=lambda: datetime(2026, 8, 8, 0, 25, tzinfo=timezone.utc),
            outcome_factory=_missing,
        ).run([slot], activation_at=datetime(2026, 8, 8, 0, 0, tzinfo=timezone.utc))


def test_event_partition_validates_all_json_receipts_before_skipping_finalization():
    slot = _slot()
    event = _observed(slot)
    storage = StrictStorage(
        {
            f"{EVENT_RECEIPTS_PREFIX}expected_slot_id={slot.expected_slot_id}/a-valid.json": event.to_document(),
            f"{EVENT_RECEIPTS_PREFIX}expected_slot_id={slot.expected_slot_id}/z-invalid.json": {
                "event_id": "not-enough-fields",
                "expected_slot_id": slot.expected_slot_id,
            },
        }
    )

    with pytest.raises(DueSlotReconciliationError, match="event"):
        DueSlotReconciler(
            storage=storage,
            receipts=CollectionSlotReceipts(storage),
            clock=lambda: datetime(2026, 8, 8, 0, 25, tzinfo=timezone.utc),
            outcome_factory=lambda _slot: pytest.fail("factory must not be called"),
        ).run([slot], activation_at=datetime(2026, 8, 8, 0, 0, tzinfo=timezone.utc))


@pytest.mark.parametrize(
    "outcome",
    [
        lambda slot: CollectionOutcome.create(
            expected_slot_id="other",
            collection_state="missing_unknown",
            recovery_state="pending",
            recovery_class="raw_replay",
            gap_reason_code="due_slot_reconciler",
            recovery_evidence_code="raw_manifest_verified",
            event_at="2026-08-08T00:25:00+00:00",
            dag_id="traffic_incident_bronze",
            dag_run_id="manual__2026-08-08T00:25:00+00:00",
        ),
        lambda slot: CollectionOutcome.create(
            expected_slot_id=slot.expected_slot_id,
            collection_state="collection_failed",
            recovery_state="pending",
            recovery_class="raw_replay",
            gap_reason_code="due_slot_reconciler",
            recovery_evidence_code="raw_manifest_verified",
            event_at="2026-08-08T00:25:00+00:00",
            dag_id="traffic_incident_bronze",
            dag_run_id="manual__2026-08-08T00:25:00+00:00",
        ),
    ],
)
def test_policy_outcome_must_match_slot_and_missing_unknown(outcome):
    slot = _slot()
    storage = StrictStorage()

    with pytest.raises(DueSlotReconciliationError):
        DueSlotReconciler(
            storage=storage,
            receipts=CollectionSlotReceipts(storage),
            clock=lambda: datetime(2026, 8, 8, 0, 25, tzinfo=timezone.utc),
            outcome_factory=outcome,
        ).run([slot], activation_at=datetime(2026, 8, 8, 0, 0, tzinfo=timezone.utc))


@pytest.mark.parametrize(
    "outcome",
    [
        lambda slot: CollectionOutcome.create(
            expected_slot_id=slot.expected_slot_id,
            collection_state="missing_unknown",
            recovery_state="pending",
            recovery_class="raw_replay",
            gap_reason_code="due_slot_reconciler",
            event_at="2026-08-08T00:25:00+00:00",
            dag_id="traffic_incident_bronze",
            dag_run_id="manual__2026-08-08T00:25:00+00:00",
        ),
        lambda slot: CollectionOutcome.create(
            expected_slot_id=slot.expected_slot_id,
            collection_state="missing_unknown",
            recovery_state="recovered",
            recovery_class="raw_replay",
            gap_reason_code="due_slot_reconciler",
            recovery_run_id="manual_recovery_1",
            recovered_at="2026-08-08T00:26:00+00:00",
            event_at="2026-08-08T00:25:00+00:00",
            dag_id="traffic_incident_bronze",
            dag_run_id="manual__2026-08-08T00:25:00+00:00",
        ),
    ],
)
def test_pending_or_recovered_missing_outcome_requires_recovery_evidence_code(outcome):
    slot = _slot()
    storage = StrictStorage()

    with pytest.raises(DueSlotReconciliationError, match="recovery_evidence_code"):
        DueSlotReconciler(
            storage=storage,
            receipts=CollectionSlotReceipts(storage),
            clock=lambda: datetime(2026, 8, 8, 0, 25, tzinfo=timezone.utc),
            outcome_factory=outcome,
        ).run([slot], activation_at=datetime(2026, 8, 8, 0, 0, tzinfo=timezone.utc))


@pytest.mark.parametrize(
    "outcome",
    [
        lambda slot: CollectionOutcome.create(
            expected_slot_id=slot.expected_slot_id,
            collection_state="missing_unknown",
            recovery_state="pending",
            recovery_class="raw_replay",
            gap_reason_code="due_slot_reconciler",
            recovery_evidence_code="due_slot_reconciler",
            event_at="2026-08-08T00:25:00+00:00",
            dag_id="traffic_incident_bronze",
            dag_run_id="manual__2026-08-08T00:25:00+00:00",
        ),
        lambda slot: CollectionOutcome.create(
            expected_slot_id=slot.expected_slot_id,
            collection_state="missing_unknown",
            recovery_state="recovered",
            recovery_class="raw_replay",
            gap_reason_code="due_slot_reconciler",
            recovery_evidence_code="due_slot_reconciler",
            recovery_run_id="manual_recovery_1",
            recovered_at="2026-08-08T00:26:00+00:00",
            event_at="2026-08-08T00:25:00+00:00",
            dag_id="traffic_incident_bronze",
            dag_run_id="manual__2026-08-08T00:25:00+00:00",
        ),
    ],
)
def test_raw_replay_missing_outcome_requires_raw_manifest_verified(outcome):
    slot = _slot()
    storage = StrictStorage()

    with pytest.raises(DueSlotReconciliationError, match="raw_manifest_verified"):
        DueSlotReconciler(
            storage=storage,
            receipts=CollectionSlotReceipts(storage),
            clock=lambda: datetime(2026, 8, 8, 0, 25, tzinfo=timezone.utc),
            outcome_factory=outcome,
        ).run([slot], activation_at=datetime(2026, 8, 8, 0, 0, tzinfo=timezone.utc))
