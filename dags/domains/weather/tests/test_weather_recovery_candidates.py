from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from common.collection_slots.contract import CollectionOutcome, ExpectedSlot
from common.collection_slots.materializer import ReceiptBatch
from weather_recovery_candidates import (
    RecoveryCandidateEvidenceError,
    candidates_from_receipts,
)


UTC = timezone.utc
NOW = datetime(2026, 8, 26, 6, 0, tzinfo=UTC)


def _slot(index: int = 0, *, boundary: str = "2026-08-19T00:00:00+00:00") -> ExpectedSlot:
    issue = datetime(2026, 8, 26, 4, 20, tzinfo=UTC)
    return ExpectedSlot.create(
        contract_version="v1",
        domain="weather",
        collection_contract_id="weather.vilage_fcst.v1",
        source_id="kma_vilage_fcst",
        collection_slot_at=issue,
        scheduled_at=issue,
        deadline_at=issue + timedelta(hours=1),
        grain={"place_id": f"place-{index}", "nx": 60 + index, "ny": 127},
        schedule_version="weather.vilage_fcst.issue_cycle.v1",
        is_scheduled=True,
        recovery_boundary_type="weather_apihub_historical_earliest_issued_at",
        recovery_boundary=boundary,
        declared_at=issue,
        declared_by="weather_vilage_fcst_bronze",
    )


def _event(
    slot: ExpectedSlot,
    *,
    recovery_class: str,
    recovery_state: str = "pending",
    raw_manifest_key: str | None = None,
    event_at: datetime = NOW - timedelta(minutes=5),
    gap_reason_code: str = "collection_failed",
) -> dict[str, object]:
    return CollectionOutcome.create(
        expected_slot_id=slot.expected_slot_id,
        collection_state="collection_failed",
        recovery_state=recovery_state,
        recovery_class=recovery_class,
        gap_reason_code=gap_reason_code,
        event_at=event_at,
        dag_id="weather_vilage_fcst_bronze",
        dag_run_id="scheduled__2026-08-26T04:20:00+00:00",
        task_id="verify_weather_collection",
        raw_manifest_key=raw_manifest_key,
        raw_object_count=80 if raw_manifest_key else None,
        recovery_run_id=("recovery__one" if recovery_state == "recovered" else None),
        recovered_at=(event_at if recovery_state == "recovered" else None),
        recovery_evidence_code=(
            "raw_manifest_verified"
            if recovery_class == "raw_replay"
            else "weather_apihub_historical_range"
            if recovery_class == "historical_query"
            else None
        ),
        row_count=1 if recovery_state == "recovered" else None,
        source_result_code="00" if recovery_state == "recovered" else None,
    ).to_document()


def test_complete_raw_manifest_evidence_becomes_one_issue_cycle_candidate() -> None:
    slots = [_slot(0), _slot(1)]
    batch = ReceiptBatch(
        expected=tuple(slot.to_document() for slot in slots),
        events=tuple(
            _event(slot, recovery_class="raw_replay", raw_manifest_key="raw/manifest.json")
            for slot in slots
        ),
    )

    candidates = candidates_from_receipts(batch, now=NOW)

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.slot_key == "2026-08-26T04:20:00+00:00"
    assert candidate.expected_count == 2
    assert candidate.covered_count == 2
    assert candidate.raw_manifest_verified is True
    assert candidate.historical_query_allowed is False


def test_historical_query_evidence_is_allowed_without_raw_coverage() -> None:
    slot = _slot()
    batch = ReceiptBatch(
        expected=(slot.to_document(),),
        events=(_event(slot, recovery_class="historical_query", raw_manifest_key=None),),
    )

    candidate = candidates_from_receipts(batch, now=NOW)[0]

    assert candidate.raw_manifest_verified is False
    assert candidate.covered_count == 0
    assert candidate.historical_query_allowed is True


def test_terminal_or_recovered_evidence_is_not_a_recovery_candidate() -> None:
    slot = _slot()
    batch = ReceiptBatch(
        expected=(slot.to_document(),),
        events=(_event(slot, recovery_class="raw_replay", recovery_state="recovered"),),
    )

    assert candidates_from_receipts(batch, now=NOW) == ()


def test_missing_event_is_blocked_by_planner_instead_of_guessed() -> None:
    slot = _slot()
    batch = ReceiptBatch(expected=(slot.to_document(),), events=())

    candidate = candidates_from_receipts(batch, now=NOW)[0]

    assert candidate.raw_manifest_verified is False
    assert candidate.historical_query_allowed is False


def test_inconsistent_group_boundaries_fail_closed() -> None:
    first = _slot(0)
    second = _slot(1, boundary="2026-08-20T00:00:00+00:00")
    batch = ReceiptBatch(
        expected=(first.to_document(), second.to_document()),
        events=tuple(
            _event(slot, recovery_class="historical_query")
            for slot in (first, second)
        ),
    )

    with pytest.raises(RecoveryCandidateEvidenceError):
        candidates_from_receipts(batch, now=NOW)
