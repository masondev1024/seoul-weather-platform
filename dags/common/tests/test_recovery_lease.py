from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from common.recovery.lease import (
    InMemoryLeaseBackend,
    LeaseDecision,
    LeaseLostError,
    RecoveryLeaseError,
    RecoveryLeaseRegistry,
)
from common.recovery.planner import RecoveryAction, RecoveryJob


UTC = timezone.utc
NOW = datetime(2026, 8, 26, 6, 0, tzinfo=UTC)


def _job(action: RecoveryAction = RecoveryAction.RAW_REPLAY) -> RecoveryJob:
    return RecoveryJob(
        job_key="weather-recovery/v1/job-001",
        action=action,
        domain="weather",
        source_id="kma_vilage_fcst",
        slot_key="2026-08-26T04:20:00+00:00",
        slot_ids=("slot-a", "slot-b"),
        priority=100,
        api_cost=0 if action is RecoveryAction.RAW_REPLAY else 1,
        reason_code="raw_manifest_verified" if action is RecoveryAction.RAW_REPLAY else "historical_query_allowed",
        scheduled_at="2026-08-26T04:20:00+00:00",
        deadline_at="2026-08-26T05:20:00+00:00",
    )


def test_claim_is_idempotent_for_same_owner_and_held_for_other_owner() -> None:
    registry = RecoveryLeaseRegistry(InMemoryLeaseBackend())
    job = _job()

    first = registry.claim(job, plan_id="plan-001", owner_id="coordinator-a", now=NOW)
    again = registry.claim(job, plan_id="plan-001", owner_id="coordinator-a", now=NOW + timedelta(minutes=1))
    other = registry.claim(job, plan_id="plan-001", owner_id="coordinator-b", now=NOW + timedelta(minutes=1))

    assert first.decision is LeaseDecision.ACQUIRED
    assert again.decision is LeaseDecision.ALREADY_OWNED
    assert again.lease.lease_id == first.lease.lease_id
    assert other.decision is LeaseDecision.HELD


def test_expired_lease_can_be_taken_over_and_old_owner_is_fenced() -> None:
    registry = RecoveryLeaseRegistry(
        InMemoryLeaseBackend(),
        lease_ttl=timedelta(minutes=15),
    )
    job = _job()
    first = registry.claim(job, plan_id="plan-001", owner_id="coordinator-a", now=NOW)
    takeover = registry.claim(
        job,
        plan_id="plan-002",
        owner_id="coordinator-b",
        now=NOW + timedelta(minutes=16),
    )

    assert takeover.decision is LeaseDecision.TAKEOVER
    assert takeover.lease.attempt_count == 2
    assert takeover.lease.lease_id != first.lease.lease_id

    with pytest.raises(LeaseLostError):
        registry.mark_running(first.lease, owner_id="coordinator-a", now=NOW + timedelta(minutes=16))

    running = registry.mark_running(
        takeover.lease,
        owner_id="coordinator-b",
        now=NOW + timedelta(minutes=16, seconds=1),
    )
    assert running.state == "running"


def test_terminal_lease_is_not_reclaimed() -> None:
    registry = RecoveryLeaseRegistry(InMemoryLeaseBackend())
    job = _job()
    claimed = registry.claim(job, plan_id="plan-001", owner_id="coordinator-a", now=NOW)
    completed = registry.mark_terminal(
        claimed.lease,
        owner_id="coordinator-a",
        state="succeeded",
        now=NOW + timedelta(minutes=2),
        evidence={"bronze_run_id_present": True},
    )
    again = registry.claim(
        job,
        plan_id="plan-002",
        owner_id="coordinator-b",
        now=NOW + timedelta(hours=1),
    )

    assert completed.state == "succeeded"
    assert again.decision is LeaseDecision.TERMINAL
    assert again.lease.lease_id == claimed.lease.lease_id


def test_renew_extends_only_an_active_owned_lease() -> None:
    registry = RecoveryLeaseRegistry(
        InMemoryLeaseBackend(),
        lease_ttl=timedelta(minutes=10),
    )
    job = _job()
    claimed = registry.claim(job, plan_id="plan-001", owner_id="coordinator-a", now=NOW)
    renewed = registry.renew(
        claimed.lease,
        owner_id="coordinator-a",
        now=NOW + timedelta(minutes=5),
    )

    assert renewed.expires_at == NOW + timedelta(minutes=15)
    with pytest.raises(LeaseLostError):
        registry.renew(
            renewed,
            owner_id="coordinator-b",
            now=NOW + timedelta(minutes=6),
        )


def test_non_action_jobs_never_create_a_lease() -> None:
    registry = RecoveryLeaseRegistry(InMemoryLeaseBackend())
    for action in (RecoveryAction.BLOCKED, RecoveryAction.DEFERRED):
        with pytest.raises(RecoveryLeaseError):
            registry.claim(
                _job(action),
                plan_id="plan-001",
                owner_id="coordinator-a",
                now=NOW,
            )


def test_malformed_backend_document_fails_closed() -> None:
    class Backend(InMemoryLeaseBackend):
        def read(self, job_key: str):
            return {"schema_version": "wrong", "job_key": job_key}

    registry = RecoveryLeaseRegistry(Backend())
    with pytest.raises(RecoveryLeaseError, match="unsupported lease schema"):
        registry.claim(_job(), plan_id="plan-001", owner_id="coordinator-a", now=NOW)


def test_lease_serialization_contains_no_raw_action_input() -> None:
    registry = RecoveryLeaseRegistry(InMemoryLeaseBackend())
    result = registry.claim(_job(), plan_id="plan-001", owner_id="coordinator-a", now=NOW)

    payload = result.to_dict()
    assert payload["lease"]["schema_version"] == "weather-recovery-lease/v1"
    assert "raw_object_keys" not in str(payload)
    assert "airflow dags trigger" not in str(payload).lower()


def test_terminal_evidence_rejects_sensitive_fields() -> None:
    registry = RecoveryLeaseRegistry(InMemoryLeaseBackend())
    claimed = registry.claim(_job(), plan_id="plan-001", owner_id="coordinator-a", now=NOW)

    with pytest.raises(RecoveryLeaseError, match="sensitive evidence field"):
        registry.mark_terminal(
            claimed.lease,
            owner_id="coordinator-a",
            state="blocked",
            now=NOW + timedelta(minutes=1),
            evidence={"manifest_key": "raw/private-key"},
        )
