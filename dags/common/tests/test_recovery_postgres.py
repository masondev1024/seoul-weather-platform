from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from common.recovery.lease import LeaseDecision, LeaseLostError, RecoveryLeaseRegistry
from common.recovery.planner import RecoveryAction, RecoveryJob
from common.recovery.postgres import LEASE_TABLE, PostgresLeaseBackend


UTC = timezone.utc
NOW = datetime(2026, 8, 26, 6, 0, tzinfo=UTC)


def _job() -> RecoveryJob:
    return RecoveryJob(
        job_key="weather-recovery/v1/postgres-001",
        action=RecoveryAction.RAW_REPLAY,
        domain="weather",
        source_id="kma_vilage_fcst",
        slot_key="2026-08-26T04:20:00+00:00",
        slot_ids=("slot-a", "slot-b"),
        priority=100,
        api_cost=0,
        reason_code="raw_manifest_verified",
        scheduled_at="2026-08-26T04:20:00+00:00",
        deadline_at="2026-08-26T05:20:00+00:00",
    )


def _backend() -> PostgresLeaseBackend:
    engine = create_engine(
        "sqlite+pysqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    backend = PostgresLeaseBackend(engine)
    backend.ensure_schema()
    return backend


def test_schema_creation_is_explicit_and_lease_operations_are_atomic() -> None:
    engine = create_engine(
        "sqlite+pysqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    backend = PostgresLeaseBackend(engine)

    # Constructing the adapter must not mutate Airflow metadata.  A migration
    # or an explicitly approved maintenance step owns ensure_schema().
    with engine.connect() as connection:
        tables_before = connection.execute(
            text("SELECT name FROM sqlite_master WHERE type = 'table' AND name = :name"),
            {"name": LEASE_TABLE},
        ).first()
    assert tables_before is None

    backend.ensure_schema()
    with engine.connect() as connection:
        tables_after = connection.execute(
            text("SELECT name FROM sqlite_master WHERE type = 'table' AND name = :name"),
            {"name": LEASE_TABLE},
        ).first()
    assert tables_after is not None

    registry = RecoveryLeaseRegistry(backend, lease_ttl=timedelta(minutes=15))
    job = _job()
    first = registry.claim(job, plan_id="plan-001", owner_id="coordinator-a", now=NOW)
    again = registry.claim(
        job,
        plan_id="plan-001",
        owner_id="coordinator-a",
        now=NOW + timedelta(minutes=1),
    )
    held = registry.claim(
        job,
        plan_id="plan-001",
        owner_id="coordinator-b",
        now=NOW + timedelta(minutes=1),
    )

    assert first.decision is LeaseDecision.ACQUIRED
    assert again.decision is LeaseDecision.ALREADY_OWNED
    assert held.decision is LeaseDecision.HELD
    assert backend.read(job.job_key)["evidence"] == {
        "action": "raw_replay",
        "reason_code": "raw_manifest_verified",
    }


def test_expired_lease_takeover_fences_old_owner_and_terminal_is_durable() -> None:
    backend = _backend()
    registry = RecoveryLeaseRegistry(backend, lease_ttl=timedelta(minutes=15))
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
    with pytest.raises(LeaseLostError):
        registry.mark_running(
            first.lease,
            owner_id="coordinator-a",
            now=NOW + timedelta(minutes=16),
        )
    assert registry.mark_running(
        takeover.lease,
        owner_id="coordinator-b",
        now=NOW + timedelta(minutes=16, seconds=1),
    ).state == "running"

    completed = registry.mark_terminal(
        takeover.lease,
        owner_id="coordinator-b",
        state="succeeded",
        now=NOW + timedelta(minutes=17),
        evidence={"bronze_run_id_present": True},
    )
    assert completed.state == "succeeded"
    assert registry.claim(
        job,
        plan_id="plan-003",
        owner_id="coordinator-c",
        now=NOW + timedelta(hours=1),
    ).decision is LeaseDecision.TERMINAL

    # The old lease id cannot overwrite the takeover, even if its owner tries
    # after the database row has changed.
    assert first.lease.lease_id != takeover.lease.lease_id
    row = backend.read(job.job_key)
    assert row["state"] == "succeeded"
    assert row["lease_id"] == takeover.lease.lease_id
    assert row["attempt_count"] == 2


def test_schema_migration_is_idempotent() -> None:
    backend = _backend()
    backend.ensure_schema()
    backend.ensure_schema()
    assert backend.read(_job().job_key) is None
