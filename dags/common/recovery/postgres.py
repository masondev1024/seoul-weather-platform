"""Postgres-backed recovery lease storage.

The lease registry owns the state machine; this adapter owns only atomic
database primitives.  Schema creation is explicit and never happens during
module import or object construction, so a dry-run coordinator cannot mutate
Airflow metadata accidentally.  The production deployment should run
``ensure_schema`` through an approved migration step before enabling an
executor.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from common.recovery.lease import SCHEMA_VERSION, LeaseBackend


LEASE_TABLE = "weather_recovery_leases"
CREATE_LEASE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {LEASE_TABLE} (
    job_key VARCHAR(512) PRIMARY KEY,
    lease_id VARCHAR(256) NOT NULL,
    owner_id VARCHAR(256) NOT NULL,
    plan_id VARCHAR(256) NOT NULL,
    action VARCHAR(32) NOT NULL
        CONSTRAINT weather_recovery_leases_action_ck
        CHECK (action IN ('raw_replay', 'recollect')),
    state VARCHAR(32) NOT NULL
        CONSTRAINT weather_recovery_leases_state_ck
        CHECK (state IN ('leased', 'running', 'succeeded', 'skipped', 'unrecoverable', 'blocked')),
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    attempt_count INTEGER NOT NULL
        CONSTRAINT weather_recovery_leases_attempt_ck CHECK (attempt_count > 0),
    evidence TEXT NOT NULL
)
"""


class PostgresLeaseBackend(LeaseBackend):
    """Atomic lease primitives implemented with SQL transactions.

    ``engine`` is deliberately duck-typed to keep the module importable in
    secretless unit tests.  It must provide SQLAlchemy ``begin`` and ``connect``
    context managers.  The production engine is Airflow's metadata engine.
    """

    def __init__(self, engine: Any) -> None:
        self._engine = engine

    def ensure_schema(self) -> None:
        """Create the control table when an approved migration invokes it."""
        from sqlalchemy import text

        with self._engine.begin() as connection:
            connection.execute(text(CREATE_LEASE_TABLE_SQL))

    def read(self, job_key: str) -> Mapping[str, object] | None:
        from sqlalchemy import text

        with self._engine.connect() as connection:
            row = connection.execute(
                text(
                    f"SELECT job_key, lease_id, owner_id, plan_id, action, state, "
                    f"acquired_at, expires_at, updated_at, attempt_count, evidence "
                    f"FROM {LEASE_TABLE} WHERE job_key = :job_key"
                ),
                {"job_key": job_key},
            ).mappings().first()
        if row is None:
            return None
        document = dict(row)
        evidence = document.get("evidence")
        if isinstance(evidence, str):
            try:
                document["evidence"] = json.loads(evidence)
            except json.JSONDecodeError as exc:
                raise ValueError("recovery lease evidence is invalid JSON") from exc
        document["schema_version"] = SCHEMA_VERSION
        return document

    def create_if_absent(self, job_key: str, document: Mapping[str, object]) -> bool:
        from sqlalchemy import text

        with self._engine.begin() as connection:
            result = connection.execute(
                text(
                    f"INSERT INTO {LEASE_TABLE} "
                    "(job_key, lease_id, owner_id, plan_id, action, state, acquired_at, "
                    "expires_at, updated_at, attempt_count, evidence) "
                    "VALUES (:job_key, :lease_id, :owner_id, :plan_id, :action, :state, "
                    ":acquired_at, :expires_at, :updated_at, :attempt_count, :evidence) "
                    "ON CONFLICT (job_key) DO NOTHING"
                ),
                _parameters(document),
            )
        return result.rowcount == 1

    def replace_if_expired(
        self,
        job_key: str,
        expected_lease_id: str,
        now: datetime,
        document: Mapping[str, object],
    ) -> bool:
        from sqlalchemy import text

        with self._engine.begin() as connection:
            result = connection.execute(
                text(
                    f"UPDATE {LEASE_TABLE} SET lease_id = :lease_id, owner_id = :owner_id, "
                    "plan_id = :plan_id, action = :action, state = :state, "
                    "acquired_at = :acquired_at, expires_at = :expires_at, "
                    "updated_at = :updated_at, attempt_count = :attempt_count, "
                    "evidence = :evidence "
                    "WHERE job_key = :job_key AND lease_id = :expected_lease_id "
                    "AND state IN ('leased', 'running') AND expires_at <= :now"
                ),
                {
                    **_parameters(document),
                    "expected_lease_id": expected_lease_id,
                    "now": _timestamp(now),
                },
            )
        return result.rowcount == 1

    def replace_if_owner(
        self,
        job_key: str,
        expected_lease_id: str,
        owner_id: str,
        now: datetime,
        document: Mapping[str, object],
    ) -> bool:
        from sqlalchemy import text

        with self._engine.begin() as connection:
            result = connection.execute(
                text(
                    f"UPDATE {LEASE_TABLE} SET lease_id = :lease_id, owner_id = :new_owner_id, "
                    "plan_id = :plan_id, action = :action, state = :state, "
                    "acquired_at = :acquired_at, expires_at = :expires_at, "
                    "updated_at = :updated_at, attempt_count = :attempt_count, "
                    "evidence = :evidence "
                    "WHERE job_key = :job_key AND lease_id = :expected_lease_id "
                    "AND owner_id = :expected_owner_id AND state IN ('leased', 'running') "
                    "AND expires_at > :now"
                ),
                {
                    **_parameters(document),
                    "new_owner_id": document["owner_id"],
                    "expected_owner_id": owner_id,
                    "expected_lease_id": expected_lease_id,
                    "now": _timestamp(now),
                },
            )
        return result.rowcount == 1


def build_airflow_postgres_lease_backend() -> PostgresLeaseBackend:
    """Build a backend from Airflow's metadata engine on explicit invocation."""
    from airflow.settings import engine

    return PostgresLeaseBackend(engine)


def _parameters(document: Mapping[str, object]) -> dict[str, object]:
    evidence = document.get("evidence") or {}
    return {
        "job_key": document["job_key"],
        "lease_id": document["lease_id"],
        "owner_id": document["owner_id"],
        "plan_id": document["plan_id"],
        "action": document["action"],
        "state": document["state"],
        "acquired_at": document["acquired_at"],
        "expires_at": document["expires_at"],
        "updated_at": document["updated_at"],
        "attempt_count": document["attempt_count"],
        "evidence": json.dumps(evidence, ensure_ascii=False, sort_keys=True),
    }


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("lease comparison timestamp must include timezone")
    return value.isoformat()


__all__ = [
    "CREATE_LEASE_TABLE_SQL",
    "LEASE_TABLE",
    "PostgresLeaseBackend",
    "build_airflow_postgres_lease_backend",
]
