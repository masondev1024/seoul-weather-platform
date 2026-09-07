"""Read-only Airflow metadata snapshot for recovery admission.

This module is the runtime adapter between Airflow's ORM and the pure
``admission`` policy.  It issues only SELECTs: no pool mutation, DagRun
creation, task clearing, or trigger is possible through this boundary.  The
adapter remains separate from the executor so a coordinator can prove the
snapshot it used before any future write-capable step is approved.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from common.recovery.admission import (
    ACTIVE_RUN_STATES,
    ActiveRunSnapshot,
    PoolSnapshot,
    RecoveryAdmissionError,
)


class AirflowSnapshotError(RecoveryAdmissionError):
    """Airflow metadata could not be converted into a bounded snapshot."""


def read_airflow_recovery_snapshot(
    session: Any,
    *,
    dag_ids: Sequence[str],
    pool_names: Sequence[str],
) -> tuple[tuple[ActiveRunSnapshot, ...], dict[str, PoolSnapshot]]:
    """Read active DagRuns and pool pressure using Airflow's ORM.

    ``session`` is supplied by the caller (normally ``create_session``).  The
    function deliberately does not own a transaction or commit/flush it; the
    caller can place it inside a short read-only transaction.  Missing pools
    are returned as an absent mapping entry and are rejected by admission.
    """
    if not hasattr(session, "query") or not callable(session.query):
        raise AirflowSnapshotError("session does not expose a query method")
    dag_ids = _safe_name_sequence(dag_ids, field="dag_ids")
    pool_names = _safe_name_sequence(pool_names, field="pool_names")
    try:
        from airflow.models.dagrun import DagRun
        from airflow.models.pool import Pool
    except Exception as exc:  # pragma: no cover - exercised in the image
        raise AirflowSnapshotError("Airflow ORM is unavailable") from exc

    try:
        rows = (
            session.query(DagRun.dag_id, DagRun.run_id, DagRun.state)
            .filter(DagRun.dag_id.in_(dag_ids))
            .filter(DagRun.state.in_(ACTIVE_RUN_STATES))
            .all()
        )
        stats = Pool.slots_stats(session=session)
    except Exception as exc:
        raise AirflowSnapshotError("Airflow metadata read failed") from exc
    return snapshot_from_metadata(rows, stats, pool_names=pool_names)


def snapshot_from_metadata(
    active_run_rows: Iterable[object],
    pool_stats: Mapping[str, Mapping[str, object]],
    *,
    pool_names: Sequence[str],
) -> tuple[tuple[ActiveRunSnapshot, ...], dict[str, PoolSnapshot]]:
    """Convert already-read ORM rows into validated admission snapshots."""
    requested_pools = _safe_name_sequence(pool_names, field="pool_names")
    runs: list[ActiveRunSnapshot] = []
    seen_runs: set[tuple[str, str]] = set()
    for row in active_run_rows:
        try:
            dag_id, run_id, state = row  # type: ignore[misc]
        except (TypeError, ValueError) as exc:
            raise AirflowSnapshotError("active DagRun row is malformed") from exc
        try:
            snapshot = ActiveRunSnapshot(
                dag_id=_text(dag_id, field="dag_id"),
                run_id=_text(run_id, field="run_id"),
                state=_text(state, field="state"),
            )
        except RecoveryAdmissionError as exc:
            raise AirflowSnapshotError("active DagRun row is invalid") from exc
        if snapshot.state not in ACTIVE_RUN_STATES:
            raise AirflowSnapshotError("active DagRun query returned an inactive state")
        identity = (snapshot.dag_id, snapshot.run_id)
        if identity in seen_runs:
            raise AirflowSnapshotError("active DagRun rows contain a duplicate")
        seen_runs.add(identity)
        runs.append(snapshot)
    runs.sort(key=lambda item: (item.dag_id, item.run_id))

    pools: dict[str, PoolSnapshot] = {}
    for pool_name in requested_pools:
        raw = pool_stats.get(pool_name)
        if raw is None:
            continue
        if not isinstance(raw, Mapping):
            raise AirflowSnapshotError("pool stats row is invalid")
        total = _finite_non_negative_number(raw.get("total"), field="total")
        running = _finite_non_negative_number(raw.get("running"), field="running")
        deferred = _finite_non_negative_number(raw.get("deferred"), field="deferred")
        queued = _finite_non_negative_number(raw.get("queued"), field="queued")
        scheduled = _finite_non_negative_number(raw.get("scheduled"), field="scheduled")
        if total < 1 or total != int(total):
            raise AirflowSnapshotError("pool total slots must be a positive integer")
        occupied = running + deferred
        if occupied > total:
            raise AirflowSnapshotError("pool occupied slots exceed total slots")
        try:
            pools[pool_name] = PoolSnapshot(
                pool=pool_name,
                total_slots=int(total),
                occupied_slots=int(occupied),
                queued_tasks=int(queued + scheduled),
            )
        except RecoveryAdmissionError as exc:
            raise AirflowSnapshotError("pool stats are outside the admission contract") from exc
    return tuple(runs), pools


def _safe_name_sequence(values: Sequence[str], *, field: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise AirflowSnapshotError(f"{field} must be a sequence of names")
    try:
        materialized = tuple(values)
    except TypeError as exc:
        raise AirflowSnapshotError(f"{field} must be a sequence of names") from exc
    if any(not isinstance(value, str) for value in materialized):
        raise AirflowSnapshotError(f"{field} must contain only strings")
    if len(set(materialized)) != len(materialized):
        raise AirflowSnapshotError(f"{field} contains duplicates")
    for value in materialized:
        _text(value, field=field.removesuffix("s"))
    return materialized


def _text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value or any(
        character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:/+-"
        for character in value
    ):
        raise AirflowSnapshotError(f"{field} contains unsafe text")
    return value


def _finite_non_negative_number(value: object, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AirflowSnapshotError(f"pool {field} is not numeric")
    converted = float(value)
    if not math.isfinite(converted) or converted < 0:
        raise AirflowSnapshotError(f"pool {field} is outside bounds")
    return converted


__all__ = [
    "AirflowSnapshotError",
    "read_airflow_recovery_snapshot",
    "snapshot_from_metadata",
]
