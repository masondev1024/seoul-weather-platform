#!/usr/bin/env python3
"""Fail-closed admission guard for manual Weather/Traffic Airflow triggers."""

from __future__ import annotations

import inspect
import json
import re
import subprocess
import sys
from collections.abc import Callable, Sequence


Runner = Callable[[list[str]], subprocess.CompletedProcess[str]]

WEATHER_DAGS = frozenset(
    {
        "weather_reference_data_refresh",
        "weather_vilage_fcst_bronze",
        "weather_vilage_fcst_collection_slot_reconciliation",
        "weather_vilage_fcst_recollect",
        "weather_vilage_fcst_bronze_backfill",
        "weather_vilage_fcst_transform",
        "weather_w1_contract_smoke",
        "weather_w2_canonical_transform",
        "weather_w2_canonical_contract_audit",
        "weather_w2_observation_recovery",
        "weather_serving_export",
        "weather_bronze_reliability_report",
        "ask_seoul_iceberg_maintenance",
    }
)

TRAFFIC_DAGS = frozenset(
    {
        "traffic_incident_landing",
        "traffic_incident_collection_slot_reconciliation",
        "traffic_incident_bronze",
        "traffic_incident_recollect",
        "traffic_incident_bronze_backfill",
        "traffic_flow_bronze",
        "traffic_link_reference_backfill",
        "traffic_link_reference_sync",
        "traffic_incident_transform",
        "traffic_flow_transform",
        "traffic_gold_transform",
        "traffic_cross_domain_gold_transform",
        "traffic_cross_domain_serving_export",
        "traffic_snapshot_recovery",
        "traffic_serving_export",
        "traffic_bronze_reliability_report",
        "ask_seoul_iceberg_maintenance",
    }
)

COMMON_DAGS = frozenset({"common_admin_dong_bronze"})

MAINTENANCE_DAG = "ask_seoul_iceberg_maintenance"
ACTIVE_STATES = frozenset({"queued", "running"})
SAFE_DIAGNOSTIC = re.compile(r"[^A-Za-z0-9_.:~+-]")
# Stable, repository-namespaced signed bigint: int.from_bytes(b"ASKSAFE1", "big").
ADVISORY_LOCK_KEY = 4707188856481793329
RESULT_PREFIX = "ASK_SAFE_TRIGGER_RESULT="


def conflict_set_for(dag_id: str) -> set[str]:
    if dag_id == MAINTENANCE_DAG:
        return set(WEATHER_DAGS | TRAFFIC_DAGS | COMMON_DAGS)
    if dag_id in COMMON_DAGS:
        return set(WEATHER_DAGS | TRAFFIC_DAGS | COMMON_DAGS)
    if dag_id in WEATHER_DAGS:
        return set(WEATHER_DAGS | COMMON_DAGS)
    if dag_id in TRAFFIC_DAGS:
        return set(TRAFFIC_DAGS | COMMON_DAGS)
    raise ValueError(f"unknown DAG id: {dag_id}")


def _scheduler_critical_section(
    *,
    dag_id: str,
    conflict_dags: Sequence[str],
    trigger_args: Sequence[str],
    check_only: bool,
    lock_key: int,
    session_factory: Callable[[], object],
    sql_text: Callable[[str], object],
    active_run_query: Callable[[object, Sequence[str]], Sequence[tuple[str, str, str]]],
    trigger_runner: Runner,
) -> dict[str, object]:
    """Query and optionally trigger while holding one metadata-DB session lock."""
    outcome: dict[str, object] | None = None
    try:
        with session_factory() as session:
            try:
                acquired = session.execute(
                    sql_text("SELECT pg_try_advisory_lock(:lock_key)"),
                    {"lock_key": lock_key},
                ).scalar()
            except Exception:
                return {"status": "lock_failed"}

            if acquired is not True:
                return {"status": "lock_unavailable"}

            try:
                try:
                    rows = active_run_query(session, conflict_dags)
                    active_runs = []
                    for row in rows:
                        row_dag_id, run_id, state = row
                        if not all(
                            isinstance(value, str)
                            for value in (row_dag_id, run_id, state)
                        ):
                            raise ValueError("active DagRun row has invalid fields")
                        if state in {"queued", "running"}:
                            active_runs.append(
                                {"dag_id": row_dag_id, "run_id": run_id, "state": state}
                            )
                except Exception:
                    outcome = {"status": "query_failed"}
                else:
                    if active_runs:
                        outcome = {"status": "active", "active_runs": active_runs}
                    elif check_only:
                        outcome = {"status": "clear"}
                    else:
                        command = ["airflow", "dags", "trigger", dag_id, *trigger_args]
                        try:
                            trigger_result = trigger_runner(command)
                        except Exception:
                            outcome = {"status": "trigger_failed"}
                        else:
                            outcome = {
                                "status": (
                                    "triggered"
                                    if trigger_result.returncode == 0
                                    else "trigger_failed"
                                )
                            }
            finally:
                try:
                    released = session.execute(
                        sql_text("SELECT pg_advisory_unlock(:lock_key)"),
                        {"lock_key": lock_key},
                    ).scalar()
                except Exception:
                    released = False

            if released is not True:
                if outcome is not None and outcome.get("status") == "triggered":
                    return {"status": "triggered_unlock_failed"}
                return {"status": "unlock_failed"}
            if outcome is None:
                return {"status": "guard_failed"}
            return outcome
    except Exception:
        if outcome is not None and outcome.get("status") == "triggered":
            return {"status": "triggered_cleanup_failed"}
        return {"status": "lock_failed"}


def _scheduler_script() -> str:
    critical_section_source = inspect.getsource(_scheduler_critical_section)
    return f"""
from __future__ import annotations

import json
import subprocess
import sys

from airflow.models.dagrun import DagRun
from airflow.utils.session import create_session
from sqlalchemy import text

{critical_section_source}

def _active_run_query(session, dag_ids):
    return (
        session.query(DagRun.dag_id, DagRun.run_id, DagRun.state)
        .filter(
            DagRun.dag_id.in_(dag_ids),
            DagRun.state.in_(["queued", "running"]),
        )
        .all()
    )


def _trigger_runner(command):
    return subprocess.run(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


try:
    payload = json.loads(sys.argv[1])
    result = _scheduler_critical_section(
        dag_id=payload["dag_id"],
        conflict_dags=payload["conflict_dags"],
        trigger_args=payload["trigger_args"],
        check_only=payload["check_only"],
        lock_key=payload["lock_key"],
        session_factory=create_session,
        sql_text=text,
        active_run_query=_active_run_query,
        trigger_runner=_trigger_runner,
    )
except Exception:
    result = {{"status": "guard_failed"}}

print("{RESULT_PREFIX}" + json.dumps(result, sort_keys=True))
""".strip()


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, check=False)


def _scheduler_command(
    dag_id: str,
    conflict_dags: set[str],
    trigger_args: Sequence[str],
    check_only: bool,
) -> list[str]:
    payload = {
        "dag_id": dag_id,
        "conflict_dags": sorted(conflict_dags),
        "trigger_args": list(trigger_args),
        "check_only": check_only,
        "lock_key": ADVISORY_LOCK_KEY,
    }
    return [
        "docker",
        "compose",
        "exec",
        "-T",
        "airflow-scheduler",
        "python",
        "-c",
        _scheduler_script(),
        json.dumps(payload, separators=(",", ":")),
    ]


def _active_runs(output: str) -> list[dict[str, str]]:
    data = json.loads(output)
    if not isinstance(data, list):
        raise ValueError("query output root is not a list")
    active: list[dict[str, str]] = []
    for item in data:
        if not isinstance(item, dict):
            raise ValueError("query output row is not an object")
        dag_id = item.get("dag_id")
        run_id = item.get("run_id")
        state = item.get("state")
        if not all(isinstance(value, str) for value in (dag_id, run_id, state)):
            raise ValueError("query output row has invalid fields")
        if state in ACTIVE_STATES:
            active.append({"dag_id": dag_id, "run_id": run_id, "state": state})
    return active


def _guard_result(output: str) -> dict[str, object]:
    """Parse one authenticated guard result while ignoring Airflow startup logs."""
    try:
        result = json.loads(output)
    except json.JSONDecodeError:
        marked = [
            line.removeprefix(RESULT_PREFIX)
            for line in output.splitlines()
            if line.startswith(RESULT_PREFIX)
        ]
        if len(marked) != 1:
            raise ValueError("guard output must contain exactly one result marker")
        result = json.loads(marked[0])
    if not isinstance(result, dict) or not isinstance(result.get("status"), str):
        raise ValueError("guard output root is not a status object")
    return result


def _sanitize(value: str) -> str:
    return SAFE_DIAGNOSTIC.sub("_", value)


def _split_args(argv: Sequence[str]) -> tuple[str, bool, list[str]]:
    if not argv:
        raise ValueError("usage: safe-trigger-dag.sh <dag_id> [--check-only] [airflow trigger args...]")
    dag_id = argv[0]
    trigger_args = list(argv[1:])
    check_only = False
    if "--check-only" in trigger_args:
        trigger_args.remove("--check-only")
        check_only = True
    return dag_id, check_only, trigger_args


def main(argv: Sequence[str] | None = None, *, runner: Runner = _run) -> int:
    if argv is None:
        argv = sys.argv[1:]
    try:
        dag_id, check_only, trigger_args = _split_args(argv)
        conflict_dags = conflict_set_for(dag_id)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 64

    guard_result = runner(
        _scheduler_command(dag_id, conflict_dags, trigger_args, check_only)
    )
    if guard_result.returncode != 0:
        print("scheduler admission guard failed; trigger blocked", file=sys.stderr)
        return 2

    try:
        result = _guard_result(guard_result.stdout)
    except (json.JSONDecodeError, ValueError) as exc:
        print(f"scheduler admission guard returned malformed JSON; trigger blocked: {exc}", file=sys.stderr)
        return 2

    status = result["status"]
    if status == "active":
        try:
            active_runs = _active_runs(json.dumps(result.get("active_runs")))
        except (json.JSONDecodeError, ValueError) as exc:
            print(
                f"scheduler admission guard returned malformed JSON; trigger blocked: {exc}",
                file=sys.stderr,
            )
            return 2
        print("active family DagRun exists; trigger blocked", file=sys.stderr)
        for run in active_runs:
            print(
                "dag_id="
                f"{_sanitize(run['dag_id'])} "
                "run_id="
                f"{_sanitize(run['run_id'])} "
                f"state={_sanitize(run['state'])}",
                file=sys.stderr,
            )
        return 3

    if status == "clear" and check_only:
        print(f"no queued/running family DagRuns for {dag_id}")
        return 0

    if status == "triggered" and not check_only:
        return 0

    if status == "lock_unavailable":
        print("manual trigger guard is busy; trigger blocked", file=sys.stderr)
        return 4

    if status in {"triggered_cleanup_failed", "triggered_unlock_failed"}:
        print(
            "trigger command succeeded but scheduler guard cleanup failed; "
            "inspect Airflow before retry",
            file=sys.stderr,
        )
        return 5

    if status in {
        "guard_failed",
        "lock_failed",
        "query_failed",
        "trigger_failed",
        "unlock_failed",
    }:
        print("scheduler admission guard failed; trigger blocked", file=sys.stderr)
        return 2

    print("scheduler admission guard returned invalid status; trigger blocked", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
