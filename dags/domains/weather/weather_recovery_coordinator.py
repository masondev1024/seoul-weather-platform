"""Paused, read-only Weather recovery coordinator.

The coordinator is deliberately a planning control plane in this first slice:
it reads validated collection-slot receipts, applies bounded recovery policy,
and emits a sanitized plan.  It does not execute a DAG, call KMA, or write
R2/Trino/D1.  Enabling an executor requires a separate approval and durable
lease implementation.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)
DOMAINS_DIR = os.path.dirname(DAG_DIR)
if DOMAINS_DIR not in sys.path:
    sys.path.insert(0, DOMAINS_DIR)
DAGS_ROOT_DIR = os.path.dirname(DOMAINS_DIR)
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.recovery.planner import RecoveryPolicy, plan_recovery  # noqa: E402
from weather_ingest.runtime import build_weather_collection_slot_storage  # noqa: E402
from weather_recovery_candidates import (  # noqa: E402
    read_weather_recovery_candidates,
)


DAG_ID = "weather_recovery_coordinator"
TASK_ID = "plan_weather_recovery"
_SCHEDULE_ENV = "ASK_SEOUL_WEATHER_RECOVERY_COORDINATOR_SCHEDULE"
_MAX_JOBS_ENV = "ASK_SEOUL_WEATHER_RECOVERY_MAX_JOBS"
_MAX_API_JOBS_ENV = "ASK_SEOUL_WEATHER_RECOVERY_MAX_API_JOBS"
_MAX_AGE_HOURS_ENV = "ASK_SEOUL_WEATHER_RECOVERY_MAX_AGE_HOURS"


def _optional_schedule() -> str | None:
    value = os.environ.get(_SCHEDULE_ENV, "").strip()
    return value or None


def _positive_int_env(name: str, default: int, *, allow_zero: bool = False) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 0 or (value == 0 and not allow_zero):
        raise ValueError(f"{name} must be positive")
    return value


def _positive_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def recovery_policy_from_environment() -> RecoveryPolicy:
    """Resolve bounded policy knobs; malformed values fail closed."""
    return RecoveryPolicy(
        max_jobs=_positive_int_env(_MAX_JOBS_ENV, 3),
        max_api_jobs=_positive_int_env(_MAX_API_JOBS_ENV, 1, allow_zero=True),
        max_recovery_age=timedelta(
            hours=_positive_float_env(_MAX_AGE_HOURS_ENV, 24.0)
        ),
    )


def plan_weather_recovery(**context) -> dict[str, object]:
    """Read receipts and print a no-write recovery plan."""
    now = datetime.now(timezone.utc)
    candidates = read_weather_recovery_candidates(
        build_weather_collection_slot_storage(),
        now=now,
    )
    plan = plan_recovery(
        candidates,
        now=now,
        policy=recovery_policy_from_environment(),
    )
    payload = plan.to_dict()
    print(
        "[weather-recovery-dry-run] "
        + json.dumps(payload, ensure_ascii=False, sort_keys=True)
    )
    return payload


with DAG(
    dag_id=DAG_ID,
    description="Read-only, bounded Weather recovery planning control plane.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=_optional_schedule(),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    is_paused_upon_creation=True,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=4),
    },
    tags=["ask_seoul", "weather", "recovery", "control", "dry-run"],
) as dag:
    PythonOperator(
        task_id=TASK_ID,
        python_callable=plan_weather_recovery,
    )


__all__ = [
    "DAG_ID",
    "TASK_ID",
    "dag",
    "plan_weather_recovery",
    "recovery_policy_from_environment",
]
