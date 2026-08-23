"""Shared, non-DAG runtime helpers for Weather dbt tasks.

The transform and hourly serving-refresh DAGs must use the same invocation
rules, but importing one DAG entrypoint from another makes DAG parsing order a
runtime dependency.  This module owns only execution mechanics and a frozen
KST serving anchor; individual DAG modules still own topology and callbacks.
"""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from weather_dbt_failure import classify_weather_dbt_failure


DOMAIN = "weather"
DBT_RETRY_DELAY = timedelta(minutes=2)
WEATHER_DBT_CONTRACT_VARS = {"weather_w2_canonical_revision_date": "2025-04-01"}
WEATHER_DBT_RUN_RESULTS_XCOM_KEY = "weather_dbt_run_results_path"
WEATHER_SNAPSHOT_VAR = "weather_snapshot_dag_run_id"
WEATHER_SNAPSHOT_LOAD_DATE_VAR = "weather_snapshot_load_date"
WEATHER_SNAPSHOT_LOAD_DATE_XCOM_KEY = "weather_snapshot_load_date"
WEATHER_SERVING_AS_OF_HOUR_VAR = "weather_serving_as_of_hour"
SERVING_AS_OF_HOUR_TASK_ID = "resolve_weather_serving_as_of_hour"

KST = ZoneInfo("Asia/Seoul")
_SERVING_AS_OF_HOUR_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:00:00$")
_SNAPSHOT_LOAD_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def resolve_weather_serving_as_of_hour(
    *, now: datetime | None = None, **_context: object
) -> str:
    """Freeze the KST hour shared by every dbt run/test task in one DAG run."""

    current = now or datetime.now(KST)
    if current.tzinfo is None:
        raise ValueError("weather serving as-of hour requires a timezone-aware datetime")
    return current.astimezone(KST).replace(
        minute=0, second=0, microsecond=0
    ).strftime("%Y-%m-%d %H:%M:%S")


def _serving_as_of_hour_from_task(ti: Any, task_id: str | None) -> str | None:
    if task_id is None:
        return None
    raw = ti.xcom_pull(task_ids=task_id)
    if not isinstance(raw, str) or not _SERVING_AS_OF_HOUR_RE.fullmatch(raw):
        raise RuntimeError("weather serving dbt phase requires a valid frozen KST as-of hour")
    return raw


def _snapshot_load_date_from_task(ti: Any, task_id: str | None) -> str | None:
    if task_id is None:
        return None
    raw = ti.xcom_pull(
        task_ids=task_id,
        key=WEATHER_SNAPSHOT_LOAD_DATE_XCOM_KEY,
    )
    if not isinstance(raw, str) or not _SNAPSHOT_LOAD_DATE_RE.fullmatch(raw):
        raise RuntimeError(
            "weather serving dbt phase requires a valid Bronze load_date"
        )
    try:
        datetime.strptime(raw, "%Y-%m-%d")
    except ValueError as error:
        raise RuntimeError(
            "weather serving dbt phase requires a valid Bronze load_date"
        ) from error
    return raw


def weather_serving_as_of_hour_state(
    *,
    ti: Any,
    now: datetime | None = None,
) -> tuple[str, str]:
    """Return the frozen KST hour and whether it is still the current hour."""

    frozen_value = _serving_as_of_hour_from_task(
        ti,
        SERVING_AS_OF_HOUR_TASK_ID,
    )
    if frozen_value is None:  # pragma: no cover - task id is fixed above
        raise RuntimeError("weather publication requires a frozen serving as-of hour")
    current_value = resolve_weather_serving_as_of_hour(now=now)
    frozen_hour = datetime.strptime(
        frozen_value,
        "%Y-%m-%d %H:%M:%S",
    ).replace(tzinfo=KST)
    current_hour = datetime.strptime(
        current_value,
        "%Y-%m-%d %H:%M:%S",
    ).replace(tzinfo=KST)
    if frozen_hour > current_hour:
        raise RuntimeError(
            "weather publication frozen serving hour is in the future: "
            f"frozen={frozen_value} current={current_value}"
        )
    state = "current" if frozen_hour == current_hour else "stale"
    return frozen_value, state


def run_weather_dbt_phase(
    *,
    dbt_command: str,
    selector: str | None,
    include_project_vars: bool,
    snapshot_task_id: str | None,
    serving_as_of_task_id: str | None,
    threads: int | None,
    context: Mapping[str, Any],
    dbt_executor: Any,
    dbt_project: str,
    dbt_bin: str,
    runner: Callable[..., Any],
    pipeline: str,
    failure_exception: Callable[[bool, str], Exception],
) -> dict[str, object]:
    """Run one dbt task with a stable serving time boundary and isolated artifacts."""

    ti = context["ti"]
    task_id = getattr(ti, "task_id", None)
    is_deps = dbt_command == "deps"
    target = (context.get("params") or {}).get("target", "dev")
    snapshot_run_id = (
        ti.xcom_pull(task_ids=snapshot_task_id) if snapshot_task_id else None
    )
    snapshot_load_date = _snapshot_load_date_from_task(ti, snapshot_task_id)
    serving_as_of_hour = _serving_as_of_hour_from_task(ti, serving_as_of_task_id)
    run_results_path = None
    try:
        variables: dict[str, object] = dict(WEATHER_DBT_CONTRACT_VARS)
        if snapshot_task_id:
            variables[WEATHER_SNAPSHOT_VAR] = snapshot_run_id
            variables[WEATHER_SNAPSHOT_LOAD_DATE_VAR] = snapshot_load_date
        if serving_as_of_hour is not None:
            variables[WEATHER_SERVING_AS_OF_HOUR_VAR] = serving_as_of_hour
        execution = dbt_executor.execute_dbt_phase(
            dbt_command=dbt_command,
            selector=selector,
            invocation_id=task_id or dbt_command.replace(" ", "-"),
            pipeline=pipeline,
            run_id=context.get("run_id"),
            task_id=task_id,
            try_number=getattr(ti, "try_number", None),
            target=target,
            variables=(
                json.dumps(variables, separators=(",", ":"))
                if include_project_vars and not is_deps
                else None
            ),
            threads=threads,
            project_dir=dbt_project,
            executable=dbt_bin,
            runner=runner,
        )
        run_results_path = execution.existing_run_results_path
    finally:
        ti.xcom_push(
            key=WEATHER_DBT_RUN_RESULTS_XCOM_KEY,
            value=run_results_path,
        )
    for completed in execution.attempts:
        if completed.stdout:
            print(completed.stdout, end="")
        if completed.stderr:
            print(completed.stderr, end="", file=sys.stderr)
    completed = execution.completed
    if completed.returncode != 0 or execution.missing_expected_artifacts:
        command_output = "\n".join(
            str(value)
            for attempt in execution.attempts
            for value in (attempt.stdout, attempt.stderr)
            if value
        )
        failure = classify_weather_dbt_failure(
            dbt_command=dbt_command,
            returncode=int(completed.returncode),
            artifact_path=execution.existing_run_results_path,
            missing_expected_artifacts=execution.missing_expected_artifacts,
            command_output=command_output,
        )
        raise failure_exception(
            failure.retryable,
            "weather dbt command failed: "
            f"classification={failure.classification}; "
            f"exit_code={completed.returncode}",
        )
    return {
        "status": "success",
        "run_results_path": execution.existing_run_results_path,
        "sources_path": execution.existing_sources_path,
        "manifest_path": execution.existing_manifest_path,
        "selected_unique_ids": list(execution.selected_unique_ids),
    }
