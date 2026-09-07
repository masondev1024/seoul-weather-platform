"""Airflow DAG for the isolated Weather canonical W2 dbt transform."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)

DAGS_ROOT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.assets import WEATHER_BRONZE_ASSET  # noqa: E402
from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.ops.product_observability import record_domain_stage_event  # noqa: E402
from common.runmetrics import dump_dbt_run_results  # noqa: E402
from common.runtime_guard import (  # noqa: E402
    TARGET_CHOICES,
    default_target,
    validate_dev_runtime,
)
from weather_ingest.common.resources import DbtWorkload, TRINO_HEAVY_POOL  # noqa: E402
from weather_ingest.kma_coordination import weather_heavy_pool_kwargs  # noqa: E402
from weather_ingest.runtime import build_weather_manifest  # noqa: E402
from weather_ingest.w2_canonical_runtime import (  # noqa: E402
    AdminDongCrosswalkSnapshotUnavailableError,
    resolve_admin_dong_crosswalk_snapshot_id,
)
import weather_dbt_execution as weather_dbt  # noqa: E402
from weather_dbt_failure import classify_weather_dbt_failure  # noqa: E402
from weather_lineage import enable_lineage_if_configured  # noqa: E402


LOGGER = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")
DAG_ID = "weather_w2_canonical_transform"
DBT_PIPELINE = "weather-w2-canonical-transform"
DBT_BIN = weather_dbt.dbt_bin()
DBT_PROJECT = weather_dbt.dbt_project_dir()
WEATHER_DBT_CONTRACT_VARS = {"weather_w2_canonical_revision_date": "2025-04-01"}
WEATHER_DBT_RUN_RESULTS_XCOM_KEY = "weather_dbt_run_results_path"
SNAPSHOT_TASK_ID = "resolve_weather_snapshot_run"
WEATHER_SNAPSHOT_VAR = "weather_snapshot_dag_run_id"
ADMIN_DONG_CROSSWALK_PIN_XCOM_KEY = "admin_dong_crosswalk_pin_snapshot_id"
ADMIN_DONG_CROSSWALK_PIN_VAR = "admin_dong_crosswalk_pin_snapshot_id"
DBT_RETRY_DELAY = timedelta(minutes=2)
DOMAIN = "weather"
WEATHER_DISCORD_WEBHOOK_ENV = "WEATHER_DISCORD_WEBHOOK_URL"
DISCORD_RED = 15158332


@dataclass(frozen=True)
class DbtPhaseSpec:
    task_id: str
    dbt_command: str
    selector: str | None = None
    include_project_vars: bool = True
    workload: DbtWorkload = DbtWorkload.TRINO
    threads: int | None = 2


DBT_PHASE_SPECS = (
    DbtPhaseSpec(
        "dbt_deps",
        "deps",
        include_project_vars=False,
        workload=DbtWorkload.LOCAL,
        threads=None,
    ),
    DbtPhaseSpec(
        "dbt_run_w2_canonical_models",
        "run",
        "ask_seoul_weather_w2_canonical_models",
    ),
    DbtPhaseSpec(
        "dbt_test_w2_canonical_contracts",
        "test",
        "ask_seoul_weather_w2_canonical_contracts",
    ),
)
DBT_PHASE_TASK_IDS = tuple(spec.task_id for spec in DBT_PHASE_SPECS)
DEFAULT_PARAMS = {
    "target": Param(
        default=default_target(),
        type="string",
        enum=list(TARGET_CHOICES),
        description="dbt target profile name; defaults to the runtime env (#561).",
    )
}
record_weather_problem = problem_failure_callback(
    domain="weather",
    dbt_project_dir=DBT_PROJECT,
    dbt_run_results_xcom_key=WEATHER_DBT_RUN_RESULTS_XCOM_KEY,
)
record_weather_gold_product_event = record_domain_stage_event("weather", "gold")
record_weather_gold_product_failure = record_domain_stage_event(
    "weather", "gold", status="failed"
)


def _triggering_asset_events(*, context: dict, asset_uri: str):
    triggering = context.get("triggering_asset_events") or {}
    matched = []
    for asset_key, events in triggering.items():
        key_uri = getattr(asset_key, "uri", None) or str(asset_key)
        if key_uri != asset_uri:
            continue
        matched.extend(events if isinstance(events, (list, tuple)) else [events])
    if not matched:
        raise AirflowFailException(
            "weather transform requires at least one triggering Bronze asset event"
        )
    return matched


def resolve_weather_snapshot_run(**context) -> str:
    """Pin and verify the exact publishable Weather Bronze snapshot."""
    events = _triggering_asset_events(
        context=context, asset_uri=WEATHER_BRONZE_ASSET
    )
    event_metadata = [getattr(event, "extra", None) for event in events]
    required = {
        "source_id",
        "bronze_run_id",
        "bronze_dag_run_id",
        "event_at",
        "load_date",
        "row_count",
        "payload_hash",
        "is_publishable",
    }
    for index, extra in enumerate(event_metadata):
        if not isinstance(extra, dict) or not required <= extra.keys():
            raise AirflowFailException("weather Bronze asset event metadata is incomplete")
        candidate_run_id = str(extra["bronze_dag_run_id"] or "")
        if (
            extra["source_id"] != "kma_vilage_fcst"
            or not candidate_run_id
            or extra["bronze_run_id"] != candidate_run_id
            or extra["is_publishable"] is not True
        ):
            raise AirflowFailException(
                "weather Bronze asset event does not identify a publishable snapshot"
            )
        try:
            event_datetime = datetime.fromisoformat(
                str(extra["event_at"]).replace("Z", "+00:00")
            )
        except (TypeError, ValueError) as exc:
            raise AirflowFailException(
                "weather Bronze asset event timestamp is malformed"
            ) from exc
        event_metadata[index] = (extra, event_datetime)

    extra, _event_datetime = max(
        event_metadata,
        key=lambda item: (item[1], str(item[0]["bronze_dag_run_id"])),
    )
    run_id = str(extra["bronze_dag_run_id"])
    for candidate, _candidate_datetime in event_metadata:
        candidate_run_id = str(candidate["bronze_dag_run_id"])
        if candidate_run_id != run_id:
            build_weather_manifest().coalesce(
                candidate_run_id, replacement_run_id=run_id
            )
    try:
        verified_run_id = build_weather_manifest().require_publishable(run_id)
    except Exception as exc:
        raise AirflowFailException(
            f"weather Bronze snapshot is not publishable: {run_id}"
        ) from exc
    if str(verified_run_id) != run_id:
        raise AirflowFailException(
            f"weather Bronze manifest identity mismatch for snapshot: {run_id}"
        )
    ti = context.get("ti") or context.get("task_instance")
    if ti is not None:
        try:
            crosswalk_snapshot_id = resolve_admin_dong_crosswalk_snapshot_id()
        except AdminDongCrosswalkSnapshotUnavailableError as exc:
            raise AirflowFailException(str(exc)) from exc
        ti.xcom_push(
            key=ADMIN_DONG_CROSSWALK_PIN_XCOM_KEY,
            value=crosswalk_snapshot_id,
        )
    return run_id


def discord_report_date(context) -> str:
    logical_date = context.get("logical_date")
    if logical_date:
        return logical_date.astimezone(KST).strftime("%Y-%m-%d")
    return datetime.now(KST).strftime("%Y-%m-%d")


def short_text(value: object, limit: int = 130) -> str:
    text = str(value or "N/A")
    return text if len(text) <= limit else text[: limit - 3] + "..."


def transform_stage_name(task_id: str) -> str:
    if "deps" in task_id:
        return "dbt package setup"
    if "canonical_models" in task_id:
        return "canonical W2 model run"
    if "canonical_contracts" in task_id:
        return "canonical W2 contract test"
    return "unknown"


def send_weather_discord(title: str, description: str, color: int, footer: str) -> None:
    webhook_url = (os.environ.get(WEATHER_DISCORD_WEBHOOK_ENV) or "").strip()
    if not webhook_url:
        LOGGER.info("[weather notify:noop] %s (webhook url not configured)", title)
        return
    payload = {
        "embeds": [
            {
                "title": title,
                "description": description[:4096],
                "color": color,
                "footer": {"text": footer[:2048]},
            }
        ]
    }
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "ask-seoul-airflow/1.0",
        },
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=10).close()
    except Exception as exc:  # noqa: BLE001 - notification must not hide dbt failure
        LOGGER.warning("[weather notify] Discord send failed: %s", type(exc).__name__)


def notify_weather_transform_failure(context) -> None:
    ti = context.get("ti") or context.get("task_instance")
    task_id = getattr(ti, "task_id", "N/A")
    exc = context.get("exception")
    run_id = context.get("run_id", "N/A")
    target = (context.get("params") or {}).get("target", "N/A")
    send_weather_discord(
        f"Weather canonical W2 transform failed - {discord_report_date(context)} (target={target})",
        "\n".join(
            [
                "status: failed",
                f"stage: {transform_stage_name(task_id)}",
                f"task: `{task_id}`",
                f"error type: `{type(exc).__name__ if exc else 'N/A'}`",
                "",
                f"Airflow log: {getattr(ti, 'log_url', 'N/A')}",
            ]
        ),
        DISCORD_RED,
        f"dag_id={context['dag'].dag_id} · run_id={short_text(run_id, 180)}",
    )


def transform_schedule() -> str | list[Asset] | None:
    if "ASK_SEOUL_WEATHER_W2_CANONICAL_TRANSFORM_DAG_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_WEATHER_W2_CANONICAL_TRANSFORM_DAG_SCHEDULE"] or None
    return [Asset(WEATHER_BRONZE_ASSET)]


def run_dbt_phase(
    *,
    dbt_command: str,
    selector: str | None,
    include_project_vars: bool = True,
    snapshot_task_id: str | None = None,
    threads: int | None = None,
    **context,
) -> dict[str, object]:
    """Run one canonical W2 dbt phase with isolated current-attempt artifacts."""
    ti = context["ti"]
    task_id = getattr(ti, "task_id", None)
    is_deps = dbt_command == "deps"
    target = (context.get("params") or {}).get("target", "dev")
    snapshot_run_id = (
        ti.xcom_pull(task_ids=snapshot_task_id) if snapshot_task_id else None
    )
    crosswalk_pin_id = (
        ti.xcom_pull(
            task_ids=snapshot_task_id,
            key=ADMIN_DONG_CROSSWALK_PIN_XCOM_KEY,
        )
        if snapshot_task_id
        else None
    )
    run_results_path = None
    try:
        execution = weather_dbt.execute_dbt_phase(
            dbt_command=dbt_command,
            selector=selector,
            invocation_id=task_id or dbt_command.replace(" ", "-"),
            pipeline=DBT_PIPELINE,
            run_id=context.get("run_id"),
            task_id=task_id,
            try_number=getattr(ti, "try_number", None),
            target=target,
            variables=(
                json.dumps(
                    {
                        **WEATHER_DBT_CONTRACT_VARS,
                        **(
                            {WEATHER_SNAPSHOT_VAR: snapshot_run_id}
                            if snapshot_task_id
                            else {}
                        ),
                        **(
                            {ADMIN_DONG_CROSSWALK_PIN_VAR: crosswalk_pin_id}
                            if crosswalk_pin_id is not None
                            else {}
                        ),
                    },
                    separators=(",", ":"),
                )
                if include_project_vars and not is_deps
                else None
            ),
            threads=threads,
            project_dir=DBT_PROJECT,
            executable=DBT_BIN,
            runner=subprocess.run,
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
        exception_type = AirflowException if failure.retryable else AirflowFailException
        raise exception_type(
            "weather dbt command failed: "
            f"classification={failure.classification}; "
            f"exit_code={completed.returncode}"
        )
    return {
        "status": "success",
        "run_results_path": execution.existing_run_results_path,
        "sources_path": getattr(execution, "existing_sources_path", None),
        "manifest_path": getattr(execution, "existing_manifest_path", None),
        "selected_unique_ids": list(getattr(execution, "selected_unique_ids", ())),
    }


def dbt_task(spec: DbtPhaseSpec) -> PythonOperator:
    operator_kwargs = {
        "task_id": spec.task_id,
        "python_callable": run_dbt_phase,
        "op_kwargs": {
            "dbt_command": spec.dbt_command,
            "selector": spec.selector,
            "include_project_vars": spec.include_project_vars,
            "snapshot_task_id": SNAPSHOT_TASK_ID,
            "threads": spec.threads,
        },
        "weight_rule": "absolute",
        "retries": 1,
        "retry_delay": DBT_RETRY_DELAY,
        "on_failure_callback": record_weather_problem,
    }
    if spec.workload is DbtWorkload.TRINO:
        # W2 is a separate writer but still competes for the same local Trino
        # heap.  Keep it behind the exclusive two-slot Weather lock so it
        # cannot consume one branch slot during the hourly transform.
        operator_kwargs.update(
            weather_heavy_pool_kwargs(TRINO_HEAVY_POOL, pool_slots=2)
        )
    if spec.task_id == DBT_PHASE_TASK_IDS[-1]:
        operator_kwargs["on_success_callback"] = record_weather_gold_product_event
        operator_kwargs["on_failure_callback"] = [
            record_weather_problem,
            record_weather_gold_product_failure,
        ]
    return PythonOperator(**operator_kwargs)


def _current_run_results_path(**context) -> str | None:
    """Return the latest existing dbt artifact recorded by this DAG run."""
    ti = context.get("ti") or context.get("task_instance")
    if ti is None:
        return None

    def existing_path(candidate: object) -> str | None:
        if not isinstance(candidate, (str, os.PathLike)):
            return None
        path = os.fspath(candidate)
        if not isinstance(path, str) or not os.path.exists(path):
            return None
        return path

    for task_id in reversed(DBT_PHASE_TASK_IDS):
        try:
            result = ti.xcom_pull(task_ids=task_id)
        except Exception:  # noqa: BLE001 - continue to earlier current-run phases
            result = None
        if isinstance(result, dict):
            path = existing_path(result.get("run_results_path"))
            if path is not None:
                return path

        try:
            failure_path = ti.xcom_pull(
                task_ids=task_id,
                key=WEATHER_DBT_RUN_RESULTS_XCOM_KEY,
            )
        except Exception:  # noqa: BLE001 - continue to earlier current-run phases
            failure_path = None
        path = existing_path(failure_path)
        if path is not None:
            return path
    return None


def publish_dbt_run_metrics(run_results_path: str | None = None, **context) -> dict:
    """Persist model/test run metrics without changing the dbt contract gate."""
    resolved_path = (
        run_results_path
        if run_results_path is not None
        else _current_run_results_path(**context)
    )
    if not resolved_path or not os.path.exists(resolved_path):
        print(f"run_results.json missing; metrics skipped: {resolved_path}")
        return {"rows": 0, "skipped": True}
    target = (context.get("params") or {}).get("target")
    records = dump_dbt_run_results(resolved_path, domain=DOMAIN, target=target)
    print(
        "dbt run metrics persisted: "
        f"{len(records)} records (domain={DOMAIN}, target={target})"
    )
    return {"rows": len(records), "skipped": False}


with DAG(
    dag_id=DAG_ID,
    description="Transform a publishable Weather Bronze snapshot into canonical W2 Gold.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=transform_schedule(),
    catchup=False,
    max_active_runs=1,
    # 배포 직후 자동 실행을 막기 위한 의도적 안전장치 — 영구 pause가 아니다.
    # 머지 후 반드시 수동 unpause 필요:
    # domains/weather/docs/operations/new-asset-triggered-dag-rollout.md
    is_paused_upon_creation=True,
    default_args={"retries": 1, "retry_delay": DBT_RETRY_DELAY},
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "weather", "transform", "w2", "canonical", "dbt"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "weather", "requested_target": "{{ params.target }}"},
        on_failure_callback=record_weather_problem,
    )

    resolve_snapshot = PythonOperator(
        task_id=SNAPSHOT_TASK_ID,
        python_callable=resolve_weather_snapshot_run,
        on_failure_callback=record_weather_problem,
    )

    dbt_phase_tasks = {spec.task_id: dbt_task(spec) for spec in DBT_PHASE_SPECS}
    dbt_tasks_in_order = list(dbt_phase_tasks.values())

    publish_dbt_metrics = PythonOperator(
        task_id="publish_dbt_run_metrics",
        python_callable=publish_dbt_run_metrics,
        on_failure_callback=record_weather_problem,
    ).as_teardown(on_failure_fail_dagrun=False)

    pipeline_tasks = [
        validate_runtime,
        resolve_snapshot,
        *dbt_tasks_in_order,
        publish_dbt_metrics,
    ]
    for upstream, downstream in zip(pipeline_tasks, pipeline_tasks[1:]):
        upstream >> downstream


enable_lineage_if_configured(dag)
