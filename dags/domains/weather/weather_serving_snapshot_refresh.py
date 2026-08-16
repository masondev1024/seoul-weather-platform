"""Hourly refresh of the four public Weather serving products.

This DAG deliberately does not collect source data or reconstruct the Weather
Bronze snapshot. It recalculates only the place-based serving Gold models from
the latest already-published pipeline state, validates their direct contracts,
then emits the same terminal asset consumed by the common D1 Publisher.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
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

from common.assets import WEATHER_GOLD_PUBLICATION_READY_ASSET  # noqa: E402
from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.pools import TRINO_WEATHER_LEGACY_HEAVY_POOL  # noqa: E402
from common.runmetrics import dump_dbt_run_results  # noqa: E402
from common.runtime_guard import (  # noqa: E402
    TARGET_CHOICES,
    default_target,
    validate_dev_runtime,
)
import weather_dbt_execution as weather_dbt  # noqa: E402
from weather_dbt_runtime import (  # noqa: E402
    DBT_RETRY_DELAY,
    DOMAIN,
    SERVING_AS_OF_HOUR_TASK_ID,
    WEATHER_DBT_RUN_RESULTS_XCOM_KEY,
    resolve_weather_serving_as_of_hour,
    run_weather_dbt_phase,
    weather_serving_as_of_hour_state,
)
from weather_lineage import enable_lineage_if_configured  # noqa: E402
from weather_serving_exclusion import guard_serving_snapshot_refresh  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
DBT_BIN = weather_dbt.dbt_bin()
DBT_PROJECT = weather_dbt.dbt_project_dir()
DEFAULT_PARAMS = {
    "target": Param(
        default=default_target(),
        type="string",
        enum=list(TARGET_CHOICES),
        description="dbt target profile name; defaults to the runtime env (#561).",
    )
}
record_weather_problem = problem_failure_callback(
    domain=DOMAIN,
    dbt_project_dir=DBT_PROJECT,
    dbt_run_results_xcom_key=WEATHER_DBT_RUN_RESULTS_XCOM_KEY,
)
SERVING_SNAPSHOT_SELECTOR = "ask_seoul_weather_serving_snapshot_refresh"
SERVING_SNAPSHOT_PRIORITY_WEIGHT = 100
REFRESH_DBT_TASK_IDS = (
    "dbt_run_serving_snapshot_refresh",
    "dbt_test_serving_snapshot_refresh",
)
WEATHER_GOLD_PUBLICATION_READY_ASSET_REF = Asset(
    WEATHER_GOLD_PUBLICATION_READY_ASSET
)


def run_dbt_phase(
    *,
    dbt_command: str,
    selector: str | None,
    include_project_vars: bool = True,
    snapshot_task_id: str | None = None,
    serving_as_of_task_id: str | None = None,
    threads: int | None = None,
    **context,
) -> dict[str, object]:
    """Run this DAG's dbt phase without importing another DAG entrypoint."""

    return run_weather_dbt_phase(
        dbt_command=dbt_command,
        selector=selector,
        include_project_vars=include_project_vars,
        snapshot_task_id=snapshot_task_id,
        serving_as_of_task_id=serving_as_of_task_id,
        threads=threads,
        context=context,
        dbt_executor=weather_dbt,
        dbt_project=DBT_PROJECT,
        dbt_bin=DBT_BIN,
        runner=subprocess.run,
        pipeline="weather-serving-snapshot",
        failure_exception=lambda retryable, message: (
            AirflowException(message) if retryable else AirflowFailException(message)
        ),
    )


def _serving_snapshot_dbt_task(task_id: str, dbt_command: str) -> PythonOperator:
    """Keep the regular Weather dbt failure/pool contract without a Bronze pin."""

    return PythonOperator(
        task_id=task_id,
        python_callable=run_dbt_phase,
        op_kwargs={
            "dbt_command": dbt_command,
            "selector": SERVING_SNAPSHOT_SELECTOR,
            "include_project_vars": True,
            "snapshot_task_id": None,
            "serving_as_of_task_id": SERVING_AS_OF_HOUR_TASK_ID,
            "threads": 2,
        },
        pool=TRINO_WEATHER_LEGACY_HEAVY_POOL,
        weight_rule="absolute",
        priority_weight=SERVING_SNAPSHOT_PRIORITY_WEIGHT,
        retries=1,
        retry_delay=DBT_RETRY_DELAY,
        on_failure_callback=record_weather_problem,
    )


def _current_refresh_run_results_path(**context) -> str | None:
    ti = context.get("ti") or context.get("task_instance")
    if ti is None:
        return None
    for task_id in reversed(REFRESH_DBT_TASK_IDS):
        try:
            result = ti.xcom_pull(task_ids=task_id)
        except Exception:  # noqa: BLE001 - try the earlier refresh phase
            result = None
        if isinstance(result, dict):
            candidate = result.get("run_results_path")
            if isinstance(candidate, (str, os.PathLike)) and os.path.exists(candidate):
                return os.fspath(candidate)
        try:
            candidate = ti.xcom_pull(
                task_ids=task_id,
                key=WEATHER_DBT_RUN_RESULTS_XCOM_KEY,
            )
        except Exception:  # noqa: BLE001 - preserve metrics teardown behavior
            candidate = None
        if isinstance(candidate, (str, os.PathLike)) and os.path.exists(candidate):
            return os.fspath(candidate)
    return None


def publish_dbt_run_metrics(**context) -> dict[str, object]:
    """Record this DAG's own run/test artifact without depending on full transform tasks."""

    run_results_path = _current_refresh_run_results_path(**context)
    if run_results_path is None:
        print("run_results.json 없음 — serving snapshot refresh 메트릭 적재 skip")
        return {"rows": 0, "skipped": True}
    target = (context.get("params") or {}).get("target")
    records = dump_dbt_run_results(run_results_path, domain=DOMAIN, target=target)
    print(
        "Weather serving snapshot refresh dbt 실행 메트릭 적재: "
        f"{len(records)} records (target={target})"
    )
    return {"rows": len(records), "skipped": False}


def mark_weather_serving_snapshot_ready(
    *, now: datetime | None = None, **context
) -> dict[str, str]:
    """Emit the existing Publisher asset without claiming a new Bronze snapshot."""

    serving_as_of_hour, serving_hour_state = weather_serving_as_of_hour_state(
        ti=context["ti"],
        now=now,
    )
    if serving_hour_state == "stale":
        raise AirflowFailException(
            "weather hourly serving snapshot completed after its frozen hour: "
            f"serving_as_of_hour={serving_as_of_hour}"
        )
    outlet_events = context.get("outlet_events")
    if outlet_events is None:
        raise RuntimeError("weather serving snapshot outlet event is unavailable")
    metadata = {
        "gold_dag_run_id": str(context.get("run_id") or ""),
        "refresh_kind": "hourly_serving_snapshot",
        "serving_as_of_hour": serving_as_of_hour,
    }
    outlet_events[WEATHER_GOLD_PUBLICATION_READY_ASSET_REF].extra = metadata
    return metadata


with DAG(
    dag_id="weather_serving_snapshot_refresh",
    description="KST hourly refresh of the four public Weather serving snapshots.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": DBT_RETRY_DELAY},
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "weather", "serving", "gold", "dbt", "hourly"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "weather", "requested_target": "{{ params.target }}"},
        on_failure_callback=record_weather_problem,
    )
    resolve_serving_as_of_hour = PythonOperator(
        task_id=SERVING_AS_OF_HOUR_TASK_ID,
        python_callable=resolve_weather_serving_as_of_hour,
        on_failure_callback=record_weather_problem,
    )
    run_snapshot = _serving_snapshot_dbt_task(
        "dbt_run_serving_snapshot_refresh", "run"
    )
    test_snapshot = _serving_snapshot_dbt_task(
        "dbt_test_serving_snapshot_refresh", "test"
    )
    mark_ready = PythonOperator(
        task_id="mark_weather_serving_snapshot_ready",
        python_callable=mark_weather_serving_snapshot_ready,
        outlets=[WEATHER_GOLD_PUBLICATION_READY_ASSET_REF],
        on_failure_callback=record_weather_problem,
    )
    publish_metrics = PythonOperator(
        task_id="publish_dbt_run_metrics",
        python_callable=publish_dbt_run_metrics,
        on_failure_callback=record_weather_problem,
    ).as_teardown(on_failure_fail_dagrun=False)

    # 공개 Gold 를 함께 쓰는 transform 이 도는 중이면 여기서 skip 한다. pool 은
    # 태스크 단위로만 직렬화해서 transform 의 run 과 test 사이에 끼어드는 것을
    # 막지 못한다 — 자세한 근거는 weather_serving_exclusion 모듈 docstring.
    guard_conflicting_transform = PythonOperator(
        task_id="guard_conflicting_weather_transform",
        python_callable=guard_serving_snapshot_refresh,
        on_failure_callback=record_weather_problem,
    )

    (
        validate_runtime
        >> guard_conflicting_transform
        >> resolve_serving_as_of_hour
        >> run_snapshot
        >> test_snapshot
        >> mark_ready
        >> publish_metrics
    )


enable_lineage_if_configured(dag)
