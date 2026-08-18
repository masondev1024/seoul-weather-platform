"""Airflow DAG: daily refresh of static Weather reference data via dbt.

행정동 차원과 place/coverage 매핑 seed 는 자주 바뀌지 않는데도, 예전에는
weather_vilage_fcst_transform 이 bronze 사이클마다(하루 8회) 이것들을 매번 다시
seed·build 했다. 80행짜리 정적 테이블에 test 하나가 20초씩 걸리는 구조라, 이
재작업만으로 사이클당 ~13분이 Trino 에 낭비됐다.

이 DAG 는 그 정적 참조 데이터를 하루 1회만 새로 만든다. transform 은 이제 이
테이블들을 ref 로 읽기만 하며, 없으면(참조 DAG 가 아직 안 돌았으면) dbt 가
자연스럽게 실패해 fail-closed 된다.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator

# 공통 패키지(dags/common)와 Weather 로컬 패키지 import 경로를 초기화한다.
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)

DAGS_ROOT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.pools import TRINO_WEATHER_LEGACY_HEAVY_POOL  # noqa: E402
from common.runmetrics import dump_dbt_run_results  # noqa: E402
from common.runtime_guard import (  # noqa: E402
    TARGET_CHOICES,
    default_target,
    validate_dev_runtime,
)
from weather_ingest.common.resources import DbtWorkload  # noqa: E402
import weather_dbt_execution as weather_dbt  # noqa: E402
from weather_dbt_runtime import (  # noqa: E402
    DBT_RETRY_DELAY,
    DOMAIN,
    WEATHER_DBT_RUN_RESULTS_XCOM_KEY,
    run_weather_dbt_phase,
)
from weather_lineage import enable_lineage_if_configured  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
DBT_BIN = weather_dbt.dbt_bin()
DBT_PROJECT = weather_dbt.dbt_project_dir()
record_weather_problem = problem_failure_callback(
    domain=DOMAIN,
    dbt_project_dir=DBT_PROJECT,
    dbt_run_results_xcom_key=WEATHER_DBT_RUN_RESULTS_XCOM_KEY,
)


@dataclass(frozen=True)
class DbtPhaseSpec:
    task_id: str
    dbt_command: str
    selector: str | None = None
    include_project_vars: bool = True
    workload: DbtWorkload = DbtWorkload.TRINO
    threads: int | None = 2


# transform 에서 옮겨온 정적 참조 phase. 순서·selector 는 그대로 유지한다.
# 이 목록이 곧 transform 에서 빠진 phase 집합이며, 둘의 합집합은 예전 transform
# 의 참조+데이터 phase 전체와 일치해야 한다(회귀 테스트로 고정).
DBT_PHASE_SPECS = (
    DbtPhaseSpec(
        "dbt_deps",
        "deps",
        include_project_vars=False,
        workload=DbtWorkload.LOCAL,
        threads=None,
    ),
    DbtPhaseSpec(
        "dbt_seed_asac_axes",
        "seed",
        "ask_seoul_weather_transform_asac_axes",
    ),
    DbtPhaseSpec(
        "dbt_run_common_admin_dong_dimension",
        "run",
        "ask_seoul_weather_transform_common_admin",
    ),
    DbtPhaseSpec(
        "dbt_test_common_admin_dong_dimension",
        "test",
        "ask_seoul_weather_transform_common_admin",
    ),
    DbtPhaseSpec(
        "dbt_seed_place_mapping",
        "seed",
        "ask_seoul_weather_transform_place_mapping",
    ),
    DbtPhaseSpec(
        "dbt_test_place_mapping_seed",
        "test",
        "ask_seoul_weather_transform_place_mapping",
    ),
    DbtPhaseSpec(
        "dbt_seed_coverage_grid",
        "seed",
        "ask_seoul_weather_transform_coverage_grid",
    ),
    DbtPhaseSpec(
        "dbt_test_coverage_grid_seed",
        "test",
        "ask_seoul_weather_transform_coverage_grid",
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


def reference_schedule() -> str | None:
    """정적 참조 데이터는 하루 1회(01:00 KST)면 충분하다.

    첫 bronze 수집(02:20 KST) 이전에 끝나도록 01:00 에 돈다. 테스트에서
    다른 스케줄을 강제할 수 있도록 환경변수 override 를 남겨둔다.
    """
    if "ASK_SEOUL_WEATHER_REFERENCE_DAG_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_WEATHER_REFERENCE_DAG_SCHEDULE"] or None
    return "0 1 * * *"


def run_dbt_phase(
    *,
    dbt_command: str,
    selector: str | None,
    include_project_vars: bool = True,
    threads: int | None = None,
    **context,
) -> dict[str, object]:
    """Run one dbt phase without a Bronze snapshot or serving-hour boundary.

    참조 데이터는 bronze snapshot 이나 serving as-of hour 로 필터하지 않으므로
    두 task_id 를 None 으로 넘긴다(run_weather_dbt_phase 는 None 을 허용한다).
    """

    return run_weather_dbt_phase(
        dbt_command=dbt_command,
        selector=selector,
        include_project_vars=include_project_vars,
        snapshot_task_id=None,
        serving_as_of_task_id=None,
        threads=threads,
        context=context,
        dbt_executor=weather_dbt,
        dbt_project=DBT_PROJECT,
        dbt_bin=DBT_BIN,
        runner=subprocess.run,
        pipeline="weather-reference",
        failure_exception=lambda retryable, message: (
            AirflowException(message) if retryable else AirflowFailException(message)
        ),
    )


def dbt_task(spec: DbtPhaseSpec) -> PythonOperator:
    operator_kwargs = {
        "task_id": spec.task_id,
        "python_callable": run_dbt_phase,
        "op_kwargs": {
            "dbt_command": spec.dbt_command,
            "selector": spec.selector,
            "include_project_vars": spec.include_project_vars,
            "threads": spec.threads,
        },
        "weight_rule": "absolute",
        "retries": 1,
        "retry_delay": DBT_RETRY_DELAY,
        "on_failure_callback": record_weather_problem,
    }
    if spec.workload is DbtWorkload.TRINO:
        operator_kwargs["pool"] = TRINO_WEATHER_LEGACY_HEAVY_POOL
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
        print(f"run_results.json 없음 — 메트릭 적재 skip: {resolved_path}")
        return {"rows": 0, "skipped": True}
    target = (context.get("params") or {}).get("target")
    records = dump_dbt_run_results(resolved_path, domain=DOMAIN, target=target)
    print(
        f"dbt 참조 데이터 메트릭 적재: {len(records)} records "
        f"(domain={DOMAIN}, target={target})"
    )
    return {"rows": len(records), "skipped": False}


with DAG(
    dag_id="weather_reference_data_refresh",
    description="Daily rebuild of static Weather reference seeds and the admin-dong dimension.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=reference_schedule(),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": DBT_RETRY_DELAY},
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "weather", "reference", "seed", "dbt"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "weather", "requested_target": "{{ params.target }}"},
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
        *dbt_tasks_in_order,
        publish_dbt_metrics,
    ]
    for upstream, downstream in zip(pipeline_tasks, pipeline_tasks[1:]):
        upstream >> downstream


enable_lineage_if_configured(dag)
