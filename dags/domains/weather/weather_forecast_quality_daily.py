"""Daily internal Weather forecast-quality Gold publication DAG."""

from __future__ import annotations

import os
import subprocess
import sys
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

from common.assets import WEATHER_FORECAST_QUALITY_READY_ASSET  # noqa: E402
from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.pools import TRINO_WEATHER_HEAVY_POOL  # noqa: E402
from common.runtime_guard import (  # noqa: E402
    TARGET_CHOICES,
    default_target,
    validate_dev_runtime,
)
import weather_dbt_execution as weather_dbt  # noqa: E402
from weather_dbt_runtime import (  # noqa: E402
    DBT_RETRY_DELAY,
    DOMAIN,
    WEATHER_DBT_CONTRACT_VARS,
    WEATHER_DBT_RUN_RESULTS_XCOM_KEY,
    run_weather_dbt_phase,
)
from weather_ingest.common.runtime import trino_cursor  # noqa: E402
from weather_lineage import enable_lineage_if_configured  # noqa: E402
from weather_quality_publication import (  # noqa: E402
    DAG_ID as QUALITY_PUBLICATION_DAG_ID,
    QualityPublicationTarget,
    begin_quality_publication,
    publish_quality_success,
    record_failed_publication,
)
from weather_quality_runtime import (  # noqa: E402
    quality_schedule,
    resolve_daily_quality_window,
    window_from_payload,
    window_payload,
)


KST = ZoneInfo("Asia/Seoul")
DBT_BIN = weather_dbt.dbt_bin()
DBT_PROJECT = weather_dbt.dbt_project_dir()
QUALITY_SELECTOR = "ask_seoul_weather_quality_candidate"
QUALITY_WINDOW_TASK_ID = "resolve_forecast_quality_window"
QUALITY_READY_ASSET_REF = Asset(WEATHER_FORECAST_QUALITY_READY_ASSET)
TRINO_DBT_QUERY_MAX_RUN_TIME = "15m"
QUALITY_DBT_ENV = {"TRINO_DBT_QUERY_MAX_RUN_TIME": TRINO_DBT_QUERY_MAX_RUN_TIME}
QUALITY_DBT_ENV_ALLOWLIST = frozenset(QUALITY_DBT_ENV)
QUALITY_PRIORITY_WEIGHT = 10
QUALITY_TASK_TIMEOUT = timedelta(minutes=15)
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


def resolve_forecast_quality_window(**context) -> dict[str, object]:
    window = resolve_daily_quality_window(
        now=_frozen_context_now(context),
        run_id=str(context.get("run_id") or ""),
    )
    return window_payload(window)


def _frozen_context_now(context: dict[str, object]) -> datetime:
    candidate = context.get("data_interval_end") or context.get("logical_date")
    if isinstance(candidate, datetime):
        if candidate.tzinfo is None or candidate.utcoffset() is None:
            raise AirflowFailException(
                "weather forecast-quality context timestamp must be timezone-aware"
            )
        return candidate
    return datetime.now(KST)


def _payload_from_context(context: dict[str, object]):
    return context["ti"].xcom_pull(task_ids=QUALITY_WINDOW_TASK_ID)


def _window_from_context(context: dict[str, object]):
    payload = _payload_from_context(context)
    return window_from_payload(payload)


def _quality_publication_cursor_and_target():
    cursor, catalog, schema = trino_cursor()
    return cursor, QualityPublicationTarget(catalog=catalog, schema=schema)


def begin_forecast_quality_publication(**context) -> dict[str, object]:
    window = _window_from_context(context)
    cursor, target = _quality_publication_cursor_and_target()
    result = begin_quality_publication(
        cursor,
        window=window,
        target=target,
        dag_id=QUALITY_PUBLICATION_DAG_ID,
    )
    return {"evaluation_run_id": result.evaluation_run_id, "status": result.status}


def publish_forecast_quality_success(**context) -> dict[str, object]:
    window = _window_from_context(context)
    cursor, target = _quality_publication_cursor_and_target()
    result = publish_quality_success(
        cursor,
        window=window,
        target=target,
        dag_id=QUALITY_PUBLICATION_DAG_ID,
    )
    return {"evaluation_run_id": result.evaluation_run_id, "status": result.status}


def record_forecast_quality_failed_publication(context) -> None:
    try:
        window = _window_from_context(context)
        cursor, target = _quality_publication_cursor_and_target()
        record_failed_publication(
            cursor,
            window=window,
            target=target,
            dag_id=QUALITY_PUBLICATION_DAG_ID,
        )
    except Exception as exc:  # noqa: BLE001 - best-effort diagnostic callback
        print(
            "weather forecast-quality FAILED publication marker skipped: "
            f"{type(exc).__name__}"
        )
    record_weather_problem(context)


def run_quality_dbt_phase(
    *,
    dbt_command: str,
    selector: str | None,
    include_project_vars: bool,
    threads: int | None = 2,
    **context,
) -> dict[str, object]:
    payload = _payload_from_context(context)
    window_from_payload(payload)
    quality_vars = dict(payload["dbt_vars"])
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
        pipeline="weather-forecast-quality",
        failure_exception=lambda retryable, message: (
            AirflowException(message) if retryable else AirflowFailException(message)
        ),
        extra_project_vars=quality_vars,
        allowed_extra_project_var_names=frozenset(quality_vars),
        extra_env=QUALITY_DBT_ENV,
        allowed_extra_env_names=QUALITY_DBT_ENV_ALLOWLIST,
        expected_extra_env_values=QUALITY_DBT_ENV,
    )


def _quality_dbt_task(task_id: str, dbt_command: str, selector: str | None) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=run_quality_dbt_phase,
        op_kwargs={
            "dbt_command": dbt_command,
            "selector": selector,
            "include_project_vars": dbt_command != "deps",
            "threads": 2 if dbt_command != "deps" else None,
        },
        pool=TRINO_WEATHER_HEAVY_POOL,
        pool_slots=1,
        weight_rule="absolute",
        priority_weight=QUALITY_PRIORITY_WEIGHT,
        retries=1,
        retry_delay=DBT_RETRY_DELAY,
        execution_timeout=QUALITY_TASK_TIMEOUT,
        on_failure_callback=record_forecast_quality_failed_publication,
    )


with DAG(
    dag_id="weather_forecast_quality_daily",
    description="Daily internal Weather forecast-quality Gold reconciliation.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=quality_schedule(),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    is_paused_upon_creation=True,
    default_args={"retries": 1, "retry_delay": DBT_RETRY_DELAY},
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "weather", "quality", "gold", "dbt", "internal"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "weather", "requested_target": "{{ params.target }}"},
        on_failure_callback=record_weather_problem,
    )
    resolve_window = PythonOperator(
        task_id=QUALITY_WINDOW_TASK_ID,
        python_callable=resolve_forecast_quality_window,
        retries=0,
        on_failure_callback=record_weather_problem,
    )
    begin_publication = PythonOperator(
        task_id="begin_forecast_quality_publication",
        python_callable=begin_forecast_quality_publication,
        pool=TRINO_WEATHER_HEAVY_POOL,
        pool_slots=1,
        weight_rule="absolute",
        priority_weight=QUALITY_PRIORITY_WEIGHT,
        retries=0,
        execution_timeout=QUALITY_TASK_TIMEOUT,
        on_failure_callback=record_forecast_quality_failed_publication,
    )
    dbt_deps = _quality_dbt_task("dbt_deps", "deps", None)
    dbt_build_quality_candidate = _quality_dbt_task(
        "dbt_build_quality_candidate",
        "build",
        QUALITY_SELECTOR,
    )
    publish_success = PythonOperator(
        task_id="publish_forecast_quality_success",
        python_callable=publish_forecast_quality_success,
        outlets=[QUALITY_READY_ASSET_REF],
        pool=TRINO_WEATHER_HEAVY_POOL,
        pool_slots=1,
        weight_rule="absolute",
        priority_weight=QUALITY_PRIORITY_WEIGHT,
        retries=0,
        execution_timeout=QUALITY_TASK_TIMEOUT,
        on_failure_callback=record_forecast_quality_failed_publication,
    )

    (
        validate_runtime
        >> resolve_window
        >> begin_publication
        >> dbt_deps
        >> dbt_build_quality_candidate
        >> publish_success
    )


enable_lineage_if_configured(dag)
