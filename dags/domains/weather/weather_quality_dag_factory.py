"""Factory for inert, internal-only forecast-quality Airflow DAGs."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

_DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if _DAG_DIR not in sys.path:
    sys.path.insert(0, _DAG_DIR)
_DAGS_ROOT = os.path.dirname(os.path.dirname(_DAG_DIR))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset

from common.assets import WEATHER_FORECAST_QUALITY_READY_ASSET
from common.errors.airflow import problem_failure_callback
from common.pools import TRINO_WEATHER_LEGACY_HEAVY_POOL
from common.runtime_guard import TARGET_CHOICES, default_target, validate_dev_runtime
from weather_ingest.kma_coordination import weather_heavy_pool_kwargs
import weather_dbt_execution as weather_dbt
from weather_dbt_runtime import DBT_RETRY_DELAY, DOMAIN, run_weather_dbt_phase
from weather_quality_publication import publish_quality_success, quality_catalog
from weather_quality_runtime import (
    QUALITY_BACKFILL_CONFIRMATION,
    QualityWindowError,
    quality_schedule,
    quality_window_from_dbt_vars,
    resolve_backfill_quality_window,
    resolve_daily_quality_window,
)


KST = ZoneInfo("Asia/Seoul")
DBT_BIN = weather_dbt.dbt_bin()
DBT_PROJECT = weather_dbt.dbt_project_dir()
QUALITY_DBT_QUERY_MAX_RUN_TIME = "15m"
QUALITY_DBT_TASK_TIMEOUT = timedelta(minutes=15)
QUALITY_PUBLICATION_TIMEOUT = timedelta(minutes=5)
QUALITY_DAGRUN_TIMEOUT = timedelta(minutes=45)
QUALITY_PRIORITY_WEIGHT = 10
QUALITY_WINDOW_TASK_ID = "resolve_quality_window"
QUALITY_CANDIDATE_SELECTOR = "ask_seoul_weather_quality_candidate"
QUALITY_PUBLISHED_SELECTOR = "ask_seoul_weather_quality_published"
WEATHER_FORECAST_QUALITY_READY_ASSET_REF = Asset(WEATHER_FORECAST_QUALITY_READY_ASSET)


def _record_weather_problem():
    return problem_failure_callback(domain=DOMAIN, dbt_project_dir=DBT_PROJECT)


def _now_kst() -> datetime:
    return datetime.now(KST)


def resolve_daily_quality_vars(*, now: datetime | None = None, **context: object) -> dict[str, str]:
    return resolve_daily_quality_window(
        now=now or _now_kst(),
        run_id=str(context.get("run_id") or ""),
    ).as_dbt_vars()


def resolve_backfill_quality_vars(*, now: datetime | None = None, **context: object) -> dict[str, str]:
    params = context.get("params") or {}
    if not isinstance(params, Mapping):
        raise QualityWindowError("weather quality backfill requires parameters")
    return resolve_backfill_quality_window(
        backfill_date=str(params.get("backfill_date") or ""),
        confirmation=str(params.get("confirmation") or ""),
        now=now or _now_kst(),
        run_id=str(context.get("run_id") or ""),
    ).as_dbt_vars()


def _quality_vars_from_context(context: Mapping[str, Any]) -> dict[str, str]:
    raw = context["ti"].xcom_pull(task_ids=QUALITY_WINDOW_TASK_ID)
    if not isinstance(raw, Mapping):
        raise AirflowFailException("weather quality evaluation window is unavailable")
    try:
        return quality_window_from_dbt_vars(raw).as_dbt_vars()
    except QualityWindowError as exc:
        raise AirflowFailException("weather quality evaluation window is invalid") from exc


def run_quality_dbt_phase(
    *,
    dbt_command: str,
    selector: str | None,
    include_quality_vars: bool,
    **context: Any,
) -> dict[str, object]:
    """Execute one bounded quality phase with its immutable XCom window."""

    return run_weather_dbt_phase(
        dbt_command=dbt_command,
        selector=selector,
        include_project_vars=True,
        snapshot_task_id=None,
        serving_as_of_task_id=None,
        threads=2,
        context=context,
        dbt_executor=weather_dbt,
        dbt_project=DBT_PROJECT,
        dbt_bin=DBT_BIN,
        runner=subprocess.run,
        pipeline="weather-forecast-quality",
        failure_exception=lambda retryable, message: (
            AirflowException(message) if retryable else AirflowFailException(message)
        ),
        additional_variables=(
            _quality_vars_from_context(context) if include_quality_vars else None
        ),
        environment_overrides={
            "TRINO_DBT_QUERY_MAX_RUN_TIME": QUALITY_DBT_QUERY_MAX_RUN_TIME
        },
    )


def _quality_cursor():
    import trino.dbapi

    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=quality_catalog(),
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    return connection, connection.cursor()


def publish_quality_manifest(**context: Any) -> dict[str, object]:
    """Commit SUCCESS only after candidate dbt build and reconciliation passed."""

    connection, cursor = _quality_cursor()
    try:
        result = publish_quality_success(
            cursor,
            dbt_vars=_quality_vars_from_context(context),
        )
    finally:
        connection.close()
    return asdict(result)


def mark_quality_ready(**context: Any) -> dict[str, str]:
    """Emit only the internal quality asset; it is not a serving publication."""

    metadata = {
        "quality_dag_run_id": str(context.get("run_id") or ""),
        "evaluation_run_id": _quality_vars_from_context(context)[
            "weather_quality_run_id"
        ],
    }
    outlet_events = context.get("outlet_events")
    if outlet_events is None:
        raise AirflowFailException("weather quality outlet event is unavailable")
    outlet_events[WEATHER_FORECAST_QUALITY_READY_ASSET_REF].extra = metadata
    return metadata


def _quality_dbt_task(
    *,
    task_id: str,
    selector: str | None,
    include_quality_vars: bool,
    record_failure: Any,
) -> PythonOperator:
    pool_kwargs = (
        weather_heavy_pool_kwargs(
            TRINO_WEATHER_LEGACY_HEAVY_POOL,
            pool_slots=2,
        )
        if task_id != "dbt_deps"
        else {}
    )
    return PythonOperator(
        task_id=task_id,
        python_callable=run_quality_dbt_phase,
        op_kwargs={
            "dbt_command": "deps" if task_id == "dbt_deps" else "build",
            "selector": selector,
            "include_quality_vars": include_quality_vars,
        },
        # Quality publication is intentionally off by default, but if enabled
        # it must not consume one of the transform branch slots while writing
        # its own candidate/manifest tables.
        **pool_kwargs,
        weight_rule="absolute",
        priority_weight=QUALITY_PRIORITY_WEIGHT,
        retries=1,
        retry_delay=DBT_RETRY_DELAY,
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
        execution_timeout=QUALITY_DBT_TASK_TIMEOUT,
        on_failure_callback=record_failure,
    )


def build_quality_dag(*, dag_id: str, backfill: bool) -> DAG:
    """Build daily or one-date backfill topology without any serving dependency."""

    record_failure = _record_weather_problem()
    params: dict[str, Param] = {
        "target": Param(
            default=default_target(),
            type="string",
            enum=list(TARGET_CHOICES),
            description="dbt target profile name.",
        )
    }
    if backfill:
        params |= {
            "backfill_date": Param(default="", type="string"),
            "confirmation": Param(default="", type="string"),
        }
    with DAG(
        dag_id=dag_id,
        description="Internal, bounded Weather forecast-quality Gold evaluation.",
        start_date=datetime(2026, 1, 1, tzinfo=KST),
        schedule=None if backfill else quality_schedule(),
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=True,
        dagrun_timeout=QUALITY_DAGRUN_TIMEOUT,
        default_args={"retries": 1, "retry_delay": DBT_RETRY_DELAY},
        params=params,
        tags=["ask_seoul", "weather", "quality", "gold", "internal"],
    ) as dag:
        validate_runtime = PythonOperator(
            task_id="validate_dev_runtime",
            python_callable=validate_dev_runtime,
            op_kwargs={"domain": "weather", "requested_target": "{{ params.target }}"},
            on_failure_callback=record_failure,
        )
        resolve_window = PythonOperator(
            task_id=QUALITY_WINDOW_TASK_ID,
            python_callable=(
                resolve_backfill_quality_vars if backfill else resolve_daily_quality_vars
            ),
            execution_timeout=timedelta(minutes=1),
            on_failure_callback=record_failure,
        )
        deps = _quality_dbt_task(
            task_id="dbt_deps",
            selector=None,
            include_quality_vars=False,
            record_failure=record_failure,
        )
        build_candidate = _quality_dbt_task(
            task_id="dbt_build_quality_candidate",
            selector=QUALITY_CANDIDATE_SELECTOR,
            include_quality_vars=True,
            record_failure=record_failure,
        )
        publish_manifest = PythonOperator(
            task_id="publish_quality_manifest",
            python_callable=publish_quality_manifest,
            **weather_heavy_pool_kwargs(
                TRINO_WEATHER_LEGACY_HEAVY_POOL,
                pool_slots=2,
            ),
            weight_rule="absolute",
            priority_weight=QUALITY_PRIORITY_WEIGHT,
            retries=0,
            execution_timeout=QUALITY_PUBLICATION_TIMEOUT,
            on_failure_callback=record_failure,
        )
        build_published = _quality_dbt_task(
            task_id="dbt_build_quality_published",
            selector=QUALITY_PUBLISHED_SELECTOR,
            include_quality_vars=True,
            record_failure=record_failure,
        )
        ready = PythonOperator(
            task_id="mark_weather_forecast_quality_ready",
            python_callable=mark_quality_ready,
            outlets=[WEATHER_FORECAST_QUALITY_READY_ASSET_REF],
            execution_timeout=timedelta(minutes=1),
            on_failure_callback=record_failure,
        )
        validate_runtime >> resolve_window >> deps >> build_candidate >> publish_manifest >> build_published >> ready
    return dag


__all__ = [
    "QUALITY_BACKFILL_CONFIRMATION",
    "QUALITY_CANDIDATE_SELECTOR",
    "QUALITY_DAGRUN_TIMEOUT",
    "QUALITY_DBT_QUERY_MAX_RUN_TIME",
    "QUALITY_DBT_TASK_TIMEOUT",
    "QUALITY_PRIORITY_WEIGHT",
    "QUALITY_PUBLISHED_SELECTOR",
    "QUALITY_WINDOW_TASK_ID",
    "WEATHER_FORECAST_QUALITY_READY_ASSET_REF",
    "build_quality_dag",
    "mark_quality_ready",
    "publish_quality_manifest",
    "resolve_backfill_quality_vars",
    "resolve_daily_quality_vars",
    "run_quality_dbt_phase",
]
