"""Disabled-by-default hourly KMA current-observation Bronze pipeline."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)
DOMAINS_DIR = os.path.dirname(DAG_DIR)
if DOMAINS_DIR not in sys.path:
    sys.path.insert(0, DOMAINS_DIR)
DAGS_ROOT_DIR = os.path.dirname(DOMAINS_DIR)
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.assets import WEATHER_OBSERVATION_BRONZE_ASSET  # noqa: E402
from common.pools import TRINO_WEATHER_HEAVY_POOL  # noqa: E402
from common.runtime_guard import validate_dev_runtime  # noqa: E402
from weather_ingest.common.runtime import trino_cursor  # noqa: E402
from weather_ingest.errors import (  # noqa: E402
    WeatherBronzeConfigurationError,
    WeatherCompletenessError,
)
from weather_ingest.kma import load_kma_grids  # noqa: E402
from weather_ingest.kma_coordination import (  # noqa: E402
    SqliteAttemptLedger,
    kma_api_pool_kwargs,
    shared_guards_enabled,
)
from weather_ingest.kma_observation import (  # noqa: E402
    KST,
    REQUIRED_CATEGORIES,
    SOURCE_ID,
    resolve_observation_slot,
)
from weather_ingest.kma_observation_bronze import (  # noqa: E402
    OBSERVATION_BRONZE_TABLE,
    append_observation_bronze_revisions,
    build_observation_bronze_rows,
    create_observation_bronze_table,
    load_observation_bronze_table,
    observation_grid_revisions,
    verify_observation_bronze_run_slot,
)
from weather_ingest.kma_observation_landing import (  # noqa: E402
    ObservationGrid,
    ObservationLandingBatch,
    ObservationLandingRequest,
    ObservationRunIdentity,
)
from weather_ingest.kma_observation_runtime import (  # noqa: E402
    build_observation_landing,
    build_observation_raw_store,
)


DAG_ID = "weather_ultra_srt_ncst_bronze"
SCHEDULE_ENV = "ASK_SEOUL_KMA_OBSERVATION_DAG_SCHEDULE"
EXPECTED_GRID_COUNT = 80
EXPECTED_ROW_COUNT = EXPECTED_GRID_COUNT * len(REQUIRED_CATEGORIES)
WEATHER_OBSERVATION_BRONZE_ASSET_REF = Asset(WEATHER_OBSERVATION_BRONZE_ASSET)


def observation_schedule() -> str | None:
    return os.getenv(SCHEDULE_ENV, "").strip() or None


def validate_observation_runtime() -> dict[str, object]:
    validate_dev_runtime(domain="weather")
    if not shared_guards_enabled():
        raise WeatherBronzeConfigurationError(
            "KMA observation shared guards must be enabled before execution"
        )
    SqliteAttemptLedger.from_environment()
    return {"shared_guards_enabled": True, "ledger_schema_version": 1}


def plan_observation_collection(*, now: datetime | None = None) -> dict[str, object]:
    base_date, base_time = resolve_observation_slot(
        now=now or datetime.now(timezone.utc)
    )
    grids = load_kma_grids(expected_grid_count=EXPECTED_GRID_COUNT)
    return {
        "source_id": SOURCE_ID,
        "base_date": base_date,
        "base_time": base_time,
        "grids": [{"nx": int(grid["nx"]), "ny": int(grid["ny"])} for grid in grids],
        "categories": list(REQUIRED_CATEGORIES),
        "expected_grid_count": EXPECTED_GRID_COUNT,
        "expected_row_count": EXPECTED_ROW_COUNT,
    }


def land_observation_raw(**context) -> dict[str, object]:
    plan = context["ti"].xcom_pull(task_ids="plan_observation_collection") or {}
    grids = tuple(
        ObservationGrid(nx=int(grid["nx"]), ny=int(grid["ny"]))
        for grid in plan.get("grids") or []
    )
    landing = build_observation_landing()
    batch = landing.collect(
        ObservationRunIdentity(
            dag_id=getattr(context.get("dag"), "dag_id", DAG_ID),
            run_id=str(context["run_id"]),
        ),
        ObservationLandingRequest(
            base_date=str(plan.get("base_date") or ""),
            base_time=str(plan.get("base_time") or ""),
            grids=grids,
        ),
    )
    return batch.to_xcom()


def load_observation_bronze(**context) -> dict[str, object]:
    landing_document = (
        context["ti"].xcom_pull(task_ids="land_observation_raw") or {}
    )
    batch = ObservationLandingBatch.from_xcom(landing_document)
    raw_store = build_observation_raw_store()
    rows = build_observation_bronze_rows(
        batch,
        read_raw=raw_store.read_bytes,
    )
    cursor, catalog, schema = trino_cursor()
    qualified_table = create_observation_bronze_table(
        cursor,
        catalog=catalog,
        schema=schema,
    )
    inserted = append_observation_bronze_revisions(
        load_observation_bronze_table(schema),
        rows,
    )
    return {
        **landing_document,
        "inserted": inserted,
        "qualified_table": qualified_table,
        "expected_grid_revisions": observation_grid_revisions(rows),
    }


def verify_observation_bronze(**context) -> int:
    loaded = context["ti"].xcom_pull(task_ids="load_observation_bronze") or {}
    cursor, catalog, schema = trino_cursor()
    qualified_table = str(
        loaded.get("qualified_table")
        or f"{catalog}.{schema}.{OBSERVATION_BRONZE_TABLE}"
    )
    return verify_observation_bronze_run_slot(
        cursor,
        qualified_table=qualified_table,
        observed_slot=str(loaded.get("observed_slot") or ""),
        expected_grid_revisions=loaded.get("expected_grid_revisions") or [],
    )


def publish_observation_bronze_asset(**context) -> dict[str, object]:
    verified_rows = context["ti"].xcom_pull(
        task_ids="verify_observation_bronze"
    )
    if int(verified_rows or 0) != EXPECTED_ROW_COUNT:
        raise WeatherCompletenessError(
            "KMA observation asset requires exactly 640 verified Bronze rows"
        )
    payload = {
        "source_id": SOURCE_ID,
        "verified_row_count": EXPECTED_ROW_COUNT,
        "dag_run_id": str(context.get("run_id") or ""),
    }
    outlet_events = context.get("outlet_events") or {}
    event = outlet_events.get(WEATHER_OBSERVATION_BRONZE_ASSET_REF)
    if event is not None:
        event.extra = payload
    return payload


with DAG(
    dag_id=DAG_ID,
    description="Collects 80-grid KMA getUltraSrtNcst Raw and exact Observation Bronze.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=observation_schedule(),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=40),
    tags=["ask_seoul", "weather", "observation", "bronze", "paused"],
) as dag:
    validate_runtime_task = PythonOperator(
        task_id="validate_observation_runtime",
        python_callable=validate_observation_runtime,
        execution_timeout=timedelta(minutes=1),
        retries=0,
    )
    plan_collection_task = PythonOperator(
        task_id="plan_observation_collection",
        python_callable=plan_observation_collection,
        execution_timeout=timedelta(minutes=1),
        retries=0,
    )
    land_raw_task = PythonOperator(
        task_id="land_observation_raw",
        python_callable=land_observation_raw,
        execution_timeout=timedelta(minutes=20),
        retries=0,
        **kma_api_pool_kwargs(),
    )
    load_bronze_task = PythonOperator(
        task_id="load_observation_bronze",
        python_callable=load_observation_bronze,
        pool=TRINO_WEATHER_HEAVY_POOL,
        pool_slots=1,
        execution_timeout=timedelta(minutes=6),
        retries=0,
    )
    verify_bronze_task = PythonOperator(
        task_id="verify_observation_bronze",
        python_callable=verify_observation_bronze,
        pool=TRINO_WEATHER_HEAVY_POOL,
        pool_slots=1,
        execution_timeout=timedelta(minutes=5),
        retries=0,
    )
    publish_asset_task = PythonOperator(
        task_id="publish_observation_bronze_asset",
        python_callable=publish_observation_bronze_asset,
        outlets=[WEATHER_OBSERVATION_BRONZE_ASSET_REF],
        execution_timeout=timedelta(minutes=1),
        retries=0,
    )

    (
        validate_runtime_task
        >> plan_collection_task
        >> land_raw_task
        >> load_bronze_task
        >> verify_bronze_task
        >> publish_asset_task
    )
