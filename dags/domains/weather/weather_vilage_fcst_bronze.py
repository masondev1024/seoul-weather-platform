"""Airflow entrypoint for KMA raw landing and Bronze publication."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from hashlib import sha256

from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowSkipException
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

from common.assets import WEATHER_BRONZE_ASSET  # noqa: E402
from common.collection_slots.contract import ExpectedSlot  # noqa: E402
from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.ops.product_observability import (  # noqa: E402
    record_domain_stage_event,
    record_product_event,
)
from common.runtime_guard import validate_dev_runtime  # noqa: E402
from weather_ingest.bronze import (  # noqa: E402
    append_kma_bronze_row_batches_pyiceberg,
    create_kma_bronze_table,
    verify_kma_bronze_runtime as verify_kma_bronze_rows,
)
from weather_ingest.bronze_batch import (  # noqa: E402
    BronzeLoadPorts,
    load_kma_bronze_batch,
)
from weather_ingest.bronze_dag_support import (  # noqa: E402
    BACKFILL_DAG_ID,
    DAG_ID,
    DISCORD_GREEN as DISCORD_GREEN,
    DISCORD_RED as DISCORD_RED,
    KMA_PUBLISH_CRON_KST as KMA_PUBLISH_CRON_KST,
    KMA_RAW_TASK_ID_LAND,
    KMA_RAW_TASK_ID_LAND_FROM_KEYS,
    LOGGER as LOGGER,
    RECOLLECT_DAG_ID,
    RECOVERY_HINT_MAX_CHARS as RECOVERY_HINT_MAX_CHARS,
    RECOVERY_HINT_MAX_KEYS as RECOVERY_HINT_MAX_KEYS,
    WEATHER_DISCORD_WEBHOOK_ENV as WEATHER_DISCORD_WEBHOOK_ENV,
    current_dag_id,
    dag_run_conf,
    discord_report_date as discord_report_date,
    fail_fast_weather_bronze,
    format_raw_object_keys_for_recovery as format_raw_object_keys_for_recovery,
    kma_dag_schedule,
    notify_weather_bronze_success as notify_weather_bronze_success,
    pull_kma_raw_result,
    raw_object_keys_from_conf,
    raw_object_page_no as raw_object_page_no,
    send_weather_discord as send_weather_discord,
    short_text as short_text,
    stage_name as stage_name,
    target_name as target_name,
)
from weather_ingest.common.resources import TRINO_HEAVY_POOL  # noqa: E402
from weather_ingest.collection_slots import (  # noqa: E402
    weather_collection_failure_outcomes,
    weather_collection_success_outcomes,
)
from weather_ingest.common.runtime import (  # noqa: E402
    download_raw_object,
    trino_cursor,
)
from weather_ingest.kma import (  # noqa: E402
    KST,
    SOURCE_ID,
    kma_base_datetime_from_conf,
    kma_num_of_rows,
    load_kma_grids,
    resolve_kma_base_datetime,
)
from weather_ingest.kma_coordination import kma_api_pool_kwargs  # noqa: E402
from weather_ingest.landing import (  # noqa: E402
    KmaGrid,
    KmaLandingRequest,
    RunIdentity,
)
from weather_ingest.run_manifest import WeatherRun  # noqa: E402
from weather_ingest.runtime import (  # noqa: E402
    build_weather_collection_slot_receipt_ports,
    build_weather_landing,
    build_weather_manifest,
    discard_weather_raw_payload,
    read_weather_raw_payload,
    weather_raw_manifest_is_verified,
)
from weather_lineage import enable_lineage_if_configured  # noqa: E402


EXPECTED_RAW_OBJECT_COUNT_KEY = "expected_raw_object_count"
WEATHER_COLLECTION_SLOT_PLAN_TASK_ID = "plan_weather_collection_slots"
WEATHER_COLLECTION_SLOT_SUCCESS_TASK_ID = "record_weather_collection_slot_success"

record_weather_problem = problem_failure_callback(
    domain="weather", source_system=SOURCE_ID
)
WEATHER_BRONZE_ASSET_REF = Asset(WEATHER_BRONZE_ASSET)
record_weather_raw_product_failure = record_domain_stage_event(
    "weather", "raw", status="failed"
)
record_weather_bronze_product_failure = record_domain_stage_event(
    "weather", "bronze", status="failed"
)


def _non_negative_row_count(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def record_weather_raw_product_event(context: dict) -> dict:
    row_count = None
    try:
        raw_result = pull_kma_raw_result(context) or {}
        raw_objects = raw_result.get("raw_objects")
        if isinstance(raw_objects, list) and raw_objects:
            counts = [
                _non_negative_row_count(item.get("row_count"))
                if isinstance(item, dict)
                else None
                for item in raw_objects
            ]
            if all(value is not None for value in counts):
                row_count = sum(value for value in counts if value is not None)
    except Exception:
        row_count = None
    return record_product_event(
        context,
        domain="weather",
        layer="raw",
        row_count=row_count,
        rows_source="raw_manifest" if row_count is not None else "not_observed",
    )


def record_weather_bronze_product_event(context: dict) -> dict:
    row_count = None
    try:
        task_instance = context.get("ti") or context.get("task_instance")
        row_count = _non_negative_row_count(
            task_instance.xcom_pull(task_ids="verify_kma_bronze_runtime")
        )
    except Exception:
        row_count = None
    return record_product_event(
        context,
        domain="weather",
        layer="bronze",
        row_count=row_count,
        rows_source=(
            "bronze_run_manifest" if row_count is not None else "not_observed"
        ),
    )


def _base_datetime_for_context(context: dict) -> tuple[str, str]:
    configured = kma_base_datetime_from_conf(dag_run_conf(context))
    if configured is not None:
        return configured
    task_instance = context.get("ti") or context.get("task_instance")
    if task_instance is not None:
        try:
            plan = task_instance.xcom_pull(
                task_ids=WEATHER_COLLECTION_SLOT_PLAN_TASK_ID
            )
        except Exception:
            plan = None
        if isinstance(plan, dict):
            planned = kma_base_datetime_from_conf(plan)
            if planned is not None:
                return planned
    return resolve_kma_base_datetime()


def _configured_kma_grids() -> tuple[KmaGrid, ...]:
    return tuple(
        KmaGrid(str(grid["place_id"]), int(grid["nx"]), int(grid["ny"]))
        for grid in load_kma_grids()
    )


def plan_weather_collection_slots(**context) -> dict:
    base_date, base_time = _base_datetime_for_context(context)
    grids = _configured_kma_grids()
    slot_receipts, slots_for_issue = build_weather_collection_slot_receipt_ports()
    slots = tuple(slots_for_issue(base_date, base_time, grids))
    for slot in slots:
        slot_receipts.record_expected(slot)
    return {
        "base_date": base_date,
        "base_time": base_time,
        "slots": [slot.to_create_kwargs() for slot in slots],
    }


def _weather_slots_from_plan(context: dict) -> tuple[ExpectedSlot, ...]:
    task_instance = context.get("ti") or context.get("task_instance")
    if task_instance is None:
        raise AirflowFailException("weather collection-slot plan task instance is unavailable")
    plan = task_instance.xcom_pull(task_ids=WEATHER_COLLECTION_SLOT_PLAN_TASK_ID)
    if not isinstance(plan, dict):
        raise AirflowFailException("weather collection-slot plan is missing")
    entries = plan.get("slots")
    if not isinstance(entries, list):
        raise AirflowFailException("weather collection-slot plan slots are invalid")
    try:
        return tuple(ExpectedSlot.create(**entry) for entry in entries)
    except (TypeError, ValueError) as exc:
        raise AirflowFailException("weather collection-slot plan is invalid") from exc


def _raw_result_manifest_fields(raw_result: object) -> tuple[str | None, int | None]:
    if not isinstance(raw_result, dict):
        return None, None
    manifest_key = raw_result.get("manifest_key")
    raw_objects = raw_result.get("raw_objects")
    return (
        manifest_key if isinstance(manifest_key, str) and manifest_key else None,
        len(raw_objects) if isinstance(raw_objects, list) else None,
    )


def record_weather_collection_slot_success(**context) -> int:
    slots = _weather_slots_from_plan(context)
    if not slots:
        return 0
    task_instance = context["ti"]
    raw_result = pull_kma_raw_result(context)
    ingest_result = task_instance.xcom_pull(task_ids="load_kma_bronze") or {}
    manifest_key, raw_object_count = _raw_result_manifest_fields(raw_result)
    raw_manifest_verified = weather_raw_manifest_is_verified(
        raw_result,
        dag_run_id=context["run_id"],
        slots=slots,
    )
    slot_receipts, _ = build_weather_collection_slot_receipt_ports()
    if not bool(ingest_result.get("is_publishable", True)):
        outcomes = weather_collection_failure_outcomes(
            slots,
            raw_manifest_key=manifest_key,
            raw_object_count=raw_object_count,
            raw_manifest_verified=raw_manifest_verified,
            event_at=datetime.now(timezone.utc),
            dag_id=current_dag_id(context),
            dag_run_id=context["run_id"],
            task_id="verify_kma_bronze_runtime",
        )
    else:
        if not raw_manifest_verified:
            raise AirflowFailException(
                "weather collection-slot success requires a verified raw manifest"
            )
        if not isinstance(raw_result, dict):
            raise AirflowFailException("weather collection-slot raw result is invalid")
        verified_rows = task_instance.xcom_pull(task_ids="verify_kma_bronze_runtime")
        outcomes = weather_collection_success_outcomes(
            slots,
            raw_manifest_key=manifest_key,
            raw_objects=raw_result.get("raw_objects") or (),
            verified_rows=verified_rows,
            event_at=datetime.now(timezone.utc),
            dag_id=current_dag_id(context),
            dag_run_id=context["run_id"],
            task_id="verify_kma_bronze_runtime",
        )
    for outcome in outcomes:
        slot_receipts.record_outcome(outcome)
    return len(outcomes)


def record_weather_collection_slot_failure(context: dict) -> None:
    """Write best-effort failed-slot evidence without masking the original task error."""
    try:
        slots = _weather_slots_from_plan(context)
        if not slots:
            return
        raw_result = pull_kma_raw_result(context)
        manifest_key, raw_object_count = _raw_result_manifest_fields(raw_result)
        raw_manifest_verified = weather_raw_manifest_is_verified(
            raw_result,
            dag_run_id=context["run_id"],
            slots=slots,
        )
        slot_receipts, _ = build_weather_collection_slot_receipt_ports()
        task_instance = context.get("ti") or context.get("task_instance")
        outcomes = weather_collection_failure_outcomes(
            slots,
            raw_manifest_key=manifest_key,
            raw_object_count=raw_object_count,
            raw_manifest_verified=raw_manifest_verified,
            event_at=datetime.now(timezone.utc),
            dag_id=current_dag_id(context),
            dag_run_id=context["run_id"],
            task_id=getattr(task_instance, "task_id", "unknown"),
        )
        for outcome in outcomes:
            slot_receipts.record_outcome(outcome)
    except Exception as exc:
        LOGGER.warning(
            "Weather collection-slot failed outcome write failed: %s",
            type(exc).__name__,
        )


@fail_fast_weather_bronze
def land_kma_raw(**context) -> dict:
    conf = dag_run_conf(context)
    base_date, base_time = _base_datetime_for_context(context)
    request = KmaLandingRequest(
        base_date=base_date,
        base_time=base_time,
        grids=_configured_kma_grids(),
        num_of_rows=kma_num_of_rows(),
    )
    batch = build_weather_landing().collect(
        RunIdentity(
            current_dag_id(context),
            context["run_id"],
            landing_load_date=(
                str(conf["load_date"]) if conf.get("load_date") is not None else None
            ),
        ),
        request,
    )
    return batch.to_xcom()


@fail_fast_weather_bronze
def land_kma_raw_object_keys(**context) -> dict:
    conf = dag_run_conf(context)
    grids = tuple(
        KmaGrid(str(grid["place_id"]), int(grid["nx"]), int(grid["ny"]))
        for grid in load_kma_grids()
    )
    return (
        build_weather_landing()
        .replay(
            raw_object_keys_from_conf(context),
            grids=grids,
            run=RunIdentity(
                current_dag_id(context),
                context["run_id"],
                landing_load_date=(
                    str(conf["load_date"])
                    if conf.get("load_date") is not None
                    else None
                ),
            ),
        )
        .to_xcom()
    )


@fail_fast_weather_bronze
def load_kma_bronze(**context) -> dict:
    return load_kma_bronze_batch(
        raw_result=pull_kma_raw_result(context),
        dag_run_id=context["run_id"],
        allow_partial_pages=bool(dag_run_conf(context).get("allow_partial_pages")),
        expected_raw_object_count_key=EXPECTED_RAW_OBJECT_COUNT_KEY,
        ports=BronzeLoadPorts(
            open_trino=trino_cursor,
            ensure_table=create_kma_bronze_table,
            download=download_raw_object,
            append_batches=append_kma_bronze_row_batches_pyiceberg,
            read_payload=lambda raw_object: read_weather_raw_payload(
                raw_object, download=download_raw_object
            ),
            discard_payload=discard_weather_raw_payload,
        ),
    )


def record_kma_run_started(**context) -> str:
    return build_weather_manifest().start(
        WeatherRun(current_dag_id(context), context["run_id"]),
        expected_raw_objects=len(load_kma_grids()),
    )


def record_kma_backfill_run_started(**context) -> str:
    return build_weather_manifest().start(
        WeatherRun(current_dag_id(context), context["run_id"]),
        expected_raw_objects=len(raw_object_keys_from_conf(context)),
    )


def record_kma_run_failed(context) -> None:
    try:
        raw_result = pull_kma_raw_result(context)
        raw_keys = raw_result.get("raw_object_keys") or []
        task_instance = context.get("ti") or context.get("task_instance")
        error = context.get("exception") or RuntimeError("Airflow task failed")
        build_weather_manifest().fail(
            WeatherRun(current_dag_id(context), context["run_id"]),
            task_id=getattr(task_instance, "task_id", "unknown"),
            error=error,
            expected_raw_objects=(
                int(raw_result[EXPECTED_RAW_OBJECT_COUNT_KEY])
                if raw_result.get(EXPECTED_RAW_OBJECT_COUNT_KEY) is not None
                else (len(raw_keys) or None)
            ),
            actual_raw_objects=(len(raw_keys) or None),
        )
    except Exception as exc:
        print(f"Failed to record KMA run manifest failure: {type(exc).__name__}")


def record_and_notify_kma_run_failed(context) -> None:
    record_kma_run_failed(context)
    record_weather_collection_slot_failure(context)


@fail_fast_weather_bronze
def verify_kma_bronze_runtime(**context) -> int:
    ingest_result = context["ti"].xcom_pull(task_ids="load_kma_bronze") or {}
    verified_rows = verify_kma_bronze_rows(
        raw_object_keys=ingest_result["raw_object_keys"],
        dag_run_id=context["run_id"],
        expected_rows=int(ingest_result["inserted"]),
        expected_raw_objects=int(ingest_result[EXPECTED_RAW_OBJECT_COUNT_KEY]),
    )
    build_weather_manifest().complete(
        WeatherRun(current_dag_id(context), context["run_id"]),
        expected_rows=int(ingest_result["expected_rows"]),
        actual_rows=verified_rows,
        expected_raw_objects=int(ingest_result[EXPECTED_RAW_OBJECT_COUNT_KEY]),
        actual_raw_objects=len(ingest_result["raw_object_keys"]),
        is_publishable=bool(ingest_result.get("is_publishable", True)),
    )
    return verified_rows


def publish_weather_bronze_asset(**context) -> str:
    ingest_result = context["ti"].xcom_pull(task_ids="load_kma_bronze") or {}
    if not bool(ingest_result.get("is_publishable", True)):
        raise AirflowSkipException("weather Bronze run is not publishable")

    raw_result = pull_kma_raw_result(context) or {}
    raw_objects = raw_result.get("raw_objects") or []
    if not raw_objects:
        raise AirflowFailException(
            "weather Bronze asset event requires at least one raw object"
        )

    collected_at_values = [str(item["collected_at"]) for item in raw_objects]
    event_at = max(
        collected_at_values,
        key=lambda value: datetime.fromisoformat(value.replace("Z", "+00:00")),
    )
    payload_hashes = sorted(
        str(item["raw_hash"])
        for item in raw_objects
        if item.get("raw_hash")
    )
    if not payload_hashes:
        raise AirflowFailException(
            "weather Bronze asset event requires raw payload hashes"
        )
    payload_hash = (
        payload_hashes[0]
        if len(payload_hashes) == 1
        else sha256("|".join(payload_hashes).encode("utf-8")).hexdigest()
    )
    outlet_events = context.get("outlet_events")
    if outlet_events is None:
        raise AirflowFailException("weather Bronze outlet event is unavailable")
    outlet_event = outlet_events[WEATHER_BRONZE_ASSET_REF]

    event_datetime = datetime.fromisoformat(event_at.replace("Z", "+00:00"))
    outlet_event.extra = {
        "source_id": SOURCE_ID,
        "bronze_run_id": context["run_id"],
        "bronze_dag_run_id": context["run_id"],
        "event_at": event_at,
        "load_date": event_datetime.astimezone(KST).date().isoformat(),
        "row_count": int(ingest_result["inserted"]),
        "payload_hash": payload_hash,
        "is_publishable": True,
    }
    return context["run_id"]


def build_kma_bronze_dag(
    dag_id: str, schedule: str | None, description: str, tags: list[str]
):
    with DAG(
        dag_id=dag_id,
        description=description,
        start_date=datetime(2026, 1, 1, tzinfo=KST),
        schedule=schedule,
        catchup=False,
        max_active_runs=1,
        # dagrun_timeout(런): 한 run 이 이 시간을 넘으면 실패 처리해 단일 active 슬롯을
        # 무한정 점유하지 못하게 한다. 2026-07-20 OOM 인시던트에서 load_kma_bronze 가
        # ~4시간 running 으로 매달려 후속 스케줄을 막았다. 최악 land(~19분)+재시도보다는
        # 넉넉하고, 3시간 스케줄 간격보다는 짧게 잡아 연속 run 이 겹치지 않게 한다.
        dagrun_timeout=timedelta(minutes=60),
        on_failure_callback=record_and_notify_kma_run_failed,
        tags=tags,
    ) as built_dag:
        validate_runtime = PythonOperator(
            task_id="validate_dev_runtime",
            python_callable=validate_dev_runtime,
            op_kwargs={"domain": "weather"},
            on_failure_callback=[record_weather_problem],
        )

        start_manifest = PythonOperator(
            task_id="record_kma_run_started",
            python_callable=record_kma_run_started,
            on_failure_callback=record_weather_problem,
        )

        plan_collection_slots = PythonOperator(
            task_id=WEATHER_COLLECTION_SLOT_PLAN_TASK_ID,
            python_callable=plan_weather_collection_slots,
            on_failure_callback=[
                record_weather_collection_slot_failure,
                record_weather_problem,
            ],
        )

        land_raw = PythonOperator(
            task_id=KMA_RAW_TASK_ID_LAND,
            python_callable=land_kma_raw,
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            on_failure_callback=[
                record_and_notify_kma_run_failed,
                record_weather_problem,
                record_weather_raw_product_failure,
            ],
            on_success_callback=record_weather_raw_product_event,
            **kma_api_pool_kwargs(),
        )

        load_bronze = PythonOperator(
            task_id="load_kma_bronze",
            python_callable=load_kma_bronze,
            pool=TRINO_HEAVY_POOL,
            # Bronze table publication is an exclusive writer in the shared
            # Weather lane; never overlap it with a transform branch.
            pool_slots=2,
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            on_failure_callback=[
                record_and_notify_kma_run_failed,
                record_weather_problem,
            ],
        )

        verify_bronze = PythonOperator(
            task_id="verify_kma_bronze_runtime",
            python_callable=verify_kma_bronze_runtime,
            pool=TRINO_HEAVY_POOL,
            pool_slots=2,
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            on_failure_callback=[
                record_and_notify_kma_run_failed,
                record_weather_problem,
                record_weather_bronze_product_failure,
            ],
            on_success_callback=record_weather_bronze_product_event,
        )
        record_collection_slot_success = PythonOperator(
            task_id=WEATHER_COLLECTION_SLOT_SUCCESS_TASK_ID,
            python_callable=record_weather_collection_slot_success,
            on_failure_callback=[
                record_and_notify_kma_run_failed,
                record_weather_problem,
            ],
        )
        publish_bronze_asset = PythonOperator(
            task_id="publish_weather_bronze_asset",
            python_callable=publish_weather_bronze_asset,
            outlets=[WEATHER_BRONZE_ASSET_REF],
        )

        (
            validate_runtime
            >> start_manifest
            >> plan_collection_slots
            >> land_raw
            >> load_bronze
            >> verify_bronze
            >> record_collection_slot_success
            >> publish_bronze_asset
        )
    return enable_lineage_if_configured(built_dag)


def build_kma_bronze_backfill_dag():
    with DAG(
        dag_id=BACKFILL_DAG_ID,
        description="Loads existing KMA getVilageFcst raw_object_keys into Iceberg bronze without API calls.",
        start_date=datetime(2026, 1, 1, tzinfo=KST),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        on_failure_callback=record_kma_run_failed,
        tags=["ask_seoul", "kma", "bronze", "backfill", "r2", "iceberg"],
    ) as built_dag:
        validate_runtime = PythonOperator(
            task_id="validate_dev_runtime",
            python_callable=validate_dev_runtime,
            op_kwargs={"domain": "weather"},
            on_failure_callback=[record_weather_problem],
        )

        start_manifest = PythonOperator(
            task_id="record_kma_run_started",
            python_callable=record_kma_backfill_run_started,
            on_failure_callback=record_weather_problem,
        )

        land_raw = PythonOperator(
            task_id=KMA_RAW_TASK_ID_LAND_FROM_KEYS,
            python_callable=land_kma_raw_object_keys,
            on_failure_callback=[
                record_and_notify_kma_run_failed,
                record_weather_problem,
                record_weather_raw_product_failure,
            ],
            on_success_callback=record_weather_raw_product_event,
        )

        load_bronze = PythonOperator(
            task_id="load_kma_bronze",
            python_callable=load_kma_bronze,
            pool=TRINO_HEAVY_POOL,
            pool_slots=2,
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            on_failure_callback=[
                record_and_notify_kma_run_failed,
                record_weather_problem,
            ],
        )

        verify_bronze = PythonOperator(
            task_id="verify_kma_bronze_runtime",
            python_callable=verify_kma_bronze_runtime,
            pool=TRINO_HEAVY_POOL,
            pool_slots=2,
            retries=3,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            on_failure_callback=[
                record_and_notify_kma_run_failed,
                record_weather_problem,
                record_weather_bronze_product_failure,
            ],
            on_success_callback=record_weather_bronze_product_event,
        )
        publish_bronze_asset = PythonOperator(
            task_id="publish_weather_bronze_asset",
            python_callable=publish_weather_bronze_asset,
            outlets=[WEATHER_BRONZE_ASSET_REF],
        )

        (
            validate_runtime
            >> start_manifest
            >> land_raw
            >> load_bronze
            >> verify_bronze
            >> publish_bronze_asset
        )
    return enable_lineage_if_configured(built_dag)


dag = build_kma_bronze_dag(
    DAG_ID,
    kma_dag_schedule(),
    "Loads KMA getVilageFcst raw JSON into R2 and validates the Iceberg bronze runtime.",
    ["ask_seoul", "kma", "bronze", "r2", "iceberg"],
)

recollect_dag = build_kma_bronze_dag(
    RECOLLECT_DAG_ID,
    None,
    "Manually recollects a KMA getVilageFcst base_date/base_time through the Bronze contract.",
    ["ask_seoul", "kma", "bronze", "recollect", "r2", "iceberg"],
)

backfill_dag = build_kma_bronze_backfill_dag()
