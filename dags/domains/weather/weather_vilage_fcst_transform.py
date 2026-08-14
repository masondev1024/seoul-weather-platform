"""Airflow DAG: weather silver/gold transform via dbt.

The bronze DAG stores KMA raw payloads in R2 and publishes verified Iceberg
bronze runs. This transform DAG consumes only publishable bronze runs through
the dbt models and keeps silver/gold retries independent from API collection.
"""

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
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset

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
from common.assets import (  # noqa: E402
    WEATHER_BRONZE_ASSET,
    WEATHER_GOLD_PUBLICATION_READY_ASSET,
)
from common.pools import TRINO_WEATHER_LEGACY_HEAVY_POOL  # noqa: E402
from common.runmetrics import dump_dbt_run_results  # noqa: E402
from common.runtime_guard import (  # noqa: E402
    TARGET_CHOICES,
    default_target,
    validate_dev_runtime,
)
from weather_ingest.common.resources import DbtWorkload  # noqa: E402
from weather_ingest.runtime import build_weather_manifest  # noqa: E402
import weather_dbt_execution as weather_dbt  # noqa: E402
from weather_dbt_runtime import (  # noqa: E402
    DBT_RETRY_DELAY,
    DOMAIN,
    SERVING_AS_OF_HOUR_TASK_ID,
    WEATHER_DBT_CONTRACT_VARS,
    WEATHER_DBT_RUN_RESULTS_XCOM_KEY,
    WEATHER_SNAPSHOT_VAR,
    resolve_weather_serving_as_of_hour,
    run_weather_dbt_phase,
    weather_serving_as_of_hour_state,
)
from weather_lineage import enable_lineage_if_configured  # noqa: E402


LOGGER = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")
DBT_BIN = weather_dbt.dbt_bin()
DBT_PROJECT = weather_dbt.dbt_project_dir()
WEATHER_DBT_CONTRACT_VARS = {"weather_w2_canonical_revision_date": "2025-04-01"}
WEATHER_DBT_RUN_RESULTS_XCOM_KEY = "weather_dbt_run_results_path"
SNAPSHOT_TASK_ID = "resolve_weather_snapshot_run"
WEATHER_SNAPSHOT_VAR = "weather_snapshot_dag_run_id"
WEATHER_GOLD_PUBLICATION_READY_ASSET_REF = Asset(
    WEATHER_GOLD_PUBLICATION_READY_ASSET
)
# Dedicated lane (#512): this DAG's own transform chain used to share
# trino_weather_heavy with weather_w2_canonical_transform and
# weather_vilage_fcst_bronze, so a single run (observed ~50min) starved both
# of the shared pool. #480's atomic swap + snapshot pin already makes the
# shared admin_dong axis safe to read concurrently, so this DAG no longer
# needs to serialize behind canonical/bronze — only against itself
# (max_active_runs=1 plus this pool's own single slot keep its internal
# step order intact, same as before).


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
        "dbt_source_freshness",
        "source freshness",
        "ask_seoul_weather_transform_source",
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
    DbtPhaseSpec(
        "dbt_run_silver",
        "run",
        "ask_seoul_weather_transform_silver",
    ),
    DbtPhaseSpec(
        "dbt_test_silver",
        "test",
        "ask_seoul_weather_transform_silver",
    ),
    DbtPhaseSpec(
        "dbt_run_place_mart",
        "run",
        "ask_seoul_weather_transform_serving_place_mart",
    ),
    DbtPhaseSpec(
        "dbt_test_place_mart",
        "test",
        "ask_seoul_weather_transform_serving_place_mart",
    ),
    DbtPhaseSpec(
        "dbt_run_coverage_grid_mart",
        "run",
        "ask_seoul_weather_transform_serving_grid_mart",
    ),
    DbtPhaseSpec(
        "dbt_test_coverage_grid_mart",
        "test",
        "ask_seoul_weather_transform_serving_grid_mart",
    ),
    DbtPhaseSpec(
        "dbt_run_gold",
        "run",
        "ask_seoul_weather_transform_serving_gold",
    ),
    DbtPhaseSpec(
        "dbt_test_gold",
        "test",
        "ask_seoul_weather_transform_serving_gold",
    ),
)
DBT_PHASE_TASK_IDS = tuple(spec.task_id for spec in DBT_PHASE_SPECS)
WEATHER_DISCORD_WEBHOOK_ENV = "WEATHER_DISCORD_WEBHOOK_URL"
DISCORD_RED = 15158332
DEFAULT_PARAMS = {
    "target": Param(
        default=default_target(),
        type="string",
        enum=list(TARGET_CHOICES),
        description="dbt target profile name; defaults to the runtime env (#561).",
    )
}
# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
# dbt transform 은 외부 소스 API 를 호출하지 않으므로 source_system 은 생략한다.
record_weather_problem = problem_failure_callback(
    domain="weather",
    dbt_project_dir=DBT_PROJECT,
    dbt_run_results_xcom_key=WEATHER_DBT_RUN_RESULTS_XCOM_KEY,
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
    """Pin and verify the exact Weather Bronze run that triggered this DAG run."""
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
        return "dbt 패키지 설치"
    if "freshness" in task_id:
        return "소스 신선도 검사"
    if "common_admin_dong_dimension" in task_id:
        return "공용 행정동 차원 실행/검증"
    if "seed" in task_id:
        return "시드 적재/검증"
    if "place_mart" in task_id:
        return "place mart run/test"
    if "silver" in task_id:
        return "silver run/test"
    if "gold" in task_id:
        return "gold run/test"
    return "알 수 없음"


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
    except Exception as exc:
        LOGGER.warning("[weather notify] Discord send failed: %s", type(exc).__name__)


def notify_weather_transform_failure(context) -> None:
    # 브론즈 수집 DAG 실패 알림과 같은 채널(WEATHER_DISCORD_WEBHOOK_URL) — 미설정이면 no-op.
    ti = context.get("ti") or context.get("task_instance")
    task_id = getattr(ti, "task_id", "N/A")
    exc = context.get("exception")
    run_id = context.get("run_id", "N/A")
    target = (context.get("params") or {}).get("target", "N/A")
    send_weather_discord(
        f"기상청 transform 실패 - {discord_report_date(context)} (target={target})",
        "\n".join(
            [
                "❌ 변환 상태: 실패",
                f"❌ 실패 단계: {transform_stage_name(task_id)}",
                f"❌ 실패 task: `{task_id}`",
                f"❌ 오류 유형: `{type(exc).__name__ if exc else 'N/A'}`",
                "",
                f"Airflow 로그: {getattr(ti, 'log_url', 'N/A')}",
            ]
        ),
        DISCORD_RED,
        f"dag_id={context['dag'].dag_id} · run_id={short_text(run_id, 180)}",
    )


def transform_schedule() -> str | list[Asset] | None:
    if "ASK_SEOUL_WEATHER_TRANSFORM_DAG_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_WEATHER_TRANSFORM_DAG_SCHEDULE"] or None
    return [Asset(WEATHER_BRONZE_ASSET)]


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
    """Run one dbt phase through the shared non-DAG Weather runtime."""

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
        pipeline="weather-transform",
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
            "snapshot_task_id": SNAPSHOT_TASK_ID,
            "serving_as_of_task_id": SERVING_AS_OF_HOUR_TASK_ID,
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
        f"dbt 실행 메트릭 적재: {len(records)} records (domain={DOMAIN}, target={target})"
    )
    return {"rows": len(records), "skipped": False}


def mark_weather_gold_publication_ready(
    *, now: datetime | None = None, **context
) -> dict[str, str]:
    """Emit the D1 trigger only after Weather Gold write and tests succeed."""
    bronze_run_id = str(
        context["ti"].xcom_pull(task_ids=SNAPSHOT_TASK_ID) or ""
    )
    if not bronze_run_id:
        raise AirflowFailException(
            "weather Gold publication marker requires a Bronze snapshot"
        )
    serving_as_of_hour, serving_hour_state = weather_serving_as_of_hour_state(
        ti=context["ti"],
        now=now,
    )
    if serving_hour_state == "stale":
        raise AirflowSkipException(
            "weather Gold publication marker skipped a stale serving hour: "
            f"serving_as_of_hour={serving_as_of_hour}"
        )
    outlet_events = context.get("outlet_events")
    if outlet_events is None:
        raise AirflowFailException(
            "weather Gold publication outlet event is unavailable"
        )
    metadata = {
        "gold_dag_run_id": str(context.get("run_id") or ""),
        "bronze_dag_run_id": bronze_run_id,
        "serving_as_of_hour": serving_as_of_hour,
    }
    outlet_events[WEATHER_GOLD_PUBLICATION_READY_ASSET_REF].extra = metadata
    return metadata


with DAG(
    dag_id="weather_vilage_fcst_transform",
    description="Transform weather bronze -> silver/gold via dbt.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=transform_schedule(),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": DBT_RETRY_DELAY},
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "weather", "transform", "silver", "gold", "dbt"],
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

    resolve_serving_as_of_hour = PythonOperator(
        task_id=SERVING_AS_OF_HOUR_TASK_ID,
        python_callable=resolve_weather_serving_as_of_hour,
        on_failure_callback=record_weather_problem,
    )

    dbt_phase_tasks = {spec.task_id: dbt_task(spec) for spec in DBT_PHASE_SPECS}
    dbt_tasks_in_order = list(dbt_phase_tasks.values())

    publish_dbt_metrics = PythonOperator(
        task_id="publish_dbt_run_metrics",
        python_callable=publish_dbt_run_metrics,
        on_failure_callback=record_weather_problem,
    ).as_teardown(on_failure_fail_dagrun=False)

    mark_gold_publication_ready = PythonOperator(
        task_id="mark_weather_gold_publication_ready",
        python_callable=mark_weather_gold_publication_ready,
        outlets=[WEATHER_GOLD_PUBLICATION_READY_ASSET_REF],
        on_failure_callback=record_weather_problem,
    )

    pipeline_tasks = [
        validate_runtime,
        resolve_snapshot,
        resolve_serving_as_of_hour,
        *dbt_tasks_in_order,
        mark_gold_publication_ready,
        publish_dbt_metrics,
    ]
    for upstream, downstream in zip(pipeline_tasks, pipeline_tasks[1:]):
        upstream >> downstream


enable_lineage_if_configured(dag)
