"""Airflow DAG: weekly Iceberg maintenance for this fork's Weather tables.

이 fork 가 소유·write 하는 Weather 테이블에 주 1회 optimize -> expire_snapshots
-> remove_orphan_files 를 돌려 스냅샷·orphan 파일 누적을 정리한다.

설계 원칙(상류 ASAC-DAG 유지보수와 동일):
  - table×operation 단위의 정적 태스크. 동적 매핑·병렬화 금지.
  - 단일 pool 슬롯 + max_active_runs=1 로 한 번에 한 동작만 돈다(Trino 메모리
    압박 방지). 이 fork prod Trino 는 resource group concurrency 가 제한돼 있다.
  - 한 테이블에서 실패해도 다음 테이블은 계속한다(테이블 단위 실패 격리).
  - mutation 은 자동 retry 하지 않는다(retries=0). DDL 재실행은 사람이 판단한다.

이 DAG 는 데이터를 파괴할 수 있는 DDL(expire_snapshots 는 히스토리 삭제)을
제출하므로, 배포 후에도 기본 paused 로 두고 사람이 조건을 확인한 뒤에만 unpause
/트리거한다.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)

DAGS_ROOT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if DAGS_ROOT_DIR not in sys.path:
    sys.path.insert(0, DAGS_ROOT_DIR)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.pools import TRINO_WEATHER_HEAVY_POOL  # noqa: E402
from common.runtime_guard import (  # noqa: E402
    TARGET_CHOICES,
    default_target,
    validate_dev_runtime,
)
from weather_ingest.iceberg_maintenance import (  # noqa: E402
    MAINTAINED_TABLES,
    OPERATIONS,
    execute_maintenance_action,
    maintenance_catalog,
)


KST = ZoneInfo("Asia/Seoul")
DOMAIN = "weather"
record_weather_problem = problem_failure_callback(domain=DOMAIN)

# A single-node personal Trino must always prioritize fresh serving data over
# optional file compaction. The task bound caps a stalled maintenance action,
# and no maintenance mutation is retried.
MAINTENANCE_ACTION_TIMEOUT = timedelta(minutes=8)
MAINTENANCE_PRIORITY_WEIGHT = 1

DEFAULT_PARAMS = {
    "target": Param(
        default=default_target(),
        type="string",
        enum=list(TARGET_CHOICES),
        description="dbt/runtime target; defaults to the runtime env (#561).",
    )
}


def _maintenance_cursor():
    """유지보수 전용 Trino 커서. 테이블을 완전 수식해 쓰므로 기본 스키마는 안 쓴다."""
    import trino.dbapi

    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=maintenance_catalog(),
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    return connection.cursor()


def run_maintenance_action(*, schema: str, table_name: str, operation: str, **context) -> dict:
    """한 테이블에 대해 한 유지보수 동작을 실행한다."""
    from weather_ingest.iceberg_maintenance import MaintainedTable

    catalog = maintenance_catalog()
    table = MaintainedTable(schema, table_name)
    cursor = _maintenance_cursor()
    try:
        result = execute_maintenance_action(
            cursor,
            catalog=catalog,
            table=table,
            operation=operation,
        )
    except Exception as exc:  # noqa: BLE001 - surface as a retryable-free failure
        raise AirflowException(
            f"iceberg maintenance {operation} failed for {table.label}: {exc}"
        ) from exc
    print(
        f"[iceberg-maintenance] {result.status} · {operation} · {table.label}"
        + (f" · {result.statement}" if result.statement else "")
    )
    return {
        "table": result.table,
        "operation": result.operation,
        "status": result.status,
    }


def summarize_maintenance(**context) -> dict:
    """모든 동작이 끝난 뒤 실행/skip/실패 집계를 남긴다(ALL_DONE gate)."""
    ti = context["ti"]
    ok = skipped = 0
    for table in MAINTAINED_TABLES:
        for operation in OPERATIONS:
            task_id = _action_task_id(table.schema, table.name, operation)
            try:
                payload = ti.xcom_pull(task_ids=task_id)
            except Exception:  # noqa: BLE001 - a failed/skipped action has no payload
                payload = None
            if isinstance(payload, dict):
                if payload.get("status") == "ok":
                    ok += 1
                elif payload.get("status") == "skipped_missing":
                    skipped += 1
    total = len(MAINTAINED_TABLES) * len(OPERATIONS)
    print(
        f"[iceberg-maintenance] 완료 요약: ok={ok} skipped_missing={skipped} "
        f"failed_or_upstream_skipped={total - ok - skipped} / total={total}"
    )
    return {"ok": ok, "skipped_missing": skipped, "total": total}


def _action_task_id(schema: str, table_name: str, operation: str) -> str:
    return f"{schema}__{table_name}__{operation}"


def _action_task(schema: str, table_name: str, operation: str, *, isolate_table: bool):
    kwargs = {
        "task_id": _action_task_id(schema, table_name, operation),
        "python_callable": run_maintenance_action,
        "op_kwargs": {
            "schema": schema,
            "table_name": table_name,
            "operation": operation,
        },
        "pool": TRINO_WEATHER_HEAVY_POOL,
        # Compaction/retention can rewrite or remove files.  Hold the whole
        # two-slot Weather lane so no transform or serving swap can observe a
        # changing Iceberg table.
        "pool_slots": 2,
        "weight_rule": "absolute",
        "priority_weight": MAINTENANCE_PRIORITY_WEIGHT,
        "execution_timeout": MAINTENANCE_ACTION_TIMEOUT,
        "retries": 0,
        "on_failure_callback": record_weather_problem,
    }
    # 테이블의 첫 동작은 ALL_DONE 으로 받아, 앞 테이블이 실패해도 이 테이블은 돈다.
    # 같은 테이블 안(optimize->expire->orphan)은 기본 all_success 로 이어, 앞 동작이
    # 실패하면 뒤 동작을 건너뛴다(참조 중 파일을 지우는 위험을 막는다).
    if isolate_table:
        kwargs["trigger_rule"] = "all_done"
    return PythonOperator(**kwargs)


def maintenance_schedule() -> str | None:
    """Require an explicit schedule for destructive, low-priority maintenance."""
    return os.getenv("ASK_SEOUL_WEATHER_MAINTENANCE_DAG_SCHEDULE", "").strip() or None


with DAG(
    dag_id="weather_iceberg_maintenance",
    description="Weekly optimize/expire/remove-orphan for this fork's Weather Iceberg tables.",
    start_date=datetime(2026, 1, 1, tzinfo=KST),
    schedule=maintenance_schedule(),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    params=DEFAULT_PARAMS,
    tags=["ask_seoul", "weather", "maintenance", "iceberg"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_dev_runtime",
        python_callable=validate_dev_runtime,
        op_kwargs={"domain": "weather", "requested_target": "{{ params.target }}"},
        on_failure_callback=record_weather_problem,
    )

    summarize = PythonOperator(
        task_id="summarize_maintenance",
        python_callable=summarize_maintenance,
        trigger_rule="all_done",
        on_failure_callback=record_weather_problem,
    )

    previous_table_last = validate_runtime
    for table_index, table in enumerate(MAINTAINED_TABLES):
        table_first = None
        table_last = None
        for op_index, operation in enumerate(OPERATIONS):
            # 첫 테이블의 첫 동작은 all_success 로 validate_runtime 을 존중한다
            # (런타임 검증이 실패하면 어떤 mutation 도 돌지 않아야 한다). 이후
            # 테이블의 첫 동작만 all_done 으로 앞 테이블 실패로부터 격리한다.
            isolate = op_index == 0 and table_index > 0
            action = _action_task(
                table.schema,
                table.name,
                operation,
                isolate_table=isolate,
            )
            if table_first is None:
                table_first = action
            else:
                table_last >> action
            table_last = action
        previous_table_last >> table_first
        table_last >> summarize
        previous_table_last = table_last
