"""행정동 마스터 브론즈 적재 DAG (#154) — 파일명=dag_id, #73 공통 접두.

공공데이터포털 ODcloud '행정동/법정동 연계 정보'의 **최신 개정일자 스냅샷**을
R2 에 랜딩(raw)한 뒤 Trino 로 Iceberg 브론즈 테이블에 멱등 적재한다.

공통 자산 재사용: HttpCore(#78) · common.storage(#109) · 에러 콜백(#77).
순수 로직은 common.masters.admin_dong(테스트 대상), 이 파일은 오케스트레이션만.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

# DAG 는 dags 루트라 common.* 를 바로 import 할 수 있으나 안전 가드로 자기 dir 삽입.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from common.errors.airflow import problem_failure_callback
from common.masters import admin_dong

LOGGER = logging.getLogger(__name__)

import re

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

BRONZE_TABLE = "bronze_admin_dong_master"
# Trino VALUES 배치 크기 — QUERY_TEXT_TOO_LARGE(traffic 교훈) 회피. 한글 필드가 많아
# 1000행이면 statement 가 수백KB 라 보수적으로 500 으로 묶는다.
_INSERT_BATCH = 500

# 브론즈 컬럼(영문 스네이크). 코드류는 varchar 캐스트(컬럼 사전 준수 — 선행 0 보존).
# 원천 한글 필드 → 영문 컬럼 매핑.
_FIELD_MAP = [
    ("revision_date", "개정일자"),
    ("sido_nm", "시도명"),
    ("sgg_nm", "시군구명"),
    ("admin_dong_nm", "행정동명"),
    ("beop_dong_nm", "법정동명"),
    ("stat_region_cd", "행정구역코드"),
    ("admin_dong_code", "행정동코드"),
    ("beop_dong_code", "법정동코드"),
    ("link_no", "연결번호"),
]


# ── env / SQL 헬퍼 (traffic runtime 관례) ────────────────────────────────────────
def is_dev_target() -> bool:
    return os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod")) == "dev"


def trino_catalog() -> str:
    if is_dev_target():
        return os.environ.get("TRINO_ICEBERG_CATALOG") or "iceberg_dev"
    return os.environ.get("TRINO_ICEBERG_CATALOG", "iceberg")


def common_schema() -> str:
    # 공유 마스터는 실행자별 스모크 스키마(dev_<id>)가 아니라 전용 common 스키마에 적재한다
    # (raw/common/ 경로·common_* DAG 접두와 대칭, 개인 스키마 파편화 차단 — #154 코멘트).
    # dev/prod 공통 이름이며 카탈로그(iceberg_dev/iceberg)로만 분기한다.
    return os.environ.get("COMMON_SCHEMA", "common")


def sql_identifier(value: str) -> str:
    if not _IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Unsafe SQL identifier: {value}")
    return value


def sql_string(value: object) -> str:
    if value is None or value == "":
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def sql_timestamp(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    return "TIMESTAMP " + sql_string(utc_value.strftime("%Y-%m-%d %H:%M:%S.%f"))


def current_dag_run_id() -> str:
    return os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual")


def _trino_cursor():
    import trino.dbapi

    catalog = sql_identifier(trino_catalog())
    schema = sql_identifier(common_schema())
    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=catalog,
        schema=schema,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    return connection.cursor(), catalog, schema


def _qualified_table(catalog: str, schema: str) -> str:
    return f"{catalog}.{schema}.{sql_identifier(BRONZE_TABLE)}"


def _ensure_table(cursor, catalog: str, schema: str) -> str:
    qualified_schema = f"{catalog}.{schema}"
    qualified_table = _qualified_table(catalog, schema)
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")
    except Exception as exc:  # noqa: BLE001
        if "already exists" not in str(exc).lower():
            raise
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            revision_date varchar,
            sido_nm varchar,
            sgg_nm varchar,
            admin_dong_nm varchar,
            beop_dong_nm varchar,
            stat_region_cd varchar,
            admin_dong_code varchar,
            beop_dong_code varchar,
            link_no varchar,
            raw_object_key varchar,
            source_system varchar,
            collected_at timestamp(6),
            load_date varchar,
            dag_run_id varchar
        )
        WITH (format = 'PARQUET')
        """
    )
    return qualified_table


# ── task 1: R2 랜딩 ──────────────────────────────────────────────────────────────
def land_admin_dong_raw(**context) -> dict:
    run_id = context.get("run_id") or current_dag_run_id()
    key = admin_dong.load_service_key()
    core = admin_dong.build_core()

    revision = admin_dong.latest_revision(core, key)
    LOGGER.info("최신 개정일자 탐지: %s", revision)

    pages = admin_dong.iter_snapshot(core, key, revision)
    result = admin_dong.land_snapshot(pages, revision=revision, run_id=str(run_id))

    LOGGER.info(
        "R2 랜딩 완료 — 개정일자=%s pages=%d rows=%d bytes=%d",
        revision, len(result["object_keys"]), result["rows"], result["bytes"],
    )
    # object_keys 는 다음 태스크가 재다운로드에 쓴다(로그엔 개수만).
    return {
        "revision": revision,
        "rows": result["rows"],
        "load_date": result["load_date"],
        "object_keys": result["object_keys"],
    }


# ── task 2: Trino Iceberg 브론즈 적재 (멱등) ─────────────────────────────────────
def load_admin_dong_bronze(**context) -> dict:
    ti = context["ti"]
    landed = ti.xcom_pull(task_ids="land_admin_dong_raw")
    revision = landed["revision"]
    object_keys = landed["object_keys"]
    load_date = landed["load_date"]
    dag_run_id = str(context.get("run_id") or current_dag_run_id())

    cursor, catalog, schema = _trino_cursor()
    qualified_table = _ensure_table(cursor, catalog, schema)

    # 멱등: 이미 이 개정일자가 적재됐으면 skip.
    cursor.execute(
        f"SELECT count(*) FROM {qualified_table} WHERE revision_date = {sql_string(revision)}"
    )
    existing = cursor.fetchone()[0]
    if existing:
        LOGGER.info(
            "멱등 skip — 개정일자=%s 는 이미 %d행 적재됨(재적재 안 함)", revision, existing
        )
        return {"revision": revision, "inserted": 0, "skipped": True, "existing": existing}

    # R2 에서 페이지 재다운로드 → 행 파싱.
    store = admin_dong.build_r2_storage()
    collected_at = datetime.now(timezone.utc)
    ts_literal = sql_timestamp(collected_at)

    pending: list[str] = []
    inserted = 0

    def flush() -> None:
        nonlocal inserted, pending
        if not pending:
            return
        cursor.execute(
            f"INSERT INTO {qualified_table} (\n"
            "  revision_date, sido_nm, sgg_nm, admin_dong_nm, beop_dong_nm,\n"
            "  stat_region_cd, admin_dong_code, beop_dong_code, link_no,\n"
            "  raw_object_key, source_system, collected_at, load_date, dag_run_id\n"
            f") VALUES {', '.join(pending)}"
        )
        inserted += len(pending)
        pending = []

    for object_key in object_keys:
        document = json.loads(store.read_bytes(object_key).decode("utf-8"))
        for row in document.get("data") or []:
            fields = [sql_string(row.get(src)) for _, src in _FIELD_MAP]
            values = (
                "("
                + ", ".join(fields)
                + f", {sql_string(object_key)}"
                + f", {sql_string(admin_dong.SOURCE_SYSTEM)}"
                + f", {ts_literal}"
                + f", {sql_string(load_date)}"
                + f", {sql_string(dag_run_id)}"
                + ")"
            )
            pending.append(values)
            if len(pending) >= _INSERT_BATCH:
                flush()
    flush()

    LOGGER.info("브론즈 적재 완료 — 개정일자=%s inserted=%d", revision, inserted)
    return {"revision": revision, "inserted": inserted, "skipped": False}


# ── task 3: 검증 ─────────────────────────────────────────────────────────────────
def verify_admin_dong_bronze(**context) -> dict:
    ti = context["ti"]
    landed = ti.xcom_pull(task_ids="land_admin_dong_raw")
    revision = landed["revision"]
    expected_rows = landed["rows"]

    cursor, catalog, schema = _trino_cursor()
    qualified_table = _qualified_table(catalog, schema)

    cursor.execute(
        f"SELECT count(*) FROM {qualified_table} WHERE revision_date = {sql_string(revision)}"
    )
    total = cursor.fetchone()[0]

    cursor.execute(
        f"SELECT count(*) FROM {qualified_table} "
        f"WHERE revision_date = {sql_string(revision)} AND sido_nm = {sql_string('서울특별시')}"
    )
    seoul = cursor.fetchone()[0]

    LOGGER.info(
        "검증 — 개정일자=%s trino_count=%d 수집rows=%d 서울행수=%d(기대 ~769)",
        revision, total, expected_rows, seoul,
    )
    if total != expected_rows:
        raise RuntimeError(
            f"검증 실패 — 개정일자={revision} trino_count={total} != 수집rows={expected_rows}"
        )
    return {"revision": revision, "trino_count": total, "seoul_rows": seoul}


record_problem = problem_failure_callback(
    domain="common", source_system=admin_dong.SOURCE_SYSTEM
)

with DAG(
    dag_id="common_admin_dong_bronze",
    description="공공데이터포털 행정동 마스터 최신 스냅샷을 R2 랜딩 후 Iceberg 브론즈에 멱등 적재.",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["common", "master", "admin_dong", "bronze"],
) as dag:
    land_raw = PythonOperator(
        task_id="land_admin_dong_raw",
        python_callable=land_admin_dong_raw,
        on_failure_callback=[record_problem],
    )
    load_bronze = PythonOperator(
        task_id="load_admin_dong_bronze",
        python_callable=load_admin_dong_bronze,
        on_failure_callback=[record_problem],
    )
    verify_bronze = PythonOperator(
        task_id="verify_admin_dong_bronze",
        python_callable=verify_admin_dong_bronze,
        on_failure_callback=[record_problem],
    )

    land_raw >> load_bronze >> verify_bronze
