"""Weather Iceberg 테이블 유지보수 계획과 한 동작 실행.

이 저장소가 소유·write 하는 Weather 테이블은 사이클마다 새 스냅샷과 데이터
파일을 쌓는다(bronze append, silver incremental, gold table 재작성). 방치하면
스냅샷·매니페스트·orphan 파일이 누적되어 그 위의 count·test 쿼리가 점점
느려진다(조직에서 겪은 metadata 팽창 이슈).

상류 ASAC-DAG 의 유지보수 모듈은 dev 단일 스키마(weather_traffic_bronze)를
전제하지만, 이 fork 의 prod 는 bronze 와 silver/gold 가 두 스키마
(weather_traffic_bronze / weather)로 나뉘어 있다. 그래서 상류를 그대로 옮기지
않고, 같은 표준 동작(optimize -> expire_snapshots -> remove_orphan_files)과 같은
직렬화·실패격리 원칙을 이 fork 의 실제 소유 테이블 집합에 맞춰 재구성한다.

이 모듈은 Airflow 에 의존하지 않는다. 계획 검증, table×operation SQL 문자열,
한 동작 실행/결과 분류만 소유한다.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Callable, Sequence


# 카탈로그 키 하나 — 값이 배포 환경을 따라간다. 미설정 시 prod 기본.
CATALOG_ENV = "TRINO_ICEBERG_CATALOG"
DEFAULT_CATALOG = "iceberg"

# expire_snapshots 는 히스토리를 지운다. 보수적으로 7일·최근 1개를 유지한다
# (상류와 동일). 이 두 값은 데이터 파괴 범위를 정하므로 상수로 고정한다.
FIXED_RETENTION = "7d"
RETAIN_LAST = 1

# 반드시 이 순서로 실행한다. optimize 로 작은 파일을 합친 뒤 스냅샷을 만료하고,
# 그 다음에야 어느 스냅샷도 참조하지 않는 orphan 파일을 지운다. 순서가 바뀌면
# 아직 참조 중인 파일을 지우거나, 합치기 전 파편을 만료해 효과가 준다.
OPERATIONS = ("optimize", "expire_snapshots", "remove_orphan_files")

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class MaintainedTable:
    """유지보수 대상 한 테이블. 이 fork 가 소유·write 하는 것만 넣는다."""

    schema: str
    name: str

    @property
    def label(self) -> str:
        return f"{self.schema}.{self.name}"


# 이 fork 가 소유하는 테이블만. 공유 dim_admin_dong(iceberg.common)과 다른 도메인
# 테이블은 이 fork 소관이 아니므로 제외한다.
BRONZE_SCHEMA = "weather_traffic_bronze"
MEDALLION_SCHEMA = "weather"

MAINTAINED_TABLES: tuple[MaintainedTable, ...] = (
    # Bronze: 매 수집 사이클 append.
    MaintainedTable(BRONZE_SCHEMA, "bronze_kma_vilage_fcst"),
    MaintainedTable(BRONZE_SCHEMA, "bronze_kma_ultra_srt_ncst"),
    MaintainedTable(BRONZE_SCHEMA, "bronze_collection_run_manifest"),
    # Silver: incremental.
    MaintainedTable(MEDALLION_SCHEMA, "silver_kma_vilage_fcst"),
    MaintainedTable(MEDALLION_SCHEMA, "silver_weather_forecast_by_admin_dong_serving"),
    MaintainedTable(MEDALLION_SCHEMA, "silver_weather_forecast_by_coverage_grid_serving"),
    # 차원(weather 전용): 참조 refresh 로 하루 1회 재작성.
    MaintainedTable(MEDALLION_SCHEMA, "dim_weather_place"),
    MaintainedTable(MEDALLION_SCHEMA, "dim_weather_coverage_grid"),
    # Gold serving 공통 입력: 사이클마다 table 재작성.
    MaintainedTable(MEDALLION_SCHEMA, "gold_weather_forecast_by_place_serving"),
    MaintainedTable(MEDALLION_SCHEMA, "gold_weather_forecast_by_grid_serving"),
    # Gold 공개 장소 상품: refresh 가 매시 재작성.
    MaintainedTable(MEDALLION_SCHEMA, "gold_weather_place_current_outlook"),
    MaintainedTable(MEDALLION_SCHEMA, "gold_weather_place_forecast_change_daily"),
    MaintainedTable(MEDALLION_SCHEMA, "gold_weather_place_hourly_outlook"),
    MaintainedTable(MEDALLION_SCHEMA, "gold_weather_place_precipitation_window"),
    MaintainedTable(MEDALLION_SCHEMA, "gold_weather_place_risk_query_availability"),
    MaintainedTable(MEDALLION_SCHEMA, "gold_weather_place_risk_window"),
    # Gold 격자 audit: transform 이 사이클마다 재작성.
    MaintainedTable(MEDALLION_SCHEMA, "gold_weather_grid_current_outlook"),
    MaintainedTable(MEDALLION_SCHEMA, "gold_weather_grid_hourly_outlook"),
    MaintainedTable(MEDALLION_SCHEMA, "gold_weather_grid_precipitation_window"),
)


class MaintenancePlanError(ValueError):
    """연결 전에, 유지보수 요청이 안전하지 않을 때 올린다."""


def maintenance_catalog(environ: "os._Environ[str] | dict[str, str] | None" = None) -> str:
    values = os.environ if environ is None else environ
    catalog = str(values.get(CATALOG_ENV, "") or "").strip() or DEFAULT_CATALOG
    if not _IDENTIFIER.match(catalog):
        raise MaintenancePlanError(f"unsafe maintenance catalog identifier: {catalog!r}")
    return catalog


def _safe_identifier(value: str, *, kind: str) -> str:
    if not _IDENTIFIER.match(value):
        raise MaintenancePlanError(f"unsafe maintenance {kind} identifier: {value!r}")
    return value


def resolve_maintained_tables(
    requested: Sequence[str] | None = None,
) -> tuple[MaintainedTable, ...]:
    """유지보수할 테이블 목록. 요청이 없으면 소유 테이블 전체.

    요청은 canonical 집합의 부분집합이어야 하며, 원래 순서를 유지한다. 이렇게
    해야 부분 재개 시에도 optimize->expire->orphan 순서와 테이블 순서가 결정적으로
    유지된다.
    """
    if requested is None:
        return MAINTAINED_TABLES
    labels = {t.label: t for t in MAINTAINED_TABLES}
    unknown = [r for r in requested if r not in labels]
    if unknown:
        raise MaintenancePlanError(
            f"unknown maintenance tables (must be owned by this fork): {unknown}"
        )
    chosen = set(requested)
    return tuple(t for t in MAINTAINED_TABLES if t.label in chosen)


def qualified_name(catalog: str, table: MaintainedTable) -> str:
    return ".".join(
        (
            _safe_identifier(catalog, kind="catalog"),
            _safe_identifier(table.schema, kind="schema"),
            _safe_identifier(table.name, kind="table"),
        )
    )


def operation_sql(catalog: str, table: MaintainedTable, operation: str) -> str:
    if operation not in OPERATIONS:
        raise MaintenancePlanError(f"unknown maintenance operation: {operation!r}")
    qualified = qualified_name(catalog, table)
    if operation == "optimize":
        command = "optimize"
    elif operation == "expire_snapshots":
        command = (
            f"expire_snapshots(retention_threshold => '{FIXED_RETENTION}', "
            f"retain_last => {RETAIN_LAST})"
        )
    else:  # remove_orphan_files
        command = f"remove_orphan_files(retention_threshold => '{FIXED_RETENTION}')"
    return f"ALTER TABLE {qualified} EXECUTE {command}"


def table_exists_sql(catalog: str, table: MaintainedTable) -> str:
    safe_catalog = _safe_identifier(catalog, kind="catalog")
    schema = table.schema.replace("'", "''")
    name = table.name.replace("'", "''")
    return (
        f"SELECT 1 FROM {safe_catalog}.information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{name}' LIMIT 1"
    )


@dataclass(frozen=True)
class MaintenanceActionResult:
    table: str
    operation: str
    status: str  # "ok" | "skipped_missing"
    statement: str


def execute_maintenance_action(
    cursor: Any,
    *,
    catalog: str,
    table: MaintainedTable,
    operation: str,
    fetchall: Callable[[Any], list] | None = None,
) -> MaintenanceActionResult:
    """한 테이블에 대해 한 유지보수 동작을 실행한다.

    테이블이 없으면 mutation 을 제출하지 않고 skipped_missing 으로 끝낸다 — 존재
    확인은 read-only 이고, 없는 테이블에 DDL 을 던지지 않는다.
    """
    fetch = fetchall or (lambda c: c.fetchall())
    cursor.execute(table_exists_sql(catalog, table))
    if not fetch(cursor):
        return MaintenanceActionResult(
            table=table.label,
            operation=operation,
            status="skipped_missing",
            statement="",
        )
    statement = operation_sql(catalog, table, operation)
    cursor.execute(statement)
    fetch(cursor)
    return MaintenanceActionResult(
        table=table.label,
        operation=operation,
        status="ok",
        statement=statement,
    )
