"""D1 access seam for the common publisher.

``D1Client`` is a small higher-level interface (not raw SQL) so the publisher's
orchestration is exercised against an in-memory fake in tests — no network, no prod
D1. ``HttpD1Client`` is the thin real implementation over the Cloudflare D1 HTTP API;
it reads its token/account/db from the environment and never logs the token.
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, Sequence

Column = tuple[str, str]  # (name, trino_type)

_SQLITE_TYPE = {
    "integer": "INTEGER", "bigint": "INTEGER", "smallint": "INTEGER", "tinyint": "INTEGER",
    "boolean": "INTEGER", "double": "REAL", "real": "REAL",
}
# Cloudflare D1 permits a 100,000-byte SQL statement. Leave margin for the
# statement itself and keep API request batches bounded so staging writes stay
# comfortably below the query-duration limit.
MAX_SQL_STATEMENT_BYTES = 80_000
MAX_STATEMENTS_PER_API_BATCH = 4
MAX_API_BATCH_BYTES = 256_000
MAX_ACTIVATION_STATEMENTS = 32
MAX_ACTIVATION_API_BATCH_BYTES = 256_000
D1_MAX_ATTEMPTS = 3
D1_RETRY_BASE_SECONDS = 0.5
D1_RETRY_MAX_SECONDS = 4.0
D1_TRANSIENT_ERROR_CODES = frozenset({"7500"})
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
CF_RAY_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
D1_ERROR_CODE_RE = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")

log = logging.getLogger(__name__)

QUERY_AVAILABILITY_TABLE = "d1_product_query_availability"
QUERY_AVAILABILITY_COLUMNS = (
    ("product_id", "TEXT NOT NULL"), ("publication_id", "TEXT NOT NULL"),
    ("place_id", "TEXT NOT NULL"), ("snapshot_as_of_hour", "TEXT NOT NULL"),
    ("available_from_at", "TEXT NOT NULL"), ("available_to_at", "TEXT NOT NULL"),
    ("forecast_collected_at_min", "TEXT NOT NULL"), ("forecast_collected_at_max", "TEXT NOT NULL"),
    ("expected_forecast_hour_count", "INTEGER NOT NULL"), ("observed_forecast_hour_count", "INTEGER NOT NULL"),
    ("availability_status", "TEXT NOT NULL"), ("source_population_revision", "TEXT NOT NULL"),
    ("availability_fingerprint", "TEXT NOT NULL"), ("measured_at", "TEXT NOT NULL"),
)
QUERY_AVAILABILITY_PRIMARY_KEY = ("product_id", "publication_id", "place_id")


def quote_identifier(identifier: str) -> str:
    if not isinstance(identifier, str) or not IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"unsafe D1 identifier: {identifier!r}")
    return f'"{identifier}"'


def sqlite_type(trino_type: str) -> str:
    base = trino_type.split("(")[0].strip().lower()
    return "REAL" if base == "decimal" else _SQLITE_TYPE.get(base, "TEXT")


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


class D1RequestError(RuntimeError):
    """Sanitized D1 transport/API failure without SQL, body, headers, or token."""

    def __init__(
        self,
        *,
        http_status: int | None,
        error_codes: Sequence[str] = (),
        cf_ray: str | None = None,
        attempt: int,
        exception_type: str | None = None,
        transient: bool = False,
    ) -> None:
        self.http_status = http_status
        self.error_codes = tuple(error_codes)
        self.cf_ray = cf_ray
        self.attempt = attempt
        self.exception_type = exception_type
        self.transient = transient
        parts = [
            f"http_status={http_status if http_status is not None else 'unknown'}",
            f"codes={','.join(self.error_codes) if self.error_codes else 'none'}",
            f"cf_ray={cf_ray or 'none'}",
            f"attempt={attempt}",
        ]
        if exception_type:
            parts.append(f"exception_type={exception_type}")
        super().__init__("D1 API 실패: " + " ".join(parts))


def _iter_error_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for key in ("error", "errors"):
            nested = value.get(key)
            if nested is not None:
                yield from _iter_error_dicts(nested)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _iter_error_dicts(item)


def _d1_error_codes(response: dict[str, Any]) -> tuple[str, ...]:
    sources: list[Any] = [response.get("errors")]
    for statement in response.get("result") or ():
        if isinstance(statement, dict) and statement.get("success") is False:
            sources.append(statement.get("errors"))
    codes = {
        code
        for source in sources
        for error in _iter_error_dicts(source)
        if error.get("code") is not None
        for code in (str(error.get("code")).strip(),)
        if D1_ERROR_CODE_RE.fullmatch(code)
    }
    return tuple(sorted(codes))


def _safe_cf_ray(headers: Any) -> str | None:
    getter = getattr(headers, "get", None)
    value = getter("CF-Ray") if callable(getter) else None
    normalized = str(value or "").strip()
    return normalized if CF_RAY_RE.fullmatch(normalized) else None


def _transient_exception(exc: Exception) -> bool:
    retryable_names = {
        "ConnectTimeout",
        "ConnectionError",
        "ReadTimeout",
        "Timeout",
    }
    return any(base.__name__ in retryable_names for base in type(exc).__mro__)


def _transient_failure(http_status: int | None, error_codes: Sequence[str]) -> bool:
    return (
        http_status == 429
        or (http_status is not None and 500 <= http_status <= 599)
        or bool(D1_TRANSIENT_ERROR_CODES.intersection(error_codes))
    )


def _read_only_sql(sql: str) -> bool:
    normalized = sql.lstrip().upper()
    return normalized.startswith("SELECT ") or normalized.startswith("PRAGMA ")


def _utf8_bytes(value: str) -> int:
    return len(value.encode("utf-8"))


def build_insert_statements(
    name: str,
    columns: Sequence[Column],
    rows: Sequence[dict[str, Any]],
    *,
    replace: bool,
) -> list[str]:
    """Render INSERT statements without exceeding D1's per-statement budget."""

    if not rows:
        return []
    colnames = [column for column, _ in columns]
    verb = "INSERT OR REPLACE INTO" if replace else "INSERT INTO"
    head = f'{verb} "{name}" ("' + '", "'.join(colnames) + '") VALUES\n'
    tail = ";"
    fixed_bytes = _utf8_bytes(head + tail)
    rendered_rows = [
        "(" + ", ".join(sql_literal(row.get(column)) for column in colnames) + ")"
        for row in rows
    ]

    statements: list[str] = []
    current_rows: list[str] = []
    current_bytes = fixed_bytes
    for rendered in rendered_rows:
        rendered_bytes = _utf8_bytes(rendered)
        separator_bytes = _utf8_bytes(",\n") if current_rows else 0
        if fixed_bytes + rendered_bytes > MAX_SQL_STATEMENT_BYTES:
            raise ValueError(
                f"{name}: one rendered row exceeds SQL byte budget "
                f"{MAX_SQL_STATEMENT_BYTES}"
            )
        if current_rows and current_bytes + separator_bytes + rendered_bytes > MAX_SQL_STATEMENT_BYTES:
            statements.append(head + ",\n".join(current_rows) + tail)
            current_rows = []
            current_bytes = fixed_bytes
            separator_bytes = 0
        current_rows.append(rendered)
        current_bytes += separator_bytes + rendered_bytes

    if current_rows:
        statements.append(head + ",\n".join(current_rows) + tail)
    return statements


def _api_batch_bytes(statements: Sequence[str]) -> int:
    body = {"batch": [{"sql": statement} for statement in statements]}
    return _utf8_bytes(json.dumps(body, ensure_ascii=False, separators=(",", ":")))


def group_api_batches(statements: Sequence[str]) -> list[list[str]]:
    """Group already-safe statements into bounded Cloudflare D1 API batches."""

    batches: list[list[str]] = []
    current: list[str] = []
    for statement in statements:
        if _utf8_bytes(statement) > MAX_SQL_STATEMENT_BYTES:
            raise ValueError(f"statement exceeds SQL byte budget {MAX_SQL_STATEMENT_BYTES}")
        candidate = [*current, statement]
        if current and (
            len(candidate) > MAX_STATEMENTS_PER_API_BATCH
            or _api_batch_bytes(candidate) > MAX_API_BATCH_BYTES
        ):
            batches.append(current)
            current = [statement]
        else:
            current = candidate
        if _api_batch_bytes(current) > MAX_API_BATCH_BYTES:
            raise ValueError(f"statement exceeds API batch byte budget {MAX_API_BATCH_BYTES}")
    if current:
        batches.append(current)
    return batches


class D1Client(Protocol):
    def table_row_count(self, name: str) -> int: ...
    def primary_key_stats(self, name: str, primary_key: Sequence[str]) -> tuple[int, int, int]: ...
    def table_max(self, name: str, column: str) -> Any | None: ...
    def catalog_row(self, name: str) -> dict[str, Any] | None: ...
    def ensure_table(self, name: str, columns: Sequence[Column], primary_key: Sequence[str]) -> None: ...
    def replace_table(self, name: str, columns: Sequence[Column], rows: Sequence[dict[str, Any]], primary_key: Sequence[str]) -> None: ...
    def restore_replaced_table(self, name: str) -> None: ...
    def finalize_replaced_table(self, name: str) -> None: ...
    def delete_where_gte(self, name: str, column: str, trino_literal: str) -> None: ...
    def insert_rows(self, name: str, columns: Sequence[Column], rows: Sequence[dict[str, Any]], *, replace: bool) -> None: ...
    def upsert_catalog(self, catalog_rows: Sequence[dict[str, Any]]) -> None: ...
    def delete_catalog_row(self, name: str) -> None: ...
    def delete_catalog_product_ids(self, product_ids: Sequence[str]) -> None: ...
    def catalog_domain_count(self, model_names: set[str]) -> int: ...
    def read_table_rows(self, name: str, ordered_columns: Sequence[Column], primary_key: Sequence[str]) -> list[dict[str, Any]]: ...
    def execute(self, sql: str) -> list[dict[str, Any]]: ...
    def append_publication_ledger(self, record: dict[str, Any]) -> None: ...
    def publish_product_meta(
        self,
        product_id: str,
        publication_id: str,
        columns_rows: Sequence[dict[str, Any]],
        ext_rows: Sequence[dict[str, Any]],
        pattern_rows: Sequence[dict[str, Any]],
        display_rows: Sequence[dict[str, Any]] = (),
        *,
        param_rows: Sequence[dict[str, Any]] = (),
        vocabulary_rows: Sequence[dict[str, Any]] = (),
    ) -> None: ...
    def publish_glossary(self, rows: Sequence[dict[str, Any]]) -> None: ...
    def publish_product_evidence(
        self,
        product_id: str,
        publication_id: str,
        sources: Sequence[dict[str, Any]] | None,
        quality: dict[str, Any],
    ) -> None: ...
    def stage_snapshot(self, name: str, columns: Sequence[Column], rows: Sequence[dict[str, Any]], primary_key: Sequence[str]) -> None: ...
    def read_staged_snapshot_rows(self, name: str, columns: Sequence[Column], primary_key: Sequence[str]) -> list[dict[str, Any]]: ...
    def stage_query_availability(self, product_id: str, publication_id: str, rows: Sequence[dict[str, Any]], *, fingerprint: str, measured_at: str) -> None: ...
    def read_query_availability_rows(self, product_id: str, publication_id: str) -> list[dict[str, Any]]: ...
    def capture_product_publication_state(self, product_id: str, model_name: str) -> "ProductPublicationState": ...
    def build_activation_statements(self, product_id: str, model_name: str, candidate: "ProductPublicationState") -> tuple[str, ...]: ...
    def build_compensation_statements(self, product_id: str, model_name: str, previous: "ProductPublicationState") -> tuple[str, ...]: ...
    def preflight_staged_transition(self, product_id: str, model_name: str, candidate: "ProductPublicationState", previous: "ProductPublicationState") -> None: ...
    def activate_staged_snapshot(self, product_id: str, model_name: str, candidate: "ProductPublicationState") -> None: ...
    def compensate_staged_snapshot(self, product_id: str, model_name: str, previous: "ProductPublicationState") -> None: ...
    def prepare_atomic_publication_schema(self) -> None: ...


@dataclass(frozen=True)
class ProductPublicationState:
    catalog_row: dict[str, Any] | None
    metadata_rows: dict[str, tuple[dict[str, Any], ...]]
    source_rows: tuple[dict[str, Any], ...]
    quality_row: dict[str, Any] | None
    active_table_exists: bool = True


# ---- catalog schema (single source for both real client and Worker) -----------------

CATALOG_COLUMN_TYPES = (
    ("name", "TEXT PRIMARY KEY"), ("product_id", "TEXT"), ("external", "INTEGER"),
    ("description", "TEXT"), ("product_question", "TEXT"),
    ("public_gold", "TEXT"), ("mcp_projection", "TEXT"), ("tests", "TEXT"),
    ("time_axis", "TEXT"), ("columns", "TEXT"), ("row_count", "INTEGER"),
    ("serving_status", "TEXT"), ("publication_id", "TEXT"), ("source_run_id", "TEXT"),
    ("published_bytes", "INTEGER"), ("freshness", "TEXT"), ("exported_at", "TEXT"),
)
CATALOG_COLUMNS = tuple(name for name, _ in CATALOG_COLUMN_TYPES)
CATALOG_DDL = "CREATE TABLE IF NOT EXISTS _catalog (" + ", ".join(
    f"{name} {column_type}" for name, column_type in CATALOG_COLUMN_TYPES
) + ");"
PUBLICATION_LEDGER_COLUMN_TYPES = (
    ("publication_id", "TEXT PRIMARY KEY"), ("product_id", "TEXT NOT NULL"),
    ("model_name", "TEXT NOT NULL"), ("source_run_id", "TEXT NOT NULL"),
    ("attempted_at", "TEXT NOT NULL"), ("outcome", "TEXT NOT NULL"),
    ("stage", "TEXT NOT NULL"), ("source_row_count", "INTEGER NOT NULL"),
    ("published_row_count", "INTEGER NOT NULL"), ("d1_row_count", "INTEGER NOT NULL"),
    ("api_smoke_status", "TEXT NOT NULL"), ("rollback_status", "TEXT NOT NULL"),
    ("reason", "TEXT NOT NULL"),
)
PUBLICATION_LEDGER_COLUMNS = tuple(name for name, _ in PUBLICATION_LEDGER_COLUMN_TYPES)
PUBLICATION_LEDGER_DDL = "CREATE TABLE IF NOT EXISTS _publication_ledger (" + ", ".join(
    f"{name} {column_type}" for name, column_type in PUBLICATION_LEDGER_COLUMN_TYPES
) + ");"


# ---- handoff metadata schema (ASAC-DAG#638 §2 — single source, all-domain shared) ----
# 자연키 upsert 전용(#638 §3): 테이블 전체 DROP/DELETE 금지. writer 는 공용 publisher 와
# commerce exporter 둘뿐이며 둘 다 아래 상수·빌더에서 같은 문장을 얻는다.
# verified_at/verified_publication_id 는 #638 §2.3 기준 필수이나 commerce 275건 백필 전이라
# NULL 허용으로 시작한다(백필 완료 후 NOT NULL 강화 — #638 §5-1 참조).

HANDOFF_COLUMN_TYPES: dict[str, tuple[tuple[str, str], ...]] = {
    "d1_catalog_columns": (
        ("product_id", "TEXT NOT NULL"), ("table_name", "TEXT NOT NULL"),
        ("ordinal", "INTEGER NOT NULL"), ("column_name", "TEXT NOT NULL"),
        ("type", "TEXT NOT NULL"), ("description_ko", "TEXT"),
        ("publication_id", "TEXT NOT NULL"),
    ),
    "d1_catalog_column_vocabularies": (
        ("product_id", "TEXT NOT NULL"), ("table_name", "TEXT NOT NULL"),
        ("column_name", "TEXT NOT NULL"), ("vocabulary_id", "TEXT NOT NULL"),
        ("publication_id", "TEXT NOT NULL"),
    ),
    "d1_catalog_ext": (
        ("product_id", "TEXT NOT NULL"), ("table_name", "TEXT NOT NULL"),
        ("source_model", "TEXT NOT NULL"), ("grain", "TEXT"),
        ("primary_key", "TEXT NOT NULL"),  # 두 writer 모두 항상 JSON 배열 문자열('[]' 포함)을 싣는다
        ("time_axis", "TEXT"),
        ("tier", "TEXT"), ("rollup_rule", "TEXT"),  # 물리 게시 확장 — 없는 도메인은 NULL(#638 §2.2)
        ("publication_id", "TEXT NOT NULL"),
    ),
    "d1_usage_patterns": (
        ("product_id", "TEXT NOT NULL"), ("pattern_id", "TEXT NOT NULL"),
        ("question_ko", "TEXT"), ("sql", "TEXT NOT NULL"), ("axes", "TEXT"),
        ("requires", "TEXT NOT NULL"), ("verified_rows", "INTEGER"),
        ("verified_at", "TEXT"), ("verified_publication_id", "TEXT"),
        ("allow_empty", "INTEGER NOT NULL DEFAULT 0"),
        ("insight_sample_ko", "TEXT"), ("publication_id", "TEXT NOT NULL"),
    ),
    # v1.11 (Serving#217 P1/P3): 패턴 파라미터 메타 — 기본값·허용값·타입 선언(JSON 문자열).
    # d1_usage_patterns 에 컬럼을 더하지 않고 **새 표**로 낸다(#706 display 와 같은 이유 —
    # handoff_schema_is_current 완전일치 검사 때문에 공유 표 컬럼 추가는 구 실행기가 되돌린다).
    # 게이트웨이는 이 표가 없으면 강등한다(전 파라미터 필수) — 게시가 늦어도 안전하다.
    "d1_pattern_params": (
        ("product_id", "TEXT NOT NULL"), ("pattern_id", "TEXT NOT NULL"),
        ("param_defaults", "TEXT"),      # JSON 객체 문자열 — {"gu": "ALL"} · 미선언 NULL
        ("param_enum", "TEXT"),          # JSON 객체 문자열 — {"dir": ["asc","desc"]}
        ("params", "TEXT"),              # JSON 객체 문자열 — {"gus": {"type":"array",...}}
        ("publication_id", "TEXT NOT NULL"),
    ),
    # v1.10 (#706): 사람이 읽는 표시 메타. **기존 표에 컬럼을 더하지 않고 새 표로 낸다** —
    # handoff_schema_is_current 가 컬럼 집합을 완전 일치로 보므로, 공유 표에 컬럼을 더하면
    # 구 코드를 가진 실행기가 자기가 아는 모양으로 되돌리며 그 컬럼을 삭제한다. 실행기가
    # 여러 대라 발행마다 왕복한다. 새 표는 구 코드가 존재조차 모르므로 그 왕복이 없다.
    "d1_catalog_display": (
        ("product_id", "TEXT NOT NULL"),
        ("title", "TEXT"), ("summary", "TEXT"), ("caveat", "TEXT"),
        ("use_cases", "TEXT"),          # JSON 배열 문자열 — 미선언은 NULL
        ("publication_id", "TEXT NOT NULL"),
    ),
    "d1_catalog_glossary": (
        ("vocabulary_id", "TEXT NOT NULL"), ("code", "TEXT NOT NULL"),
        ("label_ko", "TEXT NOT NULL"), ("origin", "TEXT NOT NULL"),
        ("source_type", "TEXT NOT NULL"), ("exported_at", "TEXT NOT NULL"),
    ),
}
HANDOFF_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "d1_catalog_columns": ("product_id", "column_name"),
    "d1_catalog_column_vocabularies": ("product_id", "column_name"),
    "d1_catalog_ext": ("product_id",),
    "d1_usage_patterns": ("product_id", "pattern_id"),
    "d1_pattern_params": ("product_id", "pattern_id"),
    "d1_catalog_display": ("product_id",),
    "d1_catalog_glossary": ("vocabulary_id", "code"),
}
# 잔여 행 정리(#638 §3 ②)의 스코프 컬럼. 판별 방식은 테이블 성질로 갈린다:
#  - columns/patterns(복합 자연키): **이번 선언 키셋 기준 NOT IN**(HANDOFF_PRUNE_KEYS).
#    publication_id 부등 판별이 아닌 이유 — 무변경 게이트(#601)가 publication_id 를 재사용하면
#    부등 삭제가 no-op 이 되어, yml 에서 지운 패턴이 같은 id 로 잔존한다(리뷰 확정 결함).
#  - ext(단일 키 = 스코프): upsert 가 곧 전체 교체라 publication_id 부등 판별로 충분.
#  - glossary: 제품 스코프가 아니고 exported_at 이 run 마다 새 값이라 부등 판별이 유효.
HANDOFF_SCOPE_COLUMNS: dict[str, str] = {
    "d1_catalog_columns": "product_id",
    "d1_catalog_column_vocabularies": "product_id",
    "d1_catalog_ext": "product_id",
    "d1_usage_patterns": "product_id",
    "d1_pattern_params": "product_id",
    "d1_catalog_display": "product_id",
    "d1_catalog_glossary": "vocabulary_id",
}
HANDOFF_PRUNE_KEYS: dict[str, str] = {
    "d1_catalog_columns": "column_name",
    "d1_catalog_column_vocabularies": "column_name",
    "d1_usage_patterns": "pattern_id",
    "d1_pattern_params": "pattern_id",
}
HANDOFF_STALE_MARKERS: dict[str, str] = {
    "d1_catalog_ext": "publication_id",
    "d1_catalog_display": "publication_id",
    "d1_catalog_glossary": "exported_at",
}
HANDOFF_PRODUCT_TABLES = (
    "d1_catalog_columns", "d1_catalog_column_vocabularies", "d1_catalog_ext", "d1_usage_patterns", "d1_catalog_display",
    "d1_pattern_params",
)
HANDOFF_COLUMNS = {table: tuple(name for name, _ in cols) for table, cols in HANDOFF_COLUMN_TYPES.items()}


# ---- V1 evidence schema (#678) -----------------------------------------------------
# Source/right records and runtime quality are intentionally separate from the #638
# metadata tables. A product can have multiple sources while it has exactly one active
# quality snapshot per publication. The D1 tables are published copies; dbt manifest
# fields remain the source of truth and D1 is never hand-edited.
EVIDENCE_COLUMN_TYPES: dict[str, tuple[tuple[str, str], ...]] = {
    "d1_catalog_sources": (
        ("product_id", "TEXT NOT NULL"), ("source_id", "TEXT NOT NULL"),
        ("source_url", "TEXT NOT NULL"), ("license", "TEXT NOT NULL"),
        ("license_url", "TEXT NOT NULL"), ("redistribution", "TEXT NOT NULL"),
        ("attribution", "TEXT NOT NULL"), ("rights_checked_at", "TEXT NOT NULL"),
        ("publication_id", "TEXT NOT NULL"),
    ),
    "d1_product_quality": (
        ("product_id", "TEXT NOT NULL"), ("source_row_count", "INTEGER NOT NULL"),
        ("d1_row_count", "INTEGER NOT NULL"), ("duplicate_primary_key_count", "INTEGER NOT NULL"),
        ("null_primary_key_count", "INTEGER NOT NULL"), ("freshness_as_of", "TEXT"),
        ("freshness_slo_minutes", "INTEGER"), ("serving_status", "TEXT NOT NULL"),
        ("measured_at", "TEXT NOT NULL"), ("coverage_json", "TEXT"),
        ("projection_schema_version", "TEXT"), ("projection_schema_hash", "TEXT"),
        ("publication_id", "TEXT NOT NULL"),
    ),
}
EVIDENCE_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "d1_catalog_sources": ("product_id", "source_id"),
    "d1_product_quality": ("product_id",),
}
EVIDENCE_COLUMNS = {table: tuple(name for name, _ in columns) for table, columns in EVIDENCE_COLUMN_TYPES.items()}
# The first #678 quality schema had no public-projection identity. Only this exact,
# additive predecessor can be migrated automatically; every other mismatch must stop
# for an explicit reviewed migration rather than guessing at user data.
_EVIDENCE_MIGRATABLE_PREDECESSORS: dict[str, tuple[str, ...]] = {
    "d1_product_quality": tuple(
        name for name in EVIDENCE_COLUMNS["d1_product_quality"]
        if name not in {"projection_schema_version", "projection_schema_hash"}
    ),
}


def evidence_ddl(table: str) -> str:
    cols = ", ".join(f'"{name}" {column_type}' for name, column_type in EVIDENCE_COLUMN_TYPES[table])
    key_columns = '", "'.join(EVIDENCE_PRIMARY_KEYS[table])
    return f'CREATE TABLE IF NOT EXISTS "{table}" ({cols}, PRIMARY KEY ("{key_columns}"));'


def evidence_schema_is_current(table: str, pragma_rows: Sequence[dict[str, Any]]) -> bool:
    if not pragma_rows:
        return False
    names = {str(row.get("name")) for row in pragma_rows}
    key_rows = [row for row in pragma_rows if int(row.get("pk") or 0) > 0]
    key_columns = tuple(
        str(row.get("name")) for row in sorted(key_rows, key=lambda row: int(row.get("pk") or 0))
    )
    return names == set(EVIDENCE_COLUMNS[table]) and key_columns == EVIDENCE_PRIMARY_KEYS[table]


def evidence_schema_is_migratable(table: str, pragma_rows: Sequence[dict[str, Any]]) -> bool:
    """Allow exactly the additive #678 v1 predecessor; reject all unknown shapes."""
    predecessor = _EVIDENCE_MIGRATABLE_PREDECESSORS.get(table)
    if not predecessor:
        return False
    names = {str(row.get("name")) for row in pragma_rows}
    key_rows = [row for row in pragma_rows if int(row.get("pk") or 0) > 0]
    key_columns = tuple(
        str(row.get("name")) for row in sorted(key_rows, key=lambda row: int(row.get("pk") or 0))
    )
    return names == set(predecessor) and key_columns == EVIDENCE_PRIMARY_KEYS[table]


def evidence_migrate_statements(table: str, existing_columns: Sequence[str]) -> str:
    """Row-preserving one-time migration for the known additive evidence predecessor."""
    if table not in _EVIDENCE_MIGRATABLE_PREDECESSORS:
        raise ValueError(f"{table}: no automatic evidence migration is defined")
    legacy = f"{table}__migrate"
    present = set(existing_columns)
    target_columns = EVIDENCE_COLUMNS[table]
    select_exprs = [f'"{name}"' if name in present else "NULL" for name in target_columns]
    quoted_columns = '", "'.join(target_columns)
    return (
        f'DROP TABLE IF EXISTS "{legacy}"; '
        f'ALTER TABLE "{table}" RENAME TO "{legacy}"; '
        + evidence_ddl(table) + " "
        f'INSERT OR REPLACE INTO "{table}" ("{quoted_columns}") '
        f'SELECT {", ".join(select_exprs)} FROM "{legacy}"; '
        f'DROP TABLE IF EXISTS "{legacy}";'
    )


def evidence_upsert_statements(table: str, rows: Sequence[dict[str, Any]]) -> list[str]:
    columns: list[Column] = [(name, column_type) for name, column_type in EVIDENCE_COLUMN_TYPES[table]]
    return build_insert_statements(table, columns, rows, replace=True)


def evidence_source_prune_statement(product_id: str, source_ids: Sequence[str]) -> str:
    base = f'DELETE FROM "d1_catalog_sources" WHERE "product_id" = {sql_literal(product_id)}'
    if not source_ids:
        return base + ";"
    identifiers = ", ".join(sql_literal(source_id) for source_id in source_ids)
    return f'{base} AND "source_id" NOT IN ({identifiers});'


def handoff_ddl(table: str) -> str:
    cols = ", ".join(f'"{name}" {column_type}' for name, column_type in HANDOFF_COLUMN_TYPES[table])
    key_columns = '", "'.join(HANDOFF_PRIMARY_KEYS[table])
    return f'CREATE TABLE IF NOT EXISTS "{table}" ({cols}, PRIMARY KEY ("{key_columns}"));'


def handoff_schema_is_current(table: str, pragma_rows: Sequence[dict[str, Any]]) -> bool:
    """``PRAGMA table_info`` 결과가 #638 v1 스키마인지 — 컬럼 집합과 자연키를 함께 본다.

    전량 교체 시절 테이블은 컬럼 집합이 같아도 PRIMARY KEY 가 없어(upsert 충돌 기준 부재)
    반드시 걸러야 한다. pragma_rows 가 비면 테이블 부재.
    """
    if not pragma_rows:
        return False
    names = {str(row.get("name")) for row in pragma_rows}
    key_rows = [row for row in pragma_rows if int(row.get("pk") or 0) > 0]
    key_columns = tuple(
        str(row.get("name")) for row in sorted(key_rows, key=lambda row: int(row.get("pk") or 0))
    )
    return names == set(HANDOFF_COLUMNS[table]) and key_columns == HANDOFF_PRIMARY_KEYS[table]


# 레거시 → v1 이행 복사에서 v1 컬럼이 레거시에 없을 때 쓰는 기본값 식. NOT NULL 컬럼은
# 반드시 여기 있어야 이행이 깨지지 않는다. glossary 는 field → 'commerce:' 네임스페이스 변환
# (구 스키마의 writer 는 commerce 단독이었으므로 출처가 자명하다 — #638 §2.4).
_MIGRATE_DEFAULTS: dict[tuple[str, str], str] = {
    ("d1_catalog_columns", "publication_id"): "''",
    ("d1_catalog_ext", "publication_id"): "''",
    ("d1_catalog_ext", "primary_key"): "'[]'",
    ("d1_usage_patterns", "publication_id"): "''",
    ("d1_usage_patterns", "requires"): "'[]'",
    ("d1_usage_patterns", "allow_empty"): "0",
    ("d1_catalog_glossary", "origin"): "'commerce'",
    ("d1_catalog_glossary", "source_type"): "'warehouse'",
    ("d1_catalog_glossary", "exported_at"): "''",
}
_MIGRATE_RENAMES: dict[tuple[str, str], tuple[str, str]] = {
    ("d1_catalog_glossary", "vocabulary_id"): ("field", "'commerce:' || \"field\""),
}


def handoff_migrate_statements(table: str, existing_columns: Sequence[str]) -> str:
    """레거시(전량 교체 시절) → v1 자연키 스키마 **행 보존** 이행(1회, #638 §4).

    DROP 재생성이 아니라 rename → create → 복사 → drop: 이행 run 에 게시되지 않는 제품
    (밴드 스킵)의 직전 메타도 살아남는다. 복사는 INSERT OR REPLACE 라 레거시 중복 행도
    자연키 기준으로 정리된다. 전 문장을 한 요청으로 보내 중간 상태 노출을 줄인다
    (replace_table 의 2-ALTER 단일 요청과 동일 관행).
    """
    legacy = f"{table}__migrate"
    present = set(existing_columns)
    select_exprs = []
    for name in HANDOFF_COLUMNS[table]:
        rename = _MIGRATE_RENAMES.get((table, name))
        if name in present:
            select_exprs.append(f'"{name}"')
        elif rename and rename[0] in present:
            select_exprs.append(rename[1])
        else:
            select_exprs.append(_MIGRATE_DEFAULTS.get((table, name), "NULL"))
    column_names = '", "'.join(HANDOFF_COLUMNS[table])
    return (
        f'DROP TABLE IF EXISTS "{legacy}"; '
        f'ALTER TABLE "{table}" RENAME TO "{legacy}"; '
        + handoff_ddl(table) + " "
        f'INSERT OR REPLACE INTO "{table}" ("{column_names}") '
        f'SELECT {", ".join(select_exprs)} FROM "{legacy}"; '
        f'DROP TABLE IF EXISTS "{legacy}";'
    )


def handoff_upsert_statements(table: str, rows: Sequence[dict[str, Any]]) -> list[str]:
    """#638 §3 ① 자연키 upsert. 항상 전 컬럼을 싣는 전제라 ``INSERT OR REPLACE`` 가
    ``ON CONFLICT DO UPDATE`` 와 결과 동치다(_catalog 처럼 보존할 레거시 컬럼이 없다)."""
    columns: list[Column] = [(name, "TEXT") for name in HANDOFF_COLUMNS[table]]
    return build_insert_statements(table, columns, rows, replace=True)


def handoff_stale_delete_statement(table: str, scope_value: str, current_marker: str) -> str:
    """#638 §3 ② (ext·glossary) — 같은 스코프에서 이번 게시본이 아닌 잔여 행 제거."""
    scope_column = HANDOFF_SCOPE_COLUMNS[table]
    marker_column = HANDOFF_STALE_MARKERS[table]
    return (
        f'DELETE FROM "{table}" WHERE "{scope_column}" = {sql_literal(scope_value)} '
        f'AND "{marker_column}" <> {sql_literal(current_marker)};'
    )


# ---- glossary registry (#638 §2.4 — 게시 시 검증 기준. D1 컬럼이 아니라 공용 계약이다) ----
# vocabulary_id 마다 쓰기 도메인(owner) 하나 — 미등록 어휘는 게시 거부(#638 §5-5). 등재는 취합
# 담당(commerce)의 인벤토리 절차를 거친 것만(#638 §2.4); 타 도메인 어휘(culture:* 등)는 그
# 도메인 온보딩 PR 에서 추가한다. origin = 취합 전 라벨이 있던 곳(도메인/공용 축 패키지).
GLOSSARY_REGISTRY: dict[str, dict[str, str]] = {
    "commerce:major":      {"owner": "commerce", "origin": "commerce",  "source_type": "warehouse"},
    "commerce:category":   {"owner": "commerce", "origin": "commerce",  "source_type": "warehouse"},
    "commerce:event_type": {"owner": "commerce", "origin": "commerce",  "source_type": "warehouse"},
    # 공통 축(#638 §2.4 승격): 게시(취합)는 commerce 가 맡되 정본은 공용 축 패키지의
    # 라이브 행안부 마스터(asac_axes.dim_admin_dong) — commerce 자체 스냅샷 파생이 아니다.
    "common:gu_code":      {"owner": "commerce", "origin": "asac_axes", "source_type": "warehouse"},
    "weather:sky_code":    {"owner": "traffic_weather", "origin": "traffic_weather", "source_type": "dbt_contract"},
    "weather:pty_code":    {"owner": "traffic_weather", "origin": "traffic_weather", "source_type": "dbt_contract"},
    "traffic:flow_value_quality": {"owner": "traffic_weather", "origin": "traffic_weather", "source_type": "dbt_contract"},
    "traffic:hotspot_state": {"owner": "traffic_weather", "origin": "traffic_weather", "source_type": "dbt_contract"},
}


def glossary_registry_violations(rows: Sequence[dict[str, Any]]) -> dict[str, str]:
    """vocabulary_id → 거부 사유. 미등록이거나 레지스트리의 origin/source_type 과 불일치."""
    violations: dict[str, str] = {}
    for row in rows:
        vocabulary_id = str(row.get("vocabulary_id") or "")
        entry = GLOSSARY_REGISTRY.get(vocabulary_id)
        if entry is None:
            violations[vocabulary_id] = "레지스트리 미등록"
        elif (row.get("origin"), row.get("source_type")) != (entry["origin"], entry["source_type"]):
            violations[vocabulary_id] = (
                f"레지스트리 불일치: origin/source_type={row.get('origin')}/{row.get('source_type')} "
                f"(정본 {entry['origin']}/{entry['source_type']})"
            )
    return violations


def handoff_prune_statement(table: str, scope_value: str, keep_keys: Sequence[str]) -> str:
    """#638 §3 ② (columns·patterns) — 이번 선언에 없는 자연키 행 제거(키셋 NOT IN).

    publication_id 부등 판별을 쓰지 않는 이유: commerce 무변경 게이트(#601)가 내용 불변 run 에
    publication_id 를 재사용하므로, 부등 삭제는 그 run 에서 no-op 이 되어 yml 에서 지운 패턴이
    같은 id 로 무기한 잔존한다. 선언 키셋 기준이면 재사용 여부와 무관하게 정리된다.
    keep_keys 가 비면 스코프 전체 삭제(그 제품이 이번 선언에서 항목을 전부 지운 경우).
    """
    scope_column = HANDOFF_SCOPE_COLUMNS[table]
    key_column = HANDOFF_PRUNE_KEYS[table]
    base = f'DELETE FROM "{table}" WHERE "{scope_column}" = {sql_literal(scope_value)}'
    if not keep_keys:
        return base + ";"
    keeps = ", ".join(sql_literal(key) for key in keep_keys)
    return f'{base} AND "{key_column}" NOT IN ({keeps});'


class HttpD1Client:
    """Cloudflare D1 HTTP API implementation. Constructed from env by the DAG factory."""

    def __init__(
        self,
        api_url: str,
        token: str,
        *,
        max_attempts: int = D1_MAX_ATTEMPTS,
        sleep_fn: Callable[[float], None] | None = None,
        random_fn: Callable[[], float] | None = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("D1 max_attempts must be at least 1")
        self._api_url = api_url
        self._token = token  # never logged
        self._max_attempts = max_attempts
        self._sleep = sleep_fn or time.sleep
        self._random = random_fn or random.random
        self._handoff_ready: set[str] = set()  # per-run schema check cache
        self._evidence_ready: set[str] = set()  # #678 evidence schema cache

    def _retry_delay(self, attempt: int) -> float:
        exponential = min(
            D1_RETRY_MAX_SECONDS,
            D1_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
        )
        return exponential + (self._random() * D1_RETRY_BASE_SECONDS)

    def _request(
        self,
        body: dict[str, Any],
        *,
        retry_safe: bool = False,
    ) -> dict[str, Any]:
        import requests  # lazy import so tests never need it

        allowed_attempts = self._max_attempts if retry_safe else 1
        for attempt in range(1, allowed_attempts + 1):
            try:
                resp = requests.post(
                    self._api_url,
                    json=body,
                    headers={"Authorization": f"Bearer {self._token}"},
                    timeout=120,
                )
            except Exception as exc:  # noqa: BLE001 - converted to a sanitized boundary error
                transient = _transient_exception(exc)
                error = D1RequestError(
                    http_status=None,
                    attempt=attempt,
                    exception_type=type(exc).__name__,
                    transient=transient,
                )
                if retry_safe and transient and attempt < allowed_attempts:
                    delay = self._retry_delay(attempt)
                    log.warning(
                        "D1 transient request retry attempt=%s/%s http_status=unknown "
                        "codes=none cf_ray=none exception_type=%s delay_seconds=%.3f",
                        attempt,
                        allowed_attempts,
                        type(exc).__name__,
                        delay,
                    )
                    self._sleep(delay)
                    continue
                raise error from exc

            raw_http_status = getattr(resp, "status_code", None)
            http_status = raw_http_status if isinstance(raw_http_status, int) else None
            cf_ray = _safe_cf_ray(getattr(resp, "headers", None))
            response: dict[str, Any] | None = None
            exception_type: str | None = None
            try:
                candidate = resp.json()
                if isinstance(candidate, dict):
                    response = candidate
                else:
                    exception_type = "InvalidJSONShape"
            except Exception as exc:  # noqa: BLE001 - body is deliberately not surfaced
                exception_type = type(exc).__name__

            result = (response or {}).get("result") or []
            failed_statements = [
                statement
                for statement in result
                if isinstance(statement, dict) and statement.get("success") is False
            ]
            succeeded = (
                http_status is not None
                and 200 <= http_status <= 299
                and response is not None
                and response.get("success") is True
                and not failed_statements
            )
            if succeeded:
                return response

            error_codes = _d1_error_codes(response or {})
            transient = _transient_failure(http_status, error_codes)
            error = D1RequestError(
                http_status=http_status,
                error_codes=error_codes,
                cf_ray=cf_ray,
                attempt=attempt,
                exception_type=exception_type,
                transient=transient,
            )
            if retry_safe and transient and attempt < allowed_attempts:
                delay = self._retry_delay(attempt)
                log.warning(
                    "D1 transient request retry attempt=%s/%s http_status=%s "
                    "codes=%s cf_ray=%s exception_type=%s delay_seconds=%.3f",
                    attempt,
                    allowed_attempts,
                    http_status if http_status is not None else "unknown",
                    ",".join(error_codes) if error_codes else "none",
                    cf_ray or "none",
                    exception_type or "none",
                    delay,
                )
                self._sleep(delay)
                continue
            raise error

        raise AssertionError("D1 request retry loop exhausted without a result")

    def _query(
        self,
        sql: str,
        *,
        retry_safe: bool | None = None,
    ) -> list[dict[str, Any]]:
        if retry_safe is None:
            retry_safe = _read_only_sql(sql)
        response = self._request({"sql": sql}, retry_safe=retry_safe)
        result = response.get("result") or []
        failed_statements = [statement for statement in result if statement.get("success") is False]
        if failed_statements:
            raise D1RequestError(
                http_status=200,
                error_codes=_d1_error_codes(response),
                attempt=1,
            )
        return (result[-1].get("results") or []) if result else []

    def _query_batch(
        self,
        statements: Sequence[str],
        *,
        retry_safe: bool = False,
    ) -> list[list[dict[str, Any]]]:
        if not statements:
            return []
        response = self._request(
            {"batch": [{"sql": statement} for statement in statements]},
            retry_safe=retry_safe,
        )
        result = response.get("result") or []
        if len(result) != len(statements) or any(not item.get("success") for item in result):
            raise RuntimeError("D1 API batch 일부 statement 실패")
        return [(item.get("results") or []) for item in result]

    def _validate_atomic_batch(self, statements: Sequence[str]) -> tuple[str, ...]:
        if not statements:
            raise ValueError("atomic batch requires at least one statement")
        if len(statements) > MAX_ACTIVATION_STATEMENTS:
            raise ValueError(f"atomic batch exceeds statement budget {MAX_ACTIVATION_STATEMENTS}")
        if any(_utf8_bytes(statement) > MAX_SQL_STATEMENT_BYTES for statement in statements):
            raise ValueError(f"atomic batch statement exceeds SQL byte budget {MAX_SQL_STATEMENT_BYTES}")
        if _api_batch_bytes(statements) > MAX_ACTIVATION_API_BATCH_BYTES:
            raise ValueError(f"atomic batch exceeds API batch byte budget {MAX_ACTIVATION_API_BATCH_BYTES}")
        return tuple(statements)

    def _query_atomic_batch(self, statements: Sequence[str]) -> list[list[dict[str, Any]]]:
        return self._query_batch(self._validate_atomic_batch(statements))

    def _create_ddl(
        self,
        name: str,
        columns: Sequence[Column],
        primary_key: Sequence[str] = (),
    ) -> str:
        if not primary_key:
            raise ValueError(f"{name}: primary_key is required for D1 publication")
        cols = ", ".join(f'"{c}" {sqlite_type(t)}' for c, t in columns)
        key_columns = '", "'.join(primary_key)
        cols += f', UNIQUE ("{key_columns}")'
        return f'CREATE TABLE IF NOT EXISTS "{name}" ({cols});'

    def _ensure_unique_primary_key(self, name: str, primary_key: Sequence[str]) -> None:
        if not primary_key:
            raise ValueError(f"{name}: primary_key is required for D1 publication")
        columns = '", "'.join(primary_key)
        self._query(
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{name}__pk_uq" '
            f'ON "{name}" ("{columns}");',
            retry_safe=True,
        )

    def _table_exists(self, name: str) -> bool:
        out = self._query(
            "SELECT name FROM sqlite_master "
            f"WHERE type = 'table' AND name = {sql_literal(name)};"
        )
        return bool(out)

    def _insert_batches(self, name: str, columns: Sequence[Column], rows: Sequence[dict[str, Any]], *, replace: bool) -> None:
        statements = build_insert_statements(name, columns, rows, replace=replace)
        for batch in group_api_batches(statements):
            self._query_batch(batch, retry_safe=replace)

    def execute(self, sql: str) -> list[dict[str, Any]]:
        """Run one statement and return the last statement's rows.

        Public seam for callers that own their SQL — the ops-record loader builds every
        statement from `common.ops.d1_ops` (constant identifiers, escaped values), so it
        needs a plain execute without reaching into the private `_query`.
        """
        return self._query(sql)

    def table_row_count(self, name: str) -> int:
        try:
            out = self._query(f'SELECT count(*) c FROM "{name}";')
        except RuntimeError:
            return 0  # table absent
        return int(out[0].get("c", 0)) if out else 0

    def primary_key_stats(self, name: str, primary_key: Sequence[str]) -> tuple[int, int, int]:
        if not primary_key:
            raise ValueError(f"{name}: primary_key is required for D1 read-back")
        columns = '", "'.join(primary_key)
        null_predicate = " OR ".join(f'"{column}" IS NULL' for column in primary_key)
        out = self._query(
            f'SELECT '
            f'(SELECT count(*) FROM "{name}") AS row_count, '
            f'(SELECT count(*) FROM (SELECT "{columns}" FROM "{name}" GROUP BY "{columns}")) '
            f'AS distinct_primary_key_count, '
            f'(SELECT count(*) FROM "{name}" WHERE {null_predicate}) AS null_primary_key_count;'
        )
        row = out[0] if out else {}
        return (
            int(row.get("row_count", 0)),
            int(row.get("distinct_primary_key_count", 0)),
            int(row.get("null_primary_key_count", 0)),
        )

    def table_max(self, name: str, column: str) -> Any | None:
        try:
            out = self._query(f'SELECT max("{column}") m FROM "{name}";')
        except RuntimeError:
            return None
        return out[0].get("m") if out else None

    def catalog_row(self, name: str) -> dict[str, Any] | None:
        try:
            out = self._query(f"SELECT * FROM _catalog WHERE name = {sql_literal(name)};")
        except RuntimeError:
            return None
        return out[0] if out else None

    def ensure_table(self, name: str, columns: Sequence[Column], primary_key: Sequence[str]) -> None:
        cols = ", ".join(f'"{c}" {sqlite_type(t)}' for c, t in columns)
        self._query(
            f'CREATE TABLE IF NOT EXISTS "{name}" ({cols});',
            retry_safe=True,
        )
        self._ensure_unique_primary_key(name, primary_key)

    def replace_table(
        self,
        name: str,
        columns: Sequence[Column],
        rows: Sequence[dict[str, Any]],
        primary_key: Sequence[str],
    ) -> None:
        # Staging populate first; only swap after a full successful load so a mid-load
        # failure leaves the previous published table (last-known-good) untouched.
        staging = f"{name}__staging"
        self._query(f'DROP TABLE IF EXISTS "{staging}";', retry_safe=True)
        # A table-level UNIQUE constraint survives the staging rename without a
        # global index-name collision on the next snapshot replacement.
        self._query(
            self._create_ddl(staging, columns, primary_key),
            retry_safe=True,
        )
        # Source primary keys are validated before this boundary. OR REPLACE makes
        # a response-lost retry of the current staging batch idempotent.
        self._insert_batches(staging, columns, rows, replace=True)
        previous = f"{name}__previous"
        if self._table_exists(previous):
            raise RuntimeError(f"{name}: unfinished previous snapshot exists; restore or finalize it before publishing")
        if self._table_exists(name):
            self._query(
                f'ALTER TABLE "{name}" RENAME TO "{previous}"; '
                f'ALTER TABLE "{staging}" RENAME TO "{name}";'
            )
        else:
            self._query(f'ALTER TABLE "{staging}" RENAME TO "{name}";')

    def stage_snapshot(
        self, name: str, columns: Sequence[Column], rows: Sequence[dict[str, Any]], primary_key: Sequence[str]
    ) -> None:
        staging = f"{name}__staging"
        self._query(f'DROP TABLE IF EXISTS "{staging}";', retry_safe=True)
        self._query(
            self._create_ddl(staging, columns, primary_key),
            retry_safe=True,
        )
        self._insert_batches(staging, columns, rows, replace=True)

    def read_staged_snapshot_rows(
        self, name: str, columns: Sequence[Column], primary_key: Sequence[str]
    ) -> list[dict[str, Any]]:
        return self.read_table_rows(f"{name}__staging", columns, primary_key)

    def _ensure_query_availability_schema(self) -> None:
        existing = self._query(f'PRAGMA table_info("{QUERY_AVAILABILITY_TABLE}");')
        if existing:
            expected_by_name = {
                name: (kind.split()[0].upper(), "NOT NULL" in kind.upper())
                for name, kind in QUERY_AVAILABILITY_COLUMNS
            }
            actual_names = {str(row.get("name")) for row in existing}
            actual_key = tuple(
                str(row.get("name")) for row in sorted(
                    (row for row in existing if int(row.get("pk") or 0) > 0),
                    key=lambda row: int(row.get("pk") or 0),
                )
            )
            shape_valid = actual_names == set(expected_by_name) and all(
                str(row.get("type") or "").strip().upper() == expected_by_name[str(row.get("name"))][0]
                and bool(int(row.get("notnull") or 0)) == expected_by_name[str(row.get("name"))][1]
                for row in existing
            )
            if not shape_valid or actual_key != QUERY_AVAILABILITY_PRIMARY_KEY:
                raise RuntimeError(
                    f"{QUERY_AVAILABILITY_TABLE}: schema mismatch; explicit migration is required"
                )
            return
        columns = ", ".join(f'"{name}" {kind}' for name, kind in QUERY_AVAILABILITY_COLUMNS)
        primary_key = '", "'.join(QUERY_AVAILABILITY_PRIMARY_KEY)
        self._query(
            f'CREATE TABLE IF NOT EXISTS "{QUERY_AVAILABILITY_TABLE}" '
            f'({columns}, PRIMARY KEY ("{primary_key}"));',
            retry_safe=True,
        )

    def prepare_atomic_publication_schema(self) -> None:
        self._ensure_catalog_schema()
        for table in HANDOFF_PRODUCT_TABLES:
            self._ensure_handoff_schema(table)
        self._ensure_evidence_schema("d1_catalog_sources")
        self._ensure_evidence_schema("d1_product_quality")

    def stage_query_availability(
        self, product_id: str, publication_id: str, rows: Sequence[dict[str, Any]], *, fingerprint: str, measured_at: str
    ) -> None:
        self._ensure_query_availability_schema()
        enriched = [dict(row, product_id=product_id, publication_id=publication_id,
                         availability_fingerprint=fingerprint, measured_at=measured_at) for row in rows]
        columns = [(name, kind.split()[0].lower()) for name, kind in QUERY_AVAILABILITY_COLUMNS]
        self._insert_batches(QUERY_AVAILABILITY_TABLE, columns, enriched, replace=True)

    def read_query_availability_rows(self, product_id: str, publication_id: str) -> list[dict[str, Any]]:
        self._ensure_query_availability_schema()
        columns = ", ".join(f'"{name}"' for name, _kind in QUERY_AVAILABILITY_COLUMNS)
        return self._query(
            f'SELECT {columns} FROM "{QUERY_AVAILABILITY_TABLE}" '
            f'WHERE "product_id" = {sql_literal(product_id)} AND "publication_id" = {sql_literal(publication_id)} '
            'ORDER BY "place_id";'
        )

    def capture_product_publication_state(self, product_id: str, model_name: str) -> ProductPublicationState:
        """Read the exact product-scoped LKG evidence before candidate activation."""
        metadata: dict[str, tuple[dict[str, Any], ...]] = {}
        for table in HANDOFF_PRODUCT_TABLES:
            metadata[table] = tuple(self._query(
                f'SELECT * FROM "{table}" WHERE "product_id" = {sql_literal(product_id)};'
            ))
        sources = tuple(self._query(
            f'SELECT * FROM "d1_catalog_sources" WHERE "product_id" = {sql_literal(product_id)};'
        ))
        quality_rows = self._query(
            f'SELECT * FROM "d1_product_quality" WHERE "product_id" = {sql_literal(product_id)};'
        )
        catalog_rows = self._query(f"SELECT * FROM _catalog WHERE name = {sql_literal(model_name)};")
        return ProductPublicationState(
            catalog_row=catalog_rows[0] if catalog_rows else None, metadata_rows=metadata,
            source_rows=sources, quality_row=quality_rows[0] if quality_rows else None,
            active_table_exists=self._table_exists(model_name),
        )

    def _catalog_upsert_statement(self, row: dict[str, Any]) -> str:
        column_names = '", "'.join(CATALOG_COLUMNS)
        update_columns = ", ".join(
            f'"{column}" = excluded."{column}"' for column in CATALOG_COLUMNS if column != "name"
        )
        values = ", ".join(sql_literal(row.get(column)) for column in CATALOG_COLUMNS)
        return f'INSERT INTO _catalog ("{column_names}") VALUES ({values}) ON CONFLICT("name") DO UPDATE SET {update_columns};'

    def _replace_state_evidence_statements(self, product_id: str, state: ProductPublicationState) -> list[str]:
        statements: list[str] = []
        for table in HANDOFF_PRODUCT_TABLES:
            statements.append(f'DELETE FROM "{table}" WHERE "product_id" = {sql_literal(product_id)};')
            statements.extend(handoff_upsert_statements(table, state.metadata_rows.get(table, ())))
        statements.append(f'DELETE FROM "d1_catalog_sources" WHERE "product_id" = {sql_literal(product_id)};')
        statements.extend(evidence_upsert_statements("d1_catalog_sources", state.source_rows))
        statements.append(f'DELETE FROM "d1_product_quality" WHERE "product_id" = {sql_literal(product_id)};')
        if state.quality_row is not None:
            statements.extend(evidence_upsert_statements("d1_product_quality", [state.quality_row]))
        return statements

    def build_activation_statements(self, product_id: str, model_name: str, candidate: ProductPublicationState) -> tuple[str, ...]:
        previous = f"{model_name}__previous"
        staging = f"{model_name}__staging"
        statements = (
            [f'ALTER TABLE "{model_name}" RENAME TO "{previous}";', f'ALTER TABLE "{staging}" RENAME TO "{model_name}";']
            if candidate.active_table_exists else [f'ALTER TABLE "{staging}" RENAME TO "{model_name}";']
        )
        statements.extend(self._replace_state_evidence_statements(product_id, candidate))
        if candidate.catalog_row is not None:
            statements.append(self._catalog_upsert_statement(candidate.catalog_row))
        return tuple(statements)

    def build_compensation_statements(self, product_id: str, model_name: str, previous: ProductPublicationState) -> tuple[str, ...]:
        prior = f"{model_name}__previous"
        statements: list[str] = [f'DROP TABLE IF EXISTS "{model_name}";']
        if previous.active_table_exists:
            statements.append(f'ALTER TABLE "{prior}" RENAME TO "{model_name}";')
        statements.extend(self._replace_state_evidence_statements(product_id, previous))
        if previous.catalog_row is None:
            statements.append(f'DELETE FROM _catalog WHERE "name" = {sql_literal(model_name)};')
        else:
            statements.append(self._catalog_upsert_statement(previous.catalog_row))
        return tuple(statements)

    def preflight_staged_transition(self, product_id: str, model_name: str, candidate: ProductPublicationState, previous: ProductPublicationState) -> None:
        self._validate_atomic_batch(self.build_activation_statements(product_id, model_name, candidate))
        self._validate_atomic_batch(self.build_compensation_statements(product_id, model_name, previous))

    def activate_staged_snapshot(self, product_id: str, model_name: str, candidate: ProductPublicationState) -> None:
        self._query_atomic_batch(self.build_activation_statements(product_id, model_name, candidate))

    def compensate_staged_snapshot(self, product_id: str, model_name: str, previous: ProductPublicationState) -> None:
        self._query_atomic_batch(self.build_compensation_statements(product_id, model_name, previous))

    def restore_replaced_table(self, name: str) -> None:
        previous = f"{name}__previous"
        if self._table_exists(previous):
            self._query(
                f'DROP TABLE IF EXISTS "{name}"; '
                f'ALTER TABLE "{previous}" RENAME TO "{name}";'
            )
        else:
            self._query(f'DROP TABLE IF EXISTS "{name}";')

    def finalize_replaced_table(self, name: str) -> None:
        self._query(
            f'DROP TABLE IF EXISTS "{name}__previous";',
            retry_safe=True,
        )

    def delete_where_gte(self, name: str, column: str, trino_literal: str) -> None:
        self._query(
            f'DELETE FROM "{name}" WHERE "{column}" >= {trino_literal};',
            retry_safe=True,
        )

    def insert_rows(self, name: str, columns: Sequence[Column], rows: Sequence[dict[str, Any]], *, replace: bool) -> None:
        self._insert_batches(name, columns, rows, replace=replace)

    def _catalog_column_names(self) -> set[str]:
        return {str(row["name"]) for row in self._query("PRAGMA table_info(_catalog);")}

    def _ensure_catalog_schema(self) -> None:
        self._query(CATALOG_DDL, retry_safe=True)
        existing = self._catalog_column_names()
        for name, column_type in CATALOG_COLUMN_TYPES:
            if name in existing:
                continue
            try:
                self._query(f'ALTER TABLE _catalog ADD COLUMN "{name}" {column_type};')
            except RuntimeError:
                # Another publisher may have added the same column between PRAGMA and ALTER.
                if name not in self._catalog_column_names():
                    raise

    def upsert_catalog(self, catalog_rows: Sequence[dict[str, Any]]) -> None:
        self._ensure_catalog_schema()
        column_names = '", "'.join(CATALOG_COLUMNS)
        update_columns = ", ".join(
            f'"{column}" = excluded."{column}"'
            for column in CATALOG_COLUMNS
            if column != "name"
        )
        for row in catalog_rows:
            values = ", ".join(sql_literal(row.get(col)) for col in CATALOG_COLUMNS)
            # Do not use INSERT OR REPLACE: on a legacy catalog that deletes the old
            # row and turns the retained serving_tier field into NULL.
            self._query(
                f'INSERT INTO _catalog ("{column_names}") VALUES ({values}) '
                f'ON CONFLICT("name") DO UPDATE SET {update_columns};',
                retry_safe=True,
            )

    def delete_catalog_row(self, name: str) -> None:
        self._ensure_catalog_schema()
        self._query(
            f"DELETE FROM _catalog WHERE name = {sql_literal(name)};",
            retry_safe=True,
        )

    def delete_catalog_product_ids(self, product_ids: Sequence[str]) -> None:
        """Remove public discovery rows without touching product tables or ledgers."""

        if any(
            not isinstance(product_id, str)
            or not IDENTIFIER_RE.fullmatch(product_id)
            for product_id in product_ids
        ):
            raise ValueError("unsafe D1 product_id for catalog retirement")
        normalized = sorted(set(product_ids))
        if not normalized:
            return
        self._ensure_catalog_schema()
        product_id_literals = ", ".join(sql_literal(product_id) for product_id in normalized)
        self._query(
            f"DELETE FROM _catalog WHERE product_id IN ({product_id_literals});",
            retry_safe=True,
        )

    def catalog_domain_count(self, model_names: set[str]) -> int:
        if not model_names:
            return 0
        names = ", ".join(sql_literal(n) for n in sorted(model_names))
        out = self._query(f"SELECT count(*) c FROM _catalog WHERE name IN ({names});")
        return int(out[0].get("c", 0)) if out else 0

    def read_table_rows(
        self,
        name: str,
        ordered_columns: Sequence[Column],
        primary_key: Sequence[str],
    ) -> list[dict[str, Any]]:
        if not ordered_columns:
            raise ValueError(f"{name}: ordered_columns are required for D1 read-back")
        if not primary_key:
            raise ValueError(f"{name}: primary_key is required for D1 read-back")
        table = quote_identifier(name)
        select_columns = ", ".join(quote_identifier(column) for column, _type in ordered_columns)
        order_columns = ", ".join(quote_identifier(column) for column in primary_key)
        return self._query(f"SELECT {select_columns} FROM {table} ORDER BY {order_columns};")

    def append_publication_ledger(self, record: dict[str, Any]) -> None:
        self._query(PUBLICATION_LEDGER_DDL, retry_safe=True)
        columns = '", "'.join(PUBLICATION_LEDGER_COLUMNS)
        values = ", ".join(sql_literal(record.get(column)) for column in PUBLICATION_LEDGER_COLUMNS)
        self._query(f'INSERT INTO _publication_ledger ("{columns}") VALUES ({values});')

    def _ensure_handoff_schema(self, table: str) -> None:
        if table in self._handoff_ready:
            return
        pragma_rows = self._query(f'PRAGMA table_info("{table}");')
        if handoff_schema_is_current(table, pragma_rows):
            pass
        elif pragma_rows:
            # 레거시 1회 **행 보존** 이행(#638 §4) — 이번 run 에 게시되지 않는 제품의 메타도 유지
            self._query(handoff_migrate_statements(table, [str(row["name"]) for row in pragma_rows]))
        else:
            self._query(handoff_ddl(table))
        self._handoff_ready.add(table)

    def _ensure_evidence_schema(self, table: str) -> None:
        """Create #678 tables once; never infer a destructive migration at runtime."""
        if table in self._evidence_ready:
            return
        pragma_rows = self._query(f'PRAGMA table_info("{table}");')
        if not pragma_rows:
            self._query(evidence_ddl(table))
        elif evidence_schema_is_migratable(table, pragma_rows):
            self._query(evidence_migrate_statements(table, [str(row["name"]) for row in pragma_rows]))
        elif not evidence_schema_is_current(table, pragma_rows):
            raise RuntimeError(
                f"{table}: evidence schema mismatch; explicit row-preserving migration is required"
            )
        self._evidence_ready.add(table)

    def publish_product_meta(
        self,
        product_id: str,
        publication_id: str,
        columns_rows: Sequence[dict[str, Any]],
        ext_rows: Sequence[dict[str, Any]],
        pattern_rows: Sequence[dict[str, Any]],
        # v1.10 (#706): 미선언 도메인은 빈 시퀀스 그대로 — 기본값이라 기존 호출부가 안 깨진다.
        display_rows: Sequence[dict[str, Any]] = (),
        *,
        # v1.11 (Serving#217): 파라미터 메타(d1_pattern_params) — 미선언 도메인은 빈 시퀀스.
        param_rows: Sequence[dict[str, Any]] = (),
        vocabulary_rows: Sequence[dict[str, Any]] = (),
    ) -> None:
        """제품 스코프 보조 메타를 자연키 upsert 후 이번 선언에 없는 잔여 행만 정리한다.

        원자성 경계는 제품 단위(#638 §3) — 중간 실패 시 이 제품의 메타만 신·구 혼재하고
        다른 제품·도메인 행은 건드리지 않는다. columns/patterns 정리는 선언 키셋 기준
        (handoff_prune_statement 참조 — publication_id 부등 판별은 id 재사용 run 에서 구멍).
        glossary 는 제품 스코프가 아니라 여기 없다(취합 소유 도메인의 게시 경로가 별도 — #638 §2.4).
        """
        statements: list[str] = []
        for table in HANDOFF_PRODUCT_TABLES:
            self._ensure_handoff_schema(table)
        statements.extend(handoff_upsert_statements("d1_catalog_columns", columns_rows))
        statements.extend(handoff_upsert_statements("d1_catalog_column_vocabularies", vocabulary_rows))
        statements.extend(handoff_upsert_statements("d1_catalog_ext", ext_rows))
        statements.extend(handoff_upsert_statements("d1_usage_patterns", pattern_rows))
        statements.extend(handoff_upsert_statements("d1_catalog_display", display_rows))
        statements.extend(handoff_upsert_statements("d1_pattern_params", param_rows))
        statements.append(handoff_prune_statement(
            "d1_catalog_columns", product_id, [str(row["column_name"]) for row in columns_rows]))
        statements.append(handoff_prune_statement(
            "d1_catalog_column_vocabularies", product_id,
            [str(row["column_name"]) for row in vocabulary_rows]))
        statements.append(handoff_stale_delete_statement("d1_catalog_ext", product_id, publication_id))
        # display 를 내린 제품의 옛 행이 남지 않게 — ext 와 같은 단일 키 스코프라 판별도 같다
        statements.append(
            handoff_stale_delete_statement("d1_catalog_display", product_id, publication_id))
        statements.append(handoff_prune_statement(
            "d1_usage_patterns", product_id, [str(row["pattern_id"]) for row in pattern_rows]))
        # 파라미터 메타(Serving#217) — 선언을 지운 패턴의 옛 행이 남으면 게이트웨이가 죽은
        # 기본값을 계속 적용한다. patterns 와 같은 선언 키셋(NOT IN) 판별로 정리한다.
        statements.append(handoff_prune_statement(
            "d1_pattern_params", product_id, [str(row["pattern_id"]) for row in param_rows]))
        for batch in group_api_batches(statements):
            self._query_batch(batch, retry_safe=True)

    def publish_glossary(self, rows: Sequence[dict[str, Any]]) -> None:
        """Fail closed, then replace declared terms only within each vocabulary scope."""
        violations = glossary_registry_violations(rows)
        if violations:
            detail = ", ".join(f"{vocabulary_id}: {reason}" for vocabulary_id, reason in sorted(violations.items()))
            raise ValueError(f"glossary registry rejected: {detail}")
        if not rows:
            return

        markers: dict[str, str] = {}
        for row in rows:
            vocabulary_id = str(row.get("vocabulary_id") or "")
            exported_at = row.get("exported_at")
            if not isinstance(exported_at, str) or not exported_at:
                raise ValueError(f"{vocabulary_id}: glossary exported_at is required")
            previous = markers.setdefault(vocabulary_id, exported_at)
            if previous != exported_at:
                raise ValueError(f"{vocabulary_id}: glossary rows must share one exported_at")

        self._ensure_handoff_schema("d1_catalog_glossary")
        statements = handoff_upsert_statements("d1_catalog_glossary", rows)
        statements.extend(
            handoff_stale_delete_statement("d1_catalog_glossary", vocabulary_id, exported_at)
            for vocabulary_id, exported_at in markers.items()
        )
        for batch in group_api_batches(statements):
            self._query_batch(batch, retry_safe=True)

    def publish_product_evidence(
        self,
        product_id: str,
        publication_id: str,
        sources: Sequence[dict[str, Any]] | None,
        quality: dict[str, Any],
    ) -> None:
        """Publish V1 source/right and runtime quality evidence for one active product.

        ``sources is None`` represents a legacy manifest which has not adopted #678;
        it must leave any previously known source rows untouched. A declared list
        replaces only this product's source scope. Quality is calculated by the
        Publisher for every successful publication and is always replaced with the
        same active ``publication_id``.
        """
        self._ensure_evidence_schema("d1_catalog_sources")
        self._ensure_evidence_schema("d1_product_quality")
        statements: list[str] = []
        if sources is not None:
            source_rows = [
                {
                    "product_id": product_id,
                    **source,
                    "publication_id": publication_id,
                }
                for source in sources
            ]
            statements.extend(evidence_upsert_statements("d1_catalog_sources", source_rows))
            statements.append(evidence_source_prune_statement(
                product_id, [str(source["source_id"]) for source in sources]
            ))
        quality_row = {
            "product_id": product_id,
            "source_row_count": quality["source_row_count"],
            "d1_row_count": quality["d1_row_count"],
            "duplicate_primary_key_count": quality["duplicate_primary_key_count"],
            "null_primary_key_count": quality["null_primary_key_count"],
            "freshness_as_of": quality.get("freshness_as_of"),
            "freshness_slo_minutes": quality.get("freshness_slo_minutes"),
            "serving_status": quality["serving_status"],
            "measured_at": quality["measured_at"],
            "coverage_json": (
                json.dumps(quality["coverage"], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                if quality.get("coverage") is not None
                else None
            ),
            "projection_schema_version": quality.get("projection_schema_version"),
            "projection_schema_hash": quality.get("projection_schema_hash"),
            "publication_id": publication_id,
        }
        statements.extend(evidence_upsert_statements("d1_product_quality", [quality_row]))
        for batch in group_api_batches(statements):
            self._query_batch(batch, retry_safe=True)
