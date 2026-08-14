"""Common D1 Publisher orchestration.

Runs the Serving Contract v1 Publication as one unit per product:
  Contract Load → Publication Gate → D1 Write → row-count verify → _catalog Upsert
  → API Smoke Test, recording runtime metadata and protecting the last-known-good
  snapshot on failure.

Pure w.r.t. runtime: it depends only on the ``SourceReader`` / ``D1Client`` /
``SmokeTester`` seams, so the whole pipeline is unit-tested with in-memory fakes —
no Trino, no Cloudflare, no prod. The real seams live in ``runtime.py``.
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Protocol, Sequence
from zoneinfo import ZoneInfo

import logging

from common.serving import gate as gatelib
from common.serving.content_identity import d1_content_hash
from common.serving.contract import QUERY_AVAILABILITY_COLUMNS, ServingContract
from common.serving.d1_client import Column, D1Client, GLOSSARY_REGISTRY, HANDOFF_PRODUCT_TABLES, ProductPublicationState, glossary_registry_violations, sqlite_type
from common.serving.pattern_audit import audit_pattern_sql, build_allowlist, deny_findings, rewrite_audited_relation
from common.serving.pattern_verify import verify_and_stamp

log = logging.getLogger(__name__)
from common.serving.gate import (
    STATUS_DEGRADED,
    STATUS_FAILED,
    STATUS_PUBLISHED,
    STATUS_SKIPPED,
    GateDecision,
)


@dataclass
class QueryAvailabilityPlan:
    columns: list[Column]
    rows: list[dict[str, Any]]


@dataclass
class ReadPlan:
    """What the source will publish this run for one product."""

    columns: list[Column]
    rows: list[dict[str, Any]]
    delete_column: str | None = None  # append: clear this window before insert
    delete_literal: str | None = None
    # `quality_coverage.measurement_scope=source_relation`의 projection 전 실측값.
    coverage_observed_distinct_count: int | None = None
    # Sparse zero-row products may carry the upstream freshness declared by
    # `empty_result_freshness`; null is intentionally not treated as healthy.
    empty_result_freshness: Any | None = None
    query_availability: QueryAvailabilityPlan | None = None


class SourceReader(Protocol):
    def read(self, contract: ServingContract, last_good_max: Any | None) -> ReadPlan: ...


class SmokeTester(Protocol):
    def check(self, model_name: str) -> str: ...


@dataclass
class ProductRecord:
    product_id: str
    model_name: str
    publication_id: str
    source_run_id: str
    published_at: str
    serving_status: str
    reason: str
    source_row_count: int = 0
    published_row_count: int = 0
    d1_row_count: int = 0
    distinct_primary_key_count: int = 0
    null_primary_key_count: int = 0
    published_bytes: int = 0
    freshness: str | None = None
    api_smoke_status: str = "not_evaluated"
    api_smoke_detail: dict[str, Any] | None = None
    stage: str = "initialized"
    rollback_status: str = "not_needed"
    projection_schema_hash: str | None = None
    source_content_hash: str | None = None
    d1_content_hash: str | None = None
    coverage: dict[str, Any] | None = None
    query_availability_fingerprint: str | None = None
    query_availability_row_count: int | None = None


@dataclass
class PublicationReport:
    records: list[ProductRecord] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures


class PublicationError(RuntimeError):
    """Raised when any product fails; ``report`` carries per-product metadata."""

    def __init__(self, report: PublicationReport) -> None:
        self.report = report
        super().__init__("; ".join(report.failures))


_SMOKE_DIAGNOSTIC_FIELDS = frozenset({
    "http_status",
    "error_code",
    "blockers",
    "cf_ray",
    "latency_ms",
    "exception_type",
    "reason",
})
_SMOKE_DIAGNOSTIC_TEXT_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


def _safe_smoke_detail(raw: Mapping[str, Any]) -> dict[str, Any]:
    detail: dict[str, Any] = {}
    http_status = raw.get("http_status")
    if isinstance(http_status, int) and 100 <= http_status <= 599:
        detail["http_status"] = http_status
    latency_ms = raw.get("latency_ms")
    if isinstance(latency_ms, int) and latency_ms >= 0:
        detail["latency_ms"] = latency_ms
    for key in ("error_code", "cf_ray", "exception_type", "reason"):
        value = raw.get(key)
        if isinstance(value, str) and _SMOKE_DIAGNOSTIC_TEXT_RE.fullmatch(value):
            detail[key] = value
    blockers = raw.get("blockers")
    if isinstance(blockers, (list, tuple)):
        safe_blockers = [
            blocker
            for blocker in blockers[:10]
            if isinstance(blocker, str)
            and _SMOKE_DIAGNOSTIC_TEXT_RE.fullmatch(blocker)
        ]
        if safe_blockers:
            detail["blockers"] = safe_blockers
    return detail


def _smoke_diagnostic(smoke: SmokeTester, model_name: str) -> dict[str, Any] | None:
    getter = getattr(smoke, "diagnostic", None)
    if not callable(getter):
        return None
    try:
        raw = getter(model_name)
    except Exception:  # noqa: BLE001 - diagnostics must never replace the smoke verdict
        return None
    if not isinstance(raw, Mapping):
        return None
    detail = _safe_smoke_detail({
        key: value for key, value in raw.items() if key in _SMOKE_DIAGNOSTIC_FIELDS
    })
    return detail or None


def _smoke_failure_message(detail: Mapping[str, Any] | None) -> str:
    if not detail:
        return "API smoke test 실패"
    parts = []
    for key in (
        "http_status",
        "error_code",
        "blockers",
        "cf_ray",
        "latency_ms",
        "exception_type",
        "reason",
    ):
        value = detail.get(key)
        if value is not None:
            parts.append(f"{key}={value}")
    return "API smoke test 실패" + (f" ({', '.join(parts)})" if parts else "")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


QUERY_AVAILABILITY_EXPECTED_PLACE_COUNT = 427
_WEATHER_PLACE_ID_RE = re.compile(r"^seoul_admd_[0-9]{10}$")
_KST_NAIVE_TIMESTAMP_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?$"
)
_POPULATION_REVISION_RE = re.compile(
    r"^kma_admin_dong_grid_20260325:[0-9a-f]{64}$"
)


def _query_availability_timestamp(
    value: Any, *, field: str
) -> tuple[datetime | None, str | None]:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return None, f"query_availability {field} must be a KST-naive timestamp"
        return value, None
    if not isinstance(value, str) or not _KST_NAIVE_TIMESTAMP_RE.fullmatch(value):
        return None, f"query_availability {field} must be a KST-naive timestamp"
    try:
        return datetime.fromisoformat(value), None
    except ValueError:
        return None, f"query_availability {field} must be a real calendar timestamp"


def query_availability_fingerprint(
    contract: ServingContract, plan: QueryAvailabilityPlan
) -> str:
    relation = contract.query_availability_relation
    if relation is None:
        raise ValueError(f"{contract.product_id}: query availability relation is required")
    return d1_content_hash(
        namespace=f"{contract.product_id}:{relation}",
        columns=plan.columns,
        rows=plan.rows,
        primary_key=("place_id",),
    )


def validate_query_availability(
    contract: ServingContract,
    plan: QueryAvailabilityPlan | None,
    *,
    checked_at: datetime | None = None,
) -> str | None:
    """Reject incomplete companion evidence; this gate never repairs source rows."""
    if plan is None:
        return "query_availability plan missing"
    actual_columns = tuple(column for column, _type in plan.columns)
    if actual_columns != QUERY_AVAILABILITY_COLUMNS:
        return "query_availability columns do not match fixed contract"
    if len(plan.rows) != QUERY_AVAILABILITY_EXPECTED_PLACE_COUNT:
        return (
            "query_availability place count mismatch "
            f"expected={QUERY_AVAILABILITY_EXPECTED_PLACE_COUNT} observed={len(plan.rows)}"
        )
    places: set[str] = set()
    common_snapshot: datetime | None = None
    common_horizon: datetime | None = None
    common_count: int | None = None
    common_revision: str | None = None
    oldest_collection: datetime | None = None
    for row in plan.rows:
        place_id = row.get("place_id")
        if not isinstance(place_id, str) or not place_id.strip():
            return "query_availability place_id is missing"
        if not _WEATHER_PLACE_ID_RE.fullmatch(place_id):
            return "query_availability canonical place_id is required"
        if place_id in places:
            return "query_availability duplicate place_id"
        places.add(place_id)

        parsed: dict[str, datetime] = {}
        for field in (
            "snapshot_as_of_hour", "available_from_at", "available_to_at",
            "forecast_collected_at_min", "forecast_collected_at_max",
        ):
            if row.get(field) is None:
                return f"query_availability {field} is missing"
            timestamp, timestamp_error = _query_availability_timestamp(row.get(field), field=field)
            if timestamp_error is not None:
                return timestamp_error
            assert timestamp is not None
            parsed[field] = timestamp

        for field in ("snapshot_as_of_hour", "available_from_at", "available_to_at"):
            timestamp = parsed[field]
            if timestamp.minute or timestamp.second or timestamp.microsecond:
                return f"query_availability {field} must be an exact hourly timestamp"
        if parsed["available_from_at"] != parsed["snapshot_as_of_hour"]:
            return "query_availability available_from_at must equal snapshot_as_of_hour"
        if parsed["available_from_at"] > parsed["available_to_at"]:
            return "query_availability availability bounds are reversed"
        if parsed["forecast_collected_at_min"] > parsed["forecast_collected_at_max"]:
            return "query_availability collection bounds are reversed"
        if oldest_collection is None or parsed["forecast_collected_at_min"] < oldest_collection:
            oldest_collection = parsed["forecast_collected_at_min"]

        if row.get("availability_status") != "complete":
            return "query_availability availability_status is not complete"
        expected = row.get("expected_forecast_hour_count")
        observed = row.get("observed_forecast_hour_count")
        if (
            not isinstance(expected, int) or isinstance(expected, bool)
            or not isinstance(observed, int) or isinstance(observed, bool)
            or expected <= 0 or observed <= 0
        ):
            return "query_availability forecast hour count must be positive"
        if expected != observed:
            return "query_availability forecast hour count expected and observed must match"
        inclusive_slots = int(
            (parsed["available_to_at"] - parsed["available_from_at"]).total_seconds() // 3600
        ) + 1
        if expected != inclusive_slots:
            return "query_availability forecast hour count must equal inclusive hourly slot count"

        revision = row.get("source_population_revision")
        if not isinstance(revision, str) or not _POPULATION_REVISION_RE.fullmatch(revision):
            return "query_availability source_population_revision is invalid"
        if common_snapshot is None:
            common_snapshot = parsed["snapshot_as_of_hour"]
            common_horizon = parsed["available_to_at"]
            common_count = expected
            common_revision = revision
            continue
        if parsed["snapshot_as_of_hour"] != common_snapshot:
            return "query_availability common snapshot mismatch"
        if parsed["available_to_at"] != common_horizon:
            return "query_availability common horizon mismatch"
        if expected != common_count:
            return "query_availability common forecast hour count mismatch"
        if revision != common_revision:
            return "query_availability uniform source_population_revision mismatch"

    if checked_at is not None and contract.freshness_slo_minutes is not None:
        if checked_at.tzinfo is None:
            return "query_availability freshness check requires a timezone-aware instant"
        assert oldest_collection is not None
        collected_at = oldest_collection.replace(
            tzinfo=ZoneInfo("Asia/Seoul")
        ).astimezone(timezone.utc)
        age_minutes = (
            checked_at.astimezone(timezone.utc) - collected_at
        ).total_seconds() / 60
        if age_minutes < 0:
            return "query_availability forecast_collected_at_min is in the future"
        if age_minutes > contract.freshness_slo_minutes:
            return "query_availability forecast_collected_at_min freshness SLO breached"
    return None


def _freshness(
    contract: ServingContract,
    rows: Sequence[dict[str, Any]],
    empty_result_freshness: Any | None = None,
) -> str | None:
    freshness_field = contract.freshness_field or contract.event_time
    if not rows:
        return str(empty_result_freshness) if empty_result_freshness is not None else None
    if not freshness_field:
        return None
    values = [row.get(freshness_field) for row in rows if row.get(freshness_field) is not None]
    return str(max(values)) if values else None


def _catalog_row(contract: ServingContract, columns: Sequence[Column], record: ProductRecord) -> dict[str, Any]:
    return {
        "name": contract.model_name,
        "product_id": contract.product_id,
        "external": 1 if contract.external else 0,
        "description": contract.description,
        "product_question": contract.product_question,
        "public_gold": (
            json.dumps(contract.public_gold, ensure_ascii=False, sort_keys=True)
            if contract.public_gold is not None
            else None
        ),
        "mcp_projection": (
            json.dumps(contract.mcp_projection, ensure_ascii=False, sort_keys=True)
            if contract.mcp_projection is not None
            else None
        ),
        "tests": json.dumps(list(contract.tests), ensure_ascii=False),
        "time_axis": contract.event_time,
        "columns": json.dumps([{"name": c, "type": t} for c, t in columns], ensure_ascii=False),
        "row_count": record.published_row_count,
        "serving_status": record.serving_status,
        "publication_id": record.publication_id,
        "source_run_id": record.source_run_id,
        "published_bytes": record.published_bytes,
        "freshness": record.freshness,
        "exported_at": record.published_at,
    }


def _product_meta_rows(
    contract: ServingContract,
    columns: Sequence[Column],
    record: ProductRecord,
    audit_allowlist: frozenset[str] | None = None,
) -> tuple[
    list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]],
    list[dict[str, Any]], list[dict[str, Any]],
]:
    """핸드오프 메타 5종(#638 §2.2 · display v1.10 #706 · param v1.11 Serving#217) 행 —
    계약 선언(dbt yml→manifest)에서 그대로 나온다.

    타입은 D1 실물과 같은 SQLite 타입으로 싣고(commerce 관행 동형), 컬럼 설명이 없는
    도메인은 description_ko=NULL 로 컬럼 행 자체는 게시한다(이름·타입·게시본 대조는 유효).

    패턴 게시 감사(Serving#217·킷 §F): 지금까지 커머스만 자체 게시기에서 감사를 받았고 공유
    게시기 도메인은 무감사였다 — 여기 한 번 걸어 전 도메인이 같은 검사를 받는다. 강제 수위는
    #217 결정의 단계 그대로: **거부 계열(구조 위반·내부표)은 게시 제외**(P0-a 동형),
    **allowlist 밖은 경보만**(P0-b 는 게시 계약·오너십과 함께 2차 — 서브셋 게시 배치에서
    형제 표 참조를 오차단하지 않기 위해서이기도 하다).
    """
    descriptions = contract.column_descriptions or {}
    columns_rows = [
        {
            "product_id": contract.product_id,
            "table_name": contract.model_name,
            "ordinal": ordinal,
            "column_name": name,
            "type": sqlite_type(trino_type),
            "description_ko": descriptions.get(name) or None,
            "publication_id": record.publication_id,
        }
        for ordinal, (name, trino_type) in enumerate(columns)
    ]
    vocabulary_rows = [
        {
            "product_id": contract.product_id,
            "table_name": contract.model_name,
            "column_name": name,
            "vocabulary_id": vocabulary_id,
            "publication_id": record.publication_id,
        }
        for name, _trino_type in columns
        if isinstance((vocabulary_id := (contract.column_vocabularies or {}).get(name)), str)
        and vocabulary_id
    ]
    ext_rows = [
        {
            "product_id": contract.product_id,
            "table_name": contract.model_name,
            "source_model": contract.model_name,
            "grain": contract.grain,
            "primary_key": json.dumps(list(contract.primary_key), ensure_ascii=False),
            "time_axis": contract.event_time,
            "tier": contract.serving_tier,
            "rollup_rule": contract.rollup_rule,
            "publication_id": record.publication_id,
        }
    ]
    # v1.10 (#706): 미선언 제품은 **행을 만들지 않는다** — 빈 문자열로 채우면 화면이
    # "설명이 있는 척"하게 된다(카탈로그 컬럼 설명에서 같은 판단을 이미 했다).
    display = contract.display if isinstance(contract.display, dict) else None
    display_rows = [
        {
            "product_id": contract.product_id,
            "title": display.get("title"),
            "summary": display.get("summary"),
            "caveat": display.get("caveat"),
            "use_cases": (
                json.dumps(list(display["use_cases"]), ensure_ascii=False)
                if isinstance(display.get("use_cases"), list) else None
            ),
            "publication_id": record.publication_id,
        }
    ] if display else []
    pattern_rows = [
        {
            "product_id": contract.product_id,
            "pattern_id": pattern.get("pattern_id"),
            "question_ko": pattern.get("question_ko"),
            "sql": pattern.get("sql"),
            "axes": pattern.get("axes"),
            "requires": json.dumps(pattern.get("requires") or [], ensure_ascii=False),
            "verified_rows": pattern.get("verified_rows"),
            "verified_at": pattern.get("verified_at"),
            "verified_publication_id": pattern.get("verified_publication_id"),
            "allow_empty": 1 if pattern.get("allow_empty") else 0,
            "insight_sample_ko": pattern.get("insight_sample_ko"),
            "publication_id": record.publication_id,
        }
        for pattern in contract.usage_patterns
        if pattern.get("pattern_id") and pattern.get("sql")
        # 한 모델→다제품 선언(commerce geo_grid 관행)과의 동형성: d1_table 명시 시 해당 제품만.
        and pattern.get("d1_table", contract.model_name) == contract.model_name
    ]

    # ── 패턴 게시 감사 (Serving#217 · 킷 §F 배선) ─────────────────────────────────
    # 거부 계열(구조 위반·내부표 참조)은 게시 제외 + 경보(제품 게시는 막지 않는다 —
    # commerce _handoff_rows 와 같은 정책). allowlist 밖은 **경보만**(P0-b 예고 신호).
    kept: list[dict[str, Any]] = []
    for row in pattern_rows:
        denied = deny_findings(row["sql"])
        if denied:
            log.error(
                "serve.pattern_audit_reject product=%s pattern=%s findings=%s "
                "(게시 제외 — lint_usage_patterns.py 로 사전 검사 후 SQL 수정)",
                contract.product_id, row["pattern_id"], denied[:2])
            continue
        if audit_allowlist is not None:
            extern = [f for f in audit_pattern_sql(row["sql"], audit_allowlist)
                      if "allowlist 밖" in f]
            if extern:
                log.warning(
                    "serve.pattern_audit_external product=%s pattern=%s findings=%s "
                    "(경보만 — allowlist 강제는 P0-b/2차, Serving#217)",
                    contract.product_id, row["pattern_id"], extern[:1])
        kept.append(row)
    pattern_rows = kept

    # v1.11 (Serving#217 P1/P3): 파라미터 메타는 별도 표 d1_pattern_params 로 —
    # 세 필드 중 하나라도 선언한 패턴만 행을 만들고, 감사 탈락 패턴의 메타는 싣지 않는다.
    published_ids = {str(row["pattern_id"]) for row in pattern_rows}
    param_rows = [
        {
            "product_id": contract.product_id,
            "pattern_id": pattern.get("pattern_id"),
            "param_defaults": (json.dumps(pattern["param_defaults"], ensure_ascii=False)
                               if isinstance(pattern.get("param_defaults"), dict) else None),
            "param_enum": (json.dumps(pattern["param_enum"], ensure_ascii=False)
                           if isinstance(pattern.get("param_enum"), dict) else None),
            "params": (json.dumps(pattern["params"], ensure_ascii=False)
                       if isinstance(pattern.get("params"), dict) else None),
            "publication_id": record.publication_id,
        }
        for pattern in contract.usage_patterns
        if str(pattern.get("pattern_id")) in published_ids
        and pattern.get("d1_table", contract.model_name) == contract.model_name
        and any(isinstance(pattern.get(k), dict)
                for k in ("param_defaults", "param_enum", "params"))
    ]
    return columns_rows, ext_rows, pattern_rows, display_rows, param_rows, vocabulary_rows


def _vocabulary_glossary_rows(contracts: Sequence[ServingContract]) -> list[dict[str, Any]]:
    """Preflight every vocabulary reference before source reads or snapshot writes."""
    references = {
        vocabulary_id
        for contract in contracts
        for vocabulary_id in (contract.column_vocabularies or {}).values()
        if isinstance(vocabulary_id, str)
    }
    terms = [
        dict(term)
        for contract in contracts
        for term in contract.vocabulary_terms
        if isinstance(term, dict)
    ]
    references.update(str(term.get("vocabulary_id") or "") for term in terms)
    unknown = sorted(vocabulary_id for vocabulary_id in references if vocabulary_id not in GLOSSARY_REGISTRY)
    if unknown:
        raise ValueError(f"unregistered vocabulary_id: {', '.join(unknown)}")
    if any(term.get("vocabulary_id") == "common:gu_code" for term in terms):
        raise ValueError("common:gu_code terms must be published by its glossary owner")
    violations = glossary_registry_violations(terms)
    if violations:
        detail = ", ".join(f"{vocabulary_id}: {reason}" for vocabulary_id, reason in sorted(violations.items()))
        raise ValueError(f"glossary registry rejected: {detail}")

    exported_at = _now_iso()
    return [dict(term, exported_at=exported_at) for term in terms]


def _product_evidence(
    contract: ServingContract,
    record: ProductRecord,
) -> tuple[tuple[dict[str, Any], ...] | None, dict[str, Any]]:
    """Build the product-level source and runtime evidence for the active publication.

    A missing ``source_evidence`` is intentionally distinct from an empty declaration.
    Legacy contracts must not erase source records that another approved publisher has
    already placed in D1.  New contracts are required to declare at least one source
    record by the contract validator.
    """
    return contract.source_evidence, {
        "source_row_count": record.source_row_count,
        "d1_row_count": record.d1_row_count,
        "duplicate_primary_key_count": record.d1_row_count - record.distinct_primary_key_count,
        "null_primary_key_count": record.null_primary_key_count,
        "freshness_as_of": record.freshness,
        "freshness_slo_minutes": contract.freshness_slo_minutes,
        "serving_status": record.serving_status,
        "measured_at": record.published_at,
        # NULL is deliberately not interpreted as passing by the K-Skill Worker.
        "coverage": record.coverage,
        # Non-empty only for an explicit public projection. The Worker fails closed
        # for legacy full-source publications so internal lineage columns never leak.
        "projection_schema_version": contract.projection_schema_version,
        "projection_schema_hash": contract.projection_schema_hash,
    }


def _primary_key_stats(rows: Sequence[dict[str, Any]], primary_key: Sequence[str]) -> tuple[int, int, int]:
    if not primary_key:
        raise ValueError("primary_key is required for publication")
    values = [tuple(row.get(column) for column in primary_key) for row in rows]
    null_count = sum(1 for value in values if any(part is None for part in value))
    return len(rows), len(set(values)), null_count


def _coverage_evidence(
    contract: ServingContract,
    rows: Sequence[dict[str, Any]],
    observed_distinct_count: int | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Compute the declared distinct coverage and return a precise pre-write failure reason."""
    declaration = contract.quality_coverage
    if declaration is None:
        return None, None
    not_applicable_reason = declaration.get("not_applicable_reason")
    if not_applicable_reason:
        return {
            "status": "not_applicable",
            "reason": not_applicable_reason,
        }, None
    field = declaration["field"]
    expected = declaration["expected_distinct_count"]
    minimum_ratio = declaration["minimum_ratio"]
    observed = (
        observed_distinct_count
        if observed_distinct_count is not None
        else len({row.get(field) for row in rows if row.get(field) is not None})
    )
    ratio = observed / expected
    coverage = {
        "field": field,
        "expected_distinct_count": expected,
        "observed_distinct_count": observed,
        "minimum_ratio": minimum_ratio,
        "ratio": ratio,
        "status": "passed" if ratio >= minimum_ratio else "failed",
    }
    if ratio < minimum_ratio:
        return coverage, (
            f"coverage validation failed: field={field} observed={observed} "
            f"expected={expected} ratio={ratio:.6f} minimum_ratio={minimum_ratio:.6f}"
        )
    return coverage, None


def _uses_replace_lifecycle(contract: ServingContract) -> bool:
    return contract.publication_mode == "snapshot" or (
        contract.publication_mode == "upsert" and contract.upsert_strategy == "exact_set"
    )


def _write(
    d1: D1Client,
    contract: ServingContract,
    plan: ReadPlan,
    rows: Sequence[dict[str, Any]],
) -> None:
    """Write ``rows`` per publication_mode; read-back happens after activation."""
    mode = contract.publication_mode
    if _uses_replace_lifecycle(contract):
        d1.replace_table(contract.model_name, plan.columns, rows, contract.primary_key)  # staging swap protects last-good
        return
    d1.ensure_table(contract.model_name, plan.columns, contract.primary_key)
    if mode == "append":
        if plan.delete_column and plan.delete_literal is not None:
            d1.delete_where_gte(contract.model_name, plan.delete_column, plan.delete_literal)
        d1.insert_rows(contract.model_name, plan.columns, rows, replace=False)
    elif mode == "upsert":
        d1.insert_rows(contract.model_name, plan.columns, rows, replace=True)
    else:
        raise ValueError(f"unknown publication_mode: {mode!r}")


def _content_contract_error(contract: ServingContract, plan: ReadPlan) -> str | None:
    if not contract.public_projection:
        return "verify_content_parity requires public_projection"
    if not contract.projection_schema_hash:
        return "verify_content_parity requires projection_schema_hash"
    if not contract.primary_key:
        return "verify_content_parity requires primary_key"
    plan_columns = tuple(column for column, _type in plan.columns)
    if plan_columns != tuple(contract.public_projection):
        return "ReadPlan columns do not match public_projection order"
    return None


def _ledger_row(record: ProductRecord, *, outcome: str) -> dict[str, Any]:
    return {
        "publication_id": record.publication_id,
        "product_id": record.product_id,
        "model_name": record.model_name,
        "source_run_id": record.source_run_id,
        "attempted_at": record.published_at,
        "outcome": outcome,
        "stage": record.stage,
        "source_row_count": record.source_row_count,
        "published_row_count": record.published_row_count,
        "d1_row_count": record.d1_row_count,
        "api_smoke_status": record.api_smoke_status,
        "rollback_status": record.rollback_status,
        "reason": record.reason,
    }


def _append_ledger(d1: D1Client, record: ProductRecord, *, outcome: str) -> None:
    try:
        d1.append_publication_ledger(_ledger_row(record, outcome=outcome))
    except Exception as exc:  # noqa: BLE001 -- ledger failures must surface in the primary task
        raise RuntimeError(f"publication ledger 기록 실패: {type(exc).__name__}: {exc}") from exc


def _restore_snapshot(
    d1: D1Client,
    record: ProductRecord,
    *,
    previous_catalog: dict[str, Any] | None,
    catalog_committed: bool,
) -> None:
    """Restore an activated snapshot and, only if needed, its old catalog row."""

    try:
        d1.restore_replaced_table(record.model_name)
        if catalog_committed:
            if previous_catalog is None:
                d1.delete_catalog_row(record.model_name)
            else:
                d1.upsert_catalog([previous_catalog])
        record.rollback_status = "restored"
    except Exception as exc:  # noqa: BLE001 -- retain the original failure plus compensation failure
        record.rollback_status = f"restore_failed:{type(exc).__name__}"


def _fail_after_write(
    d1: D1Client,
    report: PublicationReport,
    record: ProductRecord,
    contract: ServingContract,
    *,
    message: str,
    previous_catalog: dict[str, Any] | None,
    catalog_committed: bool = False,
    atomic_previous: ProductPublicationState | None = None,
) -> None:
    record.serving_status = STATUS_FAILED
    record.reason = message
    if atomic_previous is not None:
        try:
            d1.compensate_staged_snapshot(contract.product_id, contract.model_name, atomic_previous)
            record.rollback_status = "restored"
        except Exception as exc:  # noqa: BLE001 -- retain original failure and compensation state
            record.rollback_status = f"restore_failed:{type(exc).__name__}"
    elif _uses_replace_lifecycle(contract):
        _restore_snapshot(
            d1,
            record,
            previous_catalog=previous_catalog,
            catalog_committed=catalog_committed,
        )
    report.failures.append(f"{contract.model_name}: {message}")
    _append_ledger(d1, record, outcome="failed")
    report.records.append(record)


def _atomic_quality_row(contract: ServingContract, record: ProductRecord) -> dict[str, Any]:
    _sources, quality = _product_evidence(contract, record)
    return {
        "product_id": contract.product_id,
        "source_row_count": quality["source_row_count"], "d1_row_count": quality["d1_row_count"],
        "duplicate_primary_key_count": quality["duplicate_primary_key_count"],
        "null_primary_key_count": quality["null_primary_key_count"],
        "freshness_as_of": quality["freshness_as_of"], "freshness_slo_minutes": quality["freshness_slo_minutes"],
        "serving_status": quality["serving_status"], "measured_at": quality["measured_at"],
        "coverage_json": (json.dumps(quality["coverage"], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                          if quality["coverage"] is not None else None),
        "projection_schema_version": quality["projection_schema_version"],
        "projection_schema_hash": quality["projection_schema_hash"], "publication_id": record.publication_id,
    }


def _atomic_candidate_state(
    contract: ServingContract, columns: Sequence[Column], record: ProductRecord,
    pattern_rows: list[dict[str, Any]], metadata_parts: tuple[list[dict[str, Any]], ...],
    previous: ProductPublicationState,
) -> ProductPublicationState:
    columns_rows, ext_rows, _old_patterns, display_rows, param_rows, vocabulary_rows = metadata_parts
    metadata = dict(zip(
        ("d1_catalog_columns", "d1_catalog_ext", "d1_usage_patterns", "d1_catalog_display", "d1_pattern_params", "d1_catalog_column_vocabularies"),
        (columns_rows, ext_rows, pattern_rows, display_rows, param_rows, vocabulary_rows), strict=True,
    ))
    # All current product-scoped tables are present explicitly; the builder deletes
    # absent lists so a declaration removal cannot leave LKG metadata mixed in.
    metadata = {table: tuple(metadata.get(table, ())) for table in HANDOFF_PRODUCT_TABLES}
    sources, _quality = _product_evidence(contract, record)
    source_rows = tuple(
        dict(source, product_id=contract.product_id, publication_id=record.publication_id)
        for source in (sources or ())
    )
    return ProductPublicationState(
        catalog_row=_catalog_row(contract, columns, record), metadata_rows=metadata,
        source_rows=source_rows, quality_row=_atomic_quality_row(contract, record),
        active_table_exists=previous.active_table_exists,
    )


def _activation_committed_after_error(
    d1: D1Client,
    contract: ServingContract,
    publication_id: str,
) -> bool:
    """Reconcile a response-lost activation without replaying its ALTER batch.

    D1 executes the activation batch transactionally, and the catalog upsert is
    its final statement. Matching catalog identity is therefore the commit marker
    for the preceding table swap and product-scoped evidence changes.
    """

    active = d1.capture_product_publication_state(
        contract.product_id,
        contract.model_name,
    )
    catalog = active.catalog_row or {}
    return (
        active.active_table_exists
        and catalog.get("product_id") == contract.product_id
        and catalog.get("publication_id") == publication_id
    )


def publish(
    contracts: Sequence[ServingContract],
    source: SourceReader,
    d1: D1Client,
    smoke: SmokeTester,
    *,
    source_run_id: str,
    verify_content_parity: bool = False,
) -> PublicationReport:
    """Publish each contract as one Publication unit. Raises ``PublicationError`` if any fails."""
    report = PublicationReport()
    # 패턴 게시 감사 allowlist(Serving#217·킷 §F) — 이번 호출 계약들의 제품 테이블 ∪ 명시
    # 크로스도메인 소스. 서브셋 배치일 수 있어 allowlist 밖은 경보만 한다(_product_meta_rows).
    audit_allowlist = build_allowlist(
        (c.model_name for c in contracts),
        (s for c in contracts for s in (getattr(c, "cross_domain_sources", None) or ())),
    )
    try:
        d1.publish_glossary(_vocabulary_glossary_rows(contracts))
    except Exception as exc:  # noqa: BLE001 -- no source read or snapshot write may precede vocabulary rejection
        report.failures.append(f"column vocabulary preflight failed: {type(exc).__name__}: {exc}")
        raise PublicationError(report) from exc
    for contract in contracts:
        record = ProductRecord(
            product_id=contract.product_id,
            model_name=contract.model_name,
            publication_id=uuid.uuid4().hex,
            source_run_id=source_run_id,
            published_at=_now_iso(),
            serving_status=STATUS_FAILED,
            reason="",
        )
        if verify_content_parity:
            record.projection_schema_hash = contract.projection_schema_hash

        last_good_count = None
        catalog = d1.catalog_row(contract.model_name)
        if catalog and catalog.get("row_count") is not None:
            last_good_count = int(catalog["row_count"])
        incremental_upsert = (
            contract.publication_mode == "upsert" and contract.upsert_strategy == "incremental"
        )
        # append·incremental upsert 는 D1 의 현 max(event_time) 을 워터마크로 받아 그 이후만 읽는다.
        last_good_max = (
            d1.table_max(contract.model_name, contract.event_time)
            if contract.event_time and (contract.publication_mode == "append" or incremental_upsert)
            else None
        )
        if incremental_upsert and verify_content_parity:
            # 부분 소스(바뀐 그레인) vs 전체 D1 은 콘텐츠 해시가 구조적으로 불일치 — 조합 금지.
            record.serving_status = STATUS_FAILED
            record.stage = "content_contract"
            record.reason = "incremental upsert 는 verify_content_parity 와 호환 불가(부분 소스 vs 전체 D1)"
            report.failures.append(f"{contract.model_name}: {record.reason}")
            _append_ledger(d1, record, outcome="failed")
            report.records.append(record)
            continue

        plan = source.read(contract, last_good_max)
        record.source_row_count = len(plan.rows)
        if contract.query_availability_relation is not None:
            availability_error = validate_query_availability(
                contract,
                plan.query_availability,
                checked_at=datetime.fromisoformat(record.published_at),
            )
            if availability_error is not None:
                record.stage = "query_availability"
                record.reason = availability_error
                report.failures.append(f"{contract.model_name}: {availability_error}")
                _append_ledger(d1, record, outcome="failed")
                report.records.append(record)
                continue
            assert plan.query_availability is not None
            record.query_availability_row_count = len(plan.query_availability.rows)
            record.query_availability_fingerprint = query_availability_fingerprint(
                contract, plan.query_availability
            )
        decision = gatelib.evaluate_gate(contract, len(plan.rows), last_good_count)
        record.reason = decision.reason

        if decision.decision == GateDecision.FAIL:
            record.serving_status = STATUS_FAILED
            record.stage = "gate"
            report.failures.append(f"{contract.model_name}: {decision.reason}")
            _append_ledger(d1, record, outcome="failed")
            report.records.append(record)
            continue

        if decision.decision == GateDecision.SKIP_RETAIN:
            record.serving_status = STATUS_SKIPPED
            record.published_row_count = last_good_count or 0  # last-known-good untouched
            record.stage = "gate"
            _append_ledger(d1, record, outcome="skipped_retained")
            report.records.append(record)
            continue

        rows, degraded = gatelib.apply_reliability(contract, plan.rows)
        record.source_row_count = len(rows)
        freshness = _freshness(
            contract,
            rows,
            plan.empty_result_freshness if not plan.rows else None,
        )
        if not plan.rows and contract.empty_result_freshness is not None and freshness is None:
            record.serving_status = STATUS_FAILED
            record.stage = "empty_result_freshness"
            record.reason = "empty_result_freshness returned null; preserving last-known-good"
            report.failures.append(f"{contract.model_name}: {record.reason}")
            _append_ledger(d1, record, outcome="failed")
            report.records.append(record)
            continue
        if verify_content_parity:
            contract_error = _content_contract_error(contract, plan)
            if contract_error:
                record.serving_status = STATUS_FAILED
                record.stage = "content_contract"
                record.reason = contract_error
                report.failures.append(f"{contract.model_name}: {record.reason}")
                _append_ledger(d1, record, outcome="failed")
                report.records.append(record)
                continue
        source_row_count, source_distinct_count, source_null_count = _primary_key_stats(rows, contract.primary_key)
        if source_row_count != source_distinct_count or source_null_count:
            record.serving_status = STATUS_FAILED
            record.stage = "source_primary_key"
            record.reason = (
                "source primary key validation failed: "
                f"rows={source_row_count} distinct={source_distinct_count} null={source_null_count}"
            )
            report.failures.append(f"{contract.model_name}: {record.reason}")
            _append_ledger(d1, record, outcome="failed")
            report.records.append(record)
            continue
        record.coverage, coverage_error = _coverage_evidence(
            contract, rows, plan.coverage_observed_distinct_count
        )
        if coverage_error:
            record.serving_status = STATUS_FAILED
            record.stage = "quality_coverage"
            record.reason = coverage_error
            report.failures.append(f"{contract.model_name}: {coverage_error}")
            _append_ledger(d1, record, outcome="failed")
            report.records.append(record)
            continue
        if verify_content_parity:
            try:
                record.source_content_hash = d1_content_hash(
                    namespace=contract.model_name,
                    columns=plan.columns,
                    rows=rows,
                    primary_key=contract.primary_key,
                )
            except Exception as exc:  # noqa: BLE001 -- fail before physical write
                record.serving_status = STATUS_FAILED
                record.stage = "content_contract"
                record.reason = f"source content hash 실패: {type(exc).__name__}: {exc}"
                report.failures.append(f"{contract.model_name}: {record.reason}")
                _append_ledger(d1, record, outcome="failed")
                report.records.append(record)
                continue
        atomic_opt_in = (
            contract.query_availability_relation is not None
            or (_uses_replace_lifecycle(contract) and contract.zero_policy == "allow")
        )
        previous_state: ProductPublicationState | None = None
        try:
            record.stage = "write"
            if atomic_opt_in:
                # Candidate table, catalog, and quality evidence are activated together.
                # In particular, zero-row snapshots must expose their fresh fallback
                # evidence before the public API smoke check can recover a stale LKG.
                d1.prepare_atomic_publication_schema()
                d1.stage_snapshot(contract.model_name, plan.columns, rows, contract.primary_key)
                staged_rows = d1.read_staged_snapshot_rows(contract.model_name, plan.columns, contract.primary_key)
                if verify_content_parity and record.source_content_hash != d1_content_hash(
                    namespace=contract.model_name, columns=plan.columns, rows=staged_rows, primary_key=contract.primary_key,
                ):
                    raise RuntimeError("candidate snapshot read-back content mismatch")
                if plan.query_availability is not None:
                    d1.stage_query_availability(
                        contract.product_id, record.publication_id, plan.query_availability.rows,
                        fingerprint=record.query_availability_fingerprint or "", measured_at=record.published_at,
                    )
                    sidecar_rows = d1.read_query_availability_rows(contract.product_id, record.publication_id)
                    readback_content_rows = [
                        {column: row.get(column) for column in QUERY_AVAILABILITY_COLUMNS}
                        for row in sidecar_rows
                    ]
                    try:
                        readback_fingerprint = query_availability_fingerprint(
                            contract,
                            QueryAvailabilityPlan(plan.query_availability.columns, readback_content_rows),
                        )
                    except Exception as exc:  # noqa: BLE001 -- malformed/corrupt rows are a read-back mismatch
                        raise RuntimeError(
                            f"query_availability read-back mismatch: content identity invalid ({type(exc).__name__})"
                        ) from exc
                    if not (
                        len(sidecar_rows) == QUERY_AVAILABILITY_EXPECTED_PLACE_COUNT
                        and len({row.get("place_id") for row in sidecar_rows}) == QUERY_AVAILABILITY_EXPECTED_PLACE_COUNT
                        and {row.get("availability_fingerprint") for row in sidecar_rows} == {record.query_availability_fingerprint}
                        and {row.get("publication_id") for row in sidecar_rows} == {record.publication_id}
                        and readback_fingerprint == record.query_availability_fingerprint
                    ):
                        raise RuntimeError("query_availability read-back mismatch")
                # Candidate patterns may only read the staging relation. Ambiguous
                # SQL stays unverified rather than borrowing old active evidence.
                metadata_parts = _product_meta_rows(contract, plan.columns, record, audit_allowlist)
                candidate_patterns = metadata_parts[2]
                for pattern in candidate_patterns:
                    rewritten = rewrite_audited_relation(
                        str(pattern["sql"]), contract.model_name, f"{contract.model_name}__staging", audit_allowlist
                    )
                    if rewritten is None:
                        pattern["verified_at"] = None
                        pattern["verified_publication_id"] = None
                    else:
                        candidate_pattern = dict(pattern, sql=rewritten)
                        verify_and_stamp([candidate_pattern], run_sql=d1.execute,
                                         publication_id=record.publication_id,
                                         now=datetime.now(timezone.utc))
                        pattern.update({key: candidate_pattern.get(key) for key in (
                            "verified_rows", "verified_at", "verified_publication_id"
                        )})
                # Candidate catalog/quality must describe the post-swap table, so
                # derive their deterministic counts before building the program.
                record.d1_row_count = source_row_count
                record.distinct_primary_key_count = source_distinct_count
                record.null_primary_key_count = source_null_count
                record.published_row_count = source_row_count
                record.published_bytes = len(json.dumps(rows, ensure_ascii=False, default=str).encode("utf-8"))
                record.freshness = freshness
                record.serving_status = STATUS_DEGRADED if (degraded or decision.serving_status == STATUS_DEGRADED) else STATUS_PUBLISHED
                previous_state = d1.capture_product_publication_state(contract.product_id, contract.model_name)
                candidate = _atomic_candidate_state(contract, plan.columns, record, candidate_patterns, metadata_parts, previous_state)
                d1.preflight_staged_transition(contract.product_id, contract.model_name, candidate, previous_state)
                try:
                    d1.activate_staged_snapshot(
                        contract.product_id,
                        contract.model_name,
                        candidate,
                    )
                except Exception as activation_exc:  # noqa: BLE001 - reconcile unknown transport outcome
                    try:
                        activation_committed = _activation_committed_after_error(
                            d1,
                            contract,
                            record.publication_id,
                        )
                    except Exception as reconciliation_exc:  # noqa: BLE001 - never replay ALTER blindly
                        raise RuntimeError(
                            "D1 atomic activation outcome is unknown: "
                            f"activation={type(activation_exc).__name__} "
                            f"reconciliation={type(reconciliation_exc).__name__}"
                        ) from activation_exc
                    if not activation_committed:
                        raise
                    log.warning(
                        "D1 atomic activation committed after response error; "
                        "model=%s product_id=%s publication_id=%s exception_type=%s",
                        contract.model_name,
                        contract.product_id,
                        record.publication_id,
                        type(activation_exc).__name__,
                    )
            else:
                _write(d1, contract, plan, rows)
        except Exception as exc:  # noqa: BLE001 -- record + continue; activation uncertainty is explicit
            record.serving_status = STATUS_FAILED
            record.stage = "write"
            record.reason = f"write 실패: {type(exc).__name__}: {exc}"
            report.failures.append(f"{contract.model_name}: {record.reason}")
            _append_ledger(d1, record, outcome="failed")
            report.records.append(record)
            continue
        try:
            record.stage = "read_back"
            d1_row_count, distinct_primary_key_count, null_primary_key_count = d1.primary_key_stats(
                contract.model_name,
                contract.primary_key,
            )
        except Exception as exc:  # noqa: BLE001 -- activated replacement must be compensated
            _fail_after_write(
                d1,
                report,
                record,
                contract,
                message=f"D1 primary key read-back 실패: {type(exc).__name__}: {exc}",
                previous_catalog=catalog,
                atomic_previous=previous_state if atomic_opt_in else None,
            )
            continue

        record.d1_row_count = d1_row_count
        record.distinct_primary_key_count = distinct_primary_key_count
        record.null_primary_key_count = null_primary_key_count
        # 전체-테이블 parity(d1 == source)는 전량 게시(snapshot·exact_set/plain upsert)만 강제한다.
        # incremental upsert 는 부분 소스라 append 처럼 면제 — PK 유일성·비-NULL 은 그대로 검증.
        requires_full_parity = contract.publication_mode == "snapshot" or (
            contract.publication_mode == "upsert" and contract.upsert_strategy != "incremental"
        )
        if (
            d1_row_count != distinct_primary_key_count
            or null_primary_key_count != 0
            or (requires_full_parity and d1_row_count != source_row_count)
        ):
            record.stage = "read_back"
            _fail_after_write(d1, report, record, contract, message=(
                "D1 primary key read-back validation failed: "
                f"source={source_row_count} rows={d1_row_count} "
                f"distinct={distinct_primary_key_count} null={null_primary_key_count}"
            ), previous_catalog=catalog, atomic_previous=previous_state if atomic_opt_in else None)
            continue

        if verify_content_parity:
            try:
                d1_rows = d1.read_table_rows(contract.model_name, plan.columns, contract.primary_key)
                record.d1_content_hash = d1_content_hash(
                    namespace=contract.model_name,
                    columns=plan.columns,
                    rows=d1_rows,
                    primary_key=contract.primary_key,
                )
            except Exception as exc:  # noqa: BLE001 -- activated table must be compensated
                record.stage = "content_parity"
                _fail_after_write(
                    d1,
                    report,
                    record,
                    contract,
                    message=f"content parity read-back 실패: {type(exc).__name__}: {exc}",
                    previous_catalog=catalog,
                    atomic_previous=previous_state if atomic_opt_in else None,
                )
                continue
            if record.source_content_hash != record.d1_content_hash:
                record.stage = "content_parity"
                _fail_after_write(
                    d1,
                    report,
                    record,
                    contract,
                    message=(
                        "content parity mismatch: "
                        f"source={record.source_content_hash} d1={record.d1_content_hash}"
                    ),
                    previous_catalog=catalog,
                    atomic_previous=previous_state if atomic_opt_in else None,
                )
                continue

        record.published_row_count = d1_row_count
        record.published_bytes = len(json.dumps(rows, ensure_ascii=False, default=str).encode("utf-8"))
        record.freshness = freshness
        record.serving_status = (
            STATUS_DEGRADED if (degraded or decision.serving_status == STATUS_DEGRADED) else STATUS_PUBLISHED
        )
        if contract.external:
            record.stage = "api_smoke"
            record.api_smoke_status = smoke.check(contract.model_name)
            record.api_smoke_detail = _smoke_diagnostic(smoke, contract.model_name)
            if record.api_smoke_status == "failed":
                failure_message = _smoke_failure_message(record.api_smoke_detail)
                if atomic_opt_in and previous_state is not None:
                    _fail_after_write(d1, report, record, contract, message=failure_message,
                                      previous_catalog=catalog, atomic_previous=previous_state)
                    continue
                _fail_after_write(
                    d1,
                    report,
                    record,
                    contract,
                    message=failure_message,
                    previous_catalog=catalog,
                )
                continue
            elif record.api_smoke_status not in {"passed", "not_evaluated"}:
                if atomic_opt_in and previous_state is not None:
                    _fail_after_write(d1, report, record, contract,
                                      message=f"invalid API smoke status={record.api_smoke_status!r}",
                                      previous_catalog=catalog, atomic_previous=previous_state)
                    continue
                _fail_after_write(
                    d1,
                    report,
                    record,
                    contract,
                    message=f"invalid API smoke status={record.api_smoke_status!r}",
                    previous_catalog=catalog,
                )
                continue

        if atomic_opt_in:
            try:
                d1.finalize_replaced_table(contract.model_name)
            except Exception as exc:  # noqa: BLE001 -- active candidate is already valid; retain LKG for recovery
                record.stage = "finalize"
                record.serving_status = STATUS_FAILED
                record.reason = f"finalize 실패: {type(exc).__name__}: {exc}; active publication retained"
                record.rollback_status = "not_attempted_active_valid"
                report.failures.append(f"{contract.model_name}: {record.reason}")
                _append_ledger(d1, record, outcome="failed")
                report.records.append(record)
                continue
            record.stage = "completed"
            _append_ledger(d1, record, outcome=record.serving_status)
            report.records.append(record)
            continue

        catalog_committed = False
        try:
            record.stage = "catalog"
            d1.upsert_catalog([_catalog_row(contract, plan.columns, record)])
            catalog_committed = True
            registered_count = d1.catalog_domain_count({contract.model_name})
            if registered_count != 1:
                raise RuntimeError("_catalog 자기검증 실패: 등록 누락 가능")
            # 핸드오프 메타(#638) — 같은 try 안이라 실패 시 스냅샷·_catalog 가 함께 복원된다.
            # 메타 행 자체는 보상하지 않는다(#638 §3 — 제품 단위 신·구 혼재 허용, 타 제품 무영향).
            record.stage = "product_meta"
            columns_rows, ext_rows, pattern_rows, display_rows, param_rows, vocabulary_rows = _product_meta_rows(
                contract, plan.columns, record, audit_allowlist)
            # export 시점 패턴 검증(Serving#217): 방금 게시한 D1 데이터에 미검증 패턴 SQL 을 실제로
            # 돌려 통과분에 verified_at 스탬프 → 게이트웨이가 runnable 로 연다. 이미 검증(yml 스탬프)된
            # 패턴은 무접촉. 이 제품만 다루므로 도메인 간 충돌 없음. 검증 실패는 게시를 막지 않는다.
            try:
                relative_now = datetime.now(timezone.utc)
                published_pattern_ids = {str(row["pattern_id"]) for row in pattern_rows}
                param_defaults_by_pattern = {
                    str(pattern["pattern_id"]): pattern["param_defaults"]
                    for pattern in contract.usage_patterns
                    if str(pattern.get("pattern_id")) in published_pattern_ids
                    and isinstance(pattern.get("param_defaults"), dict)
                }
                vr = verify_and_stamp(
                    pattern_rows,
                    run_sql=d1.execute,
                    publication_id=record.publication_id,
                    param_defaults_by_pattern=param_defaults_by_pattern,
                    now=relative_now,
                )
                if vr["verified"] or vr["failed"] or vr["skipped"]:
                    log.info("[serving publish] 패턴 검증 스탬프 product=%s 검증=%d 실패=%d 스킵=%d",
                             contract.product_id, len(vr["verified"]), len(vr["failed"]), len(vr["skipped"]))
            except Exception as exc:  # noqa: BLE001 — 검증은 부가물, 게시를 깨지 않는다
                log.warning("[serving publish] 패턴 검증 스탬프 실패(무시) product=%s: %s",
                            contract.product_id, type(exc).__name__)
            d1.publish_product_meta(
                contract.product_id, record.publication_id, columns_rows, ext_rows, pattern_rows,
                display_rows, param_rows=param_rows, vocabulary_rows=vocabulary_rows,
            )
            # 권리/품질 증거(#678)는 이번 publication_id에 결속한다. 여기서 실패하면
            # catalog와 스냅샷을 복원하고, Worker는 publication 불일치/누락으로 계속 차단한다.
            record.stage = "product_evidence"
            sources, quality = _product_evidence(contract, record)
            d1.publish_product_evidence(
                contract.product_id,
                record.publication_id,
                sources,
                quality,
            )
        except Exception as exc:  # noqa: BLE001 -- restore snapshot after any post-write catalog/meta failure
            _fail_after_write(
                d1,
                report,
                record,
                contract,
                message=f"{record.stage} 실패: {type(exc).__name__}: {exc}",
                previous_catalog=catalog,
                catalog_committed=catalog_committed,
            )
            continue

        if _uses_replace_lifecycle(contract):
            d1.finalize_replaced_table(contract.model_name)
        record.stage = "completed"
        _append_ledger(d1, record, outcome=record.serving_status)
        report.records.append(record)

    if report.failures:
        raise PublicationError(report)
    return report
