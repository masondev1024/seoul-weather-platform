"""Load Serving Contract v1 declarations from a dbt manifest.

Common Contract Load step: the publisher trusts contracts that already passed the
ASAC-DBT validator, and only reads the fields it needs to drive publication.
Keeping this pure (manifest.json in, dataclasses out) lets the publisher run under
tests without Trino/Airflow.
"""

from __future__ import annotations

import json
import hashlib
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SOURCE_EVIDENCE_FIELDS = (
    "source_id",
    "source_url",
    "license",
    "license_url",
    "redistribution",
    "attribution",
    "rights_checked_at",
)
SOURCE_EVIDENCE_REDISTRIBUTION = frozenset({"allowed_with_attribution", "prohibited", "unknown"})
QUALITY_COVERAGE_REQUIRED_FIELDS = ("field", "expected_distinct_count", "minimum_ratio")
QUALITY_COVERAGE_OPTIONAL_FIELDS = ("measurement_scope",)
QUALITY_COVERAGE_MEASUREMENT_SCOPES = frozenset({"published_rows", "source_relation"})
QUALITY_COVERAGE_NOT_APPLICABLE_FIELDS = ("not_applicable_reason",)
EMPTY_RESULT_FRESHNESS_FIELDS = ("relation", "field")
QUERY_AVAILABILITY_FIELDS = ("relation",)
QUERY_AVAILABILITY_COLUMNS = (
    "place_id", "snapshot_as_of_hour", "available_from_at", "available_to_at",
    "forecast_collected_at_min", "forecast_collected_at_max",
    "expected_forecast_hour_count", "observed_forecast_hour_count",
    "availability_status", "source_population_revision",
)


@dataclass(frozen=True)
class ServingContract:
    """The publication-relevant subset of one model's ``meta.serving`` block."""

    product_id: str
    model_name: str  # physical D1 table name
    enabled: bool
    external: bool
    publication_mode: str
    zero_policy: str
    primary_key: tuple[str, ...]
    upsert_strategy: str | None = None
    partial_min_ratio: float | None = None
    reliability: dict[str, Any] | None = None
    event_time: str | None = None
    # Optional quality-time axis. When absent, Publisher preserves the v1 behavior
    # and measures freshness from event_time.
    freshness_field: str | None = None
    # For a valid zero-row sparse product, read the freshness timestamp from this
    # declared upstream model. A null fallback is a fail-closed publication error.
    empty_result_freshness: dict[str, str] | None = None
    query_availability_relation: str | None = None
    description: str = ""
    product_question: str = ""
    tests: tuple[str, ...] = ()
    public_gold: dict[str, Any] | None = None
    mcp_projection: dict[str, Any] | None = None
    public_projection: tuple[str, ...] | None = None
    projection_schema_version: str | None = None
    projection_schema_hash: str | None = None
    freshness_slo_minutes: int | None = None
    # Static publication cadence declared by the DBT contract. The watchdog reads
    # it at runtime; it is intentionally not copied into mutable D1 evidence.
    publication_trigger: dict[str, Any] | None = None
    # V1 evidence contract (#678): absent means legacy product, not an empty source list.
    # The Publisher preserves existing evidence for absent legacy declarations.
    source_evidence: tuple[dict[str, Any], ...] | None = None
    # Optional reproducible distinct-coverage gate. Its result is runtime evidence,
    # not a declared measured value.
    quality_coverage: dict[str, Any] | None = None
    # ── 핸드오프 메타(#638) — d1_catalog_columns/ext·d1_usage_patterns 게시 원천 ──
    grain: str | None = None
    serving_tier: str | None = None      # 물리 게시 확장 — 미선언 도메인은 None(#638 §2.2)
    rollup_rule: str | None = None       # 물리 게시 확장(commerce d1_rollup) — 동상
    display: dict[str, Any] | None = None  # v1.10(#706) 사람이 읽는 표시 메타 — 미선언은 None
    column_descriptions: dict[str, str] | None = None  # manifest node.columns description
    column_vocabularies: dict[str, str] = field(default_factory=dict)
    vocabulary_terms: tuple[dict[str, str], ...] = ()
    usage_patterns: tuple[dict[str, Any], ...] = ()

    def __post_init__(self) -> None:
        # upsert_strategy 값 검증 — 오타/미지원 값을 게시 전에 잡는다(serving_contract/schema.yml v1.5 와 정렬).
        #  * merge       : 전량 upsert(INSERT OR REPLACE 전체, 전체-테이블 parity 강제)
        #  * exact_set   : 전량 교체(staging swap 라이프사이클, 전체-테이블 parity 강제)
        #  * incremental : 부분 upsert — reader 가 event_time(워터마크) 이후 바뀐 그레인만 읽어
        #                  INSERT OR REPLACE 로 그 PK 만 덮는다(나머지 D1 행 보존). D1 쓰기 절약.
        if self.upsert_strategy is not None and self.upsert_strategy not in {"merge", "exact_set", "incremental"}:
            raise ValueError(
                f"{self.product_id}: upsert_strategy 는 'merge'|'exact_set'|'incremental' 만 허용 — got {self.upsert_strategy!r}"
            )
        if self.upsert_strategy == "incremental":
            if self.publication_mode != "upsert":
                raise ValueError(
                    f"{self.product_id}: upsert_strategy=incremental 은 publication_mode=upsert 가 필요"
                )
            if not self.event_time:
                raise ValueError(
                    f"{self.product_id}: upsert_strategy=incremental 은 event_time(워터마크 컬럼) 선언이 필요"
                )
        if self.freshness_field is not None and not IDENTIFIER_RE.fullmatch(self.freshness_field):
            raise ValueError(f"{self.product_id}: freshness_field must be a physical identifier")
        if self.empty_result_freshness is not None:
            if set(self.empty_result_freshness) != set(EMPTY_RESULT_FRESHNESS_FIELDS):
                raise ValueError(f"{self.product_id}: empty_result_freshness must contain relation and field")
            for field_name in EMPTY_RESULT_FRESHNESS_FIELDS:
                value = self.empty_result_freshness.get(field_name)
                if not isinstance(value, str) or not IDENTIFIER_RE.fullmatch(value):
                    raise ValueError(
                        f"{self.product_id}: empty_result_freshness.{field_name} must be a physical identifier"
                    )


def _merged_meta(node: dict[str, Any]) -> dict[str, Any]:
    top = node.get("meta") or {}
    config_meta = (node.get("config") or {}).get("meta") or {}
    return {**(top if isinstance(top, dict) else {}), **(config_meta if isinstance(config_meta, dict) else {})}


def _gates_by_model(manifest: dict[str, Any]) -> dict[str, list[str]]:
    """Model unique_id -> sorted test labels (for the _catalog `tests` column)."""
    gates: dict[str, set[str]] = {}
    for node in (manifest.get("nodes") or {}).values():
        if node.get("resource_type") != "test" or not node.get("attached_node"):
            continue
        meta = node.get("test_metadata") or {}
        label = meta.get("name") or node.get("name", "test")
        column = (meta.get("kwargs") or {}).get("column_name")
        gates.setdefault(node["attached_node"], set()).add(f"{label}({column})" if column else label)
    return {uid: sorted(names) for uid, names in gates.items()}


def _column_identity_meta(column: dict[str, Any]) -> dict[str, Any]:
    config_meta = ((column.get("config") or {}).get("meta") or {}) if isinstance(column, dict) else {}
    meta = config_meta if isinstance(config_meta, dict) else {}
    return {
        "data_type": str(column.get("data_type", "")).strip().lower(),
        "nullable": meta.get("nullable"),
        "semantic_role": meta.get("semantic_role"),
        "unit": meta.get("unit"),
    }


def _load_column_vocabulary_metadata(
    product_id: str,
    node: dict[str, Any],
) -> tuple[dict[str, str], tuple[dict[str, str], ...]]:
    """Normalize DBT column vocabulary metadata before any publication work starts."""
    columns = node.get("columns") or {}
    if not isinstance(columns, dict):
        raise ValueError(f"{product_id}: manifest columns must be a mapping")

    column_vocabularies: dict[str, str] = {}
    vocabulary_terms: list[dict[str, str]] = []
    for column_name, spec in columns.items():
        if not isinstance(spec, dict):
            raise ValueError(f"{product_id}.{column_name}: column metadata must be a mapping")
        top_meta = spec.get("meta")
        if top_meta is not None and not isinstance(top_meta, dict):
            raise ValueError(f"{product_id}.{column_name}: meta must be a mapping")
        config = spec.get("config")
        if config is not None and not isinstance(config, dict):
            raise ValueError(f"{product_id}.{column_name}: config must be a mapping")
        config_meta = config.get("meta") if isinstance(config, dict) else None
        if config_meta is not None and not isinstance(config_meta, dict):
            raise ValueError(f"{product_id}.{column_name}: config.meta must be a mapping")
        meta = {**(top_meta or {}), **(config_meta or {})}

        vocabulary_id = meta.get("vocabulary_id")
        terms = meta.get("vocabulary_terms")
        if vocabulary_id is None and terms is None:
            continue
        if not isinstance(vocabulary_id, str) or not vocabulary_id.strip():
            raise ValueError(f"{product_id}.{column_name}: vocabulary_terms requires vocabulary_id")
        column_vocabularies[str(column_name)] = vocabulary_id
        if terms is None:
            continue
        if not isinstance(terms, list):
            raise ValueError(f"{product_id}.{column_name}: vocabulary_terms must be a list")
        seen_codes: set[str] = set()
        for term in terms:
            if (
                not isinstance(term, dict)
                or set(term) != {"code", "label_ko"}
                or not isinstance(term.get("code"), str)
                or not term["code"].strip()
                or not isinstance(term.get("label_ko"), str)
                or not term["label_ko"].strip()
            ):
                raise ValueError(
                    f"{product_id}.{column_name}: vocabulary_terms entries require non-empty code and label_ko"
                )
            code = term["code"]
            if code in seen_codes:
                raise ValueError(f"{product_id}.{column_name}: vocabulary_terms code '{code}' is duplicated")
            seen_codes.add(code)
            vocabulary_terms.append(
                {
                    "vocabulary_id": vocabulary_id,
                    "code": code,
                    "label_ko": term["label_ko"],
                    "origin": "traffic_weather",
                    "source_type": "dbt_contract",
                }
            )
    return column_vocabularies, tuple(vocabulary_terms)


def _projection_schema_hash(
    schema_version: str,
    projection_columns: tuple[str, ...],
    manifest_columns: dict[str, Any],
) -> str:
    payload = {
        "schema_version": schema_version,
        "columns": [
            {
                "name": column_name,
                **_column_identity_meta(manifest_columns[column_name]),
            }
            for column_name in projection_columns
        ],
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _load_public_projection(
    *,
    product_id: str,
    serving: dict[str, Any],
    node: dict[str, Any],
    require_public_projection: bool,
) -> tuple[tuple[str, ...] | None, str | None, str | None]:
    projection = serving.get("public_projection")
    if projection is None:
        if require_public_projection:
            raise ValueError(f"{product_id}: public_projection required")
        return None, None, None
    if not isinstance(projection, dict):
        raise ValueError(f"{product_id}: public_projection must be object")
    if set(projection) != {"schema_version", "columns"}:
        raise ValueError(f"{product_id}: public_projection must contain exactly schema_version and columns")
    schema_version = projection.get("schema_version")
    if not isinstance(schema_version, str) or not SEMVER_RE.fullmatch(schema_version):
        raise ValueError(f"{product_id}: public_projection schema_version must be semver")
    raw_columns = projection.get("columns")
    if not isinstance(raw_columns, list) or not raw_columns:
        raise ValueError(f"{product_id}: public_projection columns must be a non-empty list")

    seen: set[str] = set()
    columns: list[str] = []
    for column in raw_columns:
        if not isinstance(column, str) or not IDENTIFIER_RE.fullmatch(column):
            raise ValueError(f"{product_id}: public_projection column must be physical identifier")
        if column in seen:
            raise ValueError(f"{product_id}: public_projection duplicate column {column}")
        seen.add(column)
        columns.append(column)

    manifest_columns = node.get("columns") if isinstance(node.get("columns"), dict) else {}
    for column in columns:
        if column not in manifest_columns:
            raise ValueError(f"{product_id}: public_projection unknown column {column}")
        identity = _column_identity_meta(manifest_columns[column])
        missing = [
            key
            for key, value in identity.items()
            if value in ("", None) or (key == "nullable" and not isinstance(value, bool))
        ]
        if missing:
            raise ValueError(f"{product_id}: public_projection identity metadata missing for {column}: {','.join(missing)}")

    public_columns = tuple(columns)
    return public_columns, schema_version, _projection_schema_hash(schema_version, public_columns, manifest_columns)


def _load_public_primary_key(
    product_id: str,
    serving: dict[str, Any],
    node: dict[str, Any],
    public_projection: tuple[str, ...] | None,
) -> tuple[str, ...]:
    """Return the physical public/D1 key, defaulting to the source model key.

    Rollup exporters may intentionally drop source-grain columns. In that case the
    public key must be declared explicitly and remain inside the public projection.
    """
    explicit_public_key = "public_primary_key" in serving
    raw = serving.get("public_primary_key", serving.get("primary_key"))
    if not isinstance(raw, list) or not raw:
        raise ValueError(f"{product_id}: public_primary_key/primary_key must be a non-empty list")
    if any(not isinstance(column, str) or not IDENTIFIER_RE.fullmatch(column) for column in raw):
        raise ValueError(f"{product_id}: public_primary_key must contain physical identifiers")
    if len(set(raw)) != len(raw):
        raise ValueError(f"{product_id}: public_primary_key contains duplicate columns")
    if explicit_public_key or public_projection is not None:
        manifest_columns = node.get("columns") if isinstance(node.get("columns"), dict) else {}
        missing = [column for column in raw if column not in manifest_columns]
        if missing:
            raise ValueError(f"{product_id}: public_primary_key unknown columns {','.join(missing)}")
    if public_projection is not None:
        projection = set(public_projection)
        missing = [column for column in raw if column not in projection]
        if missing:
            raise ValueError(f"{product_id}: public_primary_key columns missing from public_projection {','.join(missing)}")
    return tuple(raw)


def _load_freshness_field(
    product_id: str,
    serving: dict[str, Any],
    node: dict[str, Any],
    public_projection: tuple[str, ...] | None,
) -> str | None:
    """Load an explicit quality-time axis without changing legacy event_time behavior."""
    raw = serving.get("freshness_field")
    if raw is None:
        return None
    if not isinstance(raw, str) or not IDENTIFIER_RE.fullmatch(raw):
        raise ValueError(f"{product_id}: freshness_field must be a physical identifier")
    manifest_columns = node.get("columns") if isinstance(node.get("columns"), dict) else {}
    if raw not in manifest_columns:
        raise ValueError(f"{product_id}: freshness_field unknown column {raw}")
    if public_projection is not None and raw not in public_projection:
        raise ValueError(f"{product_id}: freshness_field missing from public_projection: {raw}")
    return raw


def _load_empty_result_freshness(
    product_id: str,
    serving: dict[str, Any],
    manifest: dict[str, Any],
) -> dict[str, str] | None:
    """Load the trusted upstream freshness source for a valid zero-row product."""
    raw = serving.get("empty_result_freshness")
    if raw is None:
        return None
    if not isinstance(raw, dict) or set(raw) != set(EMPTY_RESULT_FRESHNESS_FIELDS):
        raise ValueError(f"{product_id}: empty_result_freshness must contain relation and field")
    relation = raw.get("relation")
    field = raw.get("field")
    if not isinstance(relation, str) or not IDENTIFIER_RE.fullmatch(relation):
        raise ValueError(f"{product_id}: empty_result_freshness relation must be a model identifier")
    if not isinstance(field, str) or not IDENTIFIER_RE.fullmatch(field):
        raise ValueError(f"{product_id}: empty_result_freshness field must be a physical identifier")
    source_nodes = [
        node
        for node in (manifest.get("nodes") or {}).values()
        if node.get("resource_type") == "model" and node.get("name") == relation
    ]
    if len(source_nodes) != 1:
        raise ValueError(f"{product_id}: empty_result_freshness relation unknown model {relation}")
    source_columns = source_nodes[0].get("columns")
    if not isinstance(source_columns, dict) or field not in source_columns:
        raise ValueError(
            f"{product_id}: empty_result_freshness field unknown column {field} on {relation}"
        )
    return {"relation": relation, "field": field}


def _load_query_availability(
    product_id: str, serving: dict[str, Any], manifest: dict[str, Any]
) -> str | None:
    """Load the optional, fixed-shape companion used by risk publication gates."""
    raw = serving.get("query_availability")
    if raw is None:
        return None
    if not isinstance(raw, dict) or set(raw) != set(QUERY_AVAILABILITY_FIELDS):
        raise ValueError(f"{product_id}: query_availability must contain relation")
    relation = raw.get("relation")
    if not isinstance(relation, str) or not IDENTIFIER_RE.fullmatch(relation):
        raise ValueError(f"{product_id}: query_availability relation must be a model identifier")
    nodes = [
        node for node in (manifest.get("nodes") or {}).values()
        if node.get("resource_type") == "model" and node.get("name") == relation
    ]
    if len(nodes) != 1:
        raise ValueError(f"{product_id}: query_availability relation unknown model {relation}")
    columns = nodes[0].get("columns")
    if not isinstance(columns, dict):
        raise ValueError(f"{product_id}: query_availability relation unknown columns")
    for column in QUERY_AVAILABILITY_COLUMNS:
        if column not in columns:
            raise ValueError(f"{product_id}: query_availability relation {relation} unknown column {column}")
    return relation


def _load_source_evidence(product_id: str, serving: dict[str, Any]) -> tuple[dict[str, Any], ...] | None:
    """Load source/right records without silently accepting incomplete evidence.

    ``None`` means the legacy contract has not adopted the evidence extension and
    tells the Publisher to preserve any existing source rows. A declared value,
    however, is an immutable readiness input: malformed or empty values stop the
    publication rather than degrading into an apparently source-less product.
    """
    raw_evidence = serving.get("source_evidence")
    if raw_evidence is None:
        return None
    if not isinstance(raw_evidence, list) or not raw_evidence:
        raise ValueError(f"{product_id}: source_evidence must be a non-empty list")

    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, raw_source in enumerate(raw_evidence):
        label = f"{product_id}: source_evidence[{index}]"
        if not isinstance(raw_source, dict):
            raise ValueError(f"{label} must be an object")
        unknown = sorted(set(raw_source) - set(SOURCE_EVIDENCE_FIELDS))
        if unknown:
            raise ValueError(f"{label} has unsupported fields: {','.join(unknown)}")
        row: dict[str, Any] = {}
        for field in SOURCE_EVIDENCE_FIELDS:
            value = raw_source.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{label}: {field} must be a non-empty string")
            row[field] = value.strip()
        if not IDENTIFIER_RE.fullmatch(row["source_id"]):
            raise ValueError(f"{label}: source_id must be an identifier")
        if row["source_id"] in seen_ids:
            raise ValueError(f"{label}: source_id is duplicated")
        seen_ids.add(row["source_id"])
        if not _is_public_https_url(row["source_url"]) or not _is_public_https_url(row["license_url"]):
            raise ValueError(f"{label}: source_url and license_url must use https")
        if row["redistribution"] not in SOURCE_EVIDENCE_REDISTRIBUTION:
            raise ValueError(f"{label}: unsupported redistribution={row['redistribution']!r}")
        try:
            date.fromisoformat(row["rights_checked_at"])
        except ValueError as exc:
            raise ValueError(f"{label}: rights_checked_at must be YYYY-MM-DD") from exc
        rows.append(row)
    return tuple(rows)


def _is_public_https_url(value: str) -> bool:
    """Reject credentials or ambiguous non-HTTPS references before they reach D1 metadata."""
    parsed = urlparse(value)
    return parsed.scheme == "https" and bool(parsed.netloc) and not parsed.username and not parsed.password


def _load_quality_coverage(
    product_id: str,
    serving: dict[str, Any],
    node: dict[str, Any],
) -> dict[str, Any] | None:
    """Load a small, reproducible distinct-coverage declaration (#678).

    The expected count is a contract input; observed count and gate result are only
    computed by the Publisher from the exact publication source rows.
    """
    raw = serving.get("quality_coverage")
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError(f"{product_id}: quality_coverage must be object")
    if set(raw) == set(QUALITY_COVERAGE_NOT_APPLICABLE_FIELDS):
        reason = raw.get("not_applicable_reason")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError(f"{product_id}: quality_coverage not_applicable_reason must be non-empty")
        return {"not_applicable_reason": reason.strip()}
    allowed_fields = set(QUALITY_COVERAGE_REQUIRED_FIELDS) | set(QUALITY_COVERAGE_OPTIONAL_FIELDS)
    if not set(QUALITY_COVERAGE_REQUIRED_FIELDS).issubset(raw) or not set(raw).issubset(allowed_fields):
        raise ValueError(
            f"{product_id}: quality_coverage must contain {','.join(QUALITY_COVERAGE_REQUIRED_FIELDS)} "
            f"and optional {','.join(QUALITY_COVERAGE_OPTIONAL_FIELDS)}"
        )
    field = raw.get("field")
    if not isinstance(field, str) or not IDENTIFIER_RE.fullmatch(field):
        raise ValueError(f"{product_id}: quality_coverage field must be a physical identifier")
    columns = node.get("columns") if isinstance(node.get("columns"), dict) else {}
    if field not in columns:
        raise ValueError(f"{product_id}: quality_coverage field is not a model column: {field}")
    expected = raw.get("expected_distinct_count")
    if isinstance(expected, bool) or not isinstance(expected, int) or expected < 1:
        raise ValueError(f"{product_id}: quality_coverage expected_distinct_count must be a positive integer")
    minimum_ratio = raw.get("minimum_ratio")
    if (
        isinstance(minimum_ratio, bool)
        or not isinstance(minimum_ratio, (int, float))
        or not 0 < float(minimum_ratio) <= 1
    ):
        raise ValueError(f"{product_id}: quality_coverage minimum_ratio must be in (0, 1]")
    measurement_scope = raw.get("measurement_scope", "published_rows")
    if measurement_scope not in QUALITY_COVERAGE_MEASUREMENT_SCOPES:
        raise ValueError(
            f"{product_id}: quality_coverage measurement_scope must be one of "
            f"{','.join(sorted(QUALITY_COVERAGE_MEASUREMENT_SCOPES))}"
        )
    return {
        "field": field,
        "expected_distinct_count": expected,
        "minimum_ratio": float(minimum_ratio),
        "measurement_scope": measurement_scope,
    }


def load_contracts(
    manifest_path: str | Path,
    product_ids: Iterable[str] | None = None,
    *,
    enabled_only: bool = True,
    require_public_projection: bool = False,
) -> list[ServingContract]:
    """Parse a dbt manifest into ``ServingContract`` records.

    ``product_ids`` (from the thin domain DAG) restricts the set; ``None`` loads all
    models declaring a serving contract. ``enabled_only`` drops ``enabled: false``.
    """
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    wanted = set(product_ids) if product_ids is not None else None
    gates = _gates_by_model(manifest)

    contracts: list[ServingContract] = []
    for uid, node in (manifest.get("nodes") or {}).items():
        if node.get("resource_type") != "model":
            continue
        metadata = _merged_meta(node)
        serving = metadata.get("serving")
        if not isinstance(serving, dict) or not serving:
            continue
        product_id = serving.get("product_id")
        if not product_id:
            continue
        if wanted is not None and product_id not in wanted:
            continue
        if enabled_only and not bool(serving.get("enabled", False)):
            continue
        partial = serving.get("partial_policy") or {}
        public_projection, projection_schema_version, projection_schema_hash = _load_public_projection(
            product_id=str(product_id),
            serving=serving,
            node=node,
            require_public_projection=require_public_projection,
        )
        public_primary_key = _load_public_primary_key(
            str(product_id), serving, node, public_projection
        )
        freshness_field = _load_freshness_field(
            str(product_id), serving, node, public_projection
        )
        empty_result_freshness = _load_empty_result_freshness(
            str(product_id), serving, manifest
        )
        query_availability_relation = _load_query_availability(
            str(product_id), serving, manifest
        )
        column_vocabularies, vocabulary_terms = _load_column_vocabulary_metadata(
            str(product_id), node
        )
        contracts.append(
            ServingContract(
                product_id=str(product_id),
                model_name=str(node.get("name", "")),
                enabled=bool(serving.get("enabled", False)),
                external=bool(serving.get("external", False)),
                publication_mode=str(serving.get("publication_mode", "")),
                zero_policy=str(serving.get("zero_policy", "retain_last_good")),
                primary_key=public_primary_key,
                upsert_strategy=serving.get("upsert_strategy"),
                partial_min_ratio=partial.get("min_publish_ratio") if isinstance(partial, dict) else None,
                reliability=serving.get("reliability") if isinstance(serving.get("reliability"), dict) else None,
                  event_time=serving.get("event_time"),
                  freshness_field=freshness_field,
                  empty_result_freshness=empty_result_freshness,
                  query_availability_relation=query_availability_relation,
                  description=str(node.get("description", "")),
                  product_question=str(serving.get("product_question", "")),
                  tests=tuple(gates.get(uid, [])),
                  public_gold=(
                      dict(metadata["public_gold"])
                      if isinstance(metadata.get("public_gold"), dict)
                      else None
                  ),
                  mcp_projection=(
                      dict(serving["mcp_projection"])
                      if isinstance(serving.get("mcp_projection"), dict)
                      else None
                  ),
                  public_projection=public_projection,
                  projection_schema_version=projection_schema_version,
                  projection_schema_hash=projection_schema_hash,
                  freshness_slo_minutes=(
                      int(serving["freshness_slo_minutes"])
                      if isinstance(serving.get("freshness_slo_minutes"), int)
                      and not isinstance(serving.get("freshness_slo_minutes"), bool)
                      else None
                  ),
                  publication_trigger=(
                      dict(serving["publication_trigger"])
                      if isinstance(serving.get("publication_trigger"), dict)
                      else None
                  ),
                  source_evidence=_load_source_evidence(str(product_id), serving),
                  quality_coverage=_load_quality_coverage(str(product_id), serving, node),
                  grain=serving.get("grain"),
                  # commerce 로컬 키(serving_tier/d1_rollup)와 #638 §1 스펙 키(tier/rollup_rule) 겸용
                  display=serving.get("display"),
                  serving_tier=serving.get("serving_tier") or serving.get("tier"),
                  rollup_rule=serving.get("d1_rollup") or serving.get("rollup_rule"),
                  column_descriptions=(
                      {
                          str(column): str(spec.get("description") or "").strip()
                          for column, spec in node["columns"].items()
                      }
                      if isinstance(node.get("columns"), dict) and node["columns"]
                      else None
                  ),
                  column_vocabularies=column_vocabularies,
                  vocabulary_terms=vocabulary_terms,
                  usage_patterns=tuple(
                      pattern
                      for pattern in (serving.get("usage_patterns") or ())
                      if isinstance(pattern, dict)
                  ),
              )
        )
    contracts.sort(key=lambda c: c.model_name)
    return contracts


def load_domain_contracts(
    manifest_path: str | Path,
    domain: str,
    product_ids: Iterable[str],
    *,
    require_public_projection: bool = False,
    allow_partitioned_scope: bool = False,
) -> list[ServingContract]:
    """Load one wrapper's complete enabled domain contract set.

    A domain exporter must not silently publish a subset of its enabled dbt
    contracts. Model names remain the dbt-owned domain boundary, so no D1 table
    or product list is duplicated in the DAG factory. A domain with explicitly
    separated publisher paths may opt into a partitioned scope; each such path
    still has to list only enabled contracts from the same domain.
    """
    requested = list(product_ids)
    duplicates = sorted({product_id for product_id in requested if requested.count(product_id) > 1})
    if duplicates:
        raise ValueError(f"{domain}: duplicate product_ids={','.join(duplicates)}")

    prefix = f"gold_{domain}_"
    enabled_domain_contracts = [
        contract
        for contract in load_contracts(
            manifest_path,
            require_public_projection=require_public_projection,
        )
        if contract.model_name.startswith(prefix)
    ]
    enabled_ids = {contract.product_id for contract in enabled_domain_contracts}
    requested_ids = set(requested)
    missing = sorted(enabled_ids - requested_ids) if not allow_partitioned_scope else []
    unexpected = sorted(requested_ids - enabled_ids)
    if missing or unexpected:
        detail = " ".join(
            part
            for part in (
                f"missing={','.join(missing)}" if missing else "",
                f"unexpected={','.join(unexpected)}" if unexpected else "",
            )
            if part
        )
        raise ValueError(f"{domain}: enabled serving exact-set mismatch {detail}")

    return [
        contract
        for contract in enabled_domain_contracts
        if contract.product_id in requested_ids
    ]


def load_domain_retirement_product_ids(
    manifest_path: str | Path,
    domain: str,
) -> tuple[str, ...]:
    """Load disabled contracts whose public catalog entries must be retired.

    Retirement is intentionally a separate pass from ``load_domain_contracts``:
    active export contracts remain enabled-only, while catalog cleanup is allowed
    only for an explicit disabled and non-external DBT declaration.
    """

    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    prefix = f"gold_{domain}_"
    product_ids: list[str] = []
    for node in (manifest.get("nodes") or {}).values():
        if node.get("resource_type") != "model" or not str(node.get("name", "")).startswith(prefix):
            continue
        metadata = _merged_meta(node)
        serving = metadata.get("serving")
        if not isinstance(serving, dict):
            continue
        retirement = serving.get("retire_on_publish")
        if retirement is None or retirement is False:
            continue
        if retirement is not True:
            raise ValueError(f"{domain}: retire_on_publish must be boolean true")
        if serving.get("enabled") is not False or serving.get("external") is not False:
            raise ValueError(
                f"{domain}: retire_on_publish requires enabled=false and external=false"
            )
        product_id = serving.get("product_id")
        if not isinstance(product_id, str) or not IDENTIFIER_RE.fullmatch(product_id):
            raise ValueError(f"{domain}: retire_on_publish requires a valid product_id")
        product_ids.append(product_id)

    duplicates = sorted({product_id for product_id in product_ids if product_ids.count(product_id) > 1})
    if duplicates:
        raise ValueError(f"{domain}: duplicate retired product_ids={','.join(duplicates)}")
    return tuple(sorted(product_ids))
