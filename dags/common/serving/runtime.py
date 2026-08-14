"""Real runtime seams for the common publisher (Trino source, D1 client, smoke).

Domain-neutral: builds a Trino connection and the Cloudflare D1 client from the
compose environment (same env var names citydata already uses), so no domain module
is imported. Network/driver imports are lazy — this file is never imported by the
publisher unit tests, only by the DAG at runtime.

Secrets come only from the environment (``CLOUDFLARE_API_TOKEN``) and are never
logged. Account/DB ids are non-secret identifiers passed via ``SERVING_*`` env keys.
"""

from __future__ import annotations

import os
import re
import time
from typing import Any

from common.serving.d1_client import Column, HttpD1Client
from common.serving.contract import QUERY_AVAILABILITY_COLUMNS, ServingContract
from common.serving.publisher import QueryAvailabilityPlan, ReadPlan

APPEND_LOOKBACK_HOURS = 2
APPEND_LOOKBACK_DAYS = 2
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SMOKE_CODE_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


def _quote_identifier(identifier: str) -> str:
    if not isinstance(identifier, str) or not IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"unsafe projection identifier: {identifier!r}")
    return f'"{identifier}"'


# ── Trino source ──────────────────────────────────────────────────────────────────

class TrinoSourceReader:
    """Read publishable rows from the domain's Iceberg Gold via a Trino cursor."""

    def __init__(self, cursor: Any, catalog: str, schema: str) -> None:
        self._cursor = cursor
        self._catalog = catalog
        self._schema = schema

    def _relation(self, model_name: str) -> str:
        return f"{self._catalog}.{self._schema}.{model_name}"

    def _columns(self, model_name: str) -> list[Column]:
        self._cursor.execute(f"SHOW COLUMNS FROM {self._relation(model_name)}")
        return [(row[0], row[1]) for row in self._cursor.fetchall()]

    def _select(self, sql: str) -> list[dict[str, Any]]:
        self._cursor.execute(sql)
        colnames = [d[0] for d in self._cursor.description]
        return [dict(zip(colnames, row)) for row in self._cursor.fetchall()]

    def _projected_columns(self, contract: ServingContract, physical_columns: list[Column]) -> list[Column] | None:
        if contract.public_projection is None:
            return None
        projection = tuple(contract.public_projection)
        if len(set(projection)) != len(projection):
            raise ValueError(f"{contract.model_name}: duplicate public_projection columns")
        physical_by_name = dict(physical_columns)
        missing = [column for column in projection if column not in physical_by_name]
        if missing:
            raise ValueError(f"{contract.model_name}: missing projected columns {','.join(missing)}")

        projection_set = set(projection)
        missing_primary_key = [column for column in contract.primary_key if column not in projection_set]
        if missing_primary_key:
            raise ValueError(f"{contract.model_name}: primary_key columns missing from public_projection {','.join(missing_primary_key)}")
        if contract.event_time and contract.event_time not in projection_set:
            raise ValueError(f"{contract.model_name}: event_time missing from public_projection {contract.event_time}")
        sample_count_field = (
            contract.reliability.get("sample_count_field")
            if isinstance(contract.reliability, dict)
            else None
        )
        if sample_count_field and sample_count_field not in projection_set:
            raise ValueError(f"{contract.model_name}: reliability sample_count_field missing from public_projection {sample_count_field}")

        for column in projection:
            _quote_identifier(column)
        return [(column, physical_by_name[column]) for column in projection]

    def _select_list(self, projected_columns: list[Column] | None) -> str:
        if projected_columns is None:
            return "*"
        return ",".join(_quote_identifier(column) for column, _type in projected_columns)

    def _empty_result_freshness(self, contract: ServingContract) -> Any | None:
        fallback = contract.empty_result_freshness
        if fallback is None:
            return None
        relation = self._relation(fallback["relation"])
        field = _quote_identifier(fallback["field"])
        self._cursor.execute(f"SELECT MAX({field}) FROM {relation}")
        row = self._cursor.fetchone()
        return row[0] if row else None

    def _query_availability_plan(self, relation: str) -> QueryAvailabilityPlan:
        physical = self._columns(relation)
        types = dict(physical)
        missing = [column for column in QUERY_AVAILABILITY_COLUMNS if column not in types]
        if missing:
            raise ValueError(f"{relation}: query_availability missing columns {','.join(missing)}")
        columns = [(column, types[column]) for column in QUERY_AVAILABILITY_COLUMNS]
        select_list = ",".join(_quote_identifier(column) for column in QUERY_AVAILABILITY_COLUMNS)
        return QueryAvailabilityPlan(columns, self._select(
            f"SELECT {select_list} FROM {self._relation(relation)}"
        ))

    def _plan(
        self,
        contract: ServingContract,
        *,
        columns: list[Column],
        rows: list[dict[str, Any]],
        coverage_observed_distinct_count: int | None,
        delete_column: str | None = None,
        delete_literal: str | None = None,
    ) -> ReadPlan:
        return ReadPlan(
            columns=columns,
            rows=rows,
            delete_column=delete_column,
            delete_literal=delete_literal,
            coverage_observed_distinct_count=coverage_observed_distinct_count,
            empty_result_freshness=(
                self._empty_result_freshness(contract) if not rows else None
            ),
            query_availability=(
                self._query_availability_plan(contract.query_availability_relation)
                if contract.query_availability_relation is not None
                else None
            ),
        )

    def read(self, contract: ServingContract, last_good_max: Any | None) -> ReadPlan:
        columns = self._columns(contract.model_name)
        relation = self._relation(contract.model_name)
        projected_columns = self._projected_columns(contract, columns)
        select_list = self._select_list(projected_columns)
        read_columns = projected_columns or columns
        coverage_observed_distinct_count = None
        coverage = contract.quality_coverage or {}
        if coverage.get("measurement_scope") == "source_relation":
            coverage_field = _quote_identifier(coverage["field"])
            self._cursor.execute(
                f"SELECT COUNT(DISTINCT {coverage_field}) FROM {relation}"
            )
            coverage_observed_distinct_count = int(self._cursor.fetchone()[0])

        incremental_upsert = (
            contract.publication_mode == "upsert"
            and contract.upsert_strategy == "incremental"
            and bool(contract.event_time)
        )
        # 워터마크 기반 증분 읽기는 append(시간 윈도우 재적재)와 incremental upsert(바뀐 그레인만)
        # 두 경우뿐. 그 외(snapshot·exact_set upsert·event_time 없는 append)는 전량 읽는다.
        if not incremental_upsert and (contract.publication_mode != "append" or not contract.event_time):
            rows = self._select(f"SELECT {select_list} FROM {relation}")
            return self._plan(
                contract,
                columns=read_columns,
                rows=rows,
                coverage_observed_distinct_count=coverage_observed_distinct_count,
            )

        column_type = dict(columns).get(contract.event_time, "")
        if last_good_max is None:
            rows = self._select(f"SELECT {select_list} FROM {relation}")  # first run: full backfill
            return self._plan(
                contract,
                columns=read_columns,
                rows=rows,
                coverage_observed_distinct_count=coverage_observed_distinct_count,
            )

        import pendulum

        base = pendulum.parse(str(last_good_max).replace(" ", "T"))
        event_time = _quote_identifier(contract.event_time)

        if incremental_upsert:
            # incremental upsert: D1 워터마크(=현 max event_time) 이후 바뀐 그레인만 읽어
            # partial INSERT OR REPLACE 로 그 PK 만 덮는다(delete 없음 — append 아님).
            # 워터마크는 초 단위로 내림 + `>=` 로 경계 버킷을 놓치지 않게(재적재는 PK 멱등이라 무해).
            if column_type.startswith("date"):
                watermark = base.format("YYYY-MM-DD")
                literal = f"date '{watermark}'"
            else:
                watermark = base.format("YYYY-MM-DD HH:mm:ss")
                literal = f"timestamp '{watermark}'"
            rows = self._select(f"SELECT {select_list} FROM {relation} WHERE {event_time} >= {literal}")
            return self._plan(
                contract,
                columns=read_columns,
                rows=rows,
                coverage_observed_distinct_count=coverage_observed_distinct_count,
            )

        # append: re-load only the recent window (idempotent) + any new rows.
        if column_type.startswith("date"):
            cutoff = base.subtract(days=APPEND_LOOKBACK_DAYS).format("YYYY-MM-DD")
            literal = f"date '{cutoff}'"
        else:
            cutoff = base.subtract(hours=APPEND_LOOKBACK_HOURS).format("YYYY-MM-DD HH:00:00")
            literal = f"timestamp '{cutoff}'"
        rows = self._select(f"SELECT {select_list} FROM {relation} WHERE {event_time} >= {literal}")
        return self._plan(
            contract,
            columns=read_columns,
            rows=rows,
            delete_column=contract.event_time,
            delete_literal=f"'{cutoff}'",
            coverage_observed_distinct_count=coverage_observed_distinct_count,
        )


def _trino_settings(target: str, schema: str) -> dict[str, Any]:
    dev = target != "prod"
    # 카탈로그 키 이름은 배포 환경을 담지 않는다 — canonical ``TRINO_ICEBERG_CATALOG`` 하나이고
    # 값이 배포를 따라간다(호스트 컴포즈도 이 값으로 카탈로그 파일 이름을 짓는다).
    # 미설정 시 기본만 타깃에 따라 다르다.
    catalog = os.environ.get("TRINO_ICEBERG_CATALOG") or ("iceberg_dev" if dev else "iceberg")
    return {
        "host": os.environ.get("TRINO_HOST", "trino"),
        "port": int(os.environ.get("TRINO_PORT", "8080")),
        "user": os.environ.get("TRINO_USER", "airflow"),
        "http_scheme": os.environ.get("TRINO_HTTP_SCHEME", "http"),
        "catalog": catalog,
        "schema": schema,
    }


def build_trino_source_reader(target: str, schema: str) -> TrinoSourceReader:
    import trino.dbapi

    settings = _trino_settings(target, schema)
    conn = trino.dbapi.connect(
        host=settings["host"], port=settings["port"], user=settings["user"],
        catalog=settings["catalog"], http_scheme=settings["http_scheme"],
    )
    return TrinoSourceReader(conn.cursor(), settings["catalog"], settings["schema"])


# ── Cloudflare D1 client ──────────────────────────────────────────────────────────

def build_d1_client_from_env() -> HttpD1Client:
    token = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    account = os.environ.get("SERVING_CLOUDFLARE_ACCOUNT_ID", "")
    database = os.environ.get("SERVING_D1_DATABASE_ID", "")
    if not token:
        raise RuntimeError("CLOUDFLARE_API_TOKEN 미설정 — compose airflow env 에 전달 필요 (D1 Edit)")
    if not account or not database:
        raise RuntimeError("SERVING_CLOUDFLARE_ACCOUNT_ID / SERVING_D1_DATABASE_ID 미설정 — .env 에 추가 필요")
    api_url = f"https://api.cloudflare.com/client/v4/accounts/{account}/d1/database/{database}/query"
    return HttpD1Client(api_url, token)


# ── API smoke test ────────────────────────────────────────────────────────────────

class HttpSmokeTester:
    """Hit the authenticated public serving API for one representative row."""

    def __init__(self, base_url: str, bearer_token: str = "") -> None:
        base_url = base_url.rstrip("/")
        self._api_base_url = (
            base_url if base_url.endswith("/api/v1") else f"{base_url}/api/v1"
        ) if base_url else ""
        self._bearer_token = bearer_token.strip()
        self._diagnostics: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _response_diagnostic(resp: Any, latency_ms: int) -> dict[str, Any]:
        diagnostic: dict[str, Any] = {
            "http_status": getattr(resp, "status_code", None),
            "latency_ms": latency_ms,
        }
        headers = getattr(resp, "headers", None)
        header_get = getattr(headers, "get", None)
        raw_cf_ray = header_get("CF-Ray") if callable(header_get) else None
        cf_ray = raw_cf_ray.strip() if isinstance(raw_cf_ray, str) else ""
        if SMOKE_CODE_RE.fullmatch(cf_ray):
            diagnostic["cf_ray"] = cf_ray

        try:
            payload = resp.json()
        except Exception:  # noqa: BLE001 - response body is intentionally not retained
            return diagnostic
        if not isinstance(payload, dict):
            return diagnostic
        error = payload.get("error")
        source = error if isinstance(error, dict) else payload
        error_code = str(source.get("code") or "").strip()
        if SMOKE_CODE_RE.fullmatch(error_code):
            diagnostic["error_code"] = error_code
        blockers = source.get("blockers")
        details = source.get("details")
        if blockers is None and isinstance(details, dict):
            blockers = details.get("blockers")
        if isinstance(blockers, (list, tuple)):
            safe_blockers = [
                str(blocker)
                for blocker in blockers[:10]
                if SMOKE_CODE_RE.fullmatch(str(blocker))
            ]
            if safe_blockers:
                diagnostic["blockers"] = safe_blockers
        return diagnostic

    def diagnostic(self, model_name: str) -> dict[str, Any] | None:
        value = self._diagnostics.get(model_name)
        return dict(value) if value is not None else None

    def check(self, model_name: str) -> str:
        if not self._api_base_url:
            self._diagnostics[model_name] = {"reason": "missing_base_url"}
            return "not_evaluated"
        if not self._bearer_token:
            self._diagnostics[model_name] = {"reason": "missing_bearer_token"}
            return "failed"
        import requests

        started_at = time.perf_counter()
        try:
            resp = requests.get(
                f"{self._api_base_url}/data/{model_name}",
                params={"limit": 1},
                headers={"Authorization": f"Bearer {self._bearer_token}"},
                timeout=30,
            )
        except Exception as exc:  # noqa: BLE001 -- unreachable API is a smoke failure
            self._diagnostics[model_name] = {
                "reason": "request_exception",
                "exception_type": type(exc).__name__,
                "latency_ms": max(0, int((time.perf_counter() - started_at) * 1000)),
            }
            return "failed"
        self._diagnostics[model_name] = self._response_diagnostic(
            resp,
            max(0, int((time.perf_counter() - started_at) * 1000)),
        )
        return "passed" if resp.status_code == 200 else "failed"


def build_smoke_tester_from_env() -> HttpSmokeTester:
    return HttpSmokeTester(
        os.environ.get("SERVING_API_BASE_URL", ""),
        os.environ.get("SERVING_API_SMOKE_TOKEN", ""),
    )
