"""Behavioral oracle for the common D1 Publisher.

Exercises the full Publication pipeline (gate → write → verify → _catalog → smoke)
against in-memory fakes: no Trino, no Cloudflare, no Airflow, no prod D1.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from common.serving.contract import QUERY_AVAILABILITY_COLUMNS, ServingContract, load_contracts
from common.serving.d1_client import Column, ProductPublicationState
from common.serving.gate import STATUS_DEGRADED, STATUS_PUBLISHED, STATUS_SKIPPED
from common.serving.publisher import PublicationError, QueryAvailabilityPlan, ReadPlan, query_availability_fingerprint, validate_query_availability, publish

FIXTURES = Path(__file__).parent / "fixtures"
COLUMNS: list[Column] = [("product_row_id", "varchar"), ("place_id", "varchar"), ("forecast_at", "timestamp")]
AVAILABILITY_COLUMNS: list[Column] = [
    ("place_id", "varchar"),
    ("snapshot_as_of_hour", "timestamp(6)"),
    ("available_from_at", "timestamp(6)"),
    ("available_to_at", "timestamp(6)"),
    ("forecast_collected_at_min", "timestamp(6)"),
    ("forecast_collected_at_max", "timestamp(6)"),
    ("expected_forecast_hour_count", "bigint"),
    ("observed_forecast_hour_count", "bigint"),
    ("availability_status", "varchar"),
    ("source_population_revision", "varchar"),
]
POPULATION_REVISION = "kma_admin_dong_grid_20260325:638f0e8260b47eeb0335126a87a8a38e7b456da872bf0ea7e28eecf427610e32"


# ── fakes ───────────────────────────────────────────────────────────────────────────

class FakeD1:
    def __init__(self) -> None:
        self.tables: dict[str, list[dict[str, Any]]] = {}
        self.primary_keys: dict[str, tuple[str, ...]] = {}
        self.columns_by_table: dict[str, list[Column]] = {}
        self.catalog: dict[str, dict[str, Any]] = {}
        self.ledger: list[dict[str, Any]] = []
        self.previous_tables: dict[str, list[dict[str, Any]]] = {}
        self.replace_calls = 0
        self.read_table_rows_calls: list[tuple[str, list[Column], tuple[str, ...]]] = []
        self.product_meta: dict[str, dict[str, Any]] = {}  # 핸드오프 메타(#638) 게시 기록
        self.glossary_rows: list[dict[str, Any]] = []
        self.product_evidence: dict[str, dict[str, Any]] = {}  # V1 source/quality evidence (#678)
        self.execute_calls: list[str] = []                 # export 시점 패턴 검증(Serving#217)
        self.execute_result: list[dict[str, Any]] = [{"n": 1}]  # 기본: 1행 반환(검증 통과)
        self.execute_error: Exception | None = None        # 설정 시 execute 가 예외
        self.query_availability: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self.staged_query_availability: list[dict[str, Any]] = []
        self.stage_snapshot_calls: list[str] = []

    def execute(self, sql: str) -> list[dict[str, Any]]:
        self.execute_calls.append(sql)
        if self.execute_error is not None:
            raise self.execute_error
        return list(self.execute_result)

    def table_row_count(self, name: str) -> int:
        return len(self.tables.get(name, []))

    def primary_key_stats(self, name: str, primary_key) -> tuple[int, int, int]:
        rows = self.tables.get(name, [])
        values = [tuple(row.get(column) for column in primary_key) for row in rows]
        return len(rows), len(set(values)), sum(1 for value in values if any(part is None for part in value))

    def table_max(self, name: str, column: str) -> Any | None:
        values = [r.get(column) for r in self.tables.get(name, []) if r.get(column) is not None]
        return max(values) if values else None

    def catalog_row(self, name: str) -> dict[str, Any] | None:
        return dict(self.catalog[name]) if name in self.catalog else None

    def ensure_table(self, name: str, columns, primary_key) -> None:
        self.tables.setdefault(name, [])
        self.primary_keys[name] = tuple(primary_key)
        self.columns_by_table.setdefault(name, list(columns))

    def replace_table(self, name: str, columns, rows, primary_key) -> None:
        self.replace_calls += 1
        if name in self.tables:
            self.previous_tables[name] = [dict(row) for row in self.tables[name]]
        self.primary_keys[name] = tuple(primary_key)
        self.columns_by_table[name] = list(columns)
        self.tables[name] = [dict(r) for r in rows]  # atomic swap semantics

    def restore_replaced_table(self, name: str) -> None:
        if name in self.previous_tables:
            self.tables[name] = self.previous_tables.pop(name)
        else:
            self.tables.pop(name, None)

    def finalize_replaced_table(self, name: str) -> None:
        self.previous_tables.pop(name, None)

    def delete_where_gte(self, name: str, column: str, trino_literal: str) -> None:
        cutoff = trino_literal.strip().strip("'")
        self.tables[name] = [r for r in self.tables.get(name, []) if str(r.get(column)) < cutoff]

    def insert_rows(self, name: str, columns, rows, *, replace: bool) -> None:
        table = self.tables.setdefault(name, [])
        if not replace:
            table.extend(dict(row) for row in rows)
            return
        primary_key = self.primary_keys[name]
        positions = {tuple(row.get(column) for column in primary_key): index for index, row in enumerate(table)}
        for row in rows:
            key = tuple(row.get(column) for column in primary_key)
            if key in positions:
                table[positions[key]] = dict(row)
            else:
                positions[key] = len(table)
                table.append(dict(row))

    def upsert_catalog(self, catalog_rows) -> None:
        for row in catalog_rows:
            self.catalog[row["name"]] = dict(row)

    def delete_catalog_row(self, name: str) -> None:
        self.catalog.pop(name, None)

    def append_publication_ledger(self, record: dict[str, Any]) -> None:
        self.ledger.append(dict(record))

    def catalog_domain_count(self, model_names: set[str]) -> int:
        return sum(1 for n in model_names if n in self.catalog)

    def read_table_rows(self, name, ordered_columns, primary_key):
        self.read_table_rows_calls.append((name, list(ordered_columns), tuple(primary_key)))
        rows = sorted(
            self.tables.get(name, []),
            key=lambda row: tuple(row.get(column) for column in primary_key),
        )
        return [
            {column: row.get(column) for column, _type in ordered_columns}
            for row in rows
        ]

    def publish_product_meta(
        self, product_id, publication_id, columns_rows, ext_rows, pattern_rows, display_rows=(),
        *, param_rows=(), vocabulary_rows=()
    ) -> None:
        self.product_meta[product_id] = {
            "publication_id": publication_id,
            "columns": [dict(row) for row in columns_rows],
            "ext": [dict(row) for row in ext_rows],
            "patterns": [dict(row) for row in pattern_rows],
            "params": [dict(row) for row in param_rows],
            "vocabularies": [dict(row) for row in vocabulary_rows],
        }

    def publish_glossary(self, rows) -> None:
        self.glossary_rows = [dict(row) for row in rows]

    def publish_product_evidence(self, product_id, publication_id, sources, quality) -> None:
        self.product_evidence[product_id] = {
            "publication_id": publication_id,
            "sources": None if sources is None else [dict(source) for source in sources],
            "quality": dict(quality),
        }

    def stage_query_availability(self, product_id, publication_id, rows, *, fingerprint, measured_at) -> None:
        staged = [dict(row, product_id=product_id, publication_id=publication_id,
                       availability_fingerprint=fingerprint, measured_at=measured_at) for row in rows]
        self.staged_query_availability.extend(staged)
        self.query_availability[product_id, publication_id] = staged

    def read_query_availability_rows(self, product_id, publication_id):
        return [dict(row) for row in self.query_availability.get((product_id, publication_id), [])]

    def prepare_atomic_publication_schema(self):
        pass

    def stage_snapshot(self, name, columns, rows, primary_key):
        self.stage_snapshot_calls.append(name)
        self.tables[f"{name}__staging"] = [dict(row) for row in rows]
        self.columns_by_table[f"{name}__staging"] = list(columns)
        self.primary_keys[f"{name}__staging"] = tuple(primary_key)

    def read_staged_snapshot_rows(self, name, columns, primary_key):
        return self.read_table_rows(f"{name}__staging", columns, primary_key)

    def capture_product_publication_state(self, product_id, model_name):
        return ProductPublicationState(self.catalog_row(model_name), {}, (), None, model_name in self.tables)

    def preflight_staged_transition(self, product_id, model_name, candidate, previous):
        self.preflight_calls = getattr(self, "preflight_calls", 0) + 1

    def activate_staged_snapshot(self, product_id, model_name, candidate):
        self.activation_calls = getattr(self, "activation_calls", []) + [(product_id, model_name, candidate.catalog_row["publication_id"])]
        if model_name in self.tables:
            self.previous_tables[model_name] = self.tables[model_name]
        self.tables[model_name] = self.tables.pop(f"{model_name}__staging")
        self.catalog[model_name] = dict(candidate.catalog_row)

    def compensate_staged_snapshot(self, product_id, model_name, previous):
        self.compensation_calls = getattr(self, "compensation_calls", []) + [(product_id, model_name)]
        self.restore_replaced_table(model_name)
        if previous.catalog_row is None:
            self.catalog.pop(model_name, None)
        else:
            self.catalog[model_name] = dict(previous.catalog_row)


class ForgetfulCatalogD1(FakeD1):
    """Writes tables but 'forgets' to register _catalog — reproduces the #477 bug."""

    def upsert_catalog(self, catalog_rows) -> None:  # noqa: D401 - intentional no-op
        pass


class ExplodingCatalogD1(FakeD1):
    """Fails after snapshot promotion, before a new catalog value is committed."""

    def upsert_catalog(self, catalog_rows) -> None:
        raise RuntimeError("simulated catalog write failure")


class ExplodingMetaD1(FakeD1):
    """Fails in the handoff-meta step (#638), after the catalog upsert committed."""

    def publish_product_meta(self, *args, **kwargs) -> None:
        raise RuntimeError("simulated product meta write failure")


class ExplodingEvidenceD1(FakeD1):
    """Fails after catalog/meta publication to exercise the #678 rollback boundary."""

    def publish_product_evidence(self, *args, **kwargs) -> None:
        raise RuntimeError("simulated product evidence write failure")


class CorruptingReadBackD1(FakeD1):
    def read_table_rows(self, name, ordered_columns, primary_key):
        rows = super().read_table_rows(name, ordered_columns, primary_key)
        rows[0]["place_id"] = "corrupted"
        return rows


class ExplodingReadBackD1(FakeD1):
    def read_table_rows(self, name, ordered_columns, primary_key):
        super().read_table_rows(name, ordered_columns, primary_key)
        raise RuntimeError("simulated D1 read-back failure")


class ExplodingPrimaryKeyStatsD1(FakeD1):
    def primary_key_stats(self, name: str, primary_key) -> tuple[int, int, int]:
        if name in self.previous_tables:
            raise RuntimeError("simulated activated read-back failure")
        return super().primary_key_stats(name, primary_key)


class ExplodingFinalizeD1(FakeD1):
    def finalize_replaced_table(self, name: str) -> None:
        raise RuntimeError("simulated finalize failure")


class ResponseLostAfterActivationD1(FakeD1):
    def activate_staged_snapshot(self, product_id, model_name, candidate):
        super().activate_staged_snapshot(product_id, model_name, candidate)
        raise RuntimeError("simulated response loss after commit")


class CorruptingSidecarReadbackD1(FakeD1):
    def read_query_availability_rows(self, product_id, publication_id):
        return super().read_query_availability_rows(product_id, publication_id)[:-1]


class FakeSource:
    def __init__(self, plans: dict[str, ReadPlan]) -> None:
        self.plans = plans
        self.seen_last_good_max: dict[str, Any] = {}

    def read(self, contract: ServingContract, last_good_max: Any | None) -> ReadPlan:
        self.seen_last_good_max[contract.model_name] = last_good_max
        return self.plans[contract.model_name]


class FakeSmoke:
    def __init__(
        self,
        status: str = "passed",
        detail: dict[str, Any] | None = None,
    ) -> None:
        self.status = status
        self.detail = detail
        self.checked: list[str] = []

    def check(self, model_name: str) -> str:
        self.checked.append(model_name)
        return self.status

    def diagnostic(self, model_name: str) -> dict[str, Any] | None:
        return self.detail


def _contract(**overrides: Any) -> ServingContract:
    base: dict[str, Any] = dict(
        product_id="weather_place_current_outlook",
        model_name="gold_weather_place_current_outlook",
        enabled=True,
        external=True,
        publication_mode="snapshot",
        zero_policy="fail",
        primary_key=("product_row_id",),
        event_time="forecast_at",
        description="d",
        product_question="q",
    )
    base.update(overrides)
    return ServingContract(**base)


def _projected_contract(**overrides: Any) -> ServingContract:
    return _contract(
        public_projection=("product_row_id", "place_id", "forecast_at"),
        projection_schema_hash="projection-hash-1",
        **overrides,
    )


def _rows(n: int) -> list[dict[str, Any]]:
    return [{"product_row_id": f"r{i}", "place_id": "p", "forecast_at": f"2026-07-2{i}T00:00:00"} for i in range(n)]


def _availability_rows() -> list[dict[str, Any]]:
    return [
        {
            "place_id": f"seoul_admd_{1111000000 + index:010d}",
            "snapshot_as_of_hour": "2026-08-12 00:00:00",
            "available_from_at": "2026-08-12 00:00:00",
            "available_to_at": "2026-08-14 23:00:00",
            "forecast_collected_at_min": "2026-08-11 20:00:00.123456",
            "forecast_collected_at_max": "2026-08-11 20:05:00.9",
            "expected_forecast_hour_count": 72, "observed_forecast_hour_count": 72,
            "availability_status": "complete", "source_population_revision": POPULATION_REVISION,
        }
        for index in range(427)
    ]


def _risk_contract_with_query_availability(**overrides: Any) -> ServingContract:
    return _projected_contract(
        product_id="weather_place_risk_window",
        model_name="gold_weather_place_risk_window",
        query_availability_relation="gold_weather_place_risk_query_availability",
        **overrides,
    )


# ── tests ─────────────────────────────────────────────────────────────────────────

def test_query_availability_fingerprint_is_order_invariant_and_changes_evidence():
    contract = _risk_contract_with_query_availability()
    original = QueryAvailabilityPlan(AVAILABILITY_COLUMNS, _availability_rows())
    changed_rows = _availability_rows()
    changed_rows[0]["source_population_revision"] = "changed"

    assert query_availability_fingerprint(contract, original) == query_availability_fingerprint(
        contract, QueryAvailabilityPlan(AVAILABILITY_COLUMNS, list(reversed(_availability_rows())))
    )
    assert query_availability_fingerprint(contract, original) != query_availability_fingerprint(
        contract, QueryAvailabilityPlan(AVAILABILITY_COLUMNS, changed_rows)
    )


@pytest.mark.parametrize("mutate,reason", [
    (lambda rows: rows[:-1], "expected=427 observed=426"),
    (lambda rows: [{**rows[0], "place_id": None}, *rows[1:]], "place_id"),
    (lambda rows: [{**rows[0], "place_id": rows[1]["place_id"]}, *rows[1:]], "duplicate"),
    (lambda rows: [{**rows[0], "availability_status": "incomplete"}, *rows[1:]], "availability_status"),
    (lambda rows: [{**rows[0], "forecast_collected_at_min": None}, *rows[1:]], "forecast_collected_at_min"),
    (lambda rows: [{**rows[0], "observed_forecast_hour_count": 71}, *rows[1:]], "forecast hour count"),
])
def test_validate_query_availability_fails_closed(mutate, reason):
    error = validate_query_availability(
        _risk_contract_with_query_availability(),
        QueryAvailabilityPlan(AVAILABILITY_COLUMNS, mutate(_availability_rows())),
    )
    assert error is not None and reason in error


@pytest.mark.parametrize("mutate,reason", [
    (lambda rows: [{**rows[0], "place_id": "place_1111051500"}, *rows[1:]], "canonical place_id"),
    (lambda rows: [{**rows[0], "snapshot_as_of_hour": "2026-08-12T00:00:00"}, *rows[1:]], "KST-naive timestamp"),
    (lambda rows: [{**rows[0], "snapshot_as_of_hour": "2026-08-12 00:00:00+09:00"}, *rows[1:]], "KST-naive timestamp"),
    (lambda rows: [{**rows[0], "snapshot_as_of_hour": "2026-02-30 00:00:00"}, *rows[1:]], "real calendar"),
    (lambda rows: [{**rows[0], "snapshot_as_of_hour": datetime(2026, 8, 12, tzinfo=timezone.utc)}, *rows[1:]], "KST-naive timestamp"),
    (lambda rows: [{**rows[0], "available_to_at": "2026-08-14 23:00:00.1"}, *rows[1:]], "exact hourly"),
    (lambda rows: [{**rows[0], "available_from_at": "2026-08-12 01:00:00"}, *rows[1:]], "available_from_at must equal snapshot"),
    (lambda rows: [{**rows[0], "available_to_at": "2026-08-11 23:00:00"}, *rows[1:]], "availability bounds"),
    (lambda rows: [{**rows[0], "forecast_collected_at_min": "2026-08-11 20:00:00+09:00"}, *rows[1:]], "KST-naive timestamp"),
    (lambda rows: [{**rows[0], "forecast_collected_at_min": "2026-08-11 20:00:00.1234567"}, *rows[1:]], "KST-naive timestamp"),
    (lambda rows: [{**rows[0], "forecast_collected_at_min": "2026-08-11 20:06:00"}, *rows[1:]], "collection bounds"),
    (lambda rows: [{**rows[0], "expected_forecast_hour_count": 0, "observed_forecast_hour_count": 0,
                    "available_to_at": "2026-08-12 00:00:00"}, *rows[1:]], "positive"),
    (lambda rows: [{**rows[0], "expected_forecast_hour_count": 71, "observed_forecast_hour_count": 71}, *rows[1:]], "inclusive hourly slot count"),
    (lambda rows: [{**rows[0], "source_population_revision": "kma_admin_dong_grid_20260325:ABC"}, *rows[1:]], "source_population_revision"),
    (lambda rows: [{**rows[0], "snapshot_as_of_hour": "2026-08-12 01:00:00",
                    "available_from_at": "2026-08-12 01:00:00", "available_to_at": "2026-08-15 00:00:00"}, *rows[1:]], "common snapshot"),
    (lambda rows: [{**rows[0], "available_to_at": "2026-08-15 00:00:00",
                    "expected_forecast_hour_count": 73, "observed_forecast_hour_count": 73}, *rows[1:]], "common horizon"),
    (lambda rows: [{**rows[0], "source_population_revision": "kma_admin_dong_grid_20260325:" + "a" * 64}, *rows[1:]], "uniform source_population_revision"),
], ids=[
    "noncanonical_place", "t_separator", "offset_string", "invalid_calendar", "aware_datetime",
    "fractional_hour_bound", "from_not_snapshot", "reversed_availability", "collection_offset",
    "collection_fraction_too_precise", "reversed_collection", "zero_count", "inclusive_count_mismatch", "malformed_revision", "snapshot_mismatch",
    "horizon_count_mismatch", "revision_mismatch",
])
def test_semantically_invalid_availability_never_stages_or_activates(mutate, reason):
    contract = _risk_contract_with_query_availability()
    plan = QueryAvailabilityPlan(AVAILABILITY_COLUMNS, mutate(_availability_rows()))
    assert reason in (validate_query_availability(contract, plan) or "")

    d1 = FakeD1()
    with pytest.raises(PublicationError):
        publish(
            [contract], FakeSource({contract.model_name: ReadPlan(COLUMNS, _rows(1), query_availability=plan)}),
            d1, FakeSmoke("passed"), source_run_id="semantic-invalid", verify_content_parity=True,
        )
    assert d1.stage_snapshot_calls == []
    assert d1.staged_query_availability == []
    assert getattr(d1, "activation_calls", []) == []


def test_query_availability_accepts_naive_datetimes_and_fractional_collection_times():
    rows = _availability_rows()
    for row in rows:
        row.update(
            snapshot_as_of_hour=datetime(2026, 8, 12),
            available_from_at=datetime(2026, 8, 12),
            available_to_at=datetime(2026, 8, 14, 23),
            forecast_collected_at_min=datetime(2026, 8, 11, 20, 0, 0, 123456),
            forecast_collected_at_max="2026-08-11 20:05:00.9",
        )
    assert validate_query_availability(
        _risk_contract_with_query_availability(), QueryAvailabilityPlan(AVAILABILITY_COLUMNS, rows)
    ) is None


def test_query_availability_freshness_interprets_naive_collection_time_as_kst():
    assert validate_query_availability(
        _risk_contract_with_query_availability(freshness_slo_minutes=240),
        QueryAvailabilityPlan(AVAILABILITY_COLUMNS, _availability_rows()),
        checked_at=datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc),
    ) is None


def test_invalid_query_availability_fails_before_d1_stage():
    contract = _risk_contract_with_query_availability()
    d1 = FakeD1()
    plan = QueryAvailabilityPlan(AVAILABILITY_COLUMNS, _availability_rows()[:-1])

    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract], FakeSource({contract.model_name: ReadPlan(COLUMNS, _rows(1), query_availability=plan)}),
            d1, FakeSmoke("passed"), source_run_id="invalid-availability", verify_content_parity=True,
        )

    assert d1.replace_calls == 0
    assert d1.staged_query_availability == []
    assert excinfo.value.report.records[0].stage == "query_availability"


def test_stale_query_availability_fails_before_d1_stage_and_preserves_last_good(monkeypatch):
    contract = _risk_contract_with_query_availability(freshness_slo_minutes=240)
    rows = _availability_rows()
    for row in rows:
        row["forecast_collected_at_min"] = "2026-08-08 00:00:00"
        row["forecast_collected_at_max"] = "2026-08-08 00:05:00"

    d1 = FakeD1()
    last_good_rows = [{"product_row_id": "last-good"}]
    last_good_catalog = {"row_count": 1, "publication_id": "last-good-publication"}
    d1.tables[contract.model_name] = [dict(row) for row in last_good_rows]
    d1.catalog[contract.model_name] = dict(last_good_catalog)
    monkeypatch.setattr(
        "common.serving.publisher._now_iso",
        lambda: "2026-08-12T12:00:00+00:00",
    )

    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract],
            FakeSource({
                contract.model_name: ReadPlan(
                    COLUMNS,
                    _rows(1),
                    query_availability=QueryAvailabilityPlan(AVAILABILITY_COLUMNS, rows),
                )
            }),
            d1,
            FakeSmoke("passed"),
            source_run_id="stale-query-availability",
            verify_content_parity=True,
        )

    record = excinfo.value.report.records[0]
    assert record.stage == "query_availability"
    assert "freshness SLO breached" in record.reason
    assert d1.stage_snapshot_calls == []
    assert d1.staged_query_availability == []
    assert getattr(d1, "activation_calls", []) == []
    assert d1.tables[contract.model_name] == last_good_rows
    assert d1.catalog[contract.model_name] == last_good_catalog
    assert d1.ledger[-1]["outcome"] == "failed"


def test_opted_in_snapshot_stages_then_activates_once_with_same_publication_identity():
    contract = _risk_contract_with_query_availability()
    d1 = FakeD1()
    report = publish(
        [contract], FakeSource({contract.model_name: ReadPlan(COLUMNS, _rows(1), query_availability=QueryAvailabilityPlan(AVAILABILITY_COLUMNS, _availability_rows()))}),
        d1, FakeSmoke("passed"), source_run_id="risk-atomic", verify_content_parity=True,
    )
    record = report.records[0]
    assert d1.activation_calls == [(contract.product_id, contract.model_name, record.publication_id)]
    assert d1.preflight_calls == 1


def test_response_lost_atomic_activation_is_reconciled_without_replaying_transition():
    contract = _risk_contract_with_query_availability()
    d1 = ResponseLostAfterActivationD1()

    report = publish(
        [contract],
        FakeSource({
            contract.model_name: ReadPlan(
                COLUMNS,
                _rows(1),
                query_availability=QueryAvailabilityPlan(
                    AVAILABILITY_COLUMNS,
                    _availability_rows(),
                ),
            )
        }),
        d1,
        FakeSmoke("passed"),
        source_run_id="response-lost-activation",
        verify_content_parity=True,
    )

    record = report.records[0]
    assert report.ok
    assert record.serving_status == STATUS_PUBLISHED
    assert d1.activation_calls == [
        (contract.product_id, contract.model_name, record.publication_id)
    ]
    assert d1.catalog[contract.model_name]["publication_id"] == record.publication_id
    assert contract.model_name not in d1.previous_tables
    assert d1.ledger[-1]["outcome"] == "published"


def test_zero_allow_snapshot_activates_fresh_candidate_before_api_smoke():
    contract = _projected_contract(zero_policy="allow")
    d1 = FakeD1()

    class CandidateSmoke:
        def check(self, model_name):
            assert model_name == contract.model_name
            assert d1.catalog[model_name]["row_count"] == 0
            assert d1.catalog[model_name]["freshness"] == "2026-08-12 16:00:00"
            return "passed"

    report = publish(
        [contract],
        FakeSource({contract.model_name: ReadPlan(
            COLUMNS, [], empty_result_freshness="2026-08-12 16:00:00",
        )}),
        d1,
        CandidateSmoke(),
        source_run_id="zero-allow-freshness-recovery",
        verify_content_parity=True,
    )

    record = report.records[0]
    assert d1.activation_calls == [(contract.product_id, contract.model_name, record.publication_id)]
    assert record.api_smoke_status == "passed"
    assert d1.catalog[contract.model_name]["publication_id"] == record.publication_id


def test_zero_allow_snapshot_smoke_failure_restores_lkg():
    contract = _projected_contract(zero_policy="allow")
    d1 = FakeD1()
    old_rows = _rows(1)
    old_catalog = {"name": contract.model_name, "publication_id": "pub-old", "row_count": 1}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)

    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract],
            FakeSource({contract.model_name: ReadPlan(
                COLUMNS, [], empty_result_freshness="2026-08-12 16:00:00",
            )}),
            d1,
            FakeSmoke("failed"),
            source_run_id="zero-allow-smoke-failure",
            verify_content_parity=True,
        )

    assert d1.compensation_calls == [(contract.product_id, contract.model_name)]
    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert excinfo.value.report.records[0].rollback_status == "restored"


def test_opt_in_primary_key_readback_failure_compensates_once_without_legacy_restore():
    contract = _risk_contract_with_query_availability()
    d1 = ExplodingPrimaryKeyStatsD1()
    old_rows = _rows(1)
    old_catalog = {"name": contract.model_name, "publication_id": "pub-old", "row_count": 1}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)

    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract], FakeSource({contract.model_name: ReadPlan(COLUMNS, _rows(2), query_availability=QueryAvailabilityPlan(AVAILABILITY_COLUMNS, _availability_rows()))}),
            d1, FakeSmoke("passed"), source_run_id="atomic-readback-failure", verify_content_parity=True,
        )

    assert d1.compensation_calls == [(contract.product_id, contract.model_name)]
    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert excinfo.value.report.records[0].rollback_status == "restored"
    assert d1.ledger[-1]["outcome"] == "failed"


def test_opt_in_finalize_failure_keeps_valid_active_publication_but_reports_failure():
    contract = _risk_contract_with_query_availability()
    d1 = ExplodingFinalizeD1()
    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract], FakeSource({contract.model_name: ReadPlan(COLUMNS, _rows(1), query_availability=QueryAvailabilityPlan(AVAILABILITY_COLUMNS, _availability_rows()))}),
            d1, FakeSmoke("passed"), source_run_id="atomic-finalize-failure", verify_content_parity=True,
        )
    record = excinfo.value.report.records[0]
    assert d1.catalog[contract.model_name]["publication_id"] == record.publication_id
    assert record.rollback_status == "not_attempted_active_valid"
    assert d1.ledger[-1]["outcome"] == "failed"


def test_opt_in_sidecar_readback_mismatch_never_activates():
    contract = _risk_contract_with_query_availability()
    d1 = CorruptingSidecarReadbackD1()
    with pytest.raises(PublicationError):
        publish(
            [contract], FakeSource({contract.model_name: ReadPlan(COLUMNS, _rows(1), query_availability=QueryAvailabilityPlan(AVAILABILITY_COLUMNS, _availability_rows()))}),
            d1, FakeSmoke("passed"), source_run_id="sidecar-mismatch", verify_content_parity=True,
        )
    assert not hasattr(d1, "activation_calls")


@pytest.mark.parametrize("mutate", [
    lambda rows: rows[:-1],
    lambda rows: [{**rows[0], "place_id": rows[1]["place_id"]}, *rows[1:]],
    lambda rows: [{**row, "availability_fingerprint": "wrong"} for row in rows],
    lambda rows: [{**row, "publication_id": "wrong-publication"} for row in rows],
], ids=["426_rows", "duplicate_place", "wrong_fingerprint", "wrong_publication"])
def test_every_sidecar_readback_identity_mismatch_blocks_activation(mutate):
    contract = _risk_contract_with_query_availability()
    d1 = FakeD1()
    original = d1.read_query_availability_rows
    d1.read_query_availability_rows = lambda product_id, publication_id: mutate(original(product_id, publication_id))
    with pytest.raises(PublicationError):
        publish(
            [contract], FakeSource({contract.model_name: ReadPlan(COLUMNS, _rows(1), query_availability=QueryAvailabilityPlan(AVAILABILITY_COLUMNS, _availability_rows()))}),
            d1, FakeSmoke("passed"), source_run_id="sidecar-identity-mismatch", verify_content_parity=True,
        )
    assert not hasattr(d1, "activation_calls")


def test_sidecar_row_corruption_with_unchanged_stored_fingerprint_blocks_activation():
    contract = _risk_contract_with_query_availability()
    d1 = FakeD1()
    original = d1.read_query_availability_rows

    def corrupted_rows(product_id, publication_id):
        rows = original(product_id, publication_id)
        rows[0]["available_to_at"] = "2026-08-14 22:00:00"
        return rows

    d1.read_query_availability_rows = corrupted_rows
    with pytest.raises(PublicationError, match="query_availability read-back"):
        publish(
            [contract], FakeSource({contract.model_name: ReadPlan(
                COLUMNS, _rows(1),
                query_availability=QueryAvailabilityPlan(AVAILABILITY_COLUMNS, _availability_rows()),
            )}),
            d1, FakeSmoke("passed"), source_run_id="sidecar-content-corruption",
            verify_content_parity=True,
        )
    assert d1.stage_snapshot_calls == [contract.model_name]
    assert getattr(d1, "activation_calls", []) == []


def test_opt_in_smoke_failure_compensates_exact_lkg_once():
    contract = _risk_contract_with_query_availability()
    d1 = FakeD1()
    old_rows = _rows(1)
    old_catalog = {"name": contract.model_name, "publication_id": "pub-old", "row_count": 1}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)
    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract], FakeSource({contract.model_name: ReadPlan(COLUMNS, _rows(2), query_availability=QueryAvailabilityPlan(AVAILABILITY_COLUMNS, _availability_rows()))}),
            d1, FakeSmoke("failed"), source_run_id="atomic-smoke-failure", verify_content_parity=True,
        )
    assert d1.compensation_calls == [(contract.product_id, contract.model_name)]
    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert excinfo.value.report.records[0].rollback_status == "restored"


def test_opt_in_pattern_verification_queries_staging_never_old_active_table():
    contract = _risk_contract_with_query_availability()
    contract = ServingContract(**{**contract.__dict__, "usage_patterns": ({
        "pattern_id": "candidate_count", "sql": 'SELECT count(*) FROM "gold_weather_place_risk_window"',
    },)})
    d1 = FakeD1()
    d1.tables[contract.model_name] = _rows(1)
    publish(
        [contract], FakeSource({contract.model_name: ReadPlan(COLUMNS, _rows(1), query_availability=QueryAvailabilityPlan(AVAILABILITY_COLUMNS, _availability_rows()))}),
        d1, FakeSmoke("passed"), source_run_id="candidate-pattern", verify_content_parity=True,
    )
    assert any("gold_weather_place_risk_window__staging" in sql for sql in d1.execute_calls)
    assert all('FROM "gold_weather_place_risk_window";' not in sql for sql in d1.execute_calls)

def test_snapshot_publish_success_records_metadata():
    contract = _contract()
    d1 = FakeD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))})
    smoke = FakeSmoke(status="passed")

    report = publish([contract], source, d1, smoke, source_run_id="run-1")

    assert report.ok
    assert d1.table_row_count(contract.model_name) == 3
    rec = report.records[0]
    assert rec.serving_status == STATUS_PUBLISHED
    assert rec.source_run_id == "run-1"
    assert rec.publication_id and rec.published_row_count == 3
    assert rec.d1_row_count == 3
    assert rec.distinct_primary_key_count == 3
    assert rec.null_primary_key_count == 0
    assert rec.api_smoke_status == "passed"
    assert rec.published_bytes > 0 and rec.freshness == "2026-07-22T00:00:00"
    cat = d1.catalog_row(contract.model_name)
    assert cat["product_id"] == "weather_place_current_outlook" and cat["serving_status"] == STATUS_PUBLISHED
    assert smoke.checked == [contract.model_name]  # external => smoke ran


def test_opted_in_snapshot_records_matching_source_and_d1_content_hashes():
    contract = _projected_contract()
    d1 = FakeD1()
    rows = list(reversed(_rows(3)))  # source order must not matter
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=rows)})

    report = publish(
        [contract],
        source,
        d1,
        FakeSmoke(status="passed"),
        source_run_id="content-ok",
        verify_content_parity=True,
    )

    record = report.records[0]
    assert record.serving_status == STATUS_PUBLISHED
    assert record.projection_schema_hash == "projection-hash-1"
    assert record.source_content_hash
    assert record.source_content_hash == record.d1_content_hash
    assert d1.read_table_rows_calls == [(contract.model_name, COLUMNS, ("product_row_id",))]


def test_opted_in_content_mismatch_restores_lkg_before_smoke_catalog_or_meta():
    contract = _projected_contract()
    d1 = CorruptingReadBackD1()
    old_rows = _rows(2)
    old_catalog = {"name": contract.model_name, "row_count": 2, "publication_id": "old"}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)
    smoke = FakeSmoke(status="passed")

    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract],
            FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))}),
            d1,
            smoke,
            source_run_id="content-mismatch",
            verify_content_parity=True,
        )

    record = excinfo.value.report.records[0]
    assert record.stage == "content_parity"
    assert record.rollback_status == "restored"
    assert record.source_content_hash and record.d1_content_hash
    assert record.source_content_hash != record.d1_content_hash
    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert d1.product_meta == {}
    assert smoke.checked == []
    assert d1.ledger[-1]["stage"] == "content_parity"


def test_opted_in_d1_row_read_exception_after_activation_restores_lkg():
    contract = _projected_contract()
    d1 = ExplodingReadBackD1()
    old_rows = _rows(2)
    old_catalog = {"name": contract.model_name, "row_count": 2, "publication_id": "old"}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)

    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract],
            FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))}),
            d1,
            FakeSmoke(status="passed"),
            source_run_id="content-read-failure",
            verify_content_parity=True,
        )

    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert excinfo.value.report.records[0].stage == "content_parity"
    assert any("content parity" in failure for failure in excinfo.value.report.failures)


@pytest.mark.parametrize(
    "contract,error",
    [
        (_contract(projection_schema_hash="projection-hash-1"), "public_projection"),
        (_contract(public_projection=("product_row_id", "place_id", "forecast_at")), "projection_schema_hash"),
        (
            _contract(
                public_projection=("product_row_id", "place_id", "forecast_at"),
                projection_schema_hash="projection-hash-1",
                primary_key=(),
            ),
            "primary_key",
        ),
    ],
)
def test_content_parity_requires_projection_hash_and_primary_key_before_physical_write(contract, error):
    d1 = FakeD1()

    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract],
            FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(1))}),
            d1,
            FakeSmoke(status="passed"),
            source_run_id="missing-projection-hash",
            verify_content_parity=True,
        )

    assert d1.replace_calls == 0
    assert d1.read_table_rows_calls == []
    assert excinfo.value.report.records[0].stage == "content_contract"
    assert error in excinfo.value.report.records[0].reason


def test_default_legacy_publication_does_not_read_full_d1_content_rows():
    contract = _contract()
    d1 = FakeD1()

    report = publish(
        [contract],
        FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(1))}),
        d1,
        FakeSmoke(status="passed"),
        source_run_id="legacy-no-content-read",
    )

    assert report.ok
    assert d1.read_table_rows_calls == []
    assert report.records[0].source_content_hash is None
    assert report.records[0].d1_content_hash is None


def test_snapshot_catalog_carries_static_contract_and_runtime_publication_id():
    contract = _contract(
        public_gold={
            "quality": {"coverage_explanation": "부분 커버리지입니다."},
            "time": {"canonical_timezone": "Asia/Seoul"},
        },
        mcp_projection={
            "operation": {"id": "weather.get_current_outlook"},
            "question_examples": ["가", "나", "다"],
        },
    )
    d1 = FakeD1()
    source = FakeSource(
        {contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(1))}
    )

    publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="run-rich")

    catalog = d1.catalog[contract.model_name]
    assert json.loads(catalog["public_gold"])["time"]["canonical_timezone"] == "Asia/Seoul"
    assert json.loads(catalog["mcp_projection"])["operation"]["id"] == "weather.get_current_outlook"
    assert catalog["publication_id"]


def test_snapshot_publish_writes_product_meta_rows():
    """핸드오프 메타(#638 §2.2) — 컬럼 설명·ext·질의 예시가 계약 선언에서 그대로 게시된다."""
    contract = _contract(
        grain="place_id마다 한 행.",
        column_descriptions={"product_row_id": "행 식별자", "place_id": ""},
        usage_patterns=(
            {
                "pattern_id": "hottest_places_now",
                "question_ko": "지금 가장 더운 장소는?",
                "axes": "장소 랭킹",
                "requires": ["select_columns", "sort"],
                "verified_rows": 10,
                "verified_at": "2026-07-30T09:00:00Z",
                "verified_publication_id": "prev-pub",
                "sql": "SELECT 1",
            },
            {"pattern_id": "sql_missing_dropped"},  # sql 없는 선언은 게시 제외
        ),
    )
    d1 = FakeD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(2))})

    report = publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="run-meta")

    assert report.ok
    meta = d1.product_meta[contract.product_id]
    assert meta["publication_id"] == report.records[0].publication_id
    by_name = {row["column_name"]: row for row in meta["columns"]}
    assert by_name["product_row_id"]["description_ko"] == "행 식별자"
    assert by_name["place_id"]["description_ko"] is None  # 빈 설명은 NULL — 컬럼 행은 게시
    assert by_name["forecast_at"]["type"] == "TEXT"  # timestamp → D1 실물 타입
    assert meta["ext"][0]["grain"] == "place_id마다 한 행."
    assert meta["ext"][0]["primary_key"] == json.dumps(["product_row_id"], ensure_ascii=False)
    assert meta["ext"][0]["time_axis"] == "forecast_at"
    assert meta["ext"][0]["tier"] is None  # 물리 확장 미선언 도메인 — NULL
    patterns = meta["patterns"]
    assert [row["pattern_id"] for row in patterns] == ["hottest_places_now"]
    assert patterns[0]["requires"] == json.dumps(["select_columns", "sort"], ensure_ascii=False)
    assert patterns[0]["verified_publication_id"] == "prev-pub"
    assert patterns[0]["allow_empty"] == 0
    assert patterns[0]["publication_id"] == report.records[0].publication_id


def test_snapshot_publish_writes_declared_column_vocabulary_and_glossary_term():
    contract = _contract(
        column_vocabularies={"sky_code": "weather:sky_code"},
        vocabulary_terms=(
            {
                "vocabulary_id": "weather:sky_code",
                "code": "1",
                "label_ko": "맑음",
                "origin": "traffic_weather",
                "source_type": "dbt_contract",
            },
        ),
    )
    d1 = FakeD1()
    columns = [*COLUMNS, ("sky_code", "varchar")]
    source = FakeSource({contract.model_name: ReadPlan(columns=columns, rows=_rows(1))})

    report = publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="run-vocabulary")

    assert report.ok
    assert d1.product_meta[contract.product_id]["vocabularies"] == [{
        "product_id": contract.product_id,
        "table_name": contract.model_name,
        "column_name": "sky_code",
        "vocabulary_id": "weather:sky_code",
        "publication_id": report.records[0].publication_id,
    }]
    assert [{key: row[key] for key in ("vocabulary_id", "code", "label_ko", "origin", "source_type")}
            for row in d1.glossary_rows] == [{
                "vocabulary_id": "weather:sky_code",
                "code": "1",
                "label_ko": "맑음",
                "origin": "traffic_weather",
                "source_type": "dbt_contract",
            }]


def test_unknown_column_vocabulary_fails_before_snapshot_write():
    contract = _contract(column_vocabularies={"sky_code": "unknown:sky_code"})
    d1 = FakeD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(1))})

    with pytest.raises(PublicationError, match="unknown:sky_code"):
        publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="run-vocabulary-invalid")

    assert d1.replace_calls == 0


def test_weather_traffic_column_vocabulary_mappings_are_the_approved_exact_set():
    weather = _contract(
        column_vocabularies={
            "gu_code": "common:gu_code",
            "sky_code": "weather:sky_code",
            "pty_code": "weather:pty_code",
        },
        vocabulary_terms=(
            {"vocabulary_id": "weather:sky_code", "code": "1", "label_ko": "맑음", "origin": "traffic_weather", "source_type": "dbt_contract"},
            {"vocabulary_id": "weather:pty_code", "code": "0", "label_ko": "없음", "origin": "traffic_weather", "source_type": "dbt_contract"},
        ),
    )
    hotspots = _contract(
        product_id="traffic_flow_congestion_hotspots_hourly",
        model_name="gold_traffic_flow_congestion_hotspots_hourly",
        column_vocabularies={
            "flow_value_quality": "traffic:flow_value_quality",
            "hotspot_state": "traffic:hotspot_state",
        },
        vocabulary_terms=(
            {"vocabulary_id": "traffic:flow_value_quality", "code": "available", "label_ko": "원천 값 있음", "origin": "traffic_weather", "source_type": "dbt_contract"},
            {"vocabulary_id": "traffic:hotspot_state", "code": "observed", "label_ko": "관측됨", "origin": "traffic_weather", "source_type": "dbt_contract"},
        ),
    )
    flow_latest = _contract(
        product_id="traffic_flow_link_latest",
        model_name="gold_traffic_flow_link_latest",
        column_vocabularies={"flow_value_quality": "traffic:flow_value_quality"},
    )
    incident = _contract(
        product_id="traffic_incident_x_weather_current_hourly",
        model_name="gold_traffic_incident_x_weather_current_hourly",
        column_vocabularies={"gu_code": "common:gu_code"},
    )
    contracts = [weather, hotspots, flow_latest, incident]
    columns = [*COLUMNS, ("gu_code", "varchar"), ("sky_code", "varchar"), ("pty_code", "varchar"),
               ("flow_value_quality", "varchar"), ("hotspot_state", "varchar")]
    source = FakeSource({contract.model_name: ReadPlan(columns=columns, rows=_rows(1)) for contract in contracts})
    d1 = FakeD1()

    report = publish(contracts, source, d1, FakeSmoke(status="passed"), source_run_id="run-vocabulary-exact-set")

    assert report.ok
    actual = {
        (row["product_id"], row["column_name"], row["vocabulary_id"])
        for meta in d1.product_meta.values()
        for row in meta["vocabularies"]
    }
    assert actual == {
        ("weather_place_current_outlook", "gu_code", "common:gu_code"),
        ("weather_place_current_outlook", "sky_code", "weather:sky_code"),
        ("weather_place_current_outlook", "pty_code", "weather:pty_code"),
        ("traffic_flow_congestion_hotspots_hourly", "flow_value_quality", "traffic:flow_value_quality"),
        ("traffic_flow_congestion_hotspots_hourly", "hotspot_state", "traffic:hotspot_state"),
        ("traffic_flow_link_latest", "flow_value_quality", "traffic:flow_value_quality"),
        ("traffic_incident_x_weather_current_hourly", "gu_code", "common:gu_code"),
    }
    assert "common:gu_code" not in {row["vocabulary_id"] for row in d1.glossary_rows}


def test_snapshot_publish_writes_source_and_quality_evidence():
    contract = _contract(
        freshness_slo_minutes=240,
        source_evidence=(
            {
                "source_id": "kma_vilage_fcst",
                "source_url": "https://example.test/kma",
                "license": "KOGL-1",
                "license_url": "https://example.test/kogl",
                "redistribution": "allowed_with_attribution",
                "attribution": "기상청",
                "rights_checked_at": "2026-08-04",
            },
        ),
    )
    d1 = FakeD1()

    report = publish(
        [contract],
        FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))}),
        d1,
        FakeSmoke(status="passed"),
        source_run_id="run-evidence",
    )

    evidence = d1.product_evidence[contract.product_id]
    assert evidence["publication_id"] == report.records[0].publication_id
    assert evidence["sources"] == [dict(contract.source_evidence[0])]
    assert evidence["quality"] == {
        "source_row_count": 3,
        "d1_row_count": 3,
        "duplicate_primary_key_count": 0,
        "null_primary_key_count": 0,
        "freshness_as_of": "2026-07-22T00:00:00",
        "freshness_slo_minutes": 240,
        "serving_status": STATUS_PUBLISHED,
        "measured_at": report.records[0].published_at,
        "coverage": None,
        "projection_schema_version": None,
        "projection_schema_hash": None,
    }


def test_snapshot_publish_uses_declared_freshness_field_not_event_time():
    contract = _contract(
        freshness_field="collected_at",
        freshness_slo_minutes=240,
    )
    columns = [*COLUMNS, ("collected_at", "timestamp")]
    rows = [{
        "product_row_id": "r1",
        "place_id": "p1",
        "forecast_at": "2026-08-05T12:00:00",
        "collected_at": "2026-08-04T09:30:00",
    }]
    d1 = FakeD1()

    report = publish(
        [contract],
        FakeSource({contract.model_name: ReadPlan(columns=columns, rows=rows)}),
        d1,
        FakeSmoke(status="passed"),
        source_run_id="run-distinct-freshness-axis",
    )

    assert report.records[0].freshness == "2026-08-04T09:30:00"
    assert d1.catalog[contract.model_name]["time_axis"] == "forecast_at"
    assert d1.catalog[contract.model_name]["freshness"] == "2026-08-04T09:30:00"
    assert d1.product_evidence[contract.product_id]["quality"]["freshness_as_of"] == "2026-08-04T09:30:00"


def test_empty_snapshot_uses_declared_hourly_freshness_fallback():
    contract = _contract(
        zero_policy="allow",
        event_time=None,
        freshness_field=None,
        empty_result_freshness={
            "relation": "gold_weather_place_hourly_outlook",
            "field": "forecast_collected_at_max",
        },
    )
    report = publish(
        [contract],
        FakeSource({
            contract.model_name: ReadPlan(
                columns=COLUMNS,
                rows=[],
                empty_result_freshness="2026-08-10T20:00:00",
            )
        }),
        FakeD1(),
        FakeSmoke(status="passed"),
        source_run_id="empty-precipitation-window",
    )

    assert report.records[0].freshness == "2026-08-10T20:00:00"
    assert report.records[0].serving_status == STATUS_PUBLISHED


def test_empty_snapshot_refuses_null_declared_freshness_fallback():
    contract = _contract(
        zero_policy="allow",
        freshness_field="forecast_collected_at_max",
        empty_result_freshness={
            "relation": "gold_weather_place_hourly_outlook",
            "field": "forecast_collected_at_max",
        },
    )
    d1 = FakeD1()

    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract],
            FakeSource({
                contract.model_name: ReadPlan(columns=COLUMNS, rows=[])
            }),
            d1,
            FakeSmoke(status="passed"),
            source_run_id="empty-without-hourly-freshness",
        )

    assert d1.replace_calls == 0
    assert excinfo.value.report.records[0].stage == "empty_result_freshness"


def test_snapshot_publish_records_passing_distinct_coverage_gate():
    contract = _contract(
        quality_coverage={
            "field": "place_id",
            "expected_distinct_count": 1,
            "minimum_ratio": 1.0,
        }
    )
    d1 = FakeD1()

    publish(
        [contract],
        FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))}),
        d1,
        FakeSmoke(status="passed"),
        source_run_id="coverage-passing",
    )

    assert d1.product_evidence[contract.product_id]["quality"]["coverage"] == {
        "field": "place_id",
        "expected_distinct_count": 1,
        "observed_distinct_count": 1,
        "minimum_ratio": 1.0,
        "ratio": 1.0,
        "status": "passed",
    }


def test_snapshot_publish_uses_source_relation_coverage_observation():
    contract = _contract(
        quality_coverage={
            "field": "dataset",
            "expected_distinct_count": 152,
            "minimum_ratio": 0.95,
            "measurement_scope": "source_relation",
        }
    )
    d1 = FakeD1()

    publish(
        [contract],
        FakeSource({
            contract.model_name: ReadPlan(
                columns=COLUMNS,
                rows=_rows(3),
                coverage_observed_distinct_count=147,
            )
        }),
        d1,
        FakeSmoke(status="passed"),
        source_run_id="coverage-source-relation",
    )

    assert d1.product_evidence[contract.product_id]["quality"]["coverage"] == {
        "field": "dataset",
        "expected_distinct_count": 152,
        "observed_distinct_count": 147,
        "minimum_ratio": 0.95,
        "ratio": 147 / 152,
        "status": "passed",
    }


def test_snapshot_publish_records_explicit_not_applicable_coverage():
    contract = _contract(
        quality_coverage={
            "not_applicable_reason": "eligible source population is dynamic",
        }
    )
    d1 = FakeD1()

    publish(
        [contract],
        FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))}),
        d1,
        FakeSmoke(status="passed"),
        source_run_id="coverage-not-applicable",
    )

    assert d1.product_evidence[contract.product_id]["quality"]["coverage"] == {
        "status": "not_applicable",
        "reason": "eligible source population is dynamic",
    }


def test_snapshot_publish_rejects_coverage_below_contract_threshold_before_write():
    contract = _contract(
        quality_coverage={
            "field": "place_id",
            "expected_distinct_count": 2,
            "minimum_ratio": 1.0,
        }
    )
    d1 = FakeD1()

    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract],
            FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))}),
            d1,
            FakeSmoke(status="passed"),
            source_run_id="coverage-failing",
        )

    assert d1.replace_calls == 0
    record = excinfo.value.report.records[0]
    assert record.stage == "quality_coverage"
    assert "observed=1" in record.reason


def test_skip_retain_does_not_touch_product_meta():
    """스킵 제품은 upsert 자체를 건너뛴다 — 직전 메타·권리 증거가 자연 보존."""
    contract = _contract(zero_policy="retain_last_good")
    d1 = FakeD1()
    d1.tables[contract.model_name] = _rows(5)
    d1.catalog[contract.model_name] = {"name": contract.model_name, "row_count": 5}
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=[])})

    report = publish([contract], source, d1, FakeSmoke(), source_run_id="run-skip-meta")

    assert report.records[0].serving_status == STATUS_SKIPPED
    assert contract.product_id not in d1.product_meta
    assert contract.product_id not in d1.product_evidence


def test_product_meta_failure_restores_snapshot_and_catalog():
    """메타 게시 실패도 catalog 스테이지 롤백 경로를 탄다 — 스냅샷·_catalog 복원, 스테이지 기록."""
    contract = _contract()
    d1 = ExplodingMetaD1()
    old_rows = _rows(2)
    old_catalog = {"name": contract.model_name, "row_count": 2, "publication_id": "old"}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))})

    with pytest.raises(PublicationError) as excinfo:
        publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="meta-failure")

    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert d1.ledger[-1]["outcome"] == "failed"
    assert d1.ledger[-1]["stage"] == "product_meta"
    assert d1.ledger[-1]["rollback_status"] == "restored"
    assert any("product_meta 실패" in failure for failure in excinfo.value.report.failures)


def test_product_evidence_failure_restores_snapshot_and_catalog():
    """권리/품질 게시 실패는 새 snapshot을 live로 남기지 않는다."""
    contract = _contract()
    d1 = ExplodingEvidenceD1()
    old_rows = _rows(2)
    old_catalog = {"name": contract.model_name, "row_count": 2, "publication_id": "old"}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)

    with pytest.raises(PublicationError) as excinfo:
        publish(
            [contract],
            FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))}),
            d1,
            FakeSmoke(status="passed"),
            source_run_id="evidence-failure",
        )

    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert d1.ledger[-1]["stage"] == "product_evidence"
    assert d1.ledger[-1]["rollback_status"] == "restored"
    assert any("product_evidence 실패" in failure for failure in excinfo.value.report.failures)


def test_zero_rows_retain_last_good_keeps_previous():
    contract = _contract(zero_policy="retain_last_good")
    d1 = FakeD1()
    d1.tables[contract.model_name] = _rows(5)
    d1.catalog[contract.model_name] = {"name": contract.model_name, "row_count": 5, "serving_status": STATUS_PUBLISHED}
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=[])})

    report = publish([contract], source, d1, FakeSmoke(), source_run_id="run-2")

    assert report.ok
    assert report.records[0].serving_status == STATUS_SKIPPED
    assert d1.table_row_count(contract.model_name) == 5  # untouched
    assert d1.catalog[contract.model_name]["row_count"] == 5  # not overwritten


def test_zero_rows_fail_policy_raises_and_protects_table():
    contract = _contract(zero_policy="fail")
    d1 = FakeD1()
    d1.tables[contract.model_name] = _rows(4)
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=[])})

    with pytest.raises(PublicationError) as excinfo:
        publish([contract], source, d1, FakeSmoke(), source_run_id="run-3")
    assert d1.table_row_count(contract.model_name) == 4  # last-known-good intact
    assert any("zero_policy=fail" in f for f in excinfo.value.report.failures)


def test_duplicate_snapshot_source_primary_key_fails_before_replacing_last_good():
    contract = _contract()
    d1 = FakeD1()
    d1.tables[contract.model_name] = _rows(1)
    duplicate_rows = [
        {"product_row_id": "same", "place_id": "p", "forecast_at": "2026-07-22T00:00:00"},
        {"product_row_id": "same", "place_id": "p", "forecast_at": "2026-07-22T01:00:00"},
    ]
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=duplicate_rows)})

    with pytest.raises(PublicationError) as excinfo:
        publish([contract], source, d1, FakeSmoke(), source_run_id="duplicate-snapshot")

    assert d1.table_row_count(contract.model_name) == 1
    assert any("source primary key" in failure for failure in excinfo.value.report.failures)


def test_repeated_upsert_replaces_the_existing_primary_key_row():
    contract = _contract(publication_mode="upsert")
    d1 = FakeD1()
    first = ReadPlan(columns=COLUMNS, rows=[{"product_row_id": "same", "place_id": "p", "forecast_at": "old"}])
    second = ReadPlan(columns=COLUMNS, rows=[{"product_row_id": "same", "place_id": "p", "forecast_at": "new"}])

    publish([contract], FakeSource({contract.model_name: first}), d1, FakeSmoke(), source_run_id="upsert-1")
    report = publish([contract], FakeSource({contract.model_name: second}), d1, FakeSmoke(), source_run_id="upsert-2")

    assert d1.table_row_count(contract.model_name) == 1
    assert d1.tables[contract.model_name][0]["forecast_at"] == "new"
    assert report.records[0].d1_row_count == 1
    assert d1.replace_calls == 0


def test_exact_set_upsert_replaces_the_full_source_set_and_schema():
    contract = _contract(publication_mode="upsert", upsert_strategy="exact_set")
    d1 = FakeD1()
    d1.tables[contract.model_name] = [
        {"product_row_id": "stale", "place_id": "old", "forecast_at": "old"},
    ]
    current_columns = COLUMNS + [("new_metric", "integer")]
    current_rows = [
        {"product_row_id": "current", "place_id": "new", "forecast_at": "now", "new_metric": 1},
    ]

    report = publish(
        [contract],
        FakeSource({contract.model_name: ReadPlan(current_columns, current_rows)}),
        d1,
        FakeSmoke(),
        source_run_id="upsert-exact-set",
    )

    assert report.ok
    assert d1.tables[contract.model_name] == current_rows
    assert d1.columns_by_table[contract.model_name] == current_columns
    assert report.records[0].d1_row_count == 1


def test_partial_truncation_retains_last_good():
    contract = _contract(zero_policy="retain_last_good", partial_min_ratio=0.8)
    d1 = FakeD1()
    d1.tables[contract.model_name] = _rows(100)
    d1.catalog[contract.model_name] = {"name": contract.model_name, "row_count": 100}
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(50))})  # 50 < 100*0.8

    report = publish([contract], source, d1, FakeSmoke(), source_run_id="run-4")
    assert report.records[0].serving_status == STATUS_SKIPPED
    assert d1.table_row_count(contract.model_name) == 100


def test_reliability_suppress_row_filters_and_degrades():
    contract = _contract(
        external=False,
        reliability={"sample_count_field": "base_n", "minimum_sample_count": 30, "insufficient_sample_policy": "suppress_row"},
    )
    d1 = FakeD1()
    rows = [
        {"product_row_id": "a", "base_n": 50},
        {"product_row_id": "b", "base_n": 10},  # suppressed
        {"product_row_id": "c", "base_n": 40},
    ]
    source = FakeSource({contract.model_name: ReadPlan(columns=[("product_row_id", "varchar"), ("base_n", "integer")], rows=rows)})

    report = publish([contract], source, d1, FakeSmoke(), source_run_id="run-5")
    assert report.records[0].serving_status == STATUS_DEGRADED
    assert d1.table_row_count(contract.model_name) == 2  # under-sampled row dropped


def test_catalog_self_check_detects_missing_registration():
    contract = _contract()
    d1 = ForgetfulCatalogD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))})

    with pytest.raises(PublicationError) as excinfo:
        publish([contract], source, d1, FakeSmoke(), source_run_id="run-6")
    assert any("자기검증" in f for f in excinfo.value.report.failures)
    assert d1.table_row_count(contract.model_name) == 0  # failed first publication leaves no partial snapshot


def test_snapshot_catalog_registration_failure_restores_last_good_and_records_ledger():
    contract = _contract()
    d1 = ExplodingCatalogD1()
    old_rows = _rows(2)
    old_catalog = {"name": contract.model_name, "row_count": 2, "publication_id": "old"}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))})

    with pytest.raises(PublicationError):
        publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="catalog-failure")

    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert d1.ledger[-1]["outcome"] == "failed"
    assert d1.ledger[-1]["rollback_status"] == "restored"


def test_snapshot_pk_readback_exception_after_activation_restores_last_good():
    contract = _contract()
    d1 = ExplodingPrimaryKeyStatsD1()
    old_rows = _rows(2)
    old_catalog = {"name": contract.model_name, "row_count": 2, "publication_id": "old"}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))})

    with pytest.raises(PublicationError) as excinfo:
        publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="pk-readback-failure")

    record = excinfo.value.report.records[0]
    assert record.stage == "read_back"
    assert record.rollback_status == "restored"
    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert any("primary key read-back" in failure for failure in excinfo.value.report.failures)


def test_exact_set_upsert_catalog_failure_restores_last_good_and_records_ledger():
    contract = _contract(publication_mode="upsert", upsert_strategy="exact_set")
    d1 = ExplodingCatalogD1()
    old_rows = _rows(2)
    old_catalog = {"name": contract.model_name, "row_count": 2, "publication_id": "old"}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))})

    with pytest.raises(PublicationError):
        publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="upsert-catalog-failure")

    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert d1.ledger[-1]["outcome"] == "failed"
    assert d1.ledger[-1]["rollback_status"] == "restored"


def test_smoke_failure_on_external_product_raises():
    contract = _contract()
    d1 = FakeD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(2))})

    with pytest.raises(PublicationError) as excinfo:
        publish([contract], source, d1, FakeSmoke(status="failed"), source_run_id="run-7")
    assert any("smoke" in f for f in excinfo.value.report.failures)


def test_smoke_failure_records_only_allowlisted_diagnostics():
    contract = _contract()
    d1 = FakeD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(2))})
    smoke = FakeSmoke(
        status="failed",
        detail={
            "http_status": 503,
            "error_code": "product_not_ready",
            "blockers": [
                "quality_snapshot_not_current",
                "Bearer secret-must-not-escape",
            ],
            "cf_ray": "safe-ray-123",
            "latency_ms": 1250,
            "raw_body": "secret-must-not-escape",
        },
    )

    with pytest.raises(PublicationError) as excinfo:
        publish([contract], source, d1, smoke, source_run_id="smoke-diagnostic")

    record = excinfo.value.report.records[0]
    assert record.api_smoke_detail == {
        "http_status": 503,
        "latency_ms": 1250,
        "error_code": "product_not_ready",
        "cf_ray": "safe-ray-123",
        "blockers": ["quality_snapshot_not_current"],
    }
    assert "product_not_ready" in record.reason
    assert "quality_snapshot_not_current" in record.reason
    assert "secret-must-not-escape" not in record.reason
    assert "secret-must-not-escape" not in d1.ledger[-1]["reason"]


def test_snapshot_smoke_failure_restores_last_good_catalog_and_records_ledger():
    contract = _contract()
    d1 = FakeD1()
    old_rows = _rows(2)
    old_catalog = {"name": contract.model_name, "row_count": 2, "publication_id": "old"}
    d1.tables[contract.model_name] = [dict(row) for row in old_rows]
    d1.catalog[contract.model_name] = dict(old_catalog)
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(3))})

    with pytest.raises(PublicationError):
        publish([contract], source, d1, FakeSmoke(status="failed"), source_run_id="smoke-failure")

    assert d1.tables[contract.model_name] == old_rows
    assert d1.catalog[contract.model_name] == old_catalog
    assert d1.ledger[-1]["outcome"] == "failed"
    assert d1.ledger[-1]["rollback_status"] == "restored"


def test_not_evaluated_smoke_is_recorded_without_failing_d1_publication():
    contract = _contract()
    d1 = FakeD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(2))})

    report = publish([contract], source, d1, FakeSmoke(status="not_evaluated"), source_run_id="run-no-api")

    assert report.ok
    assert report.records[0].api_smoke_status == "not_evaluated"


def test_append_uses_last_good_max_and_windows():
    contract = _contract(
        model_name="gold_citydata_ppltn_hourly",
        product_id="citydata_ppltn_hourly",
        external=False,
        publication_mode="append",
        zero_policy="retain_last_good",
        event_time="event_at",
        primary_key=("area_cd", "event_at"),
    )
    d1 = FakeD1()
    d1.tables[contract.model_name] = [
        {"area_cd": "a", "event_at": "2026-07-01 00:00:00"},
        {"area_cd": "a", "event_at": "2026-07-02 00:00:00"},
    ]
    d1.catalog[contract.model_name] = {"name": contract.model_name, "row_count": 2}
    plan = ReadPlan(
        columns=[("area_cd", "varchar"), ("event_at", "timestamp")],
        rows=[
            {"area_cd": "a", "event_at": "2026-07-02 00:00:00"},
            {"area_cd": "a", "event_at": "2026-07-03 00:00:00"},
        ],
        delete_column="event_at",
        delete_literal="'2026-07-02 00:00:00'",
    )
    source = FakeSource({contract.model_name: plan})

    report = publish([contract], source, d1, FakeSmoke(), source_run_id="run-8")
    assert report.ok
    assert source.seen_last_good_max[contract.model_name] == "2026-07-02 00:00:00"  # publisher fetched + passed it
    assert d1.table_row_count(contract.model_name) == 3  # 07-01 kept, window re-inserted


def test_load_contracts_filters_enabled_and_product_ids():
    manifest = FIXTURES / "manifest.json"
    all_enabled = load_contracts(manifest)
    assert [c.model_name for c in all_enabled] == [
        "gold_citydata_ppltn_dow_hour",
        "gold_weather_place_current_outlook",
    ]  # sorted, internal disabled dropped

    one = load_contracts(manifest, ["weather_place_current_outlook"])
    assert len(one) == 1
    contract = one[0]
    assert contract.external is True and contract.publication_mode == "snapshot"
    assert contract.primary_key == ("product_row_id",)
    assert contract.tests == ("not_null(product_row_id)",)  # gate label collected from manifest
    # 핸드오프 메타(#638) — manifest 의 컬럼 설명·usage_patterns 가 계약까지 실려 온다
    assert contract.grain == "place_id마다 한 행."
    assert contract.column_descriptions == {
        "product_row_id": "행 식별자(장소).",
        "place_id": "서울 121 장소 코드.",
        "forecast_at": "",
    }
    assert [p["pattern_id"] for p in contract.usage_patterns] == [
        "hottest_places_now",
        "missing_sql_dropped",  # 계약은 선언 그대로 나른다 — sql 필터는 게시 시점(_product_meta_rows)
    ]
    assert contract.usage_patterns[0]["verified_at"] == "2026-07-30T09:00:00Z"
    assert contract.serving_tier is None and contract.rollup_rule is None


# ── incremental upsert (부분 쓰기 — D1 절약) ─────────────────────────────────────────

def _seed_full_table(d1: "FakeD1", model: str, rows: list[dict[str, Any]]) -> None:
    """직전-정상(전체) 테이블을 D1 에 미리 심는다 — incremental 은 이 위에 부분 upsert 한다."""
    d1.tables[model] = [dict(r) for r in rows]
    d1.primary_keys[model] = ("product_row_id",)
    d1.columns_by_table[model] = list(COLUMNS)
    d1.catalog[model] = {"name": model, "row_count": len(rows)}


def test_incremental_upsert_partial_write_passes_and_merges():
    """incremental upsert: 바뀐 그레인(2행)만 써도 전체-테이블 parity 로 실패하지 않고,
    나머지 D1 행은 보존한 채 해당 PK 만 덮인다(부분 INSERT OR REPLACE)."""
    contract = _contract(
        publication_mode="upsert",
        upsert_strategy="incremental",
        zero_policy="retain_last_good",
        event_time="forecast_at",
    )
    model = contract.model_name
    d1 = FakeD1()
    # 직전본 3행(오래된 forecast_at) — 워터마크 = max = 2026-07-22T00:00:00
    _seed_full_table(d1, model, [
        {"product_row_id": "r0", "place_id": "p", "forecast_at": "2026-07-20T00:00:00"},
        {"product_row_id": "r1", "place_id": "p", "forecast_at": "2026-07-21T00:00:00"},
        {"product_row_id": "r2", "place_id": "p", "forecast_at": "2026-07-22T00:00:00"},
    ])
    # 소스는 바뀐 것만: r1 갱신(place_id 변경) + r3 신규 — 둘 다 워터마크보다 최신
    changed = [
        {"product_row_id": "r1", "place_id": "UPDATED", "forecast_at": "2026-07-25T00:00:00"},
        {"product_row_id": "r3", "place_id": "p", "forecast_at": "2026-07-25T00:00:00"},
    ]
    source = FakeSource({model: ReadPlan(columns=COLUMNS, rows=changed)})

    report = publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="inc-1")

    assert report.ok
    rec = report.records[0]
    assert rec.serving_status == STATUS_PUBLISHED
    # 부분 소스(2) ≠ 전체 D1(4) 인데도 통과 — incremental 은 전체-parity 면제
    assert rec.source_row_count == 2
    assert rec.d1_row_count == 4
    assert rec.distinct_primary_key_count == 4 and rec.null_primary_key_count == 0
    # 워터마크(=직전 max event_time)가 reader 로 전달됐다
    assert source.seen_last_good_max[model] == "2026-07-22T00:00:00"
    # 부분 경로 — 전체 교체(replace_table) 는 호출되지 않았다
    assert d1.replace_calls == 0
    # 병합 결과: r0/r2 보존, r1 갱신, r3 추가
    by_id = {r["product_row_id"]: r for r in d1.tables[model]}
    assert set(by_id) == {"r0", "r1", "r2", "r3"}
    assert by_id["r1"]["place_id"] == "UPDATED"
    assert by_id["r0"]["place_id"] == "p"


def test_incremental_upsert_first_run_backfills_full_when_no_watermark():
    """최초 실행(D1 빈 테이블 → 워터마크 없음): reader 가 전량 읽어 백필하고 정상 게시."""
    contract = _contract(
        publication_mode="upsert",
        upsert_strategy="incremental",
        zero_policy="retain_last_good",
        event_time="forecast_at",
    )
    model = contract.model_name
    d1 = FakeD1()
    source = FakeSource({model: ReadPlan(columns=COLUMNS, rows=_rows(3))})

    report = publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="inc-backfill")

    assert report.ok
    assert source.seen_last_good_max[model] is None  # 빈 테이블 → 워터마크 없음
    assert d1.table_row_count(model) == 3
    assert report.records[0].serving_status == STATUS_PUBLISHED


def test_incremental_upsert_rejects_content_parity():
    """incremental upsert 는 verify_content_parity 와 조합 불가(부분 소스 vs 전체 D1)."""
    contract = _contract(
        publication_mode="upsert",
        upsert_strategy="incremental",
        zero_policy="retain_last_good",
        event_time="forecast_at",
    )
    d1 = FakeD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(2))})

    with pytest.raises(PublicationError) as exc:
        publish(
            [contract], source, d1, FakeSmoke(status="passed"),
            source_run_id="inc-parity", verify_content_parity=True,
        )
    assert "verify_content_parity" in str(exc.value)


def test_plain_upsert_still_enforces_full_parity():
    """비-incremental upsert(전량)는 종전대로 전체-테이블 parity 를 강제한다(회귀 방지)."""
    contract = _contract(
        publication_mode="upsert",
        zero_policy="retain_last_good",
        event_time="forecast_at",
    )
    model = contract.model_name
    d1 = FakeD1()
    _seed_full_table(d1, model, [
        {"product_row_id": "r0", "place_id": "p", "forecast_at": "2026-07-20T00:00:00"},
        {"product_row_id": "r1", "place_id": "p", "forecast_at": "2026-07-21T00:00:00"},
    ])
    # 부분 소스(1행)를 전량 upsert 로 쓰면 D1(2행) ≠ source(1) → parity 실패해야 정상
    source = FakeSource({model: ReadPlan(
        columns=COLUMNS, rows=[{"product_row_id": "r0", "place_id": "X", "forecast_at": "2026-07-25T00:00:00"}])})

    with pytest.raises(PublicationError):
        publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="plain-upsert")


# ── v1.11 (Serving#217 P1/P3): 파라미터 메타 게시 + 공유 감사 배선 ─────────────────────

def test_param_meta_published_only_for_declaring_patterns():
    """param_defaults/param_enum/params 를 선언한 패턴만 d1_pattern_params 행이 되고
    JSON 직렬화된다 — 공유 게시기 도메인(비커머스)도 추가 배선 없이 게시된다는 것의 증명."""
    contract = _contract(
        grain="g", usage_patterns=(
            {"pattern_id": "with_meta", "sql": "SELECT 1", "question_ko": "q",
             "param_defaults": {"n": 10, "dir": "desc"},
             "param_enum": {"dir": ["asc", "desc"]}},
            {"pattern_id": "no_meta", "sql": "SELECT 2"},
        ))
    d1 = FakeD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(1))})

    report = publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="run-p")

    assert report.ok
    meta = d1.product_meta[contract.product_id]
    assert {r["pattern_id"] for r in meta["patterns"]} == {"with_meta", "no_meta"}
    assert [r["pattern_id"] for r in meta["params"]] == ["with_meta"]
    row = meta["params"][0]
    assert json.loads(row["param_defaults"]) == {"n": 10, "dir": "desc"}
    assert json.loads(row["param_enum"]) == {"dir": ["asc", "desc"]}
    assert row["params"] is None
    assert row["publication_id"] == meta["publication_id"]


def test_pattern_audit_excludes_internal_table_patterns_but_publishes_product():
    """공유 감사 배선(Serving#217·킷 §F): 내부표를 읽는 패턴은 게시 제외(+메타도 제외)되고
    나머지 패턴·제품 게시는 계속된다 — 커머스 자체 게시기와 같은 정책."""
    contract = _contract(
        grain="g", usage_patterns=(
            {"pattern_id": "evil", "sql": "SELECT k.email FROM gold_weather_place_current_outlook a, _keys k",
             "param_defaults": {"n": 1}},
            {"pattern_id": "evil_paren", "sql": "SELECT * FROM (_keys)"},          # 킷 초판이 놓치던 우회
            {"pattern_id": "evil_cte_write", "sql": "WITH x AS (SELECT 1) DELETE FROM _usage"},
            {"pattern_id": "clean", "sql": "SELECT 1"},
        ))
    d1 = FakeD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(1))})

    report = publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="run-a")

    assert report.ok                          # 제품 게시는 막지 않는다
    meta = d1.product_meta[contract.product_id]
    assert {r["pattern_id"] for r in meta["patterns"]} == {"clean"}
    assert meta["params"] == []               # 탈락 패턴의 메타는 싣지 않는다


def test_sibling_table_pattern_survives_with_warning_only():
    """allowlist 밖(형제·타 제품 표) 참조는 **경보만** — P0-b 강제는 2차(#217 결정 단계).
    서브셋 게시 배치에서 잘 돌던 패턴이 카탈로그에서 사라지는 회귀를 막는 경계다."""
    contract = _contract(
        grain="g", usage_patterns=(
            {"pattern_id": "sibling_join",
             "sql": "SELECT 1 FROM gold_weather_place_current_outlook JOIN gold_other_product ON 1=1"},
        ))
    d1 = FakeD1()
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(1))})

    report = publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="run-s")

    assert report.ok
    assert {r["pattern_id"] for r in d1.product_meta[contract.product_id]["patterns"]} == {"sibling_join"}


# ── export 시점 패턴 검증 스탬프 (Serving#217) ──────────────────────────────────

def test_export_verifies_unverified_draft_and_stamps():
    """공용 게시기가 미검증 초안을 방금 게시한 D1 에 돌려 verified_at 를 스탬프한다 —
    yml 에 verified_at 없는 초안이 게시 후 runnable 로 열리는 경로."""
    contract = _contract(grain="g", usage_patterns=(
        {"pattern_id": "draft", "sql": "-- :as_of='2000-01-01', :n=5\nSELECT a FROM gold_weather_place_current_outlook WHERE event_date = :as_of LIMIT :n",
         "question_ko": "q", "axes": "x", "requires": ["select_columns"]},   # verified_at 없음
        {"pattern_id": "relative", "sql": "-- :as_of='2000-01-01', :n=5\nSELECT a FROM gold_weather_place_current_outlook WHERE event_date = :as_of LIMIT :n",
         "question_ko": "q", "axes": "x", "requires": ["select_columns"],
         "param_defaults": {"as_of": {"rel": "0d", "as": "date"}}},
        {"pattern_id": "verified", "sql": "SELECT 1 FROM gold_weather_place_current_outlook",
         "question_ko": "q", "axes": "x", "requires": ["select_columns"],
         "verified_at": "2026-01-01T00:00:00Z", "verified_rows": 3},
    ))
    d1 = FakeD1()
    d1.execute_result = [{"a": 1}, {"a": 2}]   # 초안 SQL 이 2행 반환 → 검증 통과
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(1))})

    report = publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="run-v")

    assert report.ok
    pats = {p["pattern_id"]: p for p in d1.product_meta[contract.product_id]["patterns"]}
    # 초안이 스탬프돼 게시됨
    assert pats["draft"]["verified_at"] and pats["draft"]["verified_rows"] == 2
    assert pats["draft"]["verified_publication_id"] == d1.product_meta[contract.product_id]["publication_id"]
    # 이미 검증된 건 원래 스탬프 유지(재실행/덮어쓰기 없음)
    assert pats["verified"]["verified_at"] == "2026-01-01T00:00:00Z"
    # 초안 SQL 이 실제로 실행됐다(예시값 치환)
    assert any("LIMIT 5" in c for c in d1.execute_calls)
    # publisher가 d1_pattern_params의 상대 기본값을 export 검증기에도 전달한다.
    assert any("event_date" in c and "2000-01-01" not in c and ":as_of" not in c
               for c in d1.execute_calls)


def test_export_verification_failure_does_not_block_publish():
    """초안 SQL 이 실행 중 깨져도(드리프트) 게시는 계속되고, 그 패턴만 미검증으로 남는다."""
    contract = _contract(grain="g", usage_patterns=(
        {"pattern_id": "broken", "sql": "-- :n=5\nSELECT bad FROM gold_weather_place_current_outlook LIMIT :n",
         "question_ko": "q", "axes": "x", "requires": ["select_columns"]},
    ))
    d1 = FakeD1()
    d1.execute_error = RuntimeError("no such column: bad")
    source = FakeSource({contract.model_name: ReadPlan(columns=COLUMNS, rows=_rows(1))})

    report = publish([contract], source, d1, FakeSmoke(status="passed"), source_run_id="run-b")

    assert report.ok    # 게시는 안 막힌다
    pats = {p["pattern_id"]: p for p in d1.product_meta[contract.product_id]["patterns"]}
    assert pats["broken"].get("verified_at") is None   # 미검증으로 남음(안전망)
