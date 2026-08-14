"""Regression coverage for the common Publisher's D1 catalog boundary."""

from __future__ import annotations

import sqlite3
import sys
from types import SimpleNamespace
from typing import Any

import pytest

from common.serving.d1_client import (
    CATALOG_COLUMNS,
    HANDOFF_COLUMNS,
    MAX_API_BATCH_BYTES,
    MAX_SQL_STATEMENT_BYTES,
    MAX_STATEMENTS_PER_API_BATCH,
    MAX_ACTIVATION_STATEMENTS,
    QUERY_AVAILABILITY_TABLE,
    D1RequestError,
    ProductPublicationState,
    HttpD1Client,
    build_insert_statements,
    group_api_batches,
    handoff_ddl,
    handoff_migrate_statements,
    handoff_schema_is_current,
    handoff_stale_delete_statement,
    handoff_upsert_statements,
)


class FakeHttpResponse:
    def __init__(self, status_code, payload, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        return self._payload


def _success_response(results=None):
    return FakeHttpResponse(
        200,
        {
            "success": True,
            "result": [{"success": True, "results": results or []}],
        },
    )


def test_retry_safe_read_retries_transient_d1_7500_without_logging_sql_or_token(
    monkeypatch, caplog
):
    responses = [
        FakeHttpResponse(
            200,
            {
                "success": False,
                "errors": [{"code": 7500, "message": "internal detail"}],
            },
            {"CF-Ray": "safe-ray-1"},
        ),
        _success_response([{"value": 1}]),
    ]
    calls = []
    sleeps = []

    def post(_url, **kwargs):
        calls.append(kwargs)
        return responses.pop(0)

    monkeypatch.setitem(sys.modules, "requests", SimpleNamespace(post=post))
    client = HttpD1Client(
        api_url="https://example.invalid",
        token="secret-token-value",
        sleep_fn=sleeps.append,
        random_fn=lambda: 0.0,
    )

    assert client._query("SELECT 1 AS value") == [{"value": 1}]
    assert len(calls) == 2
    assert sleeps == [0.5]
    assert "http_status=200" in caplog.text
    assert "codes=7500" in caplog.text
    assert "safe-ray-1" in caplog.text
    assert "SELECT 1" not in caplog.text
    assert "secret-token-value" not in caplog.text
    assert "internal detail" not in caplog.text


def test_retry_safe_read_retries_connection_timeout(monkeypatch):
    class ConnectTimeout(Exception):
        pass

    calls = []
    sleeps = []

    def post(_url, **kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise ConnectTimeout("transport detail must not escape")
        return _success_response([{"value": 1}])

    monkeypatch.setitem(sys.modules, "requests", SimpleNamespace(post=post))
    client = HttpD1Client(
        api_url="https://example.invalid",
        token="test-token",
        sleep_fn=sleeps.append,
        random_fn=lambda: 0.0,
    )

    assert client._query("SELECT 1 AS value") == [{"value": 1}]
    assert len(calls) == 2
    assert sleeps == [0.5]


@pytest.mark.parametrize("status", [429, 500, 503])
def test_non_idempotent_request_does_not_retry_transient_http_status(
    monkeypatch, status
):
    calls = []
    sleeps = []

    def post(_url, **kwargs):
        calls.append(kwargs)
        return FakeHttpResponse(status, {"success": False, "errors": []})

    monkeypatch.setitem(sys.modules, "requests", SimpleNamespace(post=post))
    client = HttpD1Client(
        api_url="https://example.invalid",
        token="test-token",
        sleep_fn=sleeps.append,
        random_fn=lambda: 0.0,
    )

    with pytest.raises(D1RequestError) as excinfo:
        client._request(
            {"sql": "INSERT INTO _publication_ledger VALUES (...)"},
            retry_safe=False,
        )

    assert excinfo.value.http_status == status
    assert excinfo.value.attempt == 1
    assert len(calls) == 1
    assert sleeps == []


def test_retry_safe_request_stops_after_bounded_attempts(monkeypatch):
    calls = []
    sleeps = []

    def post(_url, **kwargs):
        calls.append(kwargs)
        return FakeHttpResponse(503, {"success": False, "errors": []})

    monkeypatch.setitem(sys.modules, "requests", SimpleNamespace(post=post))
    client = HttpD1Client(
        api_url="https://example.invalid",
        token="test-token",
        sleep_fn=sleeps.append,
        random_fn=lambda: 0.0,
    )

    with pytest.raises(D1RequestError) as excinfo:
        client._request({"sql": "SELECT 1"}, retry_safe=True)

    assert excinfo.value.attempt == 3
    assert len(calls) == 3
    assert sleeps == [0.5, 1.0]


def test_ledger_and_atomic_transition_requests_are_explicitly_not_retry_safe(
    monkeypatch
):
    client = HttpD1Client(
        api_url="https://example.invalid",
        token="test-token",
    )
    calls = []

    def request(body, *, retry_safe=False):
        calls.append((body, retry_safe))
        statement_count = len(body.get("batch") or [body])
        return {
            "success": True,
            "result": [
                {"success": True, "results": []}
                for _index in range(statement_count)
            ],
        }

    monkeypatch.setattr(client, "_request", request)
    client.append_publication_ledger({
        "publication_id": "pub-1",
        "product_id": "product-1",
        "model_name": "model-1",
        "source_run_id": "run-1",
        "attempted_at": "2026-08-13T00:00:00+00:00",
        "outcome": "published",
        "stage": "completed",
        "source_row_count": 1,
        "published_row_count": 1,
        "d1_row_count": 1,
        "api_smoke_status": "passed",
        "rollback_status": "not_needed",
        "reason": "ok",
    })
    client._query_atomic_batch([
        'ALTER TABLE "candidate" RENAME TO "active";'
    ])

    assert calls[0][1] is True  # CREATE TABLE IF NOT EXISTS ledger
    assert calls[1][1] is False  # append-only ledger INSERT
    assert calls[2][1] is False  # non-idempotent ALTER transition


def test_staging_batches_use_idempotent_insert_or_replace():
    client = SqliteCatalogClient()
    client.stage_snapshot(
        "gold_weather_place_current_outlook",
        [("product_row_id", "varchar"), ("place_id", "varchar")],
        [{"product_row_id": "row-1", "place_id": "place-1"}],
        ("product_row_id",),
    )

    assert any(
        query.startswith(
            'INSERT OR REPLACE INTO "gold_weather_place_current_outlook__staging"'
        )
        for query in client.queries
    )


def _availability_rows() -> list[dict[str, Any]]:
    return [{
        "place_id": "p000", "snapshot_as_of_hour": "2026-08-12T00:00:00+00:00",
        "available_from_at": "2026-08-12T00:00:00+00:00", "available_to_at": "2026-08-15T00:00:00+00:00",
        "forecast_collected_at_min": "2026-08-11T23:00:00+00:00", "forecast_collected_at_max": "2026-08-12T00:00:00+00:00",
        "expected_forecast_hour_count": 72, "observed_forecast_hour_count": 72,
        "availability_status": "complete", "source_population_revision": "revision",
    }]


def test_query_availability_sidecar_uses_publication_versioned_composite_key():
    d1 = SqliteCatalogClient()
    d1.stage_query_availability("weather_place_risk_window", "pub-old", _availability_rows(), fingerprint="f-old", measured_at="2026-08-12T00:00:00+00:00")
    d1.stage_query_availability("weather_place_risk_window", "pub-new", _availability_rows(), fingerprint="f-new", measured_at="2026-08-12T00:01:00+00:00")

    assert {row["publication_id"] for row in d1.read_query_availability_rows("weather_place_risk_window", "pub-old")} == {"pub-old"}
    assert {row["publication_id"] for row in d1.read_query_availability_rows("weather_place_risk_window", "pub-new")} == {"pub-new"}
    assert [row["name"] for row in d1._query(f'PRAGMA table_info("{QUERY_AVAILABILITY_TABLE}")') if row["pk"]] == ["product_id", "publication_id", "place_id"]


@pytest.mark.parametrize("statements,match", [
    (["SELECT 1;"] * 33, "32"),
    (["SELECT '" + ("x" * 80_001) + "';"], "80000"),
])
def test_atomic_batch_rejects_budgets_before_transport(statements, match):
    d1 = HttpD1Client(api_url="https://example.invalid", token="test-token")
    with pytest.raises(ValueError, match=match):
        d1._query_atomic_batch(statements)


def test_dual_preflight_rejects_compensation_budget_before_any_activation_batch(monkeypatch):
    d1 = HttpD1Client(api_url="https://example.invalid", token="test-token")
    previous = ProductPublicationState(
        catalog_row={**_catalog_row(), "description": "x" * 80_001}, metadata_rows={}, source_rows=(), quality_row=None
    )
    candidate = ProductPublicationState(catalog_row=_catalog_row(), metadata_rows={}, source_rows=(), quality_row=None)
    sent = []
    monkeypatch.setattr(d1, "_query_atomic_batch", lambda statements: sent.append(statements))

    with pytest.raises(ValueError, match="80000"):
        d1.preflight_staged_transition("weather_place_risk_window", "gold_weather_place_risk_window", candidate, previous)
    assert sent == []


def test_atomic_risk_activation_keeps_publication_identity_out_of_active_risk_table():
    d1 = SqliteCatalogClient()
    name = "gold_weather_place_risk_window"
    columns = [("product_row_id", "varchar"), ("place_id", "varchar")]
    d1.prepare_atomic_publication_schema()
    d1.ensure_table(name, columns, ("product_row_id",))
    d1.insert_rows(name, columns, [{"product_row_id": "old", "place_id": "p-old"}], replace=False)
    d1.stage_snapshot(name, columns, [{"product_row_id": "new", "place_id": "p-new"}], ("product_row_id",))
    state = ProductPublicationState(catalog_row=None, metadata_rows={}, source_rows=(), quality_row=None)
    d1.preflight_staged_transition("weather_place_risk_window", name, state, state)
    d1.activate_staged_snapshot("weather_place_risk_window", name, state)
    assert "publication_id" not in {row["name"] for row in d1._query(f'PRAGMA table_info("{name}")')}


def test_existing_query_availability_schema_without_composite_primary_key_fails_closed():
    d1 = SqliteCatalogClient()
    d1._query('CREATE TABLE d1_product_query_availability (product_id TEXT, publication_id TEXT, place_id TEXT);')
    with pytest.raises(RuntimeError, match="schema mismatch"):
        d1.stage_query_availability("weather_place_risk_window", "pub", _availability_rows(), fingerprint="f", measured_at="2026-08-12T00:00:00+00:00")


def test_existing_query_availability_schema_with_matching_names_pk_but_wrong_type_fails_closed():
    d1 = SqliteCatalogClient()
    columns = [
        '"product_id" TEXT NOT NULL', '"publication_id" TEXT NOT NULL', '"place_id" TEXT NOT NULL',
        '"snapshot_as_of_hour" TEXT NOT NULL', '"available_from_at" TEXT NOT NULL', '"available_to_at" TEXT NOT NULL',
        '"forecast_collected_at_min" TEXT NOT NULL', '"forecast_collected_at_max" TEXT NOT NULL',
        '"expected_forecast_hour_count" TEXT NOT NULL',  # must be INTEGER
        '"observed_forecast_hour_count" INTEGER NOT NULL', '"availability_status" TEXT NOT NULL',
        '"source_population_revision" TEXT NOT NULL', '"availability_fingerprint" TEXT NOT NULL', '"measured_at" TEXT NOT NULL',
    ]
    d1._query('CREATE TABLE d1_product_query_availability (' + ', '.join(columns) + ', PRIMARY KEY (product_id, publication_id, place_id));')
    with pytest.raises(RuntimeError, match="schema mismatch"):
        d1.stage_query_availability("weather_place_risk_window", "pub", _availability_rows(), fingerprint="f", measured_at="2026-08-12T00:00:00+00:00")


def test_lkg_capture_propagates_metadata_read_failure_after_schema_is_prepared(monkeypatch):
    d1 = SqliteCatalogClient()
    d1.prepare_atomic_publication_schema()
    original = d1._query
    monkeypatch.setattr(d1, "_query", lambda sql: (_ for _ in ()).throw(RuntimeError("read failed")) if 'd1_catalog_columns' in sql else original(sql))
    with pytest.raises(RuntimeError, match="read failed"):
        d1.capture_product_publication_state("weather_place_risk_window", "gold_weather_place_risk_window")


class LegacyCatalogClient(HttpD1Client):
    """D1 HTTP seam with the pre-v1.1 eight-column catalog schema."""

    def __init__(self) -> None:
        super().__init__(api_url="https://example.invalid", token="test-token")
        self.columns = [
            "name", "description", "serving_tier", "tests",
            "time_axis", "columns", "row_count", "exported_at",
        ]
        self.queries: list[str] = []

    def _query(
        self, sql: str, *, retry_safe: bool | None = None
    ) -> list[dict[str, Any]]:
        self.queries.append(sql)
        if sql == "PRAGMA table_info(_catalog);":
            return [{"name": name} for name in self.columns]
        if sql.startswith("ALTER TABLE _catalog ADD COLUMN "):
            self.columns.append(sql.split('"')[1])
        return []

    def _query_batch(
        self, statements: list[str], *, retry_safe: bool = False
    ) -> list[list[dict[str, Any]]]:
        return [self._query(statement) for statement in statements]


class SqliteCatalogClient(HttpD1Client):
    """Real SQLite seam for preserving fields outside the v1.1 catalog model."""

    def __init__(self) -> None:
        super().__init__(api_url="https://example.invalid", token="test-token")
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        self.queries: list[str] = []

    def _query(
        self, sql: str, *, retry_safe: bool | None = None
    ) -> list[dict[str, Any]]:
        self.queries.append(sql)
        if ";" in sql.rstrip(";"):
            self.connection.executescript(sql)
            self.connection.commit()
            return []
        cursor = self.connection.execute(sql)
        self.connection.commit()
        return [dict(row) for row in cursor.fetchall()] if cursor.description else []

    def _query_batch(
        self, statements: list[str], *, retry_safe: bool = False
    ) -> list[list[dict[str, Any]]]:
        return [self._query(statement) for statement in statements]


class AtomicSqliteD1Client(SqliteCatalogClient):
    def _query_atomic_batch(self, statements):
        statements = self._validate_atomic_batch(statements)
        self.connection.execute("SAVEPOINT atomic")
        try:
            for statement in statements:
                self.connection.execute(statement)
        except Exception:
            self.connection.execute("ROLLBACK TO atomic")
            self.connection.execute("RELEASE atomic")
            raise
        self.connection.execute("RELEASE atomic")
        self.connection.commit()
        return []


def test_atomic_sqlite_batch_rolls_back_mid_statement_failure():
    d1 = AtomicSqliteD1Client()
    d1._query('CREATE TABLE t (id TEXT PRIMARY KEY, value TEXT);')
    d1._query("INSERT INTO t VALUES ('old', 'old');")
    with pytest.raises(sqlite3.IntegrityError):
        d1._query_atomic_batch([
            "UPDATE t SET value = 'new' WHERE id = 'old';",
            "INSERT INTO t VALUES ('old', 'duplicate');",
        ])
    assert d1._query("SELECT value FROM t WHERE id = 'old';") == [{"value": "old"}]


def _catalog_row() -> dict[str, Any]:
    return {
        "name": "gold_weather_place_current_outlook",
        "product_id": "weather_place_current_outlook",
        "external": True,
        "description": "현재 장소 예보",
        "product_question": "지금 이 장소의 예보는?",
        "tests": "not_null(product_row_id)",
        "time_axis": "forecast_at",
        "columns": "[]",
        "row_count": 427,
        "serving_status": "published",
        "publication_id": "publication-1",
        "source_run_id": "run-1",
        "published_bytes": 1024,
        "freshness": "2026-07-28T10:00:00",
        "exported_at": "2026-07-28T10:01:00",
    }


def test_catalog_upsert_migrates_legacy_catalog_and_names_v11_columns():
    """Catch a positional insert that cannot write the v1.1 15-field catalog row."""
    d1 = LegacyCatalogClient()

    d1.upsert_catalog([_catalog_row()])

    assert set(CATALOG_COLUMNS).issubset(d1.columns)
    insert = next(query for query in d1.queries if query.startswith("INSERT INTO _catalog"))
    assert '("name", "product_id", "external", "description", "product_question"' in insert
    assert 'ON CONFLICT("name") DO UPDATE SET' in insert
    assert '"serving_tier"' not in insert


def test_catalog_upsert_does_not_alter_an_already_migrated_schema():
    """Catch a retry that attempts to add an existing v1.1 catalog column again."""
    d1 = LegacyCatalogClient()

    d1.upsert_catalog([_catalog_row()])
    alter_count = sum(query.startswith("ALTER TABLE _catalog ADD COLUMN") for query in d1.queries)
    assert alter_count == 10
    d1.upsert_catalog([_catalog_row()])

    assert sum(query.startswith("ALTER TABLE _catalog ADD COLUMN") for query in d1.queries) == alter_count


def test_catalog_upsert_preserves_legacy_serving_tier_on_existing_row():
    d1 = SqliteCatalogClient()
    d1._query(
        "CREATE TABLE _catalog (name TEXT PRIMARY KEY, description TEXT, serving_tier TEXT, "
        "tests TEXT, time_axis TEXT, columns TEXT, row_count INTEGER, exported_at TEXT);"
    )
    d1._query(
        "INSERT INTO _catalog (name, description, serving_tier) "
        "VALUES ('gold_weather_place_current_outlook', 'legacy description', 'public');"
    )

    d1.upsert_catalog([_catalog_row()])

    row = d1._query(
        "SELECT product_id, description, serving_tier FROM _catalog "
        "WHERE name = 'gold_weather_place_current_outlook';"
    )[0]
    assert row == {
        "product_id": "weather_place_current_outlook",
        "description": _catalog_row()["description"],
        "serving_tier": "public",
    }


def test_catalog_retirement_deletes_only_exact_product_ids_and_keeps_physical_tables():
    d1 = SqliteCatalogClient()
    current = _catalog_row()
    grid_current = {
        **current,
        "name": "gold_weather_grid_current_outlook",
        "product_id": "weather_grid_current_outlook",
    }
    grid_precipitation = {
        **current,
        "name": "gold_weather_grid_precipitation_window",
        "product_id": "weather_grid_precipitation_window",
    }
    d1.upsert_catalog([current, grid_current, grid_precipitation])
    for name in (grid_current["name"], grid_precipitation["name"]):
        d1.ensure_table(name, [("product_row_id", "varchar")], ("product_row_id",))

    d1.delete_catalog_product_ids(
        [
            "weather_grid_precipitation_window",
            "weather_grid_current_outlook",
            "weather_grid_current_outlook",
        ]
    )

    assert d1._query("SELECT product_id FROM _catalog ORDER BY product_id;") == [
        {"product_id": "weather_place_current_outlook"}
    ]
    assert d1._query(
        "SELECT name FROM sqlite_master WHERE type = 'table' "
        "AND name LIKE 'gold_weather_grid_%' ORDER BY name;"
    ) == [
        {"name": "gold_weather_grid_current_outlook"},
        {"name": "gold_weather_grid_precipitation_window"},
    ]
    assert not any("DROP TABLE" in query for query in d1.queries)


@pytest.mark.parametrize(
    "product_ids",
    [
        ("weather_grid_current_outlook", {"not": "a product id"}),
        ("weather_grid_current_outlook", "unsafe product id"),
    ],
)
def test_catalog_retirement_rejects_invalid_product_ids_before_deduplication(product_ids):
    """Malformed input must fail as a validation error, not while sorting a set."""

    d1 = SqliteCatalogClient()

    with pytest.raises(ValueError, match="unsafe D1 product_id"):
        d1.delete_catalog_product_ids(product_ids)

    assert not d1.queries


def test_serving_table_enforces_contract_primary_key_and_reports_readback_counts():
    d1 = SqliteCatalogClient()
    table = "gold_weather_place_current_outlook"
    columns = [("product_row_id", "varchar"), ("forecast_at", "timestamp")]

    assert hasattr(d1, "primary_key_stats")
    d1.ensure_table(table, columns, ("product_row_id",))
    d1.insert_rows(table, columns, [{"product_row_id": "row-1", "forecast_at": "old"}], replace=False)
    d1.insert_rows(table, columns, [{"product_row_id": "row-1", "forecast_at": "new"}], replace=True)

    assert any('CREATE UNIQUE INDEX IF NOT EXISTS "gold_weather_place_current_outlook__pk_uq"' in query for query in d1.queries)
    assert d1.primary_key_stats(table, ("product_row_id",)) == (1, 1, 0)


def test_read_table_rows_selects_exact_ordered_columns_and_orders_by_primary_key():
    d1 = SqliteCatalogClient()
    table = "gold_weather_place_current_outlook"
    columns = [("product_row_id", "varchar"), ("place_id", "varchar"), ("forecast_at", "timestamp")]
    d1.replace_table(
        table,
        columns,
        [
            {"product_row_id": "b", "place_id": "p2", "forecast_at": "2026-07-22T00:00:00"},
            {"product_row_id": "a", "place_id": "p1", "forecast_at": "2026-07-21T00:00:00"},
        ],
        ("product_row_id",),
    )

    rows = d1.read_table_rows(
        table,
        [("place_id", "varchar"), ("product_row_id", "varchar")],
        ("product_row_id",),
    )

    assert rows == [
        {"place_id": "p1", "product_row_id": "a"},
        {"place_id": "p2", "product_row_id": "b"},
    ]
    assert d1.queries[-1] == (
        'SELECT "place_id", "product_row_id" FROM "gold_weather_place_current_outlook" '
        'ORDER BY "product_row_id";'
    )


def test_read_table_rows_rejects_unsafe_identifiers_before_sql():
    d1 = SqliteCatalogClient()

    with pytest.raises(ValueError, match="unsafe"):
        d1.read_table_rows("gold_weather;drop", [("product_row_id", "varchar")], ("product_row_id",))
    with pytest.raises(ValueError, match="unsafe"):
        d1.read_table_rows("gold_weather", [("product_row_id as id", "varchar")], ("product_row_id",))
    with pytest.raises(ValueError, match="unsafe"):
        d1.read_table_rows("gold_weather", [("product_row_id", "varchar")], ("product_row_id desc",))

    assert not any("gold_weather;drop" in query for query in d1.queries)


def test_repeated_snapshot_swaps_keep_a_physical_primary_key_constraint():
    d1 = SqliteCatalogClient()
    table = "gold_weather_place_current_outlook"
    columns = [("product_row_id", "varchar"), ("forecast_at", "timestamp")]

    d1.replace_table(table, columns, [{"product_row_id": "row-1", "forecast_at": "first"}], ("product_row_id",))
    d1.replace_table(table, columns, [{"product_row_id": "row-1", "forecast_at": "second"}], ("product_row_id",))

    assert any(row["unique"] for row in d1._query(f'PRAGMA index_list("{table}");'))
    with pytest.raises(sqlite3.IntegrityError):
        d1._query(
            'INSERT INTO "gold_weather_place_current_outlook" ("product_row_id", "forecast_at") '
            "VALUES ('row-1', 'duplicate');"
        )


def test_snapshot_replacement_requires_a_contract_primary_key():
    d1 = SqliteCatalogClient()

    with pytest.raises(ValueError, match="primary_key is required"):
        d1.replace_table(
            "gold_weather_place_current_outlook",
            [("product_row_id", "varchar")],
            [{"product_row_id": "row-1"}],
            (),
        )


def test_insert_statements_pack_rows_by_rendered_utf8_sql_bytes():
    rows = [{"product_row_id": f"row-{index}", "label": "용신동" * 300} for index in range(120)]

    statements = build_insert_statements(
        "gold_weather_place_risk_window",
        [("product_row_id", "varchar"), ("label", "varchar")],
        rows,
        replace=False,
    )

    assert len(statements) > 1
    assert all(len(statement.encode("utf-8")) <= MAX_SQL_STATEMENT_BYTES for statement in statements)
    assert sum(statement.count("('row-") for statement in statements) == len(rows)


def test_insert_statements_reject_one_row_that_exceeds_sql_budget_before_http():
    with pytest.raises(ValueError, match=str(MAX_SQL_STATEMENT_BYTES)):
        build_insert_statements(
            "gold_weather_place_risk_window",
            [("product_row_id", "varchar"), ("label", "varchar")],
            [{"product_row_id": "oversized", "label": "용" * 30_000}],
            replace=False,
        )


def test_group_api_batches_limits_statement_count_and_total_utf8_body_bytes():
    statement = "INSERT INTO risk (label) VALUES ('" + ("a" * 60_000) + "');"

    batches = group_api_batches([statement] * 9)

    assert len(batches) == 3
    assert all(len(batch) <= MAX_STATEMENTS_PER_API_BATCH for batch in batches)
    assert all(
        sum(len(sql.encode("utf-8")) for sql in batch) <= MAX_API_BATCH_BYTES
        for batch in batches
    )


def test_query_batch_sends_one_cloudflare_batch_request(monkeypatch):
    d1 = HttpD1Client(api_url="https://example.invalid", token="test-token")
    sent: list[dict[str, Any]] = []

    def fake_request(
        body: dict[str, Any], *, retry_safe: bool = False
    ) -> dict[str, Any]:
        sent.append(body)
        return {
            "success": True,
            "result": [
                {"success": True, "results": [{"id": 1}]},
                {"success": True, "results": [{"id": 2}]},
            ],
        }

    monkeypatch.setattr(d1, "_request", fake_request, raising=False)

    assert d1._query_batch(["SELECT 1;", "SELECT 2;"]) == [[{"id": 1}], [{"id": 2}]]
    assert sent == [{"batch": [{"sql": "SELECT 1;"}, {"sql": "SELECT 2;"}]}]


def test_query_rejects_a_failed_statement_in_a_multi_statement_response(monkeypatch):
    d1 = HttpD1Client(api_url="https://example.invalid", token="test-token")
    monkeypatch.setattr(
        d1,
        "_request",
        lambda body, *, retry_safe=False: {
            "success": True,
            "result": [
                {"success": True, "results": []},
                {"success": False, "errors": [{"message": "rename failed"}], "results": []},
            ],
        },
    )

    with pytest.raises(RuntimeError, match="D1 API"):
        d1._query('DROP TABLE IF EXISTS "gold_traffic"; ALTER TABLE "gold_traffic__staging" RENAME TO "gold_traffic";')


def test_snapshot_restore_reactivates_previous_table_after_post_promotion_failure():
    d1 = SqliteCatalogClient()
    table = "gold_weather_place_current_outlook"
    columns = [("product_row_id", "varchar"), ("forecast_at", "timestamp")]

    d1.replace_table(table, columns, [{"product_row_id": "old", "forecast_at": "old"}], ("product_row_id",))
    d1.replace_table(table, columns, [{"product_row_id": "new", "forecast_at": "new"}], ("product_row_id",))
    d1.restore_replaced_table(table)

    assert d1._query(f'SELECT product_row_id FROM "{table}";') == [{"product_row_id": "old"}]
    assert d1._query(f"SELECT name FROM sqlite_master WHERE name = '{table}__previous';") == []


def _product_meta_payload(publication_id: str) -> tuple[list, list, list]:
    columns_rows = [{
        "product_id": "commerce_x", "table_name": "d1_x", "ordinal": 0,
        "column_name": "gu", "type": "TEXT", "description_ko": "자치구명",
        "publication_id": publication_id,
    }]
    ext_rows = [{
        "product_id": "commerce_x", "table_name": "d1_x", "source_model": "gold_x",
        "grain": "자치구", "primary_key": '["gu"]', "time_axis": None,
        "tier": "d1_direct", "rollup_rule": None, "publication_id": publication_id,
    }]
    pattern_rows = [{
        "product_id": "commerce_x", "pattern_id": "top_gu", "question_ko": "어느 구가 많나?",
        "sql": "SELECT gu FROM d1_x ORDER BY cnt DESC LIMIT :n", "axes": "구 랭킹",
        "requires": '["sort"]', "verified_rows": 10, "verified_at": None,
        "verified_publication_id": None, "allow_empty": 0, "insight_sample_ko": None,
        "publication_id": publication_id,
    }]
    return columns_rows, ext_rows, pattern_rows


def _vocabulary_rows(publication_id: str, column_name: str = "sky_code") -> list[dict[str, str]]:
    return [{
        "product_id": "weather_place_current_outlook",
        "table_name": "gold_weather_place_current_outlook",
        "column_name": column_name,
        "vocabulary_id": "weather:sky_code",
        "publication_id": publication_id,
    }]


def test_column_vocabulary_sidecar_upserts_and_prunes_per_product():
    d1 = SqliteCatalogClient()
    columns_rows, ext_rows, pattern_rows = _product_meta_payload("pub-1")

    d1.publish_product_meta(
        "weather_place_current_outlook",
        "pub-1",
        columns_rows,
        ext_rows,
        pattern_rows,
        vocabulary_rows=_vocabulary_rows("pub-1"),
    )

    pragma = d1._query('PRAGMA table_info("d1_catalog_column_vocabularies");')
    assert handoff_schema_is_current("d1_catalog_column_vocabularies", pragma)
    assert [row["name"] for row in pragma if row["pk"]] == ["product_id", "column_name"]
    assert d1._query(
        "SELECT product_id, column_name, vocabulary_id, publication_id "
        "FROM d1_catalog_column_vocabularies;"
    ) == [{
        "product_id": "weather_place_current_outlook",
        "column_name": "sky_code",
        "vocabulary_id": "weather:sky_code",
        "publication_id": "pub-1",
    }]

    d1.publish_product_meta(
        "weather_place_current_outlook",
        "pub-2",
        columns_rows,
        ext_rows,
        pattern_rows,
        vocabulary_rows=_vocabulary_rows("pub-2", "pty_code"),
    )
    d1.publish_product_meta(
        "weather_place_current_outlook",
        "pub-2",
        columns_rows,
        ext_rows,
        pattern_rows,
        vocabulary_rows=_vocabulary_rows("pub-2", "pty_code"),
    )

    assert d1._query(
        "SELECT column_name, publication_id FROM d1_catalog_column_vocabularies "
        "WHERE product_id = 'weather_place_current_outlook';"
    ) == [{"column_name": "pty_code", "publication_id": "pub-2"}]


def test_publish_glossary_rejects_unknown_vocabulary_without_writing():
    d1 = SqliteCatalogClient()
    row = {
        "vocabulary_id": "unknown:code",
        "code": "x",
        "label_ko": "알 수 없음",
        "origin": "traffic_weather",
        "source_type": "dbt_contract",
        "exported_at": "t1",
    }

    with pytest.raises(ValueError, match="unknown:code"):
        d1.publish_glossary([row])

    assert d1._query(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'd1_catalog_glossary';"
    ) == []


def test_publish_glossary_replaces_only_the_declared_vocabulary_terms():
    d1 = SqliteCatalogClient()
    initial = [
        {
            "vocabulary_id": "weather:sky_code",
            "code": "1",
            "label_ko": "맑음",
            "origin": "traffic_weather",
            "source_type": "dbt_contract",
            "exported_at": "t1",
        },
        {
            "vocabulary_id": "weather:sky_code",
            "code": "3",
            "label_ko": "구름 많음",
            "origin": "traffic_weather",
            "source_type": "dbt_contract",
            "exported_at": "t1",
        },
    ]

    d1.publish_glossary(initial)
    d1.publish_glossary([dict(initial[0], label_ko="맑음(갱신)", exported_at="t2")])

    assert d1._query(
        "SELECT code, label_ko FROM d1_catalog_glossary "
        "WHERE vocabulary_id = 'weather:sky_code' ORDER BY code;"
    ) == [{"code": "1", "label_ko": "맑음(갱신)"}]


def test_product_meta_migrates_legacy_table_preserving_skipped_product_rows():
    """레거시(자연키 없음) → v1 은 **행 보존 이행** 1회(#638 §4) — 이행 run 에 게시되지 않는
    제품(밴드 스킵)의 직전 메타가 살아남아야 한다(#593 보존 시맨틱 승계)."""
    d1 = SqliteCatalogClient()
    d1._query(  # 구 commerce 전량 교체 시절 컬럼 순서 그대로
        "CREATE TABLE d1_catalog_ext (product_id TEXT, table_name TEXT, source_model TEXT, "
        "tier TEXT, grain TEXT, primary_key TEXT, rollup_rule TEXT, time_axis TEXT, "
        "publication_id TEXT);"
    )
    d1._query(
        "INSERT INTO d1_catalog_ext VALUES ('commerce_skipped', 'd1_skipped', 'gold_skipped', "
        "'d1_direct', '자치구', '[\"gu\"]', NULL, NULL, 'old-pub');"
    )

    columns_rows, ext_rows, pattern_rows = _product_meta_payload("pub-1")
    d1.publish_product_meta("commerce_x", "pub-1", columns_rows, ext_rows, pattern_rows)

    pragma = d1._query('PRAGMA table_info("d1_catalog_ext");')
    assert handoff_schema_is_current("d1_catalog_ext", pragma)
    assert [row["name"] for row in pragma if row["pk"]] == ["product_id"]  # 자연키 강제
    rows = d1._query("SELECT product_id, grain, publication_id FROM d1_catalog_ext ORDER BY product_id;")
    assert rows == [
        {"product_id": "commerce_skipped", "grain": "자치구", "publication_id": "old-pub"},  # 보존
        {"product_id": "commerce_x", "grain": "자치구", "publication_id": "pub-1"},
    ]

    migrate_count = sum('RENAME TO "d1_catalog_ext__migrate"' in q for q in d1.queries)
    assert migrate_count == 1
    d1.publish_product_meta("commerce_x", "pub-1", columns_rows, ext_rows, pattern_rows)
    assert sum('RENAME TO "d1_catalog_ext__migrate"' in q for q in d1.queries) == migrate_count  # 1회뿐
    assert d1._query("SELECT count(*) c FROM d1_catalog_ext;")[0]["c"] == 2  # upsert 멱등


def test_glossary_legacy_field_schema_migrates_to_namespaced_vocabulary():
    """glossary 레거시(field/source) → v1: 'commerce:' 네임스페이스 변환 + origin/source_type 채움."""
    d1 = SqliteCatalogClient()
    d1._query(
        "CREATE TABLE d1_catalog_glossary (field TEXT, code TEXT, label_ko TEXT, "
        "source TEXT, exported_at TEXT);"
    )
    d1._query(
        "INSERT INTO d1_catalog_glossary VALUES "
        "('major', 'health', '보건', 'gold_license_cohort_survival.major_ko', 't0');"
    )

    pragma = d1._query('PRAGMA table_info("d1_catalog_glossary");')
    d1._query(handoff_migrate_statements("d1_catalog_glossary", [r["name"] for r in pragma]))

    assert handoff_schema_is_current(
        "d1_catalog_glossary", d1._query('PRAGMA table_info("d1_catalog_glossary");'))
    rows = d1._query("SELECT * FROM d1_catalog_glossary;")
    assert rows == [{
        "vocabulary_id": "commerce:major", "code": "health", "label_ko": "보건",
        "origin": "commerce", "source_type": "warehouse", "exported_at": "t0",
    }]


def test_pattern_removed_from_declaration_pruned_even_when_publication_id_reused():
    """무변경 게이트(#601)가 publication_id 를 재사용해도 yml 에서 지운 패턴은 정리돼야 한다.

    publication_id 부등 판별이었다면 no-op 이 되는 정확히 그 경우 — 키셋(NOT IN) 정리의 근거.
    """
    d1 = SqliteCatalogClient()
    columns_rows, ext_rows, pattern_rows = _product_meta_payload("pub-1")
    removed = dict(pattern_rows[0], pattern_id="removed_later")
    d1.publish_product_meta("commerce_x", "pub-1", columns_rows, ext_rows, pattern_rows + [removed])

    # 데이터 무변경 → 같은 publication_id 로 재게시, 선언에서 removed_later 만 사라진 상황
    d1.publish_product_meta("commerce_x", "pub-1", columns_rows, ext_rows, pattern_rows)

    rows = d1._query("SELECT pattern_id FROM d1_usage_patterns WHERE product_id = 'commerce_x';")
    assert rows == [{"pattern_id": "top_gu"}]


def test_publisher_meta_rows_round_trip_through_real_sqlite_schema():
    """행 빌더 ↔ 공용 스키마 키 정합 — 빌더 출력이 실제 스키마에 그대로 실리는지(NULL 드리프트 방지)."""
    from common.serving.contract import ServingContract
    from common.serving.publisher import ProductRecord, _product_meta_rows

    contract = ServingContract(
        product_id="weather_place_current_outlook",
        model_name="gold_weather_place_current_outlook",
        enabled=True, external=True, publication_mode="snapshot", zero_policy="fail",
        primary_key=("product_row_id",), grain="place_id마다 한 행.",
        column_descriptions={"product_row_id": "행 식별자"},
        usage_patterns=({"pattern_id": "p1", "sql": "SELECT 1", "question_ko": "질문",
                         "axes": "축", "requires": ["sort"]},),
    )
    record = ProductRecord(
        product_id=contract.product_id, model_name=contract.model_name,
        publication_id="pub-9", source_run_id="r", published_at="t",
        serving_status="published", reason="")
    columns_rows, ext_rows, pattern_rows, _display_rows, _param_rows, vocabulary_rows = _product_meta_rows(
        contract, [("product_row_id", "varchar")], record)

    d1 = SqliteCatalogClient()
    d1.publish_product_meta(contract.product_id, "pub-9", columns_rows, ext_rows, pattern_rows)

    assert d1._query("SELECT column_name, description_ko, publication_id FROM d1_catalog_columns;") == [
        {"column_name": "product_row_id", "description_ko": "행 식별자", "publication_id": "pub-9"}]
    assert d1._query("SELECT grain, primary_key FROM d1_catalog_ext;") == [
        {"grain": "place_id마다 한 행.", "primary_key": '["product_row_id"]'}]
    assert d1._query("SELECT pattern_id, requires, allow_empty FROM d1_usage_patterns;") == [
        {"pattern_id": "p1", "requires": '["sort"]', "allow_empty": 0}]
    assert vocabulary_rows == []


def test_product_evidence_upserts_sources_and_current_quality_by_publication():
    d1 = SqliteCatalogClient()
    sources = [{
        "source_id": "kma_vilage_fcst",
        "source_url": "https://example.test/kma",
        "license": "KOGL-1",
        "license_url": "https://example.test/kogl",
        "redistribution": "allowed_with_attribution",
        "attribution": "기상청",
        "rights_checked_at": "2026-08-04",
    }]
    quality = {
        "source_row_count": 427,
        "d1_row_count": 427,
        "duplicate_primary_key_count": 0,
        "null_primary_key_count": 0,
        "freshness_as_of": "2026-08-04T11:00:00+09:00",
        "freshness_slo_minutes": 240,
        "serving_status": "published",
        "measured_at": "2026-08-04T11:03:00+09:00",
        "coverage": {"observed": 61, "expected": 61, "unit": "place"},
        "projection_schema_version": "1.1.0",
        "projection_schema_hash": "fixture-projection-hash",
    }

    d1.publish_product_evidence("weather_place_forecast_change_daily", "pub-1", sources, quality)

    assert d1._query(
        "SELECT source_id, attribution, publication_id FROM d1_catalog_sources;"
    ) == [{
        "source_id": "kma_vilage_fcst",
        "attribution": "기상청",
        "publication_id": "pub-1",
    }]
    assert d1._query(
        "SELECT source_row_count, d1_row_count, duplicate_primary_key_count, coverage_json, "
        "projection_schema_version, projection_schema_hash, publication_id "
        "FROM d1_product_quality;"
    ) == [{
        "source_row_count": 427,
        "d1_row_count": 427,
        "duplicate_primary_key_count": 0,
        "coverage_json": '{"expected":61,"observed":61,"unit":"place"}',
        "projection_schema_version": "1.1.0",
        "projection_schema_hash": "fixture-projection-hash",
        "publication_id": "pub-1",
    }]


def test_legacy_evidence_publish_retains_prior_source_rows_but_refreshes_quality():
    """미온보딩 계약은 기존 권리 증거를 지우지 않고, 런타임 품질만 현재 게시본으로 갱신한다."""
    d1 = SqliteCatalogClient()
    sources = [{
        "source_id": "kma_vilage_fcst",
        "source_url": "https://example.test/kma",
        "license": "KOGL-1",
        "license_url": "https://example.test/kogl",
        "redistribution": "allowed_with_attribution",
        "attribution": "기상청",
        "rights_checked_at": "2026-08-04",
    }]
    quality = {
        "source_row_count": 427,
        "d1_row_count": 427,
        "duplicate_primary_key_count": 0,
        "null_primary_key_count": 0,
        "freshness_as_of": "2026-08-04T11:00:00+09:00",
        "freshness_slo_minutes": 240,
        "serving_status": "published",
        "measured_at": "2026-08-04T11:03:00+09:00",
        "coverage": None,
    }
    d1.publish_product_evidence("weather_place_forecast_change_daily", "pub-1", sources, quality)

    d1.publish_product_evidence(
        "weather_place_forecast_change_daily",
        "pub-2",
        None,
        dict(quality, measured_at="2026-08-04T12:03:00+09:00"),
    )

    assert d1._query(
        "SELECT source_id, publication_id FROM d1_catalog_sources "
        "WHERE product_id = 'weather_place_forecast_change_daily';"
    ) == [{"source_id": "kma_vilage_fcst", "publication_id": "pub-1"}]
    assert d1._query(
        "SELECT publication_id FROM d1_product_quality "
        "WHERE product_id = 'weather_place_forecast_change_daily';"
    ) == [{"publication_id": "pub-2"}]


def test_evidence_quality_schema_migrates_missing_projection_identity_without_losing_other_product():
    """v1 evidence 초기 배포본도 projection identity 추가 시 행 보존 이행한다."""
    d1 = SqliteCatalogClient()
    d1._query(
        "CREATE TABLE d1_product_quality ("
        "product_id TEXT NOT NULL PRIMARY KEY, source_row_count INTEGER NOT NULL, d1_row_count INTEGER NOT NULL, "
        "duplicate_primary_key_count INTEGER NOT NULL, null_primary_key_count INTEGER NOT NULL, freshness_as_of TEXT, "
        "freshness_slo_minutes INTEGER, serving_status TEXT NOT NULL, measured_at TEXT NOT NULL, coverage_json TEXT, "
        "publication_id TEXT NOT NULL);"
    )
    d1._query(
        "INSERT INTO d1_product_quality VALUES ('other_product', 2, 2, 0, 0, '2026-08-03T00:00:00Z', 60, "
        "'published', '2026-08-03T00:01:00Z', NULL, 'old-pub');"
    )
    quality = {
        "source_row_count": 427,
        "d1_row_count": 427,
        "duplicate_primary_key_count": 0,
        "null_primary_key_count": 0,
        "freshness_as_of": "2026-08-04T11:00:00+09:00",
        "freshness_slo_minutes": 240,
        "serving_status": "published",
        "measured_at": "2026-08-04T11:03:00+09:00",
        "coverage": None,
        "projection_schema_version": "1.1.0",
        "projection_schema_hash": "new-projection-hash",
    }

    d1.publish_product_evidence("weather_place_forecast_change_daily", "pub-new", [], quality)

    assert d1._query(
        "SELECT product_id, publication_id, projection_schema_hash FROM d1_product_quality ORDER BY product_id;"
    ) == [
        {"product_id": "other_product", "publication_id": "old-pub", "projection_schema_hash": None},
        {"product_id": "weather_place_forecast_change_daily", "publication_id": "pub-new", "projection_schema_hash": "new-projection-hash"},
    ]


def test_product_meta_upsert_prunes_stale_rows_within_product_scope_only():
    """#638 §3 ② — 잔여 정리는 그 제품 스코프만. 타 제품(=타 도메인) 행은 무접촉."""
    d1 = SqliteCatalogClient()
    columns_rows, ext_rows, pattern_rows = _product_meta_payload("pub-1")
    d1.publish_product_meta("commerce_x", "pub-1", columns_rows, ext_rows, pattern_rows)
    d1._query(
        'INSERT INTO d1_usage_patterns ("product_id", "pattern_id", "question_ko", "sql", '
        '"axes", "requires", "verified_rows", "verified_at", "verified_publication_id", '
        '"allow_empty", "insight_sample_ko", "publication_id") VALUES '
        "('culture_event', 'ongoing_events', '진행 중 행사?', 'SELECT 1', '기간', '[]', "
        "5, NULL, NULL, 0, NULL, 'other-pub');"
    )

    columns_rows, ext_rows, pattern_rows = _product_meta_payload("pub-2")
    pattern_rows[0]["pattern_id"] = "top_gu_renamed"  # 선언에서 옛 pattern_id 가 사라진 상황
    d1.publish_product_meta("commerce_x", "pub-2", columns_rows, ext_rows, pattern_rows)

    mine = d1._query(
        "SELECT pattern_id, publication_id FROM d1_usage_patterns "
        "WHERE product_id = 'commerce_x';"
    )
    assert mine == [{"pattern_id": "top_gu_renamed", "publication_id": "pub-2"}]  # 옛 행 정리됨
    other = d1._query(
        "SELECT pattern_id, publication_id FROM d1_usage_patterns "
        "WHERE product_id = 'culture_event';"
    )
    assert other == [{"pattern_id": "ongoing_events", "publication_id": "other-pub"}]  # 무접촉


def test_glossary_upsert_scopes_cleanup_to_one_vocabulary():
    """용어사전 잔여 정리는 vocabulary_id 스코프 — 다른 어휘(=다른 소유 도메인)는 무접촉."""
    d1 = SqliteCatalogClient()
    d1._query(handoff_ddl("d1_catalog_glossary"))
    seed = [
        {"vocabulary_id": "commerce:major", "code": "health", "label_ko": "보건",
         "origin": "commerce", "source_type": "warehouse", "exported_at": "t0"},
        {"vocabulary_id": "commerce:major", "code": "food", "label_ko": "식품",
         "origin": "commerce", "source_type": "warehouse", "exported_at": "t0"},
        {"vocabulary_id": "culture:event_type", "code": "festival", "label_ko": "축제",
         "origin": "culture", "source_type": "codebook", "exported_at": "t0"},
    ]
    for statement in handoff_upsert_statements("d1_catalog_glossary", seed):
        d1._query(statement)

    refreshed = [dict(seed[0], label_ko="보건업", exported_at="t1")]  # food 는 원천에서 사라졌다
    for statement in handoff_upsert_statements("d1_catalog_glossary", refreshed):
        d1._query(statement)
    d1._query(handoff_stale_delete_statement("d1_catalog_glossary", "commerce:major", "t1"))

    major = d1._query(
        "SELECT code, label_ko FROM d1_catalog_glossary WHERE vocabulary_id = 'commerce:major';"
    )
    assert major == [{"code": "health", "label_ko": "보건업"}]
    culture = d1._query(
        "SELECT code FROM d1_catalog_glossary WHERE vocabulary_id = 'culture:event_type';"
    )
    assert culture == [{"code": "festival"}]  # 타 어휘 무접촉


def test_glossary_registry_gate_flags_unregistered_and_mismatched_rows():
    """#638 §5-5 — 미등록 어휘와 정본(origin/source_type) 불일치 행을 게시 전에 판별한다."""
    from common.serving.d1_client import glossary_registry_violations

    rows = [
        {"vocabulary_id": "commerce:major", "origin": "commerce", "source_type": "warehouse"},
        {"vocabulary_id": "culture:event_type", "origin": "culture", "source_type": "codebook"},
        {"vocabulary_id": "common:gu_code", "origin": "commerce", "source_type": "warehouse"},
    ]
    violations = glossary_registry_violations(rows)
    assert "commerce:major" not in violations                       # 등록·정합 — 통과
    assert violations["culture:event_type"] == "레지스트리 미등록"  # culture 온보딩 PR 에서 등재
    assert "asac_axes" in violations["common:gu_code"]              # 정본 origin 위조 감지


def test_publication_ledger_is_append_only_and_records_publication_stage():
    d1 = SqliteCatalogClient()
    record = {
        "publication_id": "p-1",
        "product_id": "weather_place_risk_window",
        "model_name": "gold_weather_place_risk_window",
        "source_run_id": "run-1",
        "attempted_at": "2026-07-29T00:00:00+00:00",
        "outcome": "published",
        "stage": "completed",
        "source_row_count": 304878,
        "published_row_count": 304878,
        "d1_row_count": 304878,
        "api_smoke_status": "not_evaluated",
        "rollback_status": "not_needed",
        "reason": "ok",
    }

    d1.append_publication_ledger(record)

    assert d1._query("SELECT publication_id, outcome, stage FROM _publication_ledger;") == [
        {"publication_id": "p-1", "outcome": "published", "stage": "completed"}
    ]
    with pytest.raises(sqlite3.IntegrityError):
        d1.append_publication_ledger(record)


# ── d1_catalog_display (v1.10 · ASAC-DAG#706) ─────────────────────────────────
# 이 표를 **새로 만든** 이유가 핵심이다: 공유 표에 컬럼을 더하면 구 코드를 가진 실행기가
# 자기가 아는 모양으로 되돌리며 그 컬럼을 지운다(handoff_schema_is_current 가 완전 일치 판정).
# 아래 두 테스트가 그 불변을 지킨다.

def test_display_rows_publish_and_replace():
    d1 = SqliteCatalogClient()
    columns_rows, ext_rows, pattern_rows = _product_meta_payload("pub-1")
    display_rows = [{
        "product_id": "commerce_x", "title": "제목", "summary": "요약",
        "caveat": None, "use_cases": '["a", "b"]', "publication_id": "pub-1",
    }]

    d1.publish_product_meta("commerce_x", "pub-1", columns_rows, ext_rows, pattern_rows, display_rows)

    rows = d1._query("SELECT product_id, title, use_cases, publication_id FROM d1_catalog_display;")
    assert rows == [{"product_id": "commerce_x", "title": "제목",
                     "use_cases": '["a", "b"]', "publication_id": "pub-1"}]

    # 같은 제품 재게시 = 자연키 upsert(행이 늘지 않는다)
    display_rows[0].update(title="바뀐 제목", publication_id="pub-2")
    d1.publish_product_meta("commerce_x", "pub-2", columns_rows, ext_rows, pattern_rows, display_rows)
    rows = d1._query("SELECT product_id, title FROM d1_catalog_display;")
    assert rows == [{"product_id": "commerce_x", "title": "바뀐 제목"}]


def test_display_declaration_withdrawn_removes_stale_row():
    """선언을 내린 제품의 옛 표시 메타가 남으면 화면이 없는 제목을 계속 보여준다."""
    d1 = SqliteCatalogClient()
    columns_rows, ext_rows, pattern_rows = _product_meta_payload("pub-1")
    d1.publish_product_meta("commerce_x", "pub-1", columns_rows, ext_rows, pattern_rows, [{
        "product_id": "commerce_x", "title": "제목", "summary": "요약",
        "caveat": None, "use_cases": None, "publication_id": "pub-1",
    }])
    assert d1._query("SELECT COUNT(*) AS n FROM d1_catalog_display;")[0]["n"] == 1

    # display 를 내린 채 재게시 — 빈 시퀀스
    d1.publish_product_meta("commerce_x", "pub-2", columns_rows, ext_rows, pattern_rows, [])

    assert d1._query("SELECT COUNT(*) AS n FROM d1_catalog_display;")[0]["n"] == 0


def test_display_table_does_not_disturb_shared_tables():
    """🔴 이 PR 의 안전 근거 — 기존 3종의 컬럼 집합이 그대로여야 재작성이 안 일어난다."""
    assert HANDOFF_COLUMNS["d1_catalog_columns"] == (
        "product_id", "table_name", "ordinal", "column_name", "type",
        "description_ko", "publication_id")
    assert HANDOFF_COLUMNS["d1_catalog_ext"] == (
        "product_id", "table_name", "source_model", "grain", "primary_key",
        "time_axis", "tier", "rollup_rule", "publication_id")
    assert HANDOFF_COLUMNS["d1_usage_patterns"][0] == "product_id"
    assert len(HANDOFF_COLUMNS["d1_usage_patterns"]) == 12
