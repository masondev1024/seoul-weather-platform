from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest


DOMAIN_ROOT = Path(__file__).resolve().parents[1]
DAGS_ROOT = DOMAIN_ROOT.parents[1]
sys.path.insert(0, str(DOMAIN_ROOT))
sys.path.insert(0, str(DAGS_ROOT))

import weather_ingest.kma_observation_bronze as bronze  # noqa: E402
from common.assets import WEATHER_OBSERVATION_BRONZE_ASSET  # noqa: E402
from weather_ingest.errors import WeatherCompletenessError, WeatherRawIntegrityError  # noqa: E402
from weather_ingest.kma_observation import REQUIRED_CATEGORIES, SOURCE_ID  # noqa: E402
from weather_ingest.kma_observation_bronze import (  # noqa: E402
    OBSERVATION_BRONZE_COLUMNS,
    OBSERVATION_BRONZE_TABLE,
    append_observation_bronze_revisions,
    build_observation_bronze_rows,
    create_observation_bronze_table,
    observation_grid_revisions,
    validate_observation_bronze_rows,
    verify_observation_bronze_run_slot,
)
from weather_ingest.kma_observation_landing import (  # noqa: E402
    ObservationLandingBatch,
    ObservationRawObject,
)


OBSERVED_AT = datetime(2026, 8, 22, 0, 0, tzinfo=timezone.utc)
COLLECTED_AT = datetime(2026, 8, 22, 0, 45, tzinfo=timezone.utc)


def _response(nx: int, ny: int) -> bytes:
    rows = [
        {
            "baseDate": "20260822",
            "baseTime": "0900",
            "category": category,
            "nx": nx,
            "ny": ny,
            "obsrValue": 0 if category == "PTY" else 1.0,
        }
        for category in REQUIRED_CATEGORIES
    ]
    return json.dumps(
        {
            "response": {
                "header": {"resultCode": "00"},
                "body": {"totalCount": 8, "items": {"item": rows}},
            }
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _batch(count: int = 2):
    payloads = {}
    raw_objects = []
    for index in range(count):
        nx, ny = 50 + index, 120
        payload = _response(nx, ny)
        key = f"raw/weather_observation/grid-{nx}-{ny}.json"
        payloads[key] = payload
        raw_objects.append(
            ObservationRawObject(
                request_id=f"request-{index}",
                raw_object_key=key,
                payload_sha256=hashlib.sha256(payload).hexdigest(),
                http_status=200,
                collected_at=COLLECTED_AT,
                nx=nx,
                ny=ny,
                category_count=8,
                categories=REQUIRED_CATEGORIES,
            )
        )
    batch = ObservationLandingBatch(
        source_id=SOURCE_ID,
        dag_id="weather_ultra_srt_ncst_bronze",
        run_id="scheduled__one",
        base_date="20260822",
        base_time="0900",
        observed_slot="2026-08-22T09:00:00+09:00",
        raw_objects=tuple(raw_objects),
        grid_count=count,
        row_count=count * 8,
        api_request_count=count,
        reused_grid_count=0,
        manifest_key="ops/weather/_manifests/complete.json",
        is_publishable=True,
    )
    return batch, payloads


def _grid_revisions(count: int = 80) -> list[dict[str, str]]:
    return [
        {
            "grid_id": f"kma_{50 + index}_120",
            "source_revision": f"revision-{index}",
        }
        for index in range(count)
    ]


def _change_first_grid_revision(batch, payloads):
    raw = batch.raw_objects[0]
    changed_document = json.loads(payloads[raw.raw_object_key])
    changed_document["response"]["body"]["items"]["item"][0]["obsrValue"] = 2.0
    changed_payload = json.dumps(
        changed_document,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    payloads[raw.raw_object_key] = changed_payload
    changed_raw = replace(
        raw,
        payload_sha256=hashlib.sha256(changed_payload).hexdigest(),
    )
    return replace(
        batch,
        run_id="scheduled__two",
        raw_objects=(changed_raw, *batch.raw_objects[1:]),
    )


class RecordingCursor:
    def __init__(self, row=None):
        self.statements = []
        self.row = row

    def execute(self, statement):
        self.statements.append(" ".join(statement.split()))

    def fetchone(self):
        return self.row


class RecordingTransaction:
    def __init__(self):
        self.events = []

    def __enter__(self):
        self.events.append(("begin", None))
        return self

    def __exit__(self, exc_type, _exc, _traceback):
        self.events.append(("commit" if exc_type is None else "rollback", None))

    def delete(self, expression):
        self.events.append(("delete", expression))

    def append(self, rows):
        self.events.append(("append", rows))


class RecordingTable:
    def __init__(self):
        self.txn = RecordingTransaction()

    def transaction(self):
        return self.txn


def test_observation_bronze_has_a_dedicated_publication_asset():
    assert WEATHER_OBSERVATION_BRONZE_ASSET == (
        "iceberg://weather/observation/bronze"
    )


def test_create_table_uses_dedicated_identifier_and_day_partition_transform():
    cursor = RecordingCursor()

    qualified = create_observation_bronze_table(
        cursor,
        catalog="iceberg",
        schema="weather_traffic_bronze",
    )

    assert qualified == (
        "iceberg.weather_traffic_bronze.bronze_kma_ultra_srt_ncst"
    )
    ddl = cursor.statements[-1]
    assert f"CREATE TABLE IF NOT EXISTS {qualified}" in ddl
    assert "partitioning = ARRAY['day(observed_at)']" in ddl
    for column in OBSERVATION_BRONZE_COLUMNS:
        assert column in ddl


def test_converter_preserves_lineage_values_and_provisional_quality():
    batch, payloads = _batch(count=2)

    rows = build_observation_bronze_rows(
        batch,
        read_raw=payloads.__getitem__,
        expected_grid_count=2,
    )

    assert len(rows) == 16
    first = rows[0]
    assert tuple(first) == OBSERVATION_BRONZE_COLUMNS
    assert first["source_id"] == SOURCE_ID
    assert first["dag_run_id"] == batch.run_id
    assert first["observed_slot"] == batch.observed_slot
    assert first["observed_at"] == OBSERVED_AT.replace(tzinfo=None)
    assert first["raw_object_key"] == batch.raw_objects[0].raw_object_key
    assert first["payload_sha256"] == batch.raw_objects[0].payload_sha256
    assert first["source_revision"] == (
        f"{SOURCE_ID}:{batch.raw_objects[0].payload_sha256}"
    )
    assert first["quality_status"] == "provisional"
    assert len({row["idempotency_key"] for row in rows}) == 16

    assert observation_grid_revisions(rows, expected_grid_count=2) == [
        {
            "grid_id": "kma_50_120",
            "source_revision": rows[0]["source_revision"],
        },
        {
            "grid_id": "kma_51_120",
            "source_revision": rows[8]["source_revision"],
        },
    ]


@pytest.mark.parametrize(
    "mutation",
    [
        {"is_publishable": False},
        {"grid_count": 1},
        {"row_count": 15},
        {"manifest_key": ""},
    ],
)
def test_converter_refuses_incomplete_or_unverified_manifest(mutation):
    batch, payloads = _batch(count=2)
    batch = replace(batch, **mutation)

    with pytest.raises(WeatherCompletenessError):
        build_observation_bronze_rows(
            batch,
            read_raw=payloads.__getitem__,
            expected_grid_count=2,
        )


def test_converter_hash_verifies_raw_before_building_rows():
    batch, payloads = _batch(count=1)
    payloads[batch.raw_objects[0].raw_object_key] = b"tampered"

    with pytest.raises(WeatherRawIntegrityError, match="hash"):
        build_observation_bronze_rows(
            batch,
            read_raw=payloads.__getitem__,
            expected_grid_count=1,
        )


def test_exact_80_grid_conversion_produces_640_validated_rows():
    batch, payloads = _batch(count=80)

    rows = build_observation_bronze_rows(batch, read_raw=payloads.__getitem__)

    assert len(rows) == 640
    validate_observation_bronze_rows(rows)


def test_duplicate_idempotency_keys_are_rejected_before_table_io():
    batch, payloads = _batch(count=2)
    rows = build_observation_bronze_rows(
        batch,
        read_raw=payloads.__getitem__,
        expected_grid_count=2,
    )
    rows[-1] = {**rows[-1], "idempotency_key": rows[0]["idempotency_key"]}

    with pytest.raises(WeatherCompletenessError, match="idempotency"):
        validate_observation_bronze_rows(rows, expected_grid_count=2)


def test_novel_revisions_append_in_one_bounded_transaction(monkeypatch):
    batch, payloads = _batch(count=2)
    rows = build_observation_bronze_rows(
        batch,
        read_raw=payloads.__getitem__,
        expected_grid_count=2,
    )
    table = RecordingTable()
    monkeypatch.setattr(bronze, "_existing_observation_rows", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(bronze, "_arrow_table", lambda records: list(records))

    inserted = append_observation_bronze_revisions(
        table,
        rows,
        expected_grid_count=2,
    )

    assert inserted == 16
    assert table.txn.events == [
        ("begin", None),
        ("append", rows),
        ("commit", None),
    ]


def test_rerunning_same_complete_run_builds_identical_final_rows():
    batch, payloads = _batch(count=2)

    first = build_observation_bronze_rows(
        batch,
        read_raw=payloads.__getitem__,
        expected_grid_count=2,
    )
    second = build_observation_bronze_rows(
        batch,
        read_raw=payloads.__getitem__,
        expected_grid_count=2,
    )

    assert second == first


def test_source_revision_identity_is_stable_across_airflow_run_ids():
    batch, payloads = _batch(count=2)

    first = build_observation_bronze_rows(
        batch,
        read_raw=payloads.__getitem__,
        expected_grid_count=2,
    )
    rerun = build_observation_bronze_rows(
        replace(batch, run_id="scheduled__two"),
        read_raw=payloads.__getitem__,
        expected_grid_count=2,
    )

    assert {row["idempotency_key"] for row in rerun} == {
        row["idempotency_key"] for row in first
    }
    assert {row["dag_run_id"] for row in rerun} == {"scheduled__two"}


def test_changed_source_revision_changes_only_that_grids_identity_keys():
    batch, payloads = _batch(count=2)
    first = build_observation_bronze_rows(
        batch,
        read_raw=payloads.__getitem__,
        expected_grid_count=2,
    )
    changed_batch = _change_first_grid_revision(batch, payloads)

    changed = build_observation_bronze_rows(
        changed_batch,
        read_raw=payloads.__getitem__,
        expected_grid_count=2,
    )

    first_by_grid = {}
    changed_by_grid = {}
    for row in first:
        first_by_grid.setdefault(row["grid_id"], set()).add(row["idempotency_key"])
    for row in changed:
        changed_by_grid.setdefault(row["grid_id"], set()).add(row["idempotency_key"])
    assert first_by_grid[changed[0]["grid_id"]] != changed_by_grid[changed[0]["grid_id"]]
    unchanged_grid = batch.raw_objects[1]
    unchanged_grid_id = f"kma_{unchanged_grid.nx}_{unchanged_grid.ny}"
    assert first_by_grid[unchanged_grid_id] == changed_by_grid[unchanged_grid_id]


def test_cross_run_identical_revisions_are_a_storage_noop(monkeypatch):
    batch, payloads = _batch(count=80)
    existing = build_observation_bronze_rows(
        batch,
        read_raw=payloads.__getitem__,
    )
    rerun = build_observation_bronze_rows(
        replace(batch, run_id="scheduled__two"),
        read_raw=payloads.__getitem__,
    )
    table = RecordingTable()
    monkeypatch.setattr(
        bronze,
        "_existing_observation_rows",
        lambda *_args, **_kwargs: existing,
    )

    inserted = append_observation_bronze_revisions(
        table,
        rerun,
    )

    assert inserted == 0
    assert table.txn.events == []


def test_changed_grid_appends_only_novel_revision_rows(monkeypatch):
    batch, payloads = _batch(count=2)
    existing = build_observation_bronze_rows(
        batch,
        read_raw=payloads.__getitem__,
        expected_grid_count=2,
    )
    changed_batch = _change_first_grid_revision(batch, payloads)
    changed = build_observation_bronze_rows(
        changed_batch,
        read_raw=payloads.__getitem__,
        expected_grid_count=2,
    )
    table = RecordingTable()
    monkeypatch.setattr(
        bronze,
        "_existing_observation_rows",
        lambda *_args, **_kwargs: existing,
    )
    monkeypatch.setattr(bronze, "_arrow_table", lambda records: list(records))

    inserted = append_observation_bronze_revisions(
        table,
        changed,
        expected_grid_count=2,
    )

    assert inserted == 8
    appended = table.txn.events[1][1]
    assert {row["grid_id"] for row in appended} == {changed[0]["grid_id"]}
    assert table.txn.events[0] == ("begin", None)
    assert table.txn.events[-1] == ("commit", None)


def test_existing_identity_with_conflicting_content_fails_closed(monkeypatch):
    batch, payloads = _batch(count=2)
    rows = build_observation_bronze_rows(
        batch,
        read_raw=payloads.__getitem__,
        expected_grid_count=2,
    )
    existing = [dict(row) for row in rows]
    existing[0]["observed_value"] = float(existing[0]["observed_value"]) + 1.0
    table = RecordingTable()
    monkeypatch.setattr(
        bronze,
        "_existing_observation_rows",
        lambda *_args, **_kwargs: existing,
    )

    with pytest.raises(WeatherRawIntegrityError, match="conflicting"):
        append_observation_bronze_revisions(
            table,
            rows,
            expected_grid_count=2,
        )

    assert table.txn.events == []


def test_verifier_requires_exact_640_80_8_revision_scope_and_unique_keys():
    cursor = RecordingCursor((640, 80, 8, 1, 1, 80, 640, 8, 8, 0))

    count = verify_observation_bronze_run_slot(
        cursor,
        qualified_table=(
            "iceberg.weather_traffic_bronze.bronze_kma_ultra_srt_ncst"
        ),
        observed_slot="2026-08-22T09:00:00+09:00",
        expected_grid_revisions=_grid_revisions(),
    )

    assert count == 640
    query = cursor.statements[0]
    assert "source_id = 'kma_ultra_srt_ncst'" in query
    assert "dag_run_id = 'scheduled__one'" not in query
    assert "observed_slot = '2026-08-22T09:00:00+09:00'" in query
    assert "observed_at = TIMESTAMP '2026-08-22 00:00:00.000000'" in query
    assert "JOIN expected" in query
    assert "'kma_50_120', 'revision-0'" in query


@pytest.mark.parametrize("observed_slot", ["", "not-a-slot", "2026-08-22T09:00:00"])
def test_verifier_rejects_an_invalid_or_timezone_naive_partition_slot(observed_slot):
    cursor = RecordingCursor((640, 80, 8, 1, 1, 80, 640, 8, 8, 0))

    with pytest.raises(WeatherCompletenessError, match="observed_slot"):
        verify_observation_bronze_run_slot(
            cursor,
            qualified_table=(
                "iceberg.weather_traffic_bronze.bronze_kma_ultra_srt_ncst"
            ),
            observed_slot=observed_slot,
            expected_grid_revisions=_grid_revisions(),
        )
    assert cursor.statements == []


@pytest.mark.parametrize(
    ("row", "message"),
    [
        ((640, 79, 8, 1, 1, 80, 640, 8, 8, 0), "grid_count=79"),
        ((640, 80, 7, 1, 1, 80, 640, 8, 8, 0), "category_count=7"),
        ((640, 80, 8, 2, 1, 80, 640, 8, 8, 0), "slot_count=2"),
        ((640, 80, 8, 1, 2, 80, 640, 8, 8, 0), "source_count=2"),
        ((640, 80, 8, 1, 1, 79, 640, 8, 8, 0), "grid_revision_count=79"),
        ((640, 80, 8, 1, 1, 80, 639, 8, 8, 0), "idempotency_count=639"),
        ((640, 80, 8, 1, 1, 80, 640, 7, 8, 0), "categories_per_grid_min=7"),
        ((640, 80, 8, 1, 1, 80, 640, 8, 8, 1), "invalid_category_count=1"),
    ],
)
def test_verifier_rejects_false_complete_aggregates(row, message):
    cursor = RecordingCursor(row)

    with pytest.raises(WeatherCompletenessError, match=message):
        verify_observation_bronze_run_slot(
            cursor,
            qualified_table=(
                "iceberg.weather_traffic_bronze.bronze_kma_ultra_srt_ncst"
            ),
            observed_slot="2026-08-22T09:00:00+09:00",
            expected_grid_revisions=_grid_revisions(),
        )


def test_verifier_rejects_incomplete_or_duplicate_expected_revision_scope():
    cursor = RecordingCursor()

    with pytest.raises(WeatherCompletenessError, match="revision scope"):
        verify_observation_bronze_run_slot(
            cursor,
            qualified_table=(
                "iceberg.weather_traffic_bronze.bronze_kma_ultra_srt_ncst"
            ),
            observed_slot="2026-08-22T09:00:00+09:00",
            expected_grid_revisions=_grid_revisions(79),
        )

    duplicated = _grid_revisions()
    duplicated[-1] = dict(duplicated[0])
    with pytest.raises(WeatherCompletenessError, match="revision scope"):
        verify_observation_bronze_run_slot(
            cursor,
            qualified_table=(
                "iceberg.weather_traffic_bronze.bronze_kma_ultra_srt_ncst"
            ),
            observed_slot="2026-08-22T09:00:00+09:00",
            expected_grid_revisions=duplicated,
        )

    assert cursor.statements == []
