from __future__ import annotations

import json
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


DOMAIN_ROOT = Path(__file__).resolve().parents[1]
DAGS_ROOT = DOMAIN_ROOT.parents[1]
sys.path.insert(0, str(DOMAIN_ROOT))
sys.path.insert(0, str(DAGS_ROOT))

from weather_ingest.errors import WeatherCompletenessError, WeatherRawIntegrityError  # noqa: E402
from weather_ingest.kma_observation import REQUIRED_CATEGORIES, SOURCE_ID  # noqa: E402
from weather_ingest.kma_observation_landing import (  # noqa: E402
    KmaObservationLanding,
    ObservationCheckpoint,
    ObservationGrid,
    ObservationLandingRequest,
    ObservationRunIdentity,
    build_complete_observation_manifest,
    observation_checkpoint_key,
    observation_raw_object_key,
)


START = datetime(2026, 8, 22, 0, 45, tzinfo=timezone.utc)
RUN = ObservationRunIdentity(
    dag_id="weather_ultra_srt_ncst_bronze",
    run_id="scheduled__2026-08-22T00:45:00+00:00",
)


class MemoryRawStore:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.events: list[tuple[str, str]] = []
        self.lock = threading.Lock()

    def exists(self, key: str) -> bool:
        with self.lock:
            return key in self.objects

    def read_bytes(self, key: str) -> bytes:
        with self.lock:
            return self.objects[key]

    def write_bytes_if_absent(
        self,
        key: str,
        payload: bytes,
        content_type: str,
    ) -> bool:
        assert content_type == "application/json"
        with self.lock:
            self.events.append(("write", key))
            if key in self.objects:
                return False
            self.objects[key] = payload
            return True


class FakeObservationSource:
    def __init__(self, *, fail_on_call: int | None = None):
        self.calls: list[dict[str, object]] = []
        self.fail_on_call = fail_on_call

    def fetch(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail_on_call == len(self.calls):
            raise RuntimeError("injected throttle/deadline failure")
        return 200, _response(
            base_date=kwargs["base_date"],
            base_time=kwargs["base_time"],
            nx=kwargs["nx"],
            ny=kwargs["ny"],
        )


def _response(*, base_date: str, base_time: str, nx: int, ny: int) -> bytes:
    rows = [
        {
            "baseDate": base_date,
            "baseTime": base_time,
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
                "header": {"resultCode": "00", "resultMsg": "NORMAL_SERVICE"},
                "body": {
                    "totalCount": len(rows),
                    "items": {"item": rows},
                },
            }
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _grids(count: int) -> tuple[ObservationGrid, ...]:
    return tuple(ObservationGrid(nx=50 + index, ny=120) for index in range(count))


def _request(grids) -> ObservationLandingRequest:
    return ObservationLandingRequest(
        base_date="20260822",
        base_time="0900",
        grids=tuple(grids),
    )


def _landing(
    store,
    source,
    *,
    expected_grid_count=2,
    request_ids=None,
):
    ids = iter(request_ids or (f"request-{index}" for index in range(100)))
    captured_deadlines = []

    def source_factory(deadline):
        captured_deadlines.append(deadline)
        return source

    landing = KmaObservationLanding(
        source_factory=source_factory,
        raw_store=store,
        clock=lambda: START,
        monotonic_clock=lambda: 100.0,
        request_id=lambda: next(ids),
        expected_grid_count=expected_grid_count,
    )
    return landing, captured_deadlines


def test_one_grid_writes_validated_raw_before_its_checkpoint():
    store = MemoryRawStore()
    source = FakeObservationSource()
    landing, _ = _landing(store, source, expected_grid_count=1)
    grid = ObservationGrid(nx=60, ny=127)

    batch = landing.collect(RUN, _request((grid,)))

    raw = batch.raw_objects[0]
    checkpoint = observation_checkpoint_key(RUN, "20260822", "0900", grid)
    written_keys = [key for action, key in store.events if action == "write"]
    assert written_keys.index(raw.raw_object_key) < written_keys.index(checkpoint)
    assert store.read_bytes(raw.raw_object_key) == _response(
        base_date="20260822", base_time="0900", nx=60, ny=127
    )
    assert raw.category_count == 8
    assert raw.categories == REQUIRED_CATEGORIES


def test_deadline_anchor_is_durable_before_the_first_source_attempt():
    store = MemoryRawStore()
    source = FakeObservationSource()
    landing, captured = _landing(store, source, expected_grid_count=1)

    landing.collect(RUN, _request((ObservationGrid(60, 127),)))

    first_write_key = store.events[0][1]
    assert "kma_cycle_deadlines" in first_write_key
    assert captured
    assert source.calls


def test_xcom_contract_contains_metadata_but_no_raw_payload_bytes():
    store = MemoryRawStore()
    source = FakeObservationSource()
    landing, _ = _landing(store, source, expected_grid_count=1)

    result = landing.collect(RUN, _request((ObservationGrid(60, 127),))).to_xcom()

    assert result["source_id"] == SOURCE_ID
    assert result["grid_count"] == 1
    assert result["row_count"] == 8
    assert "raw_bytes" not in result
    assert "payload" not in result
    json.dumps(result)


def test_xcom_round_trip_preserves_the_complete_landing_contract():
    store = MemoryRawStore()
    source = FakeObservationSource()
    landing, _ = _landing(store, source, expected_grid_count=1)
    batch = landing.collect(RUN, _request((ObservationGrid(60, 127),)))

    assert type(batch).from_xcom(batch.to_xcom()) == batch


def test_stateless_retry_hash_verifies_checkpoints_and_requests_only_missing_grids():
    store = MemoryRawStore()
    first_source = FakeObservationSource(fail_on_call=2)
    first_landing, _ = _landing(store, first_source, request_ids=("one", "two"))
    request = _request(_grids(2))

    with pytest.raises(RuntimeError, match="injected"):
        first_landing.collect(RUN, request)

    assert len(first_source.calls) == 2
    assert not any("_manifests" in key for key in store.objects)

    retry_source = FakeObservationSource()
    retry_landing, _ = _landing(store, retry_source, request_ids=("retry-two",))
    batch = retry_landing.collect(RUN, request)

    assert len(retry_source.calls) == 1
    assert retry_source.calls[0]["nx"] == request.grids[1].nx
    assert batch.reused_grid_count == 1
    assert batch.grid_count == 2
    assert batch.row_count == 16


def test_complete_stateless_rerun_performs_no_source_requests():
    store = MemoryRawStore()
    source = FakeObservationSource()
    landing, _ = _landing(store, source)
    request = _request(_grids(2))
    first = landing.collect(RUN, request)

    retry_source = FakeObservationSource()
    retry_landing, _ = _landing(store, retry_source)
    second = retry_landing.collect(RUN, request)

    assert retry_source.calls == []
    assert second.raw_objects == first.raw_objects
    assert second.reused_grid_count == 2
    assert second.manifest_key == first.manifest_key


def test_corrupt_checkpointed_raw_object_fails_closed_without_refetching():
    store = MemoryRawStore()
    source = FakeObservationSource()
    landing, _ = _landing(store, source, expected_grid_count=1)
    request = _request((ObservationGrid(60, 127),))
    batch = landing.collect(RUN, request)
    store.objects[batch.raw_objects[0].raw_object_key] = b"tampered"

    retry_source = FakeObservationSource()
    retry_landing, _ = _landing(store, retry_source, expected_grid_count=1)
    with pytest.raises(WeatherRawIntegrityError, match="hash"):
        retry_landing.collect(RUN, request)

    assert retry_source.calls == []


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("source_id", "foreign_source", "source"),
        ("observed_slot", "2026-08-22T08:00:00+09:00", "slot"),
        ("nx", 999, "grid"),
    ],
)
def test_foreign_slot_or_grid_checkpoint_fails_closed(field, value, message):
    store = MemoryRawStore()
    source = FakeObservationSource()
    landing, _ = _landing(store, source, expected_grid_count=1)
    grid = ObservationGrid(60, 127)
    request = _request((grid,))
    landing.collect(RUN, request)
    key = observation_checkpoint_key(RUN, "20260822", "0900", grid)
    document = json.loads(store.objects[key])
    document[field] = value
    store.objects[key] = json.dumps(document, sort_keys=True).encode()

    retry_landing, _ = _landing(
        store,
        FakeObservationSource(),
        expected_grid_count=1,
    )
    with pytest.raises(WeatherRawIntegrityError, match=message):
        retry_landing.collect(RUN, request)


def test_duplicate_grid_input_fails_before_source_or_storage_work():
    store = MemoryRawStore()
    source = FakeObservationSource()
    landing, _ = _landing(store, source)
    grid = ObservationGrid(60, 127)

    with pytest.raises(WeatherCompletenessError, match="duplicate"):
        landing.collect(RUN, _request((grid, grid)))

    assert source.calls == []
    assert store.events == []


def test_79_grids_cannot_start_an_80_grid_collection():
    store = MemoryRawStore()
    source = FakeObservationSource()
    landing, _ = _landing(store, source, expected_grid_count=80)

    with pytest.raises(WeatherCompletenessError, match="expected=80, actual=79"):
        landing.collect(RUN, _request(_grids(79)))

    assert source.calls == []
    assert store.events == []


def test_exact_80_grids_and_640_categories_create_one_complete_manifest():
    store = MemoryRawStore()
    source = FakeObservationSource()
    landing, _ = _landing(store, source, expected_grid_count=80)

    batch = landing.collect(RUN, _request(_grids(80)))

    assert batch.grid_count == 80
    assert batch.row_count == 640
    assert len(batch.raw_objects) == 80
    assert batch.is_publishable is True
    manifest = json.loads(store.objects[batch.manifest_key])
    assert manifest["status"] == "complete"
    assert manifest["grid_count"] == 80
    assert manifest["row_count"] == 640


def test_639_category_rows_cannot_build_a_complete_manifest():
    checkpoints = [
        ObservationCheckpoint(
            source_id=SOURCE_ID,
            dag_id=RUN.dag_id,
            run_id=RUN.run_id,
            base_date="20260822",
            base_time="0900",
            observed_slot="2026-08-22T09:00:00+09:00",
            nx=50 + index,
            ny=120,
            request_id=f"request-{index}",
            raw_object_key=f"raw/object-{index}.json",
            payload_sha256="a" * 64,
            http_status=200,
            collected_at=START,
            category_count=7 if index == 79 else 8,
            categories=(
                REQUIRED_CATEGORIES[:-1]
                if index == 79
                else REQUIRED_CATEGORIES
            ),
        )
        for index in range(80)
    ]

    with pytest.raises(WeatherCompletenessError, match="row_count=639"):
        build_complete_observation_manifest(
            RUN,
            base_date="20260822",
            base_time="0900",
            checkpoints=checkpoints,
            expected_grid_count=80,
        )


def test_conditional_raw_conflict_accepts_only_byte_identical_content():
    store = MemoryRawStore()
    source = FakeObservationSource()
    landing, _ = _landing(
        store,
        source,
        expected_grid_count=1,
        request_ids=("fixed",),
    )
    grid = ObservationGrid(60, 127)
    payload = _response(base_date="20260822", base_time="0900", nx=60, ny=127)
    key = observation_raw_object_key(RUN, "20260822", "0900", grid, "fixed", payload)
    store.objects[key] = payload

    batch = landing.collect(RUN, _request((grid,)))
    assert batch.raw_objects[0].raw_object_key == key

    conflicting_store = MemoryRawStore()
    conflicting_store.objects[key] = b"different"
    conflicting_landing, _ = _landing(
        conflicting_store,
        FakeObservationSource(),
        expected_grid_count=1,
        request_ids=("fixed",),
    )
    with pytest.raises(WeatherRawIntegrityError, match="conflict"):
        conflicting_landing.collect(RUN, _request((grid,)))
