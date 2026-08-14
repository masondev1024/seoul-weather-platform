from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.landing import (  # noqa: E402
    KmaGrid,
    KmaLanding,
    KmaLandingBatch,
    KmaLandingIncompleteError,
    KmaLandingRequest,
    RunIdentity,
)
from weather_ingest.errors import (  # noqa: E402
    WeatherRawIntegrityError,
    WeatherSourceSchemaError,
)


def kma_payload(
    *,
    total_count: int,
    item_count: int,
    page_no: int = 1,
    num_of_rows: int = 1000,
    result_code: str = "00",
    base_date: str = "20260714",
    base_time: str = "0800",
    nx: int = 60,
    ny: int = 127,
) -> bytes:
    return json.dumps(
        {
            "response": {
                "header": {"resultCode": result_code, "resultMsg": "OK"},
                "body": {
                    "items": {
                        "item": [
                            {
                                "baseDate": base_date,
                                "baseTime": base_time,
                                "fcstDate": base_date,
                                "fcstTime": "0900",
                                "category": "TMP",
                                "fcstValue": str(index),
                                "nx": nx,
                                "ny": ny,
                            }
                            for index in range(item_count)
                        ]
                    },
                    "pageNo": page_no,
                    "numOfRows": num_of_rows,
                    "totalCount": total_count,
                },
            }
        }
    ).encode("utf-8")


class ScriptedKmaSource:
    def __init__(self, pages: dict[tuple[int, int, int], bytes]) -> None:
        self.pages = pages
        self.requests: list[tuple[int, int, int]] = []

    def fetch_page(
        self,
        *,
        base_date: str,
        base_time: str,
        nx: int,
        ny: int,
        page_no: int,
        num_of_rows: int,
    ) -> tuple[int, bytes]:
        del base_date, base_time, num_of_rows
        key = (nx, ny, page_no)
        self.requests.append(key)
        return 200, self.pages[key]


class MemoryRawObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.write_order: list[str] = []

    def exists(self, key: str) -> bool:
        return key in self.objects

    def read_bytes(self, key: str) -> bytes:
        return self.objects[key][0]

    def write_bytes(self, key: str, payload: bytes, content_type: str) -> None:
        self.objects[key] = (payload, content_type)
        self.write_order.append(key)

    def write_bytes_if_absent(
        self, key: str, payload: bytes, content_type: str
    ) -> bool:
        if key in self.objects:
            return False
        self.write_bytes(key, payload, content_type)
        return True


def test_collect_keeps_raw_and_manifest_in_run_start_partition_across_midnight():
    clock_values = iter(
        (
            datetime(2026, 7, 30, 14, 59, 59, tzinfo=timezone.utc),
            datetime(2026, 7, 30, 15, 0, 1, tzinfo=timezone.utc),
            datetime(2026, 7, 30, 15, 0, 2, tzinfo=timezone.utc),
            datetime(2026, 7, 30, 15, 0, 3, tzinfo=timezone.utc),
        )
    )
    raw_store = MemoryRawObjectStore()
    batch = KmaLanding(
        source=ScriptedKmaSource(
            {
                (60, 127, 1): kma_payload(
                    total_count=1,
                    item_count=1,
                    base_date="20260730",
                    base_time="2300",
                ),
                (61, 127, 1): kma_payload(
                    total_count=1,
                    item_count=1,
                    base_date="20260730",
                    base_time="2300",
                    nx=61,
                ),
            }
        ),
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: next(clock_values),
        request_id=iter(("request-1", "request-2")).__next__,
    ).collect(
        RunIdentity(dag_id="weather_vilage_fcst_bronze", run_id="scheduled__midnight"),
        KmaLandingRequest(
            base_date="20260730",
            base_time="2300",
            grids=(
                KmaGrid(place_id="first", nx=60, ny=127),
                KmaGrid(place_id="second", nx=61, ny=127),
            ),
            num_of_rows=1000,
        ),
    )

    assert all("/load_date=2026-07-30/" in item.raw_object_key for item in batch.raw_objects)
    assert "/load_date=2026-07-30/" in str(batch.manifest_key)
    assert batch.to_xcom()["landing_load_date"] == "2026-07-30"
    assert json.loads(raw_store.read_bytes(str(batch.manifest_key)))["load_date"] == "2026-07-30"


def test_replay_rejects_raw_objects_from_multiple_load_date_partitions():
    raw_store = MemoryRawObjectStore()
    first_key = (
        "raw/weather_forecast/kma_vilage_fcst/load_date=2026-07-30/nx=60/ny=127/"
        "20260730T235959KST_base-202607300800_request-1.json"
    )
    second_key = (
        "raw/weather_forecast/kma_vilage_fcst/load_date=2026-07-31/nx=61/ny=127/"
        "20260731T000001KST_base-202607300800_request-2.json"
    )
    raw_store.write_bytes(
        first_key,
        kma_payload(
            total_count=1,
            item_count=1,
            base_date="20260730",
            nx=60,
        ),
        "application/json",
    )
    raw_store.write_bytes(
        second_key,
        kma_payload(
            total_count=1,
            item_count=1,
            base_date="20260730",
            nx=61,
        ),
        "application/json",
    )
    landing = KmaLanding(
        source=ScriptedKmaSource({}),
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 31, tzinfo=timezone.utc),
        request_id=lambda: "unused",
    )

    with pytest.raises(WeatherSourceSchemaError, match="one landing_load_date"):
        landing.replay(
            [first_key, second_key],
            grids=(KmaGrid("first", 60, 127), KmaGrid("second", 61, 127)),
            run=RunIdentity("weather_vilage_fcst_backfill", "manual__mixed"),
        )


def test_collect_preserves_kma_raw_lineage_for_one_grid_page():
    payload = kma_payload(total_count=1, item_count=1)
    source = ScriptedKmaSource({(60, 127, 1): payload})
    raw_store = MemoryRawObjectStore()
    landing = KmaLanding(
        source=source,
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: "request-1",
    )

    batch = landing.collect(
        RunIdentity(dag_id="weather_vilage_fcst_bronze", run_id="scheduled__weather"),
        KmaLandingRequest(
            base_date="20260714",
            base_time="0800",
            grids=(KmaGrid(place_id="seoul", nx=60, ny=127),),
            num_of_rows=1000,
        ),
    )

    assert batch.base_date == "20260714"
    assert batch.base_time == "0800"
    assert batch.grid_count == 1
    assert batch.api_request_count == 1
    assert batch.reused_raw_object_count == 0
    assert len(batch.raw_objects) == 1
    raw_object = batch.raw_objects[0]
    expected_run_prefix = (
        "raw/weather/kma_vilage_fcst/load_date=2026-07-14/"
        "run_id=scheduled__weather"
    )
    assert raw_object.raw_object_key == (
        f"{expected_run_prefix}/nx=60/ny=127/"
        "20260714T092000KST_base-202607140800_request-1.json"
    )
    assert raw_object.request_id == "request-1"
    assert raw_object.place_id == "seoul"
    assert raw_object.nx == 60
    assert raw_object.ny == 127
    assert raw_object.page_no == 1
    assert raw_object.payload_hash == hashlib.sha256(payload).hexdigest()
    assert raw_store.read_bytes(raw_object.raw_object_key) == payload
    assert (
        raw_store.objects[raw_object.raw_object_key][1]
        == "application/json; charset=utf-8"
    )
    assert "KMA_SERVICE_KEY" not in raw_object.raw_object_key
    assert batch.manifest_key == f"{expected_run_prefix}/_manifest.json"
    assert raw_store.write_order[-1] == batch.manifest_key
    assert json.loads(raw_store.read_bytes(batch.manifest_key)) == {
        "run_id": "scheduled__weather",
        "dataset": "kma_vilage_fcst",
        "load_date": "2026-07-14",
        "object_keys": [raw_object.raw_object_key],
        "expected_count": 1,
        "actual_count": 1,
        "completed_at": "2026-07-14T09:20:00+09:00",
        "status": "complete",
    }


def test_collect_fetches_all_pages_for_every_configured_grid():
    source = ScriptedKmaSource(
        {
            (60, 127, 1): kma_payload(total_count=1001, item_count=1000, page_no=1),
            (60, 127, 2): kma_payload(total_count=1001, item_count=1, page_no=2),
                (61, 127, 1): kma_payload(
                    total_count=1,
                    item_count=1,
                    page_no=1,
                    nx=61,
                ),
        }
    )
    request_ids = iter(("request-1", "request-2", "request-3"))
    landing = KmaLanding(
        source=source,
        raw_store=MemoryRawObjectStore(),
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: next(request_ids),
    )

    batch = landing.collect(
        RunIdentity(dag_id="weather_vilage_fcst_bronze", run_id="manual__pagination"),
        KmaLandingRequest(
            base_date="20260714",
            base_time="0800",
            grids=(
                KmaGrid(place_id="first", nx=60, ny=127),
                KmaGrid(place_id="second", nx=61, ny=127),
            ),
            num_of_rows=1000,
        ),
    )

    assert source.requests == [(60, 127, 1), (60, 127, 2), (61, 127, 1)]
    assert [(item.nx, item.page_no) for item in batch.raw_objects] == [
        (60, 1),
        (60, 2),
        (61, 1),
    ]
    assert [item.page_count for item in batch.raw_objects] == [2, 2, 1]
    assert batch.grid_count == 2
    assert batch.api_request_count == 3


def test_collect_resumes_from_page_checkpoint_after_mid_grid_failure():
    source = ScriptedKmaSource(
        {(60, 127, 1): kma_payload(total_count=1001, item_count=1000, page_no=1)}
    )
    raw_store = MemoryRawObjectStore()
    request_ids = iter(("request-1", "failed-request", "request-2"))
    landing = KmaLanding(
        source=source,
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: next(request_ids),
    )
    run = RunIdentity(dag_id="weather_vilage_fcst_bronze", run_id="manual__resume")
    request = KmaLandingRequest(
        base_date="20260714",
        base_time="0800",
        grids=(KmaGrid(place_id="seoul", nx=60, ny=127),),
        num_of_rows=1000,
    )

    with pytest.raises(KeyError):
        landing.collect(run, request)

    source.pages[(60, 127, 2)] = kma_payload(total_count=1001, item_count=1, page_no=2)
    source.requests.clear()
    batch = landing.collect(run, request)

    assert source.requests == [(60, 127, 2)]
    assert [(item.page_no, item.row_count) for item in batch.raw_objects] == [
        (1, 1000),
        (2, 1),
    ]
    assert batch.api_request_count == 1
    assert batch.reused_raw_object_count == 1


@pytest.mark.parametrize("mutation", ["missing", "corrupt"])
def test_collect_recollects_checkpoint_page_when_raw_object_is_not_trustworthy(
    mutation,
):
    original = kma_payload(total_count=1, item_count=1)
    replacement = kma_payload(total_count=1, item_count=1)
    raw_store = MemoryRawObjectStore()
    run = RunIdentity("weather_vilage_fcst_bronze", "manual__repair-checkpoint")
    request = KmaLandingRequest(
        base_date="20260714",
        base_time="0800",
        grids=(KmaGrid("seoul", 60, 127),),
        num_of_rows=1000,
    )
    first = KmaLanding(
        source=ScriptedKmaSource({(60, 127, 1): original}),
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: "request-1",
    ).collect(run, request)
    raw_key = first.raw_objects[0].raw_object_key
    if mutation == "missing":
        del raw_store.objects[raw_key]
    else:
        raw_store.objects[raw_key] = (b"corrupt", "application/json")

    retry_source = ScriptedKmaSource({(60, 127, 1): replacement})
    repaired = KmaLanding(
        source=retry_source,
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 21, tzinfo=timezone.utc),
        request_id=lambda: "request-2",
    ).collect(run, request)

    assert retry_source.requests == [(60, 127, 1)]
    assert repaired.reused_raw_object_count == 0
    assert repaired.raw_objects[0].request_id == "request-2"
    assert (
        repaired.raw_objects[0].payload_hash == hashlib.sha256(replacement).hexdigest()
    )


def test_collect_fails_loudly_when_grid_pages_do_not_cover_reported_total():
    source = ScriptedKmaSource(
        {
            (60, 127, 1): kma_payload(total_count=1001, item_count=1000, page_no=1),
            (60, 127, 2): kma_payload(total_count=1001, item_count=0, page_no=2),
        }
    )
    request_ids = iter(("request-1", "request-2"))
    landing = KmaLanding(
        source=source,
        raw_store=MemoryRawObjectStore(),
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: next(request_ids),
    )

    with pytest.raises(
        KmaLandingIncompleteError,
        match="nx=60, ny=127, total_count=1001, parsed_rows=1000",
    ):
        landing.collect(
            RunIdentity(
                dag_id="weather_vilage_fcst_bronze", run_id="manual__incomplete"
            ),
            KmaLandingRequest(
                base_date="20260714",
                base_time="0800",
                grids=(KmaGrid(place_id="seoul", nx=60, ny=127),),
                num_of_rows=1000,
            ),
        )


def test_replay_deduplicates_raw_keys_and_rebuilds_paginated_lineage():
    page_one_key = (
        "raw/weather_forecast/kma_vilage_fcst/load_date=2026-07-14/nx=60/ny=127/"
        "20260714T092000KST_base-202607140800_request-1.json"
    )
    page_two_key = (
        "raw/weather_forecast/kma_vilage_fcst/load_date=2026-07-14/nx=60/ny=127/"
        "20260714T092001KST_base-202607140800_request-2.json"
    )
    raw_store = MemoryRawObjectStore()
    raw_store.write_bytes(
        page_one_key,
        kma_payload(total_count=1001, item_count=1000, page_no=1),
        "application/json; charset=utf-8",
    )
    raw_store.write_bytes(
        page_two_key,
        kma_payload(total_count=1001, item_count=1, page_no=2),
        "application/json; charset=utf-8",
    )
    source = ScriptedKmaSource({})
    landing = KmaLanding(
        source=source,
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: "must-not-be-used",
    )

    batch = landing.replay(
        [page_one_key, page_two_key, page_one_key],
        grids=(KmaGrid(place_id="seoul", nx=60, ny=127),),
    )

    assert source.requests == []
    assert [(item.request_id, item.page_no) for item in batch.raw_objects] == [
        ("request-1", 1),
        ("request-2", 2),
    ]
    assert batch.grid_count == 1
    assert batch.is_publishable is True
    assert batch.api_request_count == 0
    assert batch.reused_raw_object_count == 2


def test_replay_marks_configured_grid_subset_nonpublishable():
    raw_key = (
        "raw/weather_forecast/kma_vilage_fcst/load_date=2026-07-14/nx=60/ny=127/"
        "20260714T092000KST_base-202607140800_request-1.json"
    )
    raw_store = MemoryRawObjectStore()
    raw_store.write_bytes(
        raw_key,
        kma_payload(total_count=1, item_count=1),
        "application/json; charset=utf-8",
    )
    landing = KmaLanding(
        source=ScriptedKmaSource({}),
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: "unused",
    )

    batch = landing.replay(
        [raw_key],
        grids=(
            KmaGrid("first", 60, 127),
            KmaGrid("second", 61, 127),
        ),
    )

    assert batch.grid_count == 1
    assert batch.is_publishable is False
    assert batch.to_xcom()["is_publishable"] is False
    assert batch.base_date == "20260714"
    assert batch.base_time == "0800"


def test_replay_rejects_duplicate_page_numbers_that_mask_a_missing_page():
    first_key = (
        "raw/weather_forecast/kma_vilage_fcst/load_date=2026-07-14/nx=60/ny=127/"
        "20260714T092000KST_base-202607140800_request-1.json"
    )
    duplicate_key = (
        "raw/weather_forecast/kma_vilage_fcst/load_date=2026-07-14/nx=60/ny=127/"
        "20260714T092001KST_base-202607140800_request-2.json"
    )
    page_one = kma_payload(total_count=2, item_count=1, page_no=1, num_of_rows=1)
    raw_store = MemoryRawObjectStore()
    raw_store.write_bytes(first_key, page_one, "application/json")
    raw_store.write_bytes(duplicate_key, page_one, "application/json")
    landing = KmaLanding(
        source=ScriptedKmaSource({}),
        raw_store=raw_store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, tzinfo=timezone.utc),
        request_id=lambda: "unused",
    )

    with pytest.raises(KmaLandingIncompleteError, match="expected_pages"):
        landing.replay(
            [first_key, duplicate_key],
            grids=(KmaGrid("seoul", 60, 127),),
        )


def test_landing_batch_round_trips_through_airflow_xcom_mapping():
    landing = KmaLanding(
        source=ScriptedKmaSource(
            {(60, 127, 1): kma_payload(total_count=1, item_count=1)}
        ),
        raw_store=MemoryRawObjectStore(),
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: "request-1",
    )
    batch = landing.collect(
        RunIdentity(dag_id="weather_vilage_fcst_bronze", run_id="manual__xcom"),
        KmaLandingRequest(
            base_date="20260714",
            base_time="0800",
            grids=(KmaGrid(place_id="seoul", nx=60, ny=127),),
            num_of_rows=1000,
        ),
    )

    document = batch.to_xcom()

    assert document["source_id"] == "kma_vilage_fcst"
    assert document["raw_objects"][0]["raw_hash"] == batch.raw_objects[0].payload_hash
    assert document["raw_object_keys"] == [batch.raw_objects[0].raw_object_key]
    assert document["raw_page_count"] == 1
    assert document["expected_raw_object_count"] == 1
    assert KmaLandingBatch.from_xcom(document) == batch


def test_collect_preserves_non_success_response_as_diagnostic_raw_without_manifest():
    class ErrorKmaSource:
        def fetch_page(self, **kwargs) -> tuple[int, bytes]:
            assert kwargs["nx"] == 60
            assert kwargs["ny"] == 127
            return 503, b'{"error":"upstream unavailable"}'

    raw_store = MemoryRawObjectStore()
    with pytest.raises(WeatherSourceSchemaError, match="http_status=503") as raised:
        KmaLanding(
            source=ErrorKmaSource(),
            raw_store=raw_store,
            raw_prefix="raw",
            clock=lambda: datetime(2026, 7, 31, tzinfo=timezone.utc),
            request_id=lambda: "diagnostic-1",
        ).collect(
            RunIdentity("weather_vilage_fcst_bronze", "manual__upstream-error"),
            KmaLandingRequest(
                base_date="20260714",
                base_time="0800",
                grids=(KmaGrid("seoul", 60, 127),),
                num_of_rows=1000,
            ),
        )

    raw_keys = [
        key
        for key in raw_store.objects
        if key.endswith(".json") and "/_checkpoints/" not in key
    ]
    assert len(raw_keys) == 1
    assert raw_store.read_bytes(raw_keys[0]) == b'{"error":"upstream unavailable"}'
    assert raw_keys[0] in str(raised.value)
    assert not any(key.endswith("_manifest.json") for key in raw_store.objects)


def test_collect_rejects_response_rows_that_disagree_with_the_requested_grid():
    raw_store = MemoryRawObjectStore()
    with pytest.raises(WeatherSourceSchemaError, match="response context mismatch"):
        KmaLanding(
            source=ScriptedKmaSource(
                {
                    (60, 127, 1): kma_payload(
                        total_count=1,
                        item_count=1,
                        nx=61,
                    )
                }
            ),
            raw_store=raw_store,
            raw_prefix="raw",
            clock=lambda: datetime(2026, 7, 31, tzinfo=timezone.utc),
            request_id=lambda: "context-1",
        ).collect(
            RunIdentity("weather_vilage_fcst_bronze", "manual__context-mismatch"),
            KmaLandingRequest(
                base_date="20260714",
                base_time="0800",
                grids=(KmaGrid("seoul", 60, 127),),
                num_of_rows=1000,
            ),
        )

    assert len(
        [
            key
            for key in raw_store.objects
            if key.endswith(".json") and "/_checkpoints/" not in key
        ]
    ) == 1
    assert not any(key.endswith("_manifest.json") for key in raw_store.objects)


def test_collect_rejects_divergent_payload_for_an_existing_raw_key():
    raw_key = (
        "raw/weather/kma_vilage_fcst/load_date=2026-07-31/"
        "run_id=manual__collision/nx=60/ny=127/"
        "20260731T090000KST_base-202607140800_request-1.json"
    )
    raw_store = MemoryRawObjectStore()
    raw_store.objects[raw_key] = (b'{"existing":true}', "application/json")

    with pytest.raises(WeatherRawIntegrityError, match="raw object already exists"):
        KmaLanding(
            source=ScriptedKmaSource(
                {(60, 127, 1): kma_payload(total_count=1, item_count=1)}
            ),
            raw_store=raw_store,
            raw_prefix="raw",
            clock=lambda: datetime(2026, 7, 31, tzinfo=timezone.utc),
            request_id=lambda: "request-1",
        ).collect(
            RunIdentity("weather_vilage_fcst_bronze", "manual__collision"),
            KmaLandingRequest(
                base_date="20260714",
                base_time="0800",
                grids=(KmaGrid("seoul", 60, 127),),
                num_of_rows=1000,
            ),
        )

    assert raw_store.read_bytes(raw_key) == b'{"existing":true}'
