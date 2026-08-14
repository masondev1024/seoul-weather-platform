import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from airflow.sdk.exceptions import AirflowFailException


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import weather_vilage_fcst_bronze as dag_module  # noqa: E402
from weather_ingest.landing import (  # noqa: E402
    KmaGrid,
    KmaLanding,
    KmaLandingRequest,
    RunIdentity,
    RawObjectIntegrityError,
)
from common.raw_manifest import build_raw_manifest  # noqa: E402


def manifest_bytes(raw_result: dict, run_id: str) -> bytes:
    raw_objects = raw_result["raw_objects"]
    return json.dumps(
        build_raw_manifest(
            run_id=run_id,
            dataset="kma_vilage_fcst",
            load_date="2026-07-05",
            object_keys=[item["raw_object_key"] for item in raw_objects],
            expected_count=len(raw_objects),
            actual_count=len(raw_objects),
            completed_at="2026-07-05T08:20:02+00:00",
        )
    ).encode()


class MemoryRawObjectStore:
    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = dict(objects or {})

    def exists(self, key: str) -> bool:
        return key in self.objects

    def read_bytes(self, key: str) -> bytes:
        return self.objects[key]

    def write_bytes(self, key: str, payload: bytes, _content_type: str) -> None:
        self.objects[key] = payload

    def write_bytes_if_absent(
        self, key: str, payload: bytes, content_type: str
    ) -> bool:
        if key in self.objects:
            return False
        self.write_bytes(key, payload, content_type)
        return True


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


def landing_for(source, store, request_ids=None):
    request_ids = request_ids or iter(("request-1", "request-2", "request-3"))
    return KmaLanding(
        source=source,
        raw_store=store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 5, 8, 20, tzinfo=timezone.utc),
        request_id=lambda: next(request_ids),
    )


class TaskInstance:
    def __init__(self, raw_result):
        self.raw_result = raw_result

    def xcom_pull(self, task_ids):
        assert task_ids == "land_kma_raw"
        return self.raw_result


def kma_payload(
    total_count: int = 1,
    item_count: int = 1,
    page_no: int = 1,
    num_of_rows: int = 1000,
    base_date: str = "20260705",
    base_time: str = "1700",
    nx: int = 56,
    ny: int = 130,
) -> bytes:
    return json.dumps(
        {
            "response": {
                "header": {"resultCode": "00", "resultMsg": "OK"},
                "body": {
                    "items": {
                        "item": [
                            {
                                "baseDate": base_date,
                                "baseTime": base_time,
                                "nx": nx,
                                "ny": ny,
                                "category": "TMP",
                                "fcstDate": base_date,
                                "fcstTime": "0900",
                                "fcstValue": str(seq),
                                "seq": seq,
                            }
                            for seq in range(item_count)
                        ]
                    },
                    "pageNo": page_no,
                    "numOfRows": num_of_rows,
                    "totalCount": total_count,
                },
            }
        }
    ).encode("utf-8")


def invalid_kma_payload() -> bytes:
    return json.dumps(
        {
            "response": {
                "header": {"resultCode": "99", "resultMsg": "SERVICE_BUSY"},
                "body": {"items": {"item": []}, "totalCount": 0},
            }
        }
    ).encode("utf-8")


def test_land_kma_raw_reuses_checkpointed_grid():
    request = KmaLandingRequest(
        base_date="20260703",
        base_time="0800",
        grids=(KmaGrid("first", 56, 130), KmaGrid("second", 57, 130)),
        num_of_rows=1000,
    )
    run = RunIdentity("weather_vilage_fcst_bronze", "manual__retry:1")
    store = MemoryRawObjectStore()

    first_source = ScriptedKmaSource(
        {
            (56, 130, 1): kma_payload(
                base_date="20260703",
                base_time="0800",
                nx=56,
                ny=130,
            )
        }
    )
    with pytest.raises(KeyError):
        landing_for(first_source, store, iter(("request-1", "request-fails"))).collect(
            run,
            request,
        )

    first_raw_key = next(
        key
        for key in store.objects
        if "/nx=56/ny=130/" in key and not key.endswith("landing.json")
    )
    retry_source = ScriptedKmaSource(
        {
            (57, 130, 1): kma_payload(
                base_date="20260703",
                base_time="0800",
                nx=57,
                ny=130,
            )
        }
    )
    result = (
        landing_for(retry_source, store, iter(("request-2",)))
        .collect(
            run,
            request,
        )
        .to_xcom()
    )

    assert retry_source.requests == [(57, 130, 1)]
    assert result["raw_object_keys"][0] == first_raw_key
    assert len(result["raw_object_keys"]) == 2
    assert result["grid_count"] == 2
    assert result["api_call_count"] == 2
    assert result["api_request_count"] == 1
    assert result["reused_raw_object_count"] == 1


def test_land_kma_raw_fetches_all_pages_when_total_count_exceeds_page_size():
    source = ScriptedKmaSource(
        {
            (56, 130, 1): kma_payload(total_count=1001, item_count=1000),
            (56, 130, 2): kma_payload(
                total_count=1001,
                item_count=1,
                page_no=2,
            ),
        }
    )
    result = (
        landing_for(source, MemoryRawObjectStore())
        .collect(
            RunIdentity("weather_vilage_fcst_bronze", "manual__pagination:1"),
            KmaLandingRequest(
                base_date="20260705",
                base_time="1700",
                grids=(KmaGrid("first", 56, 130),),
                num_of_rows=1000,
            ),
        )
        .to_xcom()
    )

    assert source.requests == [(56, 130, 1), (56, 130, 2)]
    assert [item["page_no"] for item in result["raw_objects"]] == [1, 2]
    assert [item["row_count"] for item in result["raw_objects"]] == [1000, 1]
    assert [item["total_count"] for item in result["raw_objects"]] == [1001, 1001]
    assert result["raw_page_count"] == 2
    assert result["api_request_count"] == 2


def test_land_kma_raw_object_keys_rebuilds_loader_input():
    raw_key = (
        "raw/weather_forecast/kma_vilage_fcst/load_date=2026-07-05/"
        "nx=56/ny=130/20260705T082000KST_base-202607050800_request-1.json"
    )

    store = MemoryRawObjectStore(
        {
            raw_key: kma_payload(
                total_count=1,
                item_count=1,
                page_no=1,
                num_of_rows=1000,
                base_time="0800",
            )
        }
    )
    result = (
        landing_for(ScriptedKmaSource({}), store)
        .replay(
            [raw_key],
            grids=(KmaGrid("first", 56, 130),),
        )
        .to_xcom()
    )

    assert result["api_request_count"] == 0
    assert result["expected_raw_object_count"] == 1
    assert result["raw_objects"][0]["request_id"] == "request-1"
    assert result["raw_objects"][0]["place_id"] == "first"
    assert result["raw_objects"][0]["page_no"] == 1
    assert result["raw_objects"][0]["num_of_rows"] == 1000


def test_land_kma_raw_fails_when_kma_response_result_code_is_not_ok():
    source = ScriptedKmaSource({(56, 130, 1): invalid_kma_payload()})
    with pytest.raises(RuntimeError, match="KMA API returned resultCode=99"):
        landing_for(source, MemoryRawObjectStore()).collect(
            RunIdentity("weather_vilage_fcst_bronze", "manual__retry:2"),
            KmaLandingRequest(
                base_date="20260703",
                base_time="0800",
                grids=(KmaGrid("first", 56, 130),),
                num_of_rows=1000,
            ),
        )


def test_load_kma_bronze_fails_before_insert_when_expected_page_is_missing(monkeypatch):
    payload = kma_payload(total_count=1001, item_count=1000)
    raw_object = {
        "request_id": "request-page-1",
        "raw_object_key": "raw/weather/kma/page-1.json",
        "raw_hash": hashlib.sha256(payload).hexdigest(),
        "http_status": 200,
        "collected_at": "2026-07-05T08:20:00+00:00",
        "place_id": "first",
        "base_date": "20260705",
        "base_time": "1700",
        "nx": 56,
        "ny": 130,
        "page_no": 1,
        "num_of_rows": 1000,
    }
    raw_result = {
        "raw_objects": [raw_object],
        "manifest_key": "raw/weather/kma/_manifest.json",
        "grid_count": 1,
        "api_call_count": 1,
        "base_date": "20260705",
        "base_time": "1700",
    }
    insert_calls = []

    monkeypatch.setattr(
        dag_module, "trino_cursor", lambda: (object(), "iceberg_dev", "dev")
    )
    monkeypatch.setattr(
        dag_module, "create_kma_bronze_table", lambda *_args: "iceberg_dev.dev.bronze"
    )
    monkeypatch.setattr(
        dag_module,
        "download_raw_object",
        lambda key, _log_label: (
            manifest_bytes(raw_result, "manual__load:missing-page")
            if key == raw_result["manifest_key"]
            else payload
        ),
    )
    monkeypatch.setattr(
        dag_module,
        "append_kma_bronze_row_batches_pyiceberg",
        lambda **kwargs: insert_calls.append(kwargs),
    )

    with pytest.raises(AirflowFailException, match="KMA bronze pagination incomplete"):
        dag_module.load_kma_bronze(
            ti=TaskInstance(raw_result), run_id="manual__load:missing-page"
        )

    assert insert_calls == []


def test_load_kma_bronze_rejects_downloaded_payload_hash_mismatch(monkeypatch):
    payload = kma_payload()
    raw_result = {
        "raw_objects": [
            {
                "request_id": "request-1",
                "raw_object_key": "raw/weather/page.json",
                "raw_hash": hashlib.sha256(b"different").hexdigest(),
                "http_status": 200,
                "collected_at": "2026-07-05T08:20:00+00:00",
                "place_id": "first",
                "base_date": "20260705",
                "base_time": "1700",
                "nx": 56,
                "ny": 130,
                "page_no": 1,
                "num_of_rows": 1000,
            }
        ],
        "manifest_key": "raw/weather/kma/_manifest.json",
        "grid_count": 1,
        "base_date": "20260705",
        "base_time": "1700",
    }
    monkeypatch.setattr(
        dag_module, "trino_cursor", lambda: (object(), "iceberg_dev", "dev")
    )
    monkeypatch.setattr(
        dag_module, "create_kma_bronze_table", lambda *_args: "iceberg_dev.dev.bronze"
    )
    monkeypatch.setattr(
        dag_module,
        "download_raw_object",
        lambda key, *_args: (
            manifest_bytes(raw_result, "manual__hash-mismatch")
            if key == raw_result["manifest_key"]
            else payload
        ),
    )
    monkeypatch.setattr(
        dag_module,
        "append_kma_bronze_row_batches_pyiceberg",
        lambda **_kwargs: pytest.fail(
            "mismatched raw bytes must not reach Bronze insert"
        ),
    )

    with pytest.raises(AirflowFailException, match="hash mismatch") as raised:
        dag_module.load_kma_bronze(
            ti=TaskInstance(raw_result),
            run_id="manual__hash-mismatch",
        )

    assert isinstance(raised.value.__cause__, RawObjectIntegrityError)


def test_load_kma_bronze_allows_partial_pages_when_conf_flag_set(monkeypatch):
    payload = kma_payload(total_count=1001, item_count=1000)
    raw_object = {
        "request_id": "request-page-1",
        "raw_object_key": "raw/weather/kma/page-1.json",
        "raw_hash": hashlib.sha256(payload).hexdigest(),
        "http_status": 200,
        "collected_at": "2026-07-05T08:20:00+00:00",
        "place_id": "first",
        "base_date": "20260705",
        "base_time": "1700",
        "nx": 56,
        "ny": 130,
        "page_no": 1,
        "num_of_rows": 1000,
    }
    raw_result = {
        "raw_objects": [raw_object],
        "manifest_key": "raw/weather/kma/_manifest.json",
        "grid_count": 1,
        "api_call_count": 1,
        "base_date": "20260705",
        "base_time": "1700",
    }
    insert_calls = []

    class PartialDagRun:
        conf = {"allow_partial_pages": True}

    def fake_insert_kma_bronze_row_batches(**kwargs):
        insert_calls.append(kwargs)
        return sum(len(batch["rows"]) for batch in kwargs["row_batches"])

    monkeypatch.setattr(
        dag_module, "trino_cursor", lambda: (object(), "iceberg_dev", "dev")
    )
    monkeypatch.setattr(
        dag_module, "create_kma_bronze_table", lambda *_args: "iceberg_dev.dev.bronze"
    )
    monkeypatch.setattr(
        dag_module,
        "download_raw_object",
        lambda key, _log_label: (
            manifest_bytes(raw_result, "manual__load:partial-page")
            if key == raw_result["manifest_key"]
            else payload
        ),
    )
    monkeypatch.setattr(
        dag_module,
        "append_kma_bronze_row_batches_pyiceberg",
        fake_insert_kma_bronze_row_batches,
    )

    result = dag_module.load_kma_bronze(
        ti=TaskInstance(raw_result),
        run_id="manual__load:partial-page",
        dag_run=PartialDagRun(),
    )

    assert len(insert_calls) == 1
    assert result["inserted"] == 1000
    assert result["expected_rows"] == 1001
    assert result["expected_raw_object_count"] == 1
    assert result["is_publishable"] is False


def test_load_kma_bronze_inserts_pages_after_aggregate_count_matches(monkeypatch):
    page_payloads = {
        "raw/weather/kma/page-1.json": kma_payload(
            total_count=1001,
            item_count=1000,
        ),
        "raw/weather/kma/page-2.json": kma_payload(total_count=1001, item_count=1),
    }
    raw_objects = [
        {
            "request_id": "request-page-1",
            "raw_object_key": "raw/weather/kma/page-1.json",
            "raw_hash": hashlib.sha256(
                page_payloads["raw/weather/kma/page-1.json"]
            ).hexdigest(),
            "http_status": 200,
            "collected_at": "2026-07-05T08:20:00+00:00",
            "place_id": "first",
            "base_date": "20260705",
            "base_time": "1700",
            "nx": 56,
            "ny": 130,
            "page_no": 1,
            "num_of_rows": 1000,
        },
        {
            "request_id": "request-page-2",
            "raw_object_key": "raw/weather/kma/page-2.json",
            "raw_hash": hashlib.sha256(
                page_payloads["raw/weather/kma/page-2.json"]
            ).hexdigest(),
            "http_status": 200,
            "collected_at": "2026-07-05T08:20:01+00:00",
            "place_id": "first",
            "base_date": "20260705",
            "base_time": "1700",
            "nx": 56,
            "ny": 130,
            "page_no": 2,
            "num_of_rows": 1000,
        },
    ]
    raw_result = {
        "raw_objects": raw_objects,
        "manifest_key": "raw/weather/kma/_manifest.json",
        "grid_count": 1,
        "api_call_count": 2,
        "api_request_count": 2,
        "reused_raw_object_count": 0,
        "base_date": "20260705",
        "base_time": "1700",
    }
    insert_calls = []

    def fake_download_raw_object(object_key, _log_label):
        if object_key == raw_result["manifest_key"]:
            return manifest_bytes(raw_result, "manual__load:all-pages")
        return page_payloads[object_key]

    def fake_insert_kma_bronze_row_batches(**kwargs):
        row_batches = kwargs["row_batches"]
        insert_calls.append(
            {
                "dag_run_id": kwargs["dag_run_id"],
                "delete_existing": kwargs["delete_existing"],
                "batch_count": len(row_batches),
                "row_counts": [len(batch["rows"]) for batch in row_batches],
                "page_nos": [batch["page_no"] for batch in row_batches],
            }
        )
        return sum(len(batch["rows"]) for batch in row_batches)

    monkeypatch.setattr(
        dag_module, "trino_cursor", lambda: (object(), "iceberg_dev", "dev")
    )
    monkeypatch.setattr(
        dag_module, "create_kma_bronze_table", lambda *_args: "iceberg_dev.dev.bronze"
    )
    monkeypatch.setattr(dag_module, "download_raw_object", fake_download_raw_object)
    monkeypatch.setattr(
        dag_module,
        "append_kma_bronze_row_batches_pyiceberg",
        fake_insert_kma_bronze_row_batches,
    )

    result = dag_module.load_kma_bronze(
        ti=TaskInstance(raw_result), run_id="manual__load:all-pages"
    )

    assert len(insert_calls) == 1
    assert insert_calls[0]["dag_run_id"] == "manual__load:all-pages"
    assert insert_calls[0]["delete_existing"] is True
    assert insert_calls[0]["batch_count"] == 2
    assert insert_calls[0]["row_counts"] == [1000, 1]
    assert insert_calls[0]["page_nos"] == [1, 2]
    assert result["inserted"] == 1001
    assert result["expected_rows"] == 1001
    assert result["expected_raw_object_count"] == 2
    assert result["raw_page_count"] == 2
