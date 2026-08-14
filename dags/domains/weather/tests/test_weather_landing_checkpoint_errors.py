from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.errors import WeatherSourceSchemaError  # noqa: E402
from weather_ingest.landing import (  # noqa: E402
    KmaGrid,
    KmaLanding,
    KmaLandingRequest,
    RunIdentity,
)


class UnusedSource:
    def __init__(self) -> None:
        self.requests = []

    def fetch_page(self, **kwargs):
        self.requests.append(kwargs)
        raise AssertionError("malformed checkpoint must fail before KMA")


class MemoryRawObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

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


def landing_for(source, store) -> KmaLanding:
    return KmaLanding(
        source=source,
        raw_store=store,
        raw_prefix="raw",
        clock=lambda: datetime(2026, 7, 14, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: "unused",
    )


def request() -> KmaLandingRequest:
    return KmaLandingRequest(
        base_date="20260714",
        base_time="0800",
        grids=(KmaGrid("first", 60, 127),),
        num_of_rows=1000,
    )


@pytest.mark.parametrize(
    "checkpoint_payload",
    [
        b"\xff",
        b"{",
        json.dumps(
            {
                "request": {
                    "base_date": "20260714",
                    "base_time": "0800",
                    "num_of_rows": 1000,
                    "grids": [{"place_id": "first", "nx": 60, "ny": 127}],
                },
                "raw_objects": [{}],
            }
        ).encode(),
        json.dumps(
            {
                "request": {
                    "base_date": "20260714",
                    "base_time": "0800",
                    "num_of_rows": 1000,
                    "grids": [{"place_id": "first", "nx": 60, "ny": 127}],
                },
                "raw_objects": "not-a-list",
            }
        ).encode(),
        json.dumps(
            {
                "request": {
                    "base_date": "20260714",
                    "base_time": "0800",
                    "num_of_rows": 1000,
                    "grids": [{"place_id": "first", "nx": 60, "ny": 127}],
                },
                "raw_objects": [
                    {
                        "request_id": "request-1",
                        "raw_object_key": "raw/weather/page.json",
                        "payload_hash": "hash",
                        "http_status": 200,
                        "collected_at": "2026-07-14T00:20:00+00:00",
                        "place_id": "first",
                        "base_date": "20260714",
                        "base_time": "0800",
                        "nx": [],
                        "ny": 127,
                        "page_no": 1,
                        "num_of_rows": 1000,
                        "total_count": 1,
                        "row_count": 1,
                        "page_count": 1,
                    }
                ],
            }
        ).encode(),
    ],
    ids=("unicode", "json", "missing-key", "wrong-type", "field-type"),
)
def test_collect_translates_malformed_checkpoint_to_source_schema(
    checkpoint_payload,
):
    source = UnusedSource()
    raw_store = MemoryRawObjectStore()
    raw_store.objects[
        "raw/_checkpoints/kma_vilage_fcst/"
        "dag_id=weather_vilage_fcst_bronze/"
        "run_id=manual__malformed-checkpoint/"
        "base-202607140800.json"
    ] = checkpoint_payload

    with pytest.raises(WeatherSourceSchemaError, match="checkpoint"):
        landing_for(source, raw_store).collect(
            RunIdentity("weather_vilage_fcst_bronze", "manual__malformed-checkpoint"),
            request(),
        )

    assert source.requests == []


def test_collect_preserves_transient_checkpoint_read_error():
    class UnavailableStore(MemoryRawObjectStore):
        def exists(self, _key: str) -> bool:
            return True

        def read_bytes(self, _key: str) -> bytes:
            raise OSError("R2 unavailable")

    with pytest.raises(OSError, match="R2 unavailable"):
        landing_for(UnusedSource(), UnavailableStore()).collect(
            RunIdentity("weather_vilage_fcst_bronze", "manual__r2-down"),
            request(),
        )
