from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.errors import WeatherSourceSchemaError  # noqa: E402
from weather_ingest.kma import parse_kma_response  # noqa: E402


def _item(**overrides: object) -> dict[str, object]:
    item = {
        "baseDate": "20260714",
        "baseTime": "0800",
        "nx": 60,
        "ny": 127,
        "category": "TMP",
        "fcstDate": "20260714",
        "fcstTime": "0900",
        "fcstValue": "25",
    }
    item.update(overrides)
    return item


def _payload(*, body: dict[str, object] | None = None) -> bytes:
    return json.dumps(
        {
            "response": {
                "header": {"resultCode": "00", "resultMsg": "OK"},
                "body": body
                if body is not None
                else {"items": {"item": [_item()]}, "totalCount": 1},
            }
        }
    ).encode("utf-8")


@pytest.mark.parametrize(
    ("payload", "field"),
    [
        (_payload(body={"items": {"item": []}}), "totalCount"),
        (_payload(body={"totalCount": 1}), "items"),
        (
            _payload(body={"items": {"item": [_item(category="")]}, "totalCount": 1}),
            "category",
        ),
        (
            _payload(body={"items": {"item": [_item(nx="west")]}, "totalCount": 1}),
            "nx",
        ),
    ],
)
def test_kma_rejects_missing_or_malformed_required_source_contract(payload, field):
    with pytest.raises(WeatherSourceSchemaError, match=field):
        parse_kma_response(payload)


def test_kma_accepts_additive_fields_and_normalizes_total_count():
    metadata, rows = parse_kma_response(
        _payload(
            body={
                "items": {"item": [_item(new_optional_field="kept-upstream")]},
                "totalCount": "1",
            }
        )
    )

    assert metadata == {
        "result_code": "00",
        "result_msg": "OK",
        "total_count": 1,
        "row_count": 1,
    }
    assert rows[0]["new_optional_field"] == "kept-upstream"


def test_kma_rejects_zero_count_without_context_bearing_forecast_rows():
    with pytest.raises(WeatherSourceSchemaError, match="no forecast rows"):
        parse_kma_response(
            _payload(body={"items": {"item": []}, "totalCount": 0})
        )
