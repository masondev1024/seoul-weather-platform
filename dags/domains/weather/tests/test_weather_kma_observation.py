from __future__ import annotations

import json
import sys
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from jsonschema import Draft202012Validator


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.errors import (  # noqa: E402
    WeatherInvalidWindowError,
    WeatherSourceBusinessError,
    WeatherSourceSchemaError,
)
from weather_ingest.kma_observation import (  # noqa: E402
    REQUIRED_CATEGORIES,
    SOURCE_ID,
    build_kma_observation_url,
    normalize_kma_observation_rows,
    normalize_observation_slot,
    observation_call_budget,
    parse_kma_observation_response,
    request_metadata_json,
    resolve_observation_slot,
)


UTC = timezone.utc
FIXTURE = (
    Path(__file__).resolve().parents[4]
    / "contracts/weather-observation/fixtures/kma-ultra-srt-ncst-v1.json"
)
SCHEMA = FIXTURE.parents[1] / "schema/kma-ultra-srt-ncst-v1.schema.json"


def _item(category: str, value: object, **overrides: object) -> dict[str, object]:
    row = {
        "baseDate": "20260822",
        "baseTime": "1400",
        "nx": 60,
        "ny": 127,
        "category": category,
        "obsrValue": value,
    }
    row.update(overrides)
    return row


def _rows() -> list[dict[str, object]]:
    values: dict[str, object] = {
        "T1H": "27.4",
        "RN1": "0",
        "UUU": "-0.8",
        "VVV": "1.1",
        "REH": "68",
        "PTY": "0",
        "VEC": "324",
        "WSD": "1.4",
    }
    return [_item(category, values[category]) for category in REQUIRED_CATEGORIES]


def _payload(rows: list[dict[str, object]] | dict[str, object] | None = None, *,
             result_code: str = "00", total_count: object | None = None) -> bytes:
    actual_rows = _rows() if rows is None else rows
    count = len(actual_rows) if isinstance(actual_rows, list) else 1
    return json.dumps(
        {
            "response": {
                "header": {"resultCode": result_code, "resultMsg": "NORMAL_SERVICE"},
                "body": {
                    "dataType": "JSON",
                    "items": {"item": actual_rows},
                    "pageNo": 1,
                    "numOfRows": 1000,
                    "totalCount": count if total_count is None else total_count,
                },
            }
        },
        separators=(",", ":"),
    ).encode()


def test_request_contract_uses_exact_endpoint_without_a_service_key() -> None:
    url = build_kma_observation_url(
        "20260822", "1400", 60, 127, base_url="https://example.test/base/"
    )
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    assert parsed.path == "/base/getUltraSrtNcst"
    assert "serviceKey" not in query
    assert query == {
        "base_date": ["20260822"],
        "base_time": ["1400"],
        "dataType": ["JSON"],
        "numOfRows": ["1000"],
        "nx": ["60"],
        "ny": ["127"],
        "pageNo": ["1"],
    }
    assert json.loads(request_metadata_json("20260822", "1400", 60, 127)) == {
        "api": "getUltraSrtNcst",
        "base_date": "20260822",
        "base_time": "1400",
        "dataType": "JSON",
        "numOfRows": "1000",
        "nx": 60,
        "ny": 127,
        "pageNo": "1",
    }


@pytest.mark.parametrize(
    ("base_date", "base_time"),
    [("20260230", "1400"), ("20260822", "1410"), ("20260822", "2400")],
)
def test_explicit_slot_rejects_invalid_dates_and_non_hourly_times(
    base_date: str, base_time: str
) -> None:
    with pytest.raises(WeatherInvalidWindowError):
        normalize_observation_slot(base_date, base_time)


def test_slot_resolution_uses_kst_delay_and_crosses_day_boundary() -> None:
    assert resolve_observation_slot(
        now=datetime(2026, 8, 22, 0, 5, tzinfo=timezone.utc).astimezone(
            timezone.utc
        ),
        publish_delay_minutes=10,
    ) == ("20260822", "0800")

    # 00:05 KST minus ten minutes resolves to 23:00 on the previous KST day.
    assert resolve_observation_slot(
        now=datetime(2026, 8, 21, 15, 5, tzinfo=UTC),
        publish_delay_minutes=10,
    ) == ("20260821", "2300")


@pytest.mark.parametrize("bad", [True, 0, -1, "not-an-int"])
def test_request_paging_rejects_bool_nonpositive_and_malformed_values(bad: object) -> None:
    with pytest.raises(WeatherInvalidWindowError):
        build_kma_observation_url("20260822", "1400", 60, 127, page_no=bad)
    with pytest.raises(WeatherInvalidWindowError):
        build_kma_observation_url("20260822", "1400", 60, 127, num_of_rows=bad)


def test_parser_accepts_list_and_single_item_envelopes() -> None:
    metadata, rows = parse_kma_observation_response(_payload())
    assert metadata["result_code"] == "00"
    assert metadata["row_count"] == 8
    assert len(rows) == 8

    metadata, rows = parse_kma_observation_response(_payload(_item("T1H", "27.4")))
    assert metadata["row_count"] == 1
    assert rows[0]["category"] == "T1H"


@pytest.mark.parametrize(
    "payload",
    [
        b"\xff",
        b"[]",
        json.dumps({"response": {"header": []}}).encode(),
        json.dumps({"response": {"header": {"resultCode": "00"}, "body": []}}).encode(),
        _payload([], total_count=0),
        _payload(_rows(), total_count=7),
    ],
)
def test_parser_fails_closed_on_invalid_envelopes_and_counts(payload: bytes) -> None:
    with pytest.raises(WeatherSourceSchemaError):
        parse_kma_observation_response(payload)


def test_parser_raises_typed_business_error_without_provider_message() -> None:
    with pytest.raises(WeatherSourceBusinessError, match="resultCode=22") as raised:
        parse_kma_observation_response(_payload(result_code="22"))
    assert "NORMAL_SERVICE" not in str(raised.value)


def test_normalization_validates_context_completeness_duplicates_and_unknowns() -> None:
    collected_at = datetime(2026, 8, 22, 5, 15, tzinfo=UTC)
    _, rows = parse_kma_observation_response(_payload())
    records = normalize_kma_observation_rows(
        rows,
        base_date="20260822",
        base_time="1400",
        nx=60,
        ny=127,
        collected_at=collected_at,
        payload_sha256="a" * 64,
    )
    assert tuple(record.category for record in records) == REQUIRED_CATEGORIES
    assert {record.source_id for record in records} == {SOURCE_ID}
    assert {record.observed_at for record in records} == {
        datetime(2026, 8, 22, 5, 0, tzinfo=UTC)
    }

    cases = [
        [_item("T1H", "27.4", nx=61), *_rows()[1:]],
        _rows()[:-1],
        [*_rows(), _item("T1H", "27.4")],
        [*_rows(), _item("EXTRA", "1")],
    ]
    for bad_rows in cases:
        with pytest.raises(WeatherSourceSchemaError):
            normalize_kma_observation_rows(
                bad_rows,
                base_date="20260822",
                base_time="1400",
                nx=60,
                ny=127,
                collected_at=collected_at,
                payload_sha256="a" * 64,
            )


@pytest.mark.parametrize(
    ("category", "value"),
    [("T1H", "NaN"), ("RN1", "-"), ("WSD", "Infinity"), ("PTY", "4"), ("PTY", "1.5")],
)
def test_normalization_rejects_sentinels_nonfinite_and_invalid_pty(
    category: str, value: str
) -> None:
    rows = _rows()
    target = next(index for index, row in enumerate(rows) if row["category"] == category)
    rows[target] = _item(category, value)
    with pytest.raises(WeatherSourceSchemaError):
        normalize_kma_observation_rows(
            rows,
            base_date="20260822",
            base_time="1400",
            nx=60,
            ny=127,
            collected_at=datetime(2026, 8, 22, 5, 15, tzinfo=UTC),
            payload_sha256="a" * 64,
        )


def test_nominal_budget_is_explicit_and_preserves_quota_headroom() -> None:
    assert observation_call_budget(grid_count=80, observation_slots_per_day=24,
                                   forecast_slots_per_day=8) == {
        "observation_calls_per_day": 1920,
        "forecast_calls_per_day": 640,
        "combined_calls_per_day": 2560,
    }


def test_checked_in_fixture_is_the_full_eight_category_contract() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(json.loads(FIXTURE.read_text(encoding="utf-8")))
    raw = FIXTURE.read_bytes()
    _, rows = parse_kma_observation_response(raw)
    records = normalize_kma_observation_rows(
        rows,
        base_date="20260822",
        base_time="1400",
        nx=60,
        ny=127,
        collected_at=datetime(2026, 8, 22, 5, 15, tzinfo=UTC),
        payload_sha256="a" * 64,
    )
    assert tuple(record.category for record in records) == REQUIRED_CATEGORIES


def test_schema_rejects_eight_rows_when_categories_are_not_exactly_once() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    duplicate_payload = deepcopy(payload)
    for row in duplicate_payload["response"]["body"]["items"]["item"]:
        row["category"] = "PTY"
        row["obsrValue"] = "0"

    assert list(Draft202012Validator(schema).iter_errors(duplicate_payload))


@pytest.mark.parametrize(
    ("category", "bad_value"),
    [("PTY", 99), ("RN1", -0.1), ("WSD", -0.1), ("REH", 101.0), ("VEC", 361.0)],
)
def test_exported_record_contract_rejects_invalid_category_domain_values(
    category: str, bad_value: float | int
) -> None:
    _, rows = parse_kma_observation_response(_payload())
    records = normalize_kma_observation_rows(
        rows,
        base_date="20260822",
        base_time="1400",
        nx=60,
        ny=127,
        collected_at=datetime(2026, 8, 22, 5, 15, tzinfo=UTC),
        payload_sha256="a" * 64,
    )
    record = next(item for item in records if item.category == category)
    with pytest.raises(WeatherSourceSchemaError):
        replace(record, value=bad_value)
