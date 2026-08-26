"""Pure source contract for KMA ultra-short current observations.

This module deliberately contains no Airflow or storage integration.  It turns a
single credential-free request context and provider response into deterministic,
validated records that downstream domains can consume.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable
from zoneinfo import ZoneInfo

from weather_ingest.errors import (
    WeatherInvalidWindowError,
    WeatherSourceBusinessError,
    WeatherSourceSchemaError,
)


SOURCE_ID = "kma_ultra_srt_ncst"
SOURCE_API = "getUltraSrtNcst"
DEFAULT_KMA_BASE_URL = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0"
DEFAULT_NUM_OF_ROWS = 1000
DEFAULT_PAGE_NO = 1
DEFAULT_PUBLISH_DELAY_MINUTES = 10
KST = ZoneInfo("Asia/Seoul")
UTC = timezone.utc

REQUIRED_CATEGORIES = tuple(sorted(("T1H", "RN1", "UUU", "VVV", "REH", "PTY", "VEC", "WSD")))
CATEGORY_UNITS = {
    "T1H": "degC",
    "RN1": "mm",
    "UUU": "m/s",
    "VVV": "m/s",
    "REH": "percent",
    "PTY": "code",
    "VEC": "degree",
    "WSD": "m/s",
}
ALLOWED_PTY_CODES = frozenset({0, 1, 2, 3, 5, 6, 7})
_DATE = re.compile(r"^\d{8}$")
_HOURLY_TIME = re.compile(r"^(?:[01]\d|2[0-3])00$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_AUTHENTICATION_RESULT_CODES = frozenset({"20", "30", "31", "32"})


class KmaObservationResultError(WeatherSourceBusinessError):
    """Sanitized, deterministic KMA business-result failure."""

    classification = "business_error"

    def __init__(self, result_code: str) -> None:
        self.result_code = result_code
        super().__init__(
            f"KMA observation resultCode={result_code} "
            f"classification={self.classification}"
        )


class KmaObservationThrottleError(KmaObservationResultError):
    classification = "throttle"


class KmaObservationDailyQuotaExhausted(KmaObservationResultError):
    classification = "daily_quota_exhausted"


class KmaObservationAuthenticationError(KmaObservationResultError):
    classification = "authentication_or_permission"


class KmaObservationUnknownResultError(KmaObservationResultError):
    classification = "unknown_business_error"


@dataclass(frozen=True, slots=True)
class KmaObservationRecord:
    source_id: str
    grid_id: str
    nx: int
    ny: int
    observed_at: datetime
    category: str
    value: float | int
    unit: str
    collected_at: datetime
    payload_sha256: str
    source_revision: str

    def __post_init__(self) -> None:
        if self.source_id != SOURCE_ID:
            raise WeatherSourceSchemaError("KMA observation source_id is invalid")
        if (
            isinstance(self.nx, bool)
            or not isinstance(self.nx, int)
            or isinstance(self.ny, bool)
            or not isinstance(self.ny, int)
            or self.nx < 1
            or self.ny < 1
            or self.grid_id != f"kma_{self.nx}_{self.ny}"
        ):
            raise WeatherSourceSchemaError("KMA observation grid identity is invalid")
        if self.category not in REQUIRED_CATEGORIES:
            raise WeatherSourceSchemaError("KMA observation category is not versioned")
        if self.unit != CATEGORY_UNITS[self.category]:
            raise WeatherSourceSchemaError("KMA observation category unit is invalid")
        object.__setattr__(self, "value", _finite_value(self.category, self.value))
        for field, value in (
            ("observed_at", self.observed_at),
            ("collected_at", self.collected_at),
        ):
            if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
                raise WeatherSourceSchemaError(f"KMA observation {field} must be timezone-aware")
            object.__setattr__(self, field, value.astimezone(UTC))
        if self.collected_at < self.observed_at:
            raise WeatherSourceSchemaError("KMA observation collected_at precedes observed_at")
        if not _SHA256.fullmatch(self.payload_sha256):
            raise WeatherSourceSchemaError("KMA observation payload_sha256 is invalid")
        expected_revision = f"{SOURCE_ID}:{self.payload_sha256}"
        if self.source_revision != expected_revision:
            raise WeatherSourceSchemaError("KMA observation source_revision is invalid")


def _positive_integer(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise WeatherInvalidWindowError(f"{field} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise WeatherInvalidWindowError(f"{field} must be a positive integer") from exc
    if parsed < 1 or isinstance(value, float) and not value.is_integer():
        raise WeatherInvalidWindowError(f"{field} must be a positive integer")
    if isinstance(value, str) and str(parsed) != value.strip():
        raise WeatherInvalidWindowError(f"{field} must be a positive integer")
    return parsed


def _nonnegative_integer(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise WeatherInvalidWindowError(f"{field} must be a non-negative integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise WeatherInvalidWindowError(f"{field} must be a non-negative integer") from exc
    if parsed < 0 or isinstance(value, float) and not value.is_integer():
        raise WeatherInvalidWindowError(f"{field} must be a non-negative integer")
    return parsed


def normalize_observation_slot(base_date: object, base_time: object) -> tuple[str, str]:
    normalized_date = str(base_date or "").strip()
    normalized_time = str(base_time or "").strip()
    if not _DATE.fullmatch(normalized_date) or not _HOURLY_TIME.fullmatch(normalized_time):
        raise WeatherInvalidWindowError("KMA observation slot must be YYYYMMDD and hourly HH00")
    try:
        datetime.strptime(normalized_date + normalized_time, "%Y%m%d%H%M")
    except ValueError as exc:
        raise WeatherInvalidWindowError("KMA observation slot is not a valid datetime") from exc
    return normalized_date, normalized_time


def observation_slot_utc(base_date: object, base_time: object) -> datetime:
    date, hour = normalize_observation_slot(base_date, base_time)
    return datetime.strptime(date + hour, "%Y%m%d%H%M").replace(tzinfo=KST).astimezone(UTC)


def resolve_observation_slot(
    *,
    now: datetime | None = None,
    publish_delay_minutes: object = DEFAULT_PUBLISH_DELAY_MINUTES,
) -> tuple[str, str]:
    delay = _nonnegative_integer(publish_delay_minutes, "publish_delay_minutes")
    current = now or datetime.now(UTC)
    if not isinstance(current, datetime) or current.tzinfo is None or current.utcoffset() is None:
        raise WeatherInvalidWindowError("now must be timezone-aware")
    available = current.astimezone(KST) - timedelta(minutes=delay)
    slot = available.replace(minute=0, second=0, microsecond=0)
    return slot.strftime("%Y%m%d"), slot.strftime("%H%M")


def _request_params(
    base_date: object,
    base_time: object,
    nx: object,
    ny: object,
    *,
    page_no: object = DEFAULT_PAGE_NO,
    num_of_rows: object = DEFAULT_NUM_OF_ROWS,
) -> dict[str, str]:
    date, hour = normalize_observation_slot(base_date, base_time)
    normalized_nx = _positive_integer(nx, "nx")
    normalized_ny = _positive_integer(ny, "ny")
    normalized_page = _positive_integer(page_no, "page_no")
    normalized_rows = _positive_integer(num_of_rows, "num_of_rows")
    return {
        "base_date": date,
        "base_time": hour,
        "dataType": "JSON",
        "numOfRows": str(normalized_rows),
        "nx": str(normalized_nx),
        "ny": str(normalized_ny),
        "pageNo": str(normalized_page),
    }


def build_kma_observation_url(
    base_date: object,
    base_time: object,
    nx: object,
    ny: object,
    *,
    page_no: object = DEFAULT_PAGE_NO,
    num_of_rows: object = DEFAULT_NUM_OF_ROWS,
    base_url: str | None = None,
) -> str:
    params = _request_params(
        base_date,
        base_time,
        nx,
        ny,
        page_no=page_no,
        num_of_rows=num_of_rows,
    )
    configured_base = (base_url or os.environ.get("KMA_BASE_URL") or DEFAULT_KMA_BASE_URL).strip()
    if not configured_base:
        raise WeatherInvalidWindowError("KMA_BASE_URL must be a non-empty URL")
    return f"{configured_base.rstrip('/')}/{SOURCE_API}?{urllib.parse.urlencode(params)}"


def request_metadata_json(
    base_date: object,
    base_time: object,
    nx: object,
    ny: object,
    *,
    page_no: object = DEFAULT_PAGE_NO,
    num_of_rows: object = DEFAULT_NUM_OF_ROWS,
) -> str:
    params = _request_params(
        base_date,
        base_time,
        nx,
        ny,
        page_no=page_no,
        num_of_rows=num_of_rows,
    )
    metadata: dict[str, object] = {"api": SOURCE_API, **params}
    metadata["nx"] = int(params["nx"])
    metadata["ny"] = int(params["ny"])
    return json.dumps(metadata, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _response_integer(value: object, field: str) -> int:
    if value is None or value == "" or isinstance(value, bool):
        raise WeatherSourceSchemaError(f"KMA observation response is missing {field}")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise WeatherSourceSchemaError(f"KMA observation {field} must be an integer") from exc
    if parsed < 0 or isinstance(value, float) and not value.is_integer():
        raise WeatherSourceSchemaError(f"KMA observation {field} must be non-negative")
    return parsed


def _required_text(row: dict[str, object], field: str, row_number: int) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        raise WeatherSourceSchemaError(
            f"KMA observation item {row_number} is missing required field: {field}"
        )
    return value.strip()


def _validate_raw_row(row: object, row_number: int) -> dict[str, object]:
    if not isinstance(row, dict):
        raise WeatherSourceSchemaError(f"KMA observation item {row_number} must be an object")
    base_date = _required_text(row, "baseDate", row_number)
    base_time = _required_text(row, "baseTime", row_number)
    _required_text(row, "category", row_number)
    value = row.get("obsrValue")
    if value is None or isinstance(value, bool) or not str(value).strip():
        raise WeatherSourceSchemaError(
            f"KMA observation item {row_number} is missing required field: obsrValue"
        )
    try:
        normalize_observation_slot(base_date, base_time)
    except WeatherInvalidWindowError as exc:
        raise WeatherSourceSchemaError(
            f"KMA observation item {row_number} has invalid observation slot"
        ) from exc
    for coordinate in ("nx", "ny"):
        try:
            _positive_integer(row.get(coordinate), coordinate)
        except WeatherInvalidWindowError as exc:
            raise WeatherSourceSchemaError(
                f"KMA observation item {row_number} has invalid {coordinate}"
            ) from exc
    return row


def parse_kma_observation_response(raw_bytes: bytes) -> tuple[dict[str, object], list[dict[str, object]]]:
    if not isinstance(raw_bytes, bytes):
        raise WeatherSourceSchemaError("KMA observation response must be bytes")
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WeatherSourceSchemaError("KMA observation response is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise WeatherSourceSchemaError("KMA observation JSON root must be an object")
    response = payload.get("response")
    if not isinstance(response, dict):
        raise WeatherSourceSchemaError("KMA observation response field must be an object")
    header = response.get("header")
    if not isinstance(header, dict):
        raise WeatherSourceSchemaError("KMA observation header field must be an object")
    result_code = header.get("resultCode")
    if not isinstance(result_code, str) or not result_code.strip():
        raise WeatherSourceSchemaError("KMA observation resultCode must be a non-empty string")
    result_code = result_code.strip()
    raise_for_kma_observation_result_code(result_code)
    body = response.get("body")
    if not isinstance(body, dict):
        raise WeatherSourceSchemaError("KMA observation body field must be an object")
    total_count = _response_integer(body.get("totalCount"), "totalCount")
    items = body.get("items")
    if not isinstance(items, dict):
        raise WeatherSourceSchemaError("KMA observation items field must be an object")
    node = items.get("item")
    if isinstance(node, dict):
        rows: list[dict[str, object]] = [node]
    elif isinstance(node, list):
        rows = node
    else:
        raise WeatherSourceSchemaError("KMA observation items.item must be an object or list")
    if not rows:
        raise WeatherSourceSchemaError("KMA observation response contains no rows")
    for index, row in enumerate(rows, start=1):
        _validate_raw_row(row, index)
    if total_count != len(rows):
        raise WeatherSourceSchemaError(
            "KMA observation totalCount does not equal the returned row count"
        )
    return {
        "result_code": result_code,
        "total_count": total_count,
        "row_count": len(rows),
    }, rows


def kma_observation_result_code(raw_bytes: bytes) -> str:
    """Read only the sanitized business result code from an HTTP 2xx body."""
    if not isinstance(raw_bytes, bytes):
        raise WeatherSourceSchemaError("KMA observation response must be bytes")
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
        response = payload["response"]
        header = response["header"]
        result_code = header["resultCode"]
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise WeatherSourceSchemaError(
            "KMA observation response header is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(payload, dict) or not isinstance(response, dict) or not isinstance(
        header, dict
    ):
        raise WeatherSourceSchemaError(
            "KMA observation response header must contain objects"
        )
    if not isinstance(result_code, str) or not result_code.strip():
        raise WeatherSourceSchemaError(
            "KMA observation resultCode must be a non-empty string"
        )
    return result_code.strip()


def raise_for_kma_observation_result_code(result_code: str) -> None:
    """Classify a code without exposing provider text or credential context."""
    if result_code == "00":
        return
    if result_code == "23":
        raise KmaObservationThrottleError(result_code)
    if result_code == "22":
        raise KmaObservationDailyQuotaExhausted(result_code)
    if result_code in _AUTHENTICATION_RESULT_CODES:
        raise KmaObservationAuthenticationError(result_code)
    raise KmaObservationUnknownResultError(result_code)


def _finite_value(category: str, raw_value: object) -> float | int:
    if raw_value is None or isinstance(raw_value, bool):
        raise WeatherSourceSchemaError(f"KMA observation {category} value is missing")
    rendered = str(raw_value).strip()
    if not rendered or rendered in {"-", "--", "null", "None"}:
        raise WeatherSourceSchemaError(f"KMA observation {category} value is a sentinel")
    try:
        parsed = float(rendered)
    except (TypeError, ValueError) as exc:
        raise WeatherSourceSchemaError(
            f"KMA observation {category} value must be numeric"
        ) from exc
    if not math.isfinite(parsed):
        raise WeatherSourceSchemaError(f"KMA observation {category} value must be finite")
    if category == "PTY":
        if not parsed.is_integer() or int(parsed) not in ALLOWED_PTY_CODES:
            raise WeatherSourceSchemaError("KMA observation PTY code is invalid")
        return int(parsed)
    if category in {"RN1", "WSD"} and parsed < 0:
        raise WeatherSourceSchemaError(f"KMA observation {category} cannot be negative")
    if category == "REH" and not 0 <= parsed <= 100:
        raise WeatherSourceSchemaError("KMA observation REH must be between 0 and 100")
    if category == "VEC" and not 0 <= parsed <= 360:
        raise WeatherSourceSchemaError("KMA observation VEC must be between 0 and 360")
    return parsed


def normalize_kma_observation_rows(
    rows: Iterable[dict[str, object]],
    *,
    base_date: object,
    base_time: object,
    nx: object,
    ny: object,
    collected_at: datetime,
    payload_sha256: str,
) -> tuple[KmaObservationRecord, ...]:
    date, hour = normalize_observation_slot(base_date, base_time)
    expected_nx = _positive_integer(nx, "nx")
    expected_ny = _positive_integer(ny, "ny")
    if not isinstance(collected_at, datetime) or collected_at.tzinfo is None or collected_at.utcoffset() is None:
        raise WeatherSourceSchemaError("KMA observation collected_at must be timezone-aware")
    collected_utc = collected_at.astimezone(UTC)
    if not isinstance(payload_sha256, str) or not _SHA256.fullmatch(payload_sha256):
        raise WeatherSourceSchemaError("KMA observation payload_sha256 is invalid")
    observed_at = observation_slot_utc(date, hour)
    seen: dict[str, float | int] = {}
    for row_number, raw_row in enumerate(tuple(rows), start=1):
        row = _validate_raw_row(raw_row, row_number)
        actual_context = (
            str(row["baseDate"]),
            str(row["baseTime"]),
            int(row["nx"]),
            int(row["ny"]),
        )
        if actual_context != (date, hour, expected_nx, expected_ny):
            raise WeatherSourceSchemaError(
                f"KMA observation item {row_number} response context mismatch"
            )
        category = str(row["category"]).strip()
        if category not in REQUIRED_CATEGORIES:
            raise WeatherSourceSchemaError(
                f"KMA observation category is not versioned: {category}"
            )
        if category in seen:
            raise WeatherSourceSchemaError(f"KMA observation category is duplicated: {category}")
        seen[category] = _finite_value(category, row["obsrValue"])
    missing = sorted(set(REQUIRED_CATEGORIES) - set(seen))
    if missing:
        raise WeatherSourceSchemaError(
            f"KMA observation response is missing required categories: {','.join(missing)}"
        )
    revision = f"{SOURCE_ID}:{payload_sha256}"
    return tuple(
        KmaObservationRecord(
            source_id=SOURCE_ID,
            grid_id=f"kma_{expected_nx}_{expected_ny}",
            nx=expected_nx,
            ny=expected_ny,
            observed_at=observed_at,
            category=category,
            value=seen[category],
            unit=CATEGORY_UNITS[category],
            collected_at=collected_utc,
            payload_sha256=payload_sha256,
            source_revision=revision,
        )
        for category in REQUIRED_CATEGORIES
    )


def parse_and_normalize_kma_observation(
    raw_bytes: bytes,
    *,
    base_date: object,
    base_time: object,
    nx: object,
    ny: object,
    collected_at: datetime,
) -> tuple[dict[str, object], tuple[KmaObservationRecord, ...]]:
    metadata, rows = parse_kma_observation_response(raw_bytes)
    payload_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    records = normalize_kma_observation_rows(
        rows,
        base_date=base_date,
        base_time=base_time,
        nx=nx,
        ny=ny,
        collected_at=collected_at,
        payload_sha256=payload_sha256,
    )
    return {**metadata, "payload_sha256": payload_sha256}, records


def observation_call_budget(
    *,
    grid_count: object,
    observation_slots_per_day: object,
    forecast_slots_per_day: object,
) -> dict[str, int]:
    grids = _positive_integer(grid_count, "grid_count")
    observation_slots = _positive_integer(
        observation_slots_per_day, "observation_slots_per_day"
    )
    forecast_slots = _positive_integer(forecast_slots_per_day, "forecast_slots_per_day")
    observation_calls = grids * observation_slots
    forecast_calls = grids * forecast_slots
    return {
        "observation_calls_per_day": observation_calls,
        "forecast_calls_per_day": forecast_calls,
        "combined_calls_per_day": observation_calls + forecast_calls,
    }


__all__ = [
    "ALLOWED_PTY_CODES",
    "CATEGORY_UNITS",
    "DEFAULT_PUBLISH_DELAY_MINUTES",
    "KmaObservationAuthenticationError",
    "KmaObservationDailyQuotaExhausted",
    "KmaObservationRecord",
    "KmaObservationResultError",
    "KmaObservationThrottleError",
    "KmaObservationUnknownResultError",
    "REQUIRED_CATEGORIES",
    "SOURCE_API",
    "SOURCE_ID",
    "build_kma_observation_url",
    "kma_observation_result_code",
    "normalize_kma_observation_rows",
    "normalize_observation_slot",
    "observation_call_budget",
    "observation_slot_utc",
    "parse_and_normalize_kma_observation",
    "parse_kma_observation_response",
    "request_metadata_json",
    "raise_for_kma_observation_result_code",
    "resolve_observation_slot",
]
