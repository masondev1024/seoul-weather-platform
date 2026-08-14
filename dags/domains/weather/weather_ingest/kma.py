import csv
import json
import math
import os
import re
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from weather_ingest.common.runtime import raw_prefix
from weather_ingest.errors import (
    WeatherBronzeConfigurationError,
    WeatherInvalidWindowError,
    WeatherSourceBusinessError,
    WeatherSourceSchemaError,
)


KMA_BASE_URL = os.environ.get(
    "KMA_BASE_URL",
    "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0",
)
KMA_BASE_TIMES = ["0200", "0500", "0800", "1100", "1400", "1700", "2000", "2300"]
KST = ZoneInfo("Asia/Seoul")

SOURCE_ID = "kma_vilage_fcst"
SOURCE_DOMAIN = "weather_forecast"
DEFAULT_GRID_CSV = (
    Path(__file__).resolve().parents[1] / "config" / "seoul_kma_grids.csv"
)
DEFAULT_EXPECTED_GRID_COUNT = 80
DEFAULT_NUM_OF_ROWS = 1000
_KMA_DATE = re.compile(r"^\d{8}$")
_KMA_TIME = re.compile(r"^\d{4}$")


def _configured_integer(name: str, value: object, *, minimum: int) -> int:
    try:
        configured = int(value)
    except (TypeError, ValueError) as exc:
        raise WeatherBronzeConfigurationError(
            f"{name} must be an integer: {value}"
        ) from exc
    if configured < minimum:
        raise WeatherBronzeConfigurationError(
            f"{name} must be at least {minimum}: {configured}"
        )
    return configured


def build_raw_object_key(
    collected_at: datetime,
    request_id: str,
    base_date: str,
    base_time: str,
    nx: int,
    ny: int,
) -> str:
    load_date = collected_at.astimezone(KST).strftime("%Y-%m-%d")
    return (
        f"{raw_prefix().rstrip('/')}/{SOURCE_DOMAIN}/{SOURCE_ID}/load_date={load_date}/"
        f"nx={nx}/ny={ny}/"
        f"{collected_at.astimezone(KST).strftime('%Y%m%dT%H%M%SKST')}"
        f"_base-{base_date}{base_time}_{request_id}.json"
    )


def expected_kma_grid_count() -> int:
    configured = os.environ.get(
        "ASK_SEOUL_KMA_EXPECTED_GRIDS",
        os.environ.get(
            "ASK_SEOUL_REPORT_EXPECTED_KMA_GRIDS", DEFAULT_EXPECTED_GRID_COUNT
        ),
    )
    return _configured_integer(
        "ASK_SEOUL_KMA_EXPECTED_GRIDS",
        configured,
        minimum=1,
    )


def load_kma_grids(
    path: str | None = None, expected_grid_count: int | None = None
) -> list[dict]:
    grid_path = Path(
        path or os.environ.get("ASK_SEOUL_KMA_GRID_CSV") or DEFAULT_GRID_CSV
    )
    grids = []
    seen = set()
    try:
        with grid_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle, strict=True)
            if not {"nx", "ny"}.issubset(reader.fieldnames or ()):
                raise KeyError("nx, ny")
            for row in reader:
                nx = int(row["nx"])
                ny = int(row["ny"])
                key = (nx, ny)
                if key in seen:
                    continue
                seen.add(key)
                grids.append(
                    {
                        "place_id": row.get("place_id") or f"kma_{nx}_{ny}",
                        "nx": nx,
                        "ny": ny,
                    }
                )
    except FileNotFoundError as exc:
        raise WeatherBronzeConfigurationError(
            f"KMA grid CSV file does not exist: {grid_path}"
        ) from exc
    except (UnicodeDecodeError, csv.Error, KeyError, TypeError, ValueError) as exc:
        raise WeatherBronzeConfigurationError(
            f"KMA grid CSV content is invalid: {grid_path}"
        ) from exc
    if not grids:
        raise WeatherBronzeConfigurationError(f"No KMA grids configured: {grid_path}")
    expected_value = (
        expected_kma_grid_count()
        if expected_grid_count is None
        else expected_grid_count
    )
    expected = _configured_integer(
        "KMA expected grid count",
        expected_value,
        minimum=1,
    )
    if len(grids) != expected:
        raise WeatherBronzeConfigurationError(
            f"KMA grid count mismatch: expected={expected}, actual={len(grids)}, path={grid_path}"
        )
    return grids


def kma_num_of_rows() -> int:
    return _configured_integer(
        "KMA_NUM_OF_ROWS",
        os.environ.get("KMA_NUM_OF_ROWS", str(DEFAULT_NUM_OF_ROWS)),
        minimum=1,
    )


def kma_page_no() -> int:
    return _configured_integer(
        "KMA_PAGE_NO",
        os.environ.get("KMA_PAGE_NO", "1"),
        minimum=1,
    )


def kma_page_count(total_count: object, num_of_rows: int | None = None) -> int:
    total = int(total_count or 0)
    if total < 0:
        raise WeatherSourceSchemaError(f"KMA total_count must be non-negative: {total}")
    rows_per_page = kma_num_of_rows() if num_of_rows is None else int(num_of_rows)
    if rows_per_page < 1:
        raise WeatherInvalidWindowError(
            f"KMA num_of_rows must be positive: {rows_per_page}"
        )
    return max(1, math.ceil(total / rows_per_page))


def kma_page_numbers(total_count: object, num_of_rows: int | None = None) -> list[int]:
    return list(range(1, kma_page_count(total_count, num_of_rows) + 1))


def request_params_json(
    base_date: str,
    base_time: str,
    nx: int,
    ny: int,
    page_no: int | None = None,
    num_of_rows: int | None = None,
) -> str:
    actual_page_no = kma_page_no() if page_no is None else int(page_no)
    actual_num_of_rows = kma_num_of_rows() if num_of_rows is None else int(num_of_rows)
    if actual_page_no < 1:
        raise WeatherInvalidWindowError(
            f"KMA page_no must be positive: {actual_page_no}"
        )
    if actual_num_of_rows < 1:
        raise WeatherInvalidWindowError(
            f"KMA num_of_rows must be positive: {actual_num_of_rows}"
        )
    return json.dumps(
        {
            "api": "getVilageFcst",
            "dataType": "JSON",
            "base_date": base_date,
            "base_time": base_time,
            "nx": nx,
            "ny": ny,
            "numOfRows": str(actual_num_of_rows),
            "pageNo": str(actual_page_no),
        },
        ensure_ascii=True,
        sort_keys=True,
    )


def normalize_kma_base_datetime(
    base_date: object, base_time: object
) -> tuple[str, str]:
    normalized_date = str(base_date or "").strip()
    normalized_time = str(base_time or "").strip()
    if not (len(normalized_date) == 8 and normalized_date.isdigit()):
        raise WeatherInvalidWindowError(
            f"base_date must be YYYYMMDD: {normalized_date}"
        )
    if normalized_time not in KMA_BASE_TIMES:
        raise WeatherInvalidWindowError(
            f"base_time must be one of {KMA_BASE_TIMES}: {normalized_time}"
        )
    try:
        datetime.strptime(normalized_date + normalized_time, "%Y%m%d%H%M")
    except ValueError as exc:
        raise WeatherInvalidWindowError(
            f"invalid KMA base datetime: {normalized_date} {normalized_time}"
        ) from exc
    return normalized_date, normalized_time


def kma_base_datetime_from_conf(conf: dict | None) -> tuple[str, str] | None:
    conf = conf or {}
    base_date = conf.get("base_date")
    base_time = conf.get("base_time")
    if base_date or base_time:
        if not base_date or not base_time:
            raise WeatherInvalidWindowError(
                "dag_run.conf base_date and base_time must be set together."
            )
        return normalize_kma_base_datetime(base_date, base_time)
    return None


def resolve_kma_base_datetime() -> tuple[str, str]:
    override_date = os.environ.get("KMA_BASE_DATE")
    override_time = os.environ.get("KMA_BASE_TIME")
    if override_date or override_time:
        if not override_date or not override_time:
            raise WeatherBronzeConfigurationError(
                "KMA_BASE_DATE and KMA_BASE_TIME must be set together."
            )
        return normalize_kma_base_datetime(override_date, override_time)

    delay_minutes = _configured_integer(
        "KMA_PUBLISH_DELAY_MINUTES",
        os.environ.get("KMA_PUBLISH_DELAY_MINUTES", "20"),
        minimum=0,
    )
    available_at = datetime.now(KST) - timedelta(minutes=delay_minutes)
    hhmm = available_at.strftime("%H%M")
    candidates = [base_time for base_time in KMA_BASE_TIMES if base_time <= hhmm]
    if candidates:
        return available_at.strftime("%Y%m%d"), candidates[-1]

    previous_day = available_at - timedelta(days=1)
    return previous_day.strftime("%Y%m%d"), KMA_BASE_TIMES[-1]


def build_kma_url(
    base_date: str,
    base_time: str,
    nx: int,
    ny: int,
    page_no: int | None = None,
    num_of_rows: int | None = None,
) -> str:
    actual_page_no = kma_page_no() if page_no is None else int(page_no)
    actual_num_of_rows = kma_num_of_rows() if num_of_rows is None else int(num_of_rows)
    if actual_page_no < 1:
        raise WeatherInvalidWindowError(
            f"KMA page_no must be positive: {actual_page_no}"
        )
    if actual_num_of_rows < 1:
        raise WeatherInvalidWindowError(
            f"KMA num_of_rows must be positive: {actual_num_of_rows}"
        )
    params = {
        "numOfRows": str(actual_num_of_rows),
        "pageNo": str(actual_page_no),
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": str(nx),
        "ny": str(ny),
    }
    query = urllib.parse.urlencode(params, safe="%")
    return f"{KMA_BASE_URL.rstrip('/')}/getVilageFcst?{query}"


if __name__ == "__main__":
    configured = load_kma_grids()
    assert len(configured) == 80
    assert configured[0] == {"place_id": "kma_56_130", "nx": 56, "ny": 130}
    assert configured[-1] == {"place_id": "kma_65_123", "nx": 65, "ny": 123}
    print(f"configured_kma_grids={len(configured)}")


def parse_kma_response(raw_bytes: bytes) -> tuple[dict, list[dict]]:
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WeatherSourceSchemaError("KMA response is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise WeatherSourceSchemaError("KMA JSON root must be an object")
    response = payload.get("response")
    if not isinstance(response, dict):
        raise WeatherSourceSchemaError("KMA response field must be an object")
    header = response.get("header")
    if not isinstance(header, dict):
        raise WeatherSourceSchemaError("KMA header field must be an object")
    result_code = header.get("resultCode")
    if not isinstance(result_code, str) or not result_code.strip():
        raise WeatherSourceSchemaError("KMA resultCode must be a non-empty string")
    result_code = result_code.strip()
    result_msg = str(header.get("resultMsg") or "")

    if result_code != "00":
        raise WeatherSourceBusinessError(
            f"KMA API returned resultCode={result_code}, resultMsg={result_msg}"
        )

    body = response.get("body")
    if not isinstance(body, dict):
        raise WeatherSourceSchemaError("KMA body field must be an object")
    total_count = _kma_nonnegative_integer(body, "totalCount")
    items = body.get("items")
    if not isinstance(items, dict):
        raise WeatherSourceSchemaError("KMA items field must be an object")
    items_node = items.get("item")
    if items_node is None:
        if total_count == 0:
            rows = []
        else:
            raise WeatherSourceSchemaError("KMA items field is missing item")
    if isinstance(items_node, dict):
        rows = [items_node]
    elif isinstance(items_node, list):
        rows = items_node
    elif items_node is not None:
        raise WeatherSourceSchemaError(
            f"Unexpected KMA item payload type: {type(items_node).__name__}"
        )
    for row_number, row in enumerate(rows, start=1):
        _validate_kma_row(row, row_number)
    if total_count == 0 and not rows:
        raise WeatherSourceSchemaError(
            "KMA response contains no forecast rows for request context validation"
        )
    if total_count == 0 and rows:
        raise WeatherSourceSchemaError("KMA totalCount=0 cannot include item data")

    metadata = {
        "result_code": result_code,
        "result_msg": result_msg,
        "total_count": total_count,
        "row_count": len(rows),
    }
    return metadata, rows


def validate_kma_response_context(
    rows: list[dict],
    *,
    base_date: str,
    base_time: str,
    nx: int,
    ny: int,
) -> None:
    """Reject a valid-shaped response that belongs to another KMA request."""

    expected = {
        "baseDate": base_date,
        "baseTime": base_time,
        "nx": nx,
        "ny": ny,
    }
    for row_number, row in enumerate(rows, start=1):
        _validate_kma_row(row, row_number)
        actual = {
            "baseDate": str(row["baseDate"]),
            "baseTime": str(row["baseTime"]),
            "nx": int(row["nx"]),
            "ny": int(row["ny"]),
        }
        for field, expected_value in expected.items():
            if actual[field] != expected_value:
                raise WeatherSourceSchemaError(
                    "KMA item response context mismatch: "
                    f"row={row_number}, field={field}, "
                    f"expected={expected_value}, actual={actual[field]}"
                )


def _kma_nonnegative_integer(body: dict, field: str) -> int:
    value = body.get(field)
    if value is None or isinstance(value, bool) or value == "":
        raise WeatherSourceSchemaError(f"KMA response is missing {field}")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise WeatherSourceSchemaError(
            f"KMA {field} must be an integer"
        ) from exc
    if parsed < 0:
        raise WeatherSourceSchemaError(f"KMA {field} must be non-negative")
    return parsed


def _kma_required_text(row: dict, field: str, row_number: int) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        raise WeatherSourceSchemaError(
            f"KMA item {row_number} is missing required field: {field}"
        )
    return value.strip()


def _validate_kma_timestamp(value: str, field: str, row_number: int) -> None:
    pattern = _KMA_DATE if field.endswith("Date") else _KMA_TIME
    format_string = "%Y%m%d" if field.endswith("Date") else "%H%M"
    if not pattern.fullmatch(value):
        raise WeatherSourceSchemaError(
            f"KMA item {row_number} has invalid {field}"
        )
    try:
        datetime.strptime(value, format_string)
    except ValueError as exc:
        raise WeatherSourceSchemaError(
            f"KMA item {row_number} has invalid {field}"
        ) from exc


def _validate_kma_row(row: object, row_number: int) -> None:
    if not isinstance(row, dict):
        raise WeatherSourceSchemaError(f"KMA item {row_number} must be an object")
    for field in ("baseDate", "baseTime", "category", "fcstDate", "fcstTime", "fcstValue"):
        value = _kma_required_text(row, field, row_number)
        if field in {"baseDate", "baseTime", "fcstDate", "fcstTime"}:
            _validate_kma_timestamp(value, field, row_number)
    for field in ("nx", "ny"):
        value = row.get(field)
        if value is None or isinstance(value, bool):
            raise WeatherSourceSchemaError(
                f"KMA item {row_number} is missing required field: {field}"
            )
        try:
            coordinate = int(value)
        except (TypeError, ValueError) as exc:
            raise WeatherSourceSchemaError(
                f"KMA item {row_number} has invalid {field}"
            ) from exc
        if coordinate <= 0:
            raise WeatherSourceSchemaError(
                f"KMA item {row_number} has invalid {field}"
            )
