"""Airflow-free validation helpers for landed KMA raw-object metadata."""

from weather_ingest.errors import WeatherSourceSchemaError


def normalize_kma_checkpoint_raw_object(item: object) -> dict:
    """Validate and normalize one raw-object mapping from a landing checkpoint."""
    if not isinstance(item, dict):
        raise TypeError("checkpoint raw object must be an object")

    def text(key: str) -> str:
        value = item[key]
        if not isinstance(value, str) or not value:
            raise TypeError(f"checkpoint {key} must be a non-empty string")
        return value

    return {
        "request_id": text("request_id"),
        "raw_object_key": text("raw_object_key"),
        "payload_hash": text("payload_hash"),
        "http_status": int(item["http_status"]),
        "collected_at": text("collected_at"),
        "place_id": text("place_id"),
        "base_date": text("base_date"),
        "base_time": text("base_time"),
        "nx": int(item["nx"]),
        "ny": int(item["ny"]),
        "page_no": int(item["page_no"]),
        "num_of_rows": int(item["num_of_rows"]),
        "total_count": int(item["total_count"]),
        "row_count": int(item["row_count"]),
        "page_count": int(item["page_count"]),
    }


def raw_object_page_no(raw_object: dict) -> int:
    """Return a positive KMA page number from a landed raw-object contract."""
    configured = raw_object.get("page_no") or 1
    try:
        page_no = int(configured)
    except (TypeError, ValueError) as exc:
        raise WeatherSourceSchemaError(
            f"KMA raw-object page_no must be an integer: {configured}"
        ) from exc
    if page_no < 1:
        raise WeatherSourceSchemaError(
            f"KMA raw-object page_no must be positive: {page_no}"
        )
    return page_no
