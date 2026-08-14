"""Preparation and atomic persistence of one KMA Bronze Airflow run."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from weather_ingest.kma import (
    SOURCE_ID,
    kma_num_of_rows,
    kma_page_numbers,
    parse_kma_response,
    validate_kma_response_context,
)
from common.raw_manifest import validate_raw_manifest
from weather_ingest.landing import verify_raw_payload_hash
from weather_ingest.errors import WeatherCompletenessError, WeatherSourceSchemaError
from weather_ingest.raw_contract import raw_object_page_no


@dataclass(frozen=True)
class BronzeLoadPorts:
    """Runtime boundaries supplied by the Airflow entrypoint."""

    open_trino: Callable[[], tuple[Any, str, str]]
    ensure_table: Callable[[Any, str, str], str]
    download: Callable[[str, str], bytes]
    append_batches: Callable[..., int]


def load_kma_bronze_batch(
    *,
    raw_result: dict,
    dag_run_id: str,
    allow_partial_pages: bool,
    expected_raw_object_count_key: str,
    ports: BronzeLoadPorts,
) -> dict:
    raw_objects = raw_result.get("raw_objects") or []
    if not raw_objects:
        raise WeatherCompletenessError(
            "KMA raw landing result is empty; cannot load bronze rows."
        )
    try:
        manifest_object_keys = [str(item["raw_object_key"]) for item in raw_objects]
    except (KeyError, TypeError) as exc:
        raise WeatherCompletenessError(
            "KMA raw landing result is missing raw_object_key"
        ) from exc
    if len(manifest_object_keys) != len(set(manifest_object_keys)):
        raise WeatherCompletenessError(
            "KMA raw landing result contains duplicate raw_object_key values"
        )
    manifest_key = str(raw_result.get("manifest_key") or "")
    if not manifest_key:
        raise WeatherCompletenessError(
            "KMA raw landing manifest is missing; cannot load bronze rows."
        )
    try:
        manifest = json.loads(ports.download(manifest_key, "KMA raw landing manifest"))
        validate_raw_manifest(
            manifest,
            run_id=dag_run_id,
            dataset=SOURCE_ID,
            object_keys=manifest_object_keys,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise WeatherCompletenessError(
            "KMA raw landing manifest validation failed"
        ) from exc
    page_identities = [
        (
            str(item["base_date"]),
            str(item["base_time"]),
            int(item["nx"]),
            int(item["ny"]),
            raw_object_page_no(item),
        )
        for item in raw_objects
    ]
    if len(page_identities) != len(set(page_identities)):
        raise WeatherCompletenessError(
            "KMA raw landing result contains duplicate page identity"
        )
    parsed_pages = []
    grid_summaries = {}
    raw_object_keys = []
    for raw_object in raw_objects:
        http_status = raw_object["http_status"]
        if not isinstance(http_status, int) or isinstance(http_status, bool):
            raise WeatherSourceSchemaError(
                f"KMA raw object has invalid http_status: {http_status!r}"
            )
        if not 200 <= http_status < 300:
            raise WeatherSourceSchemaError(
                f"KMA raw object is not HTTP-successful: http_status={http_status}"
            )
        raw_bytes = ports.download(raw_object["raw_object_key"], "KMA raw payload")
        verify_raw_payload_hash(
            raw_bytes,
            expected_hash=str(raw_object["raw_hash"]),
            raw_object_key=str(raw_object["raw_object_key"]),
        )
        metadata, rows = parse_kma_response(raw_bytes)
        validate_kma_response_context(
            rows,
            base_date=str(raw_object["base_date"]),
            base_time=str(raw_object["base_time"]),
            nx=int(raw_object["nx"]),
            ny=int(raw_object["ny"]),
        )
        metadata = dict(metadata)
        page_no = raw_object_page_no(raw_object)
        num_of_rows = int(raw_object.get("num_of_rows") or kma_num_of_rows())
        metadata["page_no"] = page_no
        metadata["num_of_rows"] = num_of_rows
        total_count = int(metadata["total_count"])
        grid_key = (
            raw_object["base_date"],
            raw_object["base_time"],
            int(raw_object["nx"]),
            int(raw_object["ny"]),
        )
        summary = grid_summaries.setdefault(
            grid_key,
            {
                "total_count": total_count,
                "parsed_rows": 0,
                "pages": set(),
                "num_of_rows": num_of_rows,
            },
        )
        summary["total_count"] = max(int(summary["total_count"]), total_count)
        summary["parsed_rows"] = int(summary["parsed_rows"]) + len(rows)
        summary["pages"].add(page_no)
        summary["num_of_rows"] = max(int(summary["num_of_rows"]), num_of_rows)
        collected_at = datetime.fromisoformat(raw_object["collected_at"])
        parsed_pages.append(
            {
                "raw_object": raw_object,
                "metadata": metadata,
                "rows": rows,
                "collected_at": collected_at,
                "page_no": page_no,
                "num_of_rows": num_of_rows,
                "grid_key": grid_key,
            }
        )
        raw_object_keys.append(raw_object["raw_object_key"])

    for grid_key, summary in grid_summaries.items():
        expected_pages = set(
            kma_page_numbers(summary["total_count"], summary["num_of_rows"])
        )
        parsed_rows = int(summary["parsed_rows"])
        total_count = int(summary["total_count"])
        if not expected_pages.issubset(summary["pages"]) or parsed_rows < total_count:
            base_date, base_time, nx, ny = grid_key
            message = (
                "KMA bronze pagination incomplete: "
                f"base_date={base_date}, base_time={base_time}, nx={nx}, ny={ny}, "
                f"total_count={total_count}, parsed_rows={parsed_rows}, "
                f"expected_pages={sorted(expected_pages)}, actual_pages={sorted(summary['pages'])}"
            )
            if not allow_partial_pages:
                raise WeatherCompletenessError(message)
            print(f"Loading partial pages (allow_partial_pages=true) — {message}")

    batch_inputs = []
    for page in sorted(
        parsed_pages,
        key=lambda item: (
            item["grid_key"][0],
            item["grid_key"][1],
            item["grid_key"][2],
            item["grid_key"][3],
            item["page_no"],
        ),
    ):
        raw_object = page["raw_object"]
        batch_inputs.append(
            {
                "metadata": page["metadata"],
                "rows": page["rows"],
                "request_id": raw_object["request_id"],
                "place_id": raw_object["place_id"],
                "base_date": raw_object["base_date"],
                "base_time": raw_object["base_time"],
                "nx": int(raw_object["nx"]),
                "ny": int(raw_object["ny"]),
                "raw_object_key": raw_object["raw_object_key"],
                "raw_hash": raw_object["raw_hash"],
                "http_status": int(raw_object["http_status"]),
                "collected_at": page["collected_at"],
                "page_no": page["page_no"],
                "num_of_rows": page["num_of_rows"],
            }
        )

    # Do not open a persistence boundary until every raw object has passed
    # hash, source-contract, and pagination preflight.
    cursor, catalog, schema = ports.open_trino()
    qualified_table = ports.ensure_table(cursor, catalog, schema)
    inserted = ports.append_batches(
        schema=schema,
        row_batches=batch_inputs,
        dag_run_id=dag_run_id,
        delete_existing=True,
    )
    expected_rows = sum(
        int(summary["total_count"]) for summary in grid_summaries.values()
    )
    is_publishable = (
        bool(raw_result.get("is_publishable", True))
        and not allow_partial_pages
        and inserted == expected_rows
    )
    print(
        f"Inserted {inserted} KMA rows for {len(raw_objects)} raw objects into {qualified_table}"
    )
    return {
        "source_id": SOURCE_ID,
        "raw_object_keys": raw_object_keys,
        "inserted": inserted,
        "expected_rows": expected_rows,
        "grid_count": int(raw_result.get("grid_count", len(raw_objects))),
        "api_call_count": int(raw_result.get("api_call_count", len(raw_objects))),
        "api_request_count": int(raw_result.get("api_request_count", 0)),
        "reused_raw_object_count": int(raw_result.get("reused_raw_object_count", 0)),
        "raw_page_count": len(raw_objects),
        expected_raw_object_count_key: len(raw_objects),
        "base_date": raw_result.get("base_date"),
        "base_time": raw_result.get("base_time"),
        "is_publishable": is_publishable,
    }
