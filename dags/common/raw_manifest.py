"""Raw landing completion manifest contract shared by Weather and Traffic."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any


REQUIRED_FIELDS = (
    "run_id",
    "dataset",
    "load_date",
    "object_keys",
    "expected_count",
    "actual_count",
    "completed_at",
    "status",
)

RAW_MANIFEST_STATUS_COMPLETE = "complete"
RAW_MANIFEST_STATUS_COMPLETE_WITH_VIOLATIONS = "complete_with_violations"
LEGACY_RAW_MANIFEST_STATUS_SUCCESS = "SUCCESS"

_SUPPORTED_STATUSES = frozenset(
    {
        RAW_MANIFEST_STATUS_COMPLETE,
        RAW_MANIFEST_STATUS_COMPLETE_WITH_VIOLATIONS,
        LEGACY_RAW_MANIFEST_STATUS_SUCCESS,
        "FAILED",
        "PARTIAL",
    }
)
_BRONZE_ADMISSION_STATUSES = frozenset(
    {
        RAW_MANIFEST_STATUS_COMPLETE,
        LEGACY_RAW_MANIFEST_STATUS_SUCCESS,
    }
)


def build_raw_manifest(
    *,
    run_id: str,
    dataset: str,
    load_date: str,
    object_keys: Iterable[str],
    expected_count: int,
    actual_count: int,
    completed_at: str,
    status: str = "SUCCESS",
) -> dict[str, Any]:
    keys = list(dict.fromkeys(str(key) for key in object_keys))
    if not run_id or not dataset or not load_date or not completed_at:
        raise ValueError("raw manifest identity and completion fields are required")
    if expected_count < 0 or actual_count < 0:
        raise ValueError("raw manifest counts must be non-negative")
    if actual_count != len(keys):
        raise ValueError("raw manifest actual_count must equal object_keys length")
    if status not in _SUPPORTED_STATUSES:
        raise ValueError(f"unsupported raw manifest status: {status}")
    if status in _BRONZE_ADMISSION_STATUSES and expected_count != actual_count:
        raise ValueError("complete raw manifest requires matching counts")
    datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
    return {
        "run_id": run_id,
        "dataset": dataset,
        "load_date": load_date,
        "object_keys": keys,
        "expected_count": expected_count,
        "actual_count": actual_count,
        "completed_at": completed_at,
        "status": status,
    }


def validate_raw_manifest(
    document: Mapping[str, Any],
    *,
    run_id: str,
    dataset: str,
    object_keys: Iterable[str],
) -> None:
    missing = [field for field in REQUIRED_FIELDS if field not in document]
    if missing:
        raise ValueError("raw manifest missing fields: " + ", ".join(missing))
    expected_keys = list(dict.fromkeys(str(key) for key in object_keys))
    actual_keys = list(dict.fromkeys(str(key) for key in document["object_keys"]))
    if document["run_id"] != run_id or document["dataset"] != dataset:
        raise ValueError("raw manifest run_id or dataset mismatch")
    if document["status"] not in _BRONZE_ADMISSION_STATUSES:
        raise ValueError("raw manifest is not complete")
    if actual_keys != expected_keys:
        raise ValueError("raw manifest object_keys mismatch")
    if document["actual_count"] != len(actual_keys):
        raise ValueError("raw manifest actual_count mismatch")
    if document["expected_count"] != document["actual_count"]:
        raise ValueError("raw manifest expected_count mismatch")
    datetime.fromisoformat(str(document["completed_at"]).replace("Z", "+00:00"))
