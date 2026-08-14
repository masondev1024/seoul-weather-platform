"""Deprecated compatibility facade for the Weather run-manifest API.

New code should import :mod:`weather_ingest.run_manifest` directly.  This module
keeps the former ``weather.bronze_run_manifest`` import path stable without
owning SQL or lifecycle behavior.
"""

from __future__ import annotations

from pathlib import Path
import sys


_WEATHER_ROOT = str(Path(__file__).resolve().parent)
if _WEATHER_ROOT not in sys.path:
    sys.path.insert(0, _WEATHER_ROOT)

from weather_ingest.run_manifest import (  # noqa: E402
    MANIFEST_TABLE,
    SOURCE_ID,
    STATUS_FAILED,
    STATUS_STARTED,
    STATUS_SUCCESS,
    WeatherRun,
    WeatherRunManifest,
    create_bronze_run_manifest_table,
    failure_reason_from_context,
    record_bronze_run_event,
    sql_bool,
    sql_int,
    sql_string,
    sql_timestamp,
)

__all__ = [
    "MANIFEST_TABLE",
    "SOURCE_ID",
    "STATUS_STARTED",
    "STATUS_SUCCESS",
    "STATUS_FAILED",
    "WeatherRun",
    "WeatherRunManifest",
    "create_bronze_run_manifest_table",
    "failure_reason_from_context",
    "record_bronze_run_event",
    "sql_bool",
    "sql_int",
    "sql_string",
    "sql_timestamp",
]
