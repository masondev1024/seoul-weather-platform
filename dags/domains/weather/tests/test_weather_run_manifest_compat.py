from __future__ import annotations

import importlib
from pathlib import Path
import subprocess
import sys


WEATHER_ROOT = Path(__file__).resolve().parents[1]
DOMAINS_ROOT = WEATHER_ROOT.parent
for import_path in (DOMAINS_ROOT, WEATHER_ROOT):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))


PUBLIC_MANIFEST_API = (
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
)


def test_deprecated_manifest_path_reexports_the_canonical_weather_api():
    compatibility = importlib.import_module("weather.bronze_run_manifest")
    canonical = importlib.import_module("weather_ingest.run_manifest")

    assert compatibility.__all__ == list(PUBLIC_MANIFEST_API)
    assert "deprecated" in (compatibility.__doc__ or "").lower()
    for name in PUBLIC_MANIFEST_API:
        assert getattr(compatibility, name) is getattr(canonical, name)


def test_deprecated_manifest_path_works_with_only_domains_on_python_path():
    script = f"""
import sys
sys.path.insert(0, {str(DOMAINS_ROOT)!r})
from weather.bronze_run_manifest import WeatherRunManifest
assert WeatherRunManifest.__module__ == "weather_ingest.run_manifest"
"""

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=DOMAINS_ROOT.parent,
        capture_output=True,
        check=False,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
