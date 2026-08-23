"""Credential-safe one-request proof for the KMA observation adapter."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WEATHER_DAGS = ROOT / "dags/domains/weather"
DAGS_ROOT = ROOT / "dags"
for import_root in (str(WEATHER_DAGS), str(DAGS_ROOT)):
    if import_root not in sys.path:
        sys.path.insert(0, import_root)

from weather_ingest.common.runtime import fetch_url  # noqa: E402
from weather_ingest.errors import (  # noqa: E402
    WeatherBronzeConfigurationError,
    WeatherInvalidWindowError,
    WeatherSourceBusinessError,
    WeatherSourceSchemaError,
)
from weather_ingest.kma_observation import (  # noqa: E402
    SOURCE_ID,
    build_kma_observation_url,
    observation_slot_utc,
    parse_and_normalize_kma_observation,
    resolve_observation_slot,
)
from weather_quality.grid_universe import load_canonical_grid_universe  # noqa: E402


UTC = timezone.utc
GRID_ID = re.compile(r"^kma_(\d+)_(\d+)$")
DEFAULT_GRID_CSV = WEATHER_DAGS / "config/seoul_kma_grids.csv"
Fetcher = Callable[[str, str], tuple[int, bytes]]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate exactly one KMA getUltraSrtNcst response without exposing values."
    )
    parser.add_argument("--grid-id", required=True)
    parser.add_argument("--grid-csv", type=Path, default=DEFAULT_GRID_CSV)
    parser.add_argument("--base-date")
    parser.add_argument("--base-time")
    parser.add_argument("--publish-delay-minutes", type=int, default=10)
    parser.add_argument("--fixture", type=Path)
    return parser


def _canonical_grid(grid_id: str, grid_csv: Path) -> tuple[int, int]:
    match = GRID_ID.fullmatch(grid_id)
    if match is None:
        raise ValueError("canonical_grid_required")
    universe = load_canonical_grid_universe(grid_csv)
    cells = {cell.grid_id: cell for cell in universe.cells}
    if grid_id not in cells:
        raise ValueError("canonical_grid_required")
    cell = cells[grid_id]
    return cell.nx, cell.ny


def _slot(args: argparse.Namespace) -> tuple[str, str]:
    if bool(args.base_date) != bool(args.base_time):
        raise ValueError("base_date_and_time_required_together")
    if args.base_date:
        # URL construction performs the strict calendar/hour validation.
        return str(args.base_date), str(args.base_time)
    return resolve_observation_slot(publish_delay_minutes=args.publish_delay_minutes)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _latency_bucket(seconds: float, *, fixture: bool) -> str:
    if fixture:
        return "fixture"
    if seconds < 1:
        return "lt_1s"
    if seconds < 3:
        return "1_to_3s"
    if seconds < 10:
        return "3_to_10s"
    return "gte_10s"


def _safe_error_code(exc: Exception) -> str:
    if isinstance(exc, WeatherSourceBusinessError):
        return "provider_business_error"
    if isinstance(
        exc,
        (WeatherSourceSchemaError, WeatherInvalidWindowError, json.JSONDecodeError),
    ) or type(exc).__name__ == "ContractError":
        return "contract_validation_failed"
    if isinstance(exc, WeatherBronzeConfigurationError):
        return "credential_missing"
    if isinstance(exc, (OSError, TimeoutError, ConnectionError)):
        return "request_failed"
    return "request_failed"


def main(argv: Sequence[str] | None = None, *, fetcher: Fetcher | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        nx, ny = _canonical_grid(args.grid_id, args.grid_csv)
        base_date, base_time = _slot(args)
        requested_at = observation_slot_utc(base_date, base_time)
        url = build_kma_observation_url(base_date, base_time, nx, ny)
        started = time.monotonic()
        if args.fixture is not None:
            raw_bytes = args.fixture.read_bytes()
            http_status = 200
            collected_at = requested_at + timedelta(minutes=15)
        else:
            if not os.environ.get("KMA_SERVICE_KEY"):
                raise RuntimeError("credential_missing")
            actual_fetcher = fetcher or fetch_url
            http_status, raw_bytes = actual_fetcher(
                url, "seoul-weather-platform-observation-smoke/1.0"
            )
            collected_at = datetime.now(UTC)
        elapsed = time.monotonic() - started
        metadata, records = parse_and_normalize_kma_observation(
            raw_bytes,
            base_date=base_date,
            base_time=base_time,
            nx=nx,
            ny=ny,
            collected_at=collected_at,
        )
        proof = {
            "category_count": len(records),
            "category_names": [record.category for record in records],
            "grid_id": args.grid_id,
            "http_status": http_status,
            "latency_bucket": _latency_bucket(elapsed, fixture=args.fixture is not None),
            "observed_slot_utc": _iso(records[0].observed_at),
            "payload_sha256": metadata["payload_sha256"],
            "requested_slot_utc": _iso(requested_at),
            "result_code": metadata["result_code"],
            "source_id": SOURCE_ID,
            "validation_status": "pass",
        }
        print(json.dumps(proof, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
        return 0
    except Exception as exc:
        code = "credential_missing" if str(exc) == "credential_missing" else _safe_error_code(exc)
        if isinstance(exc, ValueError) and str(exc) in {
            "canonical_grid_required",
            "base_date_and_time_required_together",
        }:
            code = str(exc)
        print(f"ERROR: KMA observation smoke blocked: {code}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
