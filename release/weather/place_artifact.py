from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import unicodedata
from datetime import date
from pathlib import Path
from typing import Iterable, Mapping


MAPPING_VERSION = "kma_admin_dong_grid_20260325"
ARTIFACT_SOURCE = (
    "ASAC-DBT/domains/traffic_weather/seeds/weather/weather_place_grid_mapping.csv"
)
PLACE_ID_RE = re.compile(r"^seoul_admd_[0-9]{10}$")
REQUIRED_INPUT_FIELDS = frozenset({"place_id", "admin_dong", "gu"})


class ArtifactError(ValueError):
    """Raised when the place reference cannot satisfy the K-Skill contract."""


def normalize_location_name(value: str) -> str:
    return " ".join(unicodedata.normalize("NFC", value).strip().split())


def alias_keys(canonical: str) -> set[str]:
    compact = re.sub(r"\s+", "", normalize_location_name(canonical))
    without_je = re.sub(r"제(?=\d)", "", compact)
    return {
        compact,
        without_je,
        compact.replace(".", "·"),
        compact.replace(".", ""),
        without_je.replace(".", "·"),
        without_je.replace(".", ""),
    }


def _validated_date(value: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ArtifactError("generated_at must use YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise ArtifactError("generated_at must use YYYY-MM-DD")
    return value


def _runtime_row(row: Mapping[str, str], row_number: int) -> dict[str, str]:
    values: dict[str, str] = {}
    for field in REQUIRED_INPUT_FIELDS:
        raw_value = row.get(field)
        if not isinstance(raw_value, str):
            raise ArtifactError(f"row {row_number}: missing {field}")
        value = normalize_location_name(raw_value)
        if not value:
            raise ArtifactError(f"row {row_number}: empty {field}")
        values[field] = value
    if not PLACE_ID_RE.fullmatch(values["place_id"]):
        raise ArtifactError(f"row {row_number}: invalid place_id")
    return {
        "admin_dong": values["admin_dong"],
        "gu": values["gu"],
        "place_id": values["place_id"],
    }


def build_artifact(source_csv: Path, *, generated_at: str) -> dict[str, object]:
    generated_at = _validated_date(generated_at)
    try:
        handle = source_csv.open("r", encoding="utf-8-sig", newline="")
    except OSError as exc:
        raise ArtifactError(f"cannot read source CSV: {source_csv}") from exc

    locations: list[dict[str, str]] = []
    place_ids: set[str] = set()
    location_keys: set[tuple[str, str]] = set()
    with handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or [])
        missing = REQUIRED_INPUT_FIELDS - fieldnames
        if missing:
            raise ArtifactError(f"source CSV missing columns: {', '.join(sorted(missing))}")
        for row_number, row in enumerate(reader, start=2):
            location = _runtime_row(row, row_number)
            place_id = location["place_id"]
            location_key = (location["admin_dong"], location["gu"])
            if place_id in place_ids:
                raise ArtifactError(f"row {row_number}: duplicate place_id")
            if location_key in location_keys:
                raise ArtifactError(f"row {row_number}: duplicate admin_dong and gu")
            place_ids.add(place_id)
            location_keys.add(location_key)
            locations.append(location)

    if not locations:
        raise ArtifactError("source CSV has no locations")
    return {
        "mapping_version": MAPPING_VERSION,
        "source": ARTIFACT_SOURCE,
        "generated_at": generated_at,
        "locations": locations,
    }


def canonical_json_bytes(artifact: Mapping[str, object]) -> bytes:
    locations = artifact.get("locations")
    if not isinstance(locations, list):
        raise ArtifactError("artifact locations must be a list")
    canonical_locations: list[dict[str, str]] = []
    for index, row in enumerate(locations, start=1):
        if not isinstance(row, Mapping):
            raise ArtifactError(f"artifact location {index} must be an object")
        if set(row) != {"admin_dong", "gu", "place_id"}:
            raise ArtifactError(f"artifact location {index} has unexpected fields")
        canonical_locations.append(
            {
                "admin_dong": str(row["admin_dong"]),
                "gu": str(row["gu"]),
                "place_id": str(row["place_id"]),
            }
        )
    canonical = {
        "mapping_version": artifact.get("mapping_version"),
        "source": artifact.get("source"),
        "generated_at": artifact.get("generated_at"),
        "locations": canonical_locations,
    }
    return (
        json.dumps(canonical, ensure_ascii=False, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def population_revision(locations: Iterable[Mapping[str, str]]) -> str:
    rows = sorted(
        ([row["place_id"], row["admin_dong"], row["gu"]] for row in locations),
        key=lambda row: row[0],
    )
    payload = json.dumps(rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return f"{MAPPING_VERSION}:{hashlib.sha256(payload).hexdigest()}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the Weather K-Skill place artifact.")
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--generated-at", required=True)
    parser.add_argument("--check", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        payload = canonical_json_bytes(
            build_artifact(args.source, generated_at=args.generated_at)
        )
    except ArtifactError as exc:
        print(f"ERROR: {exc}")
        return 1
    if args.check:
        if not args.output.is_file() or args.output.read_bytes() != payload:
            print("ERROR: checked-in artifact differs from deterministic output")
            return 1
        print("Place artifact is current.")
        return 0
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(payload)
    print(f"Wrote {len(json.loads(payload)['locations'])} locations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
