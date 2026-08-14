from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pytest

from release.weather.place_artifact import (
    ArtifactError,
    alias_keys,
    build_artifact,
    canonical_json_bytes,
    population_revision,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
MAPPING_VERSION = "kma_admin_dong_grid_20260325"
SOURCE = "ASAC-DBT/domains/traffic_weather/seeds/weather/weather_place_grid_mapping.csv"


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = ["place_id", "admin_dong", "gu", "unused"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_build_artifact_uses_exact_runtime_fields_and_compact_json(tmp_path: Path) -> None:
    source_csv = tmp_path / "mapping.csv"
    _write_csv(
        source_csv,
        [
            {
                "place_id": "seoul_admd_1171065000",
                "admin_dong": "잠실본동",
                "gu": "송파구",
                "unused": "not exported",
            },
            {
                "place_id": "seoul_admd_1168051000",
                "admin_dong": "신사동",
                "gu": "강남구",
                "unused": "not exported",
            },
        ],
    )

    artifact = build_artifact(source_csv, generated_at="2026-08-14")

    assert artifact == {
        "mapping_version": MAPPING_VERSION,
        "source": SOURCE,
        "generated_at": "2026-08-14",
        "locations": [
            {
                "admin_dong": "잠실본동",
                "gu": "송파구",
                "place_id": "seoul_admd_1171065000",
            },
            {
                "admin_dong": "신사동",
                "gu": "강남구",
                "place_id": "seoul_admd_1168051000",
            },
        ],
    }
    encoded = canonical_json_bytes(artifact)
    assert encoded.endswith(b"\n")
    assert b" " not in encoded
    assert json.loads(encoded) == artifact


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        (
            [
                {
                    "place_id": "seoul_admd_1171065000",
                    "admin_dong": "잠실본동",
                    "gu": "송파구",
                    "unused": "",
                },
                {
                    "place_id": "seoul_admd_1171065000",
                    "admin_dong": "잠실2동",
                    "gu": "송파구",
                    "unused": "",
                },
            ],
            "duplicate place_id",
        ),
        (
            [
                {
                    "place_id": "seoul_admd_1171065000",
                    "admin_dong": "잠실본동",
                    "gu": "송파구",
                    "unused": "",
                },
                {
                    "place_id": "seoul_admd_1171067000",
                    "admin_dong": "잠실본동",
                    "gu": "송파구",
                    "unused": "",
                },
            ],
            "duplicate admin_dong and gu",
        ),
    ],
)
def test_build_artifact_rejects_duplicate_runtime_identity(
    tmp_path: Path, rows: list[dict[str, str]], message: str
) -> None:
    source_csv = tmp_path / "mapping.csv"
    _write_csv(source_csv, rows)

    with pytest.raises(ArtifactError, match=message):
        build_artifact(source_csv, generated_at="2026-08-14")


def test_alias_keys_match_upstream_deterministic_rules() -> None:
    assert {"면목제3.8동", "면목3.8동", "면목제3·8동", "면목38동"} <= alias_keys(
        "면목제3.8동"
    )
    assert "기동" not in alias_keys("제기동")


def test_population_revision_is_sorted_by_place_id() -> None:
    locations = [
        {"place_id": "seoul_admd_1171065000", "admin_dong": "잠실본동", "gu": "송파구"},
        {"place_id": "seoul_admd_1111051500", "admin_dong": "청운효자동", "gu": "종로구"},
    ]

    assert population_revision(locations) == population_revision(list(reversed(locations)))
    assert population_revision(locations).startswith(f"{MAPPING_VERSION}:")


def test_checked_in_full_artifact_matches_fixed_seed_and_upstream_hash() -> None:
    source_csv = (
        REPO_ROOT
        / "dbt/domains/traffic_weather/seeds/weather/weather_place_grid_mapping.csv"
    )
    snapshot = REPO_ROOT / "release/weather/snapshots/admin-dong-place-map.json"

    artifact = build_artifact(source_csv, generated_at="2026-08-09")
    encoded = canonical_json_bytes(artifact)

    assert len(artifact["locations"]) == 427
    assert population_revision(artifact["locations"]) == (
        "kma_admin_dong_grid_20260325:"
        "9b34417b1418be6877e614c113a18e93f078de745f763858e13ed0e896a687ea"
    )
    assert hashlib.sha256(encoded).hexdigest() == (
        "d0f99bd96aff19684ede2ec8700f9bf6086a2b48208166d91a37126b147aaf23"
    )
    assert snapshot.read_bytes() == encoded
