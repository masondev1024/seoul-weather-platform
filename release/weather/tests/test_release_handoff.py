from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import subprocess
from typing import Any

import pytest

from release.weather.generate_place_artifact import main as generate_main
from release.weather.validate_place_artifact import (
    HandoffError,
    artifact_git_blob_oid,
    load_handoff,
    validate_local_artifact,
    validate_upstream_targets,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
RELEASE_ROOT = REPO_ROOT / "release" / "weather"
SOURCE_CSV = REPO_ROOT / "dbt/domains/traffic_weather/seeds/weather/weather_place_grid_mapping.csv"
SNAPSHOT = RELEASE_ROOT / "snapshots" / "admin-dong-place-map.json"
KSPELL_COMMIT = "43edf3c0f1037a4e510b21de61e26965212b6620"
DBT_COMMIT = "a64292d50bd8c2a19784388828de38d2b4a8c525"
ARTIFACT_SHA256 = "d0f99bd96aff19684ede2ec8700f9bf6086a2b48208166d91a37126b147aaf23"
UPSTREAM_BLOB = "7eb741c42f7701dc1d5f879de83a98cfc94132c3"
POPULATION_REVISION = (
    "kma_admin_dong_grid_20260325:"
    "9b34417b1418be6877e614c113a18e93f078de745f763858e13ed0e896a687ea"
)
TARGET_PATHS = [
    "seoul-weather-risk/references/admin-dong-place-map.json",
    "packages/k-skill-cli/skills/seoul-weather-risk/references/admin-dong-place-map.json",
]


def _handoff_with_target_paths(target_paths: list[str]) -> dict[str, Any]:
    handoff = deepcopy(load_handoff(RELEASE_ROOT / "upstream-handoff.json"))
    handoff["kskill_runtime"]["target_paths"] = target_paths
    return handoff


def _write_target(path: Path, artifact: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(artifact)


def _link_directory(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        if os.name != "nt":
            raise
        subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(target)],
            check=True,
            capture_output=True,
        )


def test_release_handoff_tools_and_manifest_are_present() -> None:
    assert {
        "generate_place_artifact.py",
        "validate_place_artifact.py",
        "upstream-handoff.json",
    } <= {path.name for path in RELEASE_ROOT.iterdir()}


def test_handoff_manifest_pins_both_upstream_targets_and_secretless_commands() -> None:
    handoff = load_handoff(RELEASE_ROOT / "upstream-handoff.json")

    assert handoff["schema_version"] == "weather-place-artifact-handoff/v1"
    assert handoff["kskill_runtime"] == {
        "commit": KSPELL_COMMIT,
        "expected_git_blob_oid": UPSTREAM_BLOB,
        "target_paths": TARGET_PATHS,
    }
    assert handoff["weather_dbt"] == {
        "commit": DBT_COMMIT,
        "source_path": "domains/traffic_weather/seeds/weather/weather_place_grid_mapping.csv",
    }
    assert handoff["artifact"] == {
        "path": "release/weather/snapshots/admin-dong-place-map.json",
        "mapping_version": "kma_admin_dong_grid_20260325",
        "sha256": ARTIFACT_SHA256,
        "git_blob_oid": UPSTREAM_BLOB,
        "location_count": 427,
        "population_revision": POPULATION_REVISION,
    }
    assert all(command[0:2] == ["python", "-m"] for command in handoff["commands"].values())
    assert "key" not in json.dumps(handoff).casefold()
    assert "token" not in json.dumps(handoff).casefold()


def test_generator_cli_writes_and_checks_the_deterministic_427_place_snapshot(
    tmp_path: Path,
) -> None:
    output = tmp_path / "admin-dong-place-map.json"

    assert generate_main(["--source", str(SOURCE_CSV), "--as-of", "2026-08-09", "--output", str(output)]) == 0
    assert hashlib.sha256(output.read_bytes()).hexdigest() == ARTIFACT_SHA256
    assert generate_main(["--source", str(SOURCE_CSV), "--as-of", "2026-08-09", "--output", str(output), "--check"]) == 0
    assert len(json.loads(output.read_text(encoding="utf-8"))["locations"]) == 427


def test_validator_requires_both_k_skill_targets_to_match_the_same_artifact(
    tmp_path: Path,
) -> None:
    handoff = load_handoff(RELEASE_ROOT / "upstream-handoff.json")
    artifact = SNAPSHOT.read_bytes()
    upstream_root = tmp_path / "k-skill"
    for relative_path in TARGET_PATHS:
        target = upstream_root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(artifact)

    validate_local_artifact(SNAPSHOT, handoff)
    validate_upstream_targets(upstream_root, SNAPSHOT, handoff)
    assert artifact_git_blob_oid(artifact) == UPSTREAM_BLOB

    (upstream_root / TARGET_PATHS[-1]).write_text("{}\n", encoding="utf-8")
    try:
        validate_upstream_targets(upstream_root, SNAPSHOT, handoff)
    except HandoffError as exc:
        assert "target artifact differs" in str(exc)
    else:
        raise AssertionError("a partial upstream update must fail")


def test_validator_rejects_absolute_upstream_target_path(tmp_path: Path) -> None:
    artifact = SNAPSHOT.read_bytes()
    upstream_root = tmp_path / "k-skill"
    outside_target = tmp_path / "outside.json"
    safe_target = upstream_root / TARGET_PATHS[1]
    _write_target(outside_target, artifact)
    _write_target(safe_target, artifact)
    handoff = _handoff_with_target_paths([str(outside_target.resolve()), TARGET_PATHS[1]])

    with pytest.raises(HandoffError, match="unsafe k-skill target path"):
        validate_upstream_targets(upstream_root, SNAPSHOT, handoff)


def test_validator_rejects_drive_qualified_upstream_target_path(tmp_path: Path) -> None:
    artifact = SNAPSHOT.read_bytes()
    upstream_root = tmp_path / "k-skill"
    drive_qualified = "C:relative.json"
    _write_target(upstream_root / drive_qualified, artifact)
    _write_target(upstream_root / TARGET_PATHS[1], artifact)
    handoff = _handoff_with_target_paths([drive_qualified, TARGET_PATHS[1]])

    with pytest.raises(HandoffError, match="unsafe k-skill target path"):
        validate_upstream_targets(upstream_root, SNAPSHOT, handoff)


def test_validator_rejects_parent_traversal_even_when_it_stays_inside_root(
    tmp_path: Path,
) -> None:
    artifact = SNAPSHOT.read_bytes()
    upstream_root = tmp_path / "k-skill"
    traversal = "nested/../same-root.json"
    _write_target(upstream_root / traversal, artifact)
    _write_target(upstream_root / TARGET_PATHS[1], artifact)
    handoff = _handoff_with_target_paths([traversal, TARGET_PATHS[1]])

    with pytest.raises(HandoffError, match="unsafe k-skill target path"):
        validate_upstream_targets(upstream_root, SNAPSHOT, handoff)


def test_validator_rejects_target_resolving_outside_upstream_root(tmp_path: Path) -> None:
    artifact = SNAPSHOT.read_bytes()
    upstream_root = tmp_path / "k-skill"
    outside_root = tmp_path / "outside"
    outside_target = outside_root / "artifact.json"
    _write_target(outside_target, artifact)
    upstream_root.mkdir()
    _link_directory(upstream_root / "escape", outside_root)
    _write_target(upstream_root / TARGET_PATHS[1], artifact)
    handoff = _handoff_with_target_paths(["escape/artifact.json", TARGET_PATHS[1]])

    with pytest.raises(HandoffError, match="outside upstream root"):
        validate_upstream_targets(upstream_root, SNAPSHOT, handoff)
