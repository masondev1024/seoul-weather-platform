"""Validate a local place artifact and its two-file K-Skill handoff boundary."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

from release.weather.place_artifact import MAPPING_VERSION, population_revision


class HandoffError(ValueError):
    """Raised when a release artifact cannot satisfy the recorded handoff."""


REQUIRED_HANDOFF_FIELDS = frozenset(
    {"schema_version", "weather_dbt", "kskill_runtime", "artifact", "commands"}
)
EXPECTED_SCHEMA_VERSION = "weather-place-artifact-handoff/v1"


def artifact_git_blob_oid(payload: bytes) -> str:
    header = f"blob {len(payload)}\0".encode("ascii")
    return hashlib.sha1(header + payload).hexdigest()


def load_handoff(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HandoffError(f"cannot read handoff manifest: {path}") from exc
    if not isinstance(payload, dict) or set(payload) != REQUIRED_HANDOFF_FIELDS:
        raise HandoffError("handoff manifest fields are invalid")
    if payload.get("schema_version") != EXPECTED_SCHEMA_VERSION:
        raise HandoffError("handoff manifest schema version is invalid")
    return payload


def _artifact_payload(path: Path) -> tuple[bytes, dict[str, Any]]:
    try:
        content = path.read_bytes()
        payload = json.loads(content.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HandoffError(f"cannot read artifact: {path}") from exc
    if not isinstance(payload, dict):
        raise HandoffError("artifact must be a JSON object")
    return content, payload


def validate_local_artifact(path: Path, handoff: dict[str, Any]) -> None:
    content, payload = _artifact_payload(path)
    artifact = handoff.get("artifact")
    if not isinstance(artifact, dict):
        raise HandoffError("handoff artifact record is invalid")
    if hashlib.sha256(content).hexdigest() != artifact.get("sha256"):
        raise HandoffError("artifact SHA-256 differs from handoff")
    if artifact_git_blob_oid(content) != artifact.get("git_blob_oid"):
        raise HandoffError("artifact git blob differs from handoff")
    if payload.get("mapping_version") != MAPPING_VERSION or payload.get("mapping_version") != artifact.get("mapping_version"):
        raise HandoffError("artifact mapping version differs from handoff")
    locations = payload.get("locations")
    if not isinstance(locations, list) or len(locations) != artifact.get("location_count"):
        raise HandoffError("artifact location coverage differs from handoff")
    if population_revision(locations) != artifact.get("population_revision"):
        raise HandoffError("artifact population revision differs from handoff")


def _resolve_upstream_target(upstream_root: Path, relative_path: str) -> Path:
    posix_path = PurePosixPath(relative_path)
    windows_path = PureWindowsPath(relative_path)
    if (
        posix_path.is_absolute()
        or windows_path.is_absolute()
        or windows_path.drive
        or windows_path.root
        or ".." in posix_path.parts
        or ".." in windows_path.parts
    ):
        raise HandoffError(f"unsafe k-skill target path: {relative_path}")

    resolved_root = upstream_root.resolve()
    resolved_target = (resolved_root / relative_path).resolve()
    try:
        resolved_target.relative_to(resolved_root)
    except ValueError as exc:
        raise HandoffError(
            f"k-skill target path resolves outside upstream root: {relative_path}"
        ) from exc
    return resolved_target


def validate_upstream_targets(upstream_root: Path, artifact_path: Path, handoff: dict[str, Any]) -> None:
    validate_local_artifact(artifact_path, handoff)
    expected = artifact_path.read_bytes()
    runtime = handoff.get("kskill_runtime")
    if not isinstance(runtime, dict):
        raise HandoffError("handoff k-skill runtime record is invalid")
    target_paths = runtime.get("target_paths")
    if not isinstance(target_paths, list) or len(target_paths) != 2 or not all(isinstance(item, str) and item for item in target_paths):
        raise HandoffError("handoff must declare exactly two k-skill target paths")
    if runtime.get("expected_git_blob_oid") != artifact_git_blob_oid(expected):
        raise HandoffError("handoff upstream blob does not match local artifact")
    for relative_path in target_paths:
        target = _resolve_upstream_target(upstream_root, relative_path)
        try:
            target_bytes = target.read_bytes()
        except OSError as exc:
            raise HandoffError(f"missing upstream target: {relative_path}") from exc
        if target_bytes != expected:
            raise HandoffError(f"target artifact differs: {relative_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate the two-target, secretless K-Skill place artifact handoff."
    )
    parser.add_argument("--upstream-root", type=Path, required=True)
    parser.add_argument(
        "--artifact",
        type=Path,
        default=Path("release/weather/snapshots/admin-dong-place-map.json"),
    )
    parser.add_argument(
        "--handoff",
        type=Path,
        default=Path("release/weather/upstream-handoff.json"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        handoff = load_handoff(args.handoff)
        validate_upstream_targets(args.upstream_root, args.artifact, handoff)
    except HandoffError as exc:
        print(f"ERROR: {exc}")
        return 1
    print("Validated 427-place artifact and both K-Skill targets.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
