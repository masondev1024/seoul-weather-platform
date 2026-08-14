from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable


COMMON_FIELDS = frozenset(
    {
        "record_type",
        "target_path",
        "target_sha256",
        "scope",
        "reason",
        "license_status",
    }
)
TYPE_FIELDS = {
    "snapshot_copy": frozenset(
        {
            "source_repo",
            "source_ref",
            "source_commit",
            "source_path",
            "source_blob_oid",
            "source_content_sha256",
        }
    ),
    "derived": frozenset({"derived_from", "derivation", "validator"}),
    "generated": frozenset({"generator", "inputs", "parameters"}),
    "local_authored": frozenset({"owner"}),
}

MANIFEST_SELF_PATH = "provenance/source-files.jsonl"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_records(manifest_path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    records: list[dict[str, Any]] = []
    errors: list[str] = []
    if not manifest_path.is_file():
        return records, [f"manifest does not exist: {manifest_path}"]

    for line_number, raw_line in enumerate(
        manifest_path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not raw_line.strip():
            continue
        try:
            value = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            errors.append(f"line {line_number}: invalid JSON ({exc.msg})")
            continue
        if not isinstance(value, dict):
            errors.append(f"line {line_number}: record must be a JSON object")
            continue
        records.append(value)
    return records, errors


def _missing_fields(record: dict[str, Any], fields: Iterable[str]) -> list[str]:
    return sorted(field for field in fields if record.get(field) in (None, "", [], {}))


def verify_manifest(repo_root: Path, manifest_path: Path) -> list[str]:
    root = repo_root.resolve()
    records, errors = _load_records(manifest_path)
    seen_targets: set[str] = set()

    for index, record in enumerate(records, start=1):
        label = f"record {index}"
        record_type = record.get("record_type")
        if record_type not in TYPE_FIELDS:
            errors.append(f"{label}: unsupported record_type {record_type!r}")
            continue

        missing = _missing_fields(record, COMMON_FIELDS | TYPE_FIELDS[record_type])
        for field in missing:
            errors.append(f"{label}: missing required field {field}")

        target_path = record.get("target_path")
        if not isinstance(target_path, str) or not target_path:
            continue
        normalized_target = Path(target_path).as_posix()
        if normalized_target in seen_targets:
            errors.append(f"{label}: duplicate target_path {normalized_target}")
        seen_targets.add(normalized_target)

        resolved_target = (root / Path(target_path)).resolve()
        if not resolved_target.is_relative_to(root):
            errors.append(f"{label}: target_path escapes repository root")
            continue
        if not resolved_target.is_file():
            errors.append(f"{label}: target does not exist: {normalized_target}")
            continue

        actual_checksum = sha256_file(resolved_target)
        expected_checksum = record.get("target_sha256")
        if actual_checksum != expected_checksum:
            errors.append(f"{label}: target checksum mismatch: {normalized_target}")

        if record_type == "snapshot_copy":
            source_checksum = record.get("source_content_sha256")
            if source_checksum != expected_checksum:
                errors.append(
                    f"{label}: snapshot source and target checksums differ: {normalized_target}"
                )

    return errors


def uncovered_candidate_paths(
    candidate_paths: Iterable[str], records: Iterable[dict[str, Any]]
) -> list[str]:
    recorded = {
        Path(record["target_path"]).as_posix()
        for record in records
        if isinstance(record.get("target_path"), str)
    }
    candidates = {Path(path).as_posix() for path in candidate_paths}
    return sorted(candidates - recorded - {MANIFEST_SELF_PATH})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify repository provenance records.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("provenance/source-files.jsonl"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest = args.manifest
    if not manifest.is_absolute():
        manifest = args.repo_root / manifest
    errors = verify_manifest(args.repo_root, manifest)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Provenance manifest verified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
