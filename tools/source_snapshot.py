from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path, PurePosixPath
from typing import Any, Mapping


COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")


class SnapshotError(RuntimeError):
    """Raised when a fixed-source snapshot cannot be exported safely."""


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotError(f"cannot read JSON contract {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SnapshotError(f"JSON contract must be an object: {path}")
    return value


def _git(repo: Path, *args: str, text: bool = False) -> bytes | str:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True,
            capture_output=True,
            text=text,
            encoding="utf-8" if text else None,
        )
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr if isinstance(exc.stderr, str) else exc.stderr.decode("utf-8", "replace")
        raise SnapshotError(f"git object read failed: {stderr.strip()}") from exc
    return result.stdout


def _safe_source_path(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise SnapshotError("source_path must be a non-empty string")
    path = PurePosixPath(value.replace("\\", "/"))
    if path.is_absolute() or ".." in path.parts or value.startswith("-"):
        raise SnapshotError(f"unsafe source_path: {value}")
    return path.as_posix()


def _safe_target(repo_root: Path, value: object) -> tuple[str, Path]:
    if not isinstance(value, str) or not value:
        raise SnapshotError("target_path must be a non-empty string")
    normalized = PurePosixPath(value.replace("\\", "/")).as_posix()
    target = (repo_root / Path(normalized)).resolve()
    if not target.is_relative_to(repo_root.resolve()):
        raise SnapshotError(f"target_path escapes repository root: {value}")
    return normalized, target


def _required_string(record: Mapping[str, Any], field: str, *, context: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value:
        raise SnapshotError(f"{context} needs a non-empty string {field}")
    return value


def _source_contracts(
    source_lock: dict[str, Any], source_checkouts: Mapping[str, Path]
) -> dict[str, dict[str, Any]]:
    sources = source_lock.get("sources")
    if not isinstance(sources, list):
        raise SnapshotError("source lock must contain a sources list")
    contracts: dict[str, dict[str, Any]] = {}
    for source in sources:
        if not isinstance(source, dict) or not isinstance(source.get("id"), str):
            raise SnapshotError("every source lock entry needs a string id")
        source_id = source["id"]
        if source_id in contracts:
            raise SnapshotError(f"duplicate source id: {source_id}")
        for field in ("repository", "ref", "license_status"):
            _required_string(source, field, context=f"source {source_id}")
        commit = source.get("commit")
        if not isinstance(commit, str) or not COMMIT_RE.fullmatch(commit):
            raise SnapshotError(f"source {source_id} does not use a full 40-character commit")
        checkout = source_checkouts.get(source_id)
        if checkout is None:
            raise SnapshotError(f"missing source checkout for {source_id}")
        checkout = Path(checkout).resolve()
        _git(checkout, "cat-file", "-e", f"{commit}^{{commit}}")
        contracts[source_id] = {**source, "checkout": checkout}
    return contracts


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def export_inventory(
    *,
    repo_root: Path,
    inventory_path: Path,
    source_lock_path: Path,
    source_checkouts: Mapping[str, Path],
    manifest_path: Path,
) -> list[dict[str, Any]]:
    root = repo_root.resolve()
    inventory = _load_json(inventory_path)
    source_lock = _load_json(source_lock_path)
    contracts = _source_contracts(source_lock, source_checkouts)
    entries = inventory.get("entries")
    if not isinstance(entries, list):
        raise SnapshotError("source inventory must contain an entries list")
    for index, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            raise SnapshotError(f"inventory entry {index} must be an object")
        for field in ("scope", "reason"):
            _required_string(entry, field, context=f"inventory entry {index}")

    records: list[dict[str, Any]] = []
    targets_seen: set[str] = set()
    for index, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            raise SnapshotError(f"inventory entry {index} must be an object")
        source_id = entry.get("source_id")
        if source_id not in contracts:
            raise SnapshotError(f"inventory entry {index} has unknown source_id {source_id!r}")
        contract = contracts[source_id]
        source_path = _safe_source_path(entry.get("source_path"))
        target_path, target = _safe_target(root, entry.get("target_path"))
        if target_path in targets_seen:
            raise SnapshotError(f"duplicate target_path in inventory: {target_path}")
        targets_seen.add(target_path)

        commit = contract["commit"]
        checkout = contract["checkout"]
        payload = _git(checkout, "show", f"{commit}:{source_path}")
        if not isinstance(payload, bytes):
            raise SnapshotError("internal error: git blob read was not binary")
        blob_oid = _git(checkout, "rev-parse", f"{commit}:{source_path}", text=True)
        if not isinstance(blob_oid, str):
            raise SnapshotError("internal error: git blob oid was not text")
        blob_oid = blob_oid.strip()
        checksum = _sha256(payload)
        source_evidence = {
            "record_type": "snapshot_copy",
            "source_repo": contract["repository"],
            "source_ref": contract["ref"],
            "source_commit": commit,
            "source_path": source_path,
            "source_blob_oid": blob_oid,
            "source_content_sha256": checksum,
        }
        record_type = entry.get("record_type", "snapshot_copy")
        if record_type == "derived":
            if not target.is_file():
                raise SnapshotError(f"derived target does not exist: {target_path}")
            derivation = entry.get("derivation")
            validator = entry.get("validator")
            if not isinstance(derivation, str) or not derivation:
                raise SnapshotError(f"derived inventory entry needs derivation: {target_path}")
            if not isinstance(validator, str) or not validator:
                raise SnapshotError(f"derived inventory entry needs validator: {target_path}")
            records.append(
                {
                    "record_type": "derived",
                    "derived_from": source_evidence,
                    "derivation": derivation,
                    "validator": validator,
                    "target_path": target_path,
                    "target_sha256": _sha256(target.read_bytes()),
                    "scope": entry["scope"],
                    "reason": entry["reason"],
                    "license_status": contract["license_status"],
                }
            )
            continue
        if record_type != "snapshot_copy":
            raise SnapshotError(
                f"unsupported inventory record_type {record_type!r}: {target_path}"
            )
        if target.exists() and target.read_bytes() != payload:
            raise SnapshotError(f"refusing to overwrite different target: {target_path}")
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)

        records.append(
            {
                **source_evidence,
                "target_path": target_path,
                "target_sha256": checksum,
                "scope": entry["scope"],
                "reason": entry["reason"],
                "license_status": contract["license_status"],
            }
        )

    records.sort(key=lambda record: record["target_path"])
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_text = "".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n" for record in records
    )
    manifest_path.write_text(manifest_text, encoding="utf-8", newline="\n")
    return records


def _source_mapping(values: list[str]) -> dict[str, Path]:
    mapping: dict[str, Path] = {}
    for value in values:
        source_id, separator, path = value.partition("=")
        if not separator or not source_id or not path:
            raise SnapshotError("--source-checkout must use SOURCE_ID=PATH")
        mapping[source_id] = Path(path)
    return mapping


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export fixed-commit source inventory.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--inventory", type=Path, default=Path("provenance/source-inventory.json")
    )
    parser.add_argument(
        "--source-lock", type=Path, default=Path("provenance/source-refs.lock.json")
    )
    parser.add_argument(
        "--manifest", type=Path, default=Path("provenance/source-files.jsonl")
    )
    parser.add_argument("--source-checkout", action="append", default=[])
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = args.repo_root.resolve()

    def relative_to_root(path: Path) -> Path:
        return path if path.is_absolute() else root / path

    try:
        records = export_inventory(
            repo_root=root,
            inventory_path=relative_to_root(args.inventory),
            source_lock_path=relative_to_root(args.source_lock),
            source_checkouts=_source_mapping(args.source_checkout),
            manifest_path=relative_to_root(args.manifest),
        )
    except SnapshotError as exc:
        print(f"ERROR: {exc}")
        return 1
    print(f"Exported {len(records)} fixed-source files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
