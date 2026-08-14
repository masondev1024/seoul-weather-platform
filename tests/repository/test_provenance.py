from __future__ import annotations

import hashlib
import json
from pathlib import Path

from tools.verify_provenance import uncovered_candidate_paths, verify_manifest


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_manifest(path: Path, records: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records),
        encoding="utf-8",
    )


def test_snapshot_copy_accepts_matching_target_checksum(tmp_path: Path) -> None:
    payload = b"select 1\n"
    target = tmp_path / "dbt" / "model.sql"
    target.parent.mkdir(parents=True)
    target.write_bytes(payload)
    manifest = tmp_path / "source-files.jsonl"
    checksum = _sha256(payload)
    _write_manifest(
        manifest,
        [
            {
                "record_type": "snapshot_copy",
                "source_repo": "owner/source",
                "source_ref": "origin/dev",
                "source_commit": "a" * 40,
                "source_path": "models/model.sql",
                "source_blob_oid": "b" * 40,
                "source_content_sha256": checksum,
                "target_path": "dbt/model.sql",
                "target_sha256": checksum,
                "scope": "dbt_weather_product",
                "reason": "test fixture",
                "license_status": "internal_private_snapshot_only",
            }
        ],
    )

    assert verify_manifest(tmp_path, manifest) == []


def test_snapshot_copy_rejects_modified_target(tmp_path: Path) -> None:
    original = b"select 1\n"
    target = tmp_path / "dbt" / "model.sql"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"select 2\n")
    manifest = tmp_path / "source-files.jsonl"
    checksum = _sha256(original)
    _write_manifest(
        manifest,
        [
            {
                "record_type": "snapshot_copy",
                "source_repo": "owner/source",
                "source_ref": "origin/dev",
                "source_commit": "a" * 40,
                "source_path": "models/model.sql",
                "source_blob_oid": "b" * 40,
                "source_content_sha256": checksum,
                "target_path": "dbt/model.sql",
                "target_sha256": checksum,
                "scope": "dbt_weather_product",
                "reason": "test fixture",
                "license_status": "internal_private_snapshot_only",
            }
        ],
    )

    errors = verify_manifest(tmp_path, manifest)

    assert any("target checksum mismatch" in error for error in errors)


def test_derived_record_requires_derivation_evidence(tmp_path: Path) -> None:
    target = tmp_path / "contracts" / "fixture.json"
    target.parent.mkdir(parents=True)
    target.write_text("{}\n", encoding="utf-8")
    checksum = _sha256(target.read_bytes())
    manifest = tmp_path / "source-files.jsonl"
    _write_manifest(
        manifest,
        [
            {
                "record_type": "derived",
                "target_path": "contracts/fixture.json",
                "target_sha256": checksum,
                "scope": "origin_contract",
                "reason": "reduced contract fixture",
                "license_status": "internal_private_snapshot_only",
            }
        ],
    )

    errors = verify_manifest(tmp_path, manifest)

    assert any("derived_from" in error for error in errors)
    assert any("validator" in error for error in errors)


def test_duplicate_target_path_is_rejected(tmp_path: Path) -> None:
    target = tmp_path / "CONTEXT.md"
    target.write_text("context\n", encoding="utf-8")
    checksum = _sha256(target.read_bytes())
    record = {
        "record_type": "local_authored",
        "target_path": "CONTEXT.md",
        "target_sha256": checksum,
        "scope": "repository_documentation",
        "reason": "domain vocabulary",
        "owner": "masondev1024/seoul-weather-platform",
        "license_status": "repository_owned",
    }
    manifest = tmp_path / "source-files.jsonl"
    _write_manifest(manifest, [record, record])

    errors = verify_manifest(tmp_path, manifest)

    assert any("duplicate target_path" in error for error in errors)


def test_candidate_coverage_requires_every_non_manifest_file() -> None:
    records = [
        {"target_path": "dags/weather.py"},
        {"target_path": "README.md"},
    ]

    assert uncovered_candidate_paths(
        [
            "provenance/source-files.jsonl",
            "dags/weather.py",
            "README.md",
            "unexplained.txt",
        ],
        records,
    ) == ["unexplained.txt"]
