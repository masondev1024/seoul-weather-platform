from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from tools.source_snapshot import SnapshotError, export_inventory


def _run(*args: str, cwd: Path) -> str:
    result = subprocess.run(
        list(args),
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout.strip()


def _source_repo(tmp_path: Path) -> tuple[Path, str]:
    source = tmp_path / "source"
    source.mkdir()
    _run("git", "init", "-q", cwd=source)
    _run("git", "config", "user.email", "snapshot@example.invalid", cwd=source)
    _run("git", "config", "user.name", "Snapshot Test", cwd=source)
    weather_file = source / "weather" / "file.txt"
    weather_file.parent.mkdir()
    weather_file.write_text("clean snapshot\n", encoding="utf-8")
    _run("git", "add", "weather/file.txt", cwd=source)
    _run("git", "commit", "-qm", "source snapshot", cwd=source)
    commit = _run("git", "rev-parse", "HEAD", cwd=source)
    weather_file.write_text("dirty working tree\n", encoding="utf-8")
    return source, commit


def _write_contract_files(
    repo: Path,
    commit: str,
    *,
    target_path: str = "dags/weather/file.txt",
    entry_overrides: dict[str, object] | None = None,
) -> tuple[Path, Path, Path]:
    inventory = repo / "provenance" / "source-inventory.json"
    source_lock = repo / "provenance" / "source-refs.lock.json"
    manifest = repo / "provenance" / "source-files.jsonl"
    inventory.parent.mkdir(parents=True)
    inventory.write_text(
        json.dumps(
            {
                "schema_version": "source-inventory/v1",
                "entries": [
                    {
                        "source_id": "fixture",
                        "source_path": "weather/file.txt",
                        "target_path": target_path,
                        "scope": "airflow_weather",
                        "reason": "snapshot test",
                        **(entry_overrides or {}),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    source_lock.write_text(
        json.dumps(
            {
                "schema_version": "source-refs/v1",
                "sources": [
                    {
                        "id": "fixture",
                        "repository": "example/source",
                        "ref": "dev",
                        "commit": commit,
                        "license_status": "test_only",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return inventory, source_lock, manifest


def _remove_contract_field(path: Path, *, collection: str, field: str) -> None:
    contract = json.loads(path.read_text(encoding="utf-8"))
    del contract[collection][0][field]
    path.write_text(json.dumps(contract), encoding="utf-8")


def test_export_reads_fixed_commit_not_dirty_working_tree(tmp_path: Path) -> None:
    source, commit = _source_repo(tmp_path)
    target_repo = tmp_path / "target"
    target_repo.mkdir()
    inventory, source_lock, manifest = _write_contract_files(target_repo, commit)

    records = export_inventory(
        repo_root=target_repo,
        inventory_path=inventory,
        source_lock_path=source_lock,
        source_checkouts={"fixture": source},
        manifest_path=manifest,
    )

    assert (target_repo / "dags/weather/file.txt").read_text(encoding="utf-8") == (
        "clean snapshot\n"
    )
    assert records[0]["source_commit"] == commit
    assert records[0]["source_content_sha256"] == records[0]["target_sha256"]
    assert json.loads(manifest.read_text(encoding="utf-8").strip()) == records[0]


@pytest.mark.parametrize("field", ["repository", "ref", "license_status"])
def test_export_rejects_source_lock_missing_required_metadata(
    tmp_path: Path, field: str
) -> None:
    source, commit = _source_repo(tmp_path)
    target_repo = tmp_path / "target"
    target_repo.mkdir()
    inventory, source_lock, manifest = _write_contract_files(target_repo, commit)
    _remove_contract_field(source_lock, collection="sources", field=field)

    with pytest.raises(SnapshotError, match=field):
        export_inventory(
            repo_root=target_repo,
            inventory_path=inventory,
            source_lock_path=source_lock,
            source_checkouts={"fixture": source},
            manifest_path=manifest,
        )

    assert not (target_repo / "dags/weather/file.txt").exists()
    assert not manifest.exists()


@pytest.mark.parametrize("field", ["scope", "reason"])
def test_export_rejects_inventory_missing_required_metadata_before_writing(
    tmp_path: Path, field: str
) -> None:
    source, commit = _source_repo(tmp_path)
    target_repo = tmp_path / "target"
    target_repo.mkdir()
    inventory, source_lock, manifest = _write_contract_files(target_repo, commit)
    _remove_contract_field(inventory, collection="entries", field=field)

    with pytest.raises(SnapshotError, match=field):
        export_inventory(
            repo_root=target_repo,
            inventory_path=inventory,
            source_lock_path=source_lock,
            source_checkouts={"fixture": source},
            manifest_path=manifest,
        )

    assert not (target_repo / "dags/weather/file.txt").exists()
    assert not manifest.exists()


def test_export_refuses_to_overwrite_different_target(tmp_path: Path) -> None:
    source, commit = _source_repo(tmp_path)
    target_repo = tmp_path / "target"
    target_repo.mkdir()
    inventory, source_lock, manifest = _write_contract_files(target_repo, commit)
    existing = target_repo / "dags/weather/file.txt"
    existing.parent.mkdir(parents=True)
    existing.write_text("local edit\n", encoding="utf-8")

    with pytest.raises(SnapshotError, match="refusing to overwrite"):
        export_inventory(
            repo_root=target_repo,
            inventory_path=inventory,
            source_lock_path=source_lock,
            source_checkouts={"fixture": source},
            manifest_path=manifest,
        )


def test_export_rejects_target_path_escape(tmp_path: Path) -> None:
    source, commit = _source_repo(tmp_path)
    target_repo = tmp_path / "target"
    target_repo.mkdir()
    inventory, source_lock, manifest = _write_contract_files(
        target_repo,
        commit,
        target_path="../outside.txt",
    )

    with pytest.raises(SnapshotError, match="escapes repository root"):
        export_inventory(
            repo_root=target_repo,
            inventory_path=inventory,
            source_lock_path=source_lock,
            source_checkouts={"fixture": source},
            manifest_path=manifest,
        )


def test_export_preserves_reviewed_derived_override_with_source_evidence(
    tmp_path: Path,
) -> None:
    source, commit = _source_repo(tmp_path)
    target_repo = tmp_path / "target"
    target_repo.mkdir()
    inventory, source_lock, manifest = _write_contract_files(
        target_repo,
        commit,
        entry_overrides={
            "record_type": "derived",
            "derivation": "Weather-only fixture boundary",
            "validator": "python -m pytest tests",
        },
    )
    target = target_repo / "dags/weather/file.txt"
    target.parent.mkdir(parents=True)
    target.write_text("reviewed derived target\n", encoding="utf-8")

    records = export_inventory(
        repo_root=target_repo,
        inventory_path=inventory,
        source_lock_path=source_lock,
        source_checkouts={"fixture": source},
        manifest_path=manifest,
    )

    assert target.read_text(encoding="utf-8") == "reviewed derived target\n"
    assert records[0]["record_type"] == "derived"
    assert records[0]["derived_from"]["source_commit"] == commit
    assert records[0]["derived_from"]["source_content_sha256"] != records[0]["target_sha256"]


def test_export_requires_derived_target_to_exist(tmp_path: Path) -> None:
    source, commit = _source_repo(tmp_path)
    target_repo = tmp_path / "target"
    target_repo.mkdir()
    inventory, source_lock, manifest = _write_contract_files(
        target_repo,
        commit,
        entry_overrides={
            "record_type": "derived",
            "derivation": "Weather-only fixture boundary",
            "validator": "python -m pytest tests",
        },
    )

    with pytest.raises(SnapshotError, match="derived target does not exist"):
        export_inventory(
            repo_root=target_repo,
            inventory_path=inventory,
            source_lock_path=source_lock,
            source_checkouts={"fixture": source},
            manifest_path=manifest,
        )
