from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tools.dagbag_check import DagBagHarnessError, build_dagbag_command
from tools.dagbag_runtime_check import (
    EXPECTED_DAG_IDS,
    dag_inventory_errors,
    normalized_import_errors,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
VERIFY_SCRIPT = REPOSITORY_ROOT / "tools" / "verify_dagbag.ps1"


def _powershell_executable() -> str:
    windir = os.environ.get("WINDIR")
    if windir:
        windows_powershell = (
            Path(windir)
            / "System32"
            / "WindowsPowerShell"
            / "v1.0"
            / "powershell.exe"
        )
        if windows_powershell.is_file():
            return str(windows_powershell)

    executable = shutil.which("pwsh") or shutil.which("powershell")
    if executable:
        return executable

    pytest.skip("PowerShell is required to verify the wrapper")


def _write_toolchain(
    repo_root: Path, digest: str, repository: str = "elt-infra-airflow"
) -> None:
    tools_dir = repo_root / "tools"
    tools_dir.mkdir(exist_ok=True)
    (tools_dir / "dagbag_runtime_check.py").write_text("# fixture\n", encoding="utf-8")
    lock = repo_root / "runtime" / "toolchain.lock.json"
    lock.parent.mkdir(parents=True)
    lock.write_text(
        json.dumps(
            {
                "tools": {
                    "airflow": {
                        "image_repository": repository,
                        "image_digest": digest,
                    }
                }
            }
        ),
        encoding="utf-8",
    )


def test_command_uses_pinned_one_off_read_only_container(tmp_path: Path) -> None:
    (tmp_path / "dags").mkdir()
    (tmp_path / "dbt").mkdir()
    digest = "sha256:" + "a" * 64
    _write_toolchain(tmp_path, digest)

    command = build_dagbag_command(tmp_path)

    assert command[:2] == ["docker", "run"]
    assert "--network" in command and command[command.index("--network") + 1] == "none"
    assert "--read-only" in command
    assert command.count("--tmpfs") == 1
    assert command[command.index("--tmpfs") + 1].startswith("/tmp:")
    mounts = [command[index + 1] for index, value in enumerate(command) if value == "--mount"]
    assert len(mounts) == 3
    assert all("readonly" in mount for mount in mounts)
    assert all("type=bind" in mount for mount in mounts)
    assert f"elt-infra-airflow@{digest}" in command
    assert command[-4:] == [
        "python",
        "/opt/verification/dagbag_runtime_check.py",
        "--dags-folder",
        "/opt/airflow/dags",
    ]
    assert "PYTHONDONTWRITEBYTECODE=1" in command


def test_command_rejects_unpinned_airflow_image(tmp_path: Path) -> None:
    (tmp_path / "dags").mkdir()
    (tmp_path / "dbt").mkdir()
    _write_toolchain(tmp_path, "apache/airflow:latest")

    with pytest.raises(DagBagHarnessError, match="sha256 digest"):
        build_dagbag_command(tmp_path)


def test_command_rejects_mutable_or_unsafe_image_repository(tmp_path: Path) -> None:
    (tmp_path / "dags").mkdir()
    (tmp_path / "dbt").mkdir()
    _write_toolchain(tmp_path, "sha256:" + "a" * 64, "elt-infra-airflow:latest")

    with pytest.raises(DagBagHarnessError, match="image_repository"):
        build_dagbag_command(tmp_path)


def test_runtime_check_normalizes_import_errors_without_metadata_db() -> None:
    assert normalized_import_errors(
        {
            "/opt/airflow/dags/b.py": "trace b",
            "/opt/airflow/dags/a.py": "trace a",
        }
    ) == [
        {"path": "/opt/airflow/dags/a.py", "error": "trace a"},
        {"path": "/opt/airflow/dags/b.py", "error": "trace b"},
    ]


def test_runtime_check_uses_airflow_safe_mode_and_source_ignore_contracts() -> None:
    runtime_check = (
        REPOSITORY_ROOT / "tools" / "dagbag_runtime_check.py"
    ).read_text(encoding="utf-8")

    assert "safe_mode=True" in runtime_check
    assert (REPOSITORY_ROOT / "dags/common/.airflowignore").is_file()
    assert (REPOSITORY_ROOT / "dags/domains/weather/.airflowignore").is_file()


def test_runtime_check_rejects_missing_or_unexpected_dag_ids() -> None:
    actual = set(EXPECTED_DAG_IDS)
    actual.remove("weather_serving_export")
    actual.add("traffic_serving_export")

    assert dag_inventory_errors(actual) == [
        "missing DAG id: weather_serving_export",
        "unexpected DAG id: traffic_serving_export",
    ]


def test_powershell_wrapper_has_no_pipeline_control_words() -> None:
    wrapper = (REPOSITORY_ROOT / "tools" / "verify_dagbag.ps1").read_text(encoding="utf-8")
    lowered = wrapper.lower()

    assert '"-m", "tools.dagbag_check"' in lowered
    assert "--repo-root" in lowered
    for forbidden in (
        "docker compose",
        "docker-compose",
        "airflow dags trigger",
        "unpause",
        "backfill",
        " stop",
        " restart",
    ):
        assert forbidden not in lowered


def test_powershell_wrapper_resolves_repo_root_when_omitted(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            _powershell_executable(),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(VERIFY_SCRIPT),
            "-PythonExecutable",
            sys.executable,
            "-PrintCommand",
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    command = json.loads(result.stdout.strip())
    assert command[:2] == ["docker", "run"]
    assert any(str(REPOSITORY_ROOT / "dags") in item for item in command)
