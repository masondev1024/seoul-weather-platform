from __future__ import annotations

import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
VERIFY_SCRIPT = REPO_ROOT / "tools" / "verify_repository.ps1"
POWERSHELL = Path(os.environ["WINDIR"]) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"


def _write_command_stub(directory: Path, command: str) -> None:
    (directory / f"{command}.cmd").write_text(
        "@echo off\n"
        "echo %~n0 %*>>\"%REPOSITORY_SAFETY_COMMAND_LOG%\"\n"
        "if /I \"%~n0\"==\"python\" if \"%~1\"==\"-c\" echo 3.11\n"
        "exit /b 0\n",
        encoding="utf-8",
    )


def test_repository_verifier_runs_only_secretless_python_checks(tmp_path: Path) -> None:
    """Removing the static-only boundary would allow pipeline control from a repo check."""
    command_log = tmp_path / "commands.log"
    for command in ("python", "docker", "airflow", "docker-compose"):
        _write_command_stub(tmp_path, command)

    environment = os.environ | {
        "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
        "REPOSITORY_SAFETY_COMMAND_LOG": str(command_log),
    }
    result = subprocess.run(
        [
            str(POWERSHELL),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(VERIFY_SCRIPT),
            "-RepoRoot",
            str(REPO_ROOT),
            "-PythonExecutable",
            str(tmp_path / "python.cmd"),
        ],
        cwd=REPO_ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "Pinned toolchain" in result.stdout
    assert "tests/repository" in result.stdout
    calls = command_log.read_text(encoding="utf-8").lower().splitlines()
    assert calls[0].startswith("python -c ")
    assert "sys.version_info.major" in calls[0]
    assert calls[1:] == [
        "python -m tools.repository_policy --repo-root " + str(REPO_ROOT).lower(),
        "python -m tools.verify_provenance --repo-root " + str(REPO_ROOT).lower(),
        "python -m tools.refresh_provenance --repo-root "
        + str(REPO_ROOT).lower()
        + " --check",
        "python -m pytest tests/repository",
    ]
    assert not any("docker" in call or "airflow" in call for call in calls)


def test_repository_verifier_resolves_repo_root_when_omitted(tmp_path: Path) -> None:
    command_log = tmp_path / "commands.log"
    _write_command_stub(tmp_path, "python")
    environment = os.environ | {
        "REPOSITORY_SAFETY_COMMAND_LOG": str(command_log),
    }

    result = subprocess.run(
        [
            str(POWERSHELL),
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(VERIFY_SCRIPT),
            "-PythonExecutable",
            str(tmp_path / "python.cmd"),
        ],
        cwd=tmp_path,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert str(REPO_ROOT).lower() in command_log.read_text(encoding="utf-8").lower()
