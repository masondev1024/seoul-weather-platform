from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
VERIFY_SCRIPT = REPO_ROOT / "tools" / "verify_repository.ps1"


def _powershell_executable() -> Path:
    windows_root = os.environ.get("WINDIR")
    if windows_root:
        return (
            Path(windows_root)
            / "System32"
            / "WindowsPowerShell"
            / "v1.0"
            / "powershell.exe"
        )
    discovered = shutil.which("pwsh") or shutil.which("powershell")
    return Path(discovered or "pwsh")


POWERSHELL = _powershell_executable()


def test_runtime_safety_harness_imports_without_windows_environment() -> None:
    """A hosted Ubuntu repository run must be able to collect this test module."""
    environment = os.environ.copy()
    environment.pop("WINDIR", None)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            f"import runpy; runpy.run_path({str(Path(__file__))!r})",
        ],
        cwd=REPO_ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def _write_command_stub(directory: Path, command: str) -> Path:
    if os.name == "nt":
        path = directory / f"{command}.cmd"
        path.write_text(
            "@echo off\n"
            "echo %~n0 %*>>\"%REPOSITORY_SAFETY_COMMAND_LOG%\"\n"
            "if /I \"%~n0\"==\"python\" if \"%~1\"==\"-c\" echo 3.11\n"
            "exit /b 0\n",
            encoding="utf-8",
        )
        return path

    path = directory / command
    path.write_text(
        "#!/bin/sh\n"
        "command_name=${0##*/}\n"
        "printf '%s %s\\n' \"$command_name\" \"$*\" "
        '">>\"$REPOSITORY_SAFETY_COMMAND_LOG\"\n'
        "if [ \"$command_name\" = python ] && [ \"$1\" = -c ]; then\n"
        "  printf '3.11\\n'\n"
        "fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def test_repository_verifier_runs_only_secretless_python_checks(tmp_path: Path) -> None:
    """Removing the static-only boundary would allow pipeline control from a repo check."""
    command_log = tmp_path / "commands.log"
    stubs = {
        command: _write_command_stub(tmp_path, command)
        for command in ("python", "docker", "airflow", "docker-compose")
    }

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
            str(stubs["python"]),
        ],
        cwd=REPO_ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "Pinned toolchain" in result.stdout
    assert "tests/repository and tests/deploy" in result.stdout
    calls = command_log.read_text(encoding="utf-8").lower().splitlines()
    assert calls[0].startswith("python -c ")
    assert "sys.version_info.major" in calls[0]
    assert calls[1:] == [
        "python -m tools.repository_policy --repo-root " + str(REPO_ROOT).lower(),
        "python -m tools.verify_provenance --repo-root " + str(REPO_ROOT).lower(),
        "python -m tools.refresh_provenance --repo-root "
        + str(REPO_ROOT).lower()
        + " --check",
        "python -m tools.workflow_policy --repo-root " + str(REPO_ROOT).lower(),
        "python -m pytest tests/repository tests/deploy",
    ]
    assert not any("docker" in call or "airflow" in call for call in calls)


def test_repository_verifier_resolves_repo_root_when_omitted(tmp_path: Path) -> None:
    command_log = tmp_path / "commands.log"
    python_stub = _write_command_stub(tmp_path, "python")
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
            str(python_stub),
        ],
        cwd=tmp_path,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert str(REPO_ROOT).lower() in command_log.read_text(encoding="utf-8").lower()
