from __future__ import annotations

import plistlib
import shutil
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "weather_startup.sh"
PLIST = ROOT / "runtime" / "launchd" / "com.mason.seoul-weather-platform.plist.example"


def test_startup_wrapper_has_valid_zsh_syntax() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    assert source.splitlines()[0] == "#!/bin/zsh"

    zsh = shutil.which("zsh")
    if zsh is None:
        pytest.skip("zsh is only available on the macOS runtime target")

    result = subprocess.run(
        [zsh, "-n", str(SCRIPT)],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_startup_wrapper_is_opt_in_and_has_no_airflow_mutation_commands() -> None:
    source = SCRIPT.read_text(encoding="utf-8").lower()
    executable = "\n".join(
        line for line in source.splitlines() if not line.lstrip().startswith("#")
    )

    assert "weather_startup_autostart" in source
    assert "--configuration-only" in source
    assert "up -d --no-build" in source
    for forbidden in (
        "airflow dags trigger",
        "airflow dags unpause",
        "airflow dags backfill",
        "--force-recreate",
        "--build",
    ):
        assert forbidden not in executable


def test_launchd_template_is_valid_and_disabled_by_default() -> None:
    payload = plistlib.loads(PLIST.read_bytes())

    assert payload["Label"] == "com.mason.seoul-weather-platform"
    assert payload["RunAtLoad"] is True
    assert payload["KeepAlive"] is True
    assert payload["EnvironmentVariables"]["WEATHER_STARTUP_AUTOSTART"] == "disabled"
    assert "YOUR_USER" in str(payload["ProgramArguments"])
