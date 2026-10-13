"""Install or run a local daily report without changing Weather pipeline state."""
from __future__ import annotations

import argparse
import logging
import os
import platform
import plistlib
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LABEL = "com.mason.seoul-weather-daily-quality"
LOGGER = logging.getLogger(__name__)


def build_plist(root: Path, python: Path, output_dir: Path) -> dict:
    return {
        "Label": LABEL,
        "ProgramArguments": [str(python), "-m", "tools.weather_daily_quality_schedule", "--run"],
        "WorkingDirectory": str(root),
        "EnvironmentVariables": {"PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"},
        "StartCalendarInterval": {"Hour": 9, "Minute": 17},
        "RunAtLoad": True,
        "ProcessType": "Background",
        "Nice": 10,
        "LowPriorityIO": True,
        "Umask": 0o077,
        "StandardOutPath": str(output_dir / "scheduler.log"),
        "StandardErrorPath": str(output_dir / "scheduler-error.log"),
    }


def run_report() -> int:
    # The child has server-side query bounds; this bounds the entire reporting process.
    try:
        return subprocess.run([sys.executable, "-m", "tools.weather_daily_quality"],
                              cwd=ROOT, timeout=240, check=False).returncode
    except (OSError, subprocess.SubprocessError):
        LOGGER.error("daily_quality_scheduler reason=report_process_failed_or_timed_out")
        return 3


def install_schedule() -> int:
    if platform.system() != "Darwin" or datetime.now().astimezone().utcoffset() != timedelta(hours=9):
        LOGGER.error("daily_quality_scheduler reason=macos_kst_required")
        return 3
    python = ROOT / ".venv/bin/python"
    if not python.is_file() or not os.access(python, os.X_OK):
        LOGGER.error("daily_quality_scheduler reason=repository_python_missing")
        return 3
    output = ROOT / "logs/weather_daily_quality"
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = Path.home() / "Library/LaunchAgents" / f"{LABEL}.plist"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = plistlib.dumps(build_plist(ROOT, python, output), sort_keys=False)
    if path.exists():
        if path.read_bytes() != payload:
            LOGGER.error("daily_quality_scheduler reason=existing_schedule_conflict")
            return 3
        existing = subprocess.run(["launchctl", "print", f"gui/{os.getuid()}/{LABEL}"],
                                  capture_output=True, timeout=10, check=False)
        if existing.returncode == 0:
            LOGGER.info("daily_quality_scheduler status=already_installed")
            return 0
    else:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
    result = subprocess.run(["launchctl", "bootstrap", f"gui/{os.getuid()}", str(path)],
                            capture_output=True, timeout=10, check=False)
    if result.returncode != 0:
        LOGGER.error("daily_quality_scheduler reason=bootstrap_failed")
        return 3
    LOGGER.info("daily_quality_scheduler status=installed schedule=09:17_KST")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--run", action="store_true")
    actions.add_argument("--install", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        return run_report() if args.run else install_schedule()
    except (OSError, subprocess.SubprocessError):
        LOGGER.error("daily_quality_scheduler reason=local_schedule_operation_failed")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
