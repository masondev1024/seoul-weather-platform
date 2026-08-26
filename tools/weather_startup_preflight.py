#!/usr/bin/env python3
"""Read-only startup preflight for the personal Weather runtime.

The preflight proves that Docker, Compose configuration, Airflow/Trino
processes, and the local memory envelope are ready.  It never starts a
container and never changes Airflow or data state; a separate launch wrapper
may consume a passing report before doing an explicitly approved start.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


SCHEMA_VERSION = "weather-startup-preflight/v1"
COMPOSE_FILES = ("docker-compose.yml", "docker-compose.local.yml")
REQUIRED_SERVICES = (
    "postgres",
    "trino",
    "airflow-apiserver",
    "airflow-scheduler",
    "airflow-dag-processor",
    "airflow-triggerer",
)
TRINO_SERVICE = "trino"
TRINO_HEALTH_URL = "http://127.0.0.1:30586/v1/info"
TRINO_MAX_MEM_PERCENT = 65.0
CORE_MAX_MEM_PERCENT = 80.0
CLOCK_SKEW_LIMIT_SECONDS = 5.0
_MEMORY_RE = re.compile(
    r"^\s*(?P<used>[0-9]+(?:\.[0-9]+)?)\s*(?P<used_unit>[A-Za-z]+)\s*/\s*"
    r"(?P<limit>[0-9]+(?:\.[0-9]+)?)\s*(?P<limit_unit>[A-Za-z]+)\s*$"
)
_UNIT_MIB = {
    "b": 1 / (1024 * 1024),
    "kb": 1 / 1024,
    "kib": 1 / 1024,
    "mb": 1,
    "mib": 1,
    "gb": 1024,
    "gib": 1024,
    "tb": 1024 * 1024,
    "tib": 1024 * 1024,
}


@dataclass(frozen=True)
class PreflightCheck:
    name: str
    status: str
    reason_code: str
    details: Mapping[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "status": self.status,
            "reason_code": self.reason_code,
            "details": dict(self.details or {}),
        }


@dataclass(frozen=True)
class PreflightReport:
    ready: bool
    checks: tuple[PreflightCheck, ...]
    generated_at: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "ready": self.ready,
            "mutation_performed": False,
            "generated_at": self.generated_at,
            "checks": [check.to_dict() for check in self.checks],
        }


CommandRunner = Callable[
    [Sequence[str], Path, Mapping[str, str]], subprocess.CompletedProcess[str]
]
ClockProbe = Callable[[str, float], tuple[bool, float | None]]
TrinoProbe = Callable[[str, float], tuple[bool, Mapping[str, object]]]


def run_preflight(
    repo_root: Path,
    env_file: Path,
    *,
    runner: CommandRunner | None = None,
    now: datetime | None = None,
    require_services: bool = True,
    clock_url: str | None = None,
    require_clock: bool = False,
    clock_probe: ClockProbe | None = None,
    trino_probe: TrinoProbe | None = None,
) -> PreflightReport:
    """Run read-only startup checks and return a redacted report."""
    generated_at = _aware_utc(now or datetime.now(timezone.utc)).isoformat()
    checks: list[PreflightCheck] = []
    root = _resolve_directory(repo_root)
    env_path = _resolve_file(env_file)
    compose_files_present = root is not None and _compose_files_present(root)
    credentials_file_secure = env_path is not None and _credentials_file_is_secure(env_path)
    checks.append(
        PreflightCheck(
            "repository_files",
            "pass" if compose_files_present else "fail",
            "files_present" if compose_files_present else "files_missing",
            {"compose_file_count": sum((root / name).is_file() for name in COMPOSE_FILES) if root else 0},
        )
    )
    checks.append(
        PreflightCheck(
            "credentials_file",
            "pass" if credentials_file_secure else "fail",
            (
                "present"
                if env_path is not None and credentials_file_secure
                else "permissions_too_open"
                if env_path is not None
                else "missing"
            ),
            {"present": env_path is not None, "secure_permissions": credentials_file_secure},
        )
    )
    if root is None or not compose_files_present or not credentials_file_secure:
        checks.append(
            PreflightCheck("docker_daemon", "fail", "precondition_failed")
        )
        return _report(checks, generated_at)

    execute = runner or _run_command
    command_env = {"ASK_SEOUL_PROD_ENV_FILE": str(env_path)}
    docker_info = execute(("docker", "info", "--format", "{{.ServerVersion}}"), root, command_env)
    docker_ok = docker_info.returncode == 0
    checks.append(
        PreflightCheck(
            "docker_daemon",
            "pass" if docker_ok else "fail",
            "ready" if docker_ok else "unavailable",
        )
    )
    if not docker_ok:
        checks.append(PreflightCheck("compose_config", "fail", "docker_unavailable"))
        return _report(checks, generated_at)

    compose = _compose_command(root, env_path)
    compose_config = execute((*compose, "config", "--quiet"), root, command_env)
    config_ok = compose_config.returncode == 0
    checks.append(
        PreflightCheck(
            "compose_config",
            "pass" if config_ok else "fail",
            "valid" if config_ok else "invalid",
        )
    )
    if not config_ok or not require_services:
        return _report(checks, generated_at)

    compose_ps = execute((*compose, "ps", "--format", "json"), root, command_env)
    services = _parse_compose_ps(compose_ps.stdout) if compose_ps.returncode == 0 else {}
    service_check = _service_readiness(services)
    checks.append(service_check)
    if service_check.status != "pass":
        return _report(checks, generated_at)

    stats = execute(
        ("docker", "stats", "--no-stream", "--format", "{{json .}}"),
        root,
        command_env,
    )
    memory_check = _memory_readiness(stats.stdout if stats.returncode == 0 else "")
    checks.append(memory_check)

    probe = trino_probe or _probe_trino
    trino_ok, trino_details = probe(TRINO_HEALTH_URL, 3.0)
    checks.append(
        PreflightCheck(
            "trino_liveness",
            "pass" if trino_ok else "fail",
            "ready" if trino_ok else "unavailable",
            trino_details,
        )
    )

    if clock_url:
        if not _valid_clock_url(clock_url):
            checks.append(PreflightCheck("clock_skew", "fail", "invalid_url"))
        else:
            probe_clock = clock_probe or _probe_clock
            clock_ok, skew = probe_clock(clock_url, 3.0)
            checks.append(
                PreflightCheck(
                    "clock_skew",
                    "pass" if clock_ok else "fail",
                    "within_limit" if clock_ok else "outside_limit",
                    {"skew_seconds": round(skew, 3) if skew is not None else None},
                )
            )
    elif require_clock:
        checks.append(
            PreflightCheck("clock_skew", "fail", "not_configured")
        )
    else:
        checks.append(
            PreflightCheck("clock_skew", "skip", "not_configured")
        )
    return _report(checks, generated_at)


def _report(checks: Sequence[PreflightCheck], generated_at: str) -> PreflightReport:
    mandatory = [check for check in checks if check.name != "clock_skew" or check.status != "skip"]
    ready = bool(mandatory) and all(check.status == "pass" for check in mandatory)
    return PreflightReport(ready=ready, checks=tuple(checks), generated_at=generated_at)


def _resolve_directory(path: Path) -> Path | None:
    try:
        resolved = path.expanduser().resolve(strict=True)
    except (OSError, RuntimeError):
        return None
    return resolved if resolved.is_dir() and resolved != Path(resolved.anchor) else None


def _resolve_file(path: Path) -> Path | None:
    try:
        resolved = path.expanduser().resolve(strict=True)
    except (OSError, RuntimeError):
        return None
    return resolved if resolved.is_file() else None


def _compose_files_present(root: Path) -> bool:
    return all((root / name).is_file() for name in COMPOSE_FILES)


def _credentials_file_is_secure(path: Path) -> bool:
    """Require private env files to be owner-only readable on macOS/Linux."""
    if path.name == ".env.example":
        return True
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
    except OSError:
        return False
    return mode & 0o077 == 0


def _compose_command(root: Path, env_file: Path) -> list[str]:
    return [
        "docker",
        "compose",
        "--env-file",
        str(env_file),
        "-f",
        COMPOSE_FILES[0],
        "-f",
        COMPOSE_FILES[1],
    ]


def _run_command(
    command: Sequence[str],
    cwd: Path,
    env: Mapping[str, str],
) -> subprocess.CompletedProcess[str]:
    merged = dict(os.environ)
    merged.update(env)
    return subprocess.run(
        list(command),
        cwd=cwd,
        env=merged,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )


def _parse_compose_ps(raw: str) -> dict[str, Mapping[str, object]]:
    records: list[object] = []
    text = raw.strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, list):
        records = parsed
    elif isinstance(parsed, Mapping):
        records = [parsed]
    else:
        for line in text.splitlines():
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, Mapping):
                records.append(item)
    result: dict[str, Mapping[str, object]] = {}
    for item in records:
        if not isinstance(item, Mapping):
            continue
        service = item.get("Service") or item.get("service")
        if isinstance(service, str) and service:
            result[service] = item
    return result


def _service_readiness(services: Mapping[str, Mapping[str, object]]) -> PreflightCheck:
    missing = [name for name in REQUIRED_SERVICES if name not in services]
    unhealthy = []
    not_running = []
    for name in REQUIRED_SERVICES:
        item = services.get(name)
        if item is None:
            continue
        state = str(item.get("State") or item.get("state") or "").lower()
        status = str(item.get("Status") or item.get("status") or "").lower()
        health = str(item.get("Health") or item.get("health") or "").lower()
        if state not in {"running", "up"}:
            not_running.append(name)
        if "unhealthy" in status or health == "unhealthy":
            unhealthy.append(name)
    if missing or not_running or unhealthy:
        return PreflightCheck(
            "compose_services",
            "fail",
            "service_not_ready",
            {
                "missing_count": len(missing),
                "not_running_count": len(not_running),
                "unhealthy_count": len(unhealthy),
            },
        )
    return PreflightCheck(
        "compose_services",
        "pass",
        "ready",
        {"service_count": len(services)},
    )


def _memory_readiness(raw: str) -> PreflightCheck:
    records: list[Mapping[str, object]] = []
    for line in raw.splitlines():
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, Mapping):
            records.append(parsed)
    trino_percent: float | None = None
    core_percentages: list[float] = []
    parse_failures = 0
    for item in records:
        name = str(item.get("Name") or item.get("name") or "")
        usage = str(item.get("MemUsage") or item.get("mem_usage") or "")
        percent_raw = str(item.get("MemPerc") or item.get("mem_perc") or "").strip().rstrip("%")
        percent = _finite_float(percent_raw)
        if percent is None:
            usage_pair = _memory_pair_mib(usage)
            percent = (usage_pair[0] / usage_pair[1] * 100) if usage_pair and usage_pair[1] > 0 else None
        if percent is None:
            parse_failures += 1
            continue
        if "trino" in name.lower():
            trino_percent = percent
        if any(token in name.lower() for token in ("airflow", "postgres", "trino")):
            core_percentages.append(percent)
    if trino_percent is None:
        return PreflightCheck("memory_budget", "fail", "trino_memory_unavailable")
    max_core = max(core_percentages, default=math.inf)
    ready = trino_percent <= TRINO_MAX_MEM_PERCENT and max_core <= CORE_MAX_MEM_PERCENT
    return PreflightCheck(
        "memory_budget",
        "pass" if ready else "fail",
        "within_budget" if ready else "budget_exceeded",
        {
            "trino_mem_percent": round(trino_percent, 2),
            "core_max_mem_percent": round(max_core, 2) if math.isfinite(max_core) else None,
            "parse_failure_count": parse_failures,
        },
    )


def _memory_pair_mib(value: str) -> tuple[float, float] | None:
    match = _MEMORY_RE.fullmatch(value)
    if match is None:
        return None
    used_unit = _UNIT_MIB.get(match.group("used_unit").lower())
    limit_unit = _UNIT_MIB.get(match.group("limit_unit").lower())
    if used_unit is None or limit_unit is None:
        return None
    return (
        float(match.group("used")) * used_unit,
        float(match.group("limit")) * limit_unit,
    )


def _finite_float(value: str) -> float | None:
    try:
        parsed = float(value)
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


def _probe_trino(url: str, timeout: float) -> tuple[bool, Mapping[str, object]]:
    try:
        with urlopen(Request(url, method="GET"), timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, URLError, ValueError, UnicodeError):
        return False, {"coordinator": None, "starting": None}
    if not isinstance(payload, Mapping):
        return False, {"coordinator": None, "starting": None}
    coordinator = payload.get("coordinator") is True
    starting = payload.get("starting") is True
    return coordinator and not starting, {
        "coordinator": coordinator,
        "starting": starting,
    }


def _probe_clock(url: str, timeout: float) -> tuple[bool, float | None]:
    if not _valid_clock_url(url):
        return False, None
    try:
        with urlopen(Request(url, method="HEAD"), timeout=timeout) as response:
            raw_date = response.headers.get("Date")
    except (OSError, URLError):
        return False, None
    if not raw_date:
        return False, None
    try:
        remote = parsedate_to_datetime(raw_date)
    except (TypeError, ValueError):
        return False, None
    if remote.tzinfo is None:
        remote = remote.replace(tzinfo=timezone.utc)
    skew = (datetime.now(timezone.utc) - remote.astimezone(timezone.utc)).total_seconds()
    return abs(skew) <= CLOCK_SKEW_LIMIT_SECONDS, skew


def _valid_clock_url(url: str) -> bool:
    parsed_url = urlparse(url)
    return (
        parsed_url.scheme in {"http", "https"}
        and bool(parsed_url.netloc)
        and parsed_url.username is None
        and parsed_url.password is None
    )


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return value.astimezone(timezone.utc)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run read-only Weather startup preflight.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--env-file", type=Path, default=Path("weather-platform.prod.env"))
    parser.add_argument("--clock-url")
    parser.add_argument("--require-clock", action="store_true")
    parser.add_argument(
        "--configuration-only",
        action="store_true",
        help="check Docker and Compose config without requiring running services",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        report = run_preflight(
            args.repo_root,
            args.env_file,
            require_services=not args.configuration_only,
            clock_url=args.clock_url,
            require_clock=args.require_clock,
        )
    except (OSError, RuntimeError, ValueError):
        print("weather_startup_preflight_error=invalid_configuration", file=sys.stderr)
        return 2
    print(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True))
    return 0 if report.ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
