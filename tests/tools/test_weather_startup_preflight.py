from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tools.weather_startup_preflight import (
    REQUIRED_SERVICES,
    TRINO_HEALTH_URL,
    run_preflight,
)


NOW = datetime(2026, 8, 26, 6, 0, tzinfo=timezone.utc)


def _repo(tmp_path: Path) -> tuple[Path, Path]:
    (tmp_path / "docker-compose.yml").write_text("services: {}\n", encoding="utf-8")
    (tmp_path / "docker-compose.local.yml").write_text("services: {}\n", encoding="utf-8")
    env_file = tmp_path / "weather-platform.prod.env"
    env_file.write_text("REDACTED=placeholder\n", encoding="utf-8")
    env_file.chmod(0o600)
    return tmp_path, env_file


def _ps_records(*, unhealthy: str | None = None) -> str:
    records = []
    for service in REQUIRED_SERVICES:
        record: dict[str, object] = {
            "Service": service,
            "State": "running",
            "Status": "Up 5 minutes (healthy)",
        }
        if service == unhealthy:
            record["Status"] = "Up 5 minutes (unhealthy)"
        records.append(record)
    return json.dumps(records)


def _stats(*, trino_percent: float = 51.0) -> str:
    rows = [
        {
            "Name": "seoul-weather-platform-mac-trino-1",
            "MemUsage": "2.55GiB / 5GiB",
            "MemPerc": f"{trino_percent}%",
        },
        {
            "Name": "seoul-weather-platform-mac-airflow-scheduler-1",
            "MemUsage": "1.3GiB / 7.6GiB",
            "MemPerc": "17%",
        },
    ]
    return "\n".join(json.dumps(row) for row in rows)


class Runner:
    def __init__(self, *, trino_percent: float = 51.0, unhealthy: str | None = None) -> None:
        self.commands: list[tuple[str, ...]] = []
        self.trino_percent = trino_percent
        self.unhealthy = unhealthy

    def __call__(self, command, _cwd, _env):
        command_tuple = tuple(command)
        self.commands.append(command_tuple)
        if command_tuple[:2] == ("docker", "info"):
            return subprocess.CompletedProcess(command, 0, "27.0\n", "")
        if command_tuple[-2:] == ("config", "--quiet"):
            return subprocess.CompletedProcess(command, 0, "", "")
        if command_tuple[-3:] == ("ps", "--format", "json"):
            return subprocess.CompletedProcess(command, 0, _ps_records(unhealthy=self.unhealthy), "")
        if command_tuple[:3] == ("docker", "stats", "--no-stream"):
            return subprocess.CompletedProcess(command, 0, _stats(trino_percent=self.trino_percent), "")
        raise AssertionError(f"unexpected command: {command_tuple}")


def _trino_probe(_url: str, _timeout: float):
    return True, {"coordinator": True, "starting": False}


def test_preflight_passes_without_mutation_and_redacts_configuration(tmp_path: Path) -> None:
    root, env_file = _repo(tmp_path)
    runner = Runner()

    report = run_preflight(
        root,
        env_file,
        runner=runner,
        now=NOW,
        trino_probe=_trino_probe,
    )

    assert report.ready is True
    assert report.to_dict()["mutation_performed"] is False
    assert all("REDACTED" not in json.dumps(check.to_dict()) for check in report.checks)
    assert any(command[:2] == ("docker", "info") for command in runner.commands)
    assert not any("up" in command or "restart" in command for command in runner.commands)
    clock = next(check for check in report.checks if check.name == "clock_skew")
    assert clock.status == "skip"


def test_preflight_fails_closed_when_a_service_is_missing(tmp_path: Path) -> None:
    root, env_file = _repo(tmp_path)

    class MissingRunner(Runner):
        def __call__(self, command, cwd, env):
            if tuple(command)[-3:] == ("ps", "--format", "json"):
                records = json.loads(_ps_records())
                records.pop()
                self.commands.append(tuple(command))
                return subprocess.CompletedProcess(command, 0, json.dumps(records), "")
            return super().__call__(command, cwd, env)

    runner = MissingRunner()
    report = run_preflight(
        root,
        env_file,
        runner=runner,
        now=NOW,
        trino_probe=_trino_probe,
    )

    assert report.ready is False
    service_check = next(check for check in report.checks if check.name == "compose_services")
    assert service_check.reason_code == "service_not_ready"
    assert not any(command[:3] == ("docker", "stats", "--no-stream") for command in runner.commands)


def test_preflight_fails_when_trino_exceeds_mac_budget(tmp_path: Path) -> None:
    root, env_file = _repo(tmp_path)

    report = run_preflight(
        root,
        env_file,
        runner=Runner(trino_percent=66.0),
        now=NOW,
        trino_probe=_trino_probe,
    )

    assert report.ready is False
    memory_check = next(check for check in report.checks if check.name == "memory_budget")
    assert memory_check.reason_code == "budget_exceeded"
    assert memory_check.details["trino_mem_percent"] == 66.0


def test_preflight_fails_closed_when_private_env_permissions_are_open(tmp_path: Path) -> None:
    root, env_file = _repo(tmp_path)
    env_file.chmod(0o644)
    runner = Runner()

    report = run_preflight(
        root,
        env_file,
        runner=runner,
        now=NOW,
        trino_probe=_trino_probe,
    )

    assert report.ready is False
    credentials = next(check for check in report.checks if check.name == "credentials_file")
    assert credentials.reason_code == "permissions_too_open"
    assert runner.commands == []


def test_preflight_rejects_non_http_clock_probe_url(tmp_path: Path) -> None:
    root, env_file = _repo(tmp_path)
    report = run_preflight(
        root,
        env_file,
        runner=Runner(),
        now=NOW,
        clock_url="file:///tmp/clock",
        require_clock=True,
        trino_probe=_trino_probe,
    )

    assert report.ready is False
    clock = next(check for check in report.checks if check.name == "clock_skew")
    assert clock.reason_code == "invalid_url"


def test_configuration_only_does_not_require_running_services(tmp_path: Path) -> None:
    root, env_file = _repo(tmp_path)
    runner = Runner()

    report = run_preflight(
        root,
        env_file,
        runner=runner,
        now=NOW,
        require_services=False,
        trino_probe=lambda *_: pytest.fail("Trino probe must not run"),
    )

    assert report.ready is True
    assert [command[:2] for command in runner.commands] == [
        ("docker", "info"),
        ("docker", "compose"),
    ]
    assert not any(command[-3:] == ("ps", "--format", "json") for command in runner.commands)


def test_preflight_trino_probe_receives_local_health_endpoint(tmp_path: Path) -> None:
    root, env_file = _repo(tmp_path)
    seen: list[str] = []

    def probe(url: str, _timeout: float):
        seen.append(url)
        return True, {"coordinator": True, "starting": False}

    report = run_preflight(root, env_file, runner=Runner(), now=NOW, trino_probe=probe)

    assert report.ready is True
    assert seen == [TRINO_HEALTH_URL]
