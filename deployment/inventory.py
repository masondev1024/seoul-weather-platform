from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from deployment.canonical_json import canonical_bytes, sha256_hex
from deployment.command import CommandRunner, CompletedCommand
from deployment.output_contracts import (
    parse_airflow_bool,
    parse_airflow_json_rows,
    parse_compose_json_rows,
)
from deployment.redaction import SensitiveArtifactError, reject_sensitive_artifact
from deployment.target import DeployTarget


class InventoryError(RuntimeError):
    """A redacted category for a failed pre-publish inventory probe."""


class LedgerReader(Protocol):
    def read_summary(self) -> Mapping[str, object]: ...


@dataclass(frozen=True)
class ReleaseInventory:
    service_states: Mapping[str, str]
    dag_paused: Mapping[str, bool]
    run_counts: Mapping[str, int]
    ledger_fingerprint: str


_SENSITIVE_STDERR = re.compile(
    r"(?:api[_-]?key|token|secret|password|credential|authorization|bearer)\s*(?:=|:|\s)",
    re.IGNORECASE,
)
_SHELL_OPERATOR = re.compile(r"[|;&`$<>]")
_FORBIDDEN_TOKENS = frozenset(
    {"up", "build", "restart", "pause", "unpause", "trigger", "backfill", "clear", "dbt", "trino", "wrangler"}
)


def _target_argv_value(value: object) -> str:
    if not isinstance(value, str) or not value or value.startswith("-") or _SHELL_OPERATOR.search(value):
        raise InventoryError("inventory_command_disallowed")
    return value


def _compose_prefix(target: DeployTarget) -> list[str]:
    argv = ["docker", "compose", "-p", _target_argv_value(target.project_name)]
    for compose_file in target.compose_files:
        argv.extend(("-f", _target_argv_value(str(compose_file))))
    return argv


def _validate_target_argv(target: DeployTarget) -> None:
    _target_argv_value(target.project_name)
    _target_argv_value(target.control_service)
    for compose_file in target.compose_files:
        _target_argv_value(str(compose_file))
    for service in target.airflow_code_services | target.forbidden_data_services:
        _target_argv_value(service)


def _checked(target: DeployTarget, runner: CommandRunner, argv: Sequence[str]) -> str:
    if any(token in _FORBIDDEN_TOKENS or _SHELL_OPERATOR.search(token) for token in argv):
        raise InventoryError("inventory_command_disallowed")
    try:
        result: CompletedCommand = runner.run(argv, Path(str(target.working_directory)))
    except Exception:
        raise InventoryError("inventory_command_failed") from None
    if _SENSITIVE_STDERR.search(result.stderr):
        raise InventoryError("inventory_sensitive_stderr")
    if result.returncode != 0 or result.stderr:
        raise InventoryError("inventory_command_failed")
    return result.stdout


def _airflow_rows(stdout: str) -> list[Mapping[str, object]]:
    try:
        return parse_airflow_json_rows(stdout)
    except Exception:
        raise InventoryError("inventory_invalid_output") from None


def _compose_rows(stdout: str) -> list[Mapping[str, object]]:
    try:
        return parse_compose_json_rows(stdout)
    except Exception:
        raise InventoryError("inventory_invalid_output") from None


def _configured_services(target: DeployTarget, runner: CommandRunner) -> frozenset[str]:
    stdout = _checked(target, runner, [*_compose_prefix(target), "config", "--services"])
    services = [line.strip() for line in stdout.splitlines() if line.strip()]
    configured = frozenset(services)
    required = target.airflow_code_services
    allowed = required | target.forbidden_data_services | {"airflow-init"}
    if (
        len(services) != len(set(services))
        or not required <= configured
        or not configured <= allowed
    ):
        raise InventoryError("inventory_invalid_output")
    return configured


def _service_states(
    target: DeployTarget, runner: CommandRunner, configured_services: frozenset[str]
) -> dict[str, str]:
    rows = _compose_rows(
        _checked(target, runner, [*_compose_prefix(target), "ps", "--format", "json"])
    )
    parsed: dict[str, str] = {}
    for row in rows:
        service = row.get("Service")
        state = row.get("State")
        if not isinstance(service, str) or not isinstance(state, str) or not state:
            raise InventoryError("inventory_invalid_output")
        if service not in configured_services or service in parsed:
            raise InventoryError("inventory_invalid_output")
        parsed[service] = state
    parsed_services = set(parsed)
    if (
        not target.airflow_code_services <= parsed_services
        or not parsed_services <= set(configured_services)
    ):
        raise InventoryError("inventory_invalid_output")
    return {service: parsed[service] for service in sorted(target.airflow_code_services)}


def _dag_paused(target: DeployTarget, runner: CommandRunner) -> dict[str, bool]:
    argv = [
        *_compose_prefix(target),
        "exec",
        "-T",
        _target_argv_value(target.control_service),
        "airflow",
        "dags",
        "list",
        "-o",
        "json",
    ]
    rows = _airflow_rows(_checked(target, runner, argv))
    parsed: dict[str, bool] = {}
    for row in rows:
        dag_id = row.get("dag_id")
        try:
            paused = parse_airflow_bool(row.get("is_paused"))
        except Exception:
            raise InventoryError("inventory_invalid_output") from None
        if not isinstance(dag_id, str) or not dag_id:
            raise InventoryError("inventory_invalid_output")
        if dag_id not in target.dag_allowlist:
            continue
        if dag_id in parsed:
            if parsed[dag_id] != paused:
                raise InventoryError("inventory_invalid_output")
            continue
        parsed[dag_id] = paused
    if set(parsed) != set(target.dag_allowlist):
        raise InventoryError("inventory_invalid_output")
    return {dag_id: parsed[dag_id] for dag_id in sorted(parsed)}


def _run_count(target: DeployTarget, runner: CommandRunner, dag_id: str, state: str) -> int:
    argv = [
        *_compose_prefix(target),
        "exec",
        "-T",
        _target_argv_value(target.control_service),
        "airflow",
        "dags",
        "list-runs",
        "--state",
        state,
        "-o",
        "json",
        dag_id,
    ]
    rows = _airflow_rows(_checked(target, runner, argv))
    run_ids: set[str] = set()
    for row in rows:
        result_dag_id = row.get("dag_id")
        run_id = row.get("run_id")
        result_state = row.get("state")
        if result_dag_id != dag_id or not isinstance(run_id, str) or not run_id or result_state != state:
            raise InventoryError("inventory_invalid_output")
        if run_id in run_ids:
            raise InventoryError("inventory_invalid_output")
        run_ids.add(run_id)
    return len(run_ids)


def _ledger_fingerprint(ledger_reader: LedgerReader) -> str:
    try:
        summary = ledger_reader.read_summary()
        if not isinstance(summary, Mapping) or set(summary) != {"baseline", "previous_success"}:
            raise InventoryError("inventory_invalid_ledger")
        if any(value is not None and not isinstance(value, str) for value in summary.values()):
            raise InventoryError("inventory_invalid_ledger")
        reject_sensitive_artifact(summary)
        return sha256_hex(canonical_bytes(summary))
    except InventoryError:
        raise
    except (SensitiveArtifactError, TypeError, ValueError):
        raise InventoryError("inventory_invalid_ledger") from None
    except Exception:
        raise InventoryError("inventory_invalid_ledger") from None


def collect_read_only_inventory(
    target: DeployTarget, runner: CommandRunner, ledger_reader: LedgerReader
) -> ReleaseInventory:
    """Collect only the validated Compose and Airflow list/read probes."""
    _validate_target_argv(target)
    configured = _configured_services(target, runner)
    services = _service_states(target, runner, configured)
    paused = _dag_paused(target, runner)
    run_counts = {"running": 0, "queued": 0}
    for dag_id in sorted(target.writer_dag_allowlist):
        for state in ("running", "queued"):
            run_counts[state] += _run_count(target, runner, dag_id, state)
    return ReleaseInventory(
        service_states=services,
        dag_paused=paused,
        run_counts=run_counts,
        ledger_fingerprint=_ledger_fingerprint(ledger_reader),
    )


def sanitize_inventory(inventory: ReleaseInventory) -> dict[str, object]:
    """Publish only logical service names, aggregate counts, and digests."""
    counts = {
        "dags": len(inventory.dag_paused),
        "paused_dags": sum(inventory.dag_paused.values()),
        "running_runs": inventory.run_counts.get("running", 0),
        "queued_runs": inventory.run_counts.get("queued", 0),
    }
    fingerprint_input = {
        "service_states": dict(sorted(inventory.service_states.items())),
        "dag_paused": dict(sorted(inventory.dag_paused.items())),
        "run_counts": dict(sorted(inventory.run_counts.items())),
        "ledger_fingerprint": inventory.ledger_fingerprint,
    }
    sanitized = {
        "services": sorted(inventory.service_states),
        "counts": counts,
        "inventory_fingerprint": sha256_hex(canonical_bytes(fingerprint_input)),
        "ledger_fingerprint": inventory.ledger_fingerprint,
    }
    reject_sensitive_artifact(sanitized)
    return sanitized
