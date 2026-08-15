from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath

from deployment.command import CommandRunner, CompletedCommand
from deployment.overlay import OverlayArtifact, validate_overlay_content
from deployment.output_contracts import (
    parse_airflow_bool,
    parse_airflow_json_rows,
    parse_compose_json_rows,
)
from deployment.target import DeployTarget


class HealthAdapterError(RuntimeError):
    """A fixed, redacted deployment health failure category."""


_UNSAFE = re.compile(r"[|;&`$<>\x00-\x1f\x7f]")


def _safe_atom(value: object) -> str:
    if type(value) is not str or not value or value.startswith("-") or _UNSAFE.search(value):
        raise HealthAdapterError("health_adapter_input_rejected")
    return value


def _safe_path(value: PurePath) -> str:
    raw = _safe_atom(str(value))
    parts = re.split(r"[\\/]", raw)
    if "." in parts or ".." in parts:
        raise HealthAdapterError("health_adapter_input_rejected")
    if not PureWindowsPath(raw).is_absolute() and not PurePosixPath(raw).is_absolute():
        raise HealthAdapterError("health_adapter_input_rejected")
    return raw


def _airflow_rows(stdout: str) -> list[Mapping[str, object]]:
    try:
        return parse_airflow_json_rows(stdout)
    except Exception:
        raise HealthAdapterError("health_adapter_invalid_output") from None


def _compose_rows(stdout: str) -> list[Mapping[str, object]]:
    try:
        return parse_compose_json_rows(stdout)
    except Exception:
        raise HealthAdapterError("health_adapter_invalid_output") from None


def _object(stdout: str) -> Mapping[str, object]:
    try:
        payload = json.loads(stdout)
    except (TypeError, ValueError):
        raise HealthAdapterError("health_adapter_invalid_output") from None
    if not isinstance(payload, Mapping):
        raise HealthAdapterError("health_adapter_invalid_output")
    return payload


class HealthCommandAdapter:
    def __init__(self, target: DeployTarget, runner: CommandRunner) -> None:
        self._target = target
        self._runner = runner
        self._cwd = Path(_safe_path(target.working_directory))
        self._stable = Path(_safe_path(target.generated_overlay_file))
        self._services = tuple(sorted(target.airflow_code_services))
        self._dags = tuple(sorted(target.dag_allowlist))
        for service in target.airflow_code_services | target.forbidden_data_services:
            _safe_atom(service)
            if "airflow-init" in service.casefold():
                raise HealthAdapterError("health_adapter_input_rejected")
        for dag_id in target.dag_allowlist:
            _safe_atom(dag_id)
        base: list[str] = ["docker", "compose", "-p", _safe_atom(target.project_name)]
        for compose_file in target.compose_files:
            base.extend(("-f", _safe_path(compose_file)))
        base.extend(("-f", str(self._stable)))
        self._compose = tuple(base)
        self._airflow = (
            *self._compose,
            "exec",
            "-T",
            _safe_atom(target.control_service),
            "airflow",
        )

    def _checked(self, argv: Sequence[str]) -> str:
        try:
            result: CompletedCommand = self._runner.run(argv, self._cwd)
        except Exception:
            raise HealthAdapterError("health_adapter_command_failed") from None
        if result.returncode != 0 or result.stderr:
            raise HealthAdapterError("health_adapter_command_failed")
        return result.stdout

    def _bind_expected(self, expected: OverlayArtifact) -> None:
        try:
            if type(expected) is not OverlayArtifact or self._stable.is_symlink():
                raise ValueError
            validated = validate_overlay_content(
                self._target, expected.content, expected.sha256
            )
            if expected != validated:
                raise ValueError
            stable_content = self._stable.read_bytes()
            if stable_content != expected.content:
                raise ValueError
            if hashlib.sha256(stable_content).hexdigest() != expected.sha256:
                raise ValueError
            stable = validate_overlay_content(
                self._target, stable_content, expected.sha256
            )
            if stable != validated:
                raise ValueError
        except Exception:
            raise HealthAdapterError("health_adapter_overlay_mismatch") from None

    def read_health(
        self, target: DeployTarget, expected_overlay: OverlayArtifact
    ) -> str:
        if target != self._target:
            raise HealthAdapterError("health_adapter_input_rejected")
        self._bind_expected(expected_overlay)

        config = _object(self._checked((*self._compose, "config", "--format", "json")))
        try:
            if config.get("name") != self._target.project_name:
                raise ValueError
            config_services = config.get("services")
            if not isinstance(config_services, Mapping):
                raise ValueError
            required = set(self._target.airflow_code_services)
            allowed = required | set(
                self._target.forbidden_data_services
            ) | {"airflow-init"}
            actual = set(config_services)
            if not required <= actual or not actual <= allowed:
                raise ValueError
            health_required: dict[str, bool] = {}
            for service in self._services:
                body = config_services.get(service)
                if not isinstance(body, Mapping):
                    raise ValueError
                if "healthcheck" not in body:
                    health_required[service] = False
                    continue
                healthcheck = body["healthcheck"]
                if (
                    not isinstance(healthcheck, Mapping)
                    or healthcheck.get("disable") is True
                ):
                    raise ValueError
                test = healthcheck.get("test")
                if (
                    type(test) is not list
                    or not test
                    or any(type(item) is not str or not item for item in test)
                    or test[0] == "NONE"
                ):
                    raise ValueError
                health_required[service] = True
        except Exception:
            raise HealthAdapterError("health_adapter_invalid_output") from None

        service_rows = _compose_rows(
            self._checked(
                (*self._compose, "ps", "--format", "json", *self._services)
            )
        )
        seen_services: set[str] = set()
        for row in service_rows:
            service = row.get("Service")
            if (
                type(service) is not str
                or service not in self._target.airflow_code_services
                or service in seen_services
                or row.get("State") != "running"
                or row.get("Health") != ("healthy" if health_required[service] else "")
            ):
                raise HealthAdapterError("health_adapter_invalid_output")
            seen_services.add(service)
        if seen_services != set(self._services):
            raise HealthAdapterError("health_adapter_invalid_output")

        dag_rows = _airflow_rows(
            self._checked((*self._airflow, "dags", "list", "-o", "json"))
        )
        seen_dags: dict[str, bool] = {}
        for row in dag_rows:
            dag_id = row.get("dag_id")
            try:
                paused = parse_airflow_bool(row.get("is_paused"))
            except Exception:
                raise HealthAdapterError("health_adapter_invalid_output") from None
            if type(dag_id) is not str or not dag_id:
                raise HealthAdapterError("health_adapter_invalid_output")
            if dag_id not in self._target.dag_allowlist:
                continue
            if dag_id in seen_dags:
                if seen_dags[dag_id] != paused:
                    raise HealthAdapterError("health_adapter_invalid_output")
                continue
            seen_dags[dag_id] = paused
        if set(seen_dags) != set(self._dags):
            raise HealthAdapterError("health_adapter_invalid_output")

        import_errors = _airflow_rows(
            self._checked(
                (*self._airflow, "dags", "list-import-errors", "-o", "json")
            )
        )
        if import_errors:
            raise HealthAdapterError("health_adapter_invalid_output")
        return "passed"
