from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath

import yaml

from deployment.command import CommandRunner, CompletedCommand
from deployment.overlay import validate_overlay_content
from deployment.target import DeployTarget


class ComposeAdapterError(RuntimeError):
    """A fixed, redacted Docker Compose adapter failure category."""


_UNSAFE = re.compile(r"[|;&`$<>\x00-\x1f\x7f]")
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")
_DRY_RUN_CONTAINER = re.compile(
    r"^(?:[✔✓]\s+)?DRY-RUN MODE - Container\s+"
    r"([A-Za-z0-9][A-Za-z0-9_.-]*)\s+"
    r"(?:Create|Created|Creating|Recreate|Recreated|Recreating|Running|Start|Started|Starting|Healthy)"
    r"(?:\s+\d+(?:\.\d+)?s)?$"
)
_DRY_RUN_PROGRESS = re.compile(r"^\[\+\]\s+Running\s+\d+/\d+$")
_DRY_RUN_V5_CONTAINER = re.compile(
    r"^Container\s+(?:(?P<prefix>[0-9a-f]{12})_)?"
    r"(?P<container>[A-Za-z0-9][A-Za-z0-9_.-]*)\s+"
    r"(?P<status>Recreate|Recreated|Starting|Started|Waiting|Healthy)$"
)
_DEPLOY_PROGRESS = re.compile(r"^\[\+\]\s+Running\s+\d+/\d+$")
_DEPLOY_CONTAINER = re.compile(
    r"^(?:[✔✓]\s+)?Container\s+"
    r"(?P<container>[A-Za-z0-9][A-Za-z0-9_.-]*)\s+"
    r"(?P<status>Recreate|Recreated|Creating|Created|Starting|Started|Running|Waiting|Healthy)"
    r"(?:\s+\d+(?:\.\d+)?s)?$"
)


def _compose_volume_dict(volume: Mapping[str, object], expected: Mapping[str, object]) -> dict[str, object]:
    normalized = dict(volume)
    if "read_only" in expected and type(expected["read_only"]) is not bool:
        raise ValueError
    if "read_only" in normalized and type(normalized["read_only"]) is not bool:
        raise ValueError
    if expected.get("read_only") is False and "read_only" not in normalized:
        normalized["read_only"] = False
    return normalized


def _safe_atom(value: object) -> str:
    if type(value) is not str or not value or value.startswith("-") or _UNSAFE.search(value):
        raise ComposeAdapterError("compose_adapter_input_rejected")
    return value


def _safe_path(value: PurePath) -> str:
    raw = _safe_atom(str(value))
    parts = re.split(r"[\\/]", raw)
    if "." in parts or ".." in parts:
        raise ComposeAdapterError("compose_adapter_input_rejected")
    if not PureWindowsPath(raw).is_absolute() and not PurePosixPath(raw).is_absolute():
        raise ComposeAdapterError("compose_adapter_input_rejected")
    return raw


class ComposeCommandAdapter:
    def __init__(self, target: DeployTarget, runner: CommandRunner) -> None:
        self._target = target
        self._runner = runner
        project = _safe_atom(target.project_name)
        self._cwd = Path(_safe_path(target.working_directory))
        self._stable = Path(_safe_path(target.generated_overlay_file))
        self._services = tuple(sorted(target.airflow_code_services))
        self._configured = frozenset(
            target.airflow_code_services | target.forbidden_data_services
        )
        for service in self._configured:
            _safe_atom(service)
            if "airflow-init" in service.casefold():
                raise ComposeAdapterError("compose_adapter_input_rejected")
        prefix: list[str] = ["docker", "compose", "-p", project]
        for compose_file in target.compose_files:
            prefix.extend(("-f", _safe_path(compose_file)))
        self._base_prefix = tuple(prefix)

    def _checked(
        self,
        argv: Sequence[str],
        *,
        allowed_progress_containers: frozenset[str] | None = None,
    ) -> str:
        try:
            result: CompletedCommand = self._runner.run(argv, self._cwd)
        except Exception:
            raise ComposeAdapterError("compose_adapter_command_failed") from None
        if result.returncode != 0:
            raise ComposeAdapterError("compose_adapter_command_failed")
        if result.stderr and (
            allowed_progress_containers is None
            or not self._allowed_deploy_progress(result.stderr, allowed_progress_containers)
        ):
            raise ComposeAdapterError("compose_adapter_command_failed")
        return result.stdout

    @staticmethod
    def _allowed_deploy_progress(stderr: str, expected_containers: frozenset[str]) -> bool:
        if type(stderr) is not str or not stderr or not expected_containers:
            return False
        seen: set[str] = set()
        for raw_line in stderr.splitlines():
            line = _ANSI_ESCAPE.sub("", raw_line).strip()
            if not line:
                continue
            if _DEPLOY_PROGRESS.fullmatch(line):
                continue
            match = _DEPLOY_CONTAINER.fullmatch(line)
            if match is None or match.group("container") not in expected_containers:
                return False
            seen.add(match.group("container"))
        return seen == set(expected_containers)

    def _code_container_names(self, config: Mapping[str, object]) -> frozenset[str]:
        try:
            services = config["services"]
            if not isinstance(services, Mapping):
                raise ValueError
            names: set[str] = set()
            for service in self._services:
                body = services[service]
                if not isinstance(body, Mapping):
                    raise ValueError
                configured = body.get("container_name")
                container = configured if type(configured) is str and configured else f"{self._target.project_name}-{service}-1"
                _safe_atom(container)
                names.add(container)
            if len(names) != len(self._services):
                raise ValueError
            return frozenset(names)
        except Exception:
            raise ComposeAdapterError("compose_adapter_config_rejected") from None

    def _dry_run_checked(self, argv: Sequence[str]) -> str:
        try:
            result: CompletedCommand = self._runner.run(argv, self._cwd)
        except Exception:
            raise ComposeAdapterError("compose_adapter_command_failed") from None
        if (
            result.returncode != 0
            or type(result.stdout) is not str
            or type(result.stderr) is not str
        ):
            raise ComposeAdapterError("compose_adapter_command_failed")
        streams = [
            stream.rstrip("\r\n")
            for stream in (result.stdout, result.stderr)
            if stream
        ]
        return "\n".join(streams)

    def _config(self, prefix: tuple[str, ...]) -> Mapping[str, object]:
        stdout = self._checked((*prefix, "config", "--format", "json"))
        try:
            payload = json.loads(stdout)
        except (TypeError, ValueError):
            raise ComposeAdapterError("compose_adapter_config_rejected") from None
        if not isinstance(payload, Mapping):
            raise ComposeAdapterError("compose_adapter_config_rejected")
        name = payload.get("name")
        services = payload.get("services")
        if name != self._target.project_name or not isinstance(services, Mapping):
            raise ComposeAdapterError("compose_adapter_config_rejected")
        configured_services = set(services)
        required = set(self._target.airflow_code_services)
        allowed = set(self._configured) | {"airflow-init"}
        if not required <= configured_services or not configured_services <= allowed:
            raise ComposeAdapterError("compose_adapter_config_rejected")
        if any(not isinstance(body, Mapping) for body in services.values()):
            raise ComposeAdapterError("compose_adapter_config_rejected")
        return payload

    def _candidate_file(self, overlay_file: PurePath) -> Path:
        try:
            raw = Path(_safe_path(overlay_file))
            if raw.is_symlink():
                raise ValueError
            if raw.parent.resolve(strict=False) != self._stable.parent.resolve(strict=False):
                raise ValueError
            if (
                not raw.name.startswith(f".{self._stable.name}.")
                or not raw.name.endswith(".tmp")
                or raw == self._stable
            ):
                raise ValueError
            return raw
        except ComposeAdapterError:
            raise
        except Exception:
            raise ComposeAdapterError("compose_adapter_candidate_rejected") from None

    def validate_candidate(self, target: DeployTarget, overlay_file: PurePath) -> None:
        if target != self._target:
            raise ComposeAdapterError("compose_adapter_input_rejected")
        staged = self._candidate_file(overlay_file)
        try:
            content = staged.read_bytes()
            artifact = validate_overlay_content(
                self._target, content, hashlib.sha256(content).hexdigest()
            )
        except Exception:
            raise ComposeAdapterError("compose_adapter_candidate_rejected") from None

        base = self._config(self._base_prefix)
        candidate_prefix = (*self._base_prefix, "-f", str(staged))
        candidate = self._config(candidate_prefix)
        self._validate_config_pair(base, candidate, artifact.content)
        dry_output = self._dry_run_checked(
            (
                *candidate_prefix,
                "--ansi",
                "never",
                "--progress",
                "plain",
                "--dry-run",
                "up",
                "-d",
                "--no-deps",
                "--no-build",
                "--pull",
                "never",
                *self._services,
            )
        )
        self._validate_dry_run(candidate, dry_output)

    def _validate_config_pair(
        self,
        base: Mapping[str, object],
        candidate: Mapping[str, object],
        overlay_content: bytes,
    ) -> None:
        try:
            base_services = base["services"]
            candidate_services = candidate["services"]
            if not isinstance(base_services, Mapping) or not isinstance(candidate_services, Mapping):
                raise ValueError
            if set(candidate_services) != set(base_services):
                raise ValueError
            for service in set(base_services) - set(self._services):
                if candidate_services[service] != base_services[service]:
                    raise ValueError
            overlay = yaml.safe_load(overlay_content)
            overlay_services = overlay["services"]
            for service in self._services:
                base_body = base_services[service]
                body = candidate_services[service]
                overlay_body = overlay_services[service]
                if (
                    not isinstance(base_body, Mapping)
                    or not isinstance(body, Mapping)
                    or not isinstance(overlay_body, Mapping)
                ):
                    raise ValueError
                volumes = body.get("volumes")
                if not isinstance(volumes, list):
                    raise ValueError
                expected_volumes = overlay_body["volumes"]
                for expected in expected_volumes:
                    matches = [
                        volume
                        for volume in volumes
                        if isinstance(volume, Mapping)
                        and volume.get("target") == expected["target"]
                    ]
                    if len(matches) != 1 or _compose_volume_dict(matches[0], expected) != expected:
                        raise ValueError
                expected_environment = overlay_body.get("environment")
                if expected_environment is not None:
                    environment = body.get("environment")
                    if (
                        not isinstance(expected_environment, Mapping)
                        or not isinstance(environment, Mapping)
                        or any(
                            environment.get(key) != value
                            for key, value in expected_environment.items()
                        )
                    ):
                        raise ValueError
                    base_volumes = base_body.get("volumes")
                    if not isinstance(base_volumes, list):
                        raise ValueError
                    base_logs = [
                        volume
                        for volume in base_volumes
                        if isinstance(volume, Mapping)
                        and volume.get("target") == "/opt/airflow/logs"
                    ]
                    candidate_logs = [
                        volume
                        for volume in volumes
                        if isinstance(volume, Mapping)
                        and volume.get("target") == "/opt/airflow/logs"
                    ]
                    if (
                        len(base_logs) != 1
                        or len(candidate_logs) != 1
                        or dict(candidate_logs[0]) != dict(base_logs[0])
                        or candidate_logs[0].get("read_only") is True
                    ):
                        raise ValueError
        except Exception:
            raise ComposeAdapterError("compose_adapter_config_rejected") from None

    def _validate_dry_run(
        self, candidate: Mapping[str, object], output: str
    ) -> None:
        try:
            services = candidate["services"]
            if not isinstance(services, Mapping):
                raise ValueError
            by_container: dict[str, str] = {}
            for service, body in services.items():
                if type(service) is not str or not isinstance(body, Mapping):
                    raise ValueError
                container = body.get("container_name", f"{self._target.project_name}-{service}-1")
                if type(container) is not str or not container or container in by_container:
                    raise ValueError
                by_container[container] = service
            lines = [
                _ANSI_ESCAPE.sub("", line).strip()
                for line in output.splitlines()
                if line.strip()
            ]
            if not lines:
                raise ValueError
            seen: set[str] = set()
            progress_lines = 0
            classic_output = True
            for line in lines:
                if _DRY_RUN_PROGRESS.fullmatch(line):
                    progress_lines += 1
                    continue
                match = _DRY_RUN_CONTAINER.fullmatch(line)
                if match is None:
                    classic_output = False
                    break
                service = by_container.get(match.group(1))
                if service not in self._target.airflow_code_services:
                    raise ValueError
                seen.add(service)
            if classic_output:
                if progress_lines == 0 or seen != set(self._services):
                    raise ValueError
                return

            lifecycle = {
                service: {"base": [], "temporary": []}
                for service in self._services
            }
            prefixes: dict[str, str] = {}
            for line in lines:
                match = _DRY_RUN_V5_CONTAINER.fullmatch(line)
                if match is None:
                    raise ValueError
                service = by_container.get(match.group("container"))
                if service not in self._target.airflow_code_services:
                    raise ValueError
                prefix = match.group("prefix")
                status = match.group("status")
                if prefix is None:
                    if status not in {"Recreate", "Recreated", "Waiting", "Healthy"}:
                        raise ValueError
                    lifecycle[service]["base"].append(status)
                else:
                    if status not in {"Starting", "Started", "Waiting", "Healthy"}:
                        raise ValueError
                    if service in prefixes and prefixes[service] != prefix:
                        raise ValueError
                    prefixes[service] = prefix
                    lifecycle[service]["temporary"].append(status)
            if len(set(prefixes.values())) != len(self._services):
                raise ValueError
            for states in lifecycle.values():
                base_states = states["base"]
                temporary_states = states["temporary"]
                if (
                    base_states.count("Recreate") != 1
                    or base_states.count("Recreated") != 1
                    or base_states.index("Recreate") > base_states.index("Recreated")
                ):
                    raise ValueError
                if (
                    temporary_states.count("Starting") != 1
                    or temporary_states.count("Started") != 1
                    or temporary_states.index("Starting")
                    > temporary_states.index("Started")
                ):
                    raise ValueError
                if any(
                    index < base_states.index("Recreated")
                    for index, status in enumerate(base_states)
                    if status in {"Waiting", "Healthy"}
                ):
                    raise ValueError
                if any(
                    index < temporary_states.index("Started")
                    for index, status in enumerate(temporary_states)
                    if status in {"Waiting", "Healthy"}
                ):
                    raise ValueError
        except Exception:
            raise ComposeAdapterError("compose_adapter_dry_run_rejected") from None

    def deploy_code_services(
        self, target: DeployTarget, overlay_file: PurePath, services: tuple[str, ...]
    ) -> None:
        if (
            target != self._target
            or type(services) is not tuple
            or services != self._services
            or Path(str(overlay_file)) != self._stable
        ):
            raise ComposeAdapterError("compose_adapter_input_rejected")
        try:
            if self._stable.is_symlink():
                raise ValueError
            content = self._stable.read_bytes()
            validate_overlay_content(
                self._target, content, hashlib.sha256(content).hexdigest()
            )
        except Exception:
            raise ComposeAdapterError("compose_adapter_candidate_rejected") from None
        config = self._config((*self._base_prefix, "-f", str(self._stable)))
        self._checked(
            (
                *self._base_prefix,
                "-f",
                str(self._stable),
                "up",
                "-d",
                "--no-deps",
                "--no-build",
                "--pull",
                "never",
                *self._services,
            ),
            allowed_progress_containers=self._code_container_names(config),
        )
