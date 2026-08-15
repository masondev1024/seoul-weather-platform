from __future__ import annotations

import base64
import json
import os
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from contextlib import suppress
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath
from typing import NoReturn

from deployment.airflow_cli_compat import probe_airflow_cli_contract
from deployment.command import CommandRunner, CompletedCommand
from deployment.compose_adapter import ComposeCommandAdapter
from deployment.ledger import DeploymentLedger
from deployment.models import BaselineRecord, WriterRunCounts
from deployment.overlay import AtomicOverlayStore, OverlayArtifact, render_baseline_overlay
from deployment.output_contracts import (
    parse_airflow_bool,
    parse_airflow_json_rows,
    parse_compose_json_rows,
)
from deployment.runtime_environment import (
    RuntimeEnvironmentError,
    validate_runtime_environment,
)
from deployment.target import (
    DeployTarget,
    load_deploy_target,
    public_target_summary,
    target_fingerprint,
)


_COMMANDS = frozenset({"inspect", "activate"})
_SHA64 = re.compile(r"^[0-9a-f]{64}$")
_BASELINE_ID = "baseline://existing-local"
_BASELINE_SCHEMA_VERSION = "weather-local-baseline-record/v1"
_UNSAFE = re.compile(r"[|;&`$<>\x00-\x1f\x7f]")


class CutoverCliError(RuntimeError):
    def __init__(self, command: str, stage: str, category: str, *, code: int = 1) -> None:
        self.command = command
        self.stage = stage
        self.category = category
        self.code = code
        super().__init__(category)

    @property
    def line(self) -> str:
        return f"{self.command}:{self.stage}:{self.category}"


@dataclass(frozen=True)
class _Invocation:
    command: str
    target_path: Path
    install_target_path: Path | None
    confirm_target_fingerprint: str | None
    confirm_baseline_sha256: str | None


class SystemClock:
    def utc_now(self) -> str:
        import time

        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _fail(command: str, stage: str, category: str, *, code: int = 1) -> NoReturn:
    raise CutoverCliError(command, stage, category, code=code)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _absolute_file_arg(command: str, value: str, option: str) -> Path:
    try:
        path = Path(value)
        if not path.is_absolute() or ".." in path.parts:
            raise ValueError
        if option == "--target-path" and not path.is_file():
            raise ValueError
        if option == "--install-target-path" and path == Path(path.anchor):
            raise ValueError
        return path
    except Exception:
        _fail(command, "usage", "invalid-arguments", code=2)


def _parse(argv: list[str]) -> _Invocation:
    if not argv or argv[0] not in _COMMANDS:
        _fail("cutover", "usage", "invalid-command", code=2)
    command = argv[0]
    expected = {"--target-path"} if command == "inspect" else {
        "--target-path",
        "--install-target-path",
        "--confirm-target-fingerprint",
        "--confirm-baseline-sha256",
    }
    if len(argv) != 1 + len(expected) * 2:
        _fail(command, "usage", "invalid-arguments", code=2)
    seen: dict[str, str] = {}
    index = 1
    while index < len(argv):
        option = argv[index]
        if option not in expected or option in seen or index + 1 >= len(argv):
            _fail(command, "usage", "invalid-arguments", code=2)
        value = argv[index + 1]
        if value.startswith("--"):
            _fail(command, "usage", "invalid-arguments", code=2)
        seen[option] = value
        index += 2
    if set(seen) != expected:
        _fail(command, "usage", "invalid-arguments", code=2)
    target_path = _absolute_file_arg(command, seen["--target-path"], "--target-path")
    if command == "inspect":
        return _Invocation(command, target_path, None, None, None)
    install_path = _absolute_file_arg(
        command, seen["--install-target-path"], "--install-target-path"
    )
    target_fp = seen["--confirm-target-fingerprint"]
    baseline_sha = seen["--confirm-baseline-sha256"]
    if not _SHA64.fullmatch(target_fp) or not _SHA64.fullmatch(baseline_sha):
        _fail(command, "usage", "invalid-arguments", code=2)
    return _Invocation(command, target_path, install_path, target_fp, baseline_sha)


def _load_target(command: str, path: Path) -> DeployTarget:
    try:
        return load_deploy_target(path, _repo_root())
    except Exception:
        _fail(command, "target", "invalid-target")


def _safe_atom(value: object) -> str:
    if type(value) is not str or not value or value.startswith("-") or _UNSAFE.search(value):
        raise ValueError
    return value


def _airflow_rows(stdout: str) -> list[Mapping[str, object]]:
    try:
        return parse_airflow_json_rows(stdout)
    except Exception:
        raise ValueError from None


def _compose_rows(stdout: str) -> list[Mapping[str, object]]:
    try:
        return parse_compose_json_rows(stdout)
    except Exception:
        raise ValueError from None


def _path_identity(path: Path | PurePath) -> tuple[str, str]:
    raw = str(path)
    windows = PureWindowsPath(raw)
    if windows.drive or "\\" in raw:
        return ("windows", str(windows).replace("/", "\\").casefold())
    return ("posix", str(PurePosixPath(raw)))


def _path_identities(path: Path | PurePath) -> frozenset[tuple[str, str]]:
    identities = {_path_identity(path)}
    if isinstance(path, Path):
        with suppress(OSError):
            identities.add(_path_identity(path.resolve(strict=False)))
    else:
        with suppress(OSError):
            identities.add(_path_identity(Path(str(path)).resolve(strict=False)))
    return frozenset(identities)


def _is_same_or_child(child: Path | PurePath, parent: Path | PurePath) -> bool:
    child_kind, child_raw = _path_identity(child)
    parent_kind, parent_raw = _path_identity(parent)
    if child_kind != parent_kind:
        return False
    child_parts = PureWindowsPath(child_raw).parts if child_kind == "windows" else PurePosixPath(child_raw).parts
    parent_parts = PureWindowsPath(parent_raw).parts if parent_kind == "windows" else PurePosixPath(parent_raw).parts
    return len(child_parts) >= len(parent_parts) and child_parts[: len(parent_parts)] == parent_parts


def _path_variants(path: Path | PurePath) -> frozenset[Path | PurePath]:
    variants: set[Path | PurePath] = {path}
    with suppress(OSError, RuntimeError):
        variants.add(Path(str(path)).resolve(strict=False))
    return frozenset(variants)


def _has_symlink_or_junction_parent(path: Path) -> bool:
    current = path.parent
    checked: set[Path] = set()
    while current not in checked:
        checked.add(current)
        if current.exists():
            if current.is_symlink():
                return True
            is_junction = getattr(current, "is_junction", None)
            if callable(is_junction) and is_junction():
                return True
        if current == current.parent:
            return False
        current = current.parent
    return True


def _validate_install_destination(command: str, source: Path, destination: Path, target: DeployTarget) -> None:
    try:
        if (
            not destination.is_absolute()
            or destination == Path(destination.anchor)
            or destination.suffix.lower() != ".json"
            or ".." in destination.parts
        ):
            raise ValueError
        if destination.exists() and (destination.is_symlink() or destination.is_dir()):
            raise ValueError
        if _has_symlink_or_junction_parent(destination):
            raise ValueError
        repo_root = _repo_root().resolve(strict=False)
        try:
            destination.resolve(strict=False).relative_to(repo_root)
        except ValueError:
            pass
        else:
            raise ValueError
        reserved: list[Path | PurePath] = [
            source,
            target.generated_overlay_file,
            target.lock_file,
            *target.compose_files,
        ]
        destination_ids = _path_identities(destination)
        if any(destination_ids & _path_identities(item) for item in reserved):
            raise ValueError
        for parent in (target.ledger_directory, target.dags_host_path, target.dbt_host_path):
            if any(
                _is_same_or_child(destination_variant, parent_variant)
                for destination_variant in _path_variants(destination)
                for parent_variant in _path_variants(parent)
            ):
                raise ValueError
    except Exception:
        _fail(command, "install-target", "invalid-destination")


class _BaseComposeReadAdapter:
    def __init__(self, target: DeployTarget, runner: CommandRunner) -> None:
        self._target = target
        self._runner = runner
        self._cwd = Path(str(target.working_directory))
        prefix: list[str] = ["docker", "compose", "-p", _safe_atom(target.project_name)]
        for compose_file in target.compose_files:
            prefix.extend(("-f", str(compose_file)))
        self._prefix = tuple(prefix)
        self._services = tuple(sorted(target.airflow_code_services))

    def code_service_status(self) -> dict[str, dict[str, str]]:
        result = self._runner.run((*self._prefix, "ps", "--format", "json", *self._services), self._cwd)
        if result.returncode != 0 or result.stderr:
            raise ValueError
        status: dict[str, dict[str, str]] = {}
        for row in _compose_rows(result.stdout):
            service = row.get("Service")
            health = row.get("Health")
            if (
                type(service) is not str
                or service not in self._target.airflow_code_services
                or service in status
                or row.get("State") != "running"
                or health not in {"", "healthy"}
            ):
                raise ValueError
            status[service] = {
                "state": "running",
                "health": "healthy" if health == "healthy" else "not-configured",
            }
        if set(status) != set(self._services):
            raise ValueError
        return {service: status[service] for service in self._services}

    def code_services_healthy(self) -> bool:
        self.code_service_status()
        return True


class _BaseAirflowReadAdapter:
    def __init__(self, target: DeployTarget, runner: CommandRunner) -> None:
        self._target = target
        self._runner = runner
        self._cwd = Path(str(target.working_directory))
        prefix: list[str] = ["docker", "compose", "-p", _safe_atom(target.project_name)]
        for compose_file in target.compose_files:
            prefix.extend(("-f", str(compose_file)))
        prefix.extend(("exec", "-T", _safe_atom(target.control_service), "airflow"))
        self._prefix = tuple(prefix)
        self._dags = tuple(sorted(target.dag_allowlist))
        self._writers = tuple(sorted(target.writer_dag_allowlist))

    def _checked(self, argv: Sequence[str]) -> str:
        result: CompletedCommand = self._runner.run(argv, self._cwd)
        if result.returncode != 0 or result.stderr:
            raise ValueError
        return result.stdout

    def capture_pause_state(self, dag_ids: tuple[str, ...]) -> dict[str, bool]:
        if dag_ids != self._dags:
            raise ValueError
        paused: dict[str, bool] = {}
        for row in _airflow_rows(self._checked((*self._prefix, "dags", "list", "-o", "json"))):
            dag_id = row.get("dag_id")
            try:
                is_paused = parse_airflow_bool(row.get("is_paused"))
            except Exception:
                raise ValueError from None
            if type(dag_id) is not str or not dag_id:
                raise ValueError
            if dag_id not in self._target.dag_allowlist:
                continue
            if dag_id in paused:
                if paused[dag_id] != is_paused:
                    raise ValueError
                continue
            paused[dag_id] = is_paused
        if tuple(sorted(paused)) != self._dags:
            raise ValueError
        return {dag_id: paused[dag_id] for dag_id in self._dags}

    def writer_run_counts(self, dag_ids: tuple[str, ...]) -> WriterRunCounts:
        if dag_ids != self._writers:
            raise ValueError
        totals = {"running": 0, "queued": 0}
        for dag_id in self._writers:
            for state in ("running", "queued"):
                rows = _airflow_rows(
                    self._checked(
                        (
                            *self._prefix,
                            "dags",
                            "list-runs",
                            "--state",
                            state,
                            "-o",
                            "json",
                            dag_id,
                        )
                    )
                )
                seen: set[str] = set()
                for row in rows:
                    run_id = row.get("run_id")
                    if (
                        row.get("dag_id") != dag_id
                        or row.get("state") != state
                        or type(run_id) is not str
                        or not run_id
                        or run_id in seen
                    ):
                        raise ValueError
                    seen.add(run_id)
                totals[state] += len(seen)
        return WriterRunCounts(running=totals["running"], queued=totals["queued"])


def _build_read_components(
    target: DeployTarget,
) -> tuple[CommandRunner, _BaseAirflowReadAdapter, _BaseComposeReadAdapter]:
    runner = CommandRunner(timeout_seconds=60)
    return runner, _BaseAirflowReadAdapter(target, runner), _BaseComposeReadAdapter(target, runner)


def _inspect_state(command: str, target: DeployTarget) -> dict[str, object]:
    try:
        environment = validate_runtime_environment(target, _repo_root())
    except RuntimeEnvironmentError:
        _fail(command, "environment", "runtime-environment-invalid")
    try:
        baseline = render_baseline_overlay(target)
        probe_runner, airflow, compose = _build_read_components(target)
        contract = probe_airflow_cli_contract(
            target,
            probe_runner,
            mode="base-only-cutover",
            overlay_file=None,
        )
        code_status = compose.code_service_status()
        code_services_ok = all(
            set(status) == {"state", "health"}
            and status["state"] == "running"
            and status["health"] in {"healthy", "not-configured"}
            for status in code_status.values()
        ) and set(code_status) == set(target.airflow_code_services)
        if not code_services_ok:
            raise ValueError
        paused = airflow.capture_pause_state(tuple(sorted(target.dag_allowlist)))
        counts = airflow.writer_run_counts(tuple(sorted(target.writer_dag_allowlist)))
        return {
            "target": public_target_summary(target),
            "target_fingerprint": target_fingerprint(target),
            "baseline_overlay_sha256": baseline.sha256,
            "airflow_cli_fingerprint": contract.capability_fingerprint,
            "compose_code_services_ok": code_services_ok,
            "code_service_states": {
                service: code_status[service]["state"] for service in sorted(code_status)
            },
            "code_service_health": {
                service: code_status[service]["health"] for service in sorted(code_status)
            },
            "dag_inventory_ok": set(paused) == set(target.dag_allowlist),
            "dag_paused": {dag_id: paused[dag_id] for dag_id in sorted(paused)},
            "writer_running": counts.running,
            "writer_queued": counts.queued,
            "stable_overlay_exists": Path(str(target.generated_overlay_file)).is_file(),
            "compose_environment_ready": environment.compose_environment_ready,
            "writes": {"dbt": 0, "trino": 0, "d1": 0, "r2": 0},
        }
    except CutoverCliError:
        raise
    except Exception:
        _fail(command, "inspect", "cutover-inspect-failed")


def _assert_operator_context(command: str) -> None:
    if os.environ.get("GITHUB_ACTIONS") == "true":
        _fail(command, "gate", "invalid-environment")


def _install_target_file(
    command: str, destination: Path, canonical_target_bytes: bytes, confirmed_fingerprint: str
) -> None:
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        tmp = destination.with_name(f".{destination.name}.tmp")
        with open(tmp, "wb") as handle:
            handle.write(canonical_target_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, destination)
        installed = load_deploy_target(destination, _repo_root())
        if target_fingerprint(installed) != confirmed_fingerprint:
            raise ValueError
    except Exception:
        _fail(command, "install-target", "target-install-failed")


def _baseline_record(target: DeployTarget, artifact: OverlayArtifact, captured_at: str) -> BaselineRecord:
    return BaselineRecord(
        schema_version=_BASELINE_SCHEMA_VERSION,
        baseline_id=_BASELINE_ID,
        target_fingerprint=target_fingerprint(target),
        captured_at=captured_at,
        rehearsal="passed",
        overlay_content_b64=base64.b64encode(artifact.content).decode("ascii"),
        overlay_sha256=artifact.sha256,
    )


def _activate(invocation: _Invocation) -> dict[str, object]:
    _assert_operator_context("activate")
    target = _load_target("activate", invocation.target_path)
    baseline = render_baseline_overlay(target)
    fingerprint = target_fingerprint(target)
    if (
        invocation.confirm_target_fingerprint != fingerprint
        or invocation.confirm_baseline_sha256 != baseline.sha256
        or invocation.install_target_path is None
    ):
        _fail("activate", "confirm", "confirmation-mismatch")
    _validate_install_destination(
        "activate", invocation.target_path, invocation.install_target_path, target
    )

    overlay = AtomicOverlayStore(target)
    ledger = DeploymentLedger(Path(str(target.ledger_directory)), Path(str(target.lock_file)))
    compose = ComposeCommandAdapter(target, CommandRunner(timeout_seconds=900))
    clock = SystemClock()
    lock_id = f"cutover-{fingerprint}"
    try:
        with ledger.acquire_lock(lock_id):
            confirmed_target = _load_target("activate", invocation.target_path)
            confirmed_baseline = render_baseline_overlay(confirmed_target)
            confirmed_fingerprint = target_fingerprint(confirmed_target)
            if (
                confirmed_fingerprint != fingerprint
                or confirmed_target.canonical_target_bytes != target.canonical_target_bytes
                or confirmed_baseline.sha256 != baseline.sha256
            ):
                _fail("activate", "confirm", "confirmation-mismatch")
            _inspect_state("activate", confirmed_target)
            staged: Path | None = None
            try:
                staged = overlay.stage(confirmed_baseline)
                compose.validate_candidate(confirmed_target, staged)
                overlay.install(staged, confirmed_baseline)
                staged = None
            except Exception:
                if staged is not None:
                    with suppress(Exception):
                        overlay.discard(staged)
                raise
            overlay.restore(baseline.content, baseline.sha256)
            overlay.verify_installed(baseline)
            ledger.record_baseline(
                _baseline_record(confirmed_target, confirmed_baseline, clock.utc_now())
            )
            _install_target_file(
                "activate",
                invocation.install_target_path,
                confirmed_target.canonical_target_bytes,
                confirmed_fingerprint,
            )
    except CutoverCliError:
        raise
    except Exception:
        _fail("activate", "baseline", "cutover-activation-failed")
    return {
        "activated": True,
        "target_fingerprint": fingerprint,
        "baseline_overlay_sha256": baseline.sha256,
        "baseline_id": _BASELINE_ID,
        "runner_started": False,
        "github_state_changed": False,
        "airflow_state_changed": False,
        "code_services_deployed": False,
    }


def main(argv: list[str] | None = None) -> int:
    try:
        invocation = _parse(list(sys.argv[1:] if argv is None else argv))
        _assert_operator_context(invocation.command)
        target = _load_target(invocation.command, invocation.target_path)
        if invocation.command == "inspect":
            print(json.dumps(_inspect_state("inspect", target), sort_keys=True, separators=(",", ":")))
            return 0
        print(json.dumps(_activate(invocation), sort_keys=True, separators=(",", ":")))
        return 0
    except CutoverCliError as exc:
        print(exc.line, file=sys.stderr)
        return exc.code


if __name__ == "__main__":
    raise SystemExit(main())
