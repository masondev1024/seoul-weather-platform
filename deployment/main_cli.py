from __future__ import annotations

import hashlib
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn

from deployment.github_evidence import GithubEvidenceError, read_main_identity_inputs
from deployment.main_identity import (
    MainDeployIdentity,
    MainIdentityError,
    validate_main_deploy_identity,
)
from deployment.models import DeploymentOutcome, DeploymentResult
from tools.github_protection import SubprocessGhRunner


_MAX_EVENT_BYTES = 65_536
_COMMANDS = frozenset({"verify-main", "deploy-main"})

AirflowCommandAdapter: Any = None
AtomicOverlayStore: Any = None
CommandRunner: Any = None
ComposeCommandAdapter: Any = None
DeploymentLedger: Any = None
GitCommandAdapter: Any = None
HealthCommandAdapter: Any = None
MainDeploymentError: Any = None
MainDeploymentOrchestrator: Any = None
load_deploy_target: Any = None
probe_airflow_cli_contract: Any = None
validate_overlay_content: Any = None
validate_runtime_environment: Any = None


@dataclass(frozen=True)
class _Invocation:
    command: str
    event_path: str
    workflow_ref: str
    workflow_sha: str


class _PublicCliError(RuntimeError):
    def __init__(self, command: str, stage: str, category: str, *, code: int = 1) -> None:
        self.command = command
        self.stage = stage
        self.category = category
        self.code = code
        super().__init__(category)

    @property
    def line(self) -> str:
        return f"{self.command}:{self.stage}:{self.category}"


class SystemClock:
    def utc_now(self) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(max(0.0, min(float(seconds), 60.0)))


def _fail(command: str, stage: str, category: str, *, code: int = 1) -> NoReturn:
    raise _PublicCliError(command, stage, category, code=code)


def _parse(argv: list[str]) -> _Invocation:
    if len(argv) != 7 or not argv:
        _fail("main", "usage", "invalid-command", code=2)
    command = argv[0]
    if command not in _COMMANDS:
        _fail("main", "usage", "invalid-command", code=2)
    seen: dict[str, str] = {}
    index = 1
    while index < len(argv):
        option = argv[index]
        if option not in {"--event-path", "--workflow-ref", "--workflow-sha"}:
            _fail(command, "usage", "invalid-arguments", code=2)
        if option in seen or index + 1 >= len(argv):
            _fail(command, "usage", "invalid-arguments", code=2)
        value = argv[index + 1]
        if value.startswith("--"):
            _fail(command, "usage", "invalid-arguments", code=2)
        seen[option] = value
        index += 2
    if set(seen) != {"--event-path", "--workflow-ref", "--workflow-sha"}:
        _fail(command, "usage", "invalid-arguments", code=2)
    return _Invocation(
        command=command,
        event_path=seen["--event-path"],
        workflow_ref=seen["--workflow-ref"],
        workflow_sha=seen["--workflow-sha"],
    )


def _regular_size_bounded_absolute_file(value: str) -> bool:
    try:
        path = Path(value)
        return path.is_absolute() and path.is_file() and path.stat().st_size <= _MAX_EVENT_BYTES
    except OSError:
        return False


def _validate_gate(invocation: _Invocation) -> tuple[str, str, str]:
    env = os.environ
    repository = env.get("GITHUB_REPOSITORY", "")
    token = env.get("GH_TOKEN", "")
    governance_mode = env.get("GOVERNANCE_MODE", "")
    if (
        env.get("GITHUB_ACTIONS") != "true"
        or governance_mode not in {"protected", "guarded_private"}
        or env.get("DEPLOYMENT_ENABLED") != "enabled"
        or not token
        or not repository
        or invocation.workflow_ref
        != f"{repository}/.github/workflows/deploy-main.yml@refs/heads/main"
        or env.get("GITHUB_EVENT_PATH") != invocation.event_path
        or env.get("GITHUB_WORKFLOW_REF") != invocation.workflow_ref
        or env.get("GITHUB_WORKFLOW_SHA") != invocation.workflow_sha
        or not _regular_size_bounded_absolute_file(invocation.event_path)
    ):
        _fail(invocation.command, "gate", "invalid-environment")
    return repository, token, governance_mode


def _verify_identity(invocation: _Invocation) -> MainDeployIdentity:
    repository, token, governance_mode = _validate_gate(invocation)
    try:
        inputs = read_main_identity_inputs(
            event_path=invocation.event_path,
            workflow_ref=invocation.workflow_ref,
            workflow_sha=invocation.workflow_sha,
            repository=repository,
            governance_mode=governance_mode,
            gh_token=token,
            runner=SubprocessGhRunner(),
        )
    except GithubEvidenceError as exc:
        _fail(invocation.command, "evidence", exc.category)
    except Exception:
        _fail(invocation.command, "evidence", "invalid-github-evidence")
    try:
        return validate_main_deploy_identity(**inputs.as_kwargs())
    except MainIdentityError as exc:
        _fail(invocation.command, "identity", exc.category)
    except Exception:
        _fail(invocation.command, "identity", "invalid-main-deploy-identity")


def _load_runtime_symbols() -> None:
    global AirflowCommandAdapter
    global AtomicOverlayStore
    global CommandRunner
    global ComposeCommandAdapter
    global DeploymentLedger
    global GitCommandAdapter
    global HealthCommandAdapter
    global MainDeploymentError
    global MainDeploymentOrchestrator
    global load_deploy_target
    global probe_airflow_cli_contract
    global validate_overlay_content
    global validate_runtime_environment
    if AirflowCommandAdapter is not None and validate_runtime_environment is not None:
        return
    from deployment.airflow_adapter import AirflowCommandAdapter as _AirflowCommandAdapter
    from deployment.airflow_cli_compat import (
        probe_airflow_cli_contract as _probe_airflow_cli_contract,
    )
    from deployment.command import CommandRunner as _CommandRunner
    from deployment.compose_adapter import ComposeCommandAdapter as _ComposeCommandAdapter
    from deployment.git_adapter import GitCommandAdapter as _GitCommandAdapter
    from deployment.health_adapter import HealthCommandAdapter as _HealthCommandAdapter
    from deployment.ledger import DeploymentLedger as _DeploymentLedger
    from deployment.main_orchestrator import (
        MainDeploymentError as _MainDeploymentError,
        MainDeploymentOrchestrator as _MainDeploymentOrchestrator,
    )
    from deployment.overlay import (
        AtomicOverlayStore as _AtomicOverlayStore,
        validate_overlay_content as _validate_overlay_content,
    )
    from deployment.target import load_deploy_target as _load_deploy_target
    from deployment.runtime_environment import (
        validate_runtime_environment as _validate_runtime_environment,
    )

    AirflowCommandAdapter = _AirflowCommandAdapter
    AtomicOverlayStore = _AtomicOverlayStore
    CommandRunner = _CommandRunner
    ComposeCommandAdapter = _ComposeCommandAdapter
    DeploymentLedger = _DeploymentLedger
    GitCommandAdapter = _GitCommandAdapter
    HealthCommandAdapter = _HealthCommandAdapter
    MainDeploymentError = _MainDeploymentError
    MainDeploymentOrchestrator = _MainDeploymentOrchestrator
    load_deploy_target = _load_deploy_target
    probe_airflow_cli_contract = _probe_airflow_cli_contract
    validate_overlay_content = _validate_overlay_content
    validate_runtime_environment = _validate_runtime_environment


def _current_stable_overlay(target: object) -> object:
    try:
        content = Path(str(target.generated_overlay_file)).read_bytes()
        return validate_overlay_content(
            target, content, hashlib.sha256(content).hexdigest()
        )
    except Exception:
        raise ValueError("invalid-stable-overlay") from None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_target(command: str) -> object:
    raw_path = os.environ.get("WEATHER_DEPLOY_TARGET_PATH", "")
    if not raw_path:
        _fail(command, "target", "invalid-target")
    try:
        path = Path(raw_path)
        if not path.is_absolute():
            raise ValueError
        target = load_deploy_target(path, _repo_root())
    except Exception:
        _fail(command, "target", "invalid-target")
    try:
        validate_runtime_environment(target, _repo_root())
    except Exception:
        _fail(command, "target", "invalid-runtime-environment")
    return target


def _build_deploy_components(
    identity: MainDeployIdentity,
) -> tuple[object, object]:
    try:
        _load_runtime_symbols()
        target = _load_target("deploy-main")
        stable = _current_stable_overlay(target)
        overlay = AtomicOverlayStore(target)
        overlay.verify_installed(stable)
        probe_runner = CommandRunner(timeout_seconds=60)
        probe_airflow_cli_contract(
            target,
            probe_runner,
            mode="stable",
            overlay_file=target.generated_overlay_file,
        )
        airflow_runner = CommandRunner(timeout_seconds=60)
        airflow = AirflowCommandAdapter(target, airflow_runner)
        compose_runner = CommandRunner(timeout_seconds=900)
        compose = ComposeCommandAdapter(target, compose_runner)
        git_runner = CommandRunner(timeout_seconds=300)
        git = GitCommandAdapter(target, _repo_root(), git_runner)
        health_runner = CommandRunner(timeout_seconds=60)
        health = HealthCommandAdapter(target, health_runner)
        clock = SystemClock()
        ledger = DeploymentLedger(
            Path(str(target.ledger_directory)),
            Path(str(target.lock_file)),
        )
        orchestrator = MainDeploymentOrchestrator(
            airflow_read=airflow,
            airflow_mutation=airflow,
            compose=compose,
            git=git,
            health=health,
            clock=clock,
            overlay_store=overlay,
            ledger=ledger,
        )
        return orchestrator, target
    except _PublicCliError:
        raise
    except Exception:
        _fail("deploy-main", "cli", "invalid-cli")


def _deploy(invocation: _Invocation) -> str:
    identity = _verify_identity(invocation)
    orchestrator, target = _build_deploy_components(identity)
    try:
        result: DeploymentResult = orchestrator.deploy(identity, target)
    except Exception as exc:
        if MainDeploymentError is not None and isinstance(exc, MainDeploymentError):
            _fail("deploy-main", "deployment", exc.category)
        _fail("deploy-main", "deployment", "main-deploy-failed")
    if result.outcome is DeploymentOutcome.SUCCESS and result.idempotent:
        return "deploy-main:deployment:noop"
    if result.outcome is DeploymentOutcome.SUCCESS:
        return "deploy-main:deployment:success"
    _fail("deploy-main", "deployment", "main-deploy-failed")


def main(argv: list[str] | None = None) -> int:
    try:
        invocation = _parse(list(sys.argv[1:] if argv is None else argv))
        if invocation.command == "verify-main":
            _verify_identity(invocation)
            print("verify-main:identity:ok")
            return 0
        print(_deploy(invocation))
        return 0
    except _PublicCliError as exc:
        print(exc.line, file=sys.stderr)
        return exc.code


if __name__ == "__main__":
    raise SystemExit(main())
