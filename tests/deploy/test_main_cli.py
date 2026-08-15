from __future__ import annotations

import builtins
import importlib
import json
import socket
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

from deployment.models import DeploymentOutcome, DeploymentResult


REPOSITORY = "masondev1024/seoul-weather-platform"
SHA = "0123456789abcdef0123456789abcdef01234567"
WORKFLOW_REF = f"{REPOSITORY}/.github/workflows/deploy-main.yml@refs/heads/main"
TOKEN = "TOKEN_MARKER"
TARGET_PATH = "C:/ProgramData/example-weather/deploy-target.json"
RUNTIME_IMPORTS = {
    "yaml",
    "deployment.airflow_adapter",
    "deployment.airflow_cli_compat",
    "deployment.compose_adapter",
    "deployment.git_adapter",
    "deployment.health_adapter",
    "deployment.ledger",
    "deployment.main_orchestrator",
    "deployment.overlay",
    "deployment.runtime_environment",
    "deployment.target",
}


def _native_target_path(tmp_path: Path) -> str:
    return str(tmp_path / "deploy-target.json")


@pytest.fixture(autouse=True)
def block_process_and_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def blocked(*args: object, **kwargs: object) -> None:
        raise AssertionError("external side effect attempted")

    monkeypatch.setattr(subprocess, "run", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket.socket, "connect", blocked)


@dataclass(frozen=True)
class _Inputs:
    def as_kwargs(self) -> dict[str, object]:
        return {"payload": "identity-inputs"}


@dataclass(frozen=True)
class _Identity:
    repository: str = REPOSITORY
    workflow_sha: str = SHA


class _Calls:
    def __init__(self) -> None:
        self.values: list[tuple[str, object]] = []

    def add(self, name: str, value: object = None) -> None:
        self.values.append((name, value))


def _event_path(tmp_path: Path) -> Path:
    path = tmp_path / "event.json"
    path.write_text(
        json.dumps(
            {
                "action": "completed",
                "repository": {"full_name": REPOSITORY},
                "workflow_run": {
                    "id": 1,
                    "name": "CI",
                    "path": ".github/workflows/ci.yml",
                    "event": "push",
                    "head_branch": "main",
                    "head_sha": SHA,
                    "status": "completed",
                    "conclusion": "success",
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def _set_base_env(monkeypatch: pytest.MonkeyPatch, event_path: Path) -> None:
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GOVERNANCE_MODE", "protected")
    monkeypatch.setenv("DEPLOYMENT_ENABLED", "enabled")
    monkeypatch.setenv("GH_TOKEN", TOKEN)
    monkeypatch.setenv("GITHUB_REPOSITORY", REPOSITORY)
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event_path))
    monkeypatch.setenv("GITHUB_WORKFLOW_REF", WORKFLOW_REF)
    monkeypatch.setenv("GITHUB_WORKFLOW_SHA", SHA)


def _block_runtime_imports(monkeypatch: pytest.MonkeyPatch, calls: _Calls) -> None:
    original_import = builtins.__import__

    def guarded_import(name: str, *args: object, **kwargs: object) -> object:
        if name in RUNTIME_IMPORTS or any(
            name.startswith(f"{blocked}.") for blocked in RUNTIME_IMPORTS
        ):
            calls.add("blocked-import", name)
            raise AssertionError(f"runtime import attempted: {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)


def test_verify_main_import_and_runtime_do_not_import_runtime_or_pyyaml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    event = _event_path(tmp_path)
    _set_base_env(monkeypatch, event)
    calls = _Calls()
    sys.modules.pop("deployment.main_cli", None)
    _block_runtime_imports(monkeypatch, calls)

    cli = importlib.import_module("deployment.main_cli")
    _install_verify_fakes(monkeypatch, calls)

    rc = cli.main(
        [
            "verify-main",
            "--event-path",
            str(event),
            "--workflow-ref",
            WORKFLOW_REF,
            "--workflow-sha",
            SHA,
        ]
    )

    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out == "verify-main:identity:ok\n"
    assert captured.err == ""
    assert not any(name == "blocked-import" for name, _ in calls.values)


def test_deploy_imports_runtime_only_after_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import deployment.main_cli as cli

    event = _event_path(tmp_path)
    _set_base_env(monkeypatch, event)
    monkeypatch.setenv("WEATHER_DEPLOY_TARGET_PATH", _native_target_path(tmp_path))
    calls = _Calls()
    _install_verify_fakes(monkeypatch, calls)

    def observe_runtime_imports() -> tuple[object, object]:
        calls.add("runtime-imports")

        class FakeOrchestrator:
            def deploy(self, identity: object, target: object) -> DeploymentResult:
                calls.add("orchestrator-deploy", (identity, target))
                return DeploymentResult(
                    "deploy-id", DeploymentOutcome.SUCCESS, "passed", False
                )

        return FakeOrchestrator(), object()

    monkeypatch.setattr(cli, "_build_deploy_components", lambda identity: observe_runtime_imports())

    rc = cli.main(
        [
            "deploy-main",
            "--event-path",
            str(event),
            "--workflow-ref",
            WORKFLOW_REF,
            "--workflow-sha",
            SHA,
        ]
    )

    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out == "deploy-main:deployment:success\n"
    assert [name for name, _ in calls.values] == [
        "gh-runner",
        "evidence",
        "identity",
        "runtime-imports",
        "orchestrator-deploy",
    ]


def test_deploy_runtime_import_failure_is_fixed_and_redacted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import deployment.main_cli as cli

    event = _event_path(tmp_path)
    _set_base_env(monkeypatch, event)
    monkeypatch.setenv("WEATHER_DEPLOY_TARGET_PATH", _native_target_path(tmp_path))
    calls = _Calls()
    _install_verify_fakes(monkeypatch, calls)

    def fail_runtime_import() -> None:
        calls.add("runtime-imports")
        raise ImportError("MISSING_PYYAML_OR_PRIVATE_PATH")

    monkeypatch.setattr(cli, "_load_runtime_symbols", fail_runtime_import)

    rc = cli.main(
        [
            "deploy-main",
            "--event-path",
            str(event),
            "--workflow-ref",
            WORKFLOW_REF,
            "--workflow-sha",
            SHA,
        ]
    )

    captured = capsys.readouterr()
    assert rc == 1
    assert captured.out == ""
    assert captured.err == "deploy-main:cli:invalid-cli\n"
    assert "MISSING_PYYAML" not in captured.err
    assert [name for name, _ in calls.values] == [
        "gh-runner",
        "evidence",
        "identity",
        "runtime-imports",
    ]


def _install_verify_fakes(monkeypatch: pytest.MonkeyPatch, calls: _Calls) -> None:
    import deployment.main_cli as cli

    class FakeGhRunner:
        def __init__(self) -> None:
            calls.add("gh-runner")

    def read_inputs(**kwargs: object) -> _Inputs:
        evidence = {
            "event_path": kwargs["event_path"],
            "workflow_ref": kwargs["workflow_ref"],
            "workflow_sha": kwargs["workflow_sha"],
            "repository": kwargs["repository"],
            "gh_token": kwargs["gh_token"],
            "runner_type": type(kwargs["runner"]).__name__,
        }
        if "governance_mode" in kwargs:
            evidence["governance_mode"] = kwargs["governance_mode"]
        calls.add("evidence", evidence)
        return _Inputs()

    def validate(**kwargs: object) -> _Identity:
        calls.add("identity", kwargs)
        return _Identity()

    monkeypatch.setattr(cli, "SubprocessGhRunner", FakeGhRunner)
    monkeypatch.setattr(cli, "read_main_identity_inputs", read_inputs)
    monkeypatch.setattr(cli, "validate_main_deploy_identity", validate)


def test_verify_main_success_prints_fixed_output_and_constructs_no_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import deployment.main_cli as cli

    event = _event_path(tmp_path)
    _set_base_env(monkeypatch, event)
    calls = _Calls()
    _install_verify_fakes(monkeypatch, calls)

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("runtime should not be constructed")

    monkeypatch.setenv("WEATHER_DEPLOY_TARGET_PATH", "C:/hostile/target.json")
    monkeypatch.setattr(cli, "load_deploy_target", forbidden)
    monkeypatch.setattr(cli, "CommandRunner", forbidden)

    rc = cli.main(
        [
            "verify-main",
            "--event-path",
            str(event),
            "--workflow-ref",
            WORKFLOW_REF,
            "--workflow-sha",
            SHA,
        ]
    )

    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out == "verify-main:identity:ok\n"
    assert captured.err == ""
    assert calls.values == [
        ("gh-runner", None),
        (
            "evidence",
            {
                "event_path": str(event),
                "workflow_ref": WORKFLOW_REF,
                "workflow_sha": SHA,
                "repository": REPOSITORY,
                "governance_mode": "protected",
                "gh_token": TOKEN,
                "runner_type": "FakeGhRunner",
            },
        ),
        ("identity", {"payload": "identity-inputs"}),
    ]


@pytest.mark.parametrize(
    ("env_name", "value"),
    [
        ("GITHUB_ACTIONS", "false"),
        ("GITHUB_ACTIONS", None),
        ("GOVERNANCE_MODE", "open"),
        ("GOVERNANCE_MODE", ""),
        ("GOVERNANCE_MODE", None),
        ("DEPLOYMENT_ENABLED", "disabled"),
        ("DEPLOYMENT_ENABLED", None),
        ("GH_TOKEN", ""),
        ("GH_TOKEN", None),
        ("GITHUB_REPOSITORY", ""),
        ("GITHUB_REPOSITORY", None),
    ],
)
def test_env_gate_failure_happens_before_github_or_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    env_name: str,
    value: str | None,
) -> None:
    import deployment.main_cli as cli

    event = _event_path(tmp_path)
    _set_base_env(monkeypatch, event)
    if value is None:
        monkeypatch.delenv(env_name)
    else:
        monkeypatch.setenv(env_name, value)

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("gate should stop before this call")

    monkeypatch.setattr(cli, "SubprocessGhRunner", forbidden)
    monkeypatch.setattr(cli, "load_deploy_target", forbidden)

    rc = cli.main(
        [
            "deploy-main",
            "--event-path",
            str(event),
            "--workflow-ref",
            WORKFLOW_REF,
            "--workflow-sha",
            SHA,
        ]
    )

    captured = capsys.readouterr()
    assert rc == 1
    assert captured.out == ""
    assert captured.err == "deploy-main:gate:invalid-environment\n"


def test_guarded_verify_passes_mode_to_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import deployment.main_cli as cli

    event = _event_path(tmp_path)
    _set_base_env(monkeypatch, event)
    monkeypatch.setenv("GOVERNANCE_MODE", "guarded_private")
    monkeypatch.setenv("GH_TOKEN", "READ_TOKEN_MARKER")
    calls = _Calls()
    _install_verify_fakes(monkeypatch, calls)

    rc = cli.main(
        [
            "verify-main",
            "--event-path",
            str(event),
            "--workflow-ref",
            WORKFLOW_REF,
            "--workflow-sha",
            SHA,
        ]
    )

    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out == "verify-main:identity:ok\n"
    assert captured.err == ""
    assert calls.values == [
        ("gh-runner", None),
        (
            "evidence",
            {
                "event_path": str(event),
                "workflow_ref": WORKFLOW_REF,
                "workflow_sha": SHA,
                "repository": REPOSITORY,
                "governance_mode": "guarded_private",
                "gh_token": "READ_TOKEN_MARKER",
                "runner_type": "FakeGhRunner",
            },
        ),
        ("identity", {"payload": "identity-inputs"}),
    ]


def test_guarded_deploy_passes_mode_to_evidence_before_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import deployment.main_cli as cli

    event = _event_path(tmp_path)
    _set_base_env(monkeypatch, event)
    monkeypatch.setenv("GOVERNANCE_MODE", "guarded_private")
    monkeypatch.setenv("GH_TOKEN", "READ_TOKEN_MARKER")
    calls = _Calls()
    _install_verify_fakes(monkeypatch, calls)

    class FakeOrchestrator:
        def deploy(self, identity: object, target: object) -> DeploymentResult:
            calls.add("orchestrator-deploy", (identity, target))
            return DeploymentResult("deploy-id", DeploymentOutcome.SUCCESS, "passed", False)

    def build_components(identity: object) -> tuple[object, object]:
        calls.add("runtime-components", identity)
        return FakeOrchestrator(), object()

    monkeypatch.setattr(cli, "_build_deploy_components", build_components)

    rc = cli.main(
        [
            "deploy-main",
            "--event-path",
            str(event),
            "--workflow-ref",
            WORKFLOW_REF,
            "--workflow-sha",
            SHA,
        ]
    )

    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out == "deploy-main:deployment:success\n"
    assert captured.err == ""
    assert [name for name, _ in calls.values] == [
        "gh-runner",
        "evidence",
        "identity",
        "runtime-components",
        "orchestrator-deploy",
    ]
    evidence = next(value for name, value in calls.values if name == "evidence")
    assert evidence["governance_mode"] == "guarded_private"


@pytest.mark.parametrize("command", ["verify-main", "deploy-main"])
@pytest.mark.parametrize(
    ("argument", "invalid_value"),
    [
        ("--workflow-ref", "other/repo/.github/workflows/deploy-main.yml@refs/heads/main"),
        ("--workflow-sha", "abcdefabcdefabcdefabcdefabcdefabcdefabcd"),
        ("--event-path", "missing-event.json"),
    ],
)
def test_gate_rejects_mismatched_identity_arguments_before_github_or_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: str,
    argument: str,
    invalid_value: str,
) -> None:
    import deployment.main_cli as cli

    event = _event_path(tmp_path)
    _set_base_env(monkeypatch, event)
    values = {
        "--event-path": str(event),
        "--workflow-ref": WORKFLOW_REF,
        "--workflow-sha": SHA,
    }
    values[argument] = (
        str(tmp_path / invalid_value) if argument == "--event-path" else invalid_value
    )
    if argument == "--event-path":
        monkeypatch.setenv("GITHUB_EVENT_PATH", values[argument])

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("gate should stop before GitHub or runtime")

    monkeypatch.setattr(cli, "SubprocessGhRunner", forbidden)
    monkeypatch.setattr(cli, "_load_runtime_symbols", forbidden)
    monkeypatch.setattr(cli, "load_deploy_target", forbidden)

    rc = cli.main(
        [
            command,
            "--event-path",
            values["--event-path"],
            "--workflow-ref",
            values["--workflow-ref"],
            "--workflow-sha",
            values["--workflow-sha"],
        ]
    )

    captured = capsys.readouterr()
    assert rc == 1
    assert captured.out == ""
    assert captured.err == f"{command}:gate:invalid-environment\n"


@pytest.mark.parametrize("command", ["verify-main", "deploy-main"])
def test_invalid_cli_flag_stops_before_github_or_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: str,
) -> None:
    import deployment.main_cli as cli

    event = _event_path(tmp_path)
    _set_base_env(monkeypatch, event)

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("parser should stop before GitHub or runtime")

    monkeypatch.setattr(cli, "SubprocessGhRunner", forbidden)
    monkeypatch.setattr(cli, "_load_runtime_symbols", forbidden)
    monkeypatch.setattr(cli, "load_deploy_target", forbidden)

    rc = cli.main(
        [
            command,
            "--event-path",
            str(event),
            "--workflow-ref",
            WORKFLOW_REF,
            "--unexpected-flag",
            SHA,
        ]
    )

    captured = capsys.readouterr()
    assert rc == 2
    assert captured.out == ""
    assert captured.err == f"{command}:usage:invalid-arguments\n"


def test_cli_identity_failure_stops_before_target_or_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import deployment.main_cli as cli
    from deployment.main_identity import MainIdentityError

    event = _event_path(tmp_path)
    _set_base_env(monkeypatch, event)
    _install_verify_fakes(monkeypatch, _Calls())

    def reject(**kwargs: object) -> None:
        raise MainIdentityError()

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("identity failure should stop before runtime")

    monkeypatch.setattr(cli, "validate_main_deploy_identity", reject)
    monkeypatch.setattr(cli, "load_deploy_target", forbidden)
    monkeypatch.setattr(cli, "CommandRunner", forbidden)

    rc = cli.main(
        [
            "deploy-main",
            "--event-path",
            str(event),
            "--workflow-ref",
            WORKFLOW_REF,
            "--workflow-sha",
            SHA,
        ]
    )

    captured = capsys.readouterr()
    assert rc == 1
    assert captured.err == "deploy-main:identity:invalid-main-deploy-identity\n"


def test_deploy_main_success_wires_stable_probe_adapters_and_orchestrator_in_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import deployment.main_cli as cli
    from deployment.overlay import render_baseline_overlay
    from tests.deploy.test_release_inventory import _target

    event = _event_path(tmp_path)
    _set_base_env(monkeypatch, event)
    monkeypatch.setenv("WEATHER_DEPLOY_TARGET_PATH", _native_target_path(tmp_path))
    calls = _Calls()
    _install_verify_fakes(monkeypatch, calls)
    target = _target(tmp_path)
    artifact = render_baseline_overlay(target)

    class FakeRunner:
        def __init__(self, *, timeout_seconds: int) -> None:
            calls.add("runner", timeout_seconds)
            self.timeout_seconds = timeout_seconds

    class FakeOverlayStore:
        def __init__(self, active_target: object) -> None:
            calls.add("overlay-store", active_target)

        def verify_installed(self, expected: object) -> None:
            calls.add("verify-installed", expected)

    class FakeAdapter:
        def __init__(self, *args: object) -> None:
            calls.add(type(self).__name__, args)

    class FakeGitAdapter(FakeAdapter):
        pass

    class FakeLedger:
        def __init__(self, ledger_directory: object, lock_file: object) -> None:
            calls.add("ledger", (ledger_directory, lock_file))

    class FakeClock:
        def __init__(self) -> None:
            calls.add("clock")

    class FakeOrchestrator:
        def __init__(self, **kwargs: object) -> None:
            calls.add("orchestrator-init", tuple(kwargs))

        def deploy(self, identity: object, active_target: object) -> DeploymentResult:
            calls.add("orchestrator-deploy", (identity, active_target))
            return DeploymentResult("deploy-id", DeploymentOutcome.SUCCESS, "passed", False)

    monkeypatch.setattr(cli, "load_deploy_target", lambda path, repo_root: target)
    monkeypatch.setattr(
        cli,
        "validate_runtime_environment",
        lambda active_target, repo_root: calls.add(
            "runtime-environment", (active_target, repo_root)
        ),
    )
    monkeypatch.setattr(cli, "_current_stable_overlay", lambda active_target: artifact)
    monkeypatch.setattr(cli, "probe_airflow_cli_contract", lambda *args, **kwargs: calls.add("probe", (args, kwargs)))
    monkeypatch.setattr(cli, "CommandRunner", FakeRunner)
    monkeypatch.setattr(cli, "AtomicOverlayStore", FakeOverlayStore)
    monkeypatch.setattr(cli, "AirflowCommandAdapter", FakeAdapter)
    monkeypatch.setattr(cli, "ComposeCommandAdapter", FakeAdapter)
    monkeypatch.setattr(cli, "GitCommandAdapter", FakeGitAdapter)
    monkeypatch.setattr(cli, "HealthCommandAdapter", FakeAdapter)
    monkeypatch.setattr(cli, "DeploymentLedger", FakeLedger)
    monkeypatch.setattr(cli, "SystemClock", FakeClock)
    monkeypatch.setattr(cli, "MainDeploymentOrchestrator", FakeOrchestrator)

    rc = cli.main(
        [
            "deploy-main",
            "--event-path",
            str(event),
            "--workflow-ref",
            WORKFLOW_REF,
            "--workflow-sha",
            SHA,
        ]
    )

    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out == "deploy-main:deployment:success\n"
    assert captured.err == ""
    names = [name for name, _ in calls.values]
    assert names == [
        "gh-runner",
        "evidence",
        "identity",
        "runtime-environment",
        "overlay-store",
        "verify-installed",
        "runner",
        "probe",
        "runner",
        "FakeAdapter",
        "runner",
        "FakeAdapter",
        "runner",
        "FakeGitAdapter",
        "runner",
        "FakeAdapter",
        "clock",
        "ledger",
        "orchestrator-init",
        "orchestrator-deploy",
    ]
    runner_timeouts = [value for name, value in calls.values if name == "runner"]
    assert runner_timeouts == [60, 60, 900, 300, 60]
    probe_args, probe_kwargs = next(value for name, value in calls.values if name == "probe")
    assert probe_args[0] == target
    assert probe_args[1].timeout_seconds == 60
    assert probe_kwargs == {"mode": "stable", "overlay_file": target.generated_overlay_file}


def test_deploy_main_rejects_invalid_existing_local_env_before_runtime_adapters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import deployment.main_cli as cli
    from deployment.runtime_environment import RuntimeEnvironmentError
    from tests.deploy.test_release_inventory import _target

    event = _event_path(tmp_path)
    _set_base_env(monkeypatch, event)
    monkeypatch.setenv("WEATHER_DEPLOY_TARGET_PATH", _native_target_path(tmp_path))
    _install_verify_fakes(monkeypatch, _Calls())
    target = _target(tmp_path)
    monkeypatch.setattr(cli, "_load_runtime_symbols", lambda: None)
    monkeypatch.setattr(cli, "load_deploy_target", lambda path, repo_root: target)
    monkeypatch.setattr(
        cli,
        "validate_runtime_environment",
        lambda active_target, repo_root: (_ for _ in ()).throw(
            RuntimeEnvironmentError("runtime_environment_invalid")
        ),
    )
    monkeypatch.setattr(
        cli,
        "_current_stable_overlay",
        lambda active_target: (_ for _ in ()).throw(
            AssertionError("runtime adapter reached")
        ),
    )

    rc = cli.main(
        [
            "deploy-main",
            "--event-path",
            str(event),
            "--workflow-ref",
            WORKFLOW_REF,
            "--workflow-sha",
            SHA,
        ]
    )

    captured = capsys.readouterr()
    assert rc == 1
    assert captured.out == ""
    assert captured.err == "deploy-main:target:invalid-runtime-environment\n"


def test_deploy_main_noop_has_fixed_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import deployment.main_cli as cli

    event = _event_path(tmp_path)
    _set_base_env(monkeypatch, event)
    monkeypatch.setenv("WEATHER_DEPLOY_TARGET_PATH", TARGET_PATH)
    _install_verify_fakes(monkeypatch, _Calls())

    class FakeOrchestrator:
        def __init__(self, **kwargs: object) -> None:
            pass

        def deploy(self, identity: object, active_target: object) -> DeploymentResult:
            return DeploymentResult("deploy-id", DeploymentOutcome.SUCCESS, "passed", True)

    monkeypatch.setattr(cli, "_build_deploy_components", lambda identity: (FakeOrchestrator(), object()))

    rc = cli.main(["deploy-main", "--event-path", str(event), "--workflow-ref", WORKFLOW_REF, "--workflow-sha", SHA])

    captured = capsys.readouterr()
    assert rc == 0
    assert captured.out == "deploy-main:deployment:noop\n"


def test_parser_rejects_unknown_release_and_cutover_commands_without_arg_leak(
    capsys: pytest.CaptureFixture[str],
) -> None:
    import deployment.main_cli as cli

    secret_arg = "deploy-prod-TOKEN_MARKER"

    rc = cli.main([secret_arg, "--raw", "C:/private/path"])

    captured = capsys.readouterr()
    assert rc == 2
    assert captured.out == ""
    assert captured.err == "main:usage:invalid-command\n"
    assert secret_arg not in captured.err
    assert "private" not in captured.err


def test_public_errors_do_not_leak_token_target_path_or_raw_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import deployment.main_cli as cli

    event = _event_path(tmp_path)
    _set_base_env(monkeypatch, event)
    monkeypatch.setenv("WEATHER_DEPLOY_TARGET_PATH", TARGET_PATH)
    _install_verify_fakes(monkeypatch, _Calls())

    def reject(path: object, repo_root: object) -> None:
        raise RuntimeError(f"{TOKEN} {TARGET_PATH} RAW_TARGET_BODY")

    monkeypatch.setattr(cli, "load_deploy_target", reject)

    rc = cli.main(["deploy-main", "--event-path", str(event), "--workflow-ref", WORKFLOW_REF, "--workflow-sha", SHA])

    captured = capsys.readouterr()
    assert rc == 1
    assert captured.err == "deploy-main:target:invalid-target\n"
    rendered = captured.out + captured.err
    assert TOKEN not in rendered
    assert TARGET_PATH not in rendered
    assert "RAW_TARGET_BODY" not in rendered
