from __future__ import annotations

import json
import socket
import subprocess
from dataclasses import dataclass
from dataclasses import replace
from pathlib import Path

import pytest

from deployment.airflow_cli_compat import AirflowCliContract
from deployment.command import CompletedCommand
from deployment.models import WriterRunCounts
from deployment.overlay import render_baseline_overlay
from deployment.target import target_fingerprint
from tests.deploy.test_release_inventory import _native_target, _target


@pytest.fixture(autouse=True)
def block_process_and_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def blocked(*args: object, **kwargs: object) -> None:
        raise AssertionError("external side effect attempted")

    monkeypatch.setattr(subprocess, "run", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket.socket, "connect", blocked)


def _target_file(tmp_path: Path) -> tuple[object, Path]:
    target = _target(tmp_path)
    path = tmp_path / "reviewed-target.json"
    path.write_bytes(target.canonical_target_bytes)
    return target, path


def _native_target_file(tmp_path: Path) -> tuple[object, Path]:
    target = _native_target(tmp_path)
    path = tmp_path / "repo" / "reviewed-target.json"
    path.write_bytes(target.canonical_target_bytes)
    return target, path


@dataclass
class _Calls:
    values: list[tuple[str, object]]

    def add(self, name: str, value: object = None) -> None:
        self.values.append((name, value))


class _ReadAdapter:
    def __init__(self, calls: _Calls, target) -> None:
        self.calls = calls
        self.target = target

    def capture_pause_state(self, dag_ids: tuple[str, ...]) -> dict[str, bool]:
        self.calls.add("airflow.capture_pause_state", dag_ids)
        return {dag_id: False for dag_id in dag_ids}

    def writer_run_counts(self, dag_ids: tuple[str, ...]) -> WriterRunCounts:
        self.calls.add("airflow.writer_run_counts", dag_ids)
        return WriterRunCounts(running=0, queued=0)


class _ComposeReadAdapter:
    def __init__(self, calls: _Calls, target, ok: bool = True) -> None:
        self.calls = calls
        self.target = target
        self.ok = ok

    def code_services_healthy(self) -> bool:
        self.calls.add("compose.code_services_healthy")
        return self.ok

    def code_service_status(self) -> dict[str, dict[str, str]]:
        self.calls.add("compose.code_service_status")
        if not self.ok:
            raise ValueError
        return {
            service: {"state": "running", "health": "healthy"}
            for service in sorted(self.target.airflow_code_services)
        }


def _install_read_fakes(monkeypatch: pytest.MonkeyPatch, calls: _Calls) -> None:
    import deployment.cutover_cli as cli

    class FakeRunner:
        timeout_seconds = 60

    def build_read(target):
        calls.add("read-components", target)
        return FakeRunner(), _ReadAdapter(calls, target), _ComposeReadAdapter(calls, target)

    def probe(target, runner, **kwargs):
        calls.add("probe", (target, runner, kwargs))
        return AirflowCliContract(version="3.2.2", capability_fingerprint="b" * 64)

    monkeypatch.setattr(cli, "_build_read_components", build_read)
    monkeypatch.setattr(cli, "probe_airflow_cli_contract", probe)


def test_inspect_is_read_only_and_prints_only_sanitized_digests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import deployment.cutover_cli as cli

    target, path = _native_target_file(tmp_path)
    calls = _Calls([])
    _install_read_fakes(monkeypatch, calls)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)

    rc = cli.main(["inspect", "--target-path", str(path)])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert rc == 0
    assert captured.err == ""
    assert payload["target_fingerprint"] == target_fingerprint(target)
    assert payload["baseline_overlay_sha256"] == render_baseline_overlay(target).sha256
    assert payload["airflow_cli_fingerprint"] == "b" * 64
    assert payload["compose_code_services_ok"] is True
    assert payload["code_service_states"] == {
        service: "running" for service in sorted(target.airflow_code_services)
    }
    assert payload["code_service_health"] == {
        service: "healthy" for service in sorted(target.airflow_code_services)
    }
    assert payload["dag_inventory_ok"] is True
    assert payload["dag_paused"] == {
        dag_id: False for dag_id in sorted(target.dag_allowlist)
    }
    assert payload["writer_running"] == 0
    assert payload["writer_queued"] == 0
    assert payload["stable_overlay_exists"] is False
    assert payload["compose_environment_ready"] is True
    assert payload["writes"] == {"dbt": 0, "trino": 0, "d1": 0, "r2": 0}
    rendered = captured.out + captured.err
    assert "ProgramData" not in rendered
    assert "weather-local-runtime" not in rendered
    assert "token" not in rendered.casefold()
    assert [name for name, _ in calls.values] == [
        "read-components",
        "probe",
        "compose.code_service_status",
        "airflow.capture_pause_state",
        "airflow.writer_run_counts",
    ]
    probe_kwargs = calls.values[1][1][2]
    assert probe_kwargs == {"mode": "base-only-cutover", "overlay_file": None}


def test_inspect_rejects_missing_existing_local_env_before_compose_or_airflow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import deployment.cutover_cli as cli

    target, path = _native_target_file(tmp_path)
    target = replace(target, credential_source_kind="existing_local_env")
    calls = _Calls([])
    monkeypatch.setattr(cli, "_load_target", lambda command, target_path: target)
    monkeypatch.setattr(
        cli,
        "_build_read_components",
        lambda active_target: calls.add("unexpected-read"),
    )
    monkeypatch.delenv("COMPOSE_ENV_FILES", raising=False)
    monkeypatch.delenv("ASK_SEOUL_PROD_ENV_FILE", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)

    rc = cli.main(["inspect", "--target-path", str(path)])

    captured = capsys.readouterr()
    assert rc == 1
    assert captured.out == ""
    assert captured.err == "inspect:environment:runtime-environment-invalid\n"
    assert calls.values == []


def test_base_inspect_filters_non_weather_dags_from_shared_airflow_inventory(
    tmp_path: Path,
) -> None:
    import deployment.cutover_cli as cli

    target = _target(tmp_path)
    expected = {
        dag_id: index % 2 == 0
        for index, dag_id in enumerate(sorted(target.dag_allowlist))
    }
    rows = [
        {"dag_id": "traffic_shared_runtime_dag", "is_paused": False},
        *[
            {"dag_id": dag_id, "is_paused": paused}
            for dag_id, paused in expected.items()
        ],
    ]

    class FakeRunner:
        def run(self, argv, cwd: Path) -> CompletedCommand:
            return CompletedCommand(
                stdout=json.dumps(rows), stderr="", returncode=0
            )

    adapter = cli._BaseAirflowReadAdapter(target, FakeRunner())

    assert adapter.capture_pause_state(tuple(sorted(target.dag_allowlist))) == expected


def test_base_inspect_accepts_airflow_322_preamble_string_bools_and_duplicates(
    tmp_path: Path,
) -> None:
    import deployment.cutover_cli as cli

    target = _target(tmp_path)
    expected = {
        dag_id: index % 2 == 0
        for index, dag_id in enumerate(sorted(target.dag_allowlist))
    }
    rows: list[dict[str, object]] = []
    for dag_id, paused in expected.items():
        row = {"dag_id": dag_id, "is_paused": str(paused)}
        rows.extend((row, dict(row)))
    plugins = (
        "schemas",
        "tables",
        "types",
        "constraints",
        "defaults",
        "comments",
    )
    preamble = "\n".join(
        "2026-08-14T17:43:56.611303Z [info     ] "
        f"setup plugin alembic.autogenerate.{plugin} "
        "[alembic.runtime.plugins] loc=plugins.py:37"
        for plugin in plugins
    )

    class FakeRunner:
        def run(self, argv, cwd: Path) -> CompletedCommand:
            return CompletedCommand(
                stdout=f"{preamble}\n{json.dumps(rows)}\n",
                stderr="",
                returncode=0,
            )

    adapter = cli._BaseAirflowReadAdapter(target, FakeRunner())

    assert adapter.capture_pause_state(tuple(sorted(target.dag_allowlist))) == expected


def test_base_inspect_counts_airflow_322_run_id_rows(tmp_path: Path) -> None:
    import deployment.cutover_cli as cli

    target = _target(tmp_path)
    writers = tuple(sorted(target.writer_dag_allowlist))
    responses: list[CompletedCommand] = []
    for dag_id in writers:
        responses.extend(
            (
                CompletedCommand(
                    stdout=json.dumps(
                        [{"dag_id": dag_id, "run_id": f"run-{dag_id}", "state": "running"}]
                    ),
                    stderr="",
                    returncode=0,
                ),
                CompletedCommand(stdout="[]", stderr="", returncode=0),
            )
        )

    class FakeRunner:
        def run(self, argv, cwd: Path) -> CompletedCommand:
            return responses.pop(0)

    counts = cli._BaseAirflowReadAdapter(target, FakeRunner()).writer_run_counts(
        writers
    )

    assert counts == WriterRunCounts(running=len(writers), queued=0)


def test_activate_rehearses_baseline_records_ledger_and_installs_target_last(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import deployment.cutover_cli as cli

    target, path = _native_target_file(tmp_path)
    baseline = render_baseline_overlay(target)
    install_path = tmp_path / "installed-target.json"
    calls = _Calls([])
    _install_read_fakes(monkeypatch, calls)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)

    class FakeRunner:
        def __init__(self, *, timeout_seconds: int) -> None:
            calls.add("runner", timeout_seconds)

        def run(self, argv, cwd: Path) -> CompletedCommand:
            raise AssertionError("runner should stay behind injected adapters")

    class FakeOverlay:
        def __init__(self, active_target) -> None:
            calls.add("overlay.init", active_target)

        def stage(self, artifact):
            calls.add("overlay.stage", artifact)
            return tmp_path / "baseline.tmp"

        def install(self, staged: Path, artifact) -> None:
            calls.add("overlay.install", (staged, artifact))

        def restore(self, content: bytes, sha256: str) -> None:
            calls.add("overlay.restore", (content, sha256))

        def verify_installed(self, artifact) -> None:
            calls.add("overlay.verify_installed", artifact)

        def discard(self, staged: Path) -> None:
            calls.add("overlay.discard", staged)

    class FakeCompose:
        def __init__(self, active_target, runner) -> None:
            calls.add("compose.init", (active_target, runner))

        def validate_candidate(self, active_target, staged: Path) -> None:
            calls.add("compose.validate_candidate", (active_target, staged))

    class FakeLedger:
        def __init__(self, ledger_directory: Path, lock_file: Path) -> None:
            calls.add("ledger.init", (ledger_directory, lock_file))

        def acquire_lock(self, lock_id: str):
            calls.add("ledger.acquire_lock", lock_id)

            class _Lock:
                def __enter__(self_inner):
                    calls.add("ledger.enter")

                def __exit__(self_inner, exc_type, exc, tb):
                    calls.add("ledger.exit")

            return _Lock()

        def record_baseline(self, record) -> None:
            calls.add("ledger.record_baseline", record)

    class FakeClock:
        def utc_now(self) -> str:
            calls.add("clock.utc_now")
            return "2026-08-15T00:00:00Z"

    monkeypatch.setattr(cli, "CommandRunner", FakeRunner)
    monkeypatch.setattr(cli, "AtomicOverlayStore", FakeOverlay)
    monkeypatch.setattr(cli, "ComposeCommandAdapter", FakeCompose)
    monkeypatch.setattr(cli, "DeploymentLedger", FakeLedger)
    monkeypatch.setattr(cli, "SystemClock", FakeClock)

    rc = cli.main(
        [
            "activate",
            "--target-path",
            str(path),
            "--install-target-path",
            str(install_path),
            "--confirm-target-fingerprint",
            target_fingerprint(target),
            "--confirm-baseline-sha256",
            baseline.sha256,
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert rc == 0
    assert captured.err == ""
    assert payload == {
        "activated": True,
        "airflow_state_changed": False,
        "baseline_id": "baseline://existing-local",
        "baseline_overlay_sha256": baseline.sha256,
        "code_services_deployed": False,
        "github_state_changed": False,
        "runner_started": False,
        "target_fingerprint": target_fingerprint(target),
    }
    assert install_path.read_bytes() == path.read_bytes()
    names = [name for name, _ in calls.values]
    assert names.index("ledger.record_baseline") < names.index("ledger.exit")
    assert names[-2:] == ["ledger.record_baseline", "ledger.exit"]
    assert names.index("ledger.enter") < names.index("read-components")
    assert names.index("read-components") < names.index("overlay.stage")
    assert "compose.validate_candidate" in names
    assert "overlay.install" in names
    assert "overlay.restore" in names
    record = next(value for name, value in calls.values if name == "ledger.record_baseline")
    assert record.baseline_id == "baseline://existing-local"
    assert record.rehearsal == "passed"
    assert record.overlay_sha256 == baseline.sha256


def test_activate_reloads_source_under_lock_and_rejects_source_swap_before_side_effects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import deployment.cutover_cli as cli

    target, path = _target_file(tmp_path)
    baseline = render_baseline_overlay(target)
    install_path = tmp_path / "installed-target.json"
    calls = _Calls([])
    _install_read_fakes(monkeypatch, calls)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)

    class FakeOverlay:
        def __init__(self, active_target) -> None:
            calls.add("overlay.init", active_target)

        def stage(self, artifact):
            calls.add("overlay.stage", artifact)
            return tmp_path / "baseline.tmp"

    class FakeCompose:
        def __init__(self, active_target, runner) -> None:
            calls.add("compose.init", (active_target, runner))

    class FakeLedger:
        def __init__(self, ledger_directory: Path, lock_file: Path) -> None:
            calls.add("ledger.init", (ledger_directory, lock_file))

        def acquire_lock(self, lock_id: str):
            calls.add("ledger.acquire_lock", lock_id)

            class _Lock:
                def __enter__(self_inner):
                    calls.add("ledger.enter")
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    payload["target_id"] = "swapped-target"
                    path.write_text(json.dumps(payload), encoding="utf-8")

                def __exit__(self_inner, exc_type, exc, tb):
                    calls.add("ledger.exit")

            return _Lock()

    monkeypatch.setattr(cli, "AtomicOverlayStore", FakeOverlay)
    monkeypatch.setattr(cli, "ComposeCommandAdapter", FakeCompose)
    monkeypatch.setattr(cli, "DeploymentLedger", FakeLedger)

    rc = cli.main(
        [
            "activate",
            "--target-path",
            str(path),
            "--install-target-path",
            str(install_path),
            "--confirm-target-fingerprint",
            target_fingerprint(target),
            "--confirm-baseline-sha256",
            baseline.sha256,
        ]
    )

    captured = capsys.readouterr()
    assert rc == 1
    assert captured.err == "activate:confirm:confirmation-mismatch\n"
    assert not install_path.exists()
    names = [name for name, _ in calls.values]
    assert "overlay.stage" not in names
    assert "read-components" not in names


def test_activate_discards_staged_overlay_when_validation_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import deployment.cutover_cli as cli

    target, path = _target_file(tmp_path)
    baseline = render_baseline_overlay(target)
    install_path = tmp_path / "installed-target.json"
    calls = _Calls([])
    _install_read_fakes(monkeypatch, calls)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    staged = tmp_path / "baseline.tmp"

    class FakeOverlay:
        def __init__(self, active_target) -> None:
            calls.add("overlay.init", active_target)

        def stage(self, artifact):
            calls.add("overlay.stage", artifact)
            return staged

        def discard(self, staged_path: Path) -> None:
            calls.add("overlay.discard", staged_path)

    class FakeCompose:
        def __init__(self, active_target, runner) -> None:
            calls.add("compose.init", (active_target, runner))

        def validate_candidate(self, active_target, staged_path: Path) -> None:
            calls.add("compose.validate_candidate", (active_target, staged_path))
            raise RuntimeError("raw private failure")

    class FakeLedger:
        def __init__(self, ledger_directory: Path, lock_file: Path) -> None:
            calls.add("ledger.init", (ledger_directory, lock_file))

        def acquire_lock(self, lock_id: str):
            class _Lock:
                def __enter__(self_inner):
                    calls.add("ledger.enter")

                def __exit__(self_inner, exc_type, exc, tb):
                    calls.add("ledger.exit")

            return _Lock()

    monkeypatch.setattr(cli, "AtomicOverlayStore", FakeOverlay)
    monkeypatch.setattr(cli, "ComposeCommandAdapter", FakeCompose)
    monkeypatch.setattr(cli, "DeploymentLedger", FakeLedger)

    rc = cli.main(
        [
            "activate",
            "--target-path",
            str(path),
            "--install-target-path",
            str(install_path),
            "--confirm-target-fingerprint",
            target_fingerprint(target),
            "--confirm-baseline-sha256",
            baseline.sha256,
        ]
    )

    captured = capsys.readouterr()
    assert rc == 1
    assert captured.err == "activate:baseline:cutover-activation-failed\n"
    assert not install_path.exists()
    assert ("overlay.discard", staged) in calls.values


@pytest.mark.parametrize(
    "destination_name",
    ["source", "overlay", "lock", "ledger-child", "dags-child", "dbt-child", "non-json"],
)
def test_activate_rejects_hostile_install_destination_before_lock_or_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    destination_name: str,
) -> None:
    import deployment.cutover_cli as cli

    target, path = _native_target_file(tmp_path)
    baseline = render_baseline_overlay(target)
    calls = _Calls([])
    _install_read_fakes(monkeypatch, calls)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    if destination_name == "source":
        install_path = path
    elif destination_name == "overlay":
        install_path = Path(str(target.generated_overlay_file))
    elif destination_name == "lock":
        install_path = Path(str(target.lock_file))
    elif destination_name == "ledger-child":
        install_path = Path(str(target.ledger_directory)) / "target.json"
    elif destination_name == "dags-child":
        install_path = Path(str(target.dags_host_path)) / "target.json"
    elif destination_name == "dbt-child":
        install_path = Path(str(target.dbt_host_path)) / "target.json"
    else:
        install_path = tmp_path / "installed-target.txt"

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("destination validation must happen before this")

    monkeypatch.setattr(cli, "DeploymentLedger", forbidden)
    rc = cli.main(
        [
            "activate",
            "--target-path",
            str(path),
            "--install-target-path",
            str(install_path),
            "--confirm-target-fingerprint",
            target_fingerprint(target),
            "--confirm-baseline-sha256",
            baseline.sha256,
        ]
    )

    captured = capsys.readouterr()
    assert rc == 1
    assert captured.err == "activate:install-target:invalid-destination\n"
    assert calls.values == []


@pytest.mark.parametrize(
    "rows",
    [
        lambda target: [],
        lambda target: [
            {"Service": sorted(target.airflow_code_services)[0], "State": "running", "Health": "healthy"}
        ],
        lambda target: [
            {"Service": sorted(target.airflow_code_services)[0], "State": "running", "Health": "healthy"},
            {"Service": sorted(target.airflow_code_services)[0], "State": "running", "Health": "healthy"},
        ],
        lambda target: [
            {"Service": sorted(target.forbidden_data_services)[0], "State": "running", "Health": "healthy"}
        ],
        lambda target: [
            {"Service": "airflow-init", "State": "running", "Health": "healthy"}
        ],
        lambda target: [
            {"Service": sorted(target.airflow_code_services)[0], "State": "exited", "Health": "healthy"}
        ],
        lambda target: [
            {"Service": sorted(target.airflow_code_services)[0], "State": "running", "Health": "starting"}
        ],
    ],
)
def test_base_compose_inspect_rejects_missing_duplicate_data_init_and_malformed_services(
    tmp_path: Path, rows
) -> None:
    from deployment.command import CompletedCommand
    from deployment.cutover_cli import _BaseComposeReadAdapter

    target = _target(tmp_path)
    prefix = ["docker", "compose", "-p", target.project_name]
    for compose_file in target.compose_files:
        prefix.extend(("-f", str(compose_file)))
    expected_argv = (*prefix, "ps", "--format", "json", *tuple(sorted(target.airflow_code_services)))

    class Runner:
        def __init__(self) -> None:
            self.calls = []

        def run(self, argv, cwd: Path) -> CompletedCommand:
            self.calls.append((tuple(argv), cwd))
            assert tuple(argv) == expected_argv
            return CompletedCommand(stdout=json.dumps(rows(target)), stderr="", returncode=0)

    runner = Runner()
    with pytest.raises(ValueError):
        _BaseComposeReadAdapter(target, runner).code_services_healthy()
    assert len(runner.calls) == 1


def test_base_compose_inspect_accepts_compose_ps_ndjson(tmp_path: Path) -> None:
    from deployment.command import CompletedCommand
    from deployment.cutover_cli import _BaseComposeReadAdapter

    target = _target(tmp_path)
    services = tuple(sorted(target.airflow_code_services))
    rows = [
        {"Service": service, "State": "running", "Health": "healthy"}
        for service in services
    ]

    class Runner:
        def run(self, argv, cwd: Path) -> CompletedCommand:
            return CompletedCommand(
                stdout="\n".join(json.dumps(row) for row in rows) + "\n",
                stderr="",
                returncode=0,
            )

    assert _BaseComposeReadAdapter(target, Runner()).code_services_healthy() is True


@pytest.mark.parametrize(
    "alias_kind",
    ["ledger-child", "dags-child", "overlay-file", "lock-file"],
)
def test_install_destination_rejects_symlink_parent_aliases_before_write(
    tmp_path: Path, alias_kind: str
) -> None:
    from deployment.cutover_cli import CutoverCliError, _validate_install_destination

    source = tmp_path / "reviewed-target.json"
    source.write_text("{}", encoding="utf-8")
    real_ledger = tmp_path / "real-ledger"
    real_dags = tmp_path / "real-dags"
    real_dbt = tmp_path / "real-dbt"
    real_generated = tmp_path / "real-generated"
    real_lock = tmp_path / "real-lock"
    for directory in (real_ledger, real_dags, real_dbt, real_generated, real_lock):
        directory.mkdir()
    target = replace(
        _target(tmp_path),
        ledger_directory=real_ledger,
        dags_host_path=real_dags,
        dbt_host_path=real_dbt,
        generated_overlay_file=real_generated / "main-deploy.override.yml",
        lock_file=real_lock / "deploy.lock",
        compose_files=(tmp_path / "compose.yml",),
    )
    if alias_kind == "ledger-child":
        link = tmp_path / "ledger-link"
        destination = link / "target.json"
        real_target = real_ledger
    elif alias_kind == "dags-child":
        link = tmp_path / "dags-link"
        destination = link / "target.json"
        real_target = real_dags
    elif alias_kind == "overlay-file":
        link = tmp_path / "generated-link"
        destination = link / "main-deploy.override.yml"
        real_target = real_generated
    else:
        link = tmp_path / "lock-link"
        destination = link / "deploy.lock"
        real_target = real_lock
    try:
        link.symlink_to(real_target, target_is_directory=True)
    except OSError as exc:
        if getattr(exc, "winerror", None) == 1314:
            pytest.skip(f"symlink unavailable: {exc}")
        raise

    with pytest.raises(CutoverCliError, match="^invalid-destination$") as error:
        _validate_install_destination("activate", source, destination, target)

    assert error.value.line == "activate:install-target:invalid-destination"


def test_activate_rejects_github_actions_and_confirmation_mismatch_before_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import deployment.cutover_cli as cli

    target, path = _target_file(tmp_path)
    install_path = tmp_path / "installed-target.json"
    calls = _Calls([])
    _install_read_fakes(monkeypatch, calls)

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("activation should stop before mutation components")

    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setattr(cli, "AtomicOverlayStore", forbidden)
    rc = cli.main(
        [
            "activate",
            "--target-path",
            str(path),
            "--install-target-path",
            str(install_path),
            "--confirm-target-fingerprint",
            target_fingerprint(target),
            "--confirm-baseline-sha256",
            render_baseline_overlay(target).sha256,
        ]
    )
    captured = capsys.readouterr()
    assert rc == 1
    assert captured.err == "activate:gate:invalid-environment\n"
    assert not install_path.exists()
    assert calls.values == []

    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    rc = cli.main(
        [
            "activate",
            "--target-path",
            str(path),
            "--install-target-path",
            str(install_path),
            "--confirm-target-fingerprint",
            "0" * 64,
            "--confirm-baseline-sha256",
            render_baseline_overlay(target).sha256,
        ]
    )
    captured = capsys.readouterr()
    assert rc == 1
    assert captured.err == "activate:confirm:confirmation-mismatch\n"
    assert not install_path.exists()
    assert calls.values == []


def test_rejects_unknown_command_without_leaking_arguments(
    capsys: pytest.CaptureFixture[str],
) -> None:
    import deployment.cutover_cli as cli

    rc = cli.main(["deploy-main", "--target-path", "C:/private/token.json"])

    captured = capsys.readouterr()
    assert rc == 2
    assert captured.out == ""
    assert captured.err == "cutover:usage:invalid-command\n"
    assert "private" not in captured.err
