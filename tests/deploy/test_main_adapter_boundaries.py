from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from deployment.airflow_adapter import AirflowAdapterError, AirflowCommandAdapter
from deployment.command import CompletedCommand
from deployment.compose_adapter import ComposeAdapterError, ComposeCommandAdapter
from deployment.git_adapter import GitAdapterError, GitCommandAdapter
from deployment.health_adapter import HealthAdapterError, HealthCommandAdapter
from deployment.models import WriterRunCounts
from deployment.overlay import OverlayArtifact, render_baseline_overlay, render_release_overlay
from tests.deploy.test_release_inventory import _target


SHA = "a" * 40
REPOSITORY = "owner/seoul-weather-platform"


def _native_target(tmp_path: Path):
    runtime = tmp_path / "runtime"
    working = tmp_path / "compose"
    generated = tmp_path / "state" / "main-deploy.override.yml"
    working.mkdir()
    (working / "docker-compose.yml").write_text("services: {}\n", encoding="utf-8")
    return replace(
        _target(tmp_path),
        working_directory=working,
        compose_files=(working / "docker-compose.yml",),
        dags_host_path=runtime / "baseline" / "dags",
        dbt_host_path=runtime / "baseline" / "dbt",
        runtime_root=runtime,
        ledger_directory=tmp_path / "state" / "ledger",
        lock_file=tmp_path / "state" / "deploy.lock",
        generated_overlay_file=generated,
    )


def _ok(stdout: str = "") -> CompletedCommand:
    return CompletedCommand(stdout=stdout, stderr="", returncode=0)


class _QueueRunner:
    def __init__(self, responses: list[CompletedCommand]):
        self.responses = list(responses)
        self.calls: list[tuple[tuple[str, ...], Path]] = []

    def run(self, argv, cwd: Path) -> CompletedCommand:
        self.calls.append((tuple(argv), cwd))
        if not self.responses:
            raise AssertionError("unexpected command")
        return self.responses.pop(0)


def _compose_prefix(target, overlay: Path | None = None) -> tuple[str, ...]:
    prefix = ("docker", "compose", "-p", target.project_name)
    for compose_file in target.compose_files:
        prefix += ("-f", str(compose_file))
    if overlay is not None:
        prefix += ("-f", str(overlay))
    return prefix


def _airflow_prefix(target) -> tuple[str, ...]:
    return (
        *_compose_prefix(target, Path(str(target.generated_overlay_file))),
        "exec",
        "-T",
        target.control_service,
        "airflow",
    )


def _candidate(tmp_path: Path):
    target = _native_target(tmp_path)
    checkout = Path(str(target.runtime_root)) / "releases" / SHA
    artifact = render_release_overlay(target, checkout, SHA)
    staged = Path(str(target.generated_overlay_file)).with_name(
        f".{Path(str(target.generated_overlay_file)).name}.candidate.tmp"
    )
    staged.parent.mkdir(parents=True)
    staged.write_bytes(artifact.content)
    return target, artifact, staged


def _baseline_candidate(tmp_path: Path):
    target = _native_target(tmp_path)
    artifact = render_baseline_overlay(target)
    staged = Path(str(target.generated_overlay_file)).with_name(
        f".{Path(str(target.generated_overlay_file)).name}.baseline.tmp"
    )
    staged.parent.mkdir(parents=True)
    staged.write_bytes(artifact.content)
    return target, artifact, staged


def _compose_documents(target, artifact):
    code = sorted(target.airflow_code_services)
    forbidden = sorted(target.forbidden_data_services)
    base_services = {
        service: {
            "container_name": f"{target.project_name}-{service}-1",
            "image": "example/airflow:3.2.2",
            "volumes": [
                {
                    "type": "bind",
                    "source": "/safe/plugins",
                    "target": "/opt/airflow/plugins",
                    "read_only": True,
                },
                {
                    "type": "volume",
                    "source": "airflow_logs",
                    "target": "/opt/airflow/logs",
                },
            ],
        }
        for service in code
    }
    base_services.update(
        {
            service: {
                "container_name": f"{target.project_name}-{service}-1",
                "image": "example/data:1",
                "volumes": [],
            }
            for service in forbidden
        }
    )
    overlay = __import__("yaml").safe_load(artifact.content)
    candidate_services = json.loads(json.dumps(base_services))
    for service in code:
        overlay_service = overlay["services"][service]
        candidate_services[service]["volumes"].extend(overlay_service["volumes"])
        if "environment" in overlay_service:
            candidate_services[service]["environment"] = dict(
                overlay_service["environment"]
            )
    return (
        {"name": target.project_name, "services": base_services},
        {"name": target.project_name, "services": candidate_services},
    )


def _dry_run_output(target, services: tuple[str, ...] | None = None) -> str:
    services = services or tuple(sorted(target.airflow_code_services))
    lines = [f"[+] Running {len(services) * 2}/{len(services) * 2}"]
    for service in services:
        container = f"{target.project_name}-{service}-1"
        lines.extend(
            [
                f" \x1b[32m✔\x1b[0m DRY-RUN MODE - Container {container} Recreated  0.1s",
                f" ✔ DRY-RUN MODE - Container {container} Healthy  0.2s",
            ]
        )
    return "\n".join(lines) + "\n"


def _compose_v5_dry_run_output(target) -> str:
    lines = []
    for index, service in enumerate(sorted(target.airflow_code_services), start=1):
        container = f"{target.project_name}-{service}-1"
        temporary = f"{index:012x}_{container}"
        lines.extend(
            [
                f"Container {container} Recreate",
                f"Container {container} Recreated",
                f"Container {temporary} Starting",
                f"Container {temporary} Started",
            ]
        )
        if service == "airflow-apiserver":
            lines.extend(
                [
                    f"Container {container} Waiting",
                    f"Container {temporary} Waiting",
                    f"Container {container} Healthy",
                    f"Container {temporary} Healthy",
                ]
            )
    return "\n".join(lines) + "\n"


def _airflow_322_output(payload: object) -> str:
    plugins = (
        "schemas",
        "tables",
        "types",
        "constraints",
        "defaults",
        "comments",
    )
    lines = [
        "2026-08-14T17:43:56.611303Z [info     ] "
        f"setup plugin alembic.autogenerate.{plugin} "
        "[alembic.runtime.plugins] loc=plugins.py:37"
        for plugin in plugins
    ]
    return "\n".join([*lines, json.dumps(payload, separators=(",", ":"))]) + "\n"


def _airflow_322_noop(message: str) -> str:
    lines = _airflow_322_output([]).splitlines()
    return "\n".join([*lines[:-1], message]) + "\n"


def test_airflow_uses_stable_overlay_and_exact_sorted_inventory_argv(tmp_path: Path):
    target, artifact, _ = _candidate(tmp_path)
    Path(str(target.generated_overlay_file)).write_bytes(artifact.content)
    dags = tuple(sorted(target.dag_allowlist))
    rows = [
        {"dag_id": "traffic_shared_runtime_dag", "is_paused": False},
        *[
            {"dag_id": dag_id, "is_paused": index % 2 == 0}
            for index, dag_id in enumerate(dags)
        ],
    ]
    runner = _QueueRunner([_ok(json.dumps(rows))])

    snapshot = AirflowCommandAdapter(target, runner).capture_pause_state(dags)

    assert snapshot == {
        row["dag_id"]: row["is_paused"]
        for row in rows
        if row["dag_id"] in target.dag_allowlist
    }
    assert runner.calls == [
        ((*_airflow_prefix(target), "dags", "list", "-o", "json"), Path(str(target.working_directory)))
    ]


def test_airflow_accepts_322_preamble_string_bools_and_consistent_duplicate_rows(
    tmp_path: Path,
):
    target, artifact, _ = _candidate(tmp_path)
    Path(str(target.generated_overlay_file)).write_bytes(artifact.content)
    dags = tuple(sorted(target.dag_allowlist))
    expected = {dag_id: index % 2 == 0 for index, dag_id in enumerate(dags)}
    rows = [{"dag_id": "traffic_shared_runtime_dag", "is_paused": "False"}]
    for dag_id, paused in expected.items():
        row = {"dag_id": dag_id, "is_paused": str(paused)}
        rows.extend((row, dict(row)))
    runner = _QueueRunner([_ok(_airflow_322_output(rows))])

    snapshot = AirflowCommandAdapter(target, runner).capture_pause_state(dags)

    assert snapshot == expected


@pytest.mark.parametrize(
    "stdout",
    [
        "unexpected log line\n[]\n",
        _airflow_322_output(
            [
                {"dag_id": "duplicate", "is_paused": "True"},
                {"dag_id": "duplicate", "is_paused": "False"},
            ]
        ),
        _airflow_322_output([{"dag_id": "duplicate", "is_paused": "FALSE"}]),
    ],
)
def test_airflow_rejects_unknown_preamble_conflicting_duplicates_and_bad_bool(
    tmp_path: Path, stdout: str
):
    target = _native_target(tmp_path)
    runner = _QueueRunner([_ok(stdout)])

    with pytest.raises(AirflowAdapterError, match="^airflow_adapter_invalid_output$"):
        AirflowCommandAdapter(target, runner).capture_pause_state(
            tuple(sorted(target.dag_allowlist))
        )


def test_airflow_counts_only_exact_writer_allowlist_and_validates_rows(tmp_path: Path):
    target, artifact, _ = _candidate(tmp_path)
    Path(str(target.generated_overlay_file)).write_bytes(artifact.content)
    writers = tuple(sorted(target.writer_dag_allowlist))
    responses: list[CompletedCommand] = []
    for dag_id in writers:
        responses.extend(
            [
                _ok(
                    _airflow_322_output(
                        [
                            {
                                "dag_id": dag_id,
                                "run_id": f"run-{dag_id}",
                                "state": "running",
                            }
                        ]
                    )
                ),
                _ok("[]"),
            ]
        )
    runner = _QueueRunner(responses)

    counts = AirflowCommandAdapter(target, runner).writer_run_counts(writers)

    assert counts == WriterRunCounts(running=len(writers), queued=0)
    expected = []
    for dag_id in writers:
        expected.extend(
            [
                (*_airflow_prefix(target), "dags", "list-runs", "--state", "running", "-o", "json", dag_id),
                (*_airflow_prefix(target), "dags", "list-runs", "--state", "queued", "-o", "json", dag_id),
            ]
        )
    assert [argv for argv, _ in runner.calls] == expected


@pytest.mark.parametrize(
    ("operation", "previously_paused"),
    [("pause", False), ("unpause", True)],
)
def test_airflow_mutation_accepts_airflow_322_previous_state_transition_output(
    tmp_path: Path, operation: str, previously_paused: bool
):
    target, artifact, _ = _candidate(tmp_path)
    Path(str(target.generated_overlay_file)).write_bytes(artifact.content)
    dag_id = sorted(target.dag_allowlist)[0]
    runner = _QueueRunner(
        [
            _ok(
                _airflow_322_output(
                    [{"dag_id": dag_id, "is_paused": str(previously_paused)}]
                )
            )
        ]
    )
    adapter = AirflowCommandAdapter(target, runner)

    getattr(adapter, f"{operation}_dag")(target, dag_id)

    assert runner.calls[0][0] == (
        *_airflow_prefix(target), "dags", operation, "-o", "json", "-y", dag_id
    )


@pytest.mark.parametrize("operation, expected_paused", [("pause", True), ("unpause", False)])
def test_airflow_mutation_rejects_post_state_json_for_a_transition(
    tmp_path: Path, operation: str, expected_paused: bool
):
    target, artifact, _ = _candidate(tmp_path)
    Path(str(target.generated_overlay_file)).write_bytes(artifact.content)
    dag_id = sorted(target.dag_allowlist)[0]
    runner = _QueueRunner(
        [
            _ok(
                _airflow_322_output(
                    [{"dag_id": dag_id, "is_paused": str(expected_paused)}]
                )
            )
        ]
    )

    with pytest.raises(AirflowAdapterError, match="^airflow_adapter_invalid_output$"):
        getattr(AirflowCommandAdapter(target, runner), f"{operation}_dag")(target, dag_id)


@pytest.mark.parametrize(
    ("operation", "paused", "message"),
    [
        ("pause", True, "No unpaused DAGs were found"),
        ("unpause", False, "No paused DAGs were found"),
    ],
)
def test_airflow_mutation_accepts_exact_322_idempotent_noop(
    tmp_path: Path, operation: str, paused: bool, message: str
):
    target, artifact, _ = _candidate(tmp_path)
    Path(str(target.generated_overlay_file)).write_bytes(artifact.content)
    dag_id = sorted(target.dag_allowlist)[0]
    runner = _QueueRunner([_ok(_airflow_322_noop(message))])

    getattr(AirflowCommandAdapter(target, runner), f"{operation}_dag")(target, dag_id)

    assert runner.calls[0][0] == (
        *_airflow_prefix(target), "dags", operation, "-o", "json", "-y", dag_id
    )


@pytest.mark.parametrize(
    ("operation", "paused", "stdout"),
    [
        ("pause", True, _airflow_322_noop("No paused DAGs were found")),
        ("unpause", False, _airflow_322_noop("No unpaused DAGs were found")),
        ("pause", True, "No unpaused DAGs were found\n"),
        ("unpause", False, _airflow_322_noop("No paused DAGs were found private")),
    ],
)
def test_airflow_mutation_rejects_nonexact_idempotent_noop(
    tmp_path: Path, operation: str, paused: bool, stdout: str
):
    target = _native_target(tmp_path)
    dag_id = sorted(target.dag_allowlist)[0]
    runner = _QueueRunner([_ok(stdout)])

    with pytest.raises(AirflowAdapterError, match="^airflow_adapter_invalid_output$") as error:
        getattr(AirflowCommandAdapter(target, runner), f"{operation}_dag")(target, dag_id)

    assert "private" not in str(error.value)


def test_airflow_rejects_mutation_response_for_another_dag(tmp_path: Path):
    target = _native_target(tmp_path)
    dag_id = sorted(target.dag_allowlist)[0]
    runner = _QueueRunner([_ok(json.dumps([{"dag_id": "foreign", "is_paused": True}]))])

    with pytest.raises(AirflowAdapterError, match="^airflow_adapter_invalid_output$"):
        AirflowCommandAdapter(target, runner).pause_dag(target, dag_id)


@pytest.mark.parametrize(
    "case",
    ["unsorted-snapshot", "foreign-snapshot", "foreign-mutation", "wrong-target"],
)
def test_airflow_rejects_allowlist_and_target_mismatch_before_runner(tmp_path: Path, case: str):
    target = _native_target(tmp_path)
    runner = _QueueRunner([])
    adapter = AirflowCommandAdapter(target, runner)
    dags = tuple(sorted(target.dag_allowlist))

    with pytest.raises(AirflowAdapterError, match="^airflow_adapter_input_rejected$"):
        if case == "unsorted-snapshot":
            adapter.capture_pause_state(tuple(reversed(dags)))
        elif case == "foreign-snapshot":
            adapter.capture_pause_state((*dags[:-1], "foreign"))
        elif case == "foreign-mutation":
            adapter.pause_dag(target, "foreign")
        else:
            adapter.pause_dag(replace(target, project_name="other"), dags[0])
    assert runner.calls == []


@pytest.mark.parametrize(
    "stdout",
    [
        "not-json token=private",
        json.dumps([{"dag_id": "foreign"}]),
        json.dumps([{"dag_id": "duplicate", "is_paused": False}] * 2),
    ],
)
def test_airflow_rejects_malformed_or_missing_weather_dag_without_leaking_output(
    tmp_path: Path, stdout: str
):
    target = _native_target(tmp_path)
    runner = _QueueRunner([_ok(stdout)])

    with pytest.raises(AirflowAdapterError, match="^airflow_adapter_invalid_output$") as error:
        AirflowCommandAdapter(target, runner).capture_pause_state(tuple(sorted(target.dag_allowlist)))

    assert "private" not in str(error.value)
    assert "foreign" not in str(error.value)


def test_compose_candidate_uses_only_staged_overlay_then_exact_dry_run(tmp_path: Path):
    target, artifact, staged = _candidate(tmp_path)
    base, candidate = _compose_documents(target, artifact)
    runner = _QueueRunner(
        [
            _ok(json.dumps(base)),
            _ok(json.dumps(candidate)),
            _ok(_dry_run_output(target)),
        ]
    )

    ComposeCommandAdapter(target, runner).validate_candidate(target, staged)

    base_prefix = _compose_prefix(target)
    candidate_prefix = _compose_prefix(target, staged)
    services = tuple(sorted(target.airflow_code_services))
    assert [argv for argv, _ in runner.calls] == [
        (*base_prefix, "config", "--format", "json"),
        (*candidate_prefix, "config", "--format", "json"),
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
            *services,
        ),
    ]


def test_compose_and_health_allow_declared_forbidden_orphan_absent_from_config(
    tmp_path: Path,
):
    base_target, artifact, staged = _candidate(tmp_path)
    orphan = "declared-orphan-data-service"
    target = replace(
        base_target,
        forbidden_data_services=base_target.forbidden_data_services | {orphan},
    )
    base, candidate = _compose_documents(target, artifact)
    base["services"].pop(orphan)
    candidate["services"].pop(orphan)
    compose_runner = _QueueRunner(
        [
            _ok(json.dumps(base)),
            _ok(json.dumps(candidate)),
            _ok(_dry_run_output(target)),
        ]
    )

    ComposeCommandAdapter(target, compose_runner).validate_candidate(target, staged)

    stable = Path(str(target.generated_overlay_file))
    stable.write_bytes(artifact.content)
    services = tuple(sorted(target.airflow_code_services))
    ps = [
        {"Service": service, "State": "running", "Health": ""}
        for service in services
    ]
    dags = [
        {"dag_id": dag_id, "is_paused": True}
        for dag_id in sorted(target.dag_allowlist)
    ]
    health_runner = _QueueRunner(
        [
            _ok(json.dumps(base)),
            _ok(json.dumps(ps)),
            _ok(json.dumps(dags)),
            _ok("[]"),
        ]
    )

    assert HealthCommandAdapter(target, health_runner).read_health(target, artifact) == "passed"


@pytest.mark.parametrize("stream_mode", ["stderr-only", "split"])
def test_compose_validates_doc_shaped_dry_run_across_progress_streams(
    tmp_path: Path, stream_mode: str
):
    target, artifact, staged = _candidate(tmp_path)
    base, candidate = _compose_documents(target, artifact)
    output_lines = _dry_run_output(target).splitlines()
    if stream_mode == "stderr-only":
        dry_run = CompletedCommand(
            stdout="", stderr="\n".join(output_lines) + "\n", returncode=0
        )
    else:
        split = 1 + len(output_lines) // 2
        dry_run = CompletedCommand(
            stdout="\n".join(output_lines[:split]) + "\n",
            stderr="\n".join(output_lines[split:]) + "\n",
            returncode=0,
        )
    runner = _QueueRunner([_ok(json.dumps(base)), _ok(json.dumps(candidate)), dry_run])

    ComposeCommandAdapter(target, runner).validate_candidate(target, staged)

    assert len(runner.calls) == 3


def test_compose_dry_run_nonzero_rejects_even_with_valid_progress(tmp_path: Path):
    target, artifact, staged = _candidate(tmp_path)
    base, candidate = _compose_documents(target, artifact)
    dry_run = CompletedCommand(
        stdout="", stderr=_dry_run_output(target), returncode=2
    )
    runner = _QueueRunner([_ok(json.dumps(base)), _ok(json.dumps(candidate)), dry_run])

    with pytest.raises(ComposeAdapterError, match="^compose_adapter_command_failed$"):
        ComposeCommandAdapter(target, runner).validate_candidate(target, staged)


def test_compose_dry_run_hostile_stderr_is_parsed_and_rejected(tmp_path: Path):
    target, artifact, staged = _candidate(tmp_path)
    base, candidate = _compose_documents(target, artifact)
    dry_run = CompletedCommand(
        stdout=_dry_run_output(target),
        stderr="warning token=private unknown-service\n",
        returncode=0,
    )
    runner = _QueueRunner([_ok(json.dumps(base)), _ok(json.dumps(candidate)), dry_run])

    with pytest.raises(ComposeAdapterError, match="^compose_adapter_dry_run_rejected$") as error:
        ComposeCommandAdapter(target, runner).validate_candidate(target, staged)

    assert "private" not in str(error.value)


def test_compose_accepts_unchanged_airflow_init_as_immutable_non_target(tmp_path: Path):
    target, artifact, staged = _candidate(tmp_path)
    base, candidate = _compose_documents(target, artifact)
    init_body = {
        "container_name": f"{target.project_name}-airflow-init-1",
        "image": "example/airflow:3.2.2",
        "volumes": [],
    }
    base["services"]["airflow-init"] = init_body
    candidate["services"]["airflow-init"] = json.loads(json.dumps(init_body))
    runner = _QueueRunner(
        [_ok(json.dumps(base)), _ok(json.dumps(candidate)), _ok(_dry_run_output(target))]
    )

    ComposeCommandAdapter(target, runner).validate_candidate(target, staged)

    assert len(runner.calls) == 3


def test_compose_deploy_uses_only_stable_overlay_and_safe_exact_services(tmp_path: Path):
    target, artifact, _ = _candidate(tmp_path)
    stable = Path(str(target.generated_overlay_file))
    stable.write_bytes(artifact.content)
    _, candidate = _compose_documents(target, artifact)
    runner = _QueueRunner([_ok(json.dumps(candidate)), _ok()])
    services = tuple(sorted(target.airflow_code_services))

    ComposeCommandAdapter(target, runner).deploy_code_services(target, stable, services)

    argv, cwd = runner.calls[1]
    assert argv == (
        *_compose_prefix(target, stable),
        "up",
        "-d",
        "--no-deps",
        "--no-build",
        "--pull",
        "never",
        *services,
    )
    assert cwd == Path(str(target.working_directory))
    assert not {"down", "restart", "build", "--force-recreate", "--remove-orphans"} & set(argv)
    assert not set(target.forbidden_data_services) & set(argv)


def _deploy_progress_output(target) -> str:
    lines = []
    for service in sorted(target.airflow_code_services):
        container = f"{target.project_name}-{service}-1"
        lines.extend(
            [
                f"Container {container} Recreate",
                f"Container {container} Recreated",
                f"Container {container} Starting",
                f"Container {container} Started",
            ]
        )
    return "\n".join(lines) + "\n"


def test_compose_deploy_accepts_only_code_container_progress_on_stderr(tmp_path: Path):
    target, artifact, _ = _candidate(tmp_path)
    stable = Path(str(target.generated_overlay_file))
    stable.write_bytes(artifact.content)
    _, candidate = _compose_documents(target, artifact)
    runner = _QueueRunner(
        [
            _ok(json.dumps(candidate)),
            CompletedCommand(stdout="", stderr=_deploy_progress_output(target), returncode=0),
        ]
    )

    ComposeCommandAdapter(target, runner).deploy_code_services(target, stable, tuple(sorted(target.airflow_code_services)))


@pytest.mark.parametrize("stderr", ["warning token=private\n", "Container unknown-service-1 Started\n", "Container example-weather-airflow-apiserver-1 Failed\n"])
def test_compose_deploy_rejects_unrecognized_stderr(tmp_path: Path, stderr: str):
    target, artifact, _ = _candidate(tmp_path)
    stable = Path(str(target.generated_overlay_file))
    stable.write_bytes(artifact.content)
    _, candidate = _compose_documents(target, artifact)
    runner = _QueueRunner([_ok(json.dumps(candidate)), CompletedCommand(stdout="", stderr=stderr, returncode=0)])

    with pytest.raises(ComposeAdapterError, match="^compose_adapter_command_failed$"):
        ComposeCommandAdapter(target, runner).deploy_code_services(target, stable, tuple(sorted(target.airflow_code_services)))


@pytest.mark.parametrize("bad_service", ["airflow-init", "unknown", "example-postgres"])
def test_compose_dry_run_rejects_non_code_container_target(tmp_path: Path, bad_service: str):
    target, artifact, staged = _candidate(tmp_path)
    base, candidate = _compose_documents(target, artifact)
    output = _dry_run_output(target, (bad_service,))
    runner = _QueueRunner([_ok(json.dumps(base)), _ok(json.dumps(candidate)), _ok(output)])

    with pytest.raises(ComposeAdapterError, match="^compose_adapter_dry_run_rejected$"):
        ComposeCommandAdapter(target, runner).validate_candidate(target, staged)


@pytest.mark.parametrize(
    "unsafe_line",
    [
        " ✔ DRY-RUN MODE - Network example-weather_default Created 0.0s",
        " ✔ DRY-RUN MODE - example-postgres Pulled 0.0s",
        " ✔ DRY-RUN MODE - Service airflow-init Started 0.0s",
        (
            " ✔ DRY-RUN MODE - Container "
            "example-weather-example-airflow-api-1 Started 0.0s; "
            "Container example-weather-example-postgres-1 Started"
        ),
        (
            " ✔ DRY-RUN MODE - Container "
            "example-weather-example-airflow-api-1 Started 0.0s credential=private"
        ),
        "unclassified progress text",
    ],
)
def test_compose_dry_run_allows_only_explicit_progress_and_code_container_lines(
    tmp_path: Path, unsafe_line: str
):
    target, artifact, staged = _candidate(tmp_path)
    base, candidate = _compose_documents(target, artifact)
    output = _dry_run_output(target) + unsafe_line + "\n"
    runner = _QueueRunner([_ok(json.dumps(base)), _ok(json.dumps(candidate)), _ok(output)])

    with pytest.raises(ComposeAdapterError, match="^compose_adapter_dry_run_rejected$"):
        ComposeCommandAdapter(target, runner).validate_candidate(target, staged)


def test_compose_rejects_changed_forbidden_service_before_dry_run(tmp_path: Path):
    target, artifact, staged = _candidate(tmp_path)
    base, candidate = _compose_documents(target, artifact)
    candidate["services"][sorted(target.forbidden_data_services)[0]]["image"] = "changed"
    runner = _QueueRunner([_ok(json.dumps(base)), _ok(json.dumps(candidate))])

    with pytest.raises(ComposeAdapterError, match="^compose_adapter_config_rejected$"):
        ComposeCommandAdapter(target, runner).validate_candidate(target, staged)

    assert len(runner.calls) == 2


def test_compose_rejects_non_read_only_code_mount_and_unproven_dry_run(tmp_path: Path):
    target, artifact, staged = _candidate(tmp_path)
    base, candidate = _compose_documents(target, artifact)
    service = sorted(target.airflow_code_services)[0]
    dbt_mount = next(
        volume
        for volume in candidate["services"][service]["volumes"]
        if volume["target"] == "/opt/airflow/dbt"
    )
    dbt_mount["read_only"] = False
    runner = _QueueRunner([_ok(json.dumps(base)), _ok(json.dumps(candidate))])

    with pytest.raises(ComposeAdapterError, match="^compose_adapter_config_rejected$"):
        ComposeCommandAdapter(target, runner).validate_candidate(target, staged)

    base, candidate = _compose_documents(target, artifact)
    runner = _QueueRunner([_ok(json.dumps(base)), _ok(json.dumps(candidate)), _ok("")])
    with pytest.raises(ComposeAdapterError, match="^compose_adapter_dry_run_rejected$"):
        ComposeCommandAdapter(target, runner).validate_candidate(target, staged)


def test_compose_accepts_omitted_read_only_false_for_writable_baseline_bind(
    tmp_path: Path,
):
    target, artifact, staged = _baseline_candidate(tmp_path)
    base, candidate = _compose_documents(target, artifact)
    service = sorted(target.airflow_code_services)[0]
    dbt_mount = next(
        volume
        for volume in candidate["services"][service]["volumes"]
        if volume["target"] == "/opt/airflow/dbt"
    )
    assert dbt_mount.pop("read_only") is False
    runner = _QueueRunner(
        [
            _ok(json.dumps(base)),
            _ok(json.dumps(candidate)),
            _ok(_dry_run_output(target)),
        ]
    )

    ComposeCommandAdapter(target, runner).validate_candidate(target, staged)


def test_compose_accepts_compose_v5_dry_run_lifecycle(tmp_path: Path):
    target, artifact, staged = _baseline_candidate(tmp_path)
    base, candidate = _compose_documents(target, artifact)
    runner = _QueueRunner(
        [
            _ok(json.dumps(base)),
            _ok(json.dumps(candidate)),
            _ok(_compose_v5_dry_run_output(target)),
        ]
    )

    ComposeCommandAdapter(target, runner).validate_candidate(target, staged)


@pytest.mark.parametrize(
    "case",
    [
        "missing-base-transition",
        "missing-temporary-transition",
        "temporary-prefix-drift",
        "forbidden-container",
        "mixed-output-grammar",
        "unknown-status",
        "reversed-base-transition",
        "reversed-temporary-transition",
        "premature-base-wait",
        "premature-temporary-wait",
    ],
)
def test_compose_v5_dry_run_rejects_unproven_lifecycle(tmp_path: Path, case: str):
    target, artifact, staged = _baseline_candidate(tmp_path)
    base, candidate = _compose_documents(target, artifact)
    service = sorted(target.airflow_code_services)[0]
    container = f"{target.project_name}-{service}-1"
    temporary = f"{'1':0>12}_{container}"
    output = _compose_v5_dry_run_output(target)
    if case == "missing-base-transition":
        output = output.replace(f"Container {container} Recreated\n", "")
    elif case == "missing-temporary-transition":
        output = output.replace(f"Container {temporary} Started\n", "")
    elif case == "temporary-prefix-drift":
        output = output.replace(
            f"Container {temporary} Started\n",
            f"Container {'f' * 12}_{container} Started\n",
        )
    elif case == "forbidden-container":
        forbidden = sorted(target.forbidden_data_services)[0]
        output += f"Container {target.project_name}-{forbidden}-1 Recreate\n"
    elif case == "mixed-output-grammar":
        output += _dry_run_output(target)
    elif case == "unknown-status":
        output = output.replace(
            f"Container {container} Recreated\n",
            f"Container {container} Removed\n",
        )
    elif case == "reversed-base-transition":
        output = output.replace(
            f"Container {container} Recreate\nContainer {container} Recreated\n",
            f"Container {container} Recreated\nContainer {container} Recreate\n",
        )
    elif case == "reversed-temporary-transition":
        output = output.replace(
            f"Container {temporary} Starting\nContainer {temporary} Started\n",
            f"Container {temporary} Started\nContainer {temporary} Starting\n",
        )
    elif case == "premature-base-wait":
        output = output.replace(f"Container {container} Waiting\n", "", 1)
        output = f"Container {container} Waiting\n" + output
    else:
        output = output.replace(f"Container {temporary} Waiting\n", "", 1)
        output = f"Container {temporary} Waiting\n" + output
    runner = _QueueRunner(
        [_ok(json.dumps(base)), _ok(json.dumps(candidate)), _ok(output)]
    )

    with pytest.raises(
        ComposeAdapterError, match="^compose_adapter_dry_run_rejected$"
    ):
        ComposeCommandAdapter(target, runner).validate_candidate(target, staged)


@pytest.mark.parametrize(
    ("candidate_factory", "mount_target", "invalid_value"),
    [
        (_baseline_candidate, "/opt/airflow/dbt", 0),
        (_candidate, "/opt/airflow/dags", 1),
    ],
)
def test_compose_rejects_bool_like_read_only_values(
    tmp_path: Path, candidate_factory, mount_target: str, invalid_value: int
):
    target, artifact, staged = candidate_factory(tmp_path)
    base, candidate = _compose_documents(target, artifact)
    service = sorted(target.airflow_code_services)[0]
    mount = next(
        volume
        for volume in candidate["services"][service]["volumes"]
        if volume["target"] == mount_target
    )
    mount["read_only"] = invalid_value
    runner = _QueueRunner([_ok(json.dumps(base)), _ok(json.dumps(candidate))])

    with pytest.raises(
        ComposeAdapterError, match="^compose_adapter_config_rejected$"
    ):
        ComposeCommandAdapter(target, runner).validate_candidate(target, staged)

    assert len(runner.calls) == 2


@pytest.mark.parametrize(
    "case",
    [
        "missing-artifact-env",
        "wrong-artifact-env",
        "missing-logs-mount",
        "read-only-logs-mount",
        "duplicate-logs-mount",
    ],
)
def test_compose_release_requires_exact_external_artifact_environment_and_writable_logs(
    tmp_path: Path, case: str
):
    target, artifact, staged = _candidate(tmp_path)
    base, candidate = _compose_documents(target, artifact)
    service = sorted(target.airflow_code_services)[0]
    body = candidate["services"][service]

    if case == "missing-artifact-env":
        body.pop("environment")
    elif case == "wrong-artifact-env":
        body["environment"]["ASK_SEOUL_DBT_ARTIFACT_ROOT"] = "/tmp/wrong"
    else:
        logs_mount = next(
            volume
            for volume in body["volumes"]
            if volume["target"] == "/opt/airflow/logs"
        )
        if case == "missing-logs-mount":
            body["volumes"].remove(logs_mount)
        elif case == "read-only-logs-mount":
            logs_mount["read_only"] = True
        else:
            body["volumes"].append(dict(logs_mount))

    runner = _QueueRunner([_ok(json.dumps(base)), _ok(json.dumps(candidate))])

    with pytest.raises(
        ComposeAdapterError, match="^compose_adapter_config_rejected$"
    ):
        ComposeCommandAdapter(target, runner).validate_candidate(target, staged)

    assert len(runner.calls) == 2


@pytest.mark.parametrize("case", ["stable-as-candidate", "foreign-temp", "bad-content", "wrong-services"])
def test_compose_rejects_bad_candidate_or_service_input_before_mutation(tmp_path: Path, case: str):
    target, artifact, staged = _candidate(tmp_path)
    runner = _QueueRunner([])
    adapter = ComposeCommandAdapter(target, runner)
    if case == "stable-as-candidate":
        path = Path(str(target.generated_overlay_file))
        path.write_bytes(artifact.content)

        def action():
            adapter.validate_candidate(target, path)
    elif case == "foreign-temp":
        path = tmp_path / staged.name
        path.write_bytes(artifact.content)

        def action():
            adapter.validate_candidate(target, path)
    elif case == "bad-content":
        staged.write_text("token=private", encoding="utf-8")

        def action():
            adapter.validate_candidate(target, staged)
    else:
        stable = Path(str(target.generated_overlay_file))
        stable.write_bytes(artifact.content)

        def action():
            adapter.deploy_code_services(target, stable, ("unknown",))

    with pytest.raises(ComposeAdapterError):
        action()
    assert runner.calls == []


class _GitRunner:
    def __init__(self, source: Path, *, source_head: str = SHA, source_origin: str = "https://github.com/owner/seoul-weather-platform.git"):
        self.source = source
        self.source_head = source_head
        self.source_origin = source_origin
        self.calls: list[tuple[tuple[str, ...], Path]] = []

    def run(self, argv, cwd: Path) -> CompletedCommand:
        command = tuple(argv)
        self.calls.append((command, cwd))
        if command == ("git", "rev-parse", "HEAD"):
            return _ok((self.source_head if cwd == self.source else SHA) + "\n")
        if command == ("git", "config", "--get", "remote.origin.url"):
            return _ok((self.source_origin if cwd == self.source else str(self.source)) + "\n")
        if command == ("git", "status", "--porcelain"):
            return _ok()
        if command[:5] == ("git", "clone", "--local", "--no-hardlinks", "--no-checkout"):
            Path(command[-1]).mkdir(parents=True, exist_ok=True)
            (Path(command[-1]) / ".git").mkdir()
            return _ok()
        if command == ("git", "checkout", "--detach", SHA):
            (cwd / "dags").mkdir()
            (cwd / "dbt").mkdir()
            return _ok()
        raise AssertionError(f"unexpected command: {command!r}")


class _CheckoutFailureGitRunner(_GitRunner):
    def run(self, argv, cwd: Path) -> CompletedCommand:
        if tuple(argv) == ("git", "checkout", "--detach", SHA):
            self.calls.append((tuple(argv), cwd))
            return CompletedCommand(stdout="private", stderr="private", returncode=2)
        return super().run(argv, cwd)


class _GitProgressRunner(_GitRunner):
    def run(self, argv, cwd: Path) -> CompletedCommand:
        result = super().run(argv, cwd)
        if tuple(argv)[1] in {"clone", "checkout"}:
            return CompletedCommand(stdout=result.stdout, stderr="progress", returncode=0)
        return result


class _GitSourceWarningRunner(_GitRunner):
    def run(self, argv, cwd: Path) -> CompletedCommand:
        result = super().run(argv, cwd)
        if tuple(argv) == ("git", "rev-parse", "HEAD") and cwd == self.source:
            return CompletedCommand(stdout=result.stdout, stderr="warning", returncode=0)
        return result


def test_git_creates_standalone_local_detached_release_with_exact_commands(tmp_path: Path):
    target = _native_target(tmp_path)
    source = tmp_path / "trusted-source"
    source.mkdir()
    (source / ".git").mkdir()
    destination = Path(str(target.runtime_root)) / "releases" / SHA
    runner = _GitRunner(source)

    result = GitCommandAdapter(target, source, runner).detached_checkout(REPOSITORY, SHA, destination)

    assert result == destination
    assert (destination / ".git").is_dir()
    assert (destination / "dags").is_dir()
    assert (destination / "dbt").is_dir()
    clone = next(call for call in runner.calls if call[0][1:5] == ("clone", "--local", "--no-hardlinks", "--no-checkout"))
    temp = Path(clone[0][-1])
    assert clone[0][5] == str(source)
    assert temp.parent == destination.parent
    assert temp.name.startswith(f".{SHA}.") and temp.name.endswith(".tmp")
    assert ("git", "checkout", "--detach", SHA) in [argv for argv, _ in runner.calls]


def test_git_allows_successful_progress_stderr_only_for_clone_and_checkout(tmp_path: Path):
    target = _native_target(tmp_path)
    source = tmp_path / "trusted-source"
    source.mkdir()
    (source / ".git").mkdir()
    destination = Path(str(target.runtime_root)) / "releases" / SHA

    assert (
        GitCommandAdapter(target, source, _GitProgressRunner(source)).detached_checkout(
            REPOSITORY, SHA, destination
        )
        == destination
    )


def test_git_rejects_successful_stderr_from_source_verification(tmp_path: Path):
    target = _native_target(tmp_path)
    source = tmp_path / "trusted-source"
    source.mkdir()
    (source / ".git").mkdir()
    destination = Path(str(target.runtime_root)) / "releases" / SHA

    with pytest.raises(GitAdapterError, match="^git_adapter_command_failed$"):
        GitCommandAdapter(target, source, _GitSourceWarningRunner(source)).detached_checkout(
            REPOSITORY, SHA, destination
        )


def test_git_checkout_failure_removes_only_its_bounded_sibling_temp(tmp_path: Path):
    target = _native_target(tmp_path)
    source = tmp_path / "trusted-source"
    source.mkdir()
    (source / ".git").mkdir()
    destination = Path(str(target.runtime_root)) / "releases" / SHA
    sibling = destination.parent / "operator-owned"
    sibling.mkdir(parents=True)
    runner = _CheckoutFailureGitRunner(source)

    with pytest.raises(GitAdapterError, match="^git_adapter_command_failed$") as error:
        GitCommandAdapter(target, source, runner).detached_checkout(REPOSITORY, SHA, destination)

    assert "private" not in str(error.value)
    assert sibling.is_dir()
    assert not destination.exists()
    assert list(destination.parent.glob(f".{SHA}.*.tmp")) == []


def test_git_reuses_only_exact_clean_existing_standalone_release(tmp_path: Path):
    target = _native_target(tmp_path)
    source = tmp_path / "trusted-source"
    source.mkdir()
    (source / ".git").mkdir()
    destination = Path(str(target.runtime_root)) / "releases" / SHA
    for child in (destination / ".git", destination / "dags", destination / "dbt"):
        child.mkdir(parents=True, exist_ok=True)
    runner = _GitRunner(source)

    assert GitCommandAdapter(target, source, runner).detached_checkout(REPOSITORY, SHA, destination) == destination
    assert not any(argv[1] == "clone" for argv, _ in runner.calls)


def test_git_rejects_existing_worktree_metadata_file_as_non_standalone(tmp_path: Path):
    target = _native_target(tmp_path)
    source = tmp_path / "trusted-source"
    source.mkdir()
    (source / ".git").mkdir()
    destination = Path(str(target.runtime_root)) / "releases" / SHA
    destination.mkdir(parents=True)
    (destination / ".git").write_text("gitdir: C:/private/worktree", encoding="utf-8")
    (destination / "dags").mkdir()
    (destination / "dbt").mkdir()
    runner = _GitRunner(source)

    with pytest.raises(GitAdapterError, match="^git_adapter_release_rejected$"):
        GitCommandAdapter(target, source, runner).detached_checkout(REPOSITORY, SHA, destination)


@pytest.mark.parametrize("case", ["wrong-head", "wrong-origin", "wrong-destination", "dirty-existing", "missing-dirs"])
def test_git_fails_closed_without_overwriting_invalid_state(tmp_path: Path, case: str):
    target = _native_target(tmp_path)
    source = tmp_path / "trusted-source"
    source.mkdir()
    (source / ".git").mkdir()
    destination = Path(str(target.runtime_root)) / "releases" / SHA
    runner = _GitRunner(
        source,
        source_head=("b" * 40 if case == "wrong-head" else SHA),
        source_origin=("https://github.com/other/repo.git" if case == "wrong-origin" else "https://github.com/owner/seoul-weather-platform.git"),
    )
    if case in {"dirty-existing", "missing-dirs"}:
        (destination / ".git").mkdir(parents=True)
        if case != "missing-dirs":
            (destination / "dags").mkdir()
            (destination / "dbt").mkdir()
        if case == "dirty-existing":
            original_run = runner.run

            def dirty(argv, cwd):
                if tuple(argv) == ("git", "status", "--porcelain") and cwd == destination:
                    runner.calls.append((tuple(argv), cwd))
                    return _ok(" M private-file\n")
                return original_run(argv, cwd)

            runner.run = dirty
    requested = destination if case != "wrong-destination" else destination.parent / ("b" * 40)

    with pytest.raises(GitAdapterError):
        GitCommandAdapter(target, source, runner).detached_checkout(REPOSITORY, SHA, requested)

    if destination.exists():
        assert destination.exists()


def test_health_binds_stable_bytes_services_dags_and_import_errors(tmp_path: Path):
    target, artifact, _ = _candidate(tmp_path)
    stable = Path(str(target.generated_overlay_file))
    stable.write_bytes(artifact.content)
    services = tuple(sorted(target.airflow_code_services))
    healthchecked = set(services[:2])
    config = {
        "name": target.project_name,
        "services": {
            service: (
                {"image": "example/airflow:3.2.2", "healthcheck": {"test": ["CMD", "true"]}}
                if service in healthchecked
                else {"image": "example/airflow:3.2.2"}
            )
            for service in services
        }
        | {
            service: {"image": "example/data:1"}
            for service in target.forbidden_data_services
        },
    }
    ps = [
        {
            "Service": service,
            "State": "running",
            "Health": ("healthy" if service in healthchecked else ""),
        }
        for service in services
    ]
    dags = [{"dag_id": dag_id, "is_paused": True} for dag_id in sorted(target.dag_allowlist)]
    runner = _QueueRunner(
        [_ok(json.dumps(config)), _ok(json.dumps(ps)), _ok(json.dumps(dags)), _ok("[]")]
    )

    result = HealthCommandAdapter(target, runner).read_health(target, artifact)

    assert result == "passed"
    assert [argv for argv, _ in runner.calls] == [
        (*_compose_prefix(target, stable), "config", "--format", "json"),
        (*_compose_prefix(target, stable), "ps", "--format", "json", *services),
        (*_airflow_prefix(target), "dags", "list", "-o", "json"),
        (*_airflow_prefix(target), "dags", "list-import-errors", "-o", "json"),
    ]


def test_health_accepts_compose_ndjson_and_airflow_322_output(tmp_path: Path):
    target, artifact, _ = _candidate(tmp_path)
    stable = Path(str(target.generated_overlay_file))
    stable.write_bytes(artifact.content)
    services = tuple(sorted(target.airflow_code_services))
    config = {
        "name": target.project_name,
        "services": {
            service: {
                "image": "example/airflow:3.2.2",
                "healthcheck": {"test": ["CMD", "true"]},
            }
            for service in services
        }
        | {
            service: {"image": "example/data:1"}
            for service in target.forbidden_data_services
        },
    }
    ps = [
        {"Service": service, "State": "running", "Health": "healthy"}
        for service in services
    ]
    ps_ndjson = "\n".join(json.dumps(row) for row in ps) + "\n"
    dags = [
        {"dag_id": dag_id, "is_paused": "True"}
        for dag_id in sorted(target.dag_allowlist)
    ]
    runner = _QueueRunner(
        [
            _ok(json.dumps(config)),
            _ok(ps_ndjson),
            _ok(_airflow_322_output(dags)),
            _ok(_airflow_322_output([])),
        ]
    )

    assert HealthCommandAdapter(target, runner).read_health(target, artifact) == "passed"


@pytest.mark.parametrize(
    "healthcheck, health, expected_calls",
    [
        ({"test": ["CMD", "true"]}, "", 2),
        (None, "healthy", 2),
        ({"disable": True}, "", 1),
    ],
)
def test_health_enforces_config_declared_healthcheck_contract(
    tmp_path: Path,
    healthcheck: dict[str, object] | None,
    health: str,
    expected_calls: int,
):
    target, artifact, _ = _candidate(tmp_path)
    stable = Path(str(target.generated_overlay_file))
    stable.write_bytes(artifact.content)
    services = tuple(sorted(target.airflow_code_services))
    config_services: dict[str, dict[str, object]] = {
        service: {"image": "example/airflow:3.2.2"} for service in services
    }
    if healthcheck is not None:
        config_services[services[0]]["healthcheck"] = healthcheck
    config_services.update(
        {service: {"image": "example/data:1"} for service in target.forbidden_data_services}
    )
    config = {"name": target.project_name, "services": config_services}
    ps = [
        {
            "Service": service,
            "State": "running",
            "Health": (health if service == services[0] else ""),
        }
        for service in services
    ]
    runner = _QueueRunner([_ok(json.dumps(config)), _ok(json.dumps(ps))])

    with pytest.raises(HealthAdapterError, match="^health_adapter_invalid_output$"):
        HealthCommandAdapter(target, runner).read_health(target, artifact)

    assert len(runner.calls) == expected_calls


@pytest.mark.parametrize("case", ["extra-service", "malformed-healthcheck"])
def test_health_rejects_config_service_drift_and_malformed_healthcheck_before_ps(
    tmp_path: Path, case: str
):
    target, artifact, _ = _candidate(tmp_path)
    stable = Path(str(target.generated_overlay_file))
    stable.write_bytes(artifact.content)
    services = tuple(sorted(target.airflow_code_services))
    config_services: dict[str, object] = {
        service: {"image": "example/airflow:3.2.2"} for service in services
    }
    config_services.update(
        {service: {"image": "example/data:1"} for service in target.forbidden_data_services}
    )
    if case == "extra-service":
        config_services["unknown-service"] = {"image": "unknown:1"}
    else:
        config_services[services[0]]["healthcheck"] = "not-an-object"
    config = {"name": target.project_name, "services": config_services}
    runner = _QueueRunner([_ok(json.dumps(config))])

    with pytest.raises(HealthAdapterError, match="^health_adapter_invalid_output$"):
        HealthCommandAdapter(target, runner).read_health(target, artifact)

    assert len(runner.calls) == 1


def test_health_rejects_forged_overlay_metadata_before_runner(tmp_path: Path):
    target, artifact, _ = _candidate(tmp_path)
    stable = Path(str(target.generated_overlay_file))
    stable.write_bytes(artifact.content)
    forged = OverlayArtifact(
        kind="baseline",
        candidate_sha=None,
        content=artifact.content,
        sha256=artifact.sha256,
    )
    runner = _QueueRunner([])

    with pytest.raises(HealthAdapterError, match="^health_adapter_overlay_mismatch$"):
        HealthCommandAdapter(target, runner).read_health(target, forged)

    assert runner.calls == []


@pytest.mark.parametrize(
    "case",
    ["wrong-bytes", "missing-service", "unhealthy", "missing-dag", "import-error"],
)
def test_health_rejects_any_unbound_or_unhealthy_readback(tmp_path: Path, case: str):
    target, artifact, _ = _candidate(tmp_path)
    stable = Path(str(target.generated_overlay_file))
    stable.write_bytes(artifact.content if case != "wrong-bytes" else artifact.content + b"# changed\n")
    services = tuple(sorted(target.airflow_code_services))
    config = {
        "name": target.project_name,
        "services": {
            service: {
                "image": "example/airflow:3.2.2",
                "healthcheck": {"test": ["CMD", "true"]},
            }
            for service in services
        }
        | {
            service: {"image": "example/data:1"}
            for service in target.forbidden_data_services
        },
    }
    ps = [{"Service": service, "State": "running", "Health": "healthy"} for service in services]
    dags = [{"dag_id": dag_id, "is_paused": True} for dag_id in sorted(target.dag_allowlist)]
    imports: list[dict[str, object]] = []
    if case == "missing-service":
        ps.pop()
    elif case == "unhealthy":
        ps[0]["Health"] = "unhealthy"
    elif case == "missing-dag":
        dags.pop()
    elif case == "import-error":
        imports.append({"filename": "/private/path.py", "stack_trace": "token=private"})
    runner = _QueueRunner(
        [
            _ok(json.dumps(config)),
            _ok(json.dumps(ps)),
            _ok(json.dumps(dags)),
            _ok(json.dumps(imports)),
        ]
    )

    with pytest.raises(HealthAdapterError) as error:
        HealthCommandAdapter(target, runner).read_health(target, artifact)

    assert "private" not in str(error.value)
    if case == "wrong-bytes":
        assert runner.calls == []


def test_health_accepts_non_weather_dags_in_shared_airflow_inventory(tmp_path: Path):
    target, artifact, _ = _candidate(tmp_path)
    Path(str(target.generated_overlay_file)).write_bytes(artifact.content)
    services = tuple(sorted(target.airflow_code_services))
    config = {
        "name": target.project_name,
        "services": {
            service: {
                "image": "example/airflow:3.2.2",
                "healthcheck": {"test": ["CMD", "true"]},
            }
            for service in services
        }
        | {
            service: {"image": "example/data:1"}
            for service in target.forbidden_data_services
        },
    }
    ps = [
        {"Service": service, "State": "running", "Health": "healthy"}
        for service in services
    ]
    dags = [
        {"dag_id": "traffic_shared_runtime_dag", "is_paused": False},
        *[
            {"dag_id": dag_id, "is_paused": True}
            for dag_id in sorted(target.dag_allowlist)
        ],
    ]
    runner = _QueueRunner(
        [
            _ok(json.dumps(config)),
            _ok(json.dumps(ps)),
            _ok(json.dumps(dags)),
            _ok("[]"),
        ]
    )

    assert HealthCommandAdapter(target, runner).read_health(target, artifact) == "passed"


def test_health_rejects_conflicting_duplicate_weather_dag_rows(tmp_path: Path):
    target, artifact, _ = _candidate(tmp_path)
    Path(str(target.generated_overlay_file)).write_bytes(artifact.content)
    services = tuple(sorted(target.airflow_code_services))
    config = {
        "name": target.project_name,
        "services": {
            service: {
                "image": "example/airflow:3.2.2",
                "healthcheck": {"test": ["CMD", "true"]},
            }
            for service in services
        }
        | {
            service: {"image": "example/data:1"}
            for service in target.forbidden_data_services
        },
    }
    ps = [
        {"Service": service, "State": "running", "Health": "healthy"}
        for service in services
    ]
    dags = [
        {"dag_id": dag_id, "is_paused": True}
        for dag_id in sorted(target.dag_allowlist)
    ]
    dags.append({"dag_id": dags[0]["dag_id"], "is_paused": False})
    runner = _QueueRunner(
        [
            _ok(json.dumps(config)),
            _ok(json.dumps(ps)),
            _ok(json.dumps(dags)),
        ]
    )

    with pytest.raises(HealthAdapterError, match="^health_adapter_invalid_output$"):
        HealthCommandAdapter(target, runner).read_health(target, artifact)


@pytest.mark.parametrize(
    "adapter_name, field, value",
    [
        ("airflow", "control_service", "--help"),
        ("compose", "project_name", "bad;name"),
        ("git", "runtime_root", Path("../escape")),
        ("health", "generated_overlay_file", Path("bad\npath.yml")),
    ],
)
def test_all_adapters_reject_command_or_path_injection_before_runner(
    tmp_path: Path, adapter_name: str, field: str, value: object
):
    target = replace(_native_target(tmp_path), **{field: value})
    runner = _QueueRunner([])

    with pytest.raises((AirflowAdapterError, ComposeAdapterError, GitAdapterError, HealthAdapterError)):
        if adapter_name == "airflow":
            AirflowCommandAdapter(target, runner)
        elif adapter_name == "compose":
            ComposeCommandAdapter(target, runner)
        elif adapter_name == "git":
            GitCommandAdapter(target, tmp_path / "source", runner)
        else:
            HealthCommandAdapter(target, runner)
    assert runner.calls == []


def test_adapter_errors_never_retain_command_output_path_or_credential(tmp_path: Path):
    target = _native_target(tmp_path)
    private = "token=private C:/private/location"
    runner = _QueueRunner([CompletedCommand(stdout=private, stderr=private, returncode=2)])

    with pytest.raises(AirflowAdapterError) as error:
        AirflowCommandAdapter(target, runner).capture_pause_state(tuple(sorted(target.dag_allowlist)))

    rendered = str(error.value)
    assert "private" not in rendered
    assert "C:/" not in rendered
    assert hashlib.sha256(private.encode()).hexdigest() not in rendered
