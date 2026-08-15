from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from pathlib import PurePosixPath

import pytest

from deployment.command import CompletedCommand
from deployment.inventory import (
    InventoryError,
    ReleaseInventory,
    collect_read_only_inventory,
    sanitize_inventory,
)
from deployment.redaction import SensitiveArtifactError
from deployment.target import load_deploy_target
from tools.dagbag_runtime_check import EXPECTED_DAG_IDS


def _target(tmp_path: Path):
    runtime = "C:/ProgramData/example-weather/runtime"
    payload = {
        "schema_version": "weather-local-deploy-target/v1",
        "target_id": "example-local-weather",
        "credential_source_kind": "windows_credential_store",
        "credential_reference": "weather-local-runtime",
        "compose": {
            "project_name": "example-weather",
            "working_directory": runtime,
            "files": [f"{runtime}/docker-compose.yml"],
            "control_service": "example-airflow-api",
            "airflow_code_services": [
                "example-airflow-api",
                "example-airflow-scheduler",
                "example-airflow-dag-processor",
                "example-airflow-triggerer",
            ],
            "forbidden_data_services": ["example-postgres", "example-trino"],
        },
        "mounts": {
            "dags_host_path": f"{runtime}/dags",
            "dags_container_path": "/opt/airflow/dags",
            "dbt_host_path": f"{runtime}/dbt",
            "dbt_container_path": "/opt/airflow/dbt",
            "runtime_root": runtime,
        },
        "airflow": {
            "dag_allowlist": sorted(EXPECTED_DAG_IDS),
            "writer_dag_allowlist": sorted(EXPECTED_DAG_IDS),
            "never_trigger": True,
        },
        "timeouts": {"drain_timeout_seconds": 1800, "poll_interval_seconds": 15},
        "local_state": {
            "ledger_directory": "C:/ProgramData/example-weather/ledger",
            "lock_file": "C:/ProgramData/example-weather/deploy.lock",
            "generated_overlay_file": "C:/ProgramData/example-weather/generated/main-deploy.override.yml",
        },
    }
    path = tmp_path / "deploy-target.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return load_deploy_target(path, repo_root=tmp_path)


class _FakeRunner:
    def __init__(self, responses: dict[tuple[str, ...], CompletedCommand]):
        self.responses = responses
        self.calls: list[tuple[tuple[str, ...], Path]] = []

    def run(self, argv, cwd: Path) -> CompletedCommand:
        key = tuple(argv)
        self.calls.append((key, cwd))
        return self.responses[key]


class _LedgerReader:
    def read_summary(self) -> dict[str, object]:
        return {"baseline": "release-2026.08.14.1", "previous_success": "release-1"}


def _command(stdout: str, *, stderr: str = "", returncode: int = 0) -> CompletedCommand:
    return CompletedCommand(stdout=stdout, stderr=stderr, returncode=returncode)


def _responses(target) -> dict[tuple[str, ...], CompletedCommand]:
    prefix = ("docker", "compose", "-p", target.project_name)
    for compose_file in map(str, target.compose_files):
        prefix += ("-f", compose_file)
    services = sorted(target.airflow_code_services | target.forbidden_data_services)
    result = {
        (*prefix, "config", "--services"): _command("\n".join(services)),
        (*prefix, "ps", "--format", "json"): _command(
            json.dumps(
                [
                    {"Service": service, "State": "running"}
                    for service in services
                ]
            )
        ),
        (*prefix, "exec", "-T", target.control_service, "airflow", "dags", "list", "-o", "json"): _command(
            json.dumps(
                [
                    {"dag_id": dag_id, "is_paused": False}
                    for dag_id in sorted(target.dag_allowlist)
                ]
            )
        ),
    }
    for dag_id in sorted(target.dag_allowlist):
        for state in ("running", "queued"):
            result[
                (
                    *prefix,
                    "exec",
                    "-T",
                    target.control_service,
                    "airflow",
                    "dags",
                    "list-runs",
                    "--state",
                    state,
                    "-o",
                    "json",
                    dag_id,
                )
            ] = _command("[]")
    return result


def test_inventory_only_executes_the_validated_read_only_argv_family(tmp_path: Path):
    target = _target(tmp_path)
    runner = _FakeRunner(_responses(target))

    inventory = collect_read_only_inventory(target, runner, _LedgerReader())

    prefix = ["docker", "compose", "-p", "example-weather", "-f", str(target.compose_files[0])]
    expected = [
        [*prefix, "config", "--services"],
        [*prefix, "ps", "--format", "json"],
        [*prefix, "exec", "-T", "example-airflow-api", "airflow", "dags", "list", "-o", "json"],
    ]
    for dag_id in sorted(EXPECTED_DAG_IDS):
        expected.extend(
            [
                [*prefix, "exec", "-T", "example-airflow-api", "airflow", "dags", "list-runs", "--state", "running", "-o", "json", dag_id],
                [*prefix, "exec", "-T", "example-airflow-api", "airflow", "dags", "list-runs", "--state", "queued", "-o", "json", dag_id],
            ]
        )
    assert [list(argv) for argv, _ in runner.calls] == expected
    forbidden = {"up", "build", "restart", "pause", "unpause", "trigger", "backfill", "clear", "dbt", "trino", "wrangler", "|", ";", "&&"}
    assert not (forbidden & {part for argv, _ in runner.calls for part in argv})
    assert all(cwd == Path(str(target.working_directory)) for _, cwd in runner.calls)
    assert inventory.service_states["example-airflow-api"] == "running"


def test_sanitized_inventory_exposes_only_logical_names_counts_and_fingerprints(tmp_path: Path):
    target = _target(tmp_path)
    inventory = collect_read_only_inventory(target, _FakeRunner(_responses(target)), _LedgerReader())

    published = sanitize_inventory(inventory)

    rendered = json.dumps(published, sort_keys=True)
    assert published["services"] == sorted(target.airflow_code_services)
    assert published["counts"] == {"dags": len(EXPECTED_DAG_IDS), "paused_dags": 0, "queued_runs": 0, "running_runs": 0}
    assert set(published) == {"services", "counts", "inventory_fingerprint", "ledger_fingerprint"}
    assert len(published["inventory_fingerprint"]) == 64
    assert len(published["ledger_fingerprint"]) == 64
    assert "release-2026" not in rendered
    assert "example-postgres" not in rendered


def test_inventory_allows_declared_forbidden_orphan_absent_from_compose_config(
    tmp_path: Path,
):
    base = _target(tmp_path)
    orphan = "declared-orphan-data-service"
    target = replace(
        base,
        forbidden_data_services=base.forbidden_data_services | {orphan},
    )
    responses = _responses(target)
    config_command = next(
        command for command in responses if command[-2:] == ("config", "--services")
    )
    configured = sorted(base.airflow_code_services | base.forbidden_data_services)
    responses[config_command] = _command("\n".join(configured))
    ps_command = next(
        command for command in responses if command[-3:] == ("ps", "--format", "json")
    )
    responses[ps_command] = _command(
        json.dumps(
            [{"Service": service, "State": "running"} for service in configured]
        )
    )

    inventory = collect_read_only_inventory(
        target, _FakeRunner(responses), _LedgerReader()
    )

    assert set(inventory.service_states) == set(target.airflow_code_services)


def test_inventory_rejects_unknown_compose_config_service(tmp_path: Path):
    target = _target(tmp_path)
    responses = _responses(target)
    config_command = next(
        command for command in responses if command[-2:] == ("config", "--services")
    )
    responses[config_command] = _command(
        responses[config_command].stdout + "\nunknown-service"
    )

    with pytest.raises(InventoryError, match="^inventory_invalid_output$"):
        collect_read_only_inventory(target, _FakeRunner(responses), _LedgerReader())


def test_inventory_allows_one_shot_airflow_init_absent_from_compose_ps(
    tmp_path: Path,
):
    target = _target(tmp_path)
    responses = _responses(target)
    config_command = next(
        command for command in responses if command[-2:] == ("config", "--services")
    )
    responses[config_command] = _command(
        responses[config_command].stdout + "\nairflow-init"
    )

    inventory = collect_read_only_inventory(
        target, _FakeRunner(responses), _LedgerReader()
    )

    assert set(inventory.service_states) == set(target.airflow_code_services)


def test_inventory_filters_unlisted_dags_from_shared_airflow(tmp_path: Path):
    target = _target(tmp_path)
    responses = _responses(target)
    list_command = next(command for command in responses if command[-3:] == ("list", "-o", "json"))
    responses[list_command] = _command(
        json.dumps(
            [
                {"dag_id": "unlisted-secret-dag", "is_paused": False},
                *[
                    {"dag_id": dag_id, "is_paused": False}
                    for dag_id in sorted(target.dag_allowlist)
                ],
            ]
        )
    )

    inventory = collect_read_only_inventory(
        target, _FakeRunner(responses), _LedgerReader()
    )

    assert set(inventory.dag_paused) == set(target.dag_allowlist)
    assert "unlisted-secret-dag" not in inventory.dag_paused


def test_inventory_fails_closed_when_stderr_might_contain_a_credential(tmp_path: Path):
    target = _target(tmp_path)
    responses = _responses(target)
    config = next(command for command in responses if command[-2:] == ("config", "--services"))
    responses[config] = _command("", stderr="token=not-for-publication")

    with pytest.raises(InventoryError, match="^inventory_sensitive_stderr$") as error:
        collect_read_only_inventory(target, _FakeRunner(responses), _LedgerReader())

    assert "not-for-publication" not in str(error.value)


@pytest.mark.parametrize(
    "response, category",
    [
        (_command("", returncode=2), "inventory_command_failed"),
        (_command("", stderr="ordinary diagnostic"), "inventory_command_failed"),
    ],
)
def test_inventory_rejects_nonzero_or_any_stderr_without_retaining_raw_text(
    tmp_path: Path, response: CompletedCommand, category: str
):
    target = _target(tmp_path)
    responses = _responses(target)
    config = next(command for command in responses if command[-2:] == ("config", "--services"))
    responses[config] = response

    with pytest.raises(InventoryError, match=f"^{category}$") as error:
        collect_read_only_inventory(target, _FakeRunner(responses), _LedgerReader())

    assert error.value.__cause__ is None
    assert "diagnostic" not in str(error.value)


def test_inventory_rejects_non_json_output_without_retaining_decoder_details(tmp_path: Path):
    target = _target(tmp_path)
    responses = _responses(target)
    ps_command = next(command for command in responses if command[-3:] == ("ps", "--format", "json"))
    responses[ps_command] = _command("json is unavailable: token=private")

    with pytest.raises(InventoryError, match="^inventory_invalid_output$") as error:
        collect_read_only_inventory(target, _FakeRunner(responses), _LedgerReader())

    assert error.value.__cause__ is None
    assert "private" not in str(error.value)


@pytest.mark.parametrize(
    "section, replacement",
    [
        ("services", [{"Service": "example-airflow-api", "State": "running"}] * 2),
        ("services", [{"Service": "example-airflow-api"}]),
        ("dags", [{"dag_id": sorted(EXPECTED_DAG_IDS)[0], "is_paused": False}] * 2),
        ("dags", [{"dag_id": sorted(EXPECTED_DAG_IDS)[0]}]),
    ],
)
def test_inventory_rejects_malformed_or_duplicate_service_and_dag_rows(
    tmp_path: Path, section: str, replacement: list[dict[str, object]]
):
    target = _target(tmp_path)
    responses = _responses(target)
    if section == "services":
        command = next(command for command in responses if command[-3:] == ("ps", "--format", "json"))
    else:
        command = next(command for command in responses if command[-3:] == ("list", "-o", "json"))
    responses[command] = _command(json.dumps(replacement))

    with pytest.raises(InventoryError, match="^inventory_invalid_output$") as error:
        collect_read_only_inventory(target, _FakeRunner(responses), _LedgerReader())

    assert error.value.__cause__ is None


@pytest.mark.parametrize(
    "row",
    [
        {},
        {"dag_id": "foreign-dag", "run_id": "run-1", "state": "running"},
        {"dag_id": "weather_vilage_fcst_bronze", "run_id": "run-1", "state": "queued"},
        {"dag_id": "weather_vilage_fcst_bronze", "run_id": "", "state": "running"},
    ],
)
def test_inventory_rejects_malformed_foreign_or_wrong_state_run_rows(
    tmp_path: Path, row: dict[str, object]
):
    target = _target(tmp_path)
    dag_id = "weather_vilage_fcst_bronze"
    responses = _responses(target)
    command = next(command for command in responses if command[-1] == dag_id and "running" in command)
    responses[command] = _command(json.dumps([row]))

    with pytest.raises(InventoryError, match="^inventory_invalid_output$"):
        collect_read_only_inventory(target, _FakeRunner(responses), _LedgerReader())


def test_inventory_counts_supported_airflow_322_run_rows_and_rejects_duplicates(tmp_path: Path):
    target = _target(tmp_path)
    dag_id = "weather_vilage_fcst_bronze"
    responses = _responses(target)
    command = next(command for command in responses if command[-1] == dag_id and "running" in command)
    row = {"dag_id": dag_id, "run_id": "scheduled__2026-08-14T00:00:00+00:00", "state": "running"}
    responses[command] = _command(json.dumps([row]))

    assert collect_read_only_inventory(target, _FakeRunner(responses), _LedgerReader()).run_counts["running"] == 1

    responses[command] = _command(json.dumps([row, row]))
    with pytest.raises(InventoryError, match="^inventory_invalid_output$"):
        collect_read_only_inventory(target, _FakeRunner(responses), _LedgerReader())


def test_inventory_accepts_compose_ndjson_and_airflow_322_output(tmp_path: Path):
    target = _target(tmp_path)
    responses = _responses(target)
    ps_command = next(
        command for command in responses if command[-3:] == ("ps", "--format", "json")
    )
    services = sorted(target.airflow_code_services | target.forbidden_data_services)
    responses[ps_command] = _command(
        "\n".join(
            json.dumps({"Service": service, "State": "running"})
            for service in services
        )
        + "\n"
    )
    list_command = next(
        command for command in responses if command[-3:] == ("list", "-o", "json")
    )
    plugins = ("schemas", "tables", "types", "constraints", "defaults", "comments")
    preamble = "\n".join(
        "2026-08-14T17:43:56.611303Z [info     ] "
        f"setup plugin alembic.autogenerate.{plugin} "
        "[alembic.runtime.plugins] loc=plugins.py:37"
        for plugin in plugins
    )
    rows: list[dict[str, object]] = [
        {"dag_id": "traffic_shared_runtime_dag", "is_paused": "false"}
    ]
    for dag_id in sorted(target.dag_allowlist):
        row = {"dag_id": dag_id, "is_paused": "False"}
        rows.extend((row, dict(row)))
    responses[list_command] = _command(f"{preamble}\n{json.dumps(rows)}\n")

    inventory = collect_read_only_inventory(
        target, _FakeRunner(responses), _LedgerReader()
    )

    assert set(inventory.dag_paused) == set(target.dag_allowlist)
    assert not any(inventory.dag_paused.values())


def test_inventory_preserves_target_compose_file_override_order(tmp_path: Path):
    target = replace(
        _target(tmp_path),
        compose_files=(
            PurePosixPath("/runtime/z-compose.yml"),
            PurePosixPath("/runtime/a-compose.yml"),
        ),
    )

    inventory = collect_read_only_inventory(target, _FakeRunner(_responses(target)), _LedgerReader())

    assert inventory.run_counts == {"running": 0, "queued": 0}


def test_inventory_counts_only_writer_allowlist_when_it_is_a_subset(tmp_path: Path):
    base = _target(tmp_path)
    writer = sorted(base.dag_allowlist)[0]
    non_writer = sorted(base.dag_allowlist)[1]
    target = replace(base, writer_dag_allowlist=frozenset({writer}))
    responses = _responses(target)
    non_writer_command = next(
        command
        for command in responses
        if command[-1] == non_writer and "running" in command
    )
    responses[non_writer_command] = _command(
        json.dumps(
            [{"dag_id": non_writer, "run_id": "unexpected-run", "state": "running"}]
        )
    )
    runner = _FakeRunner(responses)

    inventory = collect_read_only_inventory(target, runner, _LedgerReader())

    assert inventory.run_counts == {"running": 0, "queued": 0}
    queried_dags = {
        argv[-1]
        for argv, _ in runner.calls
        if "list-runs" in argv
    }
    assert queried_dags == {writer}


@pytest.mark.parametrize(
    "field, value",
    [
        ("project_name", "--project-name"),
        ("control_service", "--help"),
        ("airflow_code_services", frozenset({"--service"})),
        ("compose_files", (PurePosixPath("--compose-file"),)),
    ],
)
def test_inventory_rejects_option_shaped_target_argv_values_before_runner(
    tmp_path: Path, field: str, value: object
):
    target = replace(_target(tmp_path), **{field: value})
    runner = _FakeRunner({})

    with pytest.raises(InventoryError, match="^inventory_command_disallowed$"):
        collect_read_only_inventory(target, runner, _LedgerReader())

    assert runner.calls == []


def test_sanitized_inventory_rejects_a_path_shaped_logical_service_name():
    inventory = ReleaseInventory(
        service_states={"C:/local-only/service": "running"},
        dag_paused={"weather_vilage_fcst_bronze": False},
        run_counts={"running": 0, "queued": 0},
        ledger_fingerprint="a" * 64,
    )

    with pytest.raises(SensitiveArtifactError):
        sanitize_inventory(inventory)
