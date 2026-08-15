from __future__ import annotations

import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from deployment.airflow_cli_compat import AirflowCliCompatibilityError, probe_airflow_cli_contract
import deployment.command as command_module
from deployment.command import CommandExecutionError, CommandRunner, CompletedCommand
from tests.deploy.test_release_inventory import _target


LIST_HELP = """Usage: airflow dags list [-h] [-B BUNDLE_NAME] [--columns COLUMNS] [-l]
                         [-o (table, json, yaml, plain)] [-v]

List all the DAGs

Options:
  -h, --help            show this help message and exit
  -B, --bundle-name BUNDLE_NAME
                        The name of the DAG bundle to use; may be provided more than once
  --columns COLUMNS     List of columns to render. (default: ['dag_id', 'fileloc', 'owner', 'is_paused'])
  -l, --local           Shows local parsed DAGs and their import errors, ignores content serialized in DB
  -o, --output (table, json, yaml, plain)
                        Output format. Allowed values: json, yaml, plain, table (default: table)
  -v, --verbose         Make logging output more verbose
"""
LIST_IMPORT_ERRORS_HELP = """Usage: airflow dags list-import-errors [-h] [-B BUNDLE_NAME] [-l]
                                       [-o (table, json, yaml, plain)] [-v]

List all the DAGs that have import errors

Options:
  -h, --help            show this help message and exit
  -B, --bundle-name BUNDLE_NAME
                        The name of the DAG bundle to use; may be provided more than once
  -l, --local           Shows local parsed DAGs and their import errors, ignores content serialized in DB
  -o, --output (table, json, yaml, plain)
                        Output format. Allowed values: json, yaml, plain, table (default: table)
  -v, --verbose         Make logging output more verbose
"""
LIST_RUNS_HELP = """Usage: airflow dags list-runs [-h] [-e END_DATE] [--no-backfill]
                              [-o (table, json, yaml, plain)] [-s START_DATE]
                              [--state queued, running, success, failed] [-v]
                              dag_id

List DAG runs given a DAG id. If state option is given, it will only search for all the dagruns with the given state. If no_backfill option is given, it will filter out all backfill dagruns for given dag id. If start_date is given, it will filter out all the dagruns that were executed before this date. If end_date is given, it will filter out all the dagruns that were executed after this date.

Positional Arguments:
  dag_id                The id of the dag

Options:
  -h, --help            show this help message and exit
  -e, --end-date END_DATE
                        Override end_date. Accepts multiple datetime formats including: YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, YYYY-MM-DDTHH:MM:SS±HH:MM (ISO 8601), and other formats supported by pendulum.parse()
  --no-backfill         filter all the backfill dagruns given the dag id
  -o, --output (table, json, yaml, plain)
                        Output format. Allowed values: json, yaml, plain, table (default: table)
  -s, --start-date START_DATE
                        Override start_date. Accepts multiple datetime formats including: YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, YYYY-MM-DDTHH:MM:SS±HH:MM (ISO 8601), and other formats supported by pendulum.parse()
  --state queued, running, success, failed
                        Only list the DAG runs corresponding to the state
  -v, --verbose         Make logging output more verbose
"""
MUTATION_HELP = """Usage: airflow dags {command} [-h] [-o (table, json, yaml, plain)]
                          [--treat-dag-id-as-regex] [-v] [-y]
                          dag_id

Change one DAG's paused state.

Positional Arguments:
  dag_id                The id of the dag

Options:
  -h, --help            show this help message and exit
  -o, --output (table, json, yaml, plain)
                        Output format. Allowed values: json, yaml, plain, table (default: table)
  --treat-dag-id-as-regex
                        if set, dag_id will be treated as regex instead of an exact string
  -v, --verbose         Make logging output more verbose
  -y, --yes             Do not prompt to confirm. Use with care!
"""


class _FakeRunner:
    def __init__(self, responses: dict[tuple[str, ...], CompletedCommand]):
        self.responses = responses
        self.calls: list[tuple[tuple[str, ...], Path]] = []

    def run(self, argv, cwd: Path) -> CompletedCommand:
        key = tuple(argv)
        self.calls.append((key, cwd))
        return self.responses[key]


def _command(stdout: str) -> CompletedCommand:
    return CompletedCommand(stdout=stdout, stderr="", returncode=0)


def _responses(target) -> dict[tuple[str, ...], CompletedCommand]:
    prefix = (
        "docker", "compose", "-p", target.project_name, "-f", str(target.compose_files[0]),
        "-f", str(target.generated_overlay_file),
        "exec", "-T", target.control_service, "airflow",
    )
    return {
        (*prefix, "version"): _command("3.2.2\n"),
        (*prefix, "dags", "list", "--help"): _command(LIST_HELP),
        (*prefix, "dags", "list-import-errors", "--help"): _command(LIST_IMPORT_ERRORS_HELP),
        (*prefix, "dags", "list-runs", "--help"): _command(LIST_RUNS_HELP),
        (*prefix, "dags", "pause", "--help"): _command(MUTATION_HELP.format(command="pause")),
        (*prefix, "dags", "unpause", "--help"): _command(MUTATION_HELP.format(command="unpause")),
    }


def test_probe_records_version_and_normalized_capability_fingerprint_only(tmp_path: Path):
    target = _target(tmp_path)
    runner = _FakeRunner(_responses(target))

    contract = probe_airflow_cli_contract(target, runner)

    assert contract.version == "3.2.2"
    assert len(contract.capability_fingerprint) == 64
    assert not hasattr(contract, "help")
    assert [argv[-3:] for argv, _ in runner.calls[1:]] == [
        ("dags", "list", "--help"),
        ("dags", "list-import-errors", "--help"),
        ("dags", "list-runs", "--help"),
        ("dags", "pause", "--help"),
        ("dags", "unpause", "--help"),
    ]
    assert all("pause" not in argv or argv[-1] == "--help" for argv, _ in runner.calls)
    assert all("unpause" not in argv or argv[-1] == "--help" for argv, _ in runner.calls)


def test_probe_stable_mode_binds_exact_generated_overlay_and_base_only_is_explicit(tmp_path: Path):
    target = _target(tmp_path)
    stable_runner = _FakeRunner(_responses(target))

    probe_airflow_cli_contract(
        target,
        stable_runner,
        mode="stable",
        overlay_file=target.generated_overlay_file,
    )

    assert all(str(target.generated_overlay_file) in argv for argv, _ in stable_runner.calls)

    base_responses = {}
    for argv, response in _responses(target).items():
        items = list(argv)
        overlay_index = items.index(str(target.generated_overlay_file))
        del items[overlay_index - 1:overlay_index + 1]
        base_responses[tuple(items)] = response
    base_runner = _FakeRunner(base_responses)
    probe_airflow_cli_contract(target, base_runner, mode="base-only-cutover")
    assert all(str(target.generated_overlay_file) not in argv for argv, _ in base_runner.calls)


@pytest.mark.parametrize(
    "mode, overlay",
    [
        ("stable", Path("C:/other/overlay.yml")),
        ("base-only-cutover", Path("C:/unexpected.yml")),
        ("unknown", None),
    ],
)
def test_probe_rejects_invalid_overlay_mode_before_runner(tmp_path: Path, mode: str, overlay: Path | None):
    target = _target(tmp_path)
    runner = _FakeRunner({})

    with pytest.raises(AirflowCliCompatibilityError, match="^airflow_cli_command_disallowed$"):
        probe_airflow_cli_contract(target, runner, mode=mode, overlay_file=overlay)

    assert runner.calls == []


def test_probe_rejects_unknown_help_capability_without_exposing_help(tmp_path: Path):
    target = _target(tmp_path)
    responses = _responses(target)
    pause_help = next(command for command in responses if command[-3:] == ("dags", "pause", "--help"))
    responses[pause_help] = _command(MUTATION_HELP.format(command="pause") + "  --unsafe-token TEXT\n")

    with pytest.raises(AirflowCliCompatibilityError, match="^airflow_cli_incompatible$") as error:
        probe_airflow_cli_contract(target, _FakeRunner(responses))

    assert "unsafe-token" not in str(error.value)


def test_probe_rejects_help_without_json_output_support(tmp_path: Path):
    target = _target(tmp_path)
    responses = _responses(target)
    list_help = next(command for command in responses if command[-3:] == ("dags", "list", "--help"))
    responses[list_help] = _command(
        LIST_HELP.replace(
            "(table, json, yaml, plain)", "(table, yaml, plain)"
        ).replace(
            "Allowed values: json, yaml, plain, table",
            "Allowed values: yaml, plain, table",
        )
    )

    with pytest.raises(AirflowCliCompatibilityError, match="^airflow_cli_incompatible$"):
        probe_airflow_cli_contract(target, _FakeRunner(responses))


@pytest.mark.parametrize(
    "command_suffix, original, replacement",
    [
        (("dags", "list", "--help"), "-o, --output", "--output"),
        (("dags", "pause", "--help"), "-o, --output", "--output"),
        (("dags", "unpause", "--help"), "-y, --yes", "--yes"),
    ],
)
def test_probe_rejects_long_only_when_the_contract_invokes_short_options(
    tmp_path: Path, command_suffix: tuple[str, ...], original: str, replacement: str
):
    target = _target(tmp_path)
    responses = _responses(target)
    command = next(command for command in responses if command[-3:] == command_suffix)
    responses[command] = _command(responses[command].stdout.replace(original, replacement, 1))

    with pytest.raises(AirflowCliCompatibilityError, match="^airflow_cli_incompatible$"):
        probe_airflow_cli_contract(target, _FakeRunner(responses))


@pytest.mark.parametrize(
    "command_suffix, replacement",
    [
        (("dags", "list-runs", "--help"), LIST_RUNS_HELP.replace("                              dag_id", "                              [dag_id]")),
        (("dags", "pause", "--help"), MUTATION_HELP.format(command="pause").replace("                          dag_id", "                          [dag_id]")),
        (("dags", "unpause", "--help"), MUTATION_HELP.format(command="unpause").replace("                          dag_id", "")),
        (("dags", "list-runs", "--help"), LIST_RUNS_HELP.replace("queued, running, success, failed", "success, failed")),
        (("dags", "pause", "--help"), MUTATION_HELP.format(command="pause").replace("-y, --yes", "-y unavailable")),
        (("dags", "list", "--help"), LIST_HELP.replace("json, yaml, plain, table", "yaml, plain, table")),
    ],
)
def test_probe_rejects_nonsemantic_or_optional_help_contracts(
    tmp_path: Path, command_suffix: tuple[str, ...], replacement: str
):
    target = _target(tmp_path)
    responses = _responses(target)
    command = next(command for command in responses if command[-3:] == command_suffix)
    responses[command] = _command(replacement)

    with pytest.raises(AirflowCliCompatibilityError, match="^airflow_cli_incompatible$") as error:
        probe_airflow_cli_contract(target, _FakeRunner(responses))

    assert error.value.__cause__ is None


def test_probe_requires_exact_airflow_322_version(tmp_path: Path):
    target = _target(tmp_path)
    responses = _responses(target)
    version = next(command for command in responses if command[-1] == "version")
    responses[version] = _command("3.2.3\n")

    with pytest.raises(AirflowCliCompatibilityError, match="^airflow_cli_incompatible$"):
        probe_airflow_cli_contract(target, _FakeRunner(responses))


def test_probe_rejects_semantic_output_choice_drift(tmp_path: Path):
    target = _target(tmp_path)
    responses = _responses(target)
    list_help = next(command for command in responses if command[-3:] == ("dags", "list", "--help"))
    responses[list_help] = _command(
        LIST_HELP.replace("table, json, yaml, plain", "json, yaml")
    )

    with pytest.raises(AirflowCliCompatibilityError, match="^airflow_cli_incompatible$"):
        probe_airflow_cli_contract(target, _FakeRunner(responses))


@pytest.mark.parametrize("stderr, returncode, expected", [("token=hidden", 0, "airflow_cli_sensitive_stderr"), ("plain failure", 1, "airflow_cli_command_failed")])
def test_probe_redacts_command_failures(tmp_path: Path, stderr: str, returncode: int, expected: str):
    target = _target(tmp_path)
    responses = _responses(target)
    version = next(command for command in responses if command[-1] == "version")
    responses[version] = CompletedCommand(stdout="", stderr=stderr, returncode=returncode)

    with pytest.raises(AirflowCliCompatibilityError, match=f"^{expected}$") as error:
        probe_airflow_cli_contract(target, _FakeRunner(responses))

    assert error.value.__cause__ is None
    assert "hidden" not in str(error.value)


@pytest.mark.parametrize(
    "field, value",
    [
        ("project_name", "--project"),
        ("control_service", "--help"),
        ("working_directory", Path("bad\nworking-directory")),
    ],
)
def test_probe_rejects_option_shaped_target_values_before_runner(tmp_path: Path, field: str, value: object):
    target = replace(_target(tmp_path), **{field: value})
    runner = _FakeRunner({})

    with pytest.raises(AirflowCliCompatibilityError, match="^airflow_cli_command_disallowed$"):
        probe_airflow_cli_contract(target, runner)

    assert runner.calls == []


def test_subprocess_runner_sets_bounded_timeout_and_redacts_timeout_output(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    def _timeout(*args, **kwargs):
        assert kwargs["shell"] is False
        assert kwargs["timeout"] > 0
        raise subprocess.TimeoutExpired(args[0], kwargs["timeout"], output="token=private")

    monkeypatch.setattr(command_module.subprocess, "run", _timeout)

    with pytest.raises(CommandExecutionError, match="^command_timeout$") as error:
        CommandRunner(timeout_seconds=300).run(["docker", "compose"], tmp_path)

    assert error.value.__cause__ is None
    assert "private" not in str(error.value)


@pytest.mark.parametrize("timeout", [True, False, 0, 3601, 1.0, "30"])
def test_subprocess_runner_rejects_invalid_timeout_at_construction(timeout):
    with pytest.raises(ValueError, match="^command_timeout_invalid$"):
        CommandRunner(timeout_seconds=timeout)
