from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath

from deployment.canonical_json import canonical_bytes, sha256_hex
from deployment.command import CommandRunner, CompletedCommand
from deployment.target import DeployTarget


class AirflowCliCompatibilityError(RuntimeError):
    """A redacted category for an incompatible Airflow CLI contract."""


@dataclass(frozen=True)
class AirflowCliContract:
    version: str
    capability_fingerprint: str


_SENSITIVE_STDERR = re.compile(
    r"(?:api[_-]?key|token|secret|password|credential|authorization|bearer)\s*(?:=|:|\s)",
    re.IGNORECASE,
)
_FLAG = re.compile(r"(?<!\w)(--[A-Za-z][A-Za-z0-9-]*|-[A-Za-z])(?!\w)")
_EXPECTED_USAGE = {
    "list": (
        "Usage: airflow dags list [-h] [-B BUNDLE_NAME] [--columns COLUMNS] "
        "[-l] [-o (table, json, yaml, plain)] [-v]"
    ),
    "list-import-errors": (
        "Usage: airflow dags list-import-errors [-h] [-B BUNDLE_NAME] [-l] "
        "[-o (table, json, yaml, plain)] [-v]"
    ),
    "list-runs": (
        "Usage: airflow dags list-runs [-h] [-e END_DATE] [--no-backfill] "
        "[-o (table, json, yaml, plain)] [-s START_DATE] "
        "[--state queued, running, success, failed] [-v] dag_id"
    ),
    "pause": (
        "Usage: airflow dags pause [-h] [-o (table, json, yaml, plain)] "
        "[--treat-dag-id-as-regex] [-v] [-y] dag_id"
    ),
    "unpause": (
        "Usage: airflow dags unpause [-h] [-o (table, json, yaml, plain)] "
        "[--treat-dag-id-as-regex] [-v] [-y] dag_id"
    ),
}
_EXPECTED_DECLARED_FLAGS = {
    "list": frozenset(
        {
            "-h",
            "--help",
            "-B",
            "--bundle-name",
            "--columns",
            "-l",
            "--local",
            "-o",
            "--output",
            "-v",
            "--verbose",
        }
    ),
    "list-import-errors": frozenset(
        {
            "-h",
            "--help",
            "-B",
            "--bundle-name",
            "-l",
            "--local",
            "-o",
            "--output",
            "-v",
            "--verbose",
        }
    ),
    "list-runs": frozenset(
        {
            "-h",
            "--help",
            "-e",
            "--end-date",
            "--no-backfill",
            "-o",
            "--output",
            "-s",
            "--start-date",
            "--state",
            "-v",
            "--verbose",
        }
    ),
    "pause": frozenset(
        {
            "-h",
            "--help",
            "-o",
            "--output",
            "--treat-dag-id-as-regex",
            "-v",
            "--verbose",
            "-y",
            "--yes",
        }
    ),
    "unpause": frozenset(
        {
            "-h",
            "--help",
            "-o",
            "--output",
            "--treat-dag-id-as-regex",
            "-v",
            "--verbose",
            "-y",
            "--yes",
        }
    ),
}


def _target_argv_value(value: object) -> str:
    if (
        type(value) is not str
        or not value
        or value.startswith("-")
        or re.search(r"[|;&`$<>\x00-\x1f\x7f]", value)
    ):
        raise AirflowCliCompatibilityError("airflow_cli_command_disallowed")
    return value


def _path_argv_value(value: PurePath) -> str:
    raw = _target_argv_value(str(value))
    parts = re.split(r"[\\/]", raw)
    if "." in parts or ".." in parts:
        raise AirflowCliCompatibilityError("airflow_cli_command_disallowed")
    if not PureWindowsPath(raw).is_absolute() and not PurePosixPath(raw).is_absolute():
        raise AirflowCliCompatibilityError("airflow_cli_command_disallowed")
    return raw


def _same_path(left: PurePath, right: PurePath) -> bool:
    left_raw = str(left)
    right_raw = str(right)
    if PureWindowsPath(left_raw).is_absolute() or PureWindowsPath(right_raw).is_absolute():
        return PureWindowsPath(left_raw) == PureWindowsPath(right_raw)
    return PurePosixPath(left_raw) == PurePosixPath(right_raw)


def _compose_prefix(target: DeployTarget, overlay_file: PurePath | None) -> list[str]:
    argv = ["docker", "compose", "-p", _target_argv_value(target.project_name)]
    for compose_file in target.compose_files:
        argv.extend(("-f", _path_argv_value(compose_file)))
    if overlay_file is not None:
        argv.extend(("-f", _path_argv_value(overlay_file)))
    return argv


def _validate_target_argv(target: DeployTarget) -> None:
    _target_argv_value(target.project_name)
    _target_argv_value(target.control_service)
    _path_argv_value(target.working_directory)
    for compose_file in target.compose_files:
        _path_argv_value(compose_file)


def _checked(target: DeployTarget, runner: CommandRunner, argv: Sequence[str]) -> str:
    try:
        result: CompletedCommand = runner.run(argv, Path(str(target.working_directory)))
    except Exception:
        raise AirflowCliCompatibilityError("airflow_cli_command_failed") from None
    if _SENSITIVE_STDERR.search(result.stderr):
        raise AirflowCliCompatibilityError("airflow_cli_sensitive_stderr")
    if result.returncode != 0 or result.stderr:
        raise AirflowCliCompatibilityError("airflow_cli_command_failed")
    return result.stdout


def _usage_line(help_text: str) -> str:
    usage_lines: list[str] = []
    for line in help_text.splitlines():
        if not line.strip():
            break
        usage_lines.append(line.strip())
    return " ".join(usage_lines)


def _declared_flags(help_text: str) -> frozenset[str]:
    flags: set[str] = set()
    for line in help_text.splitlines():
        if re.match(r"^\s+-", line):
            flags.update(_FLAG.findall(line))
    return frozenset(flags)


def _capability(help_text: str, command: str, *, requires_dag_id: bool, requires_state: bool, requires_yes: bool) -> dict[str, object]:
    if type(help_text) is not str:
        raise AirflowCliCompatibilityError("airflow_cli_incompatible")
    usage = _usage_line(help_text)
    nonempty_lines = [line.rstrip() for line in help_text.splitlines() if line.strip()]
    if (
        usage != _EXPECTED_USAGE[command]
        or nonempty_lines.count("Options:") != 1
        or _declared_flags(help_text) != _EXPECTED_DECLARED_FLAGS[command]
        or len(
            re.findall(
                r"^\s*-o, --output \(table, json, yaml, plain\)$",
                help_text,
                re.MULTILINE,
            )
        )
        != 1
        or len(
            re.findall(
                r"^\s*Output format\. Allowed values: json, yaml, plain, table "
                r"\(default: table\)$",
                help_text,
                re.MULTILINE,
            )
        )
        != 1
    ):
        raise AirflowCliCompatibilityError("airflow_cli_incompatible")
    if requires_dag_id:
        if (
            nonempty_lines.count("Positional Arguments:") != 1
            or len(
                re.findall(
                    r"^\s*dag_id\s+The id of the dag$", help_text, re.MULTILINE
                )
            )
            != 1
        ):
            raise AirflowCliCompatibilityError("airflow_cli_incompatible")
    elif "Positional Arguments:" in nonempty_lines:
        raise AirflowCliCompatibilityError("airflow_cli_incompatible")
    state_choices: tuple[str, ...] = ()
    if requires_state:
        if len(
            re.findall(
                r"^\s*--state queued, running, success, failed$",
                help_text,
                re.MULTILINE,
            )
        ) != 1:
            raise AirflowCliCompatibilityError("airflow_cli_incompatible")
        state_choices = ("failed", "queued", "running", "success")
    yes_flags: tuple[str, ...] = ()
    if requires_yes:
        if len(re.findall(r"^\s*-y,\s*--yes(?:\s+.*)?$", help_text, re.MULTILINE)) != 1:
            raise AirflowCliCompatibilityError("airflow_cli_incompatible")
        yes_flags = ("-y", "--yes")
    return {
        "usage": usage,
        "requires_dag_id": requires_dag_id,
        "output_choices": ("json", "plain", "table", "yaml"),
        "state_choices": state_choices,
        "noninteractive_flags": yes_flags,
        "declared_flags": tuple(sorted(_EXPECTED_DECLARED_FLAGS[command])),
    }


def probe_airflow_cli_contract(
    target: DeployTarget,
    runner: CommandRunner,
    *,
    mode: str = "stable",
    overlay_file: PurePath | None = None,
) -> AirflowCliContract:
    """Validate the fixed read-only/mutation-help contract without mutating Airflow."""
    _validate_target_argv(target)
    if mode == "stable":
        selected_overlay = target.generated_overlay_file if overlay_file is None else overlay_file
        if not _same_path(selected_overlay, target.generated_overlay_file):
            raise AirflowCliCompatibilityError("airflow_cli_command_disallowed")
        _path_argv_value(selected_overlay)
    elif mode == "base-only-cutover" and overlay_file is None:
        selected_overlay = None
    else:
        raise AirflowCliCompatibilityError("airflow_cli_command_disallowed")
    prefix = [
        *_compose_prefix(target, selected_overlay),
        "exec",
        "-T",
        _target_argv_value(target.control_service),
        "airflow",
    ]
    version = _checked(target, runner, [*prefix, "version"]).strip()
    if version != "3.2.2":
        raise AirflowCliCompatibilityError("airflow_cli_incompatible")
    help_commands = {
        "list": ([*prefix, "dags", "list", "--help"], False, False, False),
        "list-import-errors": (
            [*prefix, "dags", "list-import-errors", "--help"],
            False,
            False,
            False,
        ),
        "list-runs": ([*prefix, "dags", "list-runs", "--help"], True, True, False),
        "pause": ([*prefix, "dags", "pause", "--help"], True, False, True),
        "unpause": ([*prefix, "dags", "unpause", "--help"], True, False, True),
    }
    capabilities = {
        command: _capability(
            _checked(target, runner, argv),
            command,
            requires_dag_id=requires_dag_id,
            requires_state=requires_state,
            requires_yes=requires_yes,
        )
        for command, (argv, requires_dag_id, requires_state, requires_yes) in help_commands.items()
    }
    return AirflowCliContract(
        version=version,
        capability_fingerprint=sha256_hex(canonical_bytes({"version": version, "commands": capabilities})),
    )
