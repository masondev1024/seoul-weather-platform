"""dbt preflight and execution command construction for Weather."""

from __future__ import annotations

import json
import re
import shlex

from .contracts import MATERIALIZATION_COMMANDS, DbtAttemptPaths, command_name


NAMED_SELECTOR_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


def _selector_args(selector: str) -> list[str]:
    if not NAMED_SELECTOR_PATTERN.fullmatch(selector):
        raise ValueError(
            "selector must be a named dbt selector containing only letters, "
            "numbers, underscores, or hyphens"
        )
    return ["--selector", selector]


def _threads_args(threads: int | None) -> list[str]:
    if threads is None:
        return []
    if isinstance(threads, bool) or not isinstance(threads, int) or threads <= 0:
        raise ValueError("threads must be a positive integer")
    return ["--threads", str(threads)]


def resource_type(dbt_command: str) -> str:
    return {
        "source freshness": "source",
        "seed": "seed",
        "run": "model",
        "test": "test",
        "build": "model",
        "snapshot": "snapshot",
    }[command_name(dbt_command)]


def _runtime_args(
    *,
    target: str,
    target_path: str,
    log_path: str,
    variables: str | None,
    include_target_path: bool = True,
) -> list[str]:
    args = ["--target", target, "--no-use-colors"]
    if variables is not None:
        args.extend(["--vars", variables])
    if include_target_path:
        args.extend(["--target-path", target_path])
    args.extend(["--log-path", log_path])
    return args


def phase_commands(
    *,
    executable: str,
    dbt_command: str,
    selector: str | None,
    threads: int | None,
    target: str,
    paths: DbtAttemptPaths,
    variables: str | None,
    fresh_parse: bool,
) -> list[tuple[str, list[str]]]:
    phase = command_name(dbt_command)
    threads_args = _threads_args(threads)
    isolated_parse_args = [] if phase == "deps" else ["--no-partial-parse"]
    preflight_args = _runtime_args(
        target=target,
        target_path=paths.preflight_target_path,
        log_path=paths.preflight_log_path,
        variables=variables,
    )
    execution_args = _runtime_args(
        target=target,
        target_path=paths.execution_target_path,
        log_path=paths.execution_log_path,
        variables=variables,
        include_target_path=phase != "deps",
    )
    commands: list[tuple[str, list[str]]] = []
    if fresh_parse:
        commands.append(
            ("parse", [executable, "parse", "--no-partial-parse", *preflight_args])
        )
    if selector is not None:
        selector_args = _selector_args(selector)
        commands.append(
            (
                "ls",
                [
                    executable,
                    "ls",
                    *isolated_parse_args,
                    "--resource-type",
                    resource_type(dbt_command),
                    *selector_args,
                    "--output",
                    "json",
                    "--output-keys",
                    "unique_id",
                    "resource_type",
                    *preflight_args,
                ],
            )
        )
        actual_args = [
            executable,
            *shlex.split(dbt_command),
            *isolated_parse_args,
            *selector_args,
            *(threads_args if phase in MATERIALIZATION_COMMANDS else []),
            *execution_args,
        ]
    else:
        actual_args = [
            executable,
            *shlex.split(dbt_command),
            *isolated_parse_args,
            *(threads_args if phase in MATERIALIZATION_COMMANDS else []),
            *execution_args,
        ]
    commands.append(("command", actual_args))
    return commands


def selected_unique_ids(stdout: str) -> tuple[str, ...]:
    selected: list[str] = []
    for line in stdout.splitlines():
        try:
            node = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(node, dict) and node.get("unique_id"):
            selected.append(str(node["unique_id"]))
    return tuple(dict.fromkeys(selected))
