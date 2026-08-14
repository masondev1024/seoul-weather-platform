"""Weather dbt phase orchestration."""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Mapping
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from . import environment
from .commands import phase_commands, resource_type, selected_unique_ids
from .contracts import (
    DEFAULT_DBT_OL_BIN,
    MATERIALIZATION_COMMANDS,
    DbtAttemptPaths,
    DbtExecution,
    command_name,
    dbt_bin,
    dbt_project_dir,
)
from .paths import (
    attempt_paths,
    existing,
    prune_pipeline_runs,
    reset_execution_directories,
    retention_runs,
)


def _packages_yml_exists(project_dir: str) -> bool:
    return Path(project_dir, "packages.yml").is_file()


def _package_sentinel(packages_path: str) -> Path:
    return Path(packages_path) / "asac_axes" / "dbt_project.yml"


def _self_heal_deps_command(
    *,
    executable: str,
    target: str,
    log_path: str,
) -> list[str]:
    return [
        executable,
        "deps",
        "--target",
        target,
        "--no-use-colors",
        "--log-path",
        log_path,
    ]


def _prepare_attempt_directories(paths: DbtAttemptPaths) -> None:
    """Fail before dbt's silent exit 2 when bind-mounted artifacts are unwritable."""

    for label, path_value in (
        ("preflight-target", paths.preflight_target_path),
        ("preflight-log", paths.preflight_log_path),
        ("packages", paths.packages_path),
    ):
        directory = Path(path_value)
        probe_path: Path | None = None
        try:
            directory.mkdir(parents=True, exist_ok=True)
            with NamedTemporaryFile(
                dir=directory,
                prefix=".weather-dbt-write-probe-",
                delete=False,
            ) as probe:
                probe.write(b"probe")
                probe_path = Path(probe.name)
            probe_path.unlink()
        except OSError as exc:
            if probe_path is not None:
                try:
                    probe_path.unlink(missing_ok=True)
                except OSError:
                    pass
            raise RuntimeError(
                f"weather dbt {label} directory is not writable: {directory}"
            ) from exc


def execute_dbt_phase(
    *,
    dbt_command: str,
    selector: str | None,
    invocation_id: str,
    pipeline: str,
    run_id: str | None,
    task_id: str | None,
    try_number: int | None,
    target: str,
    variables: str | None,
    threads: int | None = None,
    fresh_parse: bool = False,
    project_dir: str | None = None,
    executable: str | None = None,
    runner: Callable[..., Any] = subprocess.run,
    environ: Mapping[str, str] | None = None,
) -> DbtExecution:
    """Run raw preflight and one isolated dbt phase without stale artifacts."""
    resolved_project = project_dir or dbt_project_dir()
    resolved_executable = executable or dbt_bin()
    phase = command_name(dbt_command)
    paths = attempt_paths(
        project_dir=resolved_project,
        pipeline=pipeline,
        run_id=run_id,
        task_id=task_id,
        try_number=try_number,
        invocation_id=invocation_id,
        dbt_command=dbt_command,
    )
    raw_env = environment.raw_environment(
        project_dir=resolved_project,
        packages_path=paths.packages_path,
        environ=environ,
    )
    _prepare_attempt_directories(paths)
    retained_runs = retention_runs(raw_env) if phase == "deps" else None
    attempts: list[Any] = []
    selected_ids: tuple[str, ...] = ()
    actual_attempted = False

    if phase != "deps" and _packages_yml_exists(resolved_project):
        sentinel = _package_sentinel(paths.packages_path)
        if not sentinel.is_file():
            deps_command = _self_heal_deps_command(
                executable=resolved_executable,
                target=target,
                log_path=paths.preflight_log_path,
            )
            completed = runner(
                deps_command,
                cwd=resolved_project,
                env=raw_env,
                check=False,
                capture_output=True,
                text=True,
            )
            attempts.append(completed)
            if completed.returncode != 0:
                return DbtExecution(
                    attempts=tuple(attempts),
                    paths=paths,
                    selected_unique_ids=selected_ids,
                    actual_attempted=False,
                    existing_run_results_path=None,
                    existing_sources_path=None,
                    existing_manifest_path=None,
                    missing_expected_artifacts=(),
                )
            if not sentinel.is_file():
                attempts.append(
                    subprocess.CompletedProcess(
                        args=deps_command,
                        returncode=2,
                        stdout="",
                        stderr=(
                            "dbt package sentinel missing after self-heal deps: "
                            f"{sentinel}"
                        ),
                    )
                )
                return DbtExecution(
                    attempts=tuple(attempts),
                    paths=paths,
                    selected_unique_ids=selected_ids,
                    actual_attempted=False,
                    existing_run_results_path=None,
                    existing_sources_path=None,
                    existing_manifest_path=None,
                    missing_expected_artifacts=(),
                )

    for stage, command in phase_commands(
        executable=resolved_executable,
        dbt_command=dbt_command,
        selector=selector,
        threads=threads,
        target=target,
        paths=paths,
        variables=variables,
        fresh_parse=fresh_parse,
    ):
        command_env = raw_env
        command_to_run = list(command)
        if stage == "command":
            if (
                environment.openlineage_enabled(raw_env)
                and phase in MATERIALIZATION_COMMANDS
            ):
                command_env = environment.openlineage_environment(
                    raw_env, pipeline=pipeline, task_id=task_id
                )
                command_to_run[0] = DEFAULT_DBT_OL_BIN
            reset_execution_directories(paths)
            actual_attempted = True
        completed = runner(
            command_to_run,
            cwd=resolved_project,
            env=command_env,
            check=False,
            capture_output=True,
            text=True,
        )
        attempts.append(completed)
        if completed.returncode != 0:
            break
        if stage == "ls":
            selected_ids = selected_unique_ids(completed.stdout or "")
            if not selected_ids:
                attempts[-1] = subprocess.CompletedProcess(
                    args=command_to_run,
                    returncode=2,
                    stdout=completed.stdout or "",
                    stderr=(
                        f"dbt selection resolved to no {resource_type(dbt_command)} nodes: "
                        f"{selector}"
                    ),
                )
                break

    if phase == "deps" and actual_attempted and attempts[-1].returncode == 0:
        assert retained_runs is not None
        prune_pipeline_runs(
            project_dir=resolved_project,
            pipeline=pipeline,
            run_id=run_id,
            retention_runs=retained_runs,
        )

    existing_run_results = existing(
        paths.run_results_path, actual_attempted=actual_attempted
    )
    existing_sources = existing(paths.sources_path, actual_attempted=actual_attempted)
    existing_manifest = existing(paths.manifest_path, actual_attempted=actual_attempted)
    observed = {existing_run_results, existing_sources, existing_manifest}
    missing = tuple(
        path
        for path in (paths.run_results_path, paths.sources_path, paths.manifest_path)
        if actual_attempted and path is not None and path not in observed
    )
    return DbtExecution(
        attempts=tuple(attempts),
        paths=paths,
        selected_unique_ids=selected_ids,
        actual_attempted=actual_attempted,
        existing_run_results_path=existing_run_results,
        existing_sources_path=existing_sources,
        existing_manifest_path=existing_manifest,
        missing_expected_artifacts=missing,
    )
