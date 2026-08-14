"""Attempt-local artifact paths, cleanup, and retention for Weather dbt."""

from __future__ import annotations

import os
import shutil
import stat
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

from .contracts import (
    ARTIFACT_RETENTION_ENV,
    DEFAULT_ARTIFACT_RETENTION_RUNS,
    MATERIALIZATION_COMMANDS,
    DbtAttemptPaths,
    command_name,
    safe_path_segment,
)


def attempt_paths(
    *,
    project_dir: str,
    pipeline: str,
    run_id: str | None,
    task_id: str | None,
    try_number: int | None,
    invocation_id: str,
    dbt_command: str,
) -> DbtAttemptPaths:
    if not isinstance(invocation_id, str) or not invocation_id.strip():
        raise ValueError("invocation_id must be a non-empty string")
    root = PurePosixPath(project_dir.replace("\\", "/"))
    safe_pipeline = safe_path_segment(pipeline)
    safe_run_id = safe_path_segment(run_id)
    attempt_suffix = (
        safe_pipeline,
        safe_run_id,
        safe_path_segment(task_id),
        f"try{try_number if try_number is not None else 'unknown'}",
        safe_path_segment(invocation_id),
    )
    target_attempt = root / "target"
    log_attempt = root / "logs"
    for segment in attempt_suffix:
        target_attempt /= segment
        log_attempt /= segment
    preflight_target = target_attempt / "preflight"
    preflight_log = log_attempt / "preflight"
    execution_target = target_attempt / "execution"
    execution_log = log_attempt / "execution"
    if root / "target" not in execution_target.parents:
        raise RuntimeError("dbt target artifact path escaped the project target root")
    if root / "logs" not in execution_log.parents:
        raise RuntimeError("dbt log artifact path escaped the project log root")
    phase = command_name(dbt_command)
    has_run_results = phase in MATERIALIZATION_COMMANDS
    has_sources = phase == "source freshness"
    has_manifest = has_run_results or has_sources
    return DbtAttemptPaths(
        preflight_target_path=str(preflight_target),
        preflight_log_path=str(preflight_log),
        execution_target_path=str(execution_target),
        execution_log_path=str(execution_log),
        packages_path=str(
            root / "target" / safe_pipeline / safe_run_id / "dbt_packages"
        ),
        run_results_path=(
            str(execution_target / "run_results.json") if has_run_results else None
        ),
        sources_path=(str(execution_target / "sources.json") if has_sources else None),
        manifest_path=(
            str(execution_target / "manifest.json") if has_manifest else None
        ),
    )


def _absolute_lexical(path: Path) -> Path:
    if ".." in path.parts:
        raise RuntimeError(f"unsafe dbt lexical cleanup path: {path}")
    return Path(os.path.abspath(path))


def _is_symlink_or_reparse(path: Path) -> bool:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return False
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    file_attributes = getattr(path_stat, "st_file_attributes", 0)
    return stat.S_ISLNK(path_stat.st_mode) or bool(file_attributes & reparse_flag)


def _trusted_descendant(candidate: Path, *, trusted_root: Path) -> tuple[Path, Path]:
    lexical_candidate = _absolute_lexical(candidate)
    lexical_root = _absolute_lexical(trusted_root)
    if lexical_candidate == lexical_root or not lexical_candidate.is_relative_to(
        lexical_root
    ):
        raise RuntimeError(
            f"dbt cleanup target escaped the trusted artifact root: {candidate}"
        )
    if _is_symlink_or_reparse(lexical_root):
        raise RuntimeError(
            "refusing dbt cleanup through symlink or reparse-point artifact root: "
            f"{lexical_root}"
        )
    current = lexical_root
    for segment in lexical_candidate.relative_to(lexical_root).parts:
        current /= segment
        if _is_symlink_or_reparse(current):
            raise RuntimeError(
                f"refusing dbt cleanup through symlink or reparse-point ancestor: {current}"
            )
    resolved_root = lexical_root.resolve(strict=False)
    resolved_candidate = lexical_candidate.resolve(strict=False)
    if resolved_candidate == resolved_root or not resolved_candidate.is_relative_to(
        resolved_root
    ):
        raise RuntimeError(
            f"dbt cleanup target resolved outside the trusted artifact root: {candidate}"
        )
    return lexical_candidate, resolved_candidate


def _safe_remove_tree(
    candidate: Path, *, allowed_parent: Path, trusted_root: Path
) -> None:
    lexical_candidate, resolved_candidate = _trusted_descendant(
        candidate, trusted_root=trusted_root
    )
    lexical_parent = _absolute_lexical(allowed_parent)
    if lexical_candidate.parent != lexical_parent:
        raise RuntimeError(f"unsafe dbt lexical cleanup parent: {candidate}")
    resolved_parent = lexical_parent.resolve(strict=False)
    if resolved_candidate.parent != resolved_parent:
        raise RuntimeError(f"unsafe dbt resolved cleanup parent: {candidate}")
    if lexical_candidate.exists():
        shutil.rmtree(lexical_candidate)


def _execution_artifact_root(candidate: Path, *, expected_name: str) -> Path:
    try:
        artifact_root = candidate.parents[5]
    except IndexError as exc:
        raise RuntimeError(f"unsafe dbt execution directory: {candidate}") from exc
    if artifact_root.name != expected_name:
        raise RuntimeError(f"unsafe dbt execution directory: {candidate}")
    return artifact_root


def reset_execution_directories(paths: DbtAttemptPaths) -> None:
    for path_value, root_name in (
        (paths.execution_target_path, "target"),
        (paths.execution_log_path, "logs"),
    ):
        candidate = Path(path_value)
        if candidate.name != "execution":
            raise RuntimeError(f"unsafe dbt execution directory: {candidate}")
        _safe_remove_tree(
            candidate,
            allowed_parent=candidate.parent,
            trusted_root=_execution_artifact_root(candidate, expected_name=root_name),
        )
        candidate.mkdir(parents=True, exist_ok=True)


def retention_runs(env: Mapping[str, str]) -> int:
    if ARTIFACT_RETENTION_ENV not in env:
        return DEFAULT_ARTIFACT_RETENTION_RUNS
    raw = str(env[ARTIFACT_RETENTION_ENV]).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(
            f"{ARTIFACT_RETENTION_ENV} must be an integer greater than or equal to 1"
        ) from exc
    if value < 1:
        raise RuntimeError(
            f"{ARTIFACT_RETENTION_ENV} must be an integer greater than or equal to 1"
        )
    return value


def _prune_run_root(
    pipeline_root: Path,
    *,
    current_run: str,
    retention_runs: int,
    trusted_root: Path,
) -> None:
    _trusted_descendant(pipeline_root, trusted_root=trusted_root)
    if not pipeline_root.exists():
        return
    children = list(pipeline_root.iterdir())
    for path in children:
        _trusted_descendant(path, trusted_root=trusted_root)
    run_dirs = [path for path in children if path.is_dir()]
    if len(run_dirs) <= retention_runs:
        return
    current_path = pipeline_root / current_run
    keep: set[Path] = set()
    if current_path in run_dirs:
        keep.add(current_path)
    newest_first = sorted(
        (path for path in run_dirs if path != current_path),
        key=lambda path: (path.stat().st_mtime_ns, path.name),
        reverse=True,
    )
    keep.update(newest_first[: max(0, retention_runs - len(keep))])
    for candidate in run_dirs:
        if candidate not in keep:
            _safe_remove_tree(
                candidate,
                allowed_parent=pipeline_root,
                trusted_root=trusted_root,
            )


def prune_pipeline_runs(
    *, project_dir: str, pipeline: str, run_id: str | None, retention_runs: int
) -> None:
    safe_pipeline = safe_path_segment(pipeline)
    safe_run = safe_path_segment(run_id)
    project_root = Path(project_dir)
    for root_name in ("target", "logs"):
        artifact_root = project_root / root_name
        _prune_run_root(
            artifact_root / safe_pipeline,
            current_run=safe_run,
            retention_runs=retention_runs,
            trusted_root=artifact_root,
        )


def existing(path: str | None, *, actual_attempted: bool) -> str | None:
    if not actual_attempted or path is None:
        return None
    return path if Path(path).is_file() else None
