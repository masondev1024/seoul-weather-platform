import importlib
from pathlib import Path
import subprocess

import pytest

from weather_dbt_execution_test_support import load_execution_module


def _paths_module():
    load_execution_module()
    return importlib.import_module("weather_ingest._dbt_execution.paths")


def _directory_symlink(link: Path, target: Path) -> None:
    link.parent.mkdir(parents=True, exist_ok=True)
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError as symlink_error:  # pragma: no cover - Windows fallback
        completed = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(target)],
            capture_output=True,
            check=False,
            text=True,
        )
        if completed.returncode != 0:
            pytest.skip(
                "directory links are unavailable: "
                f"symlink={symlink_error}; junction={completed.stderr}"
            )


def test_weather_attempt_paths_match_command_artifact_ownership(tmp_path):
    module = load_execution_module()
    run_paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="weather-transform",
        run_id="scheduled__1",
        task_id="dbt_run_silver",
        try_number=1,
        invocation_id="run-silver",
        dbt_command="run",
    )
    source_paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="weather-transform",
        run_id="scheduled__1",
        task_id="dbt_source_freshness",
        try_number=1,
        invocation_id="source-freshness",
        dbt_command="source freshness",
    )
    deps_paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="weather-transform",
        run_id="scheduled__1",
        task_id="dbt_deps",
        try_number=1,
        invocation_id="dependencies",
        dbt_command="deps",
    )

    assert run_paths.preflight_target_path.endswith("try1/run-silver/preflight")
    assert run_paths.execution_target_path.endswith("try1/run-silver/execution")
    assert run_paths.preflight_target_path != run_paths.execution_target_path
    assert run_paths.preflight_log_path != run_paths.execution_log_path
    assert run_paths.packages_path.endswith(
        "target/weather-transform/scheduled__1/dbt_packages"
    )
    assert run_paths.run_results_path.endswith("execution/run_results.json")
    assert run_paths.sources_path is None
    assert run_paths.manifest_path.endswith("execution/manifest.json")
    assert source_paths.run_results_path is None
    assert source_paths.sources_path.endswith("execution/sources.json")
    assert source_paths.manifest_path.endswith("execution/manifest.json")
    assert deps_paths.run_results_path is None
    assert deps_paths.sources_path is None
    assert deps_paths.manifest_path is None


def test_weather_attempt_paths_separate_invocations_within_one_task_attempt(tmp_path):
    module = load_execution_module()
    common = {
        "project_dir": str(tmp_path),
        "pipeline": "weather-recovery",
        "run_id": "manual__1",
        "task_id": "execute_recovery",
        "try_number": 1,
        "dbt_command": "run",
    }

    first = module.attempt_paths(invocation_id="window-0001", **common)
    second = module.attempt_paths(invocation_id="window-0002", **common)

    assert first.preflight_target_path != second.preflight_target_path
    assert first.execution_target_path != second.execution_target_path
    assert first.preflight_log_path != second.preflight_log_path
    assert first.execution_log_path != second.execution_log_path
    assert "/window-0001/" in first.execution_target_path.replace("\\", "/")
    assert "/window-0002/" in second.execution_target_path.replace("\\", "/")


def test_weather_attempt_paths_are_collision_resistant_and_contained(tmp_path):
    module = load_execution_module()
    common = {
        "project_dir": str(tmp_path),
        "pipeline": "weather/transform",
        "task_id": "dbt/run",
        "try_number": 1,
        "invocation_id": "silver/models",
        "dbt_command": "run",
    }

    slash = module.attempt_paths(run_id="manual/run", **common)
    dash = module.attempt_paths(run_id="manual-run", **common)

    assert slash.execution_target_path != dash.execution_target_path
    target_root = (tmp_path / "target").resolve()
    log_root = (tmp_path / "logs").resolve()
    for value in (
        slash.preflight_target_path,
        slash.execution_target_path,
        slash.packages_path,
        slash.run_results_path,
        slash.manifest_path,
    ):
        assert Path(value).resolve().is_relative_to(target_root)
    for value in (slash.preflight_log_path, slash.execution_log_path):
        assert Path(value).resolve().is_relative_to(log_root)


@pytest.mark.parametrize("reserved", [".", ".."])
def test_weather_attempt_paths_reject_reserved_segments(tmp_path, reserved):
    module = load_execution_module()

    with pytest.raises(ValueError, match="reserved dbt path segment"):
        module.attempt_paths(
            project_dir=str(tmp_path),
            pipeline="weather-transform",
            run_id=reserved,
            task_id="dbt_run",
            try_number=1,
            invocation_id="silver-models",
            dbt_command="run",
        )


def test_weather_attempt_paths_bound_long_unicode_segments(tmp_path):
    module = load_execution_module()
    prefix = "날씨-" + ("x" * 300)

    first = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="weather-transform",
        run_id=prefix + "-first",
        task_id="dbt_run",
        try_number=1,
        invocation_id="silver-models",
        dbt_command="run",
    )
    second = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="weather-transform",
        run_id=prefix + "-second",
        task_id="dbt_run",
        try_number=1,
        invocation_id="silver-models",
        dbt_command="run",
    )

    assert first.execution_target_path != second.execution_target_path
    relative_parts = Path(first.execution_target_path).relative_to(tmp_path).parts
    assert max(map(len, relative_parts)) <= 96


def test_weather_cleanup_rejects_candidate_outside_trusted_artifact_root(tmp_path):
    module = _paths_module()
    trusted_root = tmp_path / "target"
    allowed_parent = tmp_path / "outside"
    candidate = allowed_parent / "victim"
    candidate.mkdir(parents=True)

    with pytest.raises(RuntimeError, match="trusted artifact root"):
        module._safe_remove_tree(
            candidate,
            allowed_parent=allowed_parent,
            trusted_root=trusted_root,
        )

    assert candidate.is_dir()


def test_weather_cleanup_rejects_dotdot_lexical_disguise(tmp_path):
    module = _paths_module()
    trusted_root = tmp_path / "target"
    allowed_parent = trusted_root / "weather-transform"
    candidate = allowed_parent / "victim"
    candidate.mkdir(parents=True)
    disguised = allowed_parent / "nested" / ".." / "victim"

    with pytest.raises(RuntimeError, match="lexical"):
        module._safe_remove_tree(
            disguised,
            allowed_parent=allowed_parent,
            trusted_root=trusted_root,
        )

    assert candidate.is_dir()


def test_weather_reset_rejects_ancestor_symlink_escape(tmp_path):
    execution = load_execution_module()
    module = _paths_module()
    paths = execution.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="weather-transform",
        run_id="scheduled__1",
        task_id="dbt_run_silver",
        try_number=1,
        invocation_id="silver-models",
        dbt_command="run",
    )
    outside_pipeline = tmp_path / "outside-target"
    escaped_execution = (
        outside_pipeline
        / "scheduled__1"
        / "dbt_run_silver"
        / "try1"
        / "silver-models"
        / "execution"
    )
    escaped_execution.mkdir(parents=True)
    sentinel = escaped_execution / "keep.txt"
    sentinel.write_text("keep", encoding="utf-8")
    _directory_symlink(tmp_path / "target" / "weather-transform", outside_pipeline)

    with pytest.raises(RuntimeError, match="symlink|resolved outside"):
        module.reset_execution_directories(paths)

    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_weather_retention_rejects_pipeline_ancestor_symlink_escape(tmp_path):
    module = _paths_module()
    outside_pipeline = tmp_path / "outside-retention"
    sentinels = []
    for run_name in ("run-a", "run-b"):
        sentinel = outside_pipeline / run_name / "keep.txt"
        sentinel.parent.mkdir(parents=True)
        sentinel.write_text("keep", encoding="utf-8")
        sentinels.append(sentinel)
    _directory_symlink(tmp_path / "target" / "weather-transform", outside_pipeline)

    with pytest.raises(RuntimeError, match="symlink|resolved outside"):
        module.prune_pipeline_runs(
            project_dir=str(tmp_path),
            pipeline="weather-transform",
            run_id="current-run",
            retention_runs=1,
        )

    assert all(path.read_text(encoding="utf-8") == "keep" for path in sentinels)


def test_weather_reset_rejects_trusted_artifact_root_symlink(tmp_path):
    execution = load_execution_module()
    module = _paths_module()
    paths = execution.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="weather-transform",
        run_id="scheduled__root",
        task_id="dbt_run_silver",
        try_number=1,
        invocation_id="silver-models",
        dbt_command="run",
    )
    outside_target = tmp_path / "outside-target-root"
    escaped_execution = (
        outside_target
        / "weather-transform"
        / "scheduled__root"
        / "dbt_run_silver"
        / "try1"
        / "silver-models"
        / "execution"
    )
    escaped_execution.mkdir(parents=True)
    sentinel = escaped_execution / "keep.txt"
    sentinel.write_text("keep", encoding="utf-8")
    _directory_symlink(tmp_path / "target", outside_target)

    with pytest.raises(RuntimeError, match="symlink|reparse"):
        module.reset_execution_directories(paths)

    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_weather_retention_rejects_trusted_artifact_root_symlink(tmp_path):
    module = _paths_module()
    outside_target = tmp_path / "outside-retention-root"
    sentinels = []
    for run_name in ("run-a", "run-b"):
        sentinel = outside_target / "weather-transform" / run_name / "keep.txt"
        sentinel.parent.mkdir(parents=True)
        sentinel.write_text("keep", encoding="utf-8")
        sentinels.append(sentinel)
    _directory_symlink(tmp_path / "target", outside_target)

    with pytest.raises(RuntimeError, match="symlink|reparse"):
        module.prune_pipeline_runs(
            project_dir=str(tmp_path),
            pipeline="weather-transform",
            run_id="current-run",
            retention_runs=1,
        )

    assert all(path.read_text(encoding="utf-8") == "keep" for path in sentinels)
