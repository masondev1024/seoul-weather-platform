import os
import sys
import types
from pathlib import Path

import pytest

from weather_dbt_execution_test_support import (
    RAW_DBT,
    completed,
    load_execution_module,
    write_artifacts,
)


def test_weather_deps_phase_does_not_self_heal_recursively(tmp_path):
    module = load_execution_module()
    (tmp_path / "packages.yml").write_text(
        "packages:\n  - package: asac_axes\n", encoding="utf-8"
    )
    observed = []

    module.execute_dbt_phase(
        dbt_command="deps",
        selector=None,
        invocation_id="deps-no-recursion",
        pipeline="weather-transform",
        run_id="manual__1",
        task_id="dbt_deps",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=lambda command, **_kwargs: observed.append(command)
        or completed(command),
        environ={},
    )

    assert [command[1] for command in observed] == ["deps"]


def test_weather_non_deps_phase_keeps_current_behavior_without_packages_yml(tmp_path):
    module = load_execution_module()
    observed = []

    def runner(command, **_kwargs):
        observed.append(command)
        if command[1] == "ls":
            return completed(
                command,
                stdout='{"unique_id":"model.asac.silver","resource_type":"model"}\n',
            )
        write_artifacts(command)
        return completed(command)

    module.execute_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_weather_transform_silver",
        invocation_id="no-packages-yml",
        pipeline="weather-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert [command[1] for command in observed] == ["ls", "run"]


def test_weather_non_deps_phase_does_not_self_heal_when_package_sentinel_exists(
    tmp_path,
):
    module = load_execution_module()
    paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="weather-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        invocation_id="sentinel-present",
        dbt_command="run",
    )
    (tmp_path / "packages.yml").write_text(
        "packages:\n  - package: asac_axes\n", encoding="utf-8"
    )
    sentinel = Path(paths.packages_path) / "asac_axes" / "dbt_project.yml"
    sentinel.parent.mkdir(parents=True)
    sentinel.write_text("name: asac_axes\n", encoding="utf-8")
    observed = []

    def runner(command, **_kwargs):
        observed.append(command)
        if command[1] == "ls":
            return completed(
                command,
                stdout='{"unique_id":"model.asac.silver","resource_type":"model"}\n',
            )
        write_artifacts(command)
        return completed(command)

    module.execute_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_weather_transform_silver",
        invocation_id="sentinel-present",
        pipeline="weather-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert [command[1] for command in observed] == ["ls", "run"]


def test_weather_non_deps_phase_self_heals_missing_packages_before_ls(tmp_path):
    module = load_execution_module()
    (tmp_path / "packages.yml").write_text(
        "packages:\n  - package: asac_axes\n", encoding="utf-8"
    )
    observed = []

    def runner(command, **kwargs):
        observed.append((command, kwargs))
        assert Path(command[command.index("--log-path") + 1]).is_dir()
        assert Path(kwargs["env"]["DBT_PACKAGES_INSTALL_PATH"]).is_dir()
        if command[1] == "deps":
            packages_path = Path(kwargs["env"]["DBT_PACKAGES_INSTALL_PATH"])
            sentinel = packages_path / "asac_axes" / "dbt_project.yml"
            sentinel.parent.mkdir(parents=True)
            sentinel.write_text("name: asac_axes\n", encoding="utf-8")
            return completed(command)
        if command[1] == "ls":
            return completed(
                command,
                stdout='{"unique_id":"model.asac.silver","resource_type":"model"}\n',
            )
        write_artifacts(command)
        return completed(command)

    execution = module.execute_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_weather_transform_silver",
        invocation_id="self-heal",
        pipeline="weather-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert [command[0][1] for command in observed] == ["deps", "ls", "run"]
    deps_command, deps_kwargs = observed[0]
    assert "--target-path" not in deps_command
    assert deps_kwargs["env"]["DBT_PACKAGES_INSTALL_PATH"] == execution.paths.packages_path


def test_weather_preflight_creates_target_and_log_directories_before_dbt_ls(
    tmp_path,
):
    module = load_execution_module()
    observed = []

    def runner(command, **_kwargs):
        observed.append(command)
        assert Path(command[command.index("--log-path") + 1]).is_dir()
        if command[1] == "ls":
            assert Path(command[command.index("--target-path") + 1]).is_dir()
            return completed(
                command,
                stdout='{"unique_id":"model.asac.silver","resource_type":"model"}\n',
            )
        write_artifacts(command)
        return completed(command)

    module.execute_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_weather_transform_silver",
        invocation_id="prepared-preflight-paths",
        pipeline="weather-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert [command[1] for command in observed] == ["ls", "run"]
    assert not list(tmp_path.rglob(".weather-dbt-write-probe-*"))


def test_weather_preflight_reports_unwritable_artifact_directory_before_dbt(
    tmp_path, monkeypatch
):
    module = load_execution_module()

    class DeniedPath:
        def __init__(self, _value):
            pass

        def mkdir(self, *, parents, exist_ok):
            assert parents is True
            assert exist_ok is True
            raise PermissionError("simulated bind-mount ownership mismatch")

    monkeypatch.setitem(module.execute_dbt_phase.__globals__, "Path", DeniedPath)

    with pytest.raises(
        RuntimeError,
        match="weather dbt preflight-target directory is not writable",
    ):
        module.execute_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_weather_transform_silver",
            invocation_id="unwritable-preflight",
            pipeline="weather-transform",
            run_id="manual__1",
            task_id="dbt_run_silver",
            try_number=1,
            target="dev",
            variables=None,
            project_dir=str(tmp_path),
            executable=RAW_DBT,
            runner=lambda *_args, **_kwargs: pytest.fail("dbt must not start"),
            environ={},
        )


def test_weather_preflight_probes_existing_artifact_directory_before_dbt(
    tmp_path, monkeypatch
):
    module = load_execution_module()
    paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="weather-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        invocation_id="existing-unwritable-preflight",
        dbt_command="run",
    )
    Path(paths.preflight_target_path).mkdir(parents=True)

    def denied_probe(*_args, **kwargs):
        assert Path(kwargs["dir"]) == Path(paths.preflight_target_path)
        raise PermissionError("simulated existing bind-mount ownership mismatch")

    monkeypatch.setitem(
        module.execute_dbt_phase.__globals__, "NamedTemporaryFile", denied_probe
    )

    with pytest.raises(
        RuntimeError,
        match="weather dbt preflight-target directory is not writable",
    ):
        module.execute_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_weather_transform_silver",
            invocation_id="existing-unwritable-preflight",
            pipeline="weather-transform",
            run_id="manual__1",
            task_id="dbt_run_silver",
            try_number=1,
            target="dev",
            variables=None,
            project_dir=str(tmp_path),
            executable=RAW_DBT,
            runner=lambda *_args, **_kwargs: pytest.fail("dbt must not start"),
            environ={},
        )


def test_weather_non_deps_phase_stops_when_self_heal_deps_fails(tmp_path):
    module = load_execution_module()
    (tmp_path / "packages.yml").write_text(
        "packages:\n  - package: asac_axes\n", encoding="utf-8"
    )
    observed = []

    def runner(command, **_kwargs):
        observed.append(command)
        return completed(command, returncode=1, stderr="deps failed")

    execution = module.execute_dbt_phase(
        dbt_command="test",
        selector="ask_seoul_weather_transform_gold",
        invocation_id="self-heal-fails",
        pipeline="weather-transform",
        run_id="manual__1",
        task_id="dbt_test_gold",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert [command[1] for command in observed] == ["deps"]
    assert execution.completed.returncode == 1
    assert execution.actual_attempted is False


def test_weather_non_deps_phase_fails_closed_when_sentinel_still_missing_after_deps(
    tmp_path,
):
    module = load_execution_module()
    (tmp_path / "packages.yml").write_text(
        "packages:\n  - package: asac_axes\n", encoding="utf-8"
    )
    observed = []

    execution = module.execute_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_weather_transform_silver",
        invocation_id="self-heal-missing-sentinel",
        pipeline="weather-transform",
        run_id="manual__1",
        task_id="dbt_run_silver",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=lambda command, **_kwargs: observed.append(command)
        or completed(command),
        environ={},
    )

    assert [command[1] for command in observed] == ["deps"]
    assert execution.completed.returncode == 2
    assert "package sentinel missing" in execution.completed.stderr
    assert execution.actual_attempted is False


def test_execution_loader_restores_existing_synthetic_module():
    name = "weather_dbt_execution_under_test"
    sentinel = types.ModuleType("preexisting_weather_execution")
    missing = object()
    previous = sys.modules.get(name, missing)
    sys.modules[name] = sentinel
    try:
        loaded = load_execution_module()

        assert loaded is not sentinel
        assert sys.modules[name] is sentinel
    finally:
        if previous is missing:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous


def test_weather_preflight_failure_ignores_stale_execution_artifacts(tmp_path):
    module = load_execution_module()
    paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="weather-transform",
        run_id="scheduled__1",
        task_id="dbt_test_gold",
        try_number=2,
        invocation_id="gold-contract-tests",
        dbt_command="test",
    )
    stale = Path(paths.run_results_path)
    stale.parent.mkdir(parents=True)
    stale.write_text('{"stale":true}', encoding="utf-8")

    execution = module.execute_dbt_phase(
        dbt_command="test",
        selector="ask_seoul_weather_transform_gold",
        invocation_id="gold-contract-tests",
        pipeline="weather-transform",
        run_id="scheduled__1",
        task_id="dbt_test_gold",
        try_number=2,
        target="dev",
        variables=None,
        fresh_parse=True,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=lambda command, **_kwargs: completed(
            command, returncode=1, stderr="Compilation Error"
        ),
        environ={},
    )

    assert execution.actual_attempted is False
    assert execution.existing_run_results_path is None
    assert execution.existing_manifest_path is None
    assert stale.exists()


def test_weather_actual_resets_only_current_execution_directory(tmp_path):
    module = load_execution_module()
    paths = module.attempt_paths(
        project_dir=str(tmp_path),
        pipeline="weather-transform",
        run_id="scheduled__1",
        task_id="dbt_test_gold",
        try_number=2,
        invocation_id="gold-contract-tests",
        dbt_command="test",
    )
    stale = Path(paths.run_results_path)
    stale.parent.mkdir(parents=True)
    stale.write_text('{"stale":true}', encoding="utf-8")
    sibling = stale.parent.parent / "keep" / "sentinel"
    sibling.parent.mkdir()
    sibling.write_text("keep", encoding="utf-8")

    def runner(command, **_kwargs):
        if command[1] == "ls":
            return completed(
                command,
                stdout='{"unique_id":"test.asac_seoul.gold","resource_type":"test"}\n',
            )
        assert not stale.exists()
        write_artifacts(command)
        return completed(command, returncode=1)

    execution = module.execute_dbt_phase(
        dbt_command="test",
        selector="ask_seoul_weather_transform_gold",
        invocation_id="gold-contract-tests",
        pipeline="weather-transform",
        run_id="scheduled__1",
        task_id="dbt_test_gold",
        try_number=2,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert execution.existing_run_results_path == paths.run_results_path
    assert execution.existing_manifest_path == paths.manifest_path
    assert sibling.is_file()


def test_weather_deps_retention_preserves_current_and_other_pipeline(tmp_path):
    module = load_execution_module()
    for root_name in ("target", "logs"):
        pipeline_root = tmp_path / root_name / "weather-transform"
        for index, run_name in enumerate(("run-a", "run-b", "run-c"), start=1):
            run_dir = pipeline_root / run_name
            run_dir.mkdir(parents=True)
            os.utime(run_dir, (index, index))
        other = tmp_path / root_name / "traffic-transform" / "other-run"
        other.mkdir(parents=True)

    module.execute_dbt_phase(
        dbt_command="deps",
        selector=None,
        invocation_id="dependencies",
        pipeline="weather-transform",
        run_id="current-run",
        task_id="dbt_deps",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=lambda command, **_kwargs: completed(command),
        environ={"ASK_SEOUL_DBT_ARTIFACT_RETENTION_RUNS": "2"},
    )

    for root_name in ("target", "logs"):
        assert {
            path.name
            for path in (tmp_path / root_name / "weather-transform").iterdir()
            if path.is_dir()
        } == {"current-run", "run-c"}
        assert (tmp_path / root_name / "traffic-transform" / "other-run").is_dir()


@pytest.mark.parametrize("value", ["0", "bad"])
def test_weather_invalid_retention_fails_fast(value, tmp_path):
    module = load_execution_module()
    calls = []

    with pytest.raises(RuntimeError, match="ASK_SEOUL_DBT_ARTIFACT_RETENTION_RUNS"):
        module.execute_dbt_phase(
            dbt_command="deps",
            selector=None,
            invocation_id="dependencies",
            pipeline="weather-transform",
            run_id="current-run",
            task_id="dbt_deps",
            try_number=1,
            target="dev",
            variables=None,
            project_dir=str(tmp_path),
            executable=RAW_DBT,
            runner=lambda command, **_kwargs: calls.append(command),
            environ={"ASK_SEOUL_DBT_ARTIFACT_RETENTION_RUNS": value},
        )

    assert calls == []
