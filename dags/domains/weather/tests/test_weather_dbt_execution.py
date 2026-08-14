import os

import pytest

from weather_dbt_execution_test_support import (
    DBT_OL,
    MODULE_PATH,
    RAW_DBT,
    completed,
    load_execution_module,
    option,
    write_artifacts,
)


def test_weather_execution_defaults_to_traffic_weather_monoproject(monkeypatch):
    monkeypatch.delenv("ASK_SEOUL_DBT_PROJECT_DIR", raising=False)
    module = load_execution_module()

    assert module.dbt_project_dir() == "/opt/airflow/dbt/domains/traffic_weather"


def test_weather_execution_uses_project_env_override(monkeypatch):
    module = load_execution_module()
    monkeypatch.setenv("ASK_SEOUL_DBT_PROJECT_DIR", "/tmp/root-dbt")

    assert module.dbt_project_dir() == "/tmp/root-dbt"


def test_weather_openlineage_env_exists_only_on_dbt_ol_actual(tmp_path, monkeypatch):
    module = load_execution_module()
    observed = []
    monkeypatch.setattr(
        module._environment, "executable_available", lambda path: path == DBT_OL
    )

    def runner(command, **kwargs):
        observed.append((command, kwargs))
        if command[1] == "ls":
            return completed(
                command,
                stdout='{"unique_id":"test.asac_seoul.weather_gold","resource_type":"test"}\n',
            )
        write_artifacts(command)
        return completed(command)

    execution = module.execute_dbt_phase(
        dbt_command="test",
        selector="ask_seoul_weather_transform_gold",
        invocation_id="gold-contract-tests",
        pipeline="weather-transform",
        run_id="scheduled__1",
        task_id="dbt_test_gold",
        try_number=1,
        target="dev",
        variables='{"revision":"2025-04-01"}',
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={
            "PATH": "/usr/bin",
            "ASK_SEOUL_DBT_OPENLINEAGE_ENABLED": "true",
            "ASK_SEOUL_DBT_OPENLINEAGE_URL": "http://marquez:5000",
            "ASK_SEOUL_DBT_OPENLINEAGE_ENDPOINT": "api/v1/lineage",
            "ASK_SEOUL_DBT_OPENLINEAGE_NAMESPACE": "ask-seoul-dbt",
            "OPENLINEAGE_PARENT_ID": "airflow-parent",
        },
    )

    assert [command[0] for command, _kwargs in observed] == [RAW_DBT, DBT_OL]
    assert option(observed[0][0], "--target-path") == (
        execution.paths.preflight_target_path
    )
    assert option(observed[1][0], "--target-path") == (
        execution.paths.execution_target_path
    )
    assert "OPENLINEAGE_URL" not in observed[0][1]["env"]
    actual_env = observed[1][1]["env"]
    assert actual_env["OPENLINEAGE_URL"] == "http://marquez:5000"
    assert actual_env["OPENLINEAGE_ENDPOINT"] == "api/v1/lineage"
    assert actual_env["OPENLINEAGE_NAMESPACE"] == "ask-seoul-dbt"
    assert actual_env["OPENLINEAGE_DBT_JOB_NAME"] == "weather-transform.dbt_test_gold"
    assert actual_env["OPENLINEAGE_PARENT_ID"] == "airflow-parent"
    assert actual_env["OPENLINEAGE__FACETS__SOURCE_CODE_LOCATION__DISABLED"] == "true"
    assert actual_env["PATH"].split(os.pathsep)[0] == "/home/airflow/dbt-venv/bin"
    assert execution.existing_run_results_path == execution.paths.run_results_path
    assert execution.existing_manifest_path == execution.paths.manifest_path


@pytest.mark.parametrize("dbt_command", ["seed", "run", "test", "build", "snapshot"])
def test_weather_materialization_commands_use_dbt_ol(
    dbt_command, tmp_path, monkeypatch
):
    module = load_execution_module()
    observed = []
    monkeypatch.setattr(module._environment, "executable_available", lambda _path: True)

    def runner(command, **_kwargs):
        observed.append(command)
        if command[1] == "ls":
            resource_type = "snapshot" if dbt_command == "snapshot" else "model"
            return completed(
                command,
                stdout=(
                    f'{{"unique_id":"{resource_type}.asac_seoul.selected",'
                    f'"resource_type":"{resource_type}"}}\n'
                ),
            )
        return completed(command)

    module.execute_dbt_phase(
        dbt_command=dbt_command,
        selector="selected",
        threads=1,
        invocation_id=f"{dbt_command}-selected",
        pipeline="weather-transform",
        run_id="scheduled__1",
        task_id=f"dbt_{dbt_command}",
        try_number=1,
        target="dev",
        variables=None,
        fresh_parse=True,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={
            "ASK_SEOUL_DBT_OPENLINEAGE_ENABLED": "true",
            "ASK_SEOUL_DBT_OPENLINEAGE_URL": "http://marquez:5000",
            "ASK_SEOUL_DBT_OPENLINEAGE_NAMESPACE": "ask-seoul-dbt",
        },
    )

    assert observed[-1][:2] == [DBT_OL, dbt_command]
    assert option(observed[-1], "--selector") == "selected"
    assert option(observed[-1], "--threads") == "1"
    assert option(observed[-2], "--selector") == "selected"
    assert observed[0][1] == "parse"
    assert all("--no-partial-parse" in command for command in observed)
    assert "--threads" not in observed[0]
    assert "--threads" not in observed[-2]
    assert not any(
        argument.startswith("--indirect-selection")
        for command in observed
        for argument in command
    )


@pytest.mark.parametrize("dbt_command", ["deps", "source freshness"])
def test_weather_deps_and_source_freshness_stay_raw(dbt_command, tmp_path, monkeypatch):
    module = load_execution_module()
    observed = []

    def availability_must_not_be_checked(_path):
        raise AssertionError("raw dbt phase must not require dbt-ol")

    monkeypatch.setattr(
        module._environment,
        "executable_available",
        availability_must_not_be_checked,
    )

    def runner(command, **_kwargs):
        observed.append(command)
        if command[1] == "ls":
            return completed(
                command,
                stdout='{"unique_id":"source.asac_seoul.weather","resource_type":"source"}\n',
            )
        if dbt_command == "source freshness":
            write_artifacts(command, sources=True)
        return completed(command)

    execution = module.execute_dbt_phase(
        dbt_command=dbt_command,
        selector=(
            "ask_seoul_weather_transform_source"
            if dbt_command == "source freshness"
            else None
        ),
        threads=1,
        invocation_id="raw-phase",
        pipeline="weather-transform",
        run_id="scheduled__1",
        task_id="dbt_raw",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={"ASK_SEOUL_DBT_OPENLINEAGE_ENABLED": "true"},
    )

    assert all(command[0] == RAW_DBT for command in observed)
    assert all("--threads" not in command for command in observed)
    if dbt_command == "source freshness":
        assert all("--no-partial-parse" in command for command in observed)
        assert execution.existing_sources_path == execution.paths.sources_path
    else:
        assert all("--no-partial-parse" not in command for command in observed)
        assert execution.paths.manifest_path is None


def test_weather_deps_omits_unsupported_target_path_and_keeps_log_path(tmp_path):
    module = load_execution_module()
    observed = {}

    def runner(command, **kwargs):
        observed.update(command=command, kwargs=kwargs)
        return completed(command)

    execution = module.execute_dbt_phase(
        dbt_command="deps",
        selector=None,
        threads=1,
        invocation_id="dependencies",
        pipeline="weather-transform",
        run_id="scheduled__deps",
        task_id="dbt_deps",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    command = observed["command"]
    assert command[:2] == [RAW_DBT, "deps"]
    assert "--target-path" not in command
    assert "--threads" not in command
    assert option(command, "--log-path") == execution.paths.execution_log_path
    assert option(command, "--target") == "dev"
    assert observed["kwargs"]["env"]["DBT_PACKAGES_INSTALL_PATH"] == (
        execution.paths.packages_path
    )


@pytest.mark.parametrize(
    "environment",
    [
        {
            "ASK_SEOUL_DBT_OPENLINEAGE_ENABLED": "true",
            "ASK_SEOUL_DBT_OPENLINEAGE_NAMESPACE": "ask-seoul-dbt",
        },
        {
            "ASK_SEOUL_DBT_OPENLINEAGE_ENABLED": "true",
            "ASK_SEOUL_DBT_OPENLINEAGE_URL": "http://marquez:5000",
        },
    ],
)
def test_weather_openlineage_missing_required_config_fails_before_actual(
    environment, tmp_path, monkeypatch
):
    module = load_execution_module()
    observed = []
    monkeypatch.setattr(module._environment, "executable_available", lambda _path: True)

    def runner(command, **_kwargs):
        observed.append(command)
        return completed(
            command,
            stdout='{"unique_id":"model.asac_seoul.weather","resource_type":"model"}\n',
        )

    with pytest.raises(RuntimeError, match="ASK_SEOUL_DBT_OPENLINEAGE"):
        module.execute_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_weather_transform_silver",
            invocation_id="silver-models",
            pipeline="weather-transform",
            run_id="scheduled__1",
            task_id="dbt_run_silver",
            try_number=1,
            target="dev",
            variables=None,
            project_dir=str(tmp_path),
            executable=RAW_DBT,
            runner=runner,
            environ=environment,
        )

    assert [command[1] for command in observed] == ["ls"]


@pytest.mark.parametrize(
    "selector",
    [
        "tag:ask_seoul_weather_transform_silver",
        "models/weather/silver.sql",
        r"models\weather\silver.sql",
        "weather selector",
        "   ",
    ],
)
def test_weather_rejects_non_named_selectors_before_runner(selector, tmp_path):
    module = load_execution_module()
    calls = []

    with pytest.raises(ValueError, match="named dbt selector"):
        module.execute_dbt_phase(
            dbt_command="run",
            selector=selector,
            invocation_id="invalid-selector",
            pipeline="weather-transform",
            run_id="scheduled__1",
            task_id="dbt_run_silver",
            try_number=1,
            target="dev",
            variables=None,
            project_dir=str(tmp_path),
            executable=RAW_DBT,
            runner=lambda command, **_kwargs: calls.append(command),
            environ={},
        )

    assert calls == []


@pytest.mark.parametrize("threads", [0, -1, True, 1.5, "2"])
def test_weather_rejects_invalid_threads_before_runner(threads, tmp_path):
    module = load_execution_module()
    calls = []

    with pytest.raises(ValueError, match="threads must be a positive integer"):
        module.execute_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_weather_transform_silver",
            threads=threads,
            invocation_id="invalid-threads",
            pipeline="weather-transform",
            run_id="scheduled__1",
            task_id="dbt_run_silver",
            try_number=1,
            target="dev",
            variables=None,
            project_dir=str(tmp_path),
            executable=RAW_DBT,
            runner=lambda command, **_kwargs: calls.append(command),
            environ={},
        )

    assert calls == []


def test_weather_empty_selector_result_skips_actual_command(tmp_path):
    module = load_execution_module()
    observed = []

    def runner(command, **_kwargs):
        observed.append(command)
        return completed(command, stdout="")

    execution = module.execute_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_weather_transform_silver",
        invocation_id="empty-silver-selection",
        pipeline="weather-transform",
        run_id="scheduled__1",
        task_id="dbt_run_silver",
        try_number=1,
        target="dev",
        variables=None,
        project_dir=str(tmp_path),
        executable=RAW_DBT,
        runner=runner,
        environ={},
    )

    assert [command[1] for command in observed] == ["ls"]
    assert execution.actual_attempted is False
    assert execution.completed.returncode == 2
    assert "resolved to no model nodes" in execution.completed.stderr


def test_weather_execution_module_has_no_traffic_dependency():
    source = MODULE_PATH.read_text(encoding="utf-8")

    assert "traffic_dbt_execution" not in source
    assert "traffic_lineage" not in source
