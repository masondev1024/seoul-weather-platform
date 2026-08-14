import json
import types
from pathlib import Path

import pytest

from weather_transform_test_support import (
    FakeAirflowException,
    FakeAirflowFailException,
    FakeTaskInstance,
    load_transform_module,
)
from weather_transform_test_support import (
    restore_airflow_modules_after_transform_import,  # noqa: F401
)


def test_weather_transform_has_no_caller_owned_artifact_path_helper():
    module = load_transform_module()

    assert not hasattr(module, "_artifact_path")
    assert not hasattr(module, "RUN_RESULTS_PATH")


def test_weather_dbt_deps_uses_isolated_runtime_paths_without_project_vars(
    tmp_path, monkeypatch, capsys
):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "weather"))
    captured = {}

    def fake_run(command, **kwargs):
        captured.update(command=command, kwargs=kwargs)
        return types.SimpleNamespace(
            returncode=0, stdout="deps stdout\n", stderr="deps stderr\n"
        )

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    ti = FakeTaskInstance(task_id="dbt_deps", try_number=1)

    assert module.run_dbt_phase(
        dbt_command="deps",
        selector=None,
        include_project_vars=False,
        ti=ti,
        run_id="manual__1",
        params={"target": "dev"},
    ) == {
        "status": "success",
        "run_results_path": None,
        "sources_path": None,
        "manifest_path": None,
        "selected_unique_ids": [],
    }

    command = captured["command"]
    assert command[:2] == [module.DBT_BIN, "deps"]
    assert "--vars" not in command
    assert "--target-path" not in command
    assert "--log-path" in command
    assert captured["kwargs"]["cwd"] == module.DBT_PROJECT
    assert captured["kwargs"]["check"] is False
    assert captured["kwargs"]["capture_output"] is True
    assert captured["kwargs"]["text"] is True
    assert captured["kwargs"]["env"]["DBT_PROFILES_DIR"] == module.DBT_PROJECT
    assert captured["kwargs"]["env"]["DBT_PROJECT_DIR"] == module.DBT_PROJECT
    assert captured["kwargs"]["env"]["DBT_PACKAGES_INSTALL_PATH"].endswith(
        "target/weather-transform/manual__1/dbt_packages"
    )
    assert ti.pushes == [(module.WEATHER_DBT_RUN_RESULTS_XCOM_KEY, None)]
    streams = capsys.readouterr()
    assert "deps stdout" in streams.out
    assert "deps stderr" in streams.err


def test_weather_dbt_model_command_writes_isolated_artifact(tmp_path, monkeypatch):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "weather"))
    captured = []

    def fake_run(command, **kwargs):
        captured.append((command, kwargs))
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"unique_id":"model.asac_seoul.weather_silver","resource_type":"model"}\n',
                stderr="",
            )
        target_path = Path(command[command.index("--target-path") + 1])
        target_path.mkdir(parents=True, exist_ok=True)
        (target_path / "run_results.json").write_text("{}", encoding="utf-8")
        (target_path / "manifest.json").write_text("{}", encoding="utf-8")
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    ti = FakeTaskInstance(task_id="dbt_run_silver", try_number=2)

    result = module.run_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_weather_transform_silver",
        threads=2,
        ti=ti,
        run_id="scheduled/2026:07",
        params={"target": "dev"},
    )

    paths = module.weather_dbt.attempt_paths(
        project_dir=module.DBT_PROJECT,
        pipeline="weather-transform",
        run_id="scheduled/2026:07",
        task_id="dbt_run_silver",
        try_number=2,
        invocation_id="dbt_run_silver",
        dbt_command="run",
    )
    ls_command, command = [item[0] for item in captured]
    assert ls_command[:2] == [module.DBT_BIN, "ls"]
    assert ls_command[ls_command.index("--resource-type") + 1] == "model"
    assert command[:2] == [module.DBT_BIN, "run"]
    assert command[command.index("--selector") + 1] == (
        "ask_seoul_weather_transform_silver"
    )
    assert ls_command[ls_command.index("--selector") + 1] == (
        "ask_seoul_weather_transform_silver"
    )
    assert "--no-partial-parse" in ls_command
    assert "--no-partial-parse" in command
    assert "--indirect-selection=buildable" not in ls_command
    assert "--indirect-selection=buildable" not in command
    assert command[command.index("--target") + 1] == "dev"
    assert command[command.index("--threads") + 1] == "2"
    assert "--threads" not in ls_command
    assert command[command.index("--vars") + 1] == json.dumps(
        module.WEATHER_DBT_CONTRACT_VARS,
        separators=(",", ":"),
    )
    assert Path(command[command.index("--target-path") + 1]) == Path(
        paths.execution_target_path
    )
    assert Path(ls_command[ls_command.index("--target-path") + 1]) == Path(
        paths.preflight_target_path
    )
    assert Path(command[command.index("--log-path") + 1]) == Path(
        paths.execution_log_path
    )
    assert "--no-use-colors" in command
    assert Path(paths.run_results_path).exists()
    assert result == {
        "status": "success",
        "run_results_path": paths.run_results_path,
        "sources_path": None,
        "manifest_path": paths.manifest_path,
        "selected_unique_ids": ["model.asac_seoul.weather_silver"],
    }
    assert ti.pushes == [
        (module.WEATHER_DBT_RUN_RESULTS_XCOM_KEY, paths.run_results_path)
    ]


def test_weather_dbt_model_command_uses_the_frozen_serving_as_of_hour(tmp_path, monkeypatch):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "weather"))
    captured = []

    def fake_run(command, **kwargs):
        captured.append((command, kwargs))
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"unique_id":"model.asac_seoul.weather_silver","resource_type":"model"}\n',
                stderr="",
            )
        target_path = Path(command[command.index("--target-path") + 1])
        target_path.mkdir(parents=True, exist_ok=True)
        (target_path / "run_results.json").write_text("{}", encoding="utf-8")
        (target_path / "manifest.json").write_text("{}", encoding="utf-8")
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    ti = FakeTaskInstance(
        task_id="dbt_run_silver",
        try_number=1,
        pulls={(module.SERVING_AS_OF_HOUR_TASK_ID, None): "2026-08-11 10:00:00"},
    )

    module.run_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_weather_transform_silver",
        serving_as_of_task_id=module.SERVING_AS_OF_HOUR_TASK_ID,
        threads=2,
        ti=ti,
        run_id="scheduled__1",
        params={"target": "dev"},
    )

    command = captured[-1][0]
    assert json.loads(command[command.index("--vars") + 1]) == {
        **module.WEATHER_DBT_CONTRACT_VARS,
        "weather_serving_as_of_hour": "2026-08-11 10:00:00",
    }


def test_weather_dbt_attempt_overwrites_artifact_xcom_when_process_cannot_start(
    tmp_path, monkeypatch
):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "weather"))
    ti = FakeTaskInstance(task_id="dbt_run_silver", try_number=3)
    paths = module.weather_dbt.attempt_paths(
        project_dir=module.DBT_PROJECT,
        pipeline="weather-transform",
        run_id="manual__spawn_failure",
        task_id=ti.task_id,
        try_number=ti.try_number,
        invocation_id=ti.task_id,
        dbt_command="run",
    )
    stale_artifact = Path(paths.run_results_path)
    stale_artifact.parent.mkdir(parents=True)
    stale_artifact.write_text('{"metadata": "stale"}', encoding="utf-8")

    def fail_to_start(*_args, **_kwargs):
        raise OSError("dbt process could not start")

    monkeypatch.setattr(module.subprocess, "run", fail_to_start)

    with pytest.raises(OSError, match="could not start"):
        module.run_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_weather_transform_silver",
            ti=ti,
            run_id="manual__spawn_failure",
            params={"target": "dev"},
        )

    assert ti.pushes == [(module.WEATHER_DBT_RUN_RESULTS_XCOM_KEY, None)]
    assert stale_artifact.exists()


def test_weather_failed_dbt_command_pushes_existing_artifact_before_raising(
    tmp_path, monkeypatch
):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "weather"))
    ti = FakeTaskInstance(task_id="dbt_test_silver", try_number=4)
    paths = module.weather_dbt.attempt_paths(
        project_dir=module.DBT_PROJECT,
        pipeline="weather-transform",
        run_id="manual__failed",
        task_id=ti.task_id,
        try_number=ti.try_number,
        invocation_id=ti.task_id,
        dbt_command="test",
    )
    artifact = Path(paths.run_results_path)

    def fail_with_artifact(command, **_kwargs):
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"unique_id":"test.asac_seoul.weather_silver","resource_type":"test"}\n',
                stderr="",
            )
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text('{"metadata": "current"}', encoding="utf-8")
        Path(paths.manifest_path).write_text("{}", encoding="utf-8")
        return types.SimpleNamespace(
            returncode=1,
            stdout="dbt stdout\n",
            stderr="dbt failed\n",
        )

    monkeypatch.setattr(
        module.subprocess,
        "run",
        fail_with_artifact,
    )

    with pytest.raises(FakeAirflowFailException, match="weather dbt command failed"):
        module.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_weather_transform_silver",
            ti=ti,
            run_id="manual__failed",
            params={"target": "dev"},
        )

    assert ti.pushes == [
        (module.WEATHER_DBT_RUN_RESULTS_XCOM_KEY, paths.run_results_path)
    ]


def test_weather_empty_selection_stops_before_phase_and_raises(monkeypatch):
    module = load_transform_module()
    commands = []
    ti = FakeTaskInstance(task_id="dbt_test_gold", try_number=1)

    def empty_preflight(command, **_kwargs):
        commands.append(command)
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", empty_preflight)

    with pytest.raises(FakeAirflowFailException, match="weather dbt command failed"):
        module.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_weather_transform_gold",
            ti=ti,
            run_id="manual__empty",
            params={"target": "dev"},
        )

    assert [command[1] for command in commands] == ["ls"]
    assert ti.pushes == [(module.WEATHER_DBT_RUN_RESULTS_XCOM_KEY, None)]


def test_weather_dbt_test_contract_failure_is_not_retried(tmp_path, monkeypatch):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "weather"))
    ti = FakeTaskInstance(task_id="dbt_test_silver", try_number=1)

    def fail_contract(command, **_kwargs):
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"unique_id":"test.asac_seoul.contract","resource_type":"test"}\n',
                stderr="",
            )
        target_path = Path(command[command.index("--target-path") + 1])
        target_path.mkdir(parents=True, exist_ok=True)
        (target_path / "manifest.json").write_text("{}", encoding="utf-8")
        (target_path / "run_results.json").write_text(
            json.dumps(
                {
                    "results": [
                        {
                            "unique_id": "test.asac_seoul.weather_contract",
                            "status": "fail",
                            "failures": 2,
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        return types.SimpleNamespace(returncode=1, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", fail_contract)

    with pytest.raises(FakeAirflowFailException, match="data-contract-violation"):
        module.run_dbt_phase(
            dbt_command="test",
            selector="ask_seoul_weather_transform_silver",
            ti=ti,
            run_id="manual__contract",
            params={"target": "dev"},
        )


def test_weather_missing_current_attempt_artifact_is_not_retried(tmp_path, monkeypatch):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "weather"))
    ti = FakeTaskInstance(task_id="dbt_run_silver", try_number=1)

    def omit_artifacts(command, **_kwargs):
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"unique_id":"model.asac_seoul.weather","resource_type":"model"}\n',
                stderr="",
            )
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", omit_artifacts)

    with pytest.raises(FakeAirflowFailException, match="artifact-contract-violation"):
        module.run_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_weather_transform_silver",
            ti=ti,
            run_id="manual__missing-artifact",
            params={"target": "dev"},
        )


def test_weather_transient_trino_failure_remains_retryable(tmp_path, monkeypatch):
    module = load_transform_module()
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "weather"))
    ti = FakeTaskInstance(task_id="dbt_run_silver", try_number=1)

    def fail_transiently(command, **_kwargs):
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"unique_id":"model.asac_seoul.weather","resource_type":"model"}\n',
                stderr="",
            )
        return types.SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="Trino adapter connection refused",
        )

    monkeypatch.setattr(module.subprocess, "run", fail_transiently)

    with pytest.raises(
        FakeAirflowException, match="retryable-infrastructure-error"
    ) as raised:
        module.run_dbt_phase(
            dbt_command="run",
            selector="ask_seoul_weather_transform_silver",
            ti=ti,
            run_id="manual__transient",
            params={"target": "dev"},
        )

    assert not isinstance(raised.value, FakeAirflowFailException)
