from pathlib import Path

import pytest

from weather_transform_test_support import (
    EXPECTED_DBT_PHASES,
    EXPECTED_REFERENCE_DBT_PHASES,
    FakePythonOperator,
    load_transform_module,
    restore_airflow_modules_after_transform_import,  # noqa: F401
)


def _module():
    return load_transform_module(
        "weather_reference_data_refresh.py",
        module_name="weather_reference_data_refresh_under_test",
    )


def test_reference_dag_runs_daily_and_owns_only_static_reference_phases():
    module = _module()
    dag = module.dag

    assert dag.dag_id == "weather_reference_data_refresh"
    assert dag.kwargs["schedule"] == "0 1 * * *"
    assert dag.kwargs["max_active_runs"] == 1

    expected_task_ids = tuple(
        task_id for task_id, _cmd, _selector, _include_vars in EXPECTED_REFERENCE_DBT_PHASES
    )
    assert module.DBT_PHASE_TASK_IDS == expected_task_ids
    assert (
        tuple(
            (spec.task_id, spec.dbt_command, spec.selector, spec.include_project_vars)
            for spec in module.DBT_PHASE_SPECS
        )
        == EXPECTED_REFERENCE_DBT_PHASES
    )


def test_reference_dag_phase_operators_carry_the_weather_contract():
    module = _module()
    dag = module.dag

    for task_id, dbt_command, selector, include_project_vars in EXPECTED_REFERENCE_DBT_PHASES:
        task = dag.task_dict[task_id]
        assert isinstance(task, FakePythonOperator)
        assert task.python_callable is module.run_dbt_phase
        assert task.kwargs["op_kwargs"] == {
            "dbt_command": dbt_command,
            "selector": selector,
            "include_project_vars": include_project_vars,
            "threads": None if task_id == "dbt_deps" else 2,
        }
        assert task.kwargs["weight_rule"] == "absolute"
        assert task.kwargs["retries"] == 1
        assert task.kwargs["retry_delay"] == module.DBT_RETRY_DELAY
        assert task.kwargs["on_failure_callback"] is module.record_weather_problem
        if task_id == "dbt_deps":
            assert "pool" not in task.kwargs
        else:
            assert task.kwargs["pool"] == module.TRINO_WEATHER_LEGACY_HEAVY_POOL


def test_reference_dag_chains_phases_linearly_after_runtime_validation():
    module = _module()
    dag = module.dag

    order = [
        "validate_dev_runtime",
        *(task_id for task_id, *_ in EXPECTED_REFERENCE_DBT_PHASES),
        "publish_dbt_run_metrics",
    ]
    assert set(order) == set(dag.task_ids)
    for upstream, downstream in zip(order, order[1:]):
        assert dag.task_dict[upstream].downstream_task_ids == {downstream}


def test_reference_dag_does_not_pin_a_bronze_snapshot_or_serving_hour():
    module = _module()
    source = Path(module.__file__).read_text(encoding="utf-8")

    # 참조 데이터는 bronze snapshot·serving as-of hour 로 필터하지 않는다.
    assert "resolve_weather_snapshot_run" not in source
    assert "SERVING_AS_OF_HOUR_TASK_ID" not in source
    assert "WEATHER_GOLD_PUBLICATION_READY_ASSET" not in source
    # run_dbt_phase 는 두 boundary task_id 를 None 으로 넘겨야 한다.
    assert "snapshot_task_id=None" in source
    assert "serving_as_of_task_id=None" in source


def test_reference_and_transform_phases_partition_the_old_transform_phase_set():
    """참조 DAG 와 transform 이 함께 예전 transform 의 모든 phase 를 정확히 덮되,
    dbt_deps 만 공유하고 나머지는 겹치지 않아야 한다. 한 phase 가 양쪽에서
    빠지거나 양쪽에서 중복되면 실패한다.
    """
    reference = {task_id for task_id, *_ in EXPECTED_REFERENCE_DBT_PHASES}
    transform = {task_id for task_id, *_ in EXPECTED_DBT_PHASES}

    # deps 는 양쪽 DAG 모두 dbt 패키지 설치를 위해 필요하다.
    assert reference & transform == {"dbt_deps"}

    # 예전 transform 이 돌던 참조+데이터 phase 전체 = 두 집합의 합집합.
    old_transform_phases = {
        "dbt_deps",
        "dbt_source_freshness",
        "dbt_seed_asac_axes",
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
        "dbt_seed_place_mapping",
        "dbt_test_place_mapping_seed",
        "dbt_seed_coverage_grid",
        "dbt_test_coverage_grid_seed",
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_run_place_mart",
        "dbt_test_place_mart",
        "dbt_run_coverage_grid_mart",
        "dbt_test_coverage_grid_mart",
        "dbt_run_gold",
        "dbt_test_gold",
    }
    assert reference | transform == old_transform_phases


@pytest.mark.parametrize(
    "reference_only_task",
    [
        "dbt_seed_asac_axes",
        "dbt_run_common_admin_dong_dimension",
        "dbt_seed_place_mapping",
        "dbt_seed_coverage_grid",
    ],
)
def test_reference_phases_left_the_transform_dag(reference_only_task):
    transform = load_transform_module()
    assert reference_only_task not in transform.dag.task_ids
