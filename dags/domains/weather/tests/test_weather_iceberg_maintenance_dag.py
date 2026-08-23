from weather_transform_test_support import (
    FakePythonOperator,
    load_transform_module,
    restore_airflow_modules_after_transform_import,  # noqa: F401
)


def _module():
    return load_transform_module(
        "weather_iceberg_maintenance.py",
        module_name="weather_iceberg_maintenance_under_test",
    )


def _action_task_id(schema, name, operation):
    return f"{schema}__{name}__{operation}"


def test_maintenance_dag_is_manual_by_default_and_paused_on_creation(monkeypatch):
    monkeypatch.delenv("ASK_SEOUL_WEATHER_MAINTENANCE_DAG_SCHEDULE", raising=False)
    module = _module()
    dag = module.dag
    assert dag.dag_id == "weather_iceberg_maintenance"
    assert dag.kwargs["schedule"] is None
    assert dag.kwargs["max_active_runs"] == 1
    # 데이터 파괴 가능 DDL 을 돌리므로 생성 시 paused. 사람이 확인 후 unpause.
    assert dag.kwargs["is_paused_upon_creation"] is True


def test_maintenance_schedule_requires_an_explicit_opt_in(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_WEATHER_MAINTENANCE_DAG_SCHEDULE", "0 4 * * 0")

    module = _module()

    assert module.dag.kwargs["schedule"] == "0 4 * * 0"


def test_every_owned_table_gets_all_three_operations_in_order():
    module = _module()
    dag = module.dag
    for table in module.MAINTAINED_TABLES:
        ids = [_action_task_id(table.schema, table.name, op) for op in module.OPERATIONS]
        for task_id in ids:
            assert task_id in dag.task_ids
        # 같은 테이블 안에서 optimize -> expire -> orphan 순으로만 이어진다.
        assert dag.task_dict[ids[0]].downstream_task_ids >= {ids[1]}
        assert dag.task_dict[ids[1]].downstream_task_ids >= {ids[2]}


def test_actions_are_serialized_bounded_and_low_priority_without_retry():
    module = _module()
    dag = module.dag
    for table in module.MAINTAINED_TABLES:
        for op in module.OPERATIONS:
            task = dag.task_dict[_action_task_id(table.schema, table.name, op)]
            assert isinstance(task, FakePythonOperator)
            assert task.python_callable is module.run_maintenance_action
            assert task.kwargs["pool"] == module.TRINO_WEATHER_HEAVY_POOL
            assert task.kwargs["pool_slots"] == 1
            assert task.kwargs["weight_rule"] == "absolute"
            assert task.kwargs["priority_weight"] == module.MAINTENANCE_PRIORITY_WEIGHT
            assert task.kwargs["execution_timeout"] == module.MAINTENANCE_ACTION_TIMEOUT
            # mutation 은 자동 retry 하지 않는다.
            assert task.kwargs["retries"] == 0


def test_first_table_respects_runtime_gate_but_later_tables_isolate_failures():
    module = _module()
    dag = module.dag
    tables = module.MAINTAINED_TABLES

    first = dag.task_dict[_action_task_id(tables[0].schema, tables[0].name, "optimize")]
    # 첫 테이블의 첫 동작은 all_success(기본) — validate_runtime 이 실패하면 안 돈다.
    assert first.kwargs.get("trigger_rule", "all_success") == "all_success"
    assert first.upstream_task_ids == {"validate_dev_runtime"}

    second = dag.task_dict[_action_task_id(tables[1].schema, tables[1].name, "optimize")]
    # 이후 테이블의 첫 동작은 all_done — 앞 테이블이 실패해도 계속 돈다.
    assert second.kwargs["trigger_rule"] == "all_done"


def test_within_a_table_the_later_operations_stay_all_success():
    module = _module()
    dag = module.dag
    table = module.MAINTAINED_TABLES[1]
    expire = dag.task_dict[_action_task_id(table.schema, table.name, "expire_snapshots")]
    orphan = dag.task_dict[_action_task_id(table.schema, table.name, "remove_orphan_files")]
    # 앞 동작이 실패하면 뒤 동작은 건너뛰어야 하므로 all_done 을 걸지 않는다.
    assert expire.kwargs.get("trigger_rule", "all_success") == "all_success"
    assert orphan.kwargs.get("trigger_rule", "all_success") == "all_success"


def test_summary_gate_waits_for_all_actions_with_all_done():
    module = _module()
    dag = module.dag
    summarize = dag.task_dict["summarize_maintenance"]
    assert summarize.kwargs["trigger_rule"] == "all_done"
    # 모든 테이블의 마지막 동작(orphan)이 summarize 로 모인다.
    for table in module.MAINTAINED_TABLES:
        last = dag.task_dict[_action_task_id(table.schema, table.name, "remove_orphan_files")]
        assert "summarize_maintenance" in last.downstream_task_ids


def test_task_count_is_gate_plus_three_ops_per_table_plus_summary():
    module = _module()
    dag = module.dag
    expected = 1 + len(module.MAINTAINED_TABLES) * len(module.OPERATIONS) + 1
    assert len(dag.task_ids) == expected
