import importlib.util
import sys
import types
from pathlib import Path

import pytest


_AIRFLOW_MODULE_NAMES = (
    "airflow",
    "airflow.exceptions",
    "airflow.models",
    "airflow.models.param",
    "airflow.providers",
    "airflow.providers.standard",
    "airflow.providers.standard.operators",
    "airflow.providers.standard.operators.python",
    "airflow.sdk",
)


@pytest.fixture(autouse=True)
def restore_airflow_modules_after_transform_import():
    originals = {name: sys.modules.get(name) for name in _AIRFLOW_MODULE_NAMES}
    yield
    for name, module in originals.items():
        if module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module


class FakeDAG:
    _stack = []

    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        self.kwargs = kwargs
        self.task_dict = {}

    def __enter__(self):
        self._stack.append(self)
        return self

    def __exit__(self, *_exc_info):
        self._stack.pop()

    @property
    def task_ids(self):
        return list(self.task_dict)

    def add_task(self, task):
        self.task_dict[task.task_id] = task


class FakePythonOperator:
    def __init__(self, task_id, python_callable, **kwargs):
        self.task_id = task_id
        self.python_callable = python_callable
        self.kwargs = kwargs
        self.downstream_task_ids = set()
        self.upstream_task_ids = set()
        FakeDAG._stack[-1].add_task(self)

    def __rshift__(self, other):
        self.downstream_task_ids.add(other.task_id)
        other.upstream_task_ids.add(self.task_id)
        return other

    def as_teardown(self, setups=None, on_failure_fail_dagrun=False):
        self.is_teardown = True
        self.on_failure_fail_dagrun = on_failure_fail_dagrun
        self.kwargs["trigger_rule"] = "all_done_setup_success"
        return self


class FakeParam:
    def __init__(self, default=None, **schema):
        self.value = default
        self.schema = schema


class FakeAsset:
    def __init__(self, uri):
        self.uri = uri

    def __eq__(self, other):
        return isinstance(other, FakeAsset) and self.uri == other.uri

    def __hash__(self):
        return hash(self.uri)


class FakeAirflowException(Exception):
    pass


class FakeAirflowFailException(FakeAirflowException):
    pass


class FakeAirflowSkipException(FakeAirflowException):
    pass


class FakeTaskInstance:
    def __init__(self, *, task_id="dbt_run_silver", try_number=1, pulls=None):
        self.task_id = task_id
        self.try_number = try_number
        self.pulls = pulls or {}
        self.pushes = []

    def xcom_pull(self, *, task_ids, key=None):
        return self.pulls.get((task_ids, key))

    def xcom_push(self, *, key, value):
        self.pushes.append((key, value))


def install_airflow_fakes():
    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDAG

    airflow_exceptions = types.ModuleType("airflow.exceptions")
    airflow_exceptions.AirflowException = FakeAirflowException
    airflow_exceptions.AirflowFailException = FakeAirflowFailException
    airflow_exceptions.AirflowSkipException = FakeAirflowSkipException

    airflow_models = types.ModuleType("airflow.models")
    airflow_models_param = types.ModuleType("airflow.models.param")
    airflow_models_param.Param = FakeParam

    airflow_providers = types.ModuleType("airflow.providers")
    airflow_standard = types.ModuleType("airflow.providers.standard")
    airflow_operators = types.ModuleType("airflow.providers.standard.operators")
    airflow_python = types.ModuleType("airflow.providers.standard.operators.python")
    airflow_python.PythonOperator = FakePythonOperator
    airflow_sdk = types.ModuleType("airflow.sdk")
    airflow_sdk.Asset = FakeAsset

    sys.modules.update(
        {
            "airflow": airflow,
            "airflow.exceptions": airflow_exceptions,
            "airflow.models": airflow_models,
            "airflow.models.param": airflow_models_param,
            "airflow.providers": airflow_providers,
            "airflow.providers.standard": airflow_standard,
            "airflow.providers.standard.operators": airflow_operators,
            "airflow.providers.standard.operators.python": airflow_python,
            "airflow.sdk": airflow_sdk,
        }
    )


def load_transform_module(
    filename="weather_vilage_fcst_transform.py", module_name=None
):
    install_airflow_fakes()
    module_path = Path(__file__).resolve().parents[1] / filename
    spec = importlib.util.spec_from_file_location(
        module_name or f"{module_path.stem}_under_test", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


EXPECTED_DBT_PHASES = (
    ("dbt_deps", "deps", None, False),
    (
        "dbt_source_freshness",
        "source freshness",
        "ask_seoul_weather_transform_source",
        True,
    ),
    (
        "dbt_seed_asac_axes",
        "seed",
        "ask_seoul_weather_transform_asac_axes",
        True,
    ),
    (
        "dbt_run_common_admin_dong_dimension",
        "run",
        "ask_seoul_weather_transform_common_admin",
        True,
    ),
    (
        "dbt_test_common_admin_dong_dimension",
        "test",
        "ask_seoul_weather_transform_common_admin",
        True,
    ),
    (
        "dbt_seed_place_mapping",
        "seed",
        "ask_seoul_weather_transform_place_mapping",
        True,
    ),
    (
        "dbt_test_place_mapping_seed",
        "test",
        "ask_seoul_weather_transform_place_mapping",
        True,
    ),
    (
        "dbt_seed_coverage_grid",
        "seed",
        "ask_seoul_weather_transform_coverage_grid",
        True,
    ),
    (
        "dbt_test_coverage_grid_seed",
        "test",
        "ask_seoul_weather_transform_coverage_grid",
        True,
    ),
    (
        "dbt_run_silver",
        "run",
        "ask_seoul_weather_transform_silver",
        True,
    ),
    (
        "dbt_test_silver",
        "test",
        "ask_seoul_weather_transform_silver",
        True,
    ),
    (
        "dbt_run_place_mart",
        "run",
        "ask_seoul_weather_transform_serving_place_mart",
        True,
    ),
    (
        "dbt_test_place_mart",
        "test",
        "ask_seoul_weather_transform_serving_place_mart",
        True,
    ),
    (
        "dbt_run_coverage_grid_mart",
        "run",
        "ask_seoul_weather_transform_serving_grid_mart",
        True,
    ),
    (
        "dbt_test_coverage_grid_mart",
        "test",
        "ask_seoul_weather_transform_serving_grid_mart",
        True,
    ),
    (
        "dbt_run_gold",
        "run",
        "ask_seoul_weather_transform_serving_gold",
        True,
    ),
    (
        "dbt_test_gold",
        "test",
        "ask_seoul_weather_transform_serving_gold",
        True,
    ),
)
