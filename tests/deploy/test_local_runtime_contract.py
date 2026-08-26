from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
AIRFLOW_LOCAL_SERVICES = (
    "airflow-init",
    "airflow-apiserver",
    "airflow-scheduler",
    "airflow-dag-processor",
    "airflow-triggerer",
)
EXPECTED_KMA_DAG_SCHEDULE = "20 2,5,8,11,14,17,20,23 * * *"
EXPECTED_SERVING_SNAPSHOT_DAG_SCHEDULE = "0 * * * *"
EXPECTED_DISABLED_QUALITY_DAG_SCHEDULE = ""
EXPECTED_ENV_FILE = "${ASK_SEOUL_PROD_ENV_FILE:-.env.prod}"
EXPECTED_DISABLED_OBSERVATION_ENVIRONMENT = {
    "AIRFLOW__CORE__DAG_IGNORE_FILE_SYNTAX": "glob",
    "AIRFLOW__EXECUTION_API__JWT_EXPIRATION_TIME": "7200",
    "ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED": "false",
    "ASK_SEOUL_KMA_OBSERVATION_DAG_SCHEDULE": "",
    "ASK_SEOUL_KMA_CONTROL_ROOT": "/opt/airflow/logs/_weather_control",
    "ASK_SEOUL_KMA_ATTEMPT_LEDGER_PATH": (
        "/opt/airflow/logs/_weather_control/kma_api_budget.sqlite3"
    ),
    "ASK_SEOUL_KMA_DAILY_ATTEMPT_LIMIT": "7500",
    "ASK_SEOUL_WEATHER_QUALITY_DAG_SCHEDULE": EXPECTED_DISABLED_QUALITY_DAG_SCHEDULE,
}
EXPECTED_AIRFLOW_CONCURRENCY_ENVIRONMENT = {
    "AIRFLOW__CORE__PARALLELISM": "8",
    "AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG": "4",
    "AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG": "2",
}


class _ComposeLoader(yaml.SafeLoader):
    pass


def _construct_override(loader: _ComposeLoader, node: yaml.Node) -> object:
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    return loader.construct_scalar(node)


_ComposeLoader.add_constructor("!reset", lambda loader, node: None)
_ComposeLoader.add_constructor("!override", _construct_override)


def _load_local_compose(path: Path) -> dict[str, object]:
    return yaml.load(path.read_text(encoding="utf-8"), Loader=_ComposeLoader)


def _run_validator(repo_root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.local_runtime_contract",
            "--repo-root",
            str(repo_root),
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _write_valid_contract(repo_root: Path) -> None:
    (repo_root / "trino").mkdir(parents=True)
    (repo_root / ".gitignore").write_text(
        "weather-platform.prod.env\n", encoding="utf-8"
    )
    airflow_services = "\n".join(
        f"  {service}:\n"
        "    image: ask-seoul-weather-airflow:mac-local\n"
        + (
            "    build:\n"
            "      context: .\n"
            "      dockerfile: Dockerfile.airflow\n"
            if service == "airflow-init"
            else "    build: !reset null\n"
        )
        + "    env_file:\n"
        f'      - "{EXPECTED_ENV_FILE}"\n'
        + "    environment:\n"
        '      AIRFLOW__OPENLINEAGE__DISABLED: "true"\n'
        '      ASK_SEOUL_DBT_OPENLINEAGE_ENABLED: "false"\n'
        + "".join(
            f'      {name}: "{value}"\n'
            for name, value in EXPECTED_AIRFLOW_CONCURRENCY_ENVIRONMENT.items()
        )
        + f'      ASK_SEOUL_KMA_DAG_SCHEDULE: "{EXPECTED_KMA_DAG_SCHEDULE}"\n'
        "      ASK_SEOUL_WEATHER_SERVING_SNAPSHOT_DAG_SCHEDULE: "
        f'"{EXPECTED_SERVING_SNAPSHOT_DAG_SCHEDULE}"\n'
        + "".join(
            f'      {name}: "{value}"\n'
            for name, value in EXPECTED_DISABLED_OBSERVATION_ENVIRONMENT.items()
        )
        + '      KMA_NUM_OF_ROWS: "2000"'
        for service in AIRFLOW_LOCAL_SERVICES
    )
    (repo_root / "docker-compose.local.yml").write_text(
        "name: seoul-weather-platform-mac\n"
        "networks:\n"
        "  elt_net:\n"
        "    name: seoul-weather-platform-mac-net\n"
        "services:\n"
        "  postgres:\n"
        "    env_file:\n"
        f'      - "{EXPECTED_ENV_FILE}"\n'
        "  trino:\n"
        "    env_file:\n"
        f'      - "{EXPECTED_ENV_FILE}"\n'
        "    mem_limit: 5g\n"
        "    healthcheck:\n"
        "      test:\n"
        "        - CMD-SHELL\n"
        "        - \"curl --fail --silent --show-error --max-time 2 "
        "http://localhost:8080/v1/info >/dev/null\"\n"
        "      interval: 15s\n"
        "      timeout: 3s\n"
        "      retries: 10\n"
        "      start_period: 30s\n"
        "    environment:\n"
        '      TRINO_TASK_CONCURRENCY: "2"\n'
        '      TRINO_QUERY_MAX_MEMORY_PER_NODE: "800MB"\n'
        '      TRINO_MEMORY_HEAP_HEADROOM_PER_NODE: "1500MB"\n'
        '      TRINO_QUERY_MAX_MEMORY: "800MB"\n'
        '      TRINO_QUERY_MAX_TOTAL_MEMORY: "1200MB"\n'
        "    depends_on:\n"
        "      trino-cache-init:\n"
        "        condition: service_completed_successfully\n"
        "    volumes:\n"
        "      - ./trino/catalog-prod:/etc/trino/catalog:ro\n"
        "      - ./trino/jvm.config:/etc/trino/jvm.config:ro\n"
        "      - ./trino/config.properties:/etc/trino/config.properties:ro\n"
        "      - ./trino/resource-groups.properties:/etc/trino/resource-groups.properties:ro\n"
        "      - ./trino/resource-groups.json:/etc/trino/resource-groups.json:ro\n"
        "      - trino_cache:/data/trino\n"
        "  trino-cache-init:\n"
        "    volumes:\n"
        "      - trino_cache:/data/trino\n"
        f"{airflow_services}\n",
        encoding="utf-8",
    )
    (repo_root / "trino" / "jvm.config").write_text(
        "-XX:MaxRAMPercentage=55\n", encoding="utf-8"
    )
    (repo_root / "trino" / "config.properties").write_text(
        "spill-enabled=true\n"
        "query.low-memory-killer.policy=total-reservation-on-blocked-nodes\n",
        encoding="utf-8",
    )
    (repo_root / "trino" / "resource-groups.json").write_text(
        json.dumps(
            {
                "rootGroups": [
                    {
                        "name": "global",
                        "softMemoryLimit": "80%",
                        "hardConcurrencyLimit": 1,
                        "maxQueued": 10,
                    }
                ],
                "selectors": [{"user": ".*", "group": "global"}],
            }
        ),
        encoding="utf-8",
    )


def test_repository_local_runtime_contract_is_safe() -> None:
    result = _run_validator(REPO_ROOT)

    assert result.returncode == 0, result.stderr
    assert result.stdout == "local_runtime_contract=passed\n"


def test_repository_exposes_only_the_local_override_name() -> None:
    assert (REPO_ROOT / "docker-compose.local.yml").is_file()
    assert not (REPO_ROOT / "docker-compose.mac.yml").exists()
    assert not (REPO_ROOT / "docker-compose.prod.yml").exists()


def test_local_override_does_not_reconfigure_retired_marquez_services() -> None:
    compose = _load_local_compose(REPO_ROOT / "docker-compose.local.yml")

    assert {"marquez-db", "marquez-api"}.isdisjoint(compose["services"])


def test_repository_retains_runtime_identity_for_state_compatibility() -> None:
    compose = _load_local_compose(REPO_ROOT / "docker-compose.local.yml")

    assert compose["name"] == "seoul-weather-platform-mac"
    assert compose["networks"]["elt_net"]["name"] == (
        "seoul-weather-platform-mac-net"
    )


def test_repository_builds_the_shared_airflow_image_exactly_once() -> None:
    compose = _load_local_compose(REPO_ROOT / "docker-compose.local.yml")
    build_owners = sorted(
        service_name
        for service_name, service in compose["services"].items()
        if service.get("build") is not None
    )
    reset_consumers = sorted(
        service_name
        for service_name, service in compose["services"].items()
        if service_name.startswith("airflow-")
        and service.get("build") is None
        and "build" in service
    )

    assert build_owners == ["airflow-init"]
    assert reset_consumers == [
        "airflow-apiserver",
        "airflow-dag-processor",
        "airflow-scheduler",
        "airflow-triggerer",
    ]


def test_repository_preserves_prod_env_and_trino_cache_contract() -> None:
    compose = _load_local_compose(REPO_ROOT / "docker-compose.local.yml")
    services = compose["services"]

    for service_name in (*AIRFLOW_LOCAL_SERVICES, "postgres", "trino"):
        assert services[service_name]["env_file"] == [EXPECTED_ENV_FILE]
    assert services["trino"]["depends_on"] == {
        "trino-cache-init": {"condition": "service_completed_successfully"}
    }
    assert "./trino/catalog-prod:/etc/trino/catalog:ro" in services["trino"][
        "volumes"
    ]
    assert "trino_cache:/data/trino" in services["trino"]["volumes"]


def test_repository_keeps_the_local_runtime_budget_and_schedules() -> None:
    compose = _load_local_compose(REPO_ROOT / "docker-compose.local.yml")
    services = compose["services"]
    trino_environment = services["trino"]["environment"]

    assert trino_environment["TRINO_QUERY_MAX_MEMORY_PER_NODE"] == "800MB"
    assert trino_environment["TRINO_QUERY_MAX_MEMORY"] == "800MB"
    assert trino_environment["TRINO_QUERY_MAX_TOTAL_MEMORY"] == "1200MB"
    assert trino_environment["TRINO_MEMORY_HEAP_HEADROOM_PER_NODE"] == "1500MB"
    for service_name in AIRFLOW_LOCAL_SERVICES:
        environment = services[service_name]["environment"]
        assert {
            name: environment[name]
            for name in EXPECTED_AIRFLOW_CONCURRENCY_ENVIRONMENT
        } == EXPECTED_AIRFLOW_CONCURRENCY_ENVIRONMENT
        assert environment["ASK_SEOUL_KMA_DAG_SCHEDULE"] == EXPECTED_KMA_DAG_SCHEDULE
        assert environment["ASK_SEOUL_WEATHER_SERVING_SNAPSHOT_DAG_SCHEDULE"] == (
            EXPECTED_SERVING_SNAPSHOT_DAG_SCHEDULE
        )
        assert (
            environment["ASK_SEOUL_WEATHER_QUALITY_DAG_SCHEDULE"]
            == EXPECTED_DISABLED_QUALITY_DAG_SCHEDULE
        )
        assert {
            name: environment[name]
            for name in EXPECTED_DISABLED_OBSERVATION_ENVIRONMENT
        } == EXPECTED_DISABLED_OBSERVATION_ENVIRONMENT
        assert environment["KMA_NUM_OF_ROWS"] == "2000"


def test_local_trino_healthcheck_is_http_liveness_not_a_queued_sql_query() -> None:
    compose = _load_local_compose(REPO_ROOT / "docker-compose.local.yml")
    healthcheck = compose["services"]["trino"].get("healthcheck")

    assert healthcheck == {
        "test": [
            "CMD-SHELL",
            "curl --fail --silent --show-error --max-time 2 "
            "http://localhost:8080/v1/info >/dev/null",
        ],
        "interval": "15s",
        "timeout": "3s",
        "retries": 10,
        "start_period": "30s",
    }


def test_validator_accepts_a_conservative_memory_and_lineage_contract(
    tmp_path: Path,
) -> None:
    _write_valid_contract(tmp_path)

    result = _run_validator(tmp_path)

    assert result.returncode == 0, result.stderr
    assert result.stdout == "local_runtime_contract=passed\n"


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (
            'TRINO_MEMORY_HEAP_HEADROOM_PER_NODE: "1500MB"',
            'TRINO_MEMORY_HEAP_HEADROOM_PER_NODE: "2200MB"',
        ),
        (
            'AIRFLOW__OPENLINEAGE__DISABLED: "true"',
            'AIRFLOW__OPENLINEAGE__DISABLED: "false"',
        ),
        (
            f'ASK_SEOUL_KMA_DAG_SCHEDULE: "{EXPECTED_KMA_DAG_SCHEDULE}"',
            'ASK_SEOUL_KMA_DAG_SCHEDULE: ""',
        ),
        (
            "ASK_SEOUL_WEATHER_SERVING_SNAPSHOT_DAG_SCHEDULE: "
            f'"{EXPECTED_SERVING_SNAPSHOT_DAG_SCHEDULE}"',
            'ASK_SEOUL_WEATHER_SERVING_SNAPSHOT_DAG_SCHEDULE: "5 * * * *"',
        ),
        (
            'ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED: "false"',
            'ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED: "true"',
        ),
        (
            'ASK_SEOUL_KMA_OBSERVATION_DAG_SCHEDULE: ""',
            'ASK_SEOUL_KMA_OBSERVATION_DAG_SCHEDULE: "45 * * * *"',
        ),
        (
            'ASK_SEOUL_WEATHER_QUALITY_DAG_SCHEDULE: ""',
            'ASK_SEOUL_WEATHER_QUALITY_DAG_SCHEDULE: "5 3 * * *"',
        ),
        ('KMA_NUM_OF_ROWS: "2000"', 'KMA_NUM_OF_ROWS: "1000"'),
        ("seoul-weather-platform-mac-net", "elt-infra-prod-net"),
        (
            "./trino/catalog-prod:/etc/trino/catalog:ro",
            "./trino/catalog:/etc/trino/catalog:ro",
        ),
        (
            "trino_cache:/data/trino",
            "trino_cache:/data/other",
        ),
    ],
)
def test_validator_rejects_unsafe_local_runtime_drift(
    tmp_path: Path, old: str, new: str
) -> None:
    _write_valid_contract(tmp_path)
    compose_path = tmp_path / "docker-compose.local.yml"
    contents = compose_path.read_text(encoding="utf-8")
    assert old in contents
    compose_path.write_text(contents.replace(old, new, 1), encoding="utf-8")

    result = _run_validator(tmp_path)

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "local_runtime_contract_invalid\n"


def test_validator_rejects_multiple_airflow_build_owners(tmp_path: Path) -> None:
    _write_valid_contract(tmp_path)
    compose_path = tmp_path / "docker-compose.local.yml"
    contents = compose_path.read_text(encoding="utf-8")
    old = (
        "  airflow-apiserver:\n"
        "    image: ask-seoul-weather-airflow:mac-local\n"
        "    build: !reset null\n"
    )
    new = (
        "  airflow-apiserver:\n"
        "    image: ask-seoul-weather-airflow:mac-local\n"
        "    build:\n"
        "      context: .\n"
        "      dockerfile: Dockerfile.airflow\n"
    )
    assert old in contents
    compose_path.write_text(contents.replace(old, new), encoding="utf-8")

    result = _run_validator(tmp_path)

    assert result.returncode == 2
    assert result.stderr == "local_runtime_contract_invalid\n"


def test_validator_rejects_a_missing_airflow_build_reset(tmp_path: Path) -> None:
    _write_valid_contract(tmp_path)
    compose_path = tmp_path / "docker-compose.local.yml"
    contents = compose_path.read_text(encoding="utf-8")
    old = (
        "  airflow-apiserver:\n"
        "    image: ask-seoul-weather-airflow:mac-local\n"
        "    build: !reset null\n"
    )
    new = (
        "  airflow-apiserver:\n"
        "    image: ask-seoul-weather-airflow:mac-local\n"
    )
    assert old in contents
    compose_path.write_text(contents.replace(old, new), encoding="utf-8")

    result = _run_validator(tmp_path)

    assert result.returncode == 2
    assert result.stderr == "local_runtime_contract_invalid\n"
