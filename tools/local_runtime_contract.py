from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml


AIRFLOW_SERVICES = frozenset(
    {
        "airflow-init",
        "airflow-apiserver",
        "airflow-scheduler",
        "airflow-dag-processor",
        "airflow-triggerer",
    }
)
LOCAL_OVERRIDE_SERVICES = AIRFLOW_SERVICES | {
    "postgres",
    "trino",
    "trino-cache-init",
}
EXPECTED_ENV_FILE = ["${ASK_SEOUL_PROD_ENV_FILE:-.env.prod}"]
EXPECTED_TRINO_ENVIRONMENT = {
    "TRINO_TASK_CONCURRENCY": "2",
    "TRINO_QUERY_MAX_MEMORY_PER_NODE": "800MB",
    "TRINO_MEMORY_HEAP_HEADROOM_PER_NODE": "1500MB",
    "TRINO_QUERY_MAX_MEMORY": "800MB",
    "TRINO_QUERY_MAX_TOTAL_MEMORY": "1200MB",
}
EXPECTED_AIRFLOW_ENVIRONMENT = {
    "ASK_SEOUL_KMA_DAG_SCHEDULE": "20 2,5,8,11,14,17,20,23 * * *",
    "ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED": "false",
    "ASK_SEOUL_KMA_OBSERVATION_DAG_SCHEDULE": "",
    "ASK_SEOUL_KMA_CONTROL_ROOT": "/opt/airflow/logs/_weather_control",
    "ASK_SEOUL_KMA_ATTEMPT_LEDGER_PATH": (
        "/opt/airflow/logs/_weather_control/kma_api_budget.sqlite3"
    ),
    "ASK_SEOUL_KMA_DAILY_ATTEMPT_LIMIT": "7500",
    "ASK_SEOUL_WEATHER_SERVING_SNAPSHOT_DAG_SCHEDULE": "0 * * * *",
}
EXPECTED_TRINO_MOUNTS = {
    "./trino/catalog-prod:/etc/trino/catalog:ro",
    "./trino/jvm.config:/etc/trino/jvm.config:ro",
    "./trino/config.properties:/etc/trino/config.properties:ro",
    "./trino/resource-groups.properties:/etc/trino/resource-groups.properties:ro",
    "./trino/resource-groups.json:/etc/trino/resource-groups.json:ro",
    "trino_cache:/data/trino",
}
_MEMORY_PATTERN = re.compile(r"^(\d+)(MB|GB|M|G)$", re.IGNORECASE)


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


class LocalRuntimeContractError(RuntimeError):
    """Redacted failure for an unsafe or incomplete local runtime contract."""


@dataclass(frozen=True)
class LocalRuntimeContractProof:
    project_name: str
    network_name: str
    trino_container_mib: int
    trino_heap_mib: int
    trino_query_and_headroom_mib: int
    hard_query_concurrency: int
    max_queued_queries: int
    operational_lineage_disabled: bool
    kma_dag_schedule: str
    serving_snapshot_dag_schedule: str


def _invalid() -> LocalRuntimeContractError:
    return LocalRuntimeContractError("local_runtime_contract_invalid")


def _mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _invalid()
    return value


def _memory_mib(value: object) -> int:
    if not isinstance(value, str):
        raise _invalid()
    match = _MEMORY_PATTERN.fullmatch(value.strip())
    if match is None:
        raise _invalid()
    amount = int(match.group(1))
    return amount * (1024 if match.group(2).upper().startswith("G") else 1)


def _properties(path: Path) -> dict[str, str]:
    properties: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not key.strip() or key.strip() in properties:
            raise _invalid()
        properties[key.strip()] = value.strip()
    return properties


def _jvm_max_ram_percentage(path: Path) -> int:
    prefix = "-XX:MaxRAMPercentage="
    values = [
        line.strip()[len(prefix) :]
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip().startswith(prefix)
    ]
    if len(values) != 1:
        raise _invalid()
    try:
        return int(values[0])
    except ValueError as error:
        raise _invalid() from error


def _validate_storage_contract(services: Mapping[str, Any]) -> None:
    for service_name in AIRFLOW_SERVICES | {"postgres", "trino"}:
        service = _mapping(services.get(service_name))
        if service.get("env_file") != EXPECTED_ENV_FILE:
            raise _invalid()

    trino = _mapping(services.get("trino"))
    depends_on = _mapping(trino.get("depends_on"))
    cache_dependency = _mapping(depends_on.get("trino-cache-init"))
    if cache_dependency.get("condition") != "service_completed_successfully":
        raise _invalid()
    volumes = trino.get("volumes")
    if not isinstance(volumes, list) or set(volumes) != EXPECTED_TRINO_MOUNTS:
        raise _invalid()
    cache_volumes = _mapping(services.get("trino-cache-init")).get("volumes")
    if cache_volumes != ["trino_cache:/data/trino"]:
        raise _invalid()


def validate_local_runtime_contract(repo_root: Path) -> LocalRuntimeContractProof:
    """Validate the secretless, conservative personal local runtime settings."""
    try:
        root = repo_root.resolve(strict=True)
        compose = _mapping(
            yaml.load(
                (root / "docker-compose.local.yml").read_text(encoding="utf-8"),
                Loader=_ComposeLoader,
            )
        )
        if compose.get("name") != "seoul-weather-platform-mac":
            raise _invalid()
        networks = _mapping(compose.get("networks"))
        elt_network = _mapping(networks.get("elt_net"))
        if elt_network.get("name") != "seoul-weather-platform-mac-net":
            raise _invalid()
        services = _mapping(compose.get("services"))
        if frozenset(services) != LOCAL_OVERRIDE_SERVICES:
            raise _invalid()
        _validate_storage_contract(services)

        build_owners: set[str] = set()
        for service_name in AIRFLOW_SERVICES:
            service = _mapping(services.get(service_name))
            if service.get("image") != "ask-seoul-weather-airflow:mac-local":
                raise _invalid()
            if service.get("build") is not None:
                build_owners.add(service_name)
            elif service_name != "airflow-init" and "build" not in service:
                raise _invalid()
            environment = _mapping(service.get("environment"))
            if str(environment.get("AIRFLOW__OPENLINEAGE__DISABLED", "")).lower() != "true":
                raise _invalid()
            if str(environment.get("ASK_SEOUL_DBT_OPENLINEAGE_ENABLED", "")).lower() != "false":
                raise _invalid()
            for name, expected in EXPECTED_AIRFLOW_ENVIRONMENT.items():
                if environment.get(name) != expected:
                    raise _invalid()
            if str(environment.get("KMA_NUM_OF_ROWS", "")) != "2000":
                raise _invalid()
        if build_owners != {"airflow-init"}:
            raise _invalid()
        airflow_build = _mapping(_mapping(services["airflow-init"]).get("build"))
        if airflow_build.get("context") != ".":
            raise _invalid()
        if airflow_build.get("dockerfile") != "Dockerfile.airflow":
            raise _invalid()

        trino = _mapping(services.get("trino"))
        container_mib = _memory_mib(trino.get("mem_limit"))
        if container_mib != 5 * 1024:
            raise _invalid()
        trino_environment = _mapping(trino.get("environment"))
        for name, expected in EXPECTED_TRINO_ENVIRONMENT.items():
            if str(trino_environment.get(name, "")) != expected:
                raise _invalid()

        max_ram_percentage = _jvm_max_ram_percentage(root / "trino" / "jvm.config")
        if max_ram_percentage != 55:
            raise _invalid()
        heap_mib = container_mib * max_ram_percentage // 100
        query_and_headroom_mib = _memory_mib(
            trino_environment["TRINO_QUERY_MAX_MEMORY_PER_NODE"]
        ) + _memory_mib(trino_environment["TRINO_MEMORY_HEAP_HEADROOM_PER_NODE"])
        if query_and_headroom_mib >= heap_mib:
            raise _invalid()
        if _memory_mib(trino_environment["TRINO_QUERY_MAX_TOTAL_MEMORY"]) <= _memory_mib(
            trino_environment["TRINO_QUERY_MAX_MEMORY"]
        ):
            raise _invalid()

        trino_properties = _properties(root / "trino" / "config.properties")
        if trino_properties.get("spill-enabled", "").lower() != "true":
            raise _invalid()
        if (
            trino_properties.get("query.low-memory-killer.policy")
            != "total-reservation-on-blocked-nodes"
        ):
            raise _invalid()

        resource_groups = _mapping(
            json.loads((root / "trino" / "resource-groups.json").read_text(encoding="utf-8"))
        )
        root_groups = resource_groups.get("rootGroups")
        if not isinstance(root_groups, list) or len(root_groups) != 1:
            raise _invalid()
        global_group = _mapping(root_groups[0])
        hard_concurrency = global_group.get("hardConcurrencyLimit")
        max_queued = global_group.get("maxQueued")
        if hard_concurrency != 1 or max_queued != 10:
            raise _invalid()

        ignore_lines = {
            line.strip()
            for line in (root / ".gitignore").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        if "weather-platform.prod.env" not in ignore_lines:
            raise _invalid()
    except LocalRuntimeContractError:
        raise
    except (OSError, TypeError, ValueError, json.JSONDecodeError, yaml.YAMLError) as error:
        raise _invalid() from error

    return LocalRuntimeContractProof(
        project_name="seoul-weather-platform-mac",
        network_name="seoul-weather-platform-mac-net",
        trino_container_mib=container_mib,
        trino_heap_mib=heap_mib,
        trino_query_and_headroom_mib=query_and_headroom_mib,
        hard_query_concurrency=hard_concurrency,
        max_queued_queries=max_queued,
        operational_lineage_disabled=True,
        kma_dag_schedule=EXPECTED_AIRFLOW_ENVIRONMENT["ASK_SEOUL_KMA_DAG_SCHEDULE"],
        serving_snapshot_dag_schedule=EXPECTED_AIRFLOW_ENVIRONMENT[
            "ASK_SEOUL_WEATHER_SERVING_SNAPSHOT_DAG_SCHEDULE"
        ],
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        validate_local_runtime_contract(args.repo_root)
    except LocalRuntimeContractError:
        print("local_runtime_contract_invalid", file=sys.stderr)
        return 2
    print("local_runtime_contract=passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
