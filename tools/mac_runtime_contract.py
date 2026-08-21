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
MAC_OVERRIDE_SERVICES = AIRFLOW_SERVICES | {"trino"}
EXPECTED_TRINO_ENVIRONMENT = {
    "TRINO_TASK_CONCURRENCY": "2",
    "TRINO_QUERY_MAX_MEMORY_PER_NODE": "800MB",
    "TRINO_MEMORY_HEAP_HEADROOM_PER_NODE": "1500MB",
    "TRINO_QUERY_MAX_MEMORY": "800MB",
    "TRINO_QUERY_MAX_TOTAL_MEMORY": "1200MB",
}
_MEMORY_PATTERN = re.compile(r"^(\d+)(MB|GB|M|G)$", re.IGNORECASE)


class _ComposeLoader(yaml.SafeLoader):
    pass


_ComposeLoader.add_constructor("!reset", lambda loader, node: None)


class MacRuntimeContractError(RuntimeError):
    """Redacted failure for an unsafe or incomplete Mac runtime contract."""


@dataclass(frozen=True)
class MacRuntimeContractProof:
    project_name: str
    network_name: str
    trino_container_mib: int
    trino_heap_mib: int
    trino_query_and_headroom_mib: int
    hard_query_concurrency: int
    max_queued_queries: int
    operational_lineage_disabled: bool


def _mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise MacRuntimeContractError("mac_runtime_contract_invalid")
    return value


def _memory_mib(value: object) -> int:
    if not isinstance(value, str):
        raise MacRuntimeContractError("mac_runtime_contract_invalid")
    match = _MEMORY_PATTERN.fullmatch(value.strip())
    if match is None:
        raise MacRuntimeContractError("mac_runtime_contract_invalid")
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
            raise MacRuntimeContractError("mac_runtime_contract_invalid")
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
        raise MacRuntimeContractError("mac_runtime_contract_invalid")
    try:
        percentage = int(values[0])
    except ValueError as error:
        raise MacRuntimeContractError("mac_runtime_contract_invalid") from error
    return percentage


def validate_mac_runtime_contract(repo_root: Path) -> MacRuntimeContractProof:
    """Validate the secretless, conservative runtime settings used on a Mac."""
    try:
        root = repo_root.resolve(strict=True)
        compose = _mapping(
            yaml.load(
                (root / "docker-compose.mac.yml").read_text(encoding="utf-8"),
                Loader=_ComposeLoader,
            )
        )
        if compose.get("name") != "seoul-weather-platform-mac":
            raise MacRuntimeContractError("mac_runtime_contract_invalid")
        networks = _mapping(compose.get("networks"))
        elt_network = _mapping(networks.get("elt_net"))
        if elt_network.get("name") != "seoul-weather-platform-mac-net":
            raise MacRuntimeContractError("mac_runtime_contract_invalid")
        services = _mapping(compose.get("services"))
        if frozenset(services) != MAC_OVERRIDE_SERVICES:
            raise MacRuntimeContractError("mac_runtime_contract_invalid")

        build_owners: set[str] = set()
        for service_name in AIRFLOW_SERVICES:
            service = _mapping(services.get(service_name))
            if service.get("image") != "ask-seoul-weather-airflow:mac-local":
                raise MacRuntimeContractError("mac_runtime_contract_invalid")
            if service.get("build") is not None:
                build_owners.add(service_name)
            elif service_name != "airflow-init" and "build" not in service:
                raise MacRuntimeContractError("mac_runtime_contract_invalid")
            environment = _mapping(service.get("environment"))
            if str(environment.get("AIRFLOW__OPENLINEAGE__DISABLED", "")).lower() != "true":
                raise MacRuntimeContractError("mac_runtime_contract_invalid")
            if str(
                environment.get("ASK_SEOUL_DBT_OPENLINEAGE_ENABLED", "")
            ).lower() != "false":
                raise MacRuntimeContractError("mac_runtime_contract_invalid")
            if environment.get("ASK_SEOUL_KMA_DAG_SCHEDULE") != "":
                raise MacRuntimeContractError("mac_runtime_contract_invalid")
            if (
                environment.get(
                    "ASK_SEOUL_WEATHER_SERVING_SNAPSHOT_DAG_SCHEDULE"
                )
                != ""
            ):
                raise MacRuntimeContractError("mac_runtime_contract_invalid")
            if str(environment.get("KMA_NUM_OF_ROWS", "")) != "2000":
                raise MacRuntimeContractError("mac_runtime_contract_invalid")
        if build_owners != {"airflow-init"}:
            raise MacRuntimeContractError("mac_runtime_contract_invalid")
        airflow_build = _mapping(_mapping(services["airflow-init"]).get("build"))
        if airflow_build.get("context") != ".":
            raise MacRuntimeContractError("mac_runtime_contract_invalid")
        if airflow_build.get("dockerfile") != "Dockerfile.airflow":
            raise MacRuntimeContractError("mac_runtime_contract_invalid")

        trino = _mapping(services.get("trino"))
        container_mib = _memory_mib(trino.get("mem_limit"))
        if container_mib != 5 * 1024:
            raise MacRuntimeContractError("mac_runtime_contract_invalid")
        trino_environment = _mapping(trino.get("environment"))
        for name, expected in EXPECTED_TRINO_ENVIRONMENT.items():
            if str(trino_environment.get(name, "")) != expected:
                raise MacRuntimeContractError("mac_runtime_contract_invalid")

        max_ram_percentage = _jvm_max_ram_percentage(root / "trino" / "jvm.config")
        if max_ram_percentage != 55:
            raise MacRuntimeContractError("mac_runtime_contract_invalid")
        heap_mib = container_mib * max_ram_percentage // 100
        query_and_headroom_mib = _memory_mib(
            trino_environment["TRINO_QUERY_MAX_MEMORY_PER_NODE"]
        ) + _memory_mib(
            trino_environment["TRINO_MEMORY_HEAP_HEADROOM_PER_NODE"]
        )
        if query_and_headroom_mib >= heap_mib:
            raise MacRuntimeContractError("mac_runtime_contract_invalid")
        if _memory_mib(trino_environment["TRINO_QUERY_MAX_TOTAL_MEMORY"]) <= _memory_mib(
            trino_environment["TRINO_QUERY_MAX_MEMORY"]
        ):
            raise MacRuntimeContractError("mac_runtime_contract_invalid")

        trino_properties = _properties(root / "trino" / "config.properties")
        if trino_properties.get("spill-enabled", "").lower() != "true":
            raise MacRuntimeContractError("mac_runtime_contract_invalid")
        if (
            trino_properties.get("query.low-memory-killer.policy")
            != "total-reservation-on-blocked-nodes"
        ):
            raise MacRuntimeContractError("mac_runtime_contract_invalid")

        resource_groups = _mapping(
            json.loads(
                (root / "trino" / "resource-groups.json").read_text(
                    encoding="utf-8"
                )
            )
        )
        root_groups = resource_groups.get("rootGroups")
        if not isinstance(root_groups, list) or len(root_groups) != 1:
            raise MacRuntimeContractError("mac_runtime_contract_invalid")
        global_group = _mapping(root_groups[0])
        hard_concurrency = global_group.get("hardConcurrencyLimit")
        max_queued = global_group.get("maxQueued")
        if hard_concurrency != 1 or max_queued != 10:
            raise MacRuntimeContractError("mac_runtime_contract_invalid")

        ignore_lines = {
            line.strip()
            for line in (root / ".gitignore").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        if "weather-platform.prod.env" not in ignore_lines:
            raise MacRuntimeContractError("mac_runtime_contract_invalid")
    except MacRuntimeContractError:
        raise
    except (OSError, TypeError, ValueError, json.JSONDecodeError, yaml.YAMLError) as error:
        raise MacRuntimeContractError("mac_runtime_contract_invalid") from error

    return MacRuntimeContractProof(
        project_name="seoul-weather-platform-mac",
        network_name="seoul-weather-platform-mac-net",
        trino_container_mib=container_mib,
        trino_heap_mib=heap_mib,
        trino_query_and_headroom_mib=query_and_headroom_mib,
        hard_query_concurrency=hard_concurrency,
        max_queued_queries=max_queued,
        operational_lineage_disabled=True,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        validate_mac_runtime_contract(args.repo_root)
    except MacRuntimeContractError:
        print("mac_runtime_contract_invalid", file=sys.stderr)
        return 2
    print("mac_runtime_contract=passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
