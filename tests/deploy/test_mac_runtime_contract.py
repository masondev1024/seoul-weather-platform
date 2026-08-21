from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
AIRFLOW_MAC_SERVICES = (
    "airflow-init",
    "airflow-apiserver",
    "airflow-scheduler",
    "airflow-dag-processor",
    "airflow-triggerer",
)


class _ComposeLoader(yaml.SafeLoader):
    pass


_ComposeLoader.add_constructor("!reset", lambda loader, node: None)


def _load_mac_compose(path: Path) -> dict[str, object]:
    return yaml.load(path.read_text(encoding="utf-8"), Loader=_ComposeLoader)


def _run_validator(repo_root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.mac_runtime_contract",
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
        + "    environment:\n"
        '      AIRFLOW__OPENLINEAGE__DISABLED: "true"\n'
        '      ASK_SEOUL_DBT_OPENLINEAGE_ENABLED: "false"\n'
        '      ASK_SEOUL_KMA_DAG_SCHEDULE: ""\n'
        '      ASK_SEOUL_WEATHER_SERVING_SNAPSHOT_DAG_SCHEDULE: ""\n'
        '      KMA_NUM_OF_ROWS: "2000"'
        for service in AIRFLOW_MAC_SERVICES
    )
    (repo_root / "docker-compose.mac.yml").write_text(
        "name: seoul-weather-platform-mac\n"
        "networks:\n"
        "  elt_net:\n"
        "    name: seoul-weather-platform-mac-net\n"
        "services:\n"
        "  trino:\n"
        "    mem_limit: 5g\n"
        "    environment:\n"
        '      TRINO_TASK_CONCURRENCY: "2"\n'
        '      TRINO_QUERY_MAX_MEMORY_PER_NODE: "800MB"\n'
        '      TRINO_MEMORY_HEAP_HEADROOM_PER_NODE: "1500MB"\n'
        '      TRINO_QUERY_MAX_MEMORY: "800MB"\n'
        '      TRINO_QUERY_MAX_TOTAL_MEMORY: "1200MB"\n'
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


def test_repository_mac_runtime_contract_is_safe() -> None:
    """An unsafe handoff or later memory/lineage drift must fail L0 validation."""
    result = _run_validator(REPO_ROOT)

    assert result.returncode == 0, result.stderr
    assert result.stdout == "mac_runtime_contract=passed\n"


def test_repository_mac_runtime_contract_uses_a_dedicated_network() -> None:
    compose = _load_mac_compose(REPO_ROOT / "docker-compose.mac.yml")

    assert compose["networks"]["elt_net"]["name"] == (
        "seoul-weather-platform-mac-net"
    )


def test_repository_builds_the_shared_airflow_image_exactly_once() -> None:
    compose = _load_mac_compose(REPO_ROOT / "docker-compose.mac.yml")
    build_owners = sorted(
        service_name
        for service_name, service in compose["services"].items()
        if service.get("build") is not None
    )
    reset_consumers = sorted(
        service_name
        for service_name, service in compose["services"].items()
        if service_name.startswith("airflow-") and service.get("build") is None
        and "build" in service
    )

    assert build_owners == ["airflow-init"]
    assert reset_consumers == [
        "airflow-apiserver",
        "airflow-dag-processor",
        "airflow-scheduler",
        "airflow-triggerer",
    ]


def test_repository_keeps_a_strict_query_budget_for_the_80_grid_mart() -> None:
    """The bounded place mart must fit the original fail-fast Mac memory budget."""
    compose = _load_mac_compose(REPO_ROOT / "docker-compose.mac.yml")
    environment = compose["services"]["trino"]["environment"]

    assert environment["TRINO_QUERY_MAX_MEMORY_PER_NODE"] == "800MB"
    assert environment["TRINO_QUERY_MAX_MEMORY"] == "800MB"
    assert environment["TRINO_QUERY_MAX_TOTAL_MEMORY"] == "1200MB"
    assert environment["TRINO_MEMORY_HEAP_HEADROOM_PER_NODE"] == "1500MB"


def test_repository_disables_the_recurring_kma_schedule_on_mac() -> None:
    """Mac cutover must require an explicit operator trigger for every KMA run."""
    compose = _load_mac_compose(REPO_ROOT / "docker-compose.mac.yml")

    for service_name in AIRFLOW_MAC_SERVICES:
        assert compose["services"][service_name]["environment"][
            "ASK_SEOUL_KMA_DAG_SCHEDULE"
        ] == ""


def test_repository_disables_the_recurring_serving_snapshot_schedule_on_mac() -> None:
    """Mac cutover must not create hourly serving refresh runs implicitly."""
    compose = _load_mac_compose(REPO_ROOT / "docker-compose.mac.yml")

    for service_name in AIRFLOW_MAC_SERVICES:
        assert compose["services"][service_name]["environment"][
            "ASK_SEOUL_WEATHER_SERVING_SNAPSHOT_DAG_SCHEDULE"
        ] == ""


def test_repository_uses_one_kma_page_per_seoul_grid_on_mac() -> None:
    """The verified 2,000-row page removes the second request for each grid."""
    compose = _load_mac_compose(REPO_ROOT / "docker-compose.mac.yml")

    for service_name in AIRFLOW_MAC_SERVICES:
        assert compose["services"][service_name]["environment"][
            "KMA_NUM_OF_ROWS"
        ] == "2000"


def test_validator_accepts_a_conservative_memory_and_lineage_contract(
    tmp_path: Path,
) -> None:
    _write_valid_contract(tmp_path)

    result = _run_validator(tmp_path)

    assert result.returncode == 0, result.stderr
    assert result.stdout == "mac_runtime_contract=passed\n"


def test_validator_rejects_query_memory_that_exceeds_the_heap_budget(
    tmp_path: Path,
) -> None:
    _write_valid_contract(tmp_path)
    compose_path = tmp_path / "docker-compose.mac.yml"
    compose_path.write_text(
        compose_path.read_text(encoding="utf-8").replace(
            'TRINO_MEMORY_HEAP_HEADROOM_PER_NODE: "1500MB"',
            'TRINO_MEMORY_HEAP_HEADROOM_PER_NODE: "2200MB"',
        ),
        encoding="utf-8",
    )

    result = _run_validator(tmp_path)

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "mac_runtime_contract_invalid\n"


def test_validator_rejects_reenabled_operational_lineage(tmp_path: Path) -> None:
    _write_valid_contract(tmp_path)
    compose_path = tmp_path / "docker-compose.mac.yml"
    compose_path.write_text(
        compose_path.read_text(encoding="utf-8").replace(
            'AIRFLOW__OPENLINEAGE__DISABLED: "true"',
            'AIRFLOW__OPENLINEAGE__DISABLED: "false"',
            1,
        ),
        encoding="utf-8",
    )

    result = _run_validator(tmp_path)

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "mac_runtime_contract_invalid\n"


def test_validator_rejects_a_recurring_kma_schedule(tmp_path: Path) -> None:
    _write_valid_contract(tmp_path)
    compose_path = tmp_path / "docker-compose.mac.yml"
    compose_path.write_text(
        compose_path.read_text(encoding="utf-8").replace(
            '      ASK_SEOUL_KMA_DAG_SCHEDULE: ""',
            '      ASK_SEOUL_KMA_DAG_SCHEDULE: "20 2,5,8,11,14,17,20,23 * * *"',
        ),
        encoding="utf-8",
    )

    result = _run_validator(tmp_path)

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "mac_runtime_contract_invalid\n"


def test_validator_rejects_a_recurring_serving_snapshot_schedule(
    tmp_path: Path,
) -> None:
    _write_valid_contract(tmp_path)
    compose_path = tmp_path / "docker-compose.mac.yml"
    compose_path.write_text(
        compose_path.read_text(encoding="utf-8").replace(
            '      ASK_SEOUL_WEATHER_SERVING_SNAPSHOT_DAG_SCHEDULE: ""',
            '      ASK_SEOUL_WEATHER_SERVING_SNAPSHOT_DAG_SCHEDULE: "0 * * * *"',
        ),
        encoding="utf-8",
    )

    result = _run_validator(tmp_path)

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "mac_runtime_contract_invalid\n"


def test_validator_rejects_the_two_page_kma_setting(tmp_path: Path) -> None:
    _write_valid_contract(tmp_path)
    compose_path = tmp_path / "docker-compose.mac.yml"
    compose_path.write_text(
        compose_path.read_text(encoding="utf-8").replace(
            '      KMA_NUM_OF_ROWS: "2000"',
            '      KMA_NUM_OF_ROWS: "1000"',
        ),
        encoding="utf-8",
    )

    result = _run_validator(tmp_path)

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "mac_runtime_contract_invalid\n"


def test_validator_rejects_a_shared_or_inherited_network_name(tmp_path: Path) -> None:
    _write_valid_contract(tmp_path)
    compose_path = tmp_path / "docker-compose.mac.yml"
    compose_path.write_text(
        compose_path.read_text(encoding="utf-8").replace(
            "seoul-weather-platform-mac-net",
            "elt-infra-prod-net",
        ),
        encoding="utf-8",
    )

    result = _run_validator(tmp_path)

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "mac_runtime_contract_invalid\n"


def test_validator_rejects_multiple_airflow_build_owners(tmp_path: Path) -> None:
    _write_valid_contract(tmp_path)
    compose_path = tmp_path / "docker-compose.mac.yml"
    compose_path.write_text(
        compose_path.read_text(encoding="utf-8").replace(
            "  airflow-apiserver:\n"
            "    image: ask-seoul-weather-airflow:mac-local\n"
            "    build: !reset null\n",
            "  airflow-apiserver:\n"
            "    image: ask-seoul-weather-airflow:mac-local\n"
            "    build:\n"
            "      context: .\n"
            "      dockerfile: Dockerfile.airflow\n",
        ),
        encoding="utf-8",
    )

    result = _run_validator(tmp_path)

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "mac_runtime_contract_invalid\n"


def test_validator_rejects_a_missing_airflow_build_reset(tmp_path: Path) -> None:
    _write_valid_contract(tmp_path)
    compose_path = tmp_path / "docker-compose.mac.yml"
    compose_path.write_text(
        compose_path.read_text(encoding="utf-8").replace(
            "  airflow-apiserver:\n"
            "    image: ask-seoul-weather-airflow:mac-local\n"
            "    build: !reset null\n",
            "  airflow-apiserver:\n"
            "    image: ask-seoul-weather-airflow:mac-local\n",
        ),
        encoding="utf-8",
    )

    result = _run_validator(tmp_path)

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "mac_runtime_contract_invalid\n"
