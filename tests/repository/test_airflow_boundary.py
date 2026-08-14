from __future__ import annotations

import json
from pathlib import Path

from tools.verify_airflow_boundary import (
    EXPECTED_ENTRYPOINTS,
    entrypoint_errors,
    find_forbidden_imports,
    inventory_manifest_errors,
    verify_airflow_boundary,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def test_checked_in_airflow_snapshot_satisfies_static_boundary() -> None:
    assert verify_airflow_boundary(REPOSITORY_ROOT) == []


def test_airflow_scan_ignores_import_packages_and_test_modules() -> None:
    assert (REPOSITORY_ROOT / "dags/common/.airflowignore").read_text(
        encoding="utf-8"
    ).strip().endswith("**")
    weather_ignore = (
        REPOSITORY_ROOT / "dags/domains/weather/.airflowignore"
    ).read_text(encoding="utf-8")

    assert {"weather_ingest/**", "docs/**", "config/**", "tests/**"} <= set(
        weather_ignore.splitlines()
    )


def test_entrypoint_contract_rejects_mixed_maintenance_lane() -> None:
    entries = [
        {"scope": "airflow_weather_entrypoint", "source_path": path}
        for path in EXPECTED_ENTRYPOINTS
    ]
    entries.append(
        {
            "scope": "airflow_weather_entrypoint",
            "source_path": "domains/weather/weather_iceberg_maintenance.py",
        }
    )

    errors = entrypoint_errors(entries)

    assert errors == [
        "airflow entrypoint set differs from the required eight Weather lanes: "
        "unexpected domains/weather/weather_iceberg_maintenance.py"
    ]


def test_import_scan_rejects_traffic_and_cost_proxy_dependencies(tmp_path: Path) -> None:
    candidate = tmp_path / "dags" / "weather_lane.py"
    candidate.parent.mkdir()
    candidate.write_text(
        "from domains.traffic import traffic_serving_export\n"
        "from weather_ingest.cost_proxy import compile\n",
        encoding="utf-8",
    )

    errors = find_forbidden_imports([candidate])

    assert errors == [
        "forbidden mixed-domain import in dags/weather_lane.py: domains.traffic",
        "forbidden mixed-domain import in dags/weather_lane.py: "
        "weather_ingest.cost_proxy",
    ]


def test_inventory_manifest_contract_rejects_missing_snapshot_record() -> None:
    entries = [
        {
            "source_id": "airflow_weather",
            "source_path": "domains/weather/weather_serving_export.py",
            "target_path": "dags/domains/weather/weather_serving_export.py",
        }
    ]
    records: list[dict[str, object]] = []

    errors = inventory_manifest_errors(entries, records, "a" * 40)

    assert errors == [
        "missing manifest record for airflow inventory target: "
        "dags/domains/weather/weather_serving_export.py"
    ]


def test_inventory_manifest_contract_accepts_derived_airflow_record() -> None:
    entries = [
        {
            "source_id": "airflow_weather",
            "source_path": "common/serving/tests/test_dag_factory.py",
            "target_path": "dags/common/serving/tests/test_dag_factory.py",
        }
    ]
    records = [
        {
            "record_type": "derived",
            "target_path": "dags/common/serving/tests/test_dag_factory.py",
            "derived_from": {
                "source_commit": "a" * 40,
                "source_path": "common/serving/tests/test_dag_factory.py",
            },
        }
    ]

    assert inventory_manifest_errors(entries, records, "a" * 40) == []
