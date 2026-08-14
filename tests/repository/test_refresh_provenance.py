from __future__ import annotations

from tools.refresh_provenance import build_repository_record, preserved_source_records


def test_contract_fixture_is_recorded_as_derived_evidence() -> None:
    record = build_repository_record(
        "contracts/weather-risk/fixtures/data-valid-empty.json",
        "a" * 64,
    )

    assert record["record_type"] == "derived"
    assert record["validator"] == "tests/contracts/test_weather_risk_contract.py"
    assert record["license_status"] == "internal_private_snapshot_only"


def test_repository_file_is_recorded_as_locally_authored() -> None:
    record = build_repository_record("README.md", "b" * 64)

    assert record == {
        "record_type": "local_authored",
        "target_path": "README.md",
        "target_sha256": "b" * 64,
        "scope": "repository_owned",
        "reason": "Repository-owned implementation, test, or documentation.",
        "license_status": "repository_owned_private",
        "owner": "masondev1024/seoul-weather-platform",
    }


def test_unclassified_dbt_or_airflow_source_cannot_be_claimed_as_local() -> None:
    for target in (
        "dags/domains/weather/unexpected.py",
        "dbt/domains/traffic_weather/models/weather/unexpected.sql",
    ):
        try:
            build_repository_record(target, "c" * 64)
        except ValueError as exc:
            assert "snapshot provenance" in str(exc)
        else:
            raise AssertionError(f"expected snapshot provenance failure for {target}")


def test_refresh_preserves_snapshot_and_reviewed_derived_overrides() -> None:
    records = [
        {"record_type": "snapshot_copy", "target_path": "dags/a.py"},
        {"record_type": "derived", "target_path": "dags/test_a.py"},
        {"record_type": "generated", "target_path": "release/map.json"},
        {"record_type": "local_authored", "target_path": "README.md"},
    ]

    assert preserved_source_records(records) == records[:3]
