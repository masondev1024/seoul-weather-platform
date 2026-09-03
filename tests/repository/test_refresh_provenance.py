from __future__ import annotations

from tools.refresh_provenance import (
    build_derived_override_record,
    build_repository_record,
    preserved_source_records,
)


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


def test_weather_quality_sources_are_explicitly_repository_owned() -> None:
    for target in (
        "dags/domains/weather/weather_forecast_quality_daily.py",
        "dags/domains/weather/weather_quality_publication.py",
        "dbt/domains/traffic_weather/macros/weather/weather_quality_contract.sql",
        "dbt/domains/traffic_weather/models/weather/quality/gold/gold_weather_forecast_quality_daily.sql",
    ):
        record = build_repository_record(target, "d" * 64)

        assert record["record_type"] == "local_authored"
        assert record["scope"] == "repository_owned"


def test_snapshot_override_keeps_upstream_lineage_as_derived() -> None:
    source_record = {
        "record_type": "snapshot_copy",
        "source_repo": "upstream/weather",
        "source_ref": "origin/dev",
        "source_commit": "a" * 40,
        "source_path": "common/pools.py",
        "source_blob_oid": "b" * 40,
        "source_content_sha256": "c" * 64,
    }

    record = build_derived_override_record(
        "dags/common/pools.py",
        "d" * 64,
        source_record=source_record,
    )

    assert record == {
        "record_type": "derived",
        "target_path": "dags/common/pools.py",
        "target_sha256": "d" * 64,
        "scope": "repository_owned",
        "reason": "Repository-owned Weather adaptation of a fixed upstream snapshot.",
        "license_status": "internal_private_snapshot_only",
        "derived_from": {
            "source_repo": "upstream/weather",
            "source_ref": "origin/dev",
            "source_commit": "a" * 40,
            "source_path": "common/pools.py",
            "source_blob_oid": "b" * 40,
            "source_content_sha256": "c" * 64,
        },
        "derivation": "Repository-owned Weather quality adaptation; see the reviewed git diff and focused tests.",
        "validator": "python -m pytest dags/common/tests dags/domains/weather/tests tests/forecast_quality -q",
    }


def test_refresh_preserves_snapshot_and_reviewed_derived_overrides() -> None:
    records = [
        {"record_type": "snapshot_copy", "target_path": "dags/a.py"},
        {"record_type": "derived", "target_path": "dags/test_a.py"},
        {"record_type": "generated", "target_path": "release/map.json"},
        {"record_type": "local_authored", "target_path": "README.md"},
    ]

    assert preserved_source_records(records) == records[:3]
