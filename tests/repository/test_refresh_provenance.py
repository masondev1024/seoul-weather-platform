from __future__ import annotations

from tools.refresh_provenance import (
    HANDOFF_OVERLAY_VALIDATORS,
    HOST_TEST_PORTABILITY_ADAPTATIONS,
    KMA_OBSERVATION_ADAPTATIONS,
    MAC_CUTOVER_ADAPTATION_VALIDATORS,
    RETIRED_HANDOFF_PATHS,
    build_handoff_overlay_record,
    build_mac_cutover_adaptation_record,
    build_repository_record,
    preserved_source_records,
)


AUTHORIZED_LICENSE_STATUS = "public_republication_authorized"
PUBLIC_AUTHORIZATION_REASON_PREFIX = (
    "Public republication authorized by approved team-code republication "
    "decision dated 2026-08-21; source lineage and validators retained."
)


def test_contract_fixture_is_recorded_as_derived_evidence() -> None:
    record = build_repository_record(
        "contracts/weather-risk/fixtures/data-valid-empty.json",
        "a" * 64,
    )

    assert record["record_type"] == "derived"
    assert record["validator"] == "tests/contracts/test_weather_risk_contract.py"
    assert record["license_status"] == AUTHORIZED_LICENSE_STATUS


def test_repository_file_is_recorded_as_locally_authored() -> None:
    record = build_repository_record("README.md", "b" * 64)

    assert record == {
        "record_type": "local_authored",
        "target_path": "README.md",
        "target_sha256": "b" * 64,
        "scope": "repository_owned",
        "reason": (
            f"{PUBLIC_AUTHORIZATION_REASON_PREFIX} Prior provenance reason: "
            "Repository-owned implementation, test, or documentation."
        ),
        "license_status": AUTHORIZED_LICENSE_STATUS,
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


def test_observation_airflow_sources_are_explicitly_classified_as_local() -> None:
    for target in (
        "dags/domains/weather/weather_ingest/kma_coordination.py",
        "dags/domains/weather/weather_ingest/kma_observation.py",
        "dags/domains/weather/weather_ingest/kma_observation_bronze.py",
        "dags/domains/weather/weather_ingest/kma_observation_http.py",
        "dags/domains/weather/weather_ingest/kma_observation_landing.py",
        "dags/domains/weather/weather_ingest/kma_observation_runtime.py",
        "dags/domains/weather/weather_ultra_srt_ncst_bronze.py",
        "dags/domains/weather/tests/test_weather_kma_attempt_ledger.py",
        "dags/domains/weather/tests/test_weather_kma_coordination.py",
        "dags/domains/weather/tests/test_weather_kma_deadline.py",
        "dags/domains/weather/tests/test_weather_kma_observation.py",
        "dags/domains/weather/tests/test_weather_kma_observation_bronze.py",
        "dags/domains/weather/tests/test_weather_kma_observation_dag.py",
        "dags/domains/weather/tests/test_weather_kma_observation_landing.py",
        "dags/domains/weather/tests/test_weather_kma_retry_policy.py",
    ):
        record = build_repository_record(target, "c" * 64)
        assert record["record_type"] == "local_authored"
        assert record["target_path"] == target


def test_refresh_preserves_snapshot_and_reviewed_derived_overrides() -> None:
    records = [
        {"record_type": "snapshot_copy", "target_path": "dags/a.py"},
        {"record_type": "derived", "target_path": "dags/test_a.py"},
        {"record_type": "generated", "target_path": "release/map.json"},
        {"record_type": "local_authored", "target_path": "README.md"},
    ]

    assert preserved_source_records(records) == [
        {
            "record_type": "snapshot_copy",
            "target_path": "dags/a.py",
            "license_status": AUTHORIZED_LICENSE_STATUS,
            "reason": (
                f"{PUBLIC_AUTHORIZATION_REASON_PREFIX} Prior provenance reason: "
                "No prior provenance reason recorded."
            ),
        },
        {
            "record_type": "derived",
            "target_path": "dags/test_a.py",
            "license_status": AUTHORIZED_LICENSE_STATUS,
            "reason": (
                f"{PUBLIC_AUTHORIZATION_REASON_PREFIX} Prior provenance reason: "
                "No prior provenance reason recorded."
            ),
        },
        {
            "record_type": "generated",
            "target_path": "release/map.json",
            "license_status": AUTHORIZED_LICENSE_STATUS,
            "reason": (
                f"{PUBLIC_AUTHORIZATION_REASON_PREFIX} Prior provenance reason: "
                "No prior provenance reason recorded."
            ),
        },
    ]


def test_handoff_overlay_is_derived_with_authorized_public_republication() -> None:
    record = build_handoff_overlay_record(
        "dags/common/ops/run_sink.py",
        target_checksum="d" * 64,
        source_checksum="e" * 64,
    )

    assert record["record_type"] == "derived"
    assert record["target_sha256"] == "d" * 64
    assert record["license_status"] == AUTHORIZED_LICENSE_STATUS
    assert record["derived_from"] == {
        "source_repo": "ASAC-DE-bigkk/ASAC-DAG",
        "source_commit": "73ff5665ffd5526c59de8be2969cf65dffaf468b",
        "source_path": "common/ops/run_sink.py",
        "handoff": {
            "inventory": "provenance/weather-mac-handoff.sha256",
            "source_path": "dags/common/ops/run_sink.py",
            "source_sha256": "e" * 64,
        },
    }
    assert record["validator"] == HANDOFF_OVERLAY_VALIDATORS[
        "dags/common/ops/run_sink.py"
    ]


def test_handoff_overlay_allowlist_is_secret_free_and_complete() -> None:
    assert "weather-platform.prod.env" not in HANDOFF_OVERLAY_VALIDATORS
    assert len(HANDOFF_OVERLAY_VALIDATORS) == 24
    assert RETIRED_HANDOFF_PATHS == {
        "README-MAC.md",
        "docker-compose.mac.yml",
        "docker-compose.prod.yml",
        "marquez.prod.yml",
    }


def test_mac_cutover_adaptation_preserves_the_fixed_upstream_source() -> None:
    source_record = {
        "record_type": "snapshot_copy",
        "target_path": "dags/domains/weather/weather_dbt_runtime.py",
        "target_sha256": "a" * 64,
        "source_repo": "ASAC-DE-bigkk/ASAC-DAG",
        "source_commit": "b" * 40,
        "source_ref": "origin/dev",
        "source_path": "domains/weather/weather_dbt_runtime.py",
        "source_blob_oid": "c" * 40,
        "source_content_sha256": "d" * 64,
        "scope": "airflow_weather_dependency",
        "reason": "Weather runtime dependency",
        "license_status": AUTHORIZED_LICENSE_STATUS,
    }

    record = build_mac_cutover_adaptation_record(
        "dags/domains/weather/weather_dbt_runtime.py",
        target_checksum="e" * 64,
        source_record=source_record,
    )

    assert record["record_type"] == "derived"
    assert record["target_sha256"] == "e" * 64
    assert record["license_status"] == AUTHORIZED_LICENSE_STATUS
    assert record["derived_from"] == {
        "record_type": "snapshot_copy",
        "target_path": "dags/domains/weather/weather_dbt_runtime.py",
        "target_sha256": "a" * 64,
        "upstream": {
            "source_repo": "ASAC-DE-bigkk/ASAC-DAG",
            "source_commit": "b" * 40,
            "source_ref": "origin/dev",
            "source_path": "domains/weather/weather_dbt_runtime.py",
            "source_blob_oid": "c" * 40,
            "source_content_sha256": "d" * 64,
        },
    }
    assert record["validator"] == MAC_CUTOVER_ADAPTATION_VALIDATORS[
        "dags/domains/weather/weather_dbt_runtime.py"
    ]


def test_mac_cutover_adaptation_allowlist_is_explicit_and_secret_free() -> None:
    assert len(MAC_CUTOVER_ADAPTATION_VALIDATORS) == 16
    assert "weather-platform.prod.env" not in MAC_CUTOVER_ADAPTATION_VALIDATORS


def test_kma_observation_adaptations_preserve_inherited_source_lineage() -> None:
    assert KMA_OBSERVATION_ADAPTATIONS == {
        "dags/common/assets.py",
        "dags/common/pools.py",
        "dags/common/tests/test_pools.py",
        "dags/domains/weather/tests/test_weather_runtime_http.py",
        "dags/domains/weather/weather_ingest/common/runtime.py",
    }
    target = "dags/common/pools.py"
    source_record = {
        "record_type": "snapshot_copy",
        "target_path": target,
        "target_sha256": "a" * 64,
        "source_repo": "ASAC-DE-bigkk/ASAC-DAG",
        "source_commit": "b" * 40,
        "source_path": "common/pools.py",
        "source_content_sha256": "c" * 64,
        "license_status": AUTHORIZED_LICENSE_STATUS,
    }

    record = build_mac_cutover_adaptation_record(
        target,
        target_checksum="d" * 64,
        source_record=source_record,
    )

    assert record["scope"] == "kma_observation_coordination"
    assert "shared" in record["reason"].lower()
    assert record["derived_from"]["target_sha256"] == "a" * 64


def test_host_test_portability_adaptation_is_explicit() -> None:
    assert HOST_TEST_PORTABILITY_ADAPTATIONS == {
        "dags/domains/weather/tests/test_weather_transform_execution.py"
    }


def test_current_outlook_grace_is_a_reviewed_availability_adaptation() -> None:
    target = (
        "dbt/domains/traffic_weather/models/weather/transform/gold/"
        "gold_weather_place_current_outlook.yml"
    )
    source_record = {
        "record_type": "snapshot_copy",
        "target_path": target,
        "target_sha256": "a" * 64,
        "source_repo": "ASAC-DE-bigkk/ASAC-DBT",
        "source_commit": "b" * 40,
        "source_path": target.removeprefix("dbt/"),
        "source_content_sha256": "c" * 64,
        "license_status": AUTHORIZED_LICENSE_STATUS,
    }

    record = build_mac_cutover_adaptation_record(
        target,
        target_checksum="d" * 64,
        source_record=source_record,
    )

    assert record["scope"] == "weather_serving_availability"
    assert "hour-boundary" in record["reason"]
    assert "30-minute" in record["derivation"]
    assert record["validator"] == MAC_CUTOVER_ADAPTATION_VALIDATORS[target]


def test_place_mart_memory_adaptation_preserves_upstream_and_explains_the_bound() -> None:
    target = (
        "dbt/domains/traffic_weather/models/weather/transform/place_mart/"
        "silver_weather_forecast_by_admin_dong_serving.sql"
    )
    source_record = {
        "record_type": "snapshot_copy",
        "target_path": target,
        "target_sha256": "a" * 64,
        "source_repo": "ASAC-DE-bigkk/ASAC-DBT",
        "source_commit": "b" * 40,
        "source_path": target.removeprefix("dbt/"),
        "source_content_sha256": "c" * 64,
        "license_status": AUTHORIZED_LICENSE_STATUS,
    }

    record = build_mac_cutover_adaptation_record(
        target,
        target_checksum="d" * 64,
        source_record=source_record,
    )

    assert record["scope"] == "weather_mac_memory_optimization"
    assert "bounded Trino memory" in record["reason"]
    assert "MERGE" in record["derivation"]
    assert record["derived_from"]["target_sha256"] == "a" * 64


def test_new_place_mart_issue_window_test_is_repository_owned() -> None:
    target = (
        "dbt/domains/traffic_weather/tests/weather/transform/place_mart/"
        "assert_silver_weather_admin_dong_serving_issue_window.sql"
    )

    record = build_repository_record(target, "e" * 64)

    assert record["record_type"] == "local_authored"
    assert record["owner"] == "masondev1024/seoul-weather-platform"


def test_forecast_quality_sources_require_explicit_local_authorship() -> None:
    targets = (
        "dags/domains/weather/weather_quality_runtime.py",
        "dags/domains/weather/tests/test_weather_quality_runtime.py",
        "dags/domains/weather/weather_quality_publication.py",
        "dags/domains/weather/weather_quality_dag_factory.py",
        "dags/domains/weather/weather_forecast_quality_daily.py",
        "dags/domains/weather/weather_forecast_quality_backfill.py",
        "dags/domains/weather/tests/test_weather_quality_publication.py",
        "dags/domains/weather/tests/test_weather_forecast_quality_dags.py",
        "dags/domains/weather/tests/test_weather_dbt_runtime_contract.py",
        "dbt/domains/traffic_weather/macros/weather/weather_quality_contract.sql",
        "dbt/domains/traffic_weather/models/weather/quality/"
        "silver/silver_weather_forecast_observation_match.sql",
        "dbt/domains/traffic_weather/models/weather/quality/"
        "gold/gold_weather_forecast_quality_grid_score_history.sql",
        "dbt/domains/traffic_weather/models/weather/quality/"
        "gold/gold_weather_forecast_quality_hourly_history.sql",
        "dbt/domains/traffic_weather/models/weather/quality/"
        "gold/gold_weather_forecast_quality_hourly.sql",
        "dbt/domains/traffic_weather/models/weather/quality/"
        "gold/gold_weather_forecast_quality_daily_history.sql",
        "dbt/domains/traffic_weather/models/weather/quality/"
        "gold/gold_weather_forecast_quality_daily.sql",
        "dbt/domains/traffic_weather/tests/weather/quality/"
        "assert_quality_grid_score_reconciles.sql",
        "dbt/domains/traffic_weather/tests/weather/quality/"
        "assert_quality_hourly_reconciles.sql",
        "dbt/domains/traffic_weather/tests/weather/quality/"
        "assert_quality_daily_reconciles.sql",
    )

    for target in targets:
        record = build_repository_record(target, "f" * 64)
        assert record["record_type"] == "local_authored"
        assert record["owner"] == "masondev1024/seoul-weather-platform"


def test_mac_cutover_adaptation_is_idempotent_after_reclassification() -> None:
    source_record = {
        "record_type": "snapshot_copy",
        "target_path": "dags/domains/weather/weather_dbt_runtime.py",
        "target_sha256": "a" * 64,
        "source_repo": "ASAC-DE-bigkk/ASAC-DAG",
        "source_commit": "b" * 40,
        "source_path": "domains/weather/weather_dbt_runtime.py",
        "source_content_sha256": "c" * 64,
        "scope": "airflow_weather_dependency",
        "reason": "Weather runtime dependency",
        "license_status": "internal_private_snapshot_only",
    }
    first = build_mac_cutover_adaptation_record(
        "dags/domains/weather/weather_dbt_runtime.py",
        target_checksum="d" * 64,
        source_record=source_record,
    )

    second = build_mac_cutover_adaptation_record(
        "dags/domains/weather/weather_dbt_runtime.py",
        target_checksum="d" * 64,
        source_record=first,
    )

    assert second == first
