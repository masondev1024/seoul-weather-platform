"""Behavioral oracle for the Serving Contract v1 validator.

Valid fixtures must PASS; invalid fixtures must FAIL with the specific expected
rules. CLI exit codes 0/1/2 are asserted directly.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from serving_contract.cli import _render_json, main
from serving_contract.model import ManifestView, ServingModel, load_manifest, load_models_from_yaml
from serving_contract.validator import validate

FIXTURES = Path(__file__).parent / "fixtures"
REPO_ROOT = Path(__file__).resolve().parents[2]
VALID = FIXTURES / "valid_contracts.yml"
INVALID = FIXTURES / "invalid_contracts.yml"
NOT_IN_MANIFEST = FIXTURES / "not_in_manifest.yml"
MANIFEST = FIXTURES / "manifest.json"
CITYDATA_GOLD_MODELS = REPO_ROOT / "domains/citydata/models/gold/_citydata_gold__models.yml"
COMMERCE_GOLD_MODELS = REPO_ROOT / "domains/commerce/models/gold/_commerce_gold__models.yml"
COMMERCE_DATASET_TAXONOMY = REPO_ROOT / "domains/commerce/seeds/commerce_dataset_taxonomy.csv"
CULTURE_GOLD_MODELS = REPO_ROOT / "domains/culture/models/gold/_culture_gold__models.yml"
TRANSIT_SCHEMA = REPO_ROOT / "domains/transit/models/schema.yml"


def _all_exist(*paths: Path) -> bool:
    return all(path.exists() for path in paths)


def _rules(findings) -> set[str]:
    return {f.rule for f in findings}


def _serving_model(
    *,
    name: str = "gold_projection_fixture",
    serving_overrides: dict | None = None,
    columns: dict | None = None,
    column_contracts: dict | None = None,
) -> ServingModel:
    serving = {
        "enabled": True,
        "external": True,
        "product_id": name.removeprefix("gold_"),
        "product_question": "projection test question",
        "grain": "one row per id",
        "primary_key": ["product_row_id"],
        "publication_mode": "snapshot",
        "zero_policy": "fail",
        "publication_trigger": {"schedule_cron": "0 * * * *"},
    }
    serving.update(serving_overrides or {})
    return ServingModel(
        name=name,
        source="fixture.yml",
        meta={},
        serving=serving,
        columns=columns
        or {
            "product_row_id": ("not_null", "unique"),
            "event_at": (),
            "collected_at": (),
            "sample_count": (),
            "public_value": (),
        },
        column_contracts=column_contracts or {},
    )


def test_valid_contracts_pass_with_manifest():
    models = load_models_from_yaml([VALID])
    result = validate(models, load_manifest(MANIFEST))
    assert result.ok, [f.as_dict() for f in result.findings]
    assert result.models_checked == 2


def test_valid_contracts_pass_without_manifest():
    models = load_models_from_yaml([VALID])
    result = validate(models)  # no manifest => membership/column checks fall back to yml
    assert result.ok, [f.as_dict() for f in result.findings]


def test_invalid_contracts_fail_with_expected_rules():
    models = load_models_from_yaml([INVALID])
    result = validate(models)
    assert not result.ok
    rules = _rules(result.findings)
    expected = {
        "required_field_missing",
        "external_enabled_conflict",
        "invalid_enum_value",
        "primary_key_not_a_column",
        "primary_key_evidence_missing",
        "excluded_field_present",
        "legacy_double_declaration",
        "publication_trigger_invalid",
        "invalid_field_format",
        "partial_policy_invalid",
        "reliability_invalid",
        "product_id_duplicate",
        "conditional_required_missing",
        "usage_pattern_required_missing",
        "usage_pattern_unknown_field",
        "usage_pattern_requires_unknown",
        "usage_pattern_duplicate",
        "usage_pattern_invalid",
        "public_projection_invalid",
        "public_projection_required_field_missing",
        "public_projection_unknown_column",
        "public_projection_internal_field",
    }
    missing = expected - rules
    assert not missing, f"expected rules not raised: {missing}"


def test_column_vocabulary_declarations_raise_specific_rules():
    """Reject malformed or contradictory code-vocabulary metadata."""
    result = validate(load_models_from_yaml([INVALID]))

    assert {
        "column_vocabulary_id_invalid",
        "column_vocabulary_terms_without_id",
        "column_vocabulary_term_invalid",
        "column_vocabulary_term_duplicate",
        "column_vocabulary_terms_conflict",
    } <= _rules(result.findings)


@pytest.mark.parametrize(
    "model_name,rule",
    [
        ("bad_missing_required", "required_field_missing"),
        ("bad_external_conflict", "external_enabled_conflict"),
        ("bad_enum", "invalid_enum_value"),
        ("bad_primary_key", "primary_key_not_a_column"),
        ("bad_excluded", "excluded_field_present"),
        ("bad_double_decl", "legacy_double_declaration"),
        ("bad_trigger", "publication_trigger_invalid"),
        ("bad_pid_format", "invalid_field_format"),
        ("bad_partial", "partial_policy_invalid"),
        ("bad_reliability", "reliability_invalid"),
        ("bad_missing_freshness_slo", "conditional_required_missing"),
        ("bad_usage_patterns", "usage_pattern_required_missing"),
        ("bad_usage_patterns", "usage_pattern_unknown_field"),
        ("bad_usage_patterns", "usage_pattern_requires_unknown"),
        ("bad_usage_patterns", "usage_pattern_duplicate"),
        ("bad_usage_patterns", "usage_pattern_invalid"),
    ],
)
def test_specific_rule_attaches_to_model(model_name, rule):
    models = load_models_from_yaml([INVALID])
    result = validate(models)
    hits = [f for f in result.findings if f.model == model_name and f.rule == rule]
    assert hits, f"{model_name} should raise {rule}; got {[f.as_dict() for f in result.findings if f.model == model_name]}"


def test_model_not_in_manifest_only_with_manifest():
    models = load_models_from_yaml([NOT_IN_MANIFEST])
    # Without a manifest the rule must not fire.
    assert "model_not_in_manifest" not in _rules(validate(models).findings)
    # With a manifest that lacks the model, it fires.
    result = validate(models, load_manifest(MANIFEST))
    assert "model_not_in_manifest" in _rules(result.findings)


def test_upsert_strategy_requires_upsert_publication_mode():
    model = ServingModel(
        name="bad_strategy",
        source="test",
        meta={},
        serving={
            "enabled": True,
            "external": False,
            "product_id": "bad_strategy",
            "product_question": "question",
            "grain": "one row per id",
            "primary_key": ["id"],
            "publication_mode": "snapshot",
            "upsert_strategy": "exact_set",
            "zero_policy": "allow",
            "publication_trigger": {"schedule_cron": "0 * * * *"},
        },
        columns={"id": ("not_null", "unique")},
    )

    assert "upsert_strategy_invalid" in _rules(validate([model]).findings)


def test_cli_exit_codes():
    assert main(["--source", str(VALID), "--manifest", str(MANIFEST)]) == 0  # PASS
    assert main(["--source", str(INVALID)]) == 1  # FAIL
    assert main(["--source", str(FIXTURES / "does_not_exist_*.yml")]) == 2  # ERROR (no match)


def test_json_report_is_deterministic_utf8():
    result = validate(load_models_from_yaml([INVALID]))
    first = _render_json(result)
    second = _render_json(result)
    assert first == second  # sorted keys + sorted findings => byte-stable
    assert first.encode("utf-8")  # Korean messages encode cleanly
    assert "product_id_duplicate" in first


@pytest.mark.parametrize(
    "projection",
    [
        {"schema_version": "1.0", "columns": ["product_row_id"]},
        {"schema_version": "1.0.0", "columns": []},
        {"schema_version": "1.0.0", "columns": ["product_row_id", "product_row_id"]},
        {"schema_version": "1.0.0", "columns": ["product_row_id"], "rename_map": {}},
        {"schema_version": "1.0.0", "columns": ["product_row_id as id"]},
    ],
)
def test_public_projection_rejects_malformed_contracts(projection):
    model = _serving_model(serving_overrides={"public_projection": projection})

    result = validate([model])

    assert "public_projection_invalid" in _rules(result.findings)


def test_public_projection_requires_primary_event_and_reliability_columns():
    model = _serving_model(
        serving_overrides={
            "event_time": "event_at",
            "freshness_slo_minutes": 60,
            "reliability": {
                "sample_count_field": "sample_count",
                "minimum_sample_count": 3,
                "insufficient_sample_policy": "flag_degraded",
            },
            "public_projection": {
                "schema_version": "1.0.0",
                "columns": ["public_value"],
            },
        }
    )

    result = validate([model])

    assert "public_projection_required_field_missing" in _rules(result.findings)


def test_public_projection_requires_explicit_freshness_field():
    model = _serving_model(
        serving_overrides={
            "event_time": "event_at",
            "freshness_field": "collected_at",
            "freshness_slo_minutes": 60,
            "public_projection": {
                "schema_version": "1.0.0",
                "columns": ["product_row_id", "event_at"],
            },
        }
    )

    result = validate([model])

    assert any(
        finding.rule == "public_projection_required_field_missing"
        and "collected_at" in finding.message
        for finding in result.findings
    )


def test_freshness_field_must_be_a_model_column():
    model = _serving_model(
        serving_overrides={
            "freshness_field": "missing_collected_at",
            "freshness_slo_minutes": 60,
        }
    )

    result = validate([model])

    assert "freshness_field_not_a_column" in _rules(result.findings)


def test_empty_result_freshness_requires_a_manifest_relation_and_physical_field():
    model = _serving_model(
        serving_overrides={
            "empty_result_freshness": {
                "relation": "gold_missing_hourly_outlook",
                "field": "forecast_collected_at_max",
            },
            "freshness_slo_minutes": 60,
        }
    )
    manifest = ManifestView(
        columns_by_model={
            "gold_projection_fixture": {"product_row_id", "event_at", "collected_at"},
        },
        supplied=True,
    )

    result = validate([model], manifest)

    assert "empty_result_freshness_invalid" in _rules(result.findings)


def test_query_availability_requires_exactly_one_other_manifest_relation():
    model = _serving_model(
        serving_overrides={
            "query_availability": {
                "relation": "gold_weather_place_risk_query_availability",
                "field": "availability_status",
            }
        }
    )
    manifest = ManifestView(
        columns_by_model={
            "gold_projection_fixture": {"product_row_id", "event_at", "collected_at"},
            "gold_weather_place_risk_query_availability": {"place_id"},
        },
        supplied=True,
    )

    assert "query_availability_invalid" in _rules(validate([model], manifest).findings)


def test_query_availability_accepts_manifest_backed_companion_relation():
    model = _serving_model(
        serving_overrides={
            "query_availability": {
                "relation": "gold_weather_place_risk_query_availability",
            }
        }
    )
    manifest = ManifestView(
        columns_by_model={
            "gold_projection_fixture": {"product_row_id", "event_at", "collected_at"},
            "gold_weather_place_risk_query_availability": {"place_id"},
        },
        supplied=True,
    )

    assert "query_availability_invalid" not in _rules(validate([model], manifest).findings)


def test_external_allow_zero_policy_requires_a_valid_empty_declaration():
    model = _serving_model(
        serving_overrides={
            "zero_policy": "allow",
            "freshness_slo_minutes": 60,
            "empty_result_freshness": {
                "relation": "gold_hourly_outlook",
                "field": "forecast_collected_at_max",
            },
        }
    )

    result = validate([model])

    assert "valid_empty_contract_invalid" in _rules(result.findings)

    model_without_freshness = _serving_model(
        serving_overrides={
            "zero_policy": "allow",
            "mcp_projection": {
                "empty_result": {
                    "state": "valid_empty",
                    "code": "no_upcoming_events",
                    "message_ko": "현재 유효한 입력에는 향후 이벤트가 없습니다.",
                }
            },
        }
    )

    assert "valid_empty_contract_invalid" in _rules(validate([model_without_freshness]).findings)


def test_external_allow_zero_policy_accepts_a_valid_empty_declaration():
    model = _serving_model(
        serving_overrides={
            "zero_policy": "allow",
            "freshness_slo_minutes": 60,
            "empty_result_freshness": {
                "relation": "gold_hourly_outlook",
                "field": "forecast_collected_at_max",
            },
            "mcp_projection": {
                "empty_result": {
                    "state": "valid_empty",
                    "code": "no_upcoming_events",
                    "message_ko": "현재 유효한 입력에는 향후 이벤트가 없습니다.",
                }
            },
        }
    )

    assert "valid_empty_contract_invalid" not in _rules(validate([model]).findings)


@pytest.mark.parametrize(
    "serving_overrides",
    [
        {"retire_on_publish": True},
        {"retire_on_publish": True, "enabled": False, "external": True},
    ],
)
def test_retire_on_publish_requires_a_disabled_non_external_contract(serving_overrides):
    model = _serving_model(serving_overrides=serving_overrides)

    result = validate([model])

    assert "retire_on_publish_invalid" in _rules(result.findings)


def test_retire_on_publish_accepts_a_disabled_non_external_contract():
    model = _serving_model(
        serving_overrides={
            "retire_on_publish": True,
            "enabled": False,
            "external": False,
        }
    )

    assert "retire_on_publish_invalid" not in _rules(validate([model]).findings)


def test_freshness_field_requires_freshness_slo():
    model = _serving_model(
        serving_overrides={"freshness_field": "collected_at"}
    )

    result = validate([model])

    assert any(
        finding.rule == "conditional_required_missing"
        and "freshness_field" in finding.message
        for finding in result.findings
    )


def test_public_projection_rejects_unknown_columns_with_or_without_manifest():
    model = _serving_model(
        serving_overrides={
            "public_projection": {
                "schema_version": "1.0.0",
                "columns": ["product_row_id", "ghost_column"],
            },
        }
    )

    result = validate([model])

    assert "public_projection_unknown_column" in _rules(result.findings)


def test_public_projection_rejects_internal_or_secret_columns():
    model = _serving_model(
        serving_overrides={
            "public_projection": {
                "schema_version": "1.0.0",
                "columns": ["product_row_id", "representative_dag_run_id", "api_token"],
            },
        },
        columns={
            "product_row_id": ("not_null", "unique"),
            "representative_dag_run_id": (),
            "api_token": (),
        },
    )

    result = validate([model])

    assert "public_projection_internal_field" in _rules(result.findings)


def test_public_projection_rejects_not_null_column_declared_nullable():
    model = _serving_model(
        serving_overrides={
            "public_projection": {
                "schema_version": "1.0.0",
                "columns": ["product_row_id"],
            },
        },
        columns={"product_row_id": ("not_null", "unique")},
        column_contracts={
            "product_row_id": {
                "name": "product_row_id",
                "description": "public row identifier",
                "data_type": "varchar",
                "config": {
                    "meta": {
                        "semantic_role": "primary_key",
                        "nullable": True,
                        "null_meaning": "null means the identifier is unavailable",
                        "unit": "not_applicable",
                    }
                },
            }
        },
    )

    result = validate([model])

    assert "public_projection_nullability_conflict" in _rules(result.findings)


def test_source_evidence_rejects_missing_or_ambiguous_rights_declaration():
    model = _serving_model(
        serving_overrides={
            "source_evidence": [
                {
                    "source_id": "kma_vilage_fcst",
                    "source_url": "http://not-secure.example.test/kma",
                    "license": "",
                    "license_url": "https://example.test/kogl",
                    "redistribution": "maybe",
                    "attribution": "",
                    "rights_checked_at": "2026/08/04",
                    "unexpected": "typo-must-not-pass",
                },
                {
                    "source_id": "kma_vilage_fcst",
                    "source_url": "https://example.test/duplicate",
                    "license": "KOGL-1",
                    "license_url": "https://example.test/kogl",
                    "redistribution": "allowed_with_attribution",
                    "attribution": "기상청",
                    "rights_checked_at": "2026-08-04",
                },
            ],
        }
    )

    result = validate([model])

    rules = _rules(result.findings)
    assert "source_evidence_invalid" in rules
    assert "source_evidence_unknown_field" in rules
    assert "source_evidence_duplicate" in rules


def test_quality_coverage_rejects_unknown_field_and_unachievable_threshold():
    model = _serving_model(
        serving_overrides={
            "quality_coverage": {
                "field": "missing_dimension",
                "expected_distinct_count": 0,
                "minimum_ratio": 1.2,
                "unexpected": "typo-must-not-pass",
            },
        }
    )

    result = validate([model])

    rules = _rules(result.findings)
    assert "quality_coverage_invalid" in rules
    assert "quality_coverage_unknown_field" in rules


def test_quality_coverage_allows_source_relation_measurement_outside_public_projection():
    model = _serving_model(
        serving_overrides={
            "public_projection": {
                "schema_version": "1.0.0",
                "columns": ["product_row_id", "event_at"],
            },
            "quality_coverage": {
                "field": "source_dimension",
                "expected_distinct_count": 152,
                "minimum_ratio": 1.0,
                "measurement_scope": "source_relation",
            },
        },
        columns={
            "product_row_id": ("not_null", "unique"),
            "event_at": (),
            "source_dimension": (),
        },
    )

    result = validate([model])

    assert not {"quality_coverage_invalid", "quality_coverage_unknown_field"} & _rules(result.findings)


def test_quality_coverage_allows_explicit_not_applicable_reason():
    model = _serving_model(
        serving_overrides={
            "quality_coverage": {
                "not_applicable_reason": "게시 모집단이 매 source run의 최근 유효 관측 집합으로 동적으로 정의됨",
            }
        }
    )

    result = validate([model])

    assert "quality_coverage_invalid" not in _rules(result.findings)


def test_public_projection_uses_public_primary_key_for_rollup_grain():
    model = _serving_model(
        serving_overrides={
            "primary_key": ["product_row_id", "source_dimension"],
            "public_primary_key": ["product_row_id"],
            "public_projection": {
                "schema_version": "1.0.0",
                "columns": ["product_row_id", "event_at"],
            },
        },
        columns={
            "product_row_id": ("not_null", "unique"),
            "source_dimension": ("not_null",),
            "event_at": (),
        },
    )

    result = validate([model])

    required_missing = [f for f in result.findings if f.rule == "public_projection_required_field_missing"]
    assert not required_missing


def test_public_primary_key_requires_public_projection():
    model = _serving_model(
        serving_overrides={
            "public_primary_key": ["product_row_id"],
        }
    )

    result = validate([model])

    assert "public_projection_invalid" in _rules(result.findings)


@pytest.mark.skipif(
    not _all_exist(CITYDATA_GOLD_MODELS, TRANSIT_SCHEMA),
    reason="Weather-only repository excludes Citydata and Transit domain contracts.",
)
def test_citydata_and_transit_declared_source_evidence_is_complete_and_valid():
    """Citydata's source/coverage and Transit's source evidence retain their declared contracts."""
    models = load_models_from_yaml(
        [
            CITYDATA_GOLD_MODELS,
            TRANSIT_SCHEMA,
        ]
    )
    by_name = {model.name: model for model in models}
    citydata = by_name["gold_citydata_purchasing_power_daily"]
    transit = by_name["gold_transit_parking_full_risk"]

    assert citydata.serving["source_evidence"] == [
        {
            "source_id": "seoul_citydata",
            "source_url": "https://data.seoul.go.kr/dataList/OA-21285/F/1/datasetView.do",
            "license": "공공누리 제1유형(출처표시)",
            "license_url": "https://www.kogl.or.kr/info/licenseType1.do",
            "redistribution": "allowed_with_attribution",
            "attribution": "서울특별시",
            "rights_checked_at": "2026-08-03",
        }
    ]
    assert citydata.serving["quality_coverage"] == {
        "field": "area_cd",
        "expected_distinct_count": 121,
        "minimum_ratio": 1.0,
    }
    assert transit.serving["source_evidence"] == [
        {
            "source_id": "park_info_master",
            "source_url": "https://data.seoul.go.kr/dataList/OA-13122/S/1/datasetView.do",
            "license": "공공누리 제1유형(출처표시)",
            "license_url": "https://www.kogl.or.kr/info/licenseType1.do",
            "redistribution": "allowed_with_attribution",
            "attribution": "서울특별시",
            "rights_checked_at": "2026-08-03",
        },
        {
            "source_id": "parking",
            "source_url": "https://data.seoul.go.kr/dataList/OA-21709/A/1/datasetView.do",
            "license": "공공누리 제1유형(출처표시)",
            "license_url": "https://www.kogl.or.kr/info/licenseType1.do",
            "redistribution": "allowed_with_attribution",
            "attribution": "서울특별시",
            "rights_checked_at": "2026-08-03",
        },
    ]

    result = validate([citydata, transit])
    assert result.ok, [finding.as_dict() for finding in result.findings]


@pytest.mark.skipif(
    not _all_exist(COMMERCE_GOLD_MODELS, COMMERCE_DATASET_TAXONOMY),
    reason="Weather-only repository excludes Commerce domain contracts.",
)
def test_commerce_localdata_source_evidence_covers_all_registry_sources():
    """Commerce preserves one static rights record for every LOCALDATA registry source."""
    models = load_models_from_yaml(
        [COMMERCE_GOLD_MODELS]
    )
    commerce = {model.name: model for model in models}["gold_license_flow_monthly"]
    sources = commerce.serving["source_evidence"]

    with (
        COMMERCE_DATASET_TAXONOMY
    ).open(encoding="utf-8-sig", newline="") as handle:
        expected_source_ids = {
            f"commerce_localdata_{row['short']}" for row in csv.DictReader(handle)
        }

    assert len(expected_source_ids) == 152
    assert len(sources) == 152
    assert {source["source_id"] for source in sources} == expected_source_ids
    assert len({source["source_url"] for source in sources}) == 152
    assert all(
        source["source_url"].startswith("https://data.seoul.go.kr/dataList/OA-")
        and source["source_url"].endswith("/S/1/datasetView.do")
        for source in sources
    )
    assert {source["license_url"] for source in sources} == {
        "https://www.kogl.or.kr/info/licenseType1.do"
    }
    assert {source["redistribution"] for source in sources} == {
        "allowed_with_attribution"
    }
    assert {source["rights_checked_at"] for source in sources} == {"2026-08-04"}
    assert len({source["license"] for source in sources}) == 1
    assert len({source["attribution"] for source in sources}) == 1

    result = validate([commerce])
    assert result.ok, [finding.as_dict() for finding in result.findings]


@pytest.mark.skipif(
    not _all_exist(CULTURE_GOLD_MODELS),
    reason="Weather-only repository excludes Culture domain contracts.",
)
def test_culture_activity_source_evidence_covers_all_six_lineages_with_approved_redistribution():
    """Culture records every lineage with approved external redistribution."""
    models = load_models_from_yaml(
        [CULTURE_GOLD_MODELS]
    )
    culture = {model.name: model for model in models}["gold_culture_activity_by_dong"]

    assert culture.serving["source_evidence"] == [
        {
            "source_id": "kopis_open_api",
            "source_url": "https://www.kopis.or.kr/por/cs/openapi/openApiFaq.do?menuId=MNU_00074",
            "license": "KOPIS Open API 2차 가공 집계의 외부 API 재배포·출처표시 허용",
            "license_url": "https://kopis.or.kr/upload/openApi/%EA%B3%B5%EC%97%B0%EC%98%88%EC%88%A0%ED%86%B5%ED%95%A9%EC%A0%84%EC%82%B0%EB%A7%9DOpenAPI%EA%B0%9C%EB%B0%9C%EA%B0%80%EC%9D%B4%EB%93%9C.pdf",
            "redistribution": "allowed_with_attribution",
            "attribution": "공연예술통합전산망(KOPIS)",
            "rights_checked_at": "2026-08-04",
        },
        {
            "source_id": "seoul_cultural_event",
            "source_url": "https://data.seoul.go.kr/dataList/OA-15486/S/1/datasetView.do",
            "license": "공공누리 제1유형(출처표시)",
            "license_url": "https://www.kogl.or.kr/info/licenseType1.do",
            "redistribution": "allowed_with_attribution",
            "attribution": "서울특별시",
            "rights_checked_at": "2026-08-04",
        },
        {
            "source_id": "sema_exhibition",
            "source_url": "https://data.seoul.go.kr/dataList/OA-15323/S/1/datasetView.do",
            "license": "공공누리 제1유형(출처표시)",
            "license_url": "https://www.kogl.or.kr/info/licenseType1.do",
            "redistribution": "allowed_with_attribution",
            "attribution": "서울시립미술관",
            "rights_checked_at": "2026-08-04",
        },
        {
            "source_id": "sejong_performance",
            "source_url": "https://data.seoul.go.kr/dataList/OA-2708/S/1/datasetView.do",
            "license": "공공누리 제1유형(출처표시)",
            "license_url": "https://www.kogl.or.kr/info/licenseType1.do",
            "redistribution": "allowed_with_attribution",
            "attribution": "세종문화회관, 각 컨텐츠주체",
            "rights_checked_at": "2026-08-04",
        },
        {
            "source_id": "kcisa_culture_info",
            "source_url": "https://www.data.go.kr/data/15138937/openapi.do",
            "license": "이용허락범위 제한 없음",
            "license_url": "https://www.data.go.kr/data/15138937/openapi.do",
            "redistribution": "allowed_with_attribution",
            "attribution": "한국문화정보원",
            "rights_checked_at": "2026-08-04",
        },
        {
            "source_id": "national_data_office_admin_dong_link",
            "source_url": "https://www.data.go.kr/data/15136368/fileData.do",
            "license": "공공저작물 제3유형(출처표시·변경금지) — 2차 가공 집계 외부 API 재배포 승인",
            "license_url": "https://www.kogl.or.kr/info/licenseType3.do",
            "redistribution": "allowed_with_attribution",
            "attribution": "국가데이터처",
            "rights_checked_at": "2026-08-04",
        },
    ]
    assert {source["redistribution"] for source in culture.serving["source_evidence"]} == {
        "allowed_with_attribution"
    }

    result = validate([culture])
    assert result.ok, [finding.as_dict() for finding in result.findings]


@pytest.mark.skipif(
    not _all_exist(TRANSIT_SCHEMA),
    reason="Weather-only repository excludes Transit domain contracts.",
)
def test_transit_external_products_declare_evidence_for_every_lineage_source():
    """Every externally served transit product names the sources its dbt lineage actually reads.

    The gateway rights gate (ASK-Seoul-Serving#88) reads only `d1_catalog_sources`, so an
    external product with no evidence is served today and fails closed at stage 2. Pinning the
    per-product source_id set here keeps a lineage change (a new dim, a dropped join) from
    silently leaving the published evidence behind.
    """
    models = load_models_from_yaml([TRANSIT_SCHEMA])
    by_name = {model.name: model for model in models}
    external = {
        name: model for name, model in by_name.items() if model.serving.get("external") is True
    }
    assert set(external) == {
        "gold_transit_dong_hourly",
        "gold_transit_dong_now",
        "gold_transit_event_access",
        "gold_transit_parking_full_risk",
        "gold_transit_bus_route_timetable",
        "gold_transit_subway_timetable",
    }

    source_ids = {
        name: [source["source_id"] for source in model.serving["source_evidence"]]
        for name, model in external.items()
    }
    # dong_now adds bus_route_master because gold_transit_dong_15min joins
    # dim_transit_bus_route_tier for the tier1 columns; dong_hourly reads the silvers directly.
    assert source_ids["gold_transit_dong_hourly"] == [
        "subway_arrival",
        "subway_station_master",
        "bus_position",
        "parking",
        "park_info_master",
    ]
    assert source_ids["gold_transit_dong_now"] == [
        "subway_arrival",
        "subway_station_master",
        "bus_position",
        "bus_route_master",
        "parking",
        "park_info_master",
    ]
    assert source_ids["gold_transit_event_access"] == [
        "park_info_master",
        "parking",
        "subway_station_master",
        "kopis_open_api",
        "seoul_cultural_event",
        "sema_exhibition",
        "sejong_performance",
        "kcisa_culture_info",
        "national_data_office_admin_dong_link",
    ]
    assert source_ids["gold_transit_parking_full_risk"] == ["park_info_master", "parking"]
    # timetable 은 노선 마스터 단일 원천(ASAC-DAG#765 시간표 필드) — lineage 도 이 소스뿐.
    assert source_ids["gold_transit_bus_route_timetable"] == ["bus_route_master"]
    # 지하철 시간표(#512) — 시간표 원천 + 역 마스터(dim_transit_station 경유 역 축 승계).
    assert source_ids["gold_transit_subway_timetable"] == ["subway_timetable", "subway_station_master"]

    for name, model in external.items():
        redistribution = {
            source["redistribution"] for source in model.serving["source_evidence"]
        }
        assert redistribution == {"allowed_with_attribution"}, name

    result = validate(list(external.values()))
    assert result.ok, [finding.as_dict() for finding in result.findings]


@pytest.mark.skipif(
    not _all_exist(TRANSIT_SCHEMA, CULTURE_GOLD_MODELS),
    reason="Weather-only repository excludes Transit and Culture domain contracts.",
)
def test_transit_event_access_quotes_culture_evidence_verbatim():
    """The cross-domain half of event_access must stay byte-identical to culture's declaration.

    #88 B requires a cross-domain product to quote the counterpart domain's declared rights
    rather than restate them, so this pins the quotation to culture's own contract file. If
    culture re-verifies or corrects a source, this fails until transit re-quotes it.
    """
    models = load_models_from_yaml(
        [
            TRANSIT_SCHEMA,
            CULTURE_GOLD_MODELS,
        ]
    )
    by_name = {model.name: model for model in models}
    transit = by_name["gold_transit_event_access"].serving["source_evidence"]
    culture = by_name["gold_culture_event_schedule"].serving["source_evidence"]

    quoted = [source for source in transit if source["source_id"] not in {
        "park_info_master", "parking", "subway_station_master",
    }]
    assert quoted == culture


def test_projection_identity_hash_preserves_order_and_ignores_descriptions():
    from serving_contract.projection_identity import canonical_projection_bytes, projection_schema_hash

    projection = {
        "schema_version": "1.0.0",
        "columns": ["product_row_id", "value"],
    }
    columns = {
        "product_row_id": {
            "description": "first wording",
            "data_type": "VARCHAR",
            "config": {
                "meta": {
                    "nullable": False,
                    "unit": "not_applicable",
                    "semantic_role": "primary_key",
                }
            },
        },
        "value": {
            "description": "measurement wording",
            "data_type": "DOUBLE",
            "config": {
                "meta": {
                    "nullable": True,
                    "unit": "km/h",
                    "semantic_role": "metric",
                }
            },
        },
    }

    first_bytes = canonical_projection_bytes(projection, columns)
    second_bytes = canonical_projection_bytes(projection, {**columns, "value": {**columns["value"], "description": "changed"}})
    reordered = {**projection, "columns": ["value", "product_row_id"]}

    assert first_bytes == second_bytes
    assert projection_schema_hash(projection, columns) == projection_schema_hash(projection, columns)
    assert projection_schema_hash(projection, columns) != projection_schema_hash(reordered, columns)
    assert projection_schema_hash(projection, columns) != projection_schema_hash(
        projection,
        {
            **columns,
            "value": {
                **columns["value"],
                "data_type": "DECIMAL(10,2)",
            },
        },
    )


# ── display (v1.10 · #706) ────────────────────────────────────────────────────
# 세 도메인이 계약 밖에서 이미 쓰던 네 키를 승격한 것이라, 검증의 목적은 "새 규칙을
# 강제한다"가 아니라 **오타와 화면이 못 쓰는 값을 막는다**이다.

def test_display_absent_is_valid():
    """미선언이 정상이다 — optional 이고, 선언 시점에 절반만 쓰고 있었다."""
    result = validate([_serving_model()])

    assert not [f for f in result.findings if f.rule.startswith("display_")]


def test_display_minimal_declaration_passes():
    model = _serving_model(serving_overrides={
        "display": {"title": "행정동별 문화 활동", "summary": "하루 단위 집계입니다."},
    })

    result = validate([model])

    assert not [f for f in result.findings if f.rule.startswith("display_")]


def test_display_full_declaration_passes():
    model = _serving_model(serving_overrides={
        "display": {
            "title": "행정동별 문화 활동",
            "summary": "하루 단위 집계입니다.",
            "caveat": "좌표가 없는 활동은 빠집니다.",
            "use_cases": ["생활권 문화 인프라 격차 분석", "동 단위 문화 히트맵"],
        },
    })

    result = validate([model])

    assert not [f for f in result.findings if f.rule.startswith("display_")]


@pytest.mark.parametrize("display", [
    {"summary": "제목이 없다"},                                  # required 누락
    {"title": "제목", "summary": "   "},                         # 공백만
    {"title": "제목", "summary": "요약", "titel": "오타"},        # 스펙 밖 필드
    {"title": "가" * 41, "summary": "요약"},                     # 40자 초과
    {"title": "제목", "summary": "요약", "use_cases": []},        # 빈 리스트
    {"title": "제목", "summary": "요약", "use_cases": ["", "b"]},  # 빈 항목
    {"title": "제목", "summary": "요약", "caveat": " "},          # 선언했는데 비었다
])
def test_display_rejects_malformed(display):
    result = validate([_serving_model(serving_overrides={"display": display})])

    rules = _rules(result.findings)
    assert "display_invalid" in rules or "display_unknown_field" in rules


def test_display_must_be_a_mapping():
    result = validate([_serving_model(serving_overrides={"display": ["제목"]})])

    assert "display_invalid" in _rules(result.findings)


# ── v1.11 (#217 P1·P3): usage_patterns 파라미터 메타 ────────────────────────────

def _pattern_model(pattern_overrides: dict) -> ServingModel:
    pattern = {
        "pattern_id": "gu_rank_any",
        "sql": "-- :dir='desc', :n=10\nSELECT g FROM t WHERE d = :dir ORDER BY g LIMIT :n",
    }
    pattern.update(pattern_overrides)
    return _serving_model(serving_overrides={"usage_patterns": [pattern]})


def test_pattern_param_meta_valid_passes():
    result = validate([_pattern_model({
        "param_defaults": {"dir": "desc", "n": 10},
        "param_enum": {"dir": ["asc", "desc"]},
        "params": {"n": {"type": "number"}},
    })])
    assert not [f for f in result.findings if f.rule.startswith("usage_pattern")]


def test_pattern_param_meta_array_spec_passes():
    result = validate([_pattern_model({
        "sql": "-- :gus=['a','b']\nSELECT g FROM t WHERE g IN (:gus)",
        "params": {"gus": {"type": "array", "item": "string", "max_len": 50}},
    })])
    assert not [f for f in result.findings if f.rule.startswith("usage_pattern")]


@pytest.mark.parametrize("overrides,rule", [
    # SQL 에 없는 파라미터의 메타 — 오타/개명 미반영 (게이트웨이는 관용하지만 CI 는 잡는다)
    ({"param_defaults": {"zzz": 1}}, "usage_pattern_param_meta_unknown"),
    ({"param_enum": {"zzz": ["a"]}}, "usage_pattern_param_meta_unknown"),
    ({"params": {"zzz": {"type": "string"}}}, "usage_pattern_param_meta_unknown"),
    # 형 위반
    ({"param_defaults": {"dir": ["desc"]}}, "usage_pattern_invalid"),        # 스칼라 아님
    ({"param_defaults": "desc"}, "usage_pattern_invalid"),                    # 매핑 아님
    ({"param_enum": {"dir": []}}, "usage_pattern_invalid"),                   # 빈 리스트
    ({"param_enum": {"dir": "asc"}}, "usage_pattern_invalid"),                # 리스트 아님
    ({"params": {"dir": {"type": "column"}}}, "usage_pattern_invalid"),       # 미지 type
    ({"params": {"dir": {"type": "array", "item": "bool"}}}, "usage_pattern_invalid"),   # 미지 item
    ({"params": {"dir": {"type": "array", "max_len": 0}}}, "usage_pattern_invalid"),     # cap 밖
    ({"params": {"dir": {"type": "array", "max_len": 101}}}, "usage_pattern_invalid"),   # cap 밖
    ({"params": {"dir": {"type": "array", "maxlen": 5}}}, "usage_pattern_invalid"),      # 스펙 밖 키
    # 기본값이 허용값 밖 — 게이트웨이 400 을 저작 시점에 잡는다
    ({"param_defaults": {"dir": "sideways"}, "param_enum": {"dir": ["asc", "desc"]}}, "usage_pattern_invalid"),
])
def test_pattern_param_meta_rejects_malformed(overrides, rule):
    result = validate([_pattern_model(overrides)])
    assert rule in _rules(result.findings)


# ── v1.12 (#217): 동적 기본값(상대 날짜) ────────────────────────────────────────

def _date_pattern(defaults):
    return _pattern_model({
        "pattern_id": "date_window",
        "sql": "-- :from='2026-01-01', :to='2026-01-31'\nSELECT d FROM t WHERE d BETWEEN :from AND :to",
        "param_defaults": defaults,
    })


def test_relative_date_default_valid_passes():
    result = validate([_date_pattern({"from": {"rel": "-30d", "as": "date"},
                                      "to": {"rel": "0d", "as": "date"}})])
    assert not [f for f in result.findings if f.rule.startswith("usage_pattern")]


def test_relative_date_default_grains_pass():
    for rel, as_ in [("-1y", "year"), ("0M", "ym"), ("-7d", "datetime")]:
        m = _pattern_model({"pattern_id": "p", "sql": "-- :y='2026-01-01'\nSELECT * FROM t WHERE y >= :y",
                            "param_defaults": {"y": {"rel": rel, "as": as_}}})
        assert not [f for f in validate([m]).findings if f.rule.startswith("usage_pattern")], (rel, as_)


@pytest.mark.parametrize("bad", [
    {"from": {"rel": "-30x", "as": "date"}},        # 단위 오타
    {"from": {"rel": "-30d", "as": "week"}},         # as 미지원
    {"from": {"rel": "-30d", "as": "date", "tz": "x"}},  # 허용 밖 키
    {"from": {"rel": "-30d"}},                        # as 누락 → 스칼라도 아니라 거부
])
def test_relative_date_default_rejects_malformed(bad):
    assert "usage_pattern_invalid" in _rules(validate([_date_pattern(bad)]).findings)


# ── v1.13 (#217 후속): export 자동검증 완결성 — 미검증 패턴은 예시값이 다 풀려야 한다 ──────

def test_unverified_pattern_prose_number_without_equals_is_flagged():
    # `=` 앵커 필수(ASAC-DAG#756 규약 잠금) — 힌트 문장 속 숫자(`상위 10곳`)로는 못 푼다.
    # 관용 탐색을 남기면 게이트는 통과하는데 export 검증이 못 풀어 영구 409 드리프트가 난다.
    m = _pattern_model({"pattern_id": "p", "question_ko": "상위 10곳은?",
                        "sql": "SELECT g FROM t ORDER BY x LIMIT :n"})
    assert "usage_pattern_unverifiable_example" in _rules(validate([m]).findings)


def test_unverified_pattern_without_example_is_flagged():
    # :gu 예시값이 없어 export 가 검증을 못 함 → 영구 미검증(게이트웨이 409)
    m = _pattern_model({"pattern_id": "p", "sql": "SELECT g FROM t WHERE gu = :gu ORDER BY g"})
    assert "usage_pattern_unverifiable_example" in _rules(validate([m]).findings)


def test_unverified_pattern_with_inline_example_passes():
    m = _pattern_model({"pattern_id": "p", "sql": "-- :gu='강남구'\nSELECT g FROM t WHERE gu = :gu"})
    assert "usage_pattern_unverifiable_example" not in _rules(validate([m]).findings)


def test_verified_pattern_skips_example_check():
    # 손 검증된 패턴(verified_at)은 예시가 없어도 runnable — 검사 대상 아님
    m = _pattern_model({"pattern_id": "p", "sql": "SELECT g FROM t WHERE gu = :gu",
                        "verified_at": "2026-07-30T09:00:00Z", "verified_rows": 5})
    assert "usage_pattern_unverifiable_example" not in _rules(validate([m]).findings)


def test_unverified_pattern_examples_resolve_all_forms():
    # 따옴표 문자열·숫자·한 줄 배열·따옴표 없는 한글 문자열 — 네 형태 모두 풀린다
    m = _pattern_model({"pattern_id": "p", "sql": (
        "-- :gu='강남구', :n=10, :gus=['a','b'], :area=광나루한강공원\n"
        "SELECT g FROM t WHERE gu=:gu AND g IN (:gus) AND a=:area ORDER BY g LIMIT :n")})
    assert "usage_pattern_unverifiable_example" not in _rules(validate([m]).findings)


def test_no_param_pattern_is_trivially_verifiable():
    m = _pattern_model({"pattern_id": "p", "sql": "SELECT 1"})
    assert "usage_pattern_unverifiable_example" not in _rules(validate([m]).findings)
