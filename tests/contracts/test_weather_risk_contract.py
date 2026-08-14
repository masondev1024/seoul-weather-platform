from __future__ import annotations

import base64
import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_ROOT = REPO_ROOT / "contracts" / "weather-risk"
REVISION = "kma_admin_dong_grid_20260325:9b34417b1418be6877e614c113a18e93f078de745f763858e13ed0e896a687ea"


def _read_json(relative_path: str) -> dict:
    return json.loads((CONTRACT_ROOT / relative_path).read_text(encoding="utf-8"))


def _parameter_names(document: dict, operation: dict) -> set[str]:
    names = set()
    for parameter in operation["parameters"]:
        if "$ref" in parameter:
            component = parameter["$ref"].rsplit("/", 1)[-1]
            parameter = document["components"]["parameters"][component]
        names.add(parameter["name"])
    return names


def test_weather_risk_contract_artifacts_are_present() -> None:
    assert {
        "openapi/origin.json",
        "openapi/proxy.json",
        "fixtures/data-covered-empty.json",
        "fixtures/data-page-v2-cursor.json",
        "fixtures/errors.json",
    } <= {
        path.relative_to(CONTRACT_ROOT).as_posix()
        for path in CONTRACT_ROOT.rglob("*.json")
    }


def test_openapi_subsets_describe_exactly_three_origin_and_proxy_routes() -> None:
    origin = _read_json("openapi/origin.json")
    proxy = _read_json("openapi/proxy.json")

    assert set(origin["paths"]) == {
        "/skill/v1/bundles/seoul-weather-risk",
        "/skill/v1/products/weather_place_risk_window",
        "/skill/v1/products/weather_place_risk_window/data",
    }
    assert set(proxy["paths"]) == {
        "/v1/ask-seoul/weather-risk/bundle",
        "/v1/ask-seoul/weather-risk/product",
        "/v1/ask-seoul/weather-risk/data",
    }
    assert origin["info"]["version"] == "weather-risk-query-context/v1"
    assert proxy["info"]["version"] == "weather-risk-query-context/v1"

    origin_data = origin["paths"]["/skill/v1/products/weather_place_risk_window/data"]["get"]
    proxy_data = proxy["paths"]["/v1/ask-seoul/weather-risk/data"]["get"]
    expected_parameters = {
        "product_row_id",
        "place_id",
        "forecast_at",
        "risk_labels",
        "from",
        "to",
        "limit",
        "cursor",
    }
    assert _parameter_names(origin, origin_data) == expected_parameters
    assert _parameter_names(proxy, proxy_data) == expected_parameters
    assert set(origin_data["responses"]) == {"200", "400", "401", "403", "404", "409", "422", "429", "500", "503"}
    assert set(proxy_data["responses"]) == {"200", "400", "401", "403", "404", "409", "422", "429", "500", "503"}


def test_data_contract_requires_all_fourteen_query_context_fields() -> None:
    origin = _read_json("openapi/origin.json")
    query_context = origin["components"]["schemas"]["QueryContext"]

    assert set(query_context["required"]) == {
        "schema_version",
        "place_id",
        "requested_from_at",
        "requested_to_at",
        "available_from_at",
        "available_to_at",
        "snapshot_as_of_hour",
        "forecast_collected_at_min",
        "forecast_collected_at_max",
        "source_population_revision",
        "publication_id",
        "coverage_status",
        "freshness_state",
        "zero_result_reason",
    }
    assert query_context["properties"]["schema_version"]["const"] == "weather-risk-query-context/v1"
    assert query_context["properties"]["source_population_revision"]["const"] == REVISION


def test_origin_declares_bearer_access_but_proxy_exposes_no_client_credential() -> None:
    origin = _read_json("openapi/origin.json")
    proxy = _read_json("openapi/proxy.json")

    assert origin["components"]["securitySchemes"]["BearerApiKey"] == {
        "type": "http",
        "scheme": "bearer",
    }
    origin_data = origin["paths"]["/skill/v1/products/weather_place_risk_window/data"]["get"]
    assert origin_data["security"] == [{"BearerApiKey": []}]
    assert "securitySchemes" not in proxy["components"]


def test_covered_empty_fixture_is_the_only_verified_zero_row_meaning() -> None:
    payload = _read_json("fixtures/data-covered-empty.json")

    assert payload["row_count"] == 0
    assert payload["rows"] == []
    assert payload["has_more"] is False
    assert payload["next_cursor"] is None
    assert payload["query_context"]["coverage_status"] == "covered"
    assert payload["query_context"]["freshness_state"] == "fresh"
    assert payload["query_context"]["zero_result_reason"] == "no_upcoming_weather_risk_candidate"
    assert payload["query_context"]["source_population_revision"] == REVISION


def test_cursor_fixture_is_base64url_v2_and_bound_to_its_publication() -> None:
    payload = _read_json("fixtures/data-page-v2-cursor.json")
    encoded = payload["next_cursor"]
    decoded = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))

    assert payload["has_more"] is True
    assert payload["row_count"] == len(payload["rows"]) == 1
    assert decoded["v"] == 2
    assert decoded["publication_id"] == payload["publication_id"]
    assert len(decoded["query_fingerprint"]) == 64
    assert set(decoded) == {"v", "publication_id", "query_fingerprint", "forecast_at", "product_row_id"}


def test_error_fixtures_cover_required_statuses_without_sensitive_values() -> None:
    errors = _read_json("fixtures/errors.json")

    assert set(errors) == {"400", "401", "403", "404", "409", "422", "429", "500", "503"}
    assert errors["400"]["code"] == "invalid_cursor"
    assert errors["409"]["code"] == "cursor_query_mismatch"
    assert errors["422"] == {
        "title": "query window unavailable",
        "detail": "requested time range is outside the current publication's complete availability window",
        "code": "query_window_unavailable",
        "product_id": "weather_place_risk_window",
        "publication_id": "publication-example-20260814",
        "place_id": "seoul_admd_1171065000",
    }
    assert errors["503"]["code"] == "product_not_ready"
    assert all("Bearer " not in json.dumps(problem) for problem in errors.values())
