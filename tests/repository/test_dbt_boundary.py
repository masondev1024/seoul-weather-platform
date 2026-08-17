from __future__ import annotations

from pathlib import Path

import yaml


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PROJECT_DIR = REPOSITORY_ROOT / "dbt" / "domains" / "traffic_weather"
EXPECTED_WEATHER_MODELS = {
    "models/weather/transform/gold/gold_weather_place_current_outlook.sql",
    "models/weather/transform/gold/gold_weather_place_forecast_change_daily.sql",
    "models/weather/transform/gold/gold_weather_place_hourly_outlook.sql",
    "models/weather/transform/gold/gold_weather_place_precipitation_window.sql",
    "models/weather/transform/gold/gold_weather_place_risk_query_availability.sql",
    "models/weather/transform/gold/gold_weather_place_risk_window.sql",
    # 80-grid audit Gold 세 개. ask_seoul_weather_transform_serving_gold selector 가
    # 이미 세 경로를 선언하고 있었는데 fork 추출 때 파일만 빠져 dangling 상태였다.
    # 공개 제품이 아니라 grid coverage audit 용이며, 아래 D1 공개 제품 경계 테스트가
    # 이 모델들이 공개 selector 로 새지 않는 것을 계속 막는다.
    "models/weather/transform/gold/gold_weather_grid_hourly_outlook.sql",
    "models/weather/transform/gold/gold_weather_grid_current_outlook.sql",
    "models/weather/transform/gold/gold_weather_grid_precipitation_window.sql",
    "models/weather/transform/place_mart/dim_weather_place.sql",
    "models/weather/transform/place_mart/gold_weather_forecast_by_place_serving.sql",
    "models/weather/transform/place_mart/silver_weather_forecast_by_admin_dong_serving.sql",
    # 80-grid coverage mart. weather_vilage_fcst_transform 의 dbt_run_coverage_grid_mart/
    # dbt_test_coverage_grid_mart 단계가 이 세 모델을 선택하므로, 없으면 그 단계가
    # empty-selection 으로 실패한다. 공개 제품이 아니라 audit/coverage 용이며
    # 위 test_weather_dbt_boundary_keeps_only_the_four_public_products 가
    # 이 모델들이 D1 공개 selector 로 새지 않는 것을 계속 막는다.
    "models/weather/transform/grid_mart/dim_weather_coverage_grid.sql",
    "models/weather/transform/grid_mart/silver_weather_forecast_by_coverage_grid_serving.sql",
    "models/weather/transform/grid_mart/gold_weather_forecast_by_grid_serving.sql",
    "models/weather/transform/silver/silver_kma_vilage_fcst.sql",
}


def _load_yaml(relative_path: str) -> dict:
    path = PROJECT_DIR / relative_path
    assert path.exists(), f"Weather-only dbt boundary file is missing: {path}"
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def test_weather_dbt_boundary_keeps_only_the_four_public_products() -> None:
    """Catches a public selector that publishes a private/grid/Traffic model."""
    project = _load_yaml("dbt_project.yml")
    packages = _load_yaml("packages.yml")
    selectors = _load_yaml("selectors.yml")["selectors"]
    sources = _load_yaml("models/weather/sources.yml")["sources"]
    seeds = _load_yaml("seeds/weather/_weather_inputs.yml")["seeds"]
    groups = _load_yaml("models/groups.yml")["groups"]
    place_models = _load_yaml("models/weather/transform/place_mart/_place_mart.yml")["models"]
    serving_models = _load_yaml("models/weather/transform/gold/_serving_gold.yml")["models"]

    assert project["name"] == "asac_seoul"
    assert project["profile"] == "asac_seoul"
    assert packages == {"packages": [{"local": "../../packages/asac_axes"}]}

    selector = next(
        item
        for item in selectors
        if item["name"] == "ask_seoul_weather_d1_public_products"
    )
    assert selector["definition"]["union"] == [
        {"method": "fqn", "value": "gold_weather_place_current_outlook", "indirect_selection": "empty"},
        {"method": "fqn", "value": "gold_weather_place_precipitation_window", "indirect_selection": "empty"},
        {"method": "fqn", "value": "gold_weather_place_risk_window", "indirect_selection": "empty"},
        {"method": "fqn", "value": "gold_weather_place_forecast_change_daily", "indirect_selection": "empty"},
    ]

    assert [source["name"] for source in sources] == ["weather_bronze"]
    assert [table["name"] for table in sources[0]["tables"]] == [
        "kma_vilage_fcst",
        "collection_run_manifest",
    ]
    # weather_coverage_grid 는 80-grid coverage mart 의 입력이다. 공개 제품이 아니며
    # 위 ask_seoul_weather_d1_public_products assert 가 D1 공개 경계를 계속 지킨다.
    assert [seed["name"] for seed in seeds] == [
        "weather_coverage_grid",
        "weather_place_grid_mapping",
    ]
    assert [group["name"] for group in groups] == ["weather"]
    assert [model["name"] for model in place_models] == [
        "silver_weather_forecast_by_admin_dong_serving",
        "dim_weather_place",
        "gold_weather_forecast_by_place_serving",
    ]
    assert [model["name"] for model in serving_models] == [
        "gold_weather_place_hourly_outlook"
    ]

    public_model_names = {
        item["value"] for item in selector["definition"]["union"]
    }
    assert "gold_weather_place_risk_query_availability" not in public_model_names


def _selector_model_names(selectors: list[dict], name: str) -> set[str]:
    """Model names a selector writes, following one level of selector indirection."""
    by_name = {item["name"]: item for item in selectors}
    found: set[str] = set()

    def walk(node: object) -> None:
        if isinstance(node, dict):
            method, value = node.get("method"), node.get("value")
            if method == "fqn" and isinstance(value, str):
                found.add(value)
            elif method == "path" and isinstance(value, str) and value.startswith("models/"):
                found.add(Path(value).stem)
            elif method == "selector" and isinstance(value, str) and value in by_name:
                walk(by_name[value]["definition"])
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(by_name[name]["definition"])
    return found


def test_weather_transform_and_refresh_never_write_the_same_gold_table() -> None:
    """weather_vilage_fcst_transform 과 weather_serving_snapshot_refresh 는 서로 다른
    스케줄(asset trigger vs 매시)로 돌기 때문에 실행 구간이 겹친다. 두 DAG 가 같은
    Gold 테이블을 build 하면 한쪽이 다른 쪽의 테이블을 갈아엎는 중에 D1 export 가
    읽어 서빙이 찢어진다. 그래서 소유권을 나눴고, 이 테스트가 그 분리를 고정한다.
    """
    selectors = _load_yaml("selectors.yml")["selectors"]

    transform_owned = _selector_model_names(
        selectors, "ask_seoul_weather_transform_serving_gold"
    )
    refresh_owned = _selector_model_names(
        selectors, "ask_seoul_weather_serving_snapshot_refresh"
    )

    assert transform_owned & refresh_owned == set(), (
        "두 DAG 가 같은 Gold 테이블을 씁니다. 하나의 소유자만 남기세요: "
        f"{sorted(transform_owned & refresh_owned)}"
    )

    # transform 은 공통 입력과 격자 audit 만, refresh 는 공개 장소 상품만 소유한다.
    assert transform_owned == {
        "gold_weather_forecast_by_place_serving",
        "gold_weather_grid_hourly_outlook",
        "gold_weather_grid_current_outlook",
        "gold_weather_grid_precipitation_window",
    }
    assert refresh_owned == {
        "gold_weather_place_hourly_outlook",
        "gold_weather_place_current_outlook",
        "gold_weather_place_precipitation_window",
        "gold_weather_place_risk_window",
        "gold_weather_place_risk_query_availability",
        "gold_weather_place_forecast_change_daily",
    }

    # D1 공개 제품 네 개는 전부 refresh 소유여야 한다. transform 쪽으로 넘어가면
    # 매시 갱신이 끊기고 서빙이 최대 3시간 낡는다.
    public = _selector_model_names(selectors, "ask_seoul_weather_d1_public_products")
    assert public <= refresh_owned


def test_weather_singular_tests_share_the_weather_group() -> None:
    """Private Weather models may only be referenced by tests in the same group."""
    project = _load_yaml("dbt_project.yml")

    assert project["data_tests"]["asac_seoul"]["weather"]["+group"] == "weather"


def test_weather_dbt_model_inventory_has_no_cross_domain_or_legacy_models() -> None:
    """Keeps the executable graph at the reviewed Weather-only extraction boundary."""
    actual = {
        path.relative_to(PROJECT_DIR).as_posix()
        for path in (PROJECT_DIR / "models").rglob("*.sql")
    }

    assert actual == EXPECTED_WEATHER_MODELS
