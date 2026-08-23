from __future__ import annotations

import re
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
WEATHER_GOLD_ROOT = (
    REPOSITORY_ROOT
    / "dbt"
    / "domains"
    / "traffic_weather"
    / "models"
    / "weather"
    / "transform"
    / "gold"
)

PUBLIC_PRODUCT_MODELS = {
    "gold_weather_place_current_outlook": {
        "expected_refs": {"gold_weather_place_hourly_outlook"},
        "window_marker": "hourly.forecast_at >= kst_now.current_hour_at",
    },
    "gold_weather_place_precipitation_window": {
        "expected_refs": {"gold_weather_place_hourly_outlook"},
        "window_marker": "hourly.forecast_at >= kst_now.current_hour_at",
    },
    "gold_weather_place_risk_window": {
        "expected_refs": {"gold_weather_place_hourly_outlook"},
        "window_marker": "hourly.forecast_at >= kst_now.current_hour_at",
    },
    "gold_weather_place_forecast_change_daily": {
        "expected_refs": {"silver_weather_forecast_by_admin_dong_serving"},
        "window_marker": "cast(forecast.forecast_at as date) >= kst_today.today",
    },
}

_REF_PATTERN = re.compile(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")


def _model_path(model_name: str) -> Path:
    return WEATHER_GOLD_ROOT / f"{model_name}.sql"


def _refs(sql: str) -> set[str]:
    return set(_REF_PATTERN.findall(sql))


def validate_model_sql(model_name: str, sql: str) -> list[str]:
    contract = PUBLIC_PRODUCT_MODELS[model_name]
    errors: list[str] = []
    refs = _refs(sql)

    if refs != contract["expected_refs"]:
        errors.append(
            f"{model_name}: expected refs {sorted(contract['expected_refs'])}, "
            f"got {sorted(refs)}"
        )
    if "full_refresh=true" in sql.lower():
        errors.append(f"{model_name}: full_refresh=true is forbidden")
    if contract["window_marker"] not in sql:
        errors.append(
            f"{model_name}: missing bounded forecast window marker "
            f"{contract['window_marker']}"
        )
    if "select * from" in sql.lower():
        errors.append(f"{model_name}: direct select * from is forbidden")
    return errors


def validate_weather_public_products(repo_root: Path = REPOSITORY_ROOT) -> list[str]:
    gold_root = (
        repo_root
        / "dbt"
        / "domains"
        / "traffic_weather"
        / "models"
        / "weather"
        / "transform"
        / "gold"
    )
    errors: list[str] = []
    for model_name in PUBLIC_PRODUCT_MODELS:
        path = gold_root / f"{model_name}.sql"
        if not path.is_file():
            errors.append(f"{model_name}: model file is missing")
            continue
        errors.extend(
            validate_model_sql(model_name, path.read_text(encoding="utf-8"))
        )
    return errors


def main() -> int:
    errors = validate_weather_public_products()
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Weather public-product query cost guardrails verified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
