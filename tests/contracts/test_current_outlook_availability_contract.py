from __future__ import annotations

from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
CURRENT_OUTLOOK_CONTRACT = (
    REPO_ROOT
    / "dbt/domains/traffic_weather/models/weather/transform/gold/gold_weather_place_current_outlook.yml"
)


def test_current_outlook_declares_bounded_refresh_grace() -> None:
    document = yaml.safe_load(CURRENT_OUTLOOK_CONTRACT.read_text(encoding="utf-8"))
    serving = document["models"][0]["config"]["meta"]["serving"]

    assert serving["mcp_projection"]["currentness"] == {
        "field": "forecast_at",
        "minimum": "current_kst_hour",
        "refresh_grace_minutes": 30,
    }
