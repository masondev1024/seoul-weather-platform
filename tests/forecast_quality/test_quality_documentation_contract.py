from __future__ import annotations

from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = REPOSITORY_ROOT / "docs" / "runbooks" / "WEATHER_FORECAST_QUALITY_RUNBOOK.md"
ARCHITECTURE = REPOSITORY_ROOT / "docs" / "architecture" / "forecast-quality-gold.md"
README = REPOSITORY_ROOT / "README.md"


def test_quality_runbook_states_replay_and_operational_boundaries() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "seven complete KST dates ending yesterday" in text
    assert "[valid_at - 27h, valid_at - 24h]" in text
    assert "[valid_at - 51h, valid_at - 48h]" in text
    assert "[valid_at - 75h, valid_at - 72h]" in text
    assert "03:05 KST" in text
    assert "15m" in text and "20m" in text
    assert "BACKFILL_ONE_KST_DATE" in text
    assert "provisional" in text and "degraded" in text
    assert "zero new KMA API calls" in text
    assert "D1" in text and "Worker" in text


def test_quality_architecture_and_repository_readme_keep_serving_isolated() -> None:
    architecture = ARCHITECTURE.read_text(encoding="utf-8")
    readme = README.read_text(encoding="utf-8")

    assert "weather_bronze.kma_vilage_fcst" in architecture
    assert "weather_bronze.kma_ultra_srt_ncst" in architecture
    assert "Gold" in architecture
    assert "D1" in architecture and "Worker" in architecture
    assert "WEATHER_FORECAST_QUALITY_RUNBOOK.md" in readme
