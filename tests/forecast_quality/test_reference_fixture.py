from __future__ import annotations

import json
from pathlib import Path

from weather_quality.fixture import build_reference_evidence, canonical_json_bytes


ROOT = Path(__file__).resolve().parents[2]
GRID_CSV = ROOT / "dags/domains/weather/config/seoul_kma_grids.csv"
SCENARIO = ROOT / "contracts/weather-forecast-quality/fixtures/reference-scenario-v1.json"


def test_reference_fixture_covers_all_80_grids_and_is_byte_stable() -> None:
    first = build_reference_evidence(GRID_CSV, SCENARIO)
    second = build_reference_evidence(GRID_CSV, SCENARIO)

    assert first["fixture_notice"] == "synthetic_reference_only_not_current_seoul_accuracy"
    assert first["universe"]["expected_grid_count"] == 80
    assert first["universe"]["observed_grid_count"] == 80
    assert len(first["cohorts"]) == 9
    assert {item["vintage_label"] for item in first["cohorts"]} == {"D-3", "D-2", "D-1"}
    assert first["vintage_gap_counts"] == {"D-1": 0, "D-2": 0, "D-3": 0}
    assert {item["metric_family"] for item in first["cohorts"]} == {
        "categorical",
        "continuous",
        "probabilistic",
    }
    assert all(item["sample_count"] == 80 for item in first["cohorts"])
    assert canonical_json_bytes(first) == canonical_json_bytes(second)
    json.loads(canonical_json_bytes(first))


def test_product_family_registry_keeps_future_adapters_disabled() -> None:
    evidence = build_reference_evidence(GRID_CSV, SCENARIO)
    adapters = {item["product_family"]: item["status"] for item in evidence["adapters"]}

    assert adapters == {
        "short_range": "fixture_enabled",
        "ultra_short": "contract_only_disabled",
        "mid_term": "contract_only_disabled",
    }
