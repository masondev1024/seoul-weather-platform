from __future__ import annotations

import csv

from tools.spatial_quality_product import build_product, write_product


def test_spatial_product_keeps_place_identity_and_joins_grid_quality(tmp_path) -> None:
    mapping = tmp_path / "mapping.csv"
    mapping.write_text(
        "place_id,place_name,gu,admin_dong,latitude,longitude,nx,ny,mapping_method,grid_distance_m\n"
        "p-1,잠실,송파구,잠실동,37.5,127.1,60,127,checked,100\n",
        encoding="utf-8",
    )
    metrics = tmp_path / "metrics.csv"
    metrics.write_text(
        "grid_id,evaluation_date_kst,forecast_horizon,matched_coverage,temperature_mae,evidence_state\n"
        "kma_60_127,2026-09-02,D-1,0.95,1.2,measured\n",
        encoding="utf-8",
    )

    rows = build_product(mapping, metrics)
    output = tmp_path / "spatial.csv"
    write_product(rows, output)

    assert rows[0]["place_id"] == "p-1"
    assert rows[0]["grid_id"] == "kma_60_127"
    assert rows[0]["quality_state"] == "measured"
    with output.open(newline="", encoding="utf-8") as handle:
        assert list(csv.DictReader(handle))[0]["temperature_mae"] == "1.2"
