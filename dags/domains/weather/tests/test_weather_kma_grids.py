import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.kma import (
    build_kma_url,
    kma_page_numbers,
    load_kma_grids,
    request_params_json,
)  # noqa: E402


def write_grid_csv(path: Path, rows: list[tuple[str, int, int]]) -> None:
    path.write_text(
        "place_id,nx,ny,coverage_scope\n"
        + "".join(f"{place_id},{nx},{ny},test\n" for place_id, nx, ny in rows),
        encoding="utf-8",
    )


def test_default_kma_grid_csv_matches_expected_contract():
    assert len(load_kma_grids()) == 80


def test_kma_grid_count_mismatch_fails_after_dedup(tmp_path):
    path = tmp_path / "grids.csv"
    write_grid_csv(path, [("a", 60, 127), ("duplicate", 60, 127)])

    with pytest.raises(RuntimeError, match="expected=2, actual=1"):
        load_kma_grids(str(path), expected_grid_count=2)


def test_kma_grid_expected_count_can_be_overridden_for_custom_csv(tmp_path):
    path = tmp_path / "grids.csv"
    write_grid_csv(path, [("a", 60, 127)])

    assert load_kma_grids(str(path), expected_grid_count=1) == [
        {"place_id": "a", "nx": 60, "ny": 127}
    ]


def test_kma_page_numbers_cover_total_count_over_default_page_size():
    assert kma_page_numbers(0, 1000) == [1]
    assert kma_page_numbers(1000, 1000) == [1]
    assert kma_page_numbers(1001, 1000) == [1, 2]
    assert kma_page_numbers(1052, 1000) == [1, 2]


def test_kma_request_metadata_and_url_include_explicit_page():
    params_json = request_params_json(
        "20260705", "1700", 56, 130, page_no=2, num_of_rows=1000
    )

    assert '"pageNo": "2"' in params_json
    assert '"numOfRows": "1000"' in params_json

    url = build_kma_url("20260705", "1700", 56, 130, page_no=2, num_of_rows=1000)

    assert "pageNo=2" in url
    assert "numOfRows=1000" in url
