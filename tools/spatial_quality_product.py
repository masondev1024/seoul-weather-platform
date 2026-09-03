"""Build a place-level spatial view from weather grid quality Gold exports.

The product is deliberately a transparent join: administrative-place metadata comes
from the checked-in crosswalk and quality facts come from a Gold export.  No private
location data or guessed business attribute is added.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _grid_id(row: dict[str, str]) -> str:
    return f"kma_{row['nx']}_{row['ny']}"


def build_product(mapping_path: Path, metrics_path: Path | None = None) -> list[dict[str, Any]]:
    mapping_rows = _read_csv(mapping_path)
    metric_rows = _read_csv(metrics_path) if metrics_path else []
    latest: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in metric_rows:
        key = (
            row.get("grid_id", ""),
            row.get("evaluation_date_kst", ""),
            row.get("forecast_horizon", ""),
        )
        latest[key] = row

    output: list[dict[str, Any]] = []
    for place in mapping_rows:
        grid_id = _grid_id(place)
        candidates = [row for (candidate_grid, _, _), row in latest.items() if candidate_grid == grid_id]
        candidates.sort(
            key=lambda row: (
                row.get("evaluation_date_kst", ""),
                row.get("forecast_horizon", ""),
            ),
            reverse=True,
        )
        metric = candidates[0] if candidates else {}
        coverage = metric.get("matched_coverage", "")
        evidence = metric.get("evidence_state", "")
        quality_state = evidence or ("MEASURED" if coverage else "NO_METRICS")
        output.append(
            {
                "place_id": place["place_id"],
                "place_name": place["place_name"],
                "gu": place["gu"],
                "admin_dong": place["admin_dong"],
                "latitude": place["latitude"],
                "longitude": place["longitude"],
                "grid_id": grid_id,
                "grid_distance_m": place["grid_distance_m"],
                "mapping_method": place["mapping_method"],
                "evaluation_date_kst": metric.get("evaluation_date_kst", ""),
                "forecast_horizon": metric.get("forecast_horizon", ""),
                "matched_coverage": coverage,
                "temperature_mae": metric.get("temperature_mae", ""),
                "precipitation_brier_score": metric.get("precipitation_brier_score", ""),
                "pty_accuracy": metric.get("pty_accuracy", ""),
                "quality_state": quality_state,
            }
        )
    return output


def write_product(rows: list[dict[str, Any]], output_path: Path) -> None:
    if not rows:
        raise ValueError("spatial product has no rows")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0])
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mapping", type=Path, required=True)
    parser.add_argument("--metrics", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    write_product(build_product(args.mapping, args.metrics), args.output)
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
