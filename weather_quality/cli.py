from __future__ import annotations

import argparse
from pathlib import Path

from weather_quality.fixture import build_reference_evidence, canonical_json_bytes
from weather_quality.models import ContractError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate the synthetic 80-grid forecast-quality evidence fixture."
    )
    parser.add_argument("--grid-csv", type=Path, required=True)
    parser.add_argument("--scenario", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail unless output already equals the deterministic evidence artifact.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        payload = canonical_json_bytes(build_reference_evidence(args.grid_csv, args.scenario))
    except ContractError as exc:
        print(f"ERROR: {exc}")
        return 1
    if args.check:
        if not args.output.is_file() or args.output.read_bytes() != payload:
            print("ERROR: forecast-quality evidence fixture differs from deterministic output")
            return 1
        print("Forecast-quality evidence fixture is current.")
        return 0
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(payload)
    print("Wrote synthetic 80-grid forecast-quality evidence fixture.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
