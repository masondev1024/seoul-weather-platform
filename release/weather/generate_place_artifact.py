"""Secretless CLI wrapper for the deterministic Weather place artifact."""

from __future__ import annotations

import argparse
from pathlib import Path

from release.weather.place_artifact import ArtifactError, build_artifact, canonical_json_bytes


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate the 427-place K-Skill compatibility artifact."
    )
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--as-of", required=True, help="Explicit YYYY-MM-DD artifact date.")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail unless output already equals the deterministic artifact.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        artifact = build_artifact(args.source, generated_at=args.as_of)
        payload = canonical_json_bytes(artifact)
    except ArtifactError as exc:
        print(f"ERROR: {exc}")
        return 1

    if args.check:
        if not args.output.is_file() or args.output.read_bytes() != payload:
            print("ERROR: output differs from the deterministic 427-place artifact")
            return 1
        print("Place artifact is current.")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(payload)
    print(f"Wrote {len(artifact['locations'])} locations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
