"""CLI for the Serving Contract validator.

Exit codes follow the ``contracts/engine`` convention:
  0 = PASS, 1 = contract FAIL (findings), 2 = invocation / IO ERROR.

JSON report goes to stdout as deterministic UTF-8 bytes; diagnostics to stderr.
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path
from typing import Sequence

from serving_contract.io_utf8 import write_utf8_stdout
from serving_contract.model import load_manifest, load_models_from_yaml
from serving_contract.validator import ValidationResult, load_schema, validate


def _expand_sources(patterns: Sequence[str]) -> list[Path]:
    paths: list[Path] = []
    for pattern in patterns:
        matches = [Path(p) for p in glob.glob(pattern, recursive=True)]
        if not matches:
            candidate = Path(pattern)
            if candidate.exists():
                matches = [candidate]
        paths.extend(sorted(matches))
    # De-duplicate while keeping order.
    seen: set[str] = set()
    unique: list[Path] = []
    for path in paths:
        key = str(path.resolve())
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return unique


def _render_json(result: ValidationResult) -> str:
    payload = {
        "status": "pass" if result.ok else "fail",
        "models_checked": result.models_checked,
        "finding_count": len(result.findings),
        "findings": [f.as_dict() for f in result.findings],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _render_text(result: ValidationResult) -> str:
    if result.ok:
        return f"PASS — {result.models_checked} models, 0 findings\n"
    lines = [f"FAIL — {result.models_checked} models, {len(result.findings)} findings"]
    for f in result.findings:
        lines.append(f"  [{f.rule}] {f.model}: {f.message}  ({f.source})")
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate meta.serving against Serving Contract v1.")
    parser.add_argument("--source", nargs="+", required=True, help="dbt schema YAML file(s) or glob(s).")
    parser.add_argument("--manifest", default=None, help="Optional dbt manifest.json for model-membership + column checks.")
    parser.add_argument("--format", choices=["json", "text"], default="json")
    args = parser.parse_args(argv)

    try:
        paths = _expand_sources(args.source)
        if not paths:
            print(f"ERROR: no source files matched {args.source}", file=sys.stderr)
            return 2
        models = load_models_from_yaml(paths)
        manifest = load_manifest(args.manifest)
        schema = load_schema()
        result = validate(models, manifest, schema)
    except Exception as exc:  # noqa: BLE001 -- invocation/IO error => exit 2, not a contract fail
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2

    rendered = _render_json(result) if args.format == "json" else _render_text(result)
    write_utf8_stdout(rendered)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
