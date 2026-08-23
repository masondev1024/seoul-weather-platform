from __future__ import annotations

import argparse
import ast
import json
import py_compile
import tempfile
from pathlib import Path
from typing import Any, Iterable


EXPECTED_ENTRYPOINTS = frozenset(
    {
        "common_admin_dong_bronze.py",
        "domains/weather/weather_serving_export.py",
        "domains/weather/weather_serving_freshness_watchdog.py",
        "domains/weather/weather_serving_snapshot_refresh.py",
        "domains/weather/weather_vilage_fcst_bronze.py",
        "domains/weather/weather_vilage_fcst_collection_slot_reconciliation.py",
        "domains/weather/weather_vilage_fcst_transform.py",
        "domains/weather/weather_w2_canonical_transform.py",
    }
)

FORBIDDEN_IMPORT_PREFIXES = (
    "domains.traffic",
    "traffic",
    "weather_iceberg_maintenance",
    "weather_w2_observation_recovery",
    "weather_reliability_report",
    "weather_ingest.cost_proxy",
    "weather_ingest.delivery_reliability",
    "weather_ingest.delivery_reliability_pilot",
    "weather_ingest.iceberg_maintenance",
    "weather_ingest.reliability",
    "weather_ingest.w2_recovery",
    "weather_ingest.weather_traffic_cost_proxy",
)


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON object required: {path}")
    return value


def _read_manifest(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"manifest line {line_number} must be an object")
        records.append(value)
    return records


def _airflow_commit(source_lock: dict[str, Any]) -> str:
    sources = source_lock.get("sources")
    if not isinstance(sources, list):
        raise ValueError("source lock must contain a sources list")
    for source in sources:
        if isinstance(source, dict) and source.get("id") == "airflow_weather":
            commit = source.get("commit")
            if isinstance(commit, str) and len(commit) == 40:
                return commit
            raise ValueError("airflow_weather source lock must use a full commit")
    raise ValueError("source lock does not define airflow_weather")


def _source_identities(record: dict[str, Any]) -> set[tuple[str, str]]:
    """Return fixed source identities through reviewed derivation wrappers."""
    identities: set[tuple[str, str]] = set()
    pending = [record]
    visited: set[int] = set()
    while pending:
        candidate = pending.pop()
        identity = id(candidate)
        if identity in visited:
            continue
        visited.add(identity)
        source_commit = candidate.get("source_commit")
        source_path = candidate.get("source_path")
        if isinstance(source_commit, str) and isinstance(source_path, str):
            identities.add((source_commit, source_path))
        for key in ("derived_from", "upstream"):
            nested = candidate.get(key)
            if isinstance(nested, dict):
                pending.append(nested)
    return identities


def entrypoint_errors(entries: Iterable[dict[str, Any]]) -> list[str]:
    actual = {
        entry.get("source_path")
        for entry in entries
        if entry.get("scope") == "airflow_weather_entrypoint"
        and isinstance(entry.get("source_path"), str)
    }
    missing = sorted(EXPECTED_ENTRYPOINTS - actual)
    unexpected = sorted(actual - EXPECTED_ENTRYPOINTS)
    if not missing and not unexpected:
        return []
    details = [*(f"missing {path}" for path in missing), *(f"unexpected {path}" for path in unexpected)]
    return [
        "airflow entrypoint set differs from the required eight Weather lanes: "
        + "; ".join(details)
    ]


def inventory_manifest_errors(
    entries: Iterable[dict[str, Any]], records: Iterable[dict[str, Any]], airflow_commit: str
) -> list[str]:
    inventory = {
        entry["target_path"]: entry["source_path"]
        for entry in entries
        if entry.get("source_id") == "airflow_weather"
        and isinstance(entry.get("target_path"), str)
        and isinstance(entry.get("source_path"), str)
    }
    manifest: dict[str, str] = {}
    for record in records:
        target_path = record.get("target_path")
        if not isinstance(target_path, str) or not target_path.startswith("dags/"):
            continue

        matching_source_paths = {
            source_path
            for source_commit, source_path in _source_identities(record)
            if source_commit == airflow_commit
        }
        if len(matching_source_paths) == 1:
            manifest[target_path] = next(iter(matching_source_paths))
    errors: list[str] = []
    for target_path in sorted(inventory):
        manifest_source = manifest.get(target_path)
        if manifest_source is None:
            errors.append(f"missing manifest record for airflow inventory target: {target_path}")
        elif manifest_source != inventory[target_path]:
            errors.append(f"manifest source_path differs for airflow target: {target_path}")
    for target_path in sorted(set(manifest) - set(inventory)):
        errors.append(f"manifest airflow record absent from inventory: {target_path}")
    return errors


def _display_path(path: Path) -> str:
    for parent in (path.parent, *path.parents):
        if parent.name == "dags":
            return path.relative_to(parent.parent).as_posix()
    return path.as_posix()


def _forbidden_import(name: str) -> str | None:
    for prefix in FORBIDDEN_IMPORT_PREFIXES:
        if name == prefix or name.startswith(f"{prefix}."):
            return prefix
    return None


def find_forbidden_imports(paths: Iterable[Path]) -> list[str]:
    errors: list[str] = []
    for path in sorted(paths):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError) as exc:
            errors.append(f"cannot parse Python source {_display_path(path)}: {exc}")
            continue
        imports: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
                imports.extend(f"{node.module}.{alias.name}" for alias in node.names)
        seen: set[str] = set()
        for imported in imports:
            forbidden = _forbidden_import(imported)
            if forbidden is None or forbidden in seen:
                continue
            seen.add(forbidden)
            errors.append(
                f"forbidden mixed-domain import in {_display_path(path)}: {forbidden}"
            )
    return errors


def _inventory_shape_errors(entries: Iterable[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    for entry in entries:
        if entry.get("source_id") != "airflow_weather":
            continue
        source_path = entry.get("source_path")
        target_path = entry.get("target_path")
        if not isinstance(source_path, str) or not isinstance(target_path, str):
            errors.append("airflow inventory entry requires source_path and target_path")
            continue
        if target_path != f"dags/{source_path}":
            errors.append(f"airflow inventory target is not a dags snapshot path: {target_path}")
    return errors


def _compile_paths(paths: Iterable[Path]) -> list[str]:
    errors: list[str] = []
    with tempfile.TemporaryDirectory(prefix="airflow-boundary-") as temporary:
        cache = Path(temporary)
        for index, path in enumerate(sorted(paths)):
            try:
                py_compile.compile(path, cfile=cache / f"{index}.pyc", doraise=True)
            except py_compile.PyCompileError as exc:
                errors.append(f"cannot compile {_display_path(path)}: {exc.msg}")
    return errors


def verify_airflow_boundary(repo_root: Path) -> list[str]:
    root = repo_root.resolve()
    try:
        inventory_data = _read_json(root / "provenance/source-inventory.json")
        source_lock = _read_json(root / "provenance/source-refs.lock.json")
        records = _read_manifest(root / "provenance/source-files.jsonl")
        entries = inventory_data.get("entries")
        if not isinstance(entries, list) or not all(isinstance(entry, dict) for entry in entries):
            return ["source inventory must contain an entries list of objects"]
        airflow_entries = [entry for entry in entries if entry.get("source_id") == "airflow_weather"]
        commit = _airflow_commit(source_lock)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return [f"cannot read Airflow boundary contracts: {exc}"]

    errors = entrypoint_errors(airflow_entries)
    errors.extend(_inventory_shape_errors(airflow_entries))
    errors.extend(inventory_manifest_errors(airflow_entries, records, commit))

    python_paths: list[Path] = []
    for entry in airflow_entries:
        target_path = entry.get("target_path")
        if not isinstance(target_path, str):
            continue
        target = root / target_path
        if not target.is_file():
            errors.append(f"airflow inventory target does not exist: {target_path}")
        elif target.suffix == ".py":
            python_paths.append(target)

    errors.extend(find_forbidden_imports(python_paths))
    errors.extend(_compile_paths(python_paths))
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify the static Airflow Weather snapshot boundary.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    errors = verify_airflow_boundary(args.repo_root)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Airflow Weather snapshot boundary verified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
