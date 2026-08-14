from __future__ import annotations

import ast
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPOSITORY_ROOT / "common" / "pools.py"
EXPECTED_POOL_IMPORT_PAYLOAD = {
    "trino_traffic_heavy": {
        "slots": 1,
        "description": "Serialize Traffic Trino writes and exact tests",
        "include_deferred": False,
    },
    "trino_traffic_ingest": {
        "slots": 1,
        "description": "Serialize Traffic Bronze materialization",
        "include_deferred": False,
    },
    "trino_traffic_transform": {
        "slots": 1,
        "description": "Serialize Traffic transform and Gold writes",
        "include_deferred": False,
    },
    "trino_transit_heavy": {
        "slots": 1,
        "description": "Serialize Transit dbt builds (fresh/heavy transform)",
        "include_deferred": False,
    },
    "trino_weather_heavy": {
        "slots": 1,
        "description": "Serialize Weather Trino writes and recovery",
        "include_deferred": False,
    },
    "trino_weather_legacy_heavy": {
        "slots": 1,
        "description": "Serialize legacy Weather transform writes",
        "include_deferred": False,
    },
    "trino_weather_recovery_heavy": {
        "slots": 1,
        "description": "Serialize Weather observation recovery",
        "include_deferred": False,
    },
    "trino_heavy": {
        "slots": 1,
        "description": "Serialize Trino/dbt memory-heavy tasks",
        "include_deferred": False,
    },
    "serving_d1_publish": {
        "slots": 1,
        "description": "Serialize common serving Publisher writes to the shared D1 database",
        "include_deferred": False,
    },
}


def _load_registry():
    if not REGISTRY_PATH.is_file():
        raise AssertionError(f"pool registry is missing: {REGISTRY_PATH}")
    spec = importlib.util.spec_from_file_location(
        "pool_registry_under_test", REGISTRY_PATH
    )
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load pool registry: {REGISTRY_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _assignment_names(node: ast.Assign | ast.AnnAssign) -> set[str]:
    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
    names: set[str] = set()
    for target in targets:
        if isinstance(target, ast.Name):
            names.add(target.id)
        elif isinstance(target, (ast.Tuple, ast.List)):
            names.update(
                item.id for item in target.elts if isinstance(item, ast.Name)
            )
    return names


def _custom_pool_literals(tree: ast.AST) -> list[ast.Constant]:
    literals: list[ast.Constant] = []
    for node in ast.walk(tree):
        candidate: ast.AST | None = None
        if isinstance(node, ast.keyword) and node.arg == "pool":
            candidate = node.value
        elif isinstance(node, (ast.Assign, ast.AnnAssign)) and any(
            name.endswith("_POOL") for name in _assignment_names(node)
        ):
            candidate = node.value
        elif isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values, strict=True):
                if (
                    isinstance(key, ast.Constant)
                    and key.value == "pool"
                    and isinstance(value, ast.Constant)
                    and isinstance(value.value, str)
                    and value.value.startswith("trino_")
                ):
                    literals.append(value)
        if (
            isinstance(candidate, ast.Constant)
            and isinstance(candidate.value, str)
            and candidate.value.startswith("trino_")
        ):
            literals.append(candidate)
    return literals


class PoolRegistryTest(unittest.TestCase):
    def test_registry_payload_is_exact(self):
        registry = _load_registry()

        self.assertEqual(
            registry.airflow_pool_import_payload(),
            EXPECTED_POOL_IMPORT_PAYLOAD,
        )

    def test_weather_and_traffic_pool_constants_are_explicit(self):
        registry = _load_registry()

        self.assertEqual(
            {
                registry.TRINO_TRAFFIC_HEAVY_POOL,
                registry.TRINO_TRAFFIC_INGEST_POOL,
                registry.TRINO_TRAFFIC_TRANSFORM_POOL,
                registry.TRINO_TRANSIT_HEAVY_POOL,
                registry.TRINO_WEATHER_HEAVY_POOL,
                registry.TRINO_WEATHER_LEGACY_HEAVY_POOL,
                registry.TRINO_WEATHER_RECOVERY_HEAVY_POOL,
                registry.TRINO_HEAVY_POOL,
                registry.SERVING_D1_PUBLISH_POOL,
            },
            set(EXPECTED_POOL_IMPORT_PAYLOAD),
        )

    def test_pool_names_are_unique(self):
        registry = _load_registry()
        names = [spec.pool for spec in registry.TRINO_POOL_SPECS]

        self.assertEqual(len(names), len(set(names)))

    def test_cli_emits_deterministic_airflow_import_json(self):
        result = subprocess.run(
            [sys.executable, str(REGISTRY_PATH)],
            cwd=REPOSITORY_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        expected_stdout = (
            json.dumps(
                EXPECTED_POOL_IMPORT_PAYLOAD,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            + "\n"
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertEqual(result.stdout, expected_stdout)
        self.assertEqual(json.loads(result.stdout), EXPECTED_POOL_IMPORT_PAYLOAD)

    def test_production_custom_pool_literals_are_registered(self):
        registry = _load_registry()
        payload = registry.airflow_pool_import_payload()
        self.assertIsInstance(payload, dict)
        registered = set(payload)
        unregistered: list[str] = []

        for source_path in sorted(REPOSITORY_ROOT.rglob("*.py")):
            relative_path = source_path.relative_to(REPOSITORY_ROOT)
            if (
                source_path == REGISTRY_PATH
                or "tests" in relative_path.parts
                or "docs" in relative_path.parts
                or source_path.name.startswith("test_")
            ):
                continue
            tree = ast.parse(source_path.read_text(encoding="utf-8"), source_path)
            for literal in _custom_pool_literals(tree):
                if literal.value not in registered:
                    unregistered.append(
                        f"{relative_path}:{literal.lineno}: {literal.value}"
                    )

        self.assertEqual(unregistered, [], "\n".join(unregistered))


if __name__ == "__main__":
    unittest.main()
