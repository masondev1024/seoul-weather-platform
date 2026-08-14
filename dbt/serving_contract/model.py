"""Extract serving-contract declarations from dbt schema YAML and (optional) manifest.

The validator consumes ``ServingModel`` records, so parsing lives here and the rules
stay pure. A model "declares a serving contract" iff it has a non-empty
``config.meta.serving`` (or top-level ``meta.serving``) block.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import yaml


@dataclass
class ServingModel:
    """One dbt model's serving declaration as seen in schema YAML."""

    name: str
    source: str  # file path, for diagnostics
    meta: dict[str, Any]  # full config.meta (merged) — to detect legacy keys
    serving: dict[str, Any]  # config.meta.serving
    columns: dict[str, tuple[str, ...]]  # column name -> declared test names
    column_contracts: dict[str, dict[str, Any]] = field(default_factory=dict)
    model_tests: tuple[str, ...] = ()  # model-level test names (composite-key evidence)


@dataclass
class ManifestView:
    """Model -> declared column names, derived from a dbt manifest.json.

    ``present`` is empty when no manifest was supplied; callers must treat an
    empty view as "manifest membership not checked" rather than "no models".
    """

    columns_by_model: dict[str, set[str]] = field(default_factory=dict)
    supplied: bool = False

    def has_model(self, name: str) -> bool:
        return name in self.columns_by_model

    def columns(self, name: str) -> set[str]:
        return self.columns_by_model.get(name, set())


def _normalize_test_names(raw_tests: Any) -> tuple[str, ...]:
    """Return the test kind names on a column (``not_null``, ``unique``, ...)."""
    names: list[str] = []
    for test in raw_tests or []:
        if isinstance(test, str):
            names.append(test)
        elif isinstance(test, dict):
            names.extend(str(key) for key in test)
    return tuple(names)


def _merged_meta(model_node: dict[str, Any]) -> dict[str, Any]:
    """dbt merges top-level ``meta`` and ``config.meta``; config.meta wins."""
    top = model_node.get("meta") or {}
    config_meta = (model_node.get("config") or {}).get("meta") or {}
    if not isinstance(top, dict):
        top = {}
    if not isinstance(config_meta, dict):
        config_meta = {}
    return {**top, **config_meta}


def load_models_from_yaml(paths: Iterable[str | Path]) -> list[ServingModel]:
    """Parse dbt schema YAML files into ``ServingModel`` records.

    Only models with a ``meta.serving`` block are returned — others do not declare
    a serving contract and are out of scope for this validator.
    """
    models: list[ServingModel] = []
    for raw_path in paths:
        path = Path(raw_path)
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for node in document.get("models") or []:
            if not isinstance(node, dict):
                continue
            meta = _merged_meta(node)
            serving = meta.get("serving")
            if not isinstance(serving, dict) or not serving:
                continue
            columns: dict[str, tuple[str, ...]] = {}
            column_contracts: dict[str, dict[str, Any]] = {}
            for column in node.get("columns") or []:
                if isinstance(column, dict) and column.get("name"):
                    name = str(column["name"])
                    columns[name] = _normalize_test_names(column.get("tests"))
                    column_contracts[name] = column
            models.append(
                ServingModel(
                    name=str(node.get("name", "")),
                    source=str(path),
                    meta=meta,
                    serving=serving,
                    columns=columns,
                    column_contracts=column_contracts,
                    model_tests=_normalize_test_names(node.get("tests")),
                )
            )
    return models


def load_manifest(path: str | Path | None) -> ManifestView:
    """Read a dbt manifest.json into a ``ManifestView`` (models -> column names)."""
    if path is None:
        return ManifestView(supplied=False)
    manifest = json.loads(Path(path).read_text(encoding="utf-8"))
    columns_by_model: dict[str, set[str]] = {}
    for node in (manifest.get("nodes") or {}).values():
        if node.get("resource_type") != "model":
            continue
        name = node.get("name")
        if not name:
            continue
        columns_by_model[str(name)] = {str(c) for c in (node.get("columns") or {})}
    return ManifestView(columns_by_model=columns_by_model, supplied=True)
