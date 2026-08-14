"""Deterministic identity helpers for public serving projections."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def _column_meta(column: dict[str, Any]) -> dict[str, Any]:
    meta = ((column.get("config") or {}).get("meta") or {}) if isinstance(column, dict) else {}
    return {
        "data_type": str(column.get("data_type", "")).strip().lower(),
        "nullable": meta.get("nullable"),
        "semantic_role": meta.get("semantic_role"),
        "unit": meta.get("unit"),
    }


def canonical_projection_bytes(projection: dict[str, Any], columns: dict[str, dict[str, Any]]) -> bytes:
    """Return deterministic UTF-8 JSON bytes for the ordered public projection.

    Descriptions are intentionally excluded so editorial copy updates do not change
    the D1/public schema identity.
    """
    payload = {
        "schema_version": projection["schema_version"],
        "columns": [
            {
                "name": column_name,
                **_column_meta(columns[column_name]),
            }
            for column_name in projection["columns"]
        ],
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def projection_schema_hash(projection: dict[str, Any], columns: dict[str, dict[str, Any]]) -> str:
    """Return the SHA-256 hex digest of the canonical projection identity."""
    return hashlib.sha256(canonical_projection_bytes(projection, columns)).hexdigest()
