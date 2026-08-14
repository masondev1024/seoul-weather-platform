"""Deterministic D1 row-content identity for local publication verification."""

from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from typing import Any, Sequence

from common.serving.d1_client import Column, sqlite_type

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _require_identifier(identifier: str, *, label: str) -> None:
    if not isinstance(identifier, str) or not IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"unsafe {label}: {identifier!r}")


def _canonical_value(value: Any, d1_type: str, column: str) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple, set)):
        raise ValueError(f"{column}: unsupported nested value for D1 content hash")
    if d1_type == "INTEGER":
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        raise ValueError(f"{column}: value {value!r} cannot be converted to INTEGER")
    if d1_type == "REAL":
        if isinstance(value, bool):
            raise ValueError(f"{column}: boolean cannot be converted to REAL")
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{column}: value {value!r} cannot be converted to REAL") from exc
        if not math.isfinite(number):
            raise ValueError(f"{column}: non-finite REAL value {value!r}")
        return format(number, ".17g")
    return unicodedata.normalize("NFC", str(value))


def _canonical_row(
    row: dict[str, Any],
    *,
    columns: Sequence[tuple[str, str]],
) -> list[str | None]:
    values: list[str | None] = []
    for column, d1_type in columns:
        if column not in row:
            raise ValueError(f"absent projected column {column}")
        values.append(_canonical_value(row[column], d1_type, column))
    return values


def _canonical_primary_key(
    row: dict[str, Any],
    *,
    column_types: dict[str, str],
    primary_key: Sequence[str],
) -> tuple[str, ...]:
    values: list[str] = []
    for column in primary_key:
        if column not in row:
            raise ValueError(f"absent primary key column {column}")
        value = _canonical_value(row[column], column_types[column], column)
        if value is None:
            raise ValueError(f"primary key column {column} is null")
        values.append(value)
    return tuple(values)


def d1_content_hash(
    *,
    namespace: str,
    columns: Sequence[Column],
    rows: Sequence[dict[str, Any]],
    primary_key: Sequence[str],
) -> str:
    """Hash rows after converting values to D1/SQLite physical affinity."""

    if not primary_key:
        raise ValueError("primary_key is required for D1 content hash")
    if not namespace:
        raise ValueError("namespace is required for D1 content hash")

    d1_columns: list[tuple[str, str]] = []
    for column, trino_type in columns:
        _require_identifier(column, label="column identifier")
        d1_columns.append((column, sqlite_type(trino_type)))
    column_types = dict(d1_columns)
    for column in primary_key:
        _require_identifier(column, label="primary_key identifier")
        if column not in column_types:
            raise ValueError(f"primary_key column {column} is not in projected columns")

    keyed_rows: list[tuple[tuple[str, ...], list[str | None]]] = []
    seen_keys: set[tuple[str, ...]] = set()
    for row in rows:
        key = _canonical_primary_key(row, column_types=column_types, primary_key=primary_key)
        if key in seen_keys:
            raise ValueError(f"duplicate primary key {key}")
        seen_keys.add(key)
        keyed_rows.append((key, _canonical_row(row, columns=d1_columns)))

    payload = {
        "namespace": namespace,
        "columns": [{"name": column, "type": d1_type} for column, d1_type in d1_columns],
        "primary_key": list(primary_key),
        "rows": [row for _key, row in sorted(keyed_rows, key=lambda item: item[0])],
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()
