from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping


def canonical_bytes(
    value: Mapping[str, object], exclude_top_level: frozenset[str] = frozenset()
) -> bytes:
    """Serialize a mapping deterministically for local contract fingerprints."""
    included = {key: item for key, item in value.items() if key not in exclude_top_level}
    return json.dumps(
        included,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
