"""Deterministic UTF-8 stdout for the serving-contract CLI.

Mirrors ``contracts/engine/artifact_io.py`` so report bytes are identical
regardless of the runner's configured stdout encoding (Windows cp949, CI utf-8).
"""

from __future__ import annotations

import sys
from typing import TextIO


def write_utf8_stdout(value: str, *, stream: TextIO | None = None) -> None:
    """Write *value* as UTF-8 bytes, independent of stdout's configured encoding."""
    target = sys.stdout if stream is None else stream
    binary_stream = getattr(target, "buffer", None)
    if binary_stream is not None:
        binary_stream.write(value.encode("utf-8"))
        binary_stream.flush()
        return
    target.write(value)
    target.flush()
