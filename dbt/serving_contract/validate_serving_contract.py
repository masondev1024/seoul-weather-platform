#!/usr/bin/env python3
"""Stable CLI facade for the Serving Contract validator.

Mirrors the ``contracts/engine`` facades: supports both direct execution
(``python serving_contract/validate_serving_contract.py ...``) and import
(``from serving_contract.validate_serving_contract import main``).
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in {None, ""}:  # Support direct execution of this public facade.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from serving_contract.cli import main

__all__ = ["main"]


if __name__ == "__main__":
    raise SystemExit(main())
