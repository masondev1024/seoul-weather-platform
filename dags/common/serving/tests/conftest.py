"""Make the dags root importable so ``import common.serving`` resolves."""

from __future__ import annotations

import sys
from pathlib import Path

DAGS_ROOT = Path(__file__).resolve().parents[3]  # tests -> serving -> common -> dags root
if str(DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(DAGS_ROOT))
