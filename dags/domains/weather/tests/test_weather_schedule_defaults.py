from __future__ import annotations

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def test_kma_bronze_defaults_to_publish_cadence_in_prod(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "prod")
    monkeypatch.delenv("ASK_SEOUL_KMA_DAG_SCHEDULE", raising=False)

    from weather_ingest.bronze_dag_support import (
        KMA_PUBLISH_CRON_KST,
        kma_dag_schedule,
    )

    assert kma_dag_schedule() == KMA_PUBLISH_CRON_KST
