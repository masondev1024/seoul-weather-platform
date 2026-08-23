"""Paused-by-default one-KST-date Weather forecast-quality backfill DAG."""

import os
import sys

_DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if _DAG_DIR not in sys.path:
    sys.path.insert(0, _DAG_DIR)

from airflow import DAG  # noqa: F401 - required by Airflow safe-mode discovery
from weather_quality_dag_factory import build_quality_dag


dag = build_quality_dag(dag_id="weather_forecast_quality_backfill", backfill=True)
