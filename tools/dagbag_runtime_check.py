from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any


EXPECTED_DAG_IDS = frozenset(
    {
        "common_admin_dong_bronze",
        "weather_forecast_quality_backfill",
        "weather_forecast_quality_daily",
        "weather_iceberg_maintenance",
        "weather_reference_data_refresh",
        "weather_recovery_coordinator",
        "weather_serving_export",
        "weather_serving_freshness_watchdog",
        "weather_serving_snapshot_refresh",
        "weather_ultra_srt_ncst_bronze",
        "weather_vilage_fcst_bronze",
        "weather_vilage_fcst_bronze_backfill",
        "weather_vilage_fcst_collection_slot_reconciliation",
        "weather_vilage_fcst_recollect",
        "weather_vilage_fcst_transform",
        "weather_w2_canonical_transform",
    }
)


def normalized_import_errors(errors: Mapping[str, Any]) -> list[dict[str, str]]:
    return [
        {"path": str(path), "error": str(errors[path])}
        for path in sorted(errors, key=str)
    ]


def dag_inventory_errors(actual_dag_ids: set[str]) -> list[str]:
    missing = sorted(EXPECTED_DAG_IDS - actual_dag_ids)
    unexpected = sorted(actual_dag_ids - EXPECTED_DAG_IDS)
    return [
        *(f"missing DAG id: {dag_id}" for dag_id in missing),
        *(f"unexpected DAG id: {dag_id}" for dag_id in unexpected),
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Load an Airflow DagBag without a metadata database."
    )
    parser.add_argument("--dags-folder", type=Path, required=True)
    args = parser.parse_args(argv)

    from airflow.models import DagBag

    dag_bag = DagBag(
        dag_folder=str(args.dags_folder),
        include_examples=False,
        safe_mode=True,
    )
    errors = normalized_import_errors(dag_bag.import_errors)
    dag_ids = sorted(str(dag_id) for dag_id in dag_bag.dags)
    inventory_errors = dag_inventory_errors(set(dag_ids))
    print(
        json.dumps(
            {
                "schema_version": "weather-dagbag-import/v1",
                "dag_count": len(dag_ids),
                "dag_ids": dag_ids,
                "import_errors": errors,
                "inventory_errors": inventory_errors,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 1 if errors or inventory_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
