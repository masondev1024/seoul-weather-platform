"""Single source of truth for ASK Seoul Airflow Trino pools."""

from __future__ import annotations

import json
from typing import Final, NamedTuple


TRINO_TRAFFIC_HEAVY_POOL: Final = "trino_traffic_heavy"
TRINO_TRAFFIC_INGEST_POOL: Final = "trino_traffic_ingest"
TRINO_TRAFFIC_TRANSFORM_POOL: Final = "trino_traffic_transform"
TRINO_TRANSIT_HEAVY_POOL: Final = "trino_transit_heavy"
TRINO_WEATHER_HEAVY_POOL: Final = "trino_weather_heavy"
TRINO_WEATHER_LEGACY_HEAVY_POOL: Final = TRINO_WEATHER_HEAVY_POOL
TRINO_WEATHER_RECOVERY_HEAVY_POOL: Final = "trino_weather_recovery_heavy"
TRINO_HEAVY_POOL: Final = "trino_heavy"
SERVING_D1_PUBLISH_POOL: Final = "serving_d1_publish"


class AirflowPoolSpec(NamedTuple):
    pool: str
    slots: int
    description: str
    include_deferred: bool


TRINO_POOL_SPECS: Final = (
    AirflowPoolSpec(
        TRINO_TRAFFIC_HEAVY_POOL,
        1,
        "Serialize Traffic Trino writes and exact tests",
        False,
    ),
    AirflowPoolSpec(
        TRINO_TRAFFIC_INGEST_POOL,
        1,
        "Serialize Traffic Bronze materialization",
        False,
    ),
    AirflowPoolSpec(
        TRINO_TRAFFIC_TRANSFORM_POOL,
        1,
        "Serialize Traffic transform and Gold writes",
        False,
    ),
    AirflowPoolSpec(
        TRINO_TRANSIT_HEAVY_POOL,
        1,
        "Serialize Transit dbt builds (fresh/heavy transform)",
        False,
    ),
    AirflowPoolSpec(
        TRINO_WEATHER_HEAVY_POOL,
        1,
        "Serialize Weather Trino writes and recovery",
        False,
    ),
    AirflowPoolSpec(
        TRINO_WEATHER_RECOVERY_HEAVY_POOL,
        1,
        "Serialize Weather observation recovery",
        False,
    ),
    AirflowPoolSpec(
        TRINO_HEAVY_POOL,
        1,
        "Serialize Trino/dbt memory-heavy tasks",
        False,
    ),
    AirflowPoolSpec(
        SERVING_D1_PUBLISH_POOL,
        1,
        "Serialize common serving Publisher writes to the shared D1 database",
        False,
    ),
)


def airflow_pool_import_payload() -> dict[str, dict[str, object]]:
    """Return fresh entries accepted by ``airflow pools import``."""
    return {
        spec.pool: {
            "slots": spec.slots,
            "description": spec.description,
            "include_deferred": spec.include_deferred,
        }
        for spec in TRINO_POOL_SPECS
    }


def main() -> int:
    print(
        json.dumps(
            airflow_pool_import_payload(),
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )
    return 0


__all__ = [
    "AirflowPoolSpec",
    "TRINO_HEAVY_POOL",
    "TRINO_POOL_SPECS",
    "TRINO_TRAFFIC_HEAVY_POOL",
    "TRINO_TRAFFIC_INGEST_POOL",
    "TRINO_TRAFFIC_TRANSFORM_POOL",
    "TRINO_TRANSIT_HEAVY_POOL",
    "TRINO_WEATHER_HEAVY_POOL",
    "TRINO_WEATHER_LEGACY_HEAVY_POOL",
    "TRINO_WEATHER_RECOVERY_HEAVY_POOL",
    "SERVING_D1_PUBLISH_POOL",
    "airflow_pool_import_payload",
]


if __name__ == "__main__":
    raise SystemExit(main())
