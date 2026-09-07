from __future__ import annotations

import pytest

from common.recovery.admission import KMA_API_POOL, TRINO_WEATHER_HEAVY_POOL
from common.recovery.airflow_snapshot import (
    AirflowSnapshotError,
    snapshot_from_metadata,
)


def _stats(*, queued: int = 0, running: int = 0) -> dict[str, dict[str, object]]:
    return {
        KMA_API_POOL: {
            "total": 1,
            "running": running,
            "deferred": 0,
            "queued": queued,
            "scheduled": 0,
        },
        TRINO_WEATHER_HEAVY_POOL: {
            "total": 1,
            "running": running,
            "deferred": 0,
            "queued": queued,
            "scheduled": 0,
        },
    }


def test_snapshot_conversion_normalizes_runs_and_pool_pressure() -> None:
    runs, pools = snapshot_from_metadata(
        [
            ("weather_vilage_fcst_transform", "run-2", "running"),
            ("weather_vilage_fcst_bronze", "run-1", "queued"),
        ],
        _stats(queued=1),
        pool_names=(KMA_API_POOL, TRINO_WEATHER_HEAVY_POOL),
    )

    assert [(run.dag_id, run.run_id) for run in runs] == [
        ("weather_vilage_fcst_bronze", "run-1"),
        ("weather_vilage_fcst_transform", "run-2"),
    ]
    assert pools[TRINO_WEATHER_HEAVY_POOL].queued_tasks == 1
    assert pools[KMA_API_POOL].available_slots == 1


def test_missing_pool_is_preserved_as_absent_for_admission_to_reject() -> None:
    _runs, pools = snapshot_from_metadata(
        [],
        _stats(),
        pool_names=(KMA_API_POOL, "pool_not_returned"),
    )

    assert KMA_API_POOL in pools
    assert "pool_not_returned" not in pools


@pytest.mark.parametrize(
    ("rows", "stats", "message"),
    [
        (
            [("weather_vilage_fcst_bronze", "run-1", "success")],
            _stats(),
            "inactive",
        ),
        (
            [
                ("weather_vilage_fcst_bronze", "run-1", "running"),
                ("weather_vilage_fcst_bronze", "run-1", "running"),
            ],
            _stats(),
            "duplicate",
        ),
        (
            [],
            {
                TRINO_WEATHER_HEAVY_POOL: {
                    "total": float("inf"),
                    "running": 0,
                    "deferred": 0,
                    "queued": 0,
                    "scheduled": 0,
                }
            },
            "outside bounds",
        ),
        (
            [],
            {
                TRINO_WEATHER_HEAVY_POOL: {
                    "total": 1,
                    "running": 2,
                    "deferred": 0,
                    "queued": 0,
                    "scheduled": 0,
                }
            },
            "exceed",
        ),
    ],
)
def test_malformed_snapshot_fails_closed(rows, stats, message) -> None:
    with pytest.raises(AirflowSnapshotError, match=message):
        snapshot_from_metadata(
            rows,
            stats,
            pool_names=(TRINO_WEATHER_HEAVY_POOL,),
        )


def test_duplicate_requested_names_and_non_sequence_fail_closed() -> None:
    with pytest.raises(AirflowSnapshotError, match="duplicates"):
        snapshot_from_metadata([], _stats(), pool_names=(KMA_API_POOL, KMA_API_POOL))
    with pytest.raises(AirflowSnapshotError, match="sequence"):
        snapshot_from_metadata([], _stats(), pool_names=KMA_API_POOL)  # type: ignore[arg-type]
