"""Shared runtime guards for Weather W2 canonical dbt workloads."""

from __future__ import annotations

import os

from weather_ingest.common.runtime import sql_identifier, trino_cursor


class AdminDongCrosswalkSnapshotUnavailableError(RuntimeError):
    """The shared admin_dong crosswalk Iceberg table has no usable snapshot."""


def resolve_admin_dong_crosswalk_snapshot_id() -> int:
    """Return the latest positive Iceberg snapshot ID for the canonical crosswalk."""
    cursor, catalog, _ = trino_cursor()
    schema = sql_identifier(os.environ.get("COMMON_SCHEMA", "common"))
    table = sql_identifier("seoul_admin_dong_crosswalk")
    cursor.execute(
        "SELECT snapshot_id "
        f'FROM {catalog}.{schema}."{table}$snapshots" '
        "ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
    )
    row = cursor.fetchone()
    try:
        snapshot_id = row[0]
    except (TypeError, IndexError) as exc:
        raise AdminDongCrosswalkSnapshotUnavailableError(
            "admin_dong crosswalk Iceberg snapshot is unavailable"
        ) from exc
    if (
        isinstance(snapshot_id, bool)
        or not isinstance(snapshot_id, int)
        or snapshot_id <= 0
    ):
        raise AdminDongCrosswalkSnapshotUnavailableError(
            "admin_dong crosswalk Iceberg snapshot ID must be a positive integer"
        )
    return snapshot_id
