"""Weather-owned Bronze run-manifest Module."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Protocol

from weather_ingest.errors import WeatherCompletenessError


MANIFEST_TABLE = "bronze_collection_run_manifest"
SOURCE_ID = "kma_vilage_fcst"
STATUS_STARTED = "STARTED"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_COALESCED = "COALESCED"


class RunNotPublishableError(RuntimeError):
    """The requested Weather Bronze snapshot is not publishable."""

__all__ = [
    "MANIFEST_TABLE",
    "SOURCE_ID",
    "STATUS_STARTED",
    "STATUS_SUCCESS",
    "STATUS_FAILED",
    "STATUS_COALESCED",
    "RunNotPublishableError",
    "WeatherRun",
    "WeatherRunManifest",
    "create_bronze_run_manifest_table",
    "failure_reason_from_context",
    "record_bronze_run_event",
    "sql_bool",
    "sql_int",
    "sql_string",
    "sql_timestamp",
]


class Cursor(Protocol):
    def execute(self, statement: str) -> None: ...

    def fetchone(self): ...


CursorFactory = Callable[[], tuple[Cursor, str, str]]


@dataclass(frozen=True)
class WeatherRun:
    dag_id: str
    run_id: str


class WeatherRunManifest:
    def __init__(self, cursor_factory: CursorFactory) -> None:
        self._cursor_factory = cursor_factory

    def start(
        self,
        run: WeatherRun,
        *,
        expected_raw_objects: int | None = None,
    ) -> str:
        return self._record(
            run,
            status=STATUS_STARTED,
            is_publishable=False,
            expected_rows=None,
            actual_rows=None,
            expected_raw_objects=expected_raw_objects,
            actual_raw_objects=None,
            failure_reason=None,
        )

    def publish(
        self,
        run: WeatherRun,
        *,
        expected_rows: int,
        actual_rows: int,
        expected_raw_objects: int,
        actual_raw_objects: int,
    ) -> str:
        return self.complete(
            run,
            expected_rows=expected_rows,
            actual_rows=actual_rows,
            expected_raw_objects=expected_raw_objects,
            actual_raw_objects=actual_raw_objects,
            is_publishable=True,
        )

    def require_publishable(self, run_id: str) -> str:
        cursor, catalog, schema = self._cursor_factory()
        cursor.execute(
            f"""
            SELECT dag_run_id
            FROM {catalog}.{schema}.{MANIFEST_TABLE}
            WHERE source_id = {sql_string(SOURCE_ID)}
              AND status = {sql_string(STATUS_SUCCESS)}
              AND is_publishable
              AND dag_run_id = {sql_string(run_id)}
              AND NOT EXISTS (
                  SELECT 1
                  FROM {catalog}.{schema}.{MANIFEST_TABLE} AS coalesced
                  WHERE coalesced.source_id = {sql_string(SOURCE_ID)}
                    AND coalesced.dag_run_id = {sql_string(run_id)}
                    AND coalesced.status = {sql_string(STATUS_COALESCED)}
              )
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        if row is None:
            raise RunNotPublishableError(
                f"Weather snapshot is not publishable: {run_id}"
            )
        return str(row[0])

    def complete(
        self,
        run: WeatherRun,
        *,
        expected_rows: int,
        actual_rows: int,
        expected_raw_objects: int,
        actual_raw_objects: int,
        is_publishable: bool,
    ) -> str:
        if is_publishable and (
            expected_rows != actual_rows or expected_raw_objects != actual_raw_objects
        ):
            raise WeatherCompletenessError(
                "KMA publishable run requires exact row and raw-object counts: "
                f"rows={actual_rows}/{expected_rows}, "
                f"raw_objects={actual_raw_objects}/{expected_raw_objects}"
            )
        return self._record(
            run,
            status=STATUS_SUCCESS,
            is_publishable=is_publishable,
            expected_rows=expected_rows,
            actual_rows=actual_rows,
            expected_raw_objects=expected_raw_objects,
            actual_raw_objects=actual_raw_objects,
            failure_reason=None,
        )

    def coalesce(self, run_id: str, *, replacement_run_id: str) -> str:
        return self._record(
            WeatherRun("weather_vilage_fcst_bronze", run_id),
            status=STATUS_COALESCED,
            is_publishable=False,
            expected_rows=None,
            actual_rows=None,
            expected_raw_objects=None,
            actual_raw_objects=None,
            failure_reason=f"replaced_by={replacement_run_id}",
        )

    def fail(
        self,
        run: WeatherRun,
        *,
        task_id: str,
        error: BaseException,
        expected_raw_objects: int | None = None,
        actual_raw_objects: int | None = None,
    ) -> str:
        return self._record(
            run,
            status=STATUS_FAILED,
            is_publishable=False,
            expected_rows=None,
            actual_rows=None,
            expected_raw_objects=expected_raw_objects,
            actual_raw_objects=actual_raw_objects,
            failure_reason=f"{type(error).__name__} in {task_id}",
        )

    def _record(
        self,
        run: WeatherRun,
        *,
        status: str,
        is_publishable: bool,
        expected_rows: int | None,
        actual_rows: int | None,
        expected_raw_objects: int | None,
        actual_raw_objects: int | None,
        failure_reason: str | None,
    ) -> str:
        cursor, catalog, schema = self._cursor_factory()
        return record_bronze_run_event(
            cursor,
            catalog,
            schema,
            source_id=SOURCE_ID,
            dag_id=run.dag_id,
            dag_run_id=run.run_id,
            status=status,
            is_publishable=is_publishable,
            expected_rows=expected_rows,
            actual_rows=actual_rows,
            expected_raw_objects=expected_raw_objects,
            actual_raw_objects=actual_raw_objects,
            failure_reason=failure_reason,
        )


def sql_string(value: object | None) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def sql_int(value: object | None) -> str:
    return "NULL" if value is None or value == "" else str(int(value))


def sql_bool(value: bool) -> str:
    return "true" if value else "false"


def sql_timestamp(value: datetime | None) -> str:
    if value is None:
        return "NULL"
    utc_value = value.astimezone(timezone.utc)
    return "TIMESTAMP " + sql_string(utc_value.strftime("%Y-%m-%d %H:%M:%S.%f"))


def create_bronze_run_manifest_table(cursor: Cursor, catalog: str, schema: str) -> str:
    qualified_schema = f"{catalog}.{schema}"
    qualified_table = f"{qualified_schema}.{MANIFEST_TABLE}"
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")
    except Exception as exc:
        if "Namespace already exists" not in str(exc):
            raise
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            source_id varchar,
            dag_id varchar,
            dag_run_id varchar,
            status varchar,
            is_publishable boolean,
            event_at timestamp(6),
            expected_rows integer,
            actual_rows integer,
            expected_raw_objects integer,
            actual_raw_objects integer,
            failure_reason varchar
        )
        WITH (format = 'PARQUET')
        """
    )
    return qualified_table


def record_bronze_run_event(
    cursor: Cursor,
    catalog: str,
    schema: str,
    *,
    source_id: str,
    dag_id: str,
    dag_run_id: str,
    status: str,
    is_publishable: bool = False,
    expected_rows: int | None = None,
    actual_rows: int | None = None,
    expected_raw_objects: int | None = None,
    actual_raw_objects: int | None = None,
    failure_reason: str | None = None,
    event_at: datetime | None = None,
) -> str:
    """Record one idempotent manifest event through the canonical atomic mutation."""
    qualified_table = create_bronze_run_manifest_table(cursor, catalog, schema)
    values = ", ".join(
        (
            sql_string(source_id),
            sql_string(dag_id),
            sql_string(dag_run_id),
            sql_string(status),
            sql_bool(is_publishable),
            sql_timestamp(event_at or datetime.now(timezone.utc)),
            sql_int(expected_rows),
            sql_int(actual_rows),
            sql_int(expected_raw_objects),
            sql_int(actual_raw_objects),
            sql_string(failure_reason),
        )
    )
    columns = (
        "source_id, dag_id, dag_run_id, status, is_publishable, event_at, "
        "expected_rows, actual_rows, expected_raw_objects, actual_raw_objects, "
        "failure_reason"
    )
    cursor.execute(
        f"""
        MERGE INTO {qualified_table} AS target
        USING (VALUES ({values})) AS incoming ({columns})
          ON target.source_id = incoming.source_id
         AND target.dag_run_id = incoming.dag_run_id
         AND target.status = incoming.status
        WHEN MATCHED THEN UPDATE SET
            dag_id = incoming.dag_id,
            is_publishable = incoming.is_publishable,
            event_at = incoming.event_at,
            expected_rows = incoming.expected_rows,
            actual_rows = incoming.actual_rows,
            expected_raw_objects = incoming.expected_raw_objects,
            actual_raw_objects = incoming.actual_raw_objects,
            failure_reason = incoming.failure_reason
        WHEN NOT MATCHED THEN INSERT ({columns})
        VALUES (
            incoming.source_id, incoming.dag_id, incoming.dag_run_id, incoming.status,
            incoming.is_publishable, incoming.event_at, incoming.expected_rows,
            incoming.actual_rows, incoming.expected_raw_objects,
            incoming.actual_raw_objects, incoming.failure_reason
        )
        """
    )
    return qualified_table


def failure_reason_from_context(context: dict) -> str:
    task = context.get("task_instance")
    task_id = getattr(task, "task_id", "unknown_task")
    exception = context.get("exception")
    exception_name = type(exception).__name__ if exception else "unknown_error"
    return f"{exception_name} in {task_id}"
