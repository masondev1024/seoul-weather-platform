"""PyIceberg persistence for validated KMA Bronze batches."""

from __future__ import annotations

import logging
import os
from time import sleep

from weather_ingest.bronze_contract import (
    BRONZE_TABLE,
    KMA_BRONZE_COLUMNS,
    iter_kma_bronze_records,
    validate_kma_bronze_row_batch,
)
from weather_ingest.common.runtime import r2_env, r2_env_name
from weather_ingest.errors import WeatherBronzeConfigurationError
from weather_ingest.kma import SOURCE_ID


LOGGER = logging.getLogger(__name__)
PYICEBERG_CHUNK_ROWS = 50_000

try:
    from pyiceberg.exceptions import CommitFailedException
except Exception:  # PyIceberg is optional while SQL-only helpers are imported.
    CommitFailedException = Exception


def _pyiceberg_catalog():
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        "weather",
        uri=r2_env("R2_DATA_CATALOG_URI"),
        warehouse=r2_env("R2_DATA_CATALOG_WAREHOUSE"),
        token=r2_env("R2_DATA_CATALOG_TOKEN"),
        **{
            "s3.endpoint": r2_env("R2_ENDPOINT"),
            "s3.access-key-id": r2_env("R2_ACCESS_KEY_ID"),
            "s3.secret-access-key": r2_env("R2_SECRET_ACCESS_KEY"),
            "s3.region": os.environ.get(r2_env_name("R2_REGION"), "auto"),
            # Bound slow object-store requests so Airflow retries remain useful.
            # A failed append is retried by the outer optimistic-lock loop.
            "s3.request-timeout": os.environ.get(
                r2_env_name("R2_S3_REQUEST_TIMEOUT"), "10"
            ),
        },
    )


# Abort only old multipart uploads; active concurrent writers remain untouched.
STALE_UPLOAD_SECONDS = 3600.0


def _abort_stale_multipart_uploads(
    client,
    bucket: str,
    prefix: str,
    *,
    older_than_seconds: float = STALE_UPLOAD_SECONDS,
    now=None,
) -> int:
    from datetime import datetime, timezone

    now = now or datetime.now(timezone.utc)
    aborted = 0
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    while True:
        response = client.list_multipart_uploads(**kwargs)
        for upload in response.get("Uploads") or []:
            age_seconds = (now - upload["Initiated"]).total_seconds()
            if age_seconds < older_than_seconds:
                continue
            client.abort_multipart_upload(
                Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"]
            )
            aborted += 1
            print(
                f"Aborted stale multipart upload (age={age_seconds:.0f}s): {upload['Key']}"
            )
        if not response.get("IsTruncated"):
            break
        kwargs["KeyMarker"] = response.get("NextKeyMarker")
        kwargs["UploadIdMarker"] = response.get("NextUploadIdMarker")
    return aborted


def _cleanup_stale_uploads(table) -> None:
    # Cleanup is best-effort and must never block the Bronze append.
    try:
        import boto3

        location = table.location()
        bucket, _, prefix = location.removeprefix("s3://").partition("/")
        client = boto3.client(
            "s3",
            endpoint_url=r2_env("R2_ENDPOINT"),
            aws_access_key_id=r2_env("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=r2_env("R2_SECRET_ACCESS_KEY"),
        )
        _abort_stale_multipart_uploads(client, bucket, prefix)
    except Exception as exc:  # Cleanup failure must not block a valid write.
        LOGGER.warning(
            "KMA Bronze best-effort cleanup failed: "
            "operation=cleanup_stale_multipart_uploads error_type=%s",
            type(exc).__name__,
        )


def _pyiceberg_table(schema: str):
    table = _pyiceberg_catalog().load_table(f"{schema}.{BRONZE_TABLE}")
    _cleanup_stale_uploads(table)
    return table


def _kma_pyiceberg_delete_filter(dag_run_id: str):
    from pyiceberg.expressions import And, EqualTo

    return And(EqualTo("source_id", SOURCE_ID), EqualTo("dag_run_id", dag_run_id))


def _arrow_table(rows: list[dict]):
    import pyarrow as pa

    types = {
        "nx": pa.int32(),
        "ny": pa.int32(),
        "http_status": pa.int32(),
        "total_count": pa.int32(),
        "item_count": pa.int32(),
        "page_no": pa.int32(),
        "collected_at": pa.timestamp("us"),
    }
    fields = []
    arrays = []
    for column in KMA_BRONZE_COLUMNS:
        arrow_type = types.get(column, pa.string())
        fields.append(pa.field(column, arrow_type))
        arrays.append(pa.array([row[column] for row in rows], type=arrow_type))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def append_kma_bronze_row_batches_pyiceberg(
    schema: str,
    row_batches: list[dict],
    dag_run_id: str,
    *,
    delete_existing: bool = True,
    chunk_rows: int = PYICEBERG_CHUNK_ROWS,
    max_retry_attempts: int = 4,
    retry_base_delay_seconds: float = 3.0,
    table=None,
) -> int:
    if not row_batches:
        return 0
    if chunk_rows <= 0:
        raise WeatherBronzeConfigurationError(
            f"chunk_rows must be positive: {chunk_rows}"
        )
    if max_retry_attempts <= 0:
        raise WeatherBronzeConfigurationError(
            f"max_retry_attempts must be positive: {max_retry_attempts}"
        )
    for batch in row_batches:
        validate_kma_bronze_row_batch(batch)

    def append_once(iceberg_table) -> int:
        total = 0
        chunk: list[dict] = []

        with iceberg_table.transaction() as txn:
            if delete_existing:
                txn.delete(_kma_pyiceberg_delete_filter(dag_run_id))

            def flush() -> None:
                nonlocal total
                if not chunk:
                    return
                txn.append(_arrow_table(chunk))
                total += len(chunk)
                chunk.clear()

            for record in iter_kma_bronze_records(row_batches, dag_run_id):
                chunk.append(record)
                if len(chunk) >= chunk_rows:
                    flush()
            flush()
        return total

    for attempt in range(1, max_retry_attempts + 1):
        iceberg_table = table or _pyiceberg_table(schema)
        try:
            return append_once(iceberg_table)
        except CommitFailedException as exc:
            if attempt >= max_retry_attempts:
                raise
            if table is None:
                try:
                    iceberg_table.refresh()
                except Exception as refresh_exc:
                    LOGGER.warning(
                        "KMA Bronze best-effort recovery failed: "
                        "operation=refresh_after_commit_conflict error_type=%s",
                        type(refresh_exc).__name__,
                    )
            delay = min(retry_base_delay_seconds * (2 ** (attempt - 1)), 30.0)
            print(
                "Retrying KMA bronze append after optimistic lock conflict "
                f"(attempt {attempt}/{max_retry_attempts}) in {delay:.1f}s: {type(exc).__name__}"
            )
            sleep(delay)
