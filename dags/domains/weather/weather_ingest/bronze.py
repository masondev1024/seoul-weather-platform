"""Compatibility facade for KMA Bronze persistence.

Implementation responsibilities live in focused modules:

- :mod:`bronze_contract`: schema and row contracts
- :mod:`bronze_pyiceberg`: atomic Iceberg writes and upload cleanup
- :mod:`bronze_trino`: SQL fallback writes
- :mod:`bronze_verification`: runtime verification
"""

from __future__ import annotations

from weather_ingest.bronze_contract import (
    BRONZE_TABLE,
    KMA_BRONZE_COLUMNS,
    KST,
    BronzeValidationError,
    create_kma_bronze_table,
    ensure_kma_bronze_schema,
    iter_kma_bronze_records,
    metadata_int,
    validate_kma_bronze_row_batch,
    validate_kma_row_count,
)
from weather_ingest.bronze_pyiceberg import (
    LOGGER,
    PYICEBERG_CHUNK_ROWS,
    STALE_UPLOAD_SECONDS,
    CommitFailedException,
    _abort_stale_multipart_uploads,
    _arrow_table,
    _cleanup_stale_uploads,
    _kma_pyiceberg_delete_filter,
    _pyiceberg_catalog,
    _pyiceberg_table,
    append_kma_bronze_row_batches_pyiceberg,
)
from weather_ingest.bronze_trino import (
    MAX_KMA_INSERT_QUERY_CHARS,
    insert_kma_bronze_row_batches,
    insert_kma_bronze_rows,
)
from weather_ingest.bronze_verification import verify_kma_bronze_runtime

__all__ = [
    "BRONZE_TABLE",
    "KMA_BRONZE_COLUMNS",
    "KST",
    "LOGGER",
    "MAX_KMA_INSERT_QUERY_CHARS",
    "PYICEBERG_CHUNK_ROWS",
    "STALE_UPLOAD_SECONDS",
    "BronzeValidationError",
    "CommitFailedException",
    "_abort_stale_multipart_uploads",
    "_arrow_table",
    "_cleanup_stale_uploads",
    "_kma_pyiceberg_delete_filter",
    "_pyiceberg_catalog",
    "_pyiceberg_table",
    "append_kma_bronze_row_batches_pyiceberg",
    "create_kma_bronze_table",
    "ensure_kma_bronze_schema",
    "insert_kma_bronze_row_batches",
    "insert_kma_bronze_rows",
    "iter_kma_bronze_records",
    "metadata_int",
    "validate_kma_bronze_row_batch",
    "validate_kma_row_count",
    "verify_kma_bronze_runtime",
]
