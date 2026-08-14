"""Weather landing adapters for network, object storage, and runtime wiring."""

from __future__ import annotations

import uuid
import time
import os
from datetime import datetime, timezone
from collections.abc import Iterable, Mapping
from typing import Callable, Protocol

from common.collection_slots import ExpectedSlot, is_slot_active, parse_activation_at, require_policy_boundary
from common.raw_manifest import validate_raw_manifest
from weather_ingest.common.runtime import (
    checkpoint_prefix,
    fetch_url,
    raw_prefix,
    r2_env,
    trino_cursor,
)
from weather_ingest.collection_slots import (
    weather_issue_at_kst,
    weather_manifest_covers_planned_slots,
    weather_vilage_fcst_slots,
)
from weather_ingest.kma import SOURCE_ID, build_kma_url
from weather_ingest.landing import KmaGrid, KmaLanding
from weather_ingest.run_manifest import WeatherRunManifest


KMA_RETRY_STATUSES = (429, 500, 502, 503, 504)
KMA_429_BACKOFF_SECONDS = (3600.0, 5400.0, 7200.0)
_MISSING_OBJECT_CODES = frozenset({"404", "NoSuchKey", "NotFound"})
_COLLECTION_SLOT_ACTIVATION_ENV = "ASK_SEOUL_COLLECTION_SLOT_ACTIVATION_AT"
_WEATHER_HISTORICAL_BOUNDARY_ENV = (
    "ASK_SEOUL_WEATHER_API_HUB_HISTORICAL_EARLIEST_ISSUED_AT"
)


FetchUrl = Callable[..., tuple[int, bytes]]


class S3Client(Protocol):
    def head_object(self, *, Bucket: str, Key: str): ...

    def get_object(self, *, Bucket: str, Key: str): ...

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
        IfNoneMatch: str | None = None,
    ): ...


class KmaHttpAdapter:
    def __init__(
        self,
        fetch: FetchUrl,
        *,
        delay_seconds: float = 4.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._fetch = fetch
        self._delay_seconds = delay_seconds
        self._sleep = sleep
        self._has_successful_request = False

    def fetch_page(
        self,
        *,
        base_date: str,
        base_time: str,
        nx: int,
        ny: int,
        page_no: int,
        num_of_rows: int,
    ) -> tuple[int, bytes]:
        if self._has_successful_request:
            self._sleep(self._delay_seconds)
        response = self._fetch(
            build_kma_url(
                base_date=base_date,
                base_time=base_time,
                nx=nx,
                ny=ny,
                page_no=page_no,
                num_of_rows=num_of_rows,
            ),
            "ask-seoul-kma-bronze/1.0",
            max_attempts=4,
            retry_statuses=KMA_RETRY_STATUSES,
            retry_base_delay_seconds=30,
            retry_429_backoff_seconds=KMA_429_BACKOFF_SECONDS,
        )
        self._has_successful_request = True
        return response


class R2RawObjectStore:
    def __init__(self, client: S3Client, *, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
        except Exception as exc:
            error = (getattr(exc, "response", {}) or {}).get("Error", {})
            if str(error.get("Code", "")) in _MISSING_OBJECT_CODES:
                return False
            raise
        return True

    def read_bytes(self, key: str) -> bytes:
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        return response["Body"].read()

    def write_bytes(self, key: str, payload: bytes, content_type: str) -> None:
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=payload,
            ContentType=content_type,
        )

    def write_bytes_if_absent(
        self,
        key: str,
        payload: bytes,
        content_type: str,
    ) -> bool:
        from botocore.exceptions import ClientError

        for _ in range(3):
            try:
                self._client.put_object(
                    Bucket=self._bucket,
                    Key=key,
                    Body=payload,
                    ContentType=content_type,
                    IfNoneMatch="*",
                )
                return True
            except ClientError as exc:
                response = exc.response or {}
                error = response.get("Error", {})
                code = str(error.get("Code", ""))
                status = int((response.get("ResponseMetadata", {}) or {}).get(
                    "HTTPStatusCode", 0
                ))
                if code == "PreconditionFailed" or status == 412:
                    return False
                if code == "ConditionalRequestConflict" or status == 409:
                    continue
                raise
        raise RuntimeError("R2 conditional write conflicted repeatedly")


def _build_s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=r2_env("R2_ENDPOINT"),
        aws_access_key_id=r2_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=r2_env("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    )


def build_weather_landing() -> KmaLanding:
    raw_store = R2RawObjectStore(
        _build_s3_client(),
        bucket=r2_env("R2_BUCKET_NAME"),
    )
    return KmaLanding(
        source=KmaHttpAdapter(fetch_url),
        raw_store=raw_store,
        raw_prefix=raw_prefix(),
        checkpoint_prefix=checkpoint_prefix(),
        clock=lambda: datetime.now(timezone.utc),
        request_id=lambda: str(uuid.uuid4()),
    )


def build_weather_manifest() -> WeatherRunManifest:
    return WeatherRunManifest(trino_cursor)


def _build_weather_r2_storage():
    from common.storage import build_storage

    return build_storage(
        "r2",
        bucket=r2_env("R2_BUCKET_NAME"),
        endpoint=r2_env("R2_ENDPOINT"),
        key=r2_env("R2_ACCESS_KEY_ID"),
        secret=r2_env("R2_SECRET_ACCESS_KEY"),
        region="auto",
    )


def build_weather_collection_slot_storage():
    return _build_weather_r2_storage()


def build_weather_collection_slot_receipts():
    from common.collection_slots.receipts import CollectionSlotReceipts

    return CollectionSlotReceipts(build_weather_collection_slot_storage())


class _NoOpCollectionSlotReceipts:
    def record_expected(self, _slot: ExpectedSlot) -> str:
        return "collection-slot-receipts/noop"

    def record_outcome(self, _outcome) -> str:
        return "collection-slot-receipts/noop"


def weather_collection_slots_for_issue(
    base_date: object,
    base_time: object,
    grids: Iterable[KmaGrid],
) -> tuple[ExpectedSlot, ...]:
    """Return active KMA expected slots without inventing a historical boundary."""
    activation_at = parse_activation_at(
        os.environ.get(_COLLECTION_SLOT_ACTIVATION_ENV)
    )
    issue_at = weather_issue_at_kst(base_date, base_time)
    if not is_slot_active(issue_at, activation_at):
        return ()
    recovery_boundary = require_policy_boundary(
        os.environ.get(_WEATHER_HISTORICAL_BOUNDARY_ENV),
        _WEATHER_HISTORICAL_BOUNDARY_ENV,
    )
    return weather_vilage_fcst_slots(
        base_date,
        base_time,
        grids,
        recovery_boundary=recovery_boundary,
    )


def build_weather_collection_slot_receipt_ports():
    activation_at = parse_activation_at(
        os.environ.get(_COLLECTION_SLOT_ACTIVATION_ENV)
    )
    if activation_at is None:
        return _NoOpCollectionSlotReceipts(), weather_collection_slots_for_issue
    return build_weather_collection_slot_receipts(), weather_collection_slots_for_issue


def weather_raw_manifest_is_verified(
    raw_result: object,
    *,
    dag_run_id: str,
    slots: Iterable[ExpectedSlot] | None = None,
) -> bool:
    """Return true only for a complete manifest matching this run's raw objects.

    A non-empty diagnostic response object is intentionally insufficient: only a
    validated completed manifest may authorize a raw-replay recovery outcome.
    """
    if not isinstance(raw_result, Mapping):
        return False
    manifest_key = raw_result.get("manifest_key")
    raw_objects = raw_result.get("raw_objects")
    if not isinstance(manifest_key, str) or not manifest_key:
        return False
    if not isinstance(raw_objects, list) or not raw_objects:
        return False
    object_keys: list[str] = []
    for raw_object in raw_objects:
        if not isinstance(raw_object, Mapping):
            return False
        raw_object_key = raw_object.get("raw_object_key")
        if not isinstance(raw_object_key, str) or not raw_object_key:
            return False
        object_keys.append(raw_object_key)
    if len(object_keys) != len(set(object_keys)):
        return False
    try:
        manifest = build_weather_collection_slot_storage().read_json(manifest_key)
        validate_raw_manifest(
            manifest,
            run_id=dag_run_id,
            dataset=SOURCE_ID,
            object_keys=object_keys,
        )
    except (FileNotFoundError, TypeError, ValueError):
        return False
    if slots is not None:
        try:
            if not weather_manifest_covers_planned_slots(object_keys, slots):
                return False
        except (TypeError, ValueError):
            return False
    return True
