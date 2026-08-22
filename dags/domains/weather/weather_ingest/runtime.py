"""Weather landing adapters for network, object storage, and runtime wiring."""

from __future__ import annotations

import base64
import hashlib
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
    download_raw_object,
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
from weather_ingest.kma_coordination import (
    PhysicalAttempt,
    PhysicalAttemptBudgetHook,
    SqliteAttemptLedger,
    shared_guards_enabled,
)
from weather_ingest.landing import KmaGrid, KmaLanding
from weather_ingest.raw_spool import RawPayloadSpool, configured_raw_payload_spool
from weather_ingest.run_manifest import WeatherRunManifest


KMA_RETRY_STATUSES = (429, 500, 502, 503, 504)
# 429 재시도 대기. weather_vilage_fcst_bronze 의 dagrun_timeout 이 60분이라
# 이 값들의 합이 60분을 넘으면 backoff 가 끝나기 전에 run 이 죽어서 "재시도"가
# 실제로는 존재하지 않는다. 예전 값 (3600, 5400, 7200) 은 합이 4.5시간이라
# 첫 대기조차 완주할 수 없었다 - 429 를 맞으면 그냥 죽는 것과 같았다.
# 수집 자체가 5분 안팎이므로 합 9분이면 최악의 경우에도 timeout 안에 들어온다.
KMA_429_BACKOFF_SECONDS = (60.0, 180.0, 300.0)
# 요청 사이 간격. 승인된 운영 계정에서 4초는 과보호였다 (2026-08-16 측정:
# 동시 4·8 요청 모두 429 0건, API 평균 응답 1.274초, land 518초 중 316초가 sleep).
# 재배포 없이 되돌릴 수 있도록 환경변수로 덮어쓸 수 있게 둔다.
KMA_REQUEST_DELAY_SECONDS_ENV = "ASK_SEOUL_KMA_REQUEST_DELAY_SECONDS"
DEFAULT_KMA_REQUEST_DELAY_SECONDS = 1.0
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
        Metadata: dict[str, str] | None = None,
        ContentMD5: str | None = None,
    ): ...


def kma_request_delay_seconds() -> float:
    """Return the inter-request delay, honouring the ops override.

    잘못된 값은 조용히 기본값으로 되돌리지 않고 실패시킨다. 오타 하나로 딜레이가
    말없이 바뀌면 429 를 맞고 나서야 알게 된다.
    """
    raw = os.environ.get(KMA_REQUEST_DELAY_SECONDS_ENV)
    if raw is None or not raw.strip():
        return DEFAULT_KMA_REQUEST_DELAY_SECONDS
    try:
        delay = float(raw)
    except ValueError as error:
        raise ValueError(
            f"{KMA_REQUEST_DELAY_SECONDS_ENV} must be a number, got {raw!r}"
        ) from error
    if delay < 0:
        raise ValueError(
            f"{KMA_REQUEST_DELAY_SECONDS_ENV} must not be negative, got {raw!r}"
        )
    return delay


class KmaHttpAdapter:
    def __init__(
        self,
        fetch: FetchUrl,
        *,
        delay_seconds: float = DEFAULT_KMA_REQUEST_DELAY_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
        before_attempt: Callable[[PhysicalAttempt], object] | None = None,
        dag_run_id: str = "forecast-runtime",
        request_id: Callable[[], str] = lambda: str(uuid.uuid4()),
    ) -> None:
        self._fetch = fetch
        self._delay_seconds = delay_seconds
        self._sleep = sleep
        self._has_successful_request = False
        self._before_attempt = before_attempt
        self._dag_run_id = dag_run_id
        self._request_id = request_id

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
        fetch_options = {
            "max_attempts": 4,
            "retry_statuses": KMA_RETRY_STATUSES,
            "retry_base_delay_seconds": 30,
            "retry_429_backoff_seconds": KMA_429_BACKOFF_SECONDS,
        }
        if self._before_attempt is not None:
            logical_request_id = self._request_id()

            def reserve(attempt_ordinal: int) -> None:
                material = "\x1f".join(
                    (
                        SOURCE_ID,
                        self._dag_run_id,
                        base_date,
                        base_time,
                        str(nx),
                        str(ny),
                        str(page_no),
                        logical_request_id,
                        str(attempt_ordinal),
                    )
                )
                self._before_attempt(
                    PhysicalAttempt(
                        reservation_id=hashlib.sha256(
                            material.encode("utf-8")
                        ).hexdigest(),
                        source_id=SOURCE_ID,
                        dag_run_id=self._dag_run_id,
                        observed_slot=f"{base_date}T{base_time}",
                        nx=nx,
                        ny=ny,
                        attempt_ordinal=attempt_ordinal,
                    )
                )

            fetch_options["before_attempt"] = reserve
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
            **fetch_options,
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

    def read_sha256(self, key: str) -> str | None:
        response = self._client.head_object(Bucket=self._bucket, Key=key) or {}
        metadata = response.get("Metadata") or {}
        value = metadata.get("sha256")
        return str(value) if value is not None else None

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
                    Metadata={"sha256": hashlib.sha256(payload).hexdigest()},
                    ContentMD5=base64.b64encode(
                        hashlib.md5(payload, usedforsecurity=False).digest()
                    ).decode("ascii"),
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


def _r2_client_config():
    from botocore.config import Config

    return Config(
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )


def _build_s3_client(
    *,
    endpoint_url: str | None = None,
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
):
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url or r2_env("R2_ENDPOINT"),
        aws_access_key_id=access_key_id or r2_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=secret_access_key or r2_env("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
        config=_r2_client_config(),
    )


def build_weather_landing() -> KmaLanding:
    raw_store = R2RawObjectStore(
        _build_s3_client(),
        bucket=r2_env("R2_BUCKET_NAME"),
    )
    raw_spool = configured_raw_payload_spool()
    try:
        removed = raw_spool.prune_expired()
        if removed:
            print(f"Pruned {removed} expired Weather raw spool files")
    except OSError as exc:
        print(
            "Weather raw spool prune deferred: "
            f"error_type={type(exc).__name__}"
        )
    before_attempt = None
    if shared_guards_enabled():
        before_attempt = PhysicalAttemptBudgetHook(
            SqliteAttemptLedger.from_environment(),
            clock=lambda: datetime.now(timezone.utc),
        )
    return KmaLanding(
        source=KmaHttpAdapter(
            fetch_url,
            delay_seconds=kma_request_delay_seconds(),
            before_attempt=before_attempt,
        ),
        raw_store=raw_store,
        raw_spool=raw_spool,
        raw_prefix=raw_prefix(),
        checkpoint_prefix=checkpoint_prefix(),
        clock=lambda: datetime.now(timezone.utc),
        request_id=lambda: str(uuid.uuid4()),
    )


def _raw_payload_identity(raw_object: Mapping[str, object]) -> tuple[str, str]:
    raw_object_key = str(raw_object.get("raw_object_key") or "")
    expected_hash = str(
        raw_object.get("raw_hash") or raw_object.get("payload_hash") or ""
    )
    if not raw_object_key or not expected_hash:
        raise ValueError("raw payload identity requires raw_object_key and raw_hash")
    return raw_object_key, expected_hash


def read_weather_raw_payload(
    raw_object: Mapping[str, object],
    *,
    spool: RawPayloadSpool | None = None,
    download: Callable[[str, str], bytes] = download_raw_object,
) -> bytes:
    raw_object_key, expected_hash = _raw_payload_identity(raw_object)
    local_spool = spool or configured_raw_payload_spool()
    try:
        payload = local_spool.read_verified(raw_object_key, expected_hash)
    except OSError as exc:
        print(
            "Weather raw spool read unavailable; falling back to R2: "
            f"error_type={type(exc).__name__}"
        )
        payload = None
    if payload is not None:
        print(f"Read KMA raw payload from local spool: {raw_object_key}")
        return payload
    return download(raw_object_key, "KMA raw payload")


def discard_weather_raw_payload(
    raw_object: Mapping[str, object],
    *,
    spool: RawPayloadSpool | None = None,
) -> None:
    raw_object_key, expected_hash = _raw_payload_identity(raw_object)
    try:
        (spool or configured_raw_payload_spool()).discard(
            raw_object_key, expected_hash
        )
    except OSError as exc:
        print(
            "Weather raw spool cleanup deferred: "
            f"error_type={type(exc).__name__}"
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
