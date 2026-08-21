from __future__ import annotations

import hashlib
import base64
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "domains" / "weather"))

from common.raw_manifest import build_raw_manifest  # noqa: E402
from common.raw_write import (  # noqa: E402
    RawObjectWriteConflictError,
    write_immutable_raw_object,
)
from weather_ingest.bronze_batch import (  # noqa: E402
    BronzeLoadPorts,
    load_kma_bronze_batch,
)
from weather_ingest.landing import (  # noqa: E402
    KmaGrid,
    KmaLanding,
    KmaLandingRequest,
    RawObjectIntegrityError,
    RunIdentity,
)
from weather_ingest.raw_spool import RawPayloadSpool  # noqa: E402
import weather_ingest.runtime as weather_runtime  # noqa: E402
from weather_ingest.runtime import (  # noqa: E402
    R2RawObjectStore,
    discard_weather_raw_payload,
    read_weather_raw_payload,
)


class CountingS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict[str, object]] = {}
        self.get_calls = 0
        self.get_keys: list[str] = []
        self.head_calls = 0

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
    ) -> None:
        identity = (Bucket, Key)
        if IfNoneMatch == "*" and identity in self.objects:
            from botocore.exceptions import ClientError

            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )
        self.objects[identity] = {
            "payload": Body,
            "content_type": ContentType,
            "metadata": dict(Metadata or {}),
            "content_md5": ContentMD5,
        }

    def get_object(self, *, Bucket: str, Key: str):
        self.get_calls += 1
        self.get_keys.append(Key)
        payload = self.objects[(Bucket, Key)]["payload"]
        return {"Body": SimpleNamespace(read=lambda: payload)}

    def head_object(self, *, Bucket: str, Key: str):
        self.head_calls += 1
        if (Bucket, Key) not in self.objects:
            error = RuntimeError("missing")
            error.response = {"Error": {"Code": "404"}}
            raise error
        item = self.objects[(Bucket, Key)]
        return {"Metadata": dict(item["metadata"])}


def _payload() -> bytes:
    return json.dumps(
        {
            "response": {
                "header": {"resultCode": "00", "resultMsg": "OK"},
                "body": {
                    "pageNo": 1,
                    "numOfRows": 1000,
                    "totalCount": 1,
                    "items": {
                        "item": [
                            {
                                "baseDate": "20260820",
                                "baseTime": "0800",
                                "nx": 60,
                                "ny": 127,
                                "category": "TMP",
                                "fcstDate": "20260820",
                                "fcstTime": "0900",
                                "fcstValue": "25",
                            }
                        ]
                    },
                },
            }
        }
    ).encode("utf-8")


def _raw_result(payload: bytes) -> tuple[dict, bytes]:
    raw_key = "raw/weather/kma/page-1.json"
    run_id = "manual__raw-transfer-budget"
    raw_object = {
        "request_id": "request-1",
        "raw_object_key": raw_key,
        "raw_hash": hashlib.sha256(payload).hexdigest(),
        "http_status": 200,
        "collected_at": "2026-08-20T00:20:00+00:00",
        "place_id": "seoul-grid-60-127",
        "base_date": "20260820",
        "base_time": "0800",
        "nx": 60,
        "ny": 127,
        "page_no": 1,
        "num_of_rows": 1000,
    }
    manifest_key = "raw/weather/kma/_manifest.json"
    manifest = json.dumps(
        build_raw_manifest(
            run_id=run_id,
            dataset="kma_vilage_fcst",
            load_date="2026-08-20",
            object_keys=[raw_key],
            expected_count=1,
            actual_count=1,
            completed_at="2026-08-20T00:20:01+00:00",
        )
    ).encode("utf-8")
    return {
        "raw_objects": [raw_object],
        "manifest_key": manifest_key,
        "grid_count": 1,
    }, manifest


def test_new_immutable_r2_write_uses_head_checksum_without_payload_get():
    client = CountingS3Client()
    store = R2RawObjectStore(client, bucket="weather-raw")
    payload = b"weather payload"

    assert write_immutable_raw_object(
        store, "raw/weather/page.json", payload, "application/json"
    )

    item = client.objects[("weather-raw", "raw/weather/page.json")]
    assert item["metadata"] == {"sha256": hashlib.sha256(payload).hexdigest()}
    assert item["content_md5"] == base64.b64encode(
        hashlib.md5(payload, usedforsecurity=False).digest()
    ).decode("ascii")
    assert client.head_calls == 1
    assert client.get_calls == 0


def test_r2_client_serialization_sends_only_the_required_content_checksum():
    client = weather_runtime._build_s3_client(
        endpoint_url="https://example.r2.cloudflarestorage.com",
        access_key_id="test-access-key",
        secret_access_key="test-secret-key",
    )
    captured: dict[str, str] = {}

    class RequestCaptured(RuntimeError):
        pass

    def capture(request, **_kwargs):
        captured.update(
            {
                str(name).lower(): (
                    value.decode("ascii") if isinstance(value, bytes) else str(value)
                )
                for name, value in request.headers.items()
            }
        )
        raise RequestCaptured

    client.meta.events.register("before-send.s3.PutObject", capture)
    payload = b"serialized weather payload"

    with pytest.raises(RequestCaptured):
        client.put_object(
            Bucket="weather-raw",
            Key="raw/weather/page.json",
            Body=payload,
            ContentType="application/json",
            IfNoneMatch="*",
            Metadata={"sha256": hashlib.sha256(payload).hexdigest()},
            ContentMD5=base64.b64encode(
                hashlib.md5(payload, usedforsecurity=False).digest()
            ).decode("ascii"),
        )

    assert "content-md5" in captured
    assert "x-amz-meta-sha256" in captured
    assert "if-none-match" in captured
    assert "x-amz-sdk-checksum-algorithm" not in captured
    assert "x-amz-checksum-crc32" not in captured


def test_legacy_raw_without_checksum_metadata_falls_back_to_body_verification():
    client = CountingS3Client()
    key = ("weather-raw", "raw/weather/legacy.json")
    client.objects[key] = {
        "payload": b"legacy",
        "content_type": "application/json",
        "metadata": {},
        "content_md5": None,
    }
    store = R2RawObjectStore(client, bucket="weather-raw")

    assert not write_immutable_raw_object(store, key[1], b"legacy", "application/json")
    assert client.get_calls == 1

    with pytest.raises(RawObjectWriteConflictError, match="divergent payload"):
        write_immutable_raw_object(store, key[1], b"changed", "application/json")


def test_existing_raw_never_trusts_matching_custom_metadata_without_body_check():
    client = CountingS3Client()
    key = ("weather-raw", "raw/weather/existing.json")
    expected = b"expected"
    client.objects[key] = {
        "payload": b"divergent",
        "content_type": "application/json",
        "metadata": {"sha256": hashlib.sha256(expected).hexdigest()},
        "content_md5": None,
    }
    store = R2RawObjectStore(client, bucket="weather-raw")

    with pytest.raises(RawObjectWriteConflictError, match="divergent payload"):
        write_immutable_raw_object(store, key[1], expected, "application/json")

    assert client.get_calls == 1


def test_normal_landing_to_bronze_cycle_has_no_r2_payload_get(tmp_path):
    payload = _payload()
    client = CountingS3Client()
    store = R2RawObjectStore(client, bucket="weather-raw")
    spool = RawPayloadSpool(tmp_path)

    class Source:
        def fetch_page(self, **_kwargs):
            return 200, payload

    landing = KmaLanding(
        source=Source(),
        raw_store=store,
        raw_spool=spool,
        raw_prefix="raw",
        checkpoint_prefix="ops/checkpoints/weather",
        clock=lambda: datetime(2026, 8, 20, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: "request-1",
    )
    batch = landing.collect(
        RunIdentity("weather_vilage_fcst_bronze", "manual__normal-cycle"),
        KmaLandingRequest(
            base_date="20260820",
            base_time="0800",
            grids=(KmaGrid("seoul-grid-60-127", 60, 127),),
            num_of_rows=1000,
        ),
    )
    raw_result = batch.to_xcom()
    raw_key = raw_result["raw_objects"][0]["raw_object_key"]

    assert client.get_calls == 0

    result = load_kma_bronze_batch(
        raw_result=raw_result,
        dag_run_id="manual__normal-cycle",
        allow_partial_pages=False,
        expected_raw_object_count_key="expected_raw_object_count",
        ports=BronzeLoadPorts(
            open_trino=lambda: (object(), "iceberg", "weather_bronze"),
            ensure_table=lambda *_args: "iceberg.weather_bronze.kma",
            download=lambda key, _label: store.read_bytes(key),
            append_batches=lambda **_kwargs: 1,
            read_payload=lambda item: read_weather_raw_payload(
                item,
                spool=spool,
                download=lambda key, _label: store.read_bytes(key),
            ),
            discard_payload=lambda item: discard_weather_raw_payload(item, spool=spool),
        ),
    )

    assert result["inserted"] == 1
    assert client.get_keys == [raw_result["manifest_key"]]
    assert raw_key not in client.get_keys


def test_checkpoint_retry_recollects_when_canonical_r2_object_is_missing(tmp_path):
    payload = _payload()
    client = CountingS3Client()
    store = R2RawObjectStore(client, bucket="weather-raw")
    spool = RawPayloadSpool(tmp_path)

    class Source:
        calls = 0

        def fetch_page(self, **_kwargs):
            self.calls += 1
            return 200, payload

    source = Source()
    request_ids = iter(("request-1", "request-2"))
    landing = KmaLanding(
        source=source,
        raw_store=store,
        raw_spool=spool,
        raw_prefix="raw",
        checkpoint_prefix="ops/checkpoints/weather",
        clock=lambda: datetime(2026, 8, 20, 0, 20, tzinfo=timezone.utc),
        request_id=lambda: next(request_ids),
    )
    run = RunIdentity("weather_vilage_fcst_bronze", "manual__checkpoint-retry")
    request = KmaLandingRequest(
        base_date="20260820",
        base_time="0800",
        grids=(KmaGrid("seoul-grid-60-127", 60, 127),),
        num_of_rows=1000,
    )

    first = landing.collect(run, request)
    first_key = first.raw_objects[0].raw_object_key
    client.objects.pop(("weather-raw", first_key))

    second = landing.collect(run, request)

    assert source.calls == 2
    assert second.raw_objects[0].raw_object_key != first_key
    assert ("weather-raw", second.raw_objects[0].raw_object_key) in client.objects


def test_bronze_reads_verified_local_spool_and_discards_only_after_success(tmp_path):
    payload = _payload()
    raw_result, manifest = _raw_result(payload)
    raw_object = raw_result["raw_objects"][0]
    spool = RawPayloadSpool(tmp_path)
    spool.write_verified(raw_object["raw_object_key"], payload, raw_object["raw_hash"])
    remote_payload_calls: list[str] = []

    def remote_download(key: str, _label: str) -> bytes:
        if key == raw_result["manifest_key"]:
            return manifest
        remote_payload_calls.append(key)
        return payload

    result = load_kma_bronze_batch(
        raw_result=raw_result,
        dag_run_id="manual__raw-transfer-budget",
        allow_partial_pages=False,
        expected_raw_object_count_key="expected_raw_object_count",
        ports=BronzeLoadPorts(
            open_trino=lambda: (object(), "iceberg", "weather_bronze"),
            ensure_table=lambda *_args: "iceberg.weather_bronze.kma",
            download=remote_download,
            append_batches=lambda **_kwargs: 1,
            read_payload=lambda item: read_weather_raw_payload(
                item, spool=spool, download=remote_download
            ),
            discard_payload=lambda item: discard_weather_raw_payload(item, spool=spool),
        ),
    )

    assert result["inserted"] == 1
    assert remote_payload_calls == []
    assert (
        spool.read_verified(raw_object["raw_object_key"], raw_object["raw_hash"])
        is None
    )


def test_failed_bronze_append_keeps_spool_for_retry(tmp_path):
    payload = _payload()
    raw_result, manifest = _raw_result(payload)
    raw_object = raw_result["raw_objects"][0]
    spool = RawPayloadSpool(tmp_path)
    spool.write_verified(raw_object["raw_object_key"], payload, raw_object["raw_hash"])

    with pytest.raises(RuntimeError, match="append failed"):
        load_kma_bronze_batch(
            raw_result=raw_result,
            dag_run_id="manual__raw-transfer-budget",
            allow_partial_pages=False,
            expected_raw_object_count_key="expected_raw_object_count",
            ports=BronzeLoadPorts(
                open_trino=lambda: (object(), "iceberg", "weather_bronze"),
                ensure_table=lambda *_args: "iceberg.weather_bronze.kma",
                download=lambda key, _label: (
                    manifest
                    if key == raw_result["manifest_key"]
                    else pytest.fail("payload should come from spool")
                ),
                append_batches=lambda **_kwargs: (_ for _ in ()).throw(
                    RuntimeError("append failed")
                ),
                read_payload=lambda item: read_weather_raw_payload(
                    item, spool=spool, download=pytest.fail
                ),
                discard_payload=lambda item: discard_weather_raw_payload(
                    item, spool=spool
                ),
            ),
        )

    assert (
        spool.read_verified(raw_object["raw_object_key"], raw_object["raw_hash"])
        == payload
    )


def test_missing_or_corrupt_spool_falls_back_to_r2_and_keeps_hash_gate(tmp_path):
    payload = _payload()
    raw_result, manifest = _raw_result(payload)
    raw_object = raw_result["raw_objects"][0]
    spool = RawPayloadSpool(tmp_path)
    spool.write_verified(raw_object["raw_object_key"], payload, raw_object["raw_hash"])
    next(tmp_path.rglob("*.raw")).write_bytes(b"corrupt local payload")
    remote_calls: list[str] = []

    def mismatched_remote(key: str, _label: str) -> bytes:
        if key == raw_result["manifest_key"]:
            return manifest
        remote_calls.append(key)
        return b"corrupt remote payload"

    with pytest.raises(RawObjectIntegrityError, match="hash mismatch"):
        load_kma_bronze_batch(
            raw_result=raw_result,
            dag_run_id="manual__raw-transfer-budget",
            allow_partial_pages=False,
            expected_raw_object_count_key="expected_raw_object_count",
            ports=BronzeLoadPorts(
                open_trino=lambda: pytest.fail("must fail before Trino"),
                ensure_table=lambda *_args: pytest.fail("must fail before Trino"),
                download=mismatched_remote,
                append_batches=lambda **_kwargs: pytest.fail("must fail before append"),
                read_payload=lambda item: read_weather_raw_payload(
                    item, spool=spool, download=mismatched_remote
                ),
            ),
        )

    assert remote_calls == [raw_object["raw_object_key"]]


def test_spool_prunes_only_abandoned_payloads_past_retention(tmp_path):
    payload = b"retained payload"
    checksum = hashlib.sha256(payload).hexdigest()
    spool = RawPayloadSpool(tmp_path)
    spool.write_verified("raw/weather/expired.json", payload, checksum)
    spool_file = next(tmp_path.rglob("*.raw"))
    unrelated = tmp_path / "aa" / "unrelated.raw"
    unrelated.parent.mkdir(exist_ok=True)
    unrelated.write_bytes(b"not owned by the Weather spool")
    old_timestamp = 100.0
    spool_file.touch()
    os.utime(spool_file, (old_timestamp, old_timestamp))
    os.utime(unrelated, (old_timestamp, old_timestamp))

    assert spool.prune_expired(max_age_seconds=60, now=200.0) == 1
    assert spool.read_verified("raw/weather/expired.json", checksum) is None
    assert unrelated.read_bytes() == b"not owned by the Weather spool"


def test_spool_rejects_a_filesystem_root():
    with pytest.raises(ValueError, match="filesystem root"):
        RawPayloadSpool(Path(Path.cwd().anchor))
