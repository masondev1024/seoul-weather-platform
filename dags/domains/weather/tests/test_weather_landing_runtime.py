from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.raw_write import write_immutable_raw_object  # noqa: E402
import weather_ingest.runtime as runtime  # noqa: E402
from weather_ingest.runtime import (  # noqa: E402
    DEFAULT_KMA_REQUEST_DELAY_SECONDS,
    KMA_429_BACKOFF_SECONDS,
    KMA_REQUEST_DELAY_SECONDS_ENV,
    KmaHttpAdapter,
    R2RawObjectStore,
    kma_request_delay_seconds,
)


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, str]] = {}
        self.put_conditions: list[str | None] = []

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
        IfNoneMatch: str | None = None,
    ) -> None:
        self.put_conditions.append(IfNoneMatch)
        if IfNoneMatch == "*" and (Bucket, Key) in self.objects:
            from botocore.exceptions import ClientError

            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )
        self.objects[(Bucket, Key)] = (Body, ContentType)

    def get_object(self, *, Bucket: str, Key: str):
        payload, _ = self.objects[(Bucket, Key)]
        return {"Body": SimpleNamespace(read=lambda: payload)}

    def head_object(self, *, Bucket: str, Key: str) -> None:
        if (Bucket, Key) not in self.objects:
            error = RuntimeError("missing")
            error.response = {"Error": {"Code": "404"}}
            raise error


def test_kma_adapter_delegates_auth_and_retry_policy_to_domain_http_runtime():
    calls: list[tuple[str, str, dict]] = []

    def fetch(url: str, user_agent: str, **options):
        calls.append((url, user_agent, options))
        return 200, b'{"response": {}}'

    status, payload = KmaHttpAdapter(fetch).fetch_page(
        base_date="20260714",
        base_time="0800",
        nx=60,
        ny=127,
        page_no=2,
        num_of_rows=500,
    )

    assert (status, payload) == (200, b'{"response": {}}')
    url, user_agent, options = calls[0]
    assert user_agent == "ask-seoul-kma-bronze/1.0"
    assert "serviceKey" not in url
    assert "base_date=20260714" in url
    assert "base_time=0800" in url
    assert "pageNo=2" in url
    assert "numOfRows=500" in url
    assert options == {
        "max_attempts": 4,
        "retry_statuses": (429, 500, 502, 503, 504),
        "retry_base_delay_seconds": 30,
        "retry_429_backoff_seconds": (60.0, 180.0, 300.0),
    }


def test_kma_429_backoff_fits_inside_the_bronze_dagrun_timeout():
    """backoff 합이 dagrun_timeout(60분)을 넘으면 재시도가 실행될 수 없다."""
    assert sum(KMA_429_BACKOFF_SECONDS) < 60 * 60


def test_kma_request_delay_defaults_without_the_env_override(monkeypatch):
    monkeypatch.delenv(KMA_REQUEST_DELAY_SECONDS_ENV, raising=False)

    assert kma_request_delay_seconds() == DEFAULT_KMA_REQUEST_DELAY_SECONDS


def test_kma_request_delay_reads_the_env_override(monkeypatch):
    monkeypatch.setenv(KMA_REQUEST_DELAY_SECONDS_ENV, "0.25")

    assert kma_request_delay_seconds() == 0.25


def test_kma_request_delay_treats_blank_override_as_absent(monkeypatch):
    monkeypatch.setenv(KMA_REQUEST_DELAY_SECONDS_ENV, "   ")

    assert kma_request_delay_seconds() == DEFAULT_KMA_REQUEST_DELAY_SECONDS


@pytest.mark.parametrize("value", ["fast", "-1"])
def test_kma_request_delay_rejects_invalid_override(monkeypatch, value):
    """오타 하나가 조용히 기본값으로 되돌아가면 429 를 맞고 나서야 알게 된다."""
    monkeypatch.setenv(KMA_REQUEST_DELAY_SECONDS_ENV, value)

    with pytest.raises(ValueError, match=KMA_REQUEST_DELAY_SECONDS_ENV):
        kma_request_delay_seconds()


def test_kma_adapter_preserves_delay_between_successful_api_requests():
    sleeps: list[float] = []

    adapter = KmaHttpAdapter(
        lambda *_args, **_kwargs: (200, b"{}"),
        delay_seconds=4.0,
        sleep=sleeps.append,
    )
    request = {
        "base_date": "20260714",
        "base_time": "0800",
        "nx": 60,
        "ny": 127,
        "num_of_rows": 1000,
    }

    adapter.fetch_page(page_no=1, **request)
    adapter.fetch_page(page_no=2, **request)

    assert sleeps == [4.0]


def test_r2_store_preserves_content_type_and_missing_object_semantics():
    client = FakeS3Client()
    store = R2RawObjectStore(client, bucket="seoul-dev")

    assert store.exists("raw/missing.json") is False

    store.write_bytes("raw/page.json", b"payload", "application/json; charset=utf-8")

    assert store.exists("raw/page.json") is True
    assert store.read_bytes("raw/page.json") == b"payload"
    assert (
        client.objects[("seoul-dev", "raw/page.json")][1]
        == "application/json; charset=utf-8"
    )


def test_r2_store_create_only_write_rejects_an_existing_raw_key():
    client = FakeS3Client()
    store = R2RawObjectStore(client, bucket="seoul-dev")

    assert store.write_bytes_if_absent("raw/page.json", b"first", "application/json")
    assert not store.write_bytes_if_absent(
        "raw/page.json", b"second", "application/json"
    )

    assert store.read_bytes("raw/page.json") == b"first"
    assert client.put_conditions == ["*", "*"]


def test_r2_store_retries_conditional_conflict_until_raw_creation_succeeds():
    from botocore.exceptions import ClientError

    class ConflictThenSuccessClient(FakeS3Client):
        def __init__(self) -> None:
            super().__init__()
            self._conflict = True

        def put_object(self, **kwargs) -> None:
            if self._conflict:
                self._conflict = False
                self.put_conditions.append(kwargs.get("IfNoneMatch"))
                raise ClientError(
                    {
                        "Error": {"Code": "ConditionalRequestConflict"},
                        "ResponseMetadata": {"HTTPStatusCode": 409},
                    },
                    "PutObject",
                )
            super().put_object(**kwargs)

    client = ConflictThenSuccessClient()
    store = R2RawObjectStore(client, bucket="seoul-dev")

    assert store.write_bytes_if_absent("raw/page.json", b"payload", "application/json")
    assert store.read_bytes("raw/page.json") == b"payload"
    assert client.put_conditions == ["*", "*"]


def test_r2_store_retries_conditional_conflict_then_reuses_matching_raw_object():
    from botocore.exceptions import ClientError

    class ConflictThenOtherWriterClient(FakeS3Client):
        def __init__(self) -> None:
            super().__init__()
            self._conflict = True

        def put_object(self, **kwargs) -> None:
            if self._conflict:
                self._conflict = False
                self.put_conditions.append(kwargs.get("IfNoneMatch"))
                self.objects[(kwargs["Bucket"], kwargs["Key"])] = (
                    kwargs["Body"],
                    kwargs["ContentType"],
                )
                raise ClientError(
                    {
                        "Error": {"Code": "ConditionalRequestConflict"},
                        "ResponseMetadata": {"HTTPStatusCode": 409},
                    },
                    "PutObject",
                )
            super().put_object(**kwargs)

    client = ConflictThenOtherWriterClient()
    store = R2RawObjectStore(client, bucket="seoul-dev")

    assert not write_immutable_raw_object(
        store,
        "raw/page.json",
        b"payload",
        "application/json",
    )
    assert store.read_bytes("raw/page.json") == b"payload"
    assert client.put_conditions == ["*", "*"]


def test_r2_store_does_not_hide_permission_or_transport_failures():
    class DeniedClient(FakeS3Client):
        def head_object(self, *, Bucket: str, Key: str) -> None:
            error = RuntimeError("denied")
            error.response = {"Error": {"Code": "AccessDenied"}}
            raise error

    with pytest.raises(RuntimeError, match="denied"):
        R2RawObjectStore(DeniedClient(), bucket="seoul-dev").exists("raw/page.json")


def test_runtime_factory_lazily_composes_domain_landing(monkeypatch):
    sentinel_fetch = object()
    sentinel_s3 = object()
    captured: dict[str, object] = {}

    # 운영이 재배포 없이 딜레이를 조정할 수 있어야 하므로, 빌더가 환경변수를
    # 실제로 읽어 adapter 에 넣는지까지 확인한다.
    monkeypatch.setenv(KMA_REQUEST_DELAY_SECONDS_ENV, "0.5")
    monkeypatch.setattr(runtime, "fetch_url", sentinel_fetch)
    monkeypatch.setattr(runtime, "_build_s3_client", lambda: sentinel_s3)
    monkeypatch.setattr(
        runtime, "r2_env", lambda name: {"R2_BUCKET_NAME": "seoul-dev"}[name]
    )
    monkeypatch.setattr(runtime, "raw_prefix", lambda: "dev/raw")
    monkeypatch.setattr(
        runtime,
        "KmaLanding",
        lambda **dependencies: captured.setdefault("landing", dependencies),
    )

    landing = runtime.build_weather_landing()

    assert landing is captured["landing"]
    source = captured["landing"]["source"]
    assert isinstance(source, KmaHttpAdapter)
    assert source._fetch is sentinel_fetch
    assert source._delay_seconds == 0.5
    raw_store = captured["landing"]["raw_store"]
    assert isinstance(raw_store, R2RawObjectStore)
    assert raw_store._client is sentinel_s3
    assert raw_store._bucket == "seoul-dev"
    assert captured["landing"]["raw_prefix"] == "dev/raw"
    # 프로덕션 경로는 항상 빌더가 주입한다 — KmaLanding 의 생성자 폴백
    # (`{raw_prefix}/_checkpoints`)은 직접 생성하는 테스트 편의일 뿐이다.
    # 새 caller 가 주입을 빠뜨리면 구 위치로 새므로 여기서 고정한다(#60 약속②).
    assert (
        captured["landing"]["checkpoint_prefix"] == "ops/control/checkpoints/weather"
    )


def test_manifest_factory_keeps_trino_wiring_out_of_the_dag(monkeypatch):
    cursor_factory = object()
    sentinel_manifest = object()
    monkeypatch.setattr(runtime, "trino_cursor", cursor_factory)
    monkeypatch.setattr(
        runtime,
        "WeatherRunManifest",
        lambda factory: sentinel_manifest if factory is cursor_factory else None,
    )

    assert runtime.build_weather_manifest() is sentinel_manifest
