import hashlib
import os
import re
import urllib.parse
import time
from datetime import datetime, timezone
from typing import Callable

from common.errors import types as error_types
from common.http import HttpCore, NoAuth, OK_2XX, QueryKey
from common.http.errors import HttpProblemError
from weather_ingest.errors import WeatherBronzeConfigurationError


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_TRINO_PORT = 8080
MIN_TCP_PORT = 1
MAX_TCP_PORT = 65535


def is_dev_target() -> bool:
    return (
        os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod"))
        == "dev"
    )


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise WeatherBronzeConfigurationError(
            f"Missing required environment variable: {name}"
        )
    return value


def r2_env_name(name: str) -> str:
    """canonical 키 이름 그대로 — 배포 환경은 키가 아니라 값이 정한다(ASAC-DAG#647)."""
    return name


def r2_env(name: str) -> str:
    return required_env(r2_env_name(name))


def raw_prefix() -> str:
    if is_dev_target():
        return os.environ.get("ASK_SEOUL_DEV_RAW_PREFIX", "raw")
    return os.environ.get("ASK_SEOUL_RAW_PREFIX", "raw")


def checkpoint_prefix() -> str:
    """landing checkpoint 루트 — 기본 ops 존, `WEATHER_CHECKPOINT_PREFIX` 는 롤백용(#60 약속②).

    checkpoint 는 다음 실행의 재개 지점을 바꾸는 가변 상태라, 불변 박제 구역인
    raw 밖(ops/control)이 목적지다. 기본값이 곧 목적지이므로 배포만 하면 맞고,
    env 는 구 위치(`raw/_checkpoints`)로 되돌릴 때만 쓴다 — common(#573)·
    culture(#579)·recovery(#585)와 같은 방식.
    """
    configured = os.environ.get("WEATHER_CHECKPOINT_PREFIX", "").strip()
    if configured:
        return configured.rstrip("/")
    return "ops/control/checkpoints/weather"


def trino_catalog() -> str:
    # canonical 키 하나 — 값이 배포 환경을 따라간다(미설정 시 기본만 타깃별).
    return os.environ.get("TRINO_ICEBERG_CATALOG") or (
        "iceberg_dev" if is_dev_target() else "iceberg")


def ask_seoul_schema() -> str:
    return os.environ.get("ASK_SEOUL_SCHEMA", "ask_seoul")


def trino_port() -> int:
    configured = os.environ.get("TRINO_PORT", str(DEFAULT_TRINO_PORT))
    try:
        port = int(configured)
    except (TypeError, ValueError) as exc:
        raise WeatherBronzeConfigurationError(
            f"TRINO_PORT must be an integer: {configured}"
        ) from exc
    if not MIN_TCP_PORT <= port <= MAX_TCP_PORT:
        raise WeatherBronzeConfigurationError(
            f"TRINO_PORT must be between {MIN_TCP_PORT} and {MAX_TCP_PORT}: {port}"
        )
    return port


def sql_identifier(value: str) -> str:
    if not IDENTIFIER_PATTERN.match(value):
        raise WeatherBronzeConfigurationError(f"Unsafe SQL identifier: {value}")
    return value


def sql_string(value: object) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def sql_int(value: object) -> str:
    if value is None or value == "":
        return "NULL"
    return str(int(value))


def sql_timestamp(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    return "TIMESTAMP " + sql_string(utc_value.strftime("%Y-%m-%d %H:%M:%S.%f"))


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


_HTTP = HttpCore(source="weather_kma", timeout=30.0, max_attempts=1, rate_limit=None)


def http_retry_delay(
    response_headers: dict[str, str] | None,
    attempt: int,
    base_delay_seconds: float,
    *,
    status_429_backoff_seconds: tuple[float, ...] | None = None,
) -> float:
    for key, value in (response_headers or {}).items():
        if key.lower() == "retry-after":
            try:
                return max(0.0, float(value))
            except ValueError:
                break
    if status_429_backoff_seconds:
        if 0 <= attempt - 1 < len(status_429_backoff_seconds):
            return max(0.0, float(status_429_backoff_seconds[attempt - 1]))
    return min(base_delay_seconds * (2 ** (attempt - 1)), 300.0)


def _redacted_request_url(url: str, params: dict[str, str] | None) -> str:
    if not params:
        return url
    return f"{url}?{urllib.parse.urlencode(params, safe='%')}"


def fetch_url(
    url: str,
    user_agent: str,
    *,
    max_attempts: int = 1,
    retry_statuses: tuple[int, ...] = (),
    retry_base_delay_seconds: float = 1.0,
    retry_429_backoff_seconds: tuple[float, ...] | None = None,
    before_attempt: Callable[[int], object] | None = None,
) -> tuple[int, bytes]:
    retry_codes = set(retry_statuses)
    auth = QueryKey("serviceKey", required_env("KMA_SERVICE_KEY"))
    for attempt in range(1, max_attempts + 1):
        prepared = auth.apply(
            url,
            None,
            {"User-Agent": user_agent},
        )
        if before_attempt is not None:
            before_attempt(attempt)
        try:
            response = _HTTP.get(
                prepared.url,
                params=prepared.params,
                headers=prepared.headers,
                auth=NoAuth(),
                expected_status=tuple(range(200, 600)),
            )
        except HttpProblemError:
            raise

        if response.status in OK_2XX:
            return response.status, response.content
        if response.status not in retry_codes:
            break
        if attempt >= max_attempts:
            raise HttpProblemError(
                error_types.HTTP_ERROR,
                method="GET",
                url=_redacted_request_url(prepared.url, prepared.params),
                status=response.status,
                detail=f"status={response.status}",
                attempts=attempt,
                source_system="weather_kma",
            )

        delay = http_retry_delay(
            response.headers,
            attempt,
            retry_base_delay_seconds,
            status_429_backoff_seconds=(
                retry_429_backoff_seconds if response.status == 429 else None
            ),
        )
        print(
            f"Source API HTTP {response.status}; retrying in {delay:.1f}s "
            f"(attempt {attempt + 1}/{max_attempts})"
        )
        time.sleep(delay)
    raise HttpProblemError(
        error_types.HTTP_ERROR,
        method="GET",
        url=_redacted_request_url(prepared.url, prepared.params),
        status=response.status,
        detail=f"status={response.status}",
        attempts=max_attempts,
        source_system="weather_kma",
    )


def upload_raw_object(
    raw_bytes: bytes,
    object_key: str,
    content_type: str,
    log_label: str,
) -> str:
    import boto3

    bucket_name = r2_env("R2_BUCKET_NAME")
    boto3.client(
        "s3",
        endpoint_url=r2_env("R2_ENDPOINT"),
        aws_access_key_id=r2_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=r2_env("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    ).put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=raw_bytes,
        ContentType=content_type,
    )
    print(f"Uploaded {log_label} to R2: {object_key}")
    return object_key


def download_raw_object(object_key: str, log_label: str) -> bytes:
    import boto3

    bucket_name = r2_env("R2_BUCKET_NAME")
    response = boto3.client(
        "s3",
        endpoint_url=r2_env("R2_ENDPOINT"),
        aws_access_key_id=r2_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=r2_env("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    ).get_object(Bucket=bucket_name, Key=object_key)
    raw_bytes = response["Body"].read()
    print(f"Downloaded {log_label} from R2: {object_key}")
    return raw_bytes


def trino_cursor():
    port = trino_port()
    import trino.dbapi

    catalog = sql_identifier(trino_catalog())
    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=port,
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=catalog,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    return connection.cursor(), catalog, sql_identifier(ask_seoul_schema())


def create_schema_if_needed(cursor, qualified_schema: str) -> None:
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")
    except Exception as exc:
        if "Namespace already exists" not in str(exc):
            raise
