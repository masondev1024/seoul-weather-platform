"""Lazy live adapters for the disabled-by-default observation DAG."""

from __future__ import annotations

import os
import time
import uuid
from datetime import datetime, timezone

from common.http import HttpCore, HttpProblemError, QueryKey
from weather_ingest.common.runtime import (
    checkpoint_prefix,
    raw_prefix,
    r2_env,
    required_env,
)
from weather_ingest.kma_coordination import (
    PhysicalAttemptBudgetHook,
    SqliteAttemptLedger,
)
from weather_ingest.kma_observation_http import (
    HttpResponse,
    KmaObservationHttpClient,
    KmaThrottleCircuit,
)
from weather_ingest.kma_observation_landing import KmaObservationLanding
from weather_ingest.runtime import R2RawObjectStore, _build_s3_client


THROTTLE_THRESHOLD_ENV = "ASK_SEOUL_KMA_OBSERVATION_THROTTLE_THRESHOLD"
REQUESTS_PER_SECOND_ENV = "ASK_SEOUL_KMA_OBSERVATION_REQUESTS_PER_SECOND"


def _positive_integer(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _positive_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return value


class KmaSingleAttemptTransport:
    """Inject the service key and perform exactly one physical HTTP attempt."""

    def __init__(self, *, requests_per_second: float) -> None:
        self._http = HttpCore(
            source="weather_kma_observation",
            max_attempts=1,
            rate_limit=requests_per_second,
            user_agent="ask-seoul-kma-observation/1.0",
        )
        self._auth = QueryKey("serviceKey", required_env("KMA_SERVICE_KEY"))

    def __call__(self, url: str, timeout_seconds: float) -> HttpResponse:
        try:
            response = self._http.get(
                url,
                auth=self._auth,
                timeout=timeout_seconds,
                expected_status=tuple(range(100, 600)),
            )
        except HttpProblemError as exc:
            raise OSError("KMA observation transport failed") from exc
        return HttpResponse(
            status=response.status,
            content=response.content,
            headers=response.headers,
        )


def build_observation_raw_store() -> R2RawObjectStore:
    return R2RawObjectStore(
        _build_s3_client(),
        bucket=r2_env("R2_BUCKET_NAME"),
    )


def build_observation_landing() -> KmaObservationLanding:
    """Build one cycle with a shared quota hook and one cycle-wide circuit."""
    raw_store = build_observation_raw_store()
    ledger = SqliteAttemptLedger.from_environment()
    attempt_hook = PhysicalAttemptBudgetHook(
        ledger,
        clock=lambda: datetime.now(timezone.utc),
    )
    transport = KmaSingleAttemptTransport(
        requests_per_second=_positive_float(REQUESTS_PER_SECOND_ENV, 1.0)
    )
    circuit = KmaThrottleCircuit(
        threshold=_positive_integer(THROTTLE_THRESHOLD_ENV, 3)
    )

    def source_factory(deadline):
        return KmaObservationHttpClient(
            transport=transport,
            deadline=deadline,
            circuit=circuit,
            before_attempt=attempt_hook,
            sleep=time.sleep,
            max_attempts=4,
            request_timeout_seconds=10,
            request_headroom_seconds=2,
            retry_backoff_seconds=(5.0, 10.0, 20.0),
            max_retry_delay_seconds=60,
        )

    checkpoint_root = checkpoint_prefix().rstrip("/")
    return KmaObservationLanding(
        source_factory=source_factory,
        raw_store=raw_store,
        clock=lambda: datetime.now(timezone.utc),
        monotonic_clock=time.monotonic,
        request_id=lambda: str(uuid.uuid4()),
        expected_grid_count=80,
        raw_prefix=f"{raw_prefix().rstrip('/')}/weather_observation/kma_ultra_srt_ncst",
        checkpoint_prefix=f"{checkpoint_root}/kma_ultra_srt_ncst/grids",
        manifest_prefix=f"{checkpoint_root}/kma_ultra_srt_ncst/manifests",
    )


__all__ = [
    "KmaSingleAttemptTransport",
    "build_observation_landing",
    "build_observation_raw_store",
]
