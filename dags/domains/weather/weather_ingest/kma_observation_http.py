"""Deadline-, quota-, and circuit-aware HTTP policy for KMA observations."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from typing import Callable, Mapping, Protocol, Sequence

from weather_ingest.errors import WeatherBronzeConfigurationError
from weather_ingest.kma_coordination import (
    CycleDeadline,
    PhysicalAttempt,
)
from weather_ingest.kma_observation import (
    SOURCE_ID,
    KmaObservationThrottleError,
    build_kma_observation_url,
    kma_observation_result_code,
    raise_for_kma_observation_result_code,
)


RETRYABLE_HTTP_STATUSES = frozenset({500, 502, 503, 504})


class KmaObservationHttpError(RuntimeError):
    """Sanitized non-success HTTP response."""

    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(f"KMA observation HTTP status={status}")


class KmaObservationTransportError(RuntimeError):
    """Sanitized exhausted timeout/connection failure."""


class KmaThrottleCircuitOpen(RuntimeError):
    """Raised when the shared cycle throttle threshold has been reached."""


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int
    content: bytes
    headers: Mapping[str, str]


class HttpTransport(Protocol):
    def __call__(self, url: str, timeout_seconds: float) -> HttpResponse: ...


class DeadlinePolicy(Protocol):
    def require_request(
        self,
        *,
        request_timeout_seconds: float,
        headroom_seconds: float,
    ) -> None: ...

    def require_retry_sleep(
        self,
        *,
        sleep_seconds: float,
        request_headroom_seconds: float,
    ) -> None: ...


class KmaThrottleCircuit:
    """One throttle counter shared by every grid in a collection cycle."""

    def __init__(self, *, threshold: int) -> None:
        if isinstance(threshold, bool) or not isinstance(threshold, int) or threshold < 1:
            raise WeatherBronzeConfigurationError(
                "KMA throttle circuit threshold must be a positive integer"
            )
        self._threshold = threshold
        self._throttle_count = 0

    @property
    def throttle_count(self) -> int:
        return self._throttle_count

    @property
    def is_open(self) -> bool:
        return self._throttle_count >= self._threshold

    def require_closed(self) -> None:
        if self.is_open:
            raise KmaThrottleCircuitOpen(
                "KMA observation cycle throttle circuit is open"
            )

    def record_throttle(self) -> None:
        self._throttle_count += 1
        self.require_closed()


def _positive_finite(value: float, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise WeatherBronzeConfigurationError(f"{field} must be numeric") from exc
    if not math.isfinite(number) or number <= 0:
        raise WeatherBronzeConfigurationError(
            f"{field} must be finite and greater than zero"
        )
    return number


def _nonnegative_finite(value: float, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise WeatherBronzeConfigurationError(f"{field} must be numeric") from exc
    if not math.isfinite(number) or number < 0:
        raise WeatherBronzeConfigurationError(
            f"{field} must be finite and non-negative"
        )
    return number


class KmaObservationHttpClient:
    """Issue bounded physical attempts without leaking provider response bodies."""

    def __init__(
        self,
        *,
        transport: HttpTransport,
        deadline: DeadlinePolicy | CycleDeadline,
        circuit: KmaThrottleCircuit,
        before_attempt: Callable[[PhysicalAttempt], object],
        sleep: Callable[[float], None],
        max_attempts: int = 4,
        request_timeout_seconds: float = 10,
        request_headroom_seconds: float = 2,
        retry_backoff_seconds: Sequence[float] = (5.0, 10.0, 20.0),
        max_retry_delay_seconds: float = 60,
        jitter: Callable[[float], float] = lambda delay: delay,
    ) -> None:
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts < 1:
            raise WeatherBronzeConfigurationError(
                "KMA HTTP max_attempts must be a positive integer"
            )
        if not retry_backoff_seconds:
            raise WeatherBronzeConfigurationError(
                "KMA HTTP retry backoff must not be empty"
            )
        self._transport = transport
        self._deadline = deadline
        self._circuit = circuit
        self._before_attempt = before_attempt
        self._sleep = sleep
        self._max_attempts = max_attempts
        self._request_timeout = _positive_finite(
            request_timeout_seconds,
            field="KMA HTTP request timeout",
        )
        self._request_headroom = _nonnegative_finite(
            request_headroom_seconds,
            field="KMA HTTP request headroom",
        )
        self._backoff = tuple(
            _nonnegative_finite(value, field="KMA HTTP retry backoff")
            for value in retry_backoff_seconds
        )
        self._max_retry_delay = _positive_finite(
            max_retry_delay_seconds,
            field="KMA HTTP maximum retry delay",
        )
        self._jitter = jitter

    def fetch(
        self,
        *,
        base_date: str,
        base_time: str,
        nx: int,
        ny: int,
        dag_run_id: str,
        observed_slot: str,
        request_id: str,
    ) -> tuple[int, bytes]:
        url = build_kma_observation_url(base_date, base_time, nx, ny)
        for attempt_ordinal in range(1, self._max_attempts + 1):
            self._circuit.require_closed()
            self._deadline.require_request(
                request_timeout_seconds=self._request_timeout,
                headroom_seconds=self._request_headroom,
            )
            self._before_attempt(
                PhysicalAttempt(
                    reservation_id=self._reservation_id(
                        dag_run_id=dag_run_id,
                        observed_slot=observed_slot,
                        nx=nx,
                        ny=ny,
                        request_id=request_id,
                        attempt_ordinal=attempt_ordinal,
                    ),
                    source_id=SOURCE_ID,
                    dag_run_id=dag_run_id,
                    observed_slot=observed_slot,
                    nx=nx,
                    ny=ny,
                    attempt_ordinal=attempt_ordinal,
                )
            )
            try:
                response = self._transport(url, self._request_timeout)
            except OSError as exc:
                if attempt_ordinal >= self._max_attempts:
                    raise KmaObservationTransportError(
                        "KMA observation transport attempts exhausted"
                    ) from exc
                self._wait_before_retry(attempt_ordinal, response_headers=None)
                continue

            if 200 <= response.status < 300:
                result_code = kma_observation_result_code(response.content)
                try:
                    raise_for_kma_observation_result_code(result_code)
                except KmaObservationThrottleError:
                    self._circuit.record_throttle()
                    if attempt_ordinal >= self._max_attempts:
                        raise
                    self._wait_before_retry(attempt_ordinal, response_headers=None)
                    continue
                return response.status, response.content

            if response.status == 429:
                self._circuit.record_throttle()
                if attempt_ordinal >= self._max_attempts:
                    raise KmaObservationHttpError(response.status)
                self._wait_before_retry(
                    attempt_ordinal,
                    response_headers=response.headers,
                )
                continue

            if response.status in RETRYABLE_HTTP_STATUSES:
                if attempt_ordinal >= self._max_attempts:
                    raise KmaObservationHttpError(response.status)
                self._wait_before_retry(attempt_ordinal, response_headers=None)
                continue
            raise KmaObservationHttpError(response.status)

        raise AssertionError("unreachable KMA HTTP attempt state")

    def _wait_before_retry(
        self,
        attempt_ordinal: int,
        *,
        response_headers: Mapping[str, str] | None,
    ) -> None:
        delay = self._retry_after(response_headers)
        if delay is None:
            configured = self._backoff[min(attempt_ordinal - 1, len(self._backoff) - 1)]
            jittered = _nonnegative_finite(
                self._jitter(configured),
                field="KMA HTTP jittered retry delay",
            )
            delay = min(jittered, self._max_retry_delay)
        self._deadline.require_retry_sleep(
            sleep_seconds=delay,
            request_headroom_seconds=(
                self._request_timeout + self._request_headroom
            ),
        )
        self._sleep(delay)

    def _retry_after(self, headers: Mapping[str, str] | None) -> float | None:
        for key, raw_value in (headers or {}).items():
            if key.lower() != "retry-after":
                continue
            try:
                delay = float(raw_value)
            except (TypeError, ValueError):
                return None
            if not math.isfinite(delay) or delay < 0:
                return None
            return min(delay, self._max_retry_delay)
        return None

    @staticmethod
    def _reservation_id(
        *,
        dag_run_id: str,
        observed_slot: str,
        nx: int,
        ny: int,
        request_id: str,
        attempt_ordinal: int,
    ) -> str:
        material = "\x1f".join(
            (
                SOURCE_ID,
                dag_run_id,
                observed_slot,
                str(nx),
                str(ny),
                request_id,
                str(attempt_ordinal),
            )
        )
        return hashlib.sha256(material.encode("utf-8")).hexdigest()


__all__ = [
    "HttpResponse",
    "KmaObservationHttpClient",
    "KmaObservationHttpError",
    "KmaObservationTransportError",
    "KmaThrottleCircuit",
    "KmaThrottleCircuitOpen",
    "RETRYABLE_HTTP_STATUSES",
]
