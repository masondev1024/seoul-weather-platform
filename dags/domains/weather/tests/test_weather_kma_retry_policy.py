from __future__ import annotations

import sys
from pathlib import Path

import pytest


DOMAIN_ROOT = Path(__file__).resolve().parents[1]
DAGS_ROOT = DOMAIN_ROOT.parents[1]
sys.path.insert(0, str(DOMAIN_ROOT))
sys.path.insert(0, str(DAGS_ROOT))

from weather_ingest.errors import WeatherSourceSchemaError  # noqa: E402
from weather_ingest.kma_observation import (  # noqa: E402
    KmaObservationAuthenticationError,
    KmaObservationDailyQuotaExhausted,
    KmaObservationThrottleError,
    KmaObservationUnknownResultError,
    parse_kma_observation_response,
)
from weather_ingest.kma_observation_http import (  # noqa: E402
    HttpResponse,
    KmaObservationHttpClient,
    KmaObservationHttpError,
    KmaThrottleCircuit,
    KmaThrottleCircuitOpen,
)


def _body(result_code: str, result_message: str = "provider-secret-message") -> bytes:
    if result_code != "00":
        return (
            '{"response":{"header":{"resultCode":"'
            + result_code
            + '","resultMsg":"'
            + result_message
            + '"}}}'
        ).encode()
    return b'{"response":{"header":{"resultCode":"00"}}}'


class DeadlineStub:
    def __init__(self, *, reject_request=False, reject_sleep=False):
        self.reject_request = reject_request
        self.reject_sleep = reject_sleep
        self.requests: list[tuple[float, float]] = []
        self.sleeps: list[tuple[float, float]] = []

    def require_request(self, *, request_timeout_seconds, headroom_seconds):
        self.requests.append((request_timeout_seconds, headroom_seconds))
        if self.reject_request:
            raise RuntimeError("deadline request rejected")

    def require_retry_sleep(self, *, sleep_seconds, request_headroom_seconds):
        self.sleeps.append((sleep_seconds, request_headroom_seconds))
        if self.reject_sleep:
            raise RuntimeError("deadline sleep rejected")


class SequenceTransport:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, float]] = []

    def __call__(self, url: str, timeout_seconds: float) -> HttpResponse:
        self.calls.append((url, timeout_seconds))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _client(
    outcomes,
    *,
    deadline=None,
    circuit=None,
    before_attempt=None,
    sleep=None,
    max_attempts=4,
    backoff=(5.0, 15.0, 30.0),
    jitter=lambda delay: delay,
):
    transport = SequenceTransport(outcomes)
    attempts = []
    client = KmaObservationHttpClient(
        transport=transport,
        deadline=deadline or DeadlineStub(),
        circuit=circuit or KmaThrottleCircuit(threshold=3),
        before_attempt=before_attempt or attempts.append,
        sleep=sleep or (lambda _seconds: None),
        max_attempts=max_attempts,
        request_timeout_seconds=10,
        request_headroom_seconds=2,
        retry_backoff_seconds=backoff,
        jitter=jitter,
    )
    return client, transport, attempts


def _fetch(client, *, nx=60, request_id="request-1"):
    return client.fetch(
        base_date="20260822",
        base_time="0900",
        nx=nx,
        ny=127,
        dag_run_id="scheduled__one",
        observed_slot="2026-08-22T09:00:00+09:00",
        request_id=request_id,
    )


def test_http_429_honors_numeric_retry_after_and_reserves_each_attempt():
    sleeps = []
    client, transport, attempts = _client(
        [
            HttpResponse(429, b"do-not-log", {"Retry-After": "7"}),
            HttpResponse(200, _body("00"), {}),
        ],
        sleep=sleeps.append,
    )

    status, body = _fetch(client)

    assert (status, body) == (200, _body("00"))
    assert sleeps == [7.0]
    assert [attempt.attempt_ordinal for attempt in attempts] == [1, 2]
    assert len({attempt.reservation_id for attempt in attempts}) == 2
    assert len(transport.calls) == 2


@pytest.mark.parametrize("retry_after", ["not-a-number", "-1", "nan", "inf"])
def test_invalid_retry_after_uses_bounded_jittered_backoff(retry_after):
    sleeps = []
    client, _, _ = _client(
        [
            HttpResponse(429, b"", {"retry-after": retry_after}),
            HttpResponse(200, _body("00"), {}),
        ],
        sleep=sleeps.append,
        backoff=(5.0,),
        jitter=lambda delay: delay + 1,
    )

    _fetch(client)

    assert sleeps == [6.0]


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_retryable_http_statuses_retry_with_exponential_policy(status):
    sleeps = []
    client, transport, attempts = _client(
        [HttpResponse(status, b"", {}), HttpResponse(200, _body("00"), {})],
        sleep=sleeps.append,
    )

    _fetch(client)

    assert sleeps == [5.0]
    assert len(transport.calls) == 2
    assert len(attempts) == 2


@pytest.mark.parametrize(
    "error",
    [TimeoutError("timeout"), ConnectionError("reset"), OSError("network down")],
)
def test_transport_errors_retry_without_leaking_error_text(error):
    sleeps = []
    client, transport, _ = _client(
        [error, HttpResponse(200, _body("00"), {})],
        sleep=sleeps.append,
    )

    _fetch(client)

    assert sleeps == [5.0]
    assert len(transport.calls) == 2


def test_deadline_rejects_request_before_hook_and_http():
    deadline = DeadlineStub(reject_request=True)
    client, transport, attempts = _client(
        [HttpResponse(200, _body("00"), {})],
        deadline=deadline,
    )

    with pytest.raises(RuntimeError, match="deadline request"):
        _fetch(client)

    assert attempts == []
    assert transport.calls == []


def test_hook_rejection_performs_no_http_request():
    def reject(_attempt):
        raise RuntimeError("budget rejected")

    client, transport, _ = _client(
        [HttpResponse(200, _body("00"), {})],
        before_attempt=reject,
    )

    with pytest.raises(RuntimeError, match="budget rejected"):
        _fetch(client)

    assert transport.calls == []


def test_retry_sleep_rejection_prevents_the_next_physical_attempt():
    deadline = DeadlineStub(reject_sleep=True)
    client, transport, attempts = _client(
        [HttpResponse(503, b"", {}), HttpResponse(200, _body("00"), {})],
        deadline=deadline,
    )

    with pytest.raises(RuntimeError, match="deadline sleep"):
        _fetch(client)

    assert len(attempts) == 1
    assert len(transport.calls) == 1
    assert deadline.sleeps == [(5.0, 12.0)]


def test_result_23_opens_one_cycle_wide_circuit_across_grids():
    circuit = KmaThrottleCircuit(threshold=2)
    client, transport, _ = _client(
        [
            HttpResponse(200, _body("23"), {}),
            HttpResponse(200, _body("23"), {}),
            HttpResponse(200, _body("00"), {}),
        ],
        circuit=circuit,
        max_attempts=1,
    )

    with pytest.raises(KmaObservationThrottleError):
        _fetch(client, nx=60, request_id="one")
    with pytest.raises(KmaThrottleCircuitOpen):
        _fetch(client, nx=61, request_id="two")
    with pytest.raises(KmaThrottleCircuitOpen):
        _fetch(client, nx=62, request_id="three")

    assert circuit.throttle_count == 2
    assert circuit.is_open is True
    assert len(transport.calls) == 2


def test_success_does_not_reset_prior_cycle_throttle_evidence():
    circuit = KmaThrottleCircuit(threshold=2)
    client, _, _ = _client(
        [
            HttpResponse(200, _body("23"), {}),
            HttpResponse(200, _body("00"), {}),
            HttpResponse(429, b"", {}),
        ],
        circuit=circuit,
        max_attempts=1,
    )

    with pytest.raises(KmaObservationThrottleError):
        _fetch(client, nx=60, request_id="one")
    assert _fetch(client, nx=61, request_id="two")[0] == 200
    with pytest.raises(KmaThrottleCircuitOpen):
        _fetch(client, nx=62, request_id="three")

    assert circuit.throttle_count == 2


def test_result_22_is_non_retryable_daily_exhaustion():
    client, transport, attempts = _client(
        [HttpResponse(200, _body("22", "quota details"), {})]
    )

    with pytest.raises(KmaObservationDailyQuotaExhausted) as captured:
        _fetch(client)

    assert "quota details" not in str(captured.value)
    assert len(transport.calls) == 1
    assert len(attempts) == 1


@pytest.mark.parametrize("result_code", ["20", "30", "31", "32"])
def test_auth_and_permission_codes_fail_fast_with_sanitized_errors(result_code):
    client, transport, _ = _client(
        [HttpResponse(200, _body(result_code, "credential detail"), {})]
    )

    with pytest.raises(KmaObservationAuthenticationError) as captured:
        _fetch(client)

    assert result_code in str(captured.value)
    assert "credential detail" not in str(captured.value)
    assert len(transport.calls) == 1


def test_unknown_business_result_is_sanitized_and_not_retried():
    client, transport, _ = _client(
        [HttpResponse(200, _body("99", "unknown provider message"), {})]
    )

    with pytest.raises(KmaObservationUnknownResultError) as captured:
        _fetch(client)

    assert "resultCode=99" in str(captured.value)
    assert "unknown provider message" not in str(captured.value)
    assert len(transport.calls) == 1


def test_non_retryable_http_status_is_sanitized():
    client, transport, _ = _client([HttpResponse(403, b"secret body", {})])

    with pytest.raises(KmaObservationHttpError) as captured:
        _fetch(client)

    assert str(captured.value) == "KMA observation HTTP status=403"
    assert len(transport.calls) == 1


def test_malformed_success_json_is_not_retried():
    client, transport, _ = _client([HttpResponse(200, b"not-json", {})])

    with pytest.raises(WeatherSourceSchemaError):
        _fetch(client)

    assert len(transport.calls) == 1


def test_parser_uses_the_same_sanitized_result_policy():
    with pytest.raises(KmaObservationThrottleError) as captured:
        parse_kma_observation_response(_body("23", "do not expose"))

    assert "do not expose" not in str(captured.value)
