import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.http import TransportResponse  # noqa: E402
from common.http.errors import HttpProblemError  # noqa: E402
from common.security import PLACEHOLDER  # noqa: E402
from weather_ingest.common import runtime  # noqa: E402


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def send(self, method, url, *, params, headers, timeout):
        self.calls.append(
            (method, url, dict(params or {}), dict(headers or {}), timeout)
        )
        return self.responses.pop(0)


def test_fetch_url_retries_configured_429(monkeypatch):
    transport = FakeTransport(
        [
            TransportResponse(status=429, headers={"Retry-After": "0"}),
            TransportResponse(status=200, content=b"ok"),
        ]
    )
    monkeypatch.setenv("KMA_SERVICE_KEY", "kma-key-12345")
    monkeypatch.setattr(runtime._HTTP, "_transport", transport)

    sleeps = []
    monkeypatch.setattr(runtime.time, "sleep", sleeps.append)

    assert runtime.fetch_url(
        "https://example.test/data",
        "ask-seoul-test/1.0",
        max_attempts=2,
        retry_statuses=(429,),
        retry_base_delay_seconds=30,
    ) == (200, b"ok")

    assert len(transport.calls) == 2
    assert transport.calls[0][1] == "https://example.test/data"
    assert transport.calls[0][2] == {"serviceKey": "kma-key-12345"}
    assert transport.calls[1][2] == {"serviceKey": "kma-key-12345"}
    assert sleeps == [0.0]


def test_fetch_url_uses_429_backoff_schedule(monkeypatch):
    transport = FakeTransport(
        [
            TransportResponse(status=429),
            TransportResponse(status=429),
            TransportResponse(status=200, content=b"ok"),
        ]
    )
    monkeypatch.setenv("KMA_SERVICE_KEY", "kma-service-key")
    monkeypatch.setattr(runtime._HTTP, "_transport", transport)

    sleeps = []
    monkeypatch.setattr(runtime.time, "sleep", sleeps.append)

    assert runtime.fetch_url(
        "https://example.test/data",
        "ask-seoul-test/1.0",
        max_attempts=3,
        retry_statuses=(429,),
        retry_base_delay_seconds=30,
        retry_429_backoff_seconds=(3600, 5400, 7200),
    ) == (200, b"ok")

    assert len(transport.calls) == 3
    assert sleeps == [3600.0, 5400.0]


def test_fetch_url_raises_redacted_metadata_on_retriable_exhaustion(monkeypatch):
    transport = FakeTransport(
        [TransportResponse(status=500), TransportResponse(status=500)]
    )
    monkeypatch.setenv("KMA_SERVICE_KEY", "kma-secret-key")
    monkeypatch.setattr(runtime._HTTP, "_transport", transport)

    with pytest.raises(HttpProblemError) as exc_info:
        runtime.fetch_url(
            "https://example.test/data",
            "ask-seoul-test/1.0",
            max_attempts=2,
            retry_statuses=(500,),
            retry_base_delay_seconds=1,
        )

    request = exc_info.value.problem.request
    assert PLACEHOLDER in request["url"]
    assert "kma-secret-key" not in request["url"]


def test_fetch_url_calls_budget_hook_once_before_each_physical_retry(monkeypatch):
    transport = FakeTransport(
        [
            TransportResponse(status=503),
            TransportResponse(status=200, content=b"ok"),
        ]
    )
    monkeypatch.setenv("KMA_SERVICE_KEY", "kma-key")
    monkeypatch.setattr(runtime._HTTP, "_transport", transport)
    monkeypatch.setattr(runtime.time, "sleep", lambda _seconds: None)
    attempts = []

    result = runtime.fetch_url(
        "https://example.test/data",
        "ask-seoul-test/1.0",
        max_attempts=2,
        retry_statuses=(503,),
        before_attempt=attempts.append,
    )

    assert result == (200, b"ok")
    assert attempts == [1, 2]
    assert len(transport.calls) == 2


def test_fetch_url_budget_rejection_performs_no_physical_http(monkeypatch):
    transport = FakeTransport([TransportResponse(status=200, content=b"ok")])
    monkeypatch.setenv("KMA_SERVICE_KEY", "kma-key")
    monkeypatch.setattr(runtime._HTTP, "_transport", transport)

    def reject(_attempt):
        raise RuntimeError("daily budget rejected")

    with pytest.raises(RuntimeError, match="daily budget rejected"):
        runtime.fetch_url(
            "https://example.test/data",
            "ask-seoul-test/1.0",
            before_attempt=reject,
        )

    assert transport.calls == []
