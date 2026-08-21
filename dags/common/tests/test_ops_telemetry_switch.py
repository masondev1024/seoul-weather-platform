"""소비자 없는 ops 관측 기록기가 기본 off 인지, 그리고 무엇이 잠기지 않는지 못박는다.

이 fork 에는 ``ops/*`` 를 읽는 소비자가 없다(상류의 ``common_ops_d1_load`` ·
``common_ops_logship`` 은 이관되지 않았고 ops 대시보드는 폐기됐다). 그래서 run/metric
계열 기록은 기본 off 로 두되, **실패 상세(errors)와 파이프라인이 되읽는 control 상태는
그대로 남는다** — 이 테스트가 그 경계다.
"""

from __future__ import annotations

import pytest

import common.ops.run_sink as run_sink
import common.runmetrics as runmetrics
from common.ops.telemetry_switch import OPS_TELEMETRY_ENV, ops_telemetry_enabled


@pytest.fixture(autouse=True)
def _clean_switch_env(monkeypatch):
    monkeypatch.delenv(OPS_TELEMETRY_ENV, raising=False)


def _explode(*_args, **_kwargs):
    raise AssertionError("consumer-less ops telemetry must not reach R2")


# ── 스위치 자체 ────────────────────────────────────────────────────────────────

def test_telemetry_is_off_when_the_switch_is_unset():
    assert ops_telemetry_enabled() is False


@pytest.mark.parametrize("raw", ["1", "true", "TRUE", "Yes", " on "])
def test_documented_truthy_values_reopen_the_writers(monkeypatch, raw):
    monkeypatch.setenv(OPS_TELEMETRY_ENV, raw)

    assert ops_telemetry_enabled() is True


@pytest.mark.parametrize("raw", ["", "0", "false", "no", "off", "maybe"])
def test_unrecognized_values_stay_off_rather_than_guessing(monkeypatch, raw):
    monkeypatch.setenv(OPS_TELEMETRY_ENV, raw)

    assert ops_telemetry_enabled() is False


def test_switch_is_read_per_call_so_it_can_be_flipped_without_restart(monkeypatch):
    assert ops_telemetry_enabled() is False
    monkeypatch.setenv(OPS_TELEMETRY_ENV, "1")
    assert ops_telemetry_enabled() is True


# ── ops/runs · ops/product-events · ops/product-health (run_sink._put_r2 관문) ──

def test_run_sink_put_skips_r2_entirely_when_disabled(monkeypatch):
    from common.errors.sink import R2ErrorSink

    monkeypatch.setattr(R2ErrorSink, "_put_r2_object", staticmethod(_explode))

    run_sink._put_r2("ops/runs/domain=weather/observed_date=2026-08-19/x.json", b"{}")


def test_run_sink_put_reaches_r2_again_once_the_switch_is_on(monkeypatch):
    from common.errors.sink import R2ErrorSink

    writes: list[tuple[str, bytes]] = []
    monkeypatch.setattr(
        R2ErrorSink,
        "_put_r2_object",
        staticmethod(lambda key, payload: writes.append((key, payload))),
    )
    monkeypatch.setenv(OPS_TELEMETRY_ENV, "1")

    run_sink._put_r2("ops/runs/domain=weather/observed_date=2026-08-19/x.json", b"{}")

    assert writes == [("ops/runs/domain=weather/observed_date=2026-08-19/x.json", b"{}")]


def test_record_run_callback_stays_wired_and_silent_when_disabled(monkeypatch):
    """콜백 자리는 그대로 둔다 — ops 대시보드를 다시 세우면 스위치만 켜면 된다."""
    from common.errors.sink import R2ErrorSink

    monkeypatch.setattr(R2ErrorSink, "_put_r2_object", staticmethod(_explode))
    callback = run_sink.record_run("weather", "bronze", status="success")

    callback({"params": {"target": "dev"}})


def test_product_event_still_builds_its_record_but_writes_nothing(monkeypatch):
    from common.errors.sink import R2ErrorSink
    import common.ops.product_observability as product_observability

    monkeypatch.setattr(R2ErrorSink, "_put_r2_object", staticmethod(_explode))
    monkeypatch.setattr(product_observability, "resolve_runtime_target", lambda: "dev")

    event = product_observability.record_product_event(
        {"run_id": "run-1"}, domain="weather", layer="bronze"
    )

    assert event["domain"] == "weather"
    assert event["layer"] == "bronze"


# ── ops/metrics (MetricsR2Sink) ────────────────────────────────────────────────

def test_metrics_r2_sink_skips_the_put_when_disabled(monkeypatch):
    """gate 는 ``_put_r2_object`` 안에 있으므로, 실제 R2 진입로(boto3)를 막아 확인한다."""
    import sys
    import types

    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = _explode  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    sink = runmetrics.MetricsR2Sink()

    key = sink.write({"domain": "weather", "dag_id": "d", "task_id": "t", "run_id": "r",
                      "try_number": 1, "started_at": "2026-08-19T00:00:00+00:00"})

    assert key.startswith("ops/metrics/weather/observed_date=")


def test_metrics_r2_sink_reaches_r2_again_once_the_switch_is_on(monkeypatch):
    import sys
    import types

    calls: list[dict] = []

    class _Client:
        @staticmethod
        def put_object(**kwargs):
            calls.append(kwargs)

    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = lambda *_a, **_k: _Client()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    monkeypatch.setattr("common.storage.r2_env", lambda name: f"value-for-{name}")
    monkeypatch.setenv(OPS_TELEMETRY_ENV, "1")
    sink = runmetrics.MetricsR2Sink()

    sink.write({"domain": "weather", "dag_id": "d", "task_id": "t", "run_id": "r",
                "try_number": 1, "started_at": "2026-08-19T00:00:00+00:00"})

    assert len(calls) == 1
    assert calls[0]["Key"].startswith("ops/metrics/weather/observed_date=")


def test_dbt_metric_records_are_still_parsed_and_returned_when_disabled(tmp_path):
    """gate 는 PUT 만 막는다 — 태스크 반환값(`{"rows": len(records)}`)이 바뀌면 안 된다."""
    run_results = tmp_path / "run_results.json"
    run_results.write_text(
        '{"results": [{"unique_id": "model.p.m", "status": "success",'
        ' "execution_time": 1.5}]}',
        encoding="utf-8",
    )

    records = runmetrics.dump_dbt_run_results(run_results, "weather", target="dev")

    assert len(records) == 1


def test_injected_metrics_sink_stays_usable_for_local_debugging(monkeypatch):
    """주입 seam 은 gate 위가 아니라 아래에 있다 — 로컬 디버그 sink 는 계속 쓴다."""
    writes: list[str] = []
    sink = runmetrics.MetricsR2Sink(put_object=lambda key, _payload: writes.append(key))

    sink.write({"domain": "weather", "dag_id": "d", "task_id": "t", "run_id": "r",
                "try_number": 1, "started_at": "2026-08-19T00:00:00+00:00"})

    assert len(writes) == 1


# ── 잠기지 않는 것 ──────────────────────────────────────────────────────────────

def test_error_sink_keeps_writing_because_discord_and_humans_read_it(monkeypatch):
    from datetime import datetime, timezone

    from common.errors.problem import Problem
    from common.errors.sink import R2ErrorSink

    writes: list[str] = []
    sink = R2ErrorSink(put_object=lambda key, _payload: writes.append(key))
    problem = Problem.from_exception(
        RuntimeError("boom"),
        domain="weather",
        dag_id="weather_vilage_fcst_bronze",
        task_id="land_kma_raw",
        run_id="scheduled__2026-08-19T00:00:00+00:00",
        try_number=1,
    )
    object.__setattr__(problem, "occurred_at", datetime(2026, 8, 19, tzinfo=timezone.utc))

    sink.write(problem)

    assert writes and writes[0].startswith("ops/errors/weather/observed_date=")


def test_control_zone_receipts_never_route_through_the_gated_put(monkeypatch):
    """control 상태는 파이프라인이 되읽는다(R-4 자동 삭제 금지) — gate 밖에 있어야 한다."""
    import inspect

    from common.collection_slots import receipts

    source = inspect.getsource(receipts)

    assert "_put_r2" not in source
    assert "write_json_if_absent" in source
