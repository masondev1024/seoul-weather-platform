from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_quality_runtime import (  # noqa: E402
    QUALITY_BACKFILL_CONFIRMATION,
    QUALITY_EVIDENCE_POLICY_VERSION,
    QUALITY_POP_POLICY_VERSION,
    QUALITY_TRUTH_POLICY_VERSION,
    QUALITY_VINTAGE_POLICY_VERSION,
    QualityWindowError,
    quality_schedule,
    resolve_backfill_quality_window,
    resolve_daily_quality_window,
    window_from_payload,
    window_payload,
)


KST = ZoneInfo("Asia/Seoul")


def test_daily_window_is_seven_complete_kst_dates():
    now = datetime(2026, 8, 22, 3, 5, tzinfo=KST)
    window = resolve_daily_quality_window(now=now, run_id="scheduled__quality")

    assert window.window_start_date == date(2026, 8, 15)
    assert window.window_end_date == date(2026, 8, 21)
    assert window.forecast_load_start_date == date(2026, 8, 11)
    assert window.forecast_load_end_date == date(2026, 8, 20)
    assert window.evaluation_as_of == datetime(2026, 8, 21, 18, 5, tzinfo=timezone.utc)


def test_backfill_rejects_range_or_wrong_confirmation():
    with pytest.raises(QualityWindowError, match="single KST date"):
        resolve_backfill_quality_window(
            backfill_date="2026-08-20/2026-08-21",
            confirmation=QUALITY_BACKFILL_CONFIRMATION,
            now=datetime.now(timezone.utc),
            run_id="manual__bad",
        )


def test_daily_window_uses_kst_yesterday_across_year_boundary():
    now = datetime(2027, 1, 1, 3, 30, tzinfo=KST)
    window = resolve_daily_quality_window(now=now, run_id="scheduled__year_boundary")

    assert window.window_start_date == date(2026, 12, 25)
    assert window.window_end_date == date(2026, 12, 31)
    assert window.forecast_load_start_date == date(2026, 12, 21)
    assert window.forecast_load_end_date == date(2026, 12, 30)
    assert window.evaluation_as_of == datetime(2026, 12, 31, 18, 30, tzinfo=timezone.utc)


def test_quality_window_requires_aware_datetime():
    with pytest.raises(QualityWindowError, match="timezone-aware"):
        resolve_daily_quality_window(
            now=datetime(2026, 8, 22, 3, 5),
            run_id="scheduled__quality",
        )


@pytest.mark.parametrize(
    "run_id",
    ["", "   ", " scheduled__quality ", "../manual", "manual/run", "한글"],
)
def test_quality_window_rejects_blank_or_unsafe_run_ids(run_id):
    with pytest.raises(QualityWindowError, match="safe evaluation run ID"):
        resolve_daily_quality_window(
            now=datetime(2026, 8, 22, 3, 5, tzinfo=KST),
            run_id=run_id,
        )


def test_backfill_accepts_one_past_iso_kst_date():
    window = resolve_backfill_quality_window(
        backfill_date="2026-08-20",
        confirmation=QUALITY_BACKFILL_CONFIRMATION,
        now=datetime(2026, 8, 22, 3, 5, tzinfo=KST),
        run_id="manual__2026_08_20",
    )

    assert window.window_start_date == date(2026, 8, 20)
    assert window.window_end_date == date(2026, 8, 20)
    assert window.forecast_load_start_date == date(2026, 8, 16)
    assert window.forecast_load_end_date == date(2026, 8, 19)
    assert window.evaluation_as_of == datetime(2026, 8, 20, 18, 5, tzinfo=timezone.utc)


@pytest.mark.parametrize("backfill_date", ["2026-8-20", "2026-02-30", "20260820"])
def test_backfill_rejects_non_iso_or_invalid_dates(backfill_date):
    with pytest.raises(QualityWindowError, match="single KST date"):
        resolve_backfill_quality_window(
            backfill_date=backfill_date,
            confirmation=QUALITY_BACKFILL_CONFIRMATION,
            now=datetime(2026, 8, 22, 3, 5, tzinfo=KST),
            run_id="manual__bad_date",
        )


@pytest.mark.parametrize("backfill_date", ["2026-08-22", "2026-08-23"])
def test_backfill_rejects_current_or_future_kst_dates(backfill_date):
    with pytest.raises(QualityWindowError, match="complete past KST date"):
        resolve_backfill_quality_window(
            backfill_date=backfill_date,
            confirmation=QUALITY_BACKFILL_CONFIRMATION,
            now=datetime(2026, 8, 22, 3, 5, tzinfo=KST),
            run_id="manual__not_complete",
        )


def test_backfill_requires_exact_confirmation_without_echoing_bad_value():
    bad_confirmation = "BACKFILL_ONE_KST_DATE serviceKey=secret password=hunter2"

    with pytest.raises(QualityWindowError) as exc_info:
        resolve_backfill_quality_window(
            backfill_date="2026-08-20",
            confirmation=bad_confirmation,
            now=datetime(2026, 8, 22, 3, 5, tzinfo=KST),
            run_id="manual__wrong_confirmation",
        )

    message = str(exc_info.value)
    assert QUALITY_BACKFILL_CONFIRMATION in message
    assert bad_confirmation not in message
    assert "serviceKey" not in message
    assert "hunter2" not in message


def test_as_dbt_vars_returns_stable_contract_keys_and_policy_versions():
    window = resolve_daily_quality_window(
        now=datetime(2026, 8, 22, 3, 5, tzinfo=KST),
        run_id="scheduled__quality",
    )

    assert window.as_dbt_vars() == {
        "weather_quality_run_id": "scheduled__quality",
        "weather_quality_evaluation_as_of": "2026-08-21T18:05:00+00:00",
        "weather_quality_window_start_date": "2026-08-15",
        "weather_quality_window_end_date": "2026-08-21",
        "weather_quality_forecast_load_start_date": "2026-08-11",
        "weather_quality_forecast_load_end_date": "2026-08-20",
        "weather_quality_truth_policy_version": QUALITY_TRUTH_POLICY_VERSION,
        "weather_quality_vintage_policy_version": QUALITY_VINTAGE_POLICY_VERSION,
        "weather_quality_evidence_policy_version": QUALITY_EVIDENCE_POLICY_VERSION,
        "weather_quality_pop_policy_version": QUALITY_POP_POLICY_VERSION,
    }


def test_window_payload_round_trips_and_rejects_mutated_dbt_vars():
    window = resolve_daily_quality_window(
        now=datetime(2026, 8, 22, 3, 5, tzinfo=KST),
        run_id="scheduled__quality",
    )
    payload = window_payload(window)

    assert window_from_payload(payload) == window

    mutated = dict(payload)
    mutated["dbt_vars"] = dict(payload["dbt_vars"])
    mutated["dbt_vars"]["weather_quality_window_end_date"] = "2026-08-20"
    with pytest.raises(QualityWindowError, match="dbt vars are unstable"):
        window_from_payload(mutated)


def test_quality_schedule_trims_env_override_and_defaults_to_none(monkeypatch):
    monkeypatch.delenv("ASK_SEOUL_WEATHER_QUALITY_DAG_SCHEDULE", raising=False)
    assert quality_schedule() is None

    monkeypatch.setenv("ASK_SEOUL_WEATHER_QUALITY_DAG_SCHEDULE", "  ")
    assert quality_schedule() is None

    monkeypatch.setenv("ASK_SEOUL_WEATHER_QUALITY_DAG_SCHEDULE", "  5 21 * * *  ")
    assert quality_schedule() == "5 21 * * *"
