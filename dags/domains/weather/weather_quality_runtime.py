"""Airflow-free runtime contract for Weather forecast-quality evaluation."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


QUALITY_REPAIR_DAYS = 7
FORECAST_LOAD_LOOKBACK_DAYS = 4
QUALITY_TRUTH_POLICY_VERSION = "observation-truth-policy/v2-internal"
QUALITY_VINTAGE_POLICY_VERSION = "forecast-vintage-cutoff/v1"
QUALITY_EVIDENCE_POLICY_VERSION = "metric-evidence-gate/v1"
QUALITY_POP_POLICY_VERSION = "pop-threshold-0.5/v1"
QUALITY_BACKFILL_CONFIRMATION = "BACKFILL_ONE_KST_DATE"
QUALITY_SCHEDULE_ENV = "ASK_SEOUL_WEATHER_QUALITY_DAG_SCHEDULE"

KST = ZoneInfo("Asia/Seoul")
_ISO_KST_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SAFE_RUN_ID_RE = re.compile(r"^[A-Za-z0-9._=:+-]+$")


class QualityWindowError(ValueError):
    """Raised when the quality evaluation window contract is invalid."""


@dataclass(frozen=True, slots=True)
class QualityEvaluationWindow:
    evaluation_run_id: str
    evaluation_as_of: datetime
    window_start_date: date
    window_end_date: date
    forecast_load_start_date: date
    forecast_load_end_date: date

    def as_dbt_vars(self) -> dict[str, str]:
        return {
            "weather_quality_run_id": self.evaluation_run_id,
            "weather_quality_evaluation_as_of": self.evaluation_as_of.isoformat(),
            "weather_quality_window_start_date": self.window_start_date.isoformat(),
            "weather_quality_window_end_date": self.window_end_date.isoformat(),
            "weather_quality_forecast_load_start_date": (
                self.forecast_load_start_date.isoformat()
            ),
            "weather_quality_forecast_load_end_date": (
                self.forecast_load_end_date.isoformat()
            ),
            "weather_quality_truth_policy_version": QUALITY_TRUTH_POLICY_VERSION,
            "weather_quality_vintage_policy_version": QUALITY_VINTAGE_POLICY_VERSION,
            "weather_quality_evidence_policy_version": QUALITY_EVIDENCE_POLICY_VERSION,
            "weather_quality_pop_policy_version": QUALITY_POP_POLICY_VERSION,
        }


def quality_schedule() -> str | None:
    return os.getenv(QUALITY_SCHEDULE_ENV, "").strip() or None


def as_dbt_vars(window: QualityEvaluationWindow) -> dict[str, str]:
    return window.as_dbt_vars()


def resolve_daily_quality_window(
    *,
    now: datetime,
    run_id: str,
) -> QualityEvaluationWindow:
    kst_now = _require_kst_now(now)
    end_date = kst_now.date() - timedelta(days=1)
    start_date = end_date - timedelta(days=QUALITY_REPAIR_DAYS - 1)
    return _build_window(
        window_start_date=start_date,
        window_end_date=end_date,
        kst_now=kst_now,
        run_id=run_id,
    )


def resolve_backfill_quality_window(
    *,
    backfill_date: str,
    confirmation: str,
    now: datetime,
    run_id: str,
) -> QualityEvaluationWindow:
    if confirmation != QUALITY_BACKFILL_CONFIRMATION:
        raise QualityWindowError(
            "weather quality backfill requires confirmation "
            f"{QUALITY_BACKFILL_CONFIRMATION}"
        )
    requested_date = _parse_one_kst_date(backfill_date)
    kst_now = _require_kst_now(now)
    if requested_date >= kst_now.date():
        raise QualityWindowError(
            "weather quality backfill requires a complete past KST date"
        )
    return _build_window(
        window_start_date=requested_date,
        window_end_date=requested_date,
        kst_now=kst_now,
        run_id=run_id,
    )


def _build_window(
    *,
    window_start_date: date,
    window_end_date: date,
    kst_now: datetime,
    run_id: str,
) -> QualityEvaluationWindow:
    evaluation_run_id = _require_safe_run_id(run_id)
    evaluation_as_of = _evaluation_as_of(window_end_date, kst_now)
    return QualityEvaluationWindow(
        evaluation_run_id=evaluation_run_id,
        evaluation_as_of=evaluation_as_of,
        window_start_date=window_start_date,
        window_end_date=window_end_date,
        forecast_load_start_date=(
            window_start_date - timedelta(days=FORECAST_LOAD_LOOKBACK_DAYS)
        ),
        forecast_load_end_date=window_end_date - timedelta(days=1),
    )


def _require_kst_now(now: datetime) -> datetime:
    if now.tzinfo is None or now.utcoffset() is None:
        raise QualityWindowError(
            "weather quality window requires a timezone-aware datetime"
        )
    return now.astimezone(KST)


def _require_safe_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or not run_id or run_id != run_id.strip():
        raise QualityWindowError(
            "weather quality window requires a safe evaluation run ID"
        )
    if not _SAFE_RUN_ID_RE.fullmatch(run_id):
        raise QualityWindowError(
            "weather quality window requires a safe evaluation run ID"
        )
    return run_id


def _parse_one_kst_date(value: str) -> date:
    if not isinstance(value, str) or not _ISO_KST_DATE_RE.fullmatch(value):
        raise QualityWindowError(
            "weather quality backfill requires a single KST date in ISO format"
        )
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise QualityWindowError(
            "weather quality backfill requires a single KST date in ISO format"
        ) from exc


def _evaluation_as_of(window_end_date: date, kst_now: datetime) -> datetime:
    kst_boundary = datetime.combine(
        window_end_date + timedelta(days=1),
        time(
            hour=kst_now.hour,
            minute=kst_now.minute,
            second=kst_now.second,
            microsecond=kst_now.microsecond,
            tzinfo=KST,
        ),
    )
    return kst_boundary.astimezone(timezone.utc)
