"""Context, scheduling, and notification helpers for the KMA Bronze DAG."""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from collections.abc import Callable
from datetime import datetime
from functools import wraps
from typing import ParamSpec, TypeVar

from airflow.sdk.exceptions import AirflowFailException

from weather_ingest.errors import (
    WeatherBronzeConfigurationError,
    WeatherBronzeDeterministicError,
)
from weather_ingest.kma import KST
from weather_ingest.raw_contract import raw_object_page_no as raw_object_page_no


KMA_PUBLISH_CRON_KST = "20 2,5,8,11,14,17,20,23 * * *"
WEATHER_DISCORD_WEBHOOK_ENV = "WEATHER_DISCORD_WEBHOOK_URL"
DISCORD_GREEN = 3066993
DISCORD_RED = 15158332
LOGGER = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


def fail_fast_weather_bronze(callable_: Callable[P, R]) -> Callable[P, R]:
    """Map only permanent Weather contract failures to Airflow no-retry errors."""

    @wraps(callable_)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return callable_(*args, **kwargs)
        except WeatherBronzeDeterministicError as exc:
            raise AirflowFailException(str(exc)) from exc

    return wrapped


DAG_ID = "weather_vilage_fcst_bronze"
RECOLLECT_DAG_ID = "weather_vilage_fcst_recollect"
BACKFILL_DAG_ID = "weather_vilage_fcst_bronze_backfill"
RECOVERY_HINT_MAX_KEYS = 8
RECOVERY_HINT_MAX_CHARS = 1400
KMA_RAW_TASK_ID_LAND = "land_kma_raw"
KMA_RAW_TASK_ID_LAND_FROM_KEYS = "land_kma_raw_from_keys"
LOGGER = logging.getLogger(__name__)


def dag_run_conf(context: dict) -> dict:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return conf if isinstance(conf, dict) else {}


def raw_object_keys_from_conf(context: dict) -> list[str]:
    raw_keys = dag_run_conf(context).get("raw_object_keys")
    if isinstance(raw_keys, str):
        raw_keys = [raw_keys]
    if not isinstance(raw_keys, list):
        raise WeatherBronzeConfigurationError(
            "dag_run.conf.raw_object_keys must be a non-empty string or list."
        )
    cleaned = [str(key).strip() for key in raw_keys if str(key).strip()]
    if not cleaned:
        raise WeatherBronzeConfigurationError(
            "dag_run.conf.raw_object_keys must not be empty."
        )
    return cleaned


def current_dag_id(context: dict) -> str:
    return getattr(context.get("dag"), "dag_id", DAG_ID)


def pull_kma_raw_result(context: dict) -> dict:
    ti = context["ti"]
    raw_result = ti.xcom_pull(task_ids=KMA_RAW_TASK_ID_LAND) or {}
    if not raw_result:
        raw_result = ti.xcom_pull(task_ids=KMA_RAW_TASK_ID_LAND_FROM_KEYS) or {}
    return raw_result


def format_raw_object_keys_for_recovery(raw_object_keys: list[str]) -> str:
    if not raw_object_keys:
        return "raw_object_keys=none"
    preview = raw_object_keys[:RECOVERY_HINT_MAX_KEYS]
    formatted = f"raw_object_keys(count={len(raw_object_keys)}): {','.join(preview)}"
    if len(raw_object_keys) > len(preview):
        formatted += f", +{len(raw_object_keys) - len(preview)} more"
    if len(formatted) > RECOVERY_HINT_MAX_CHARS:
        formatted = f"{formatted[: RECOVERY_HINT_MAX_CHARS - 3]}..."
    return formatted


def discord_report_date(context) -> str:
    logical_date = context.get("logical_date")
    if logical_date:
        return logical_date.astimezone(KST).strftime("%Y-%m-%d")
    return datetime.now(KST).strftime("%Y-%m-%d")


def target_name() -> str:
    return os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod"))


def short_text(value: object, limit: int = 130) -> str:
    text = str(value or "N/A")
    return text if len(text) <= limit else text[: limit - 3] + "..."


def stage_name(task_id: str) -> str:
    if "land" in task_id or "ingest" in task_id:
        return "API 수집/R2 적재"
    if "load" in task_id:
        return "Bronze 적재"
    if "verify" in task_id:
        return "Bronze 검증"
    return "알 수 없음"


def send_weather_discord(title: str, description: str, color: int, footer: str) -> None:
    webhook_url = (os.environ.get(WEATHER_DISCORD_WEBHOOK_ENV) or "").strip()
    if not webhook_url:
        LOGGER.info("[weather notify:noop] %s (webhook url not configured)", title)
        return
    payload = {
        "embeds": [
            {
                "title": title,
                "description": description[:4096],
                "color": color,
                "footer": {"text": footer[:2048]},
            }
        ]
    }
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "ask-seoul-airflow/1.0",
        },
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=10).close()
    except Exception as exc:
        LOGGER.warning("[weather notify] Discord send failed: %s", type(exc).__name__)


def notify_weather_bronze_success(context) -> None:
    ti = context["ti"]
    ingest_result = ti.xcom_pull(task_ids="load_kma_bronze") or {}
    raw_keys = ingest_result.get("raw_object_keys") or []
    api_call_count = ingest_result.get("api_call_count", len(raw_keys))
    api_request_count = ingest_result.get("api_request_count", "N/A")
    run_id = context["run_id"]
    send_weather_discord(
        f"기상청 단기예보 수집 리포트 - {discord_report_date(context)} (target={target_name()})",
        "\n".join(
            [
                "✅ 수집 상태: 성공",
                f"✅ 예보 발표시각: {ingest_result.get('base_date', 'N/A')} {ingest_result.get('base_time', 'N/A')}",
                f"✅ raw page: {api_call_count}개",
                f"✅ actual API requests: {api_request_count}회",
                f"✅ 서울 격자 커버리지: {ingest_result.get('grid_count', 'N/A')}개 grid",
                f"✅ raw JSON: {len(raw_keys)}개",
                f"✅ Bronze 적재: {int(ingest_result.get('inserted', 0)):,}행",
                "",
                "테이블: `bronze_kma_vilage_fcst`",
                f"raw 샘플: `{short_text(raw_keys[0] if raw_keys else 'N/A')}`",
            ]
        ),
        DISCORD_GREEN,
        f"dag_id={context['dag'].dag_id} · run_id={short_text(run_id, 180)}",
    )


def notify_weather_bronze_failure(context) -> None:
    ti = context.get("ti") or context.get("task_instance")
    task_id = getattr(ti, "task_id", "N/A")
    exc = context.get("exception")
    run_id = context.get("run_id", "N/A")
    raw_keys = []
    try:
        raw_keys = pull_kma_raw_result(context).get("raw_object_keys", [])
    except Exception:
        raw_keys = []
    send_weather_discord(
        f"기상청 단기예보 수집 실패 - {discord_report_date(context)} (target={target_name()})",
        "\n".join(
            [
                "❌ 수집 상태: 실패",
                f"❌ 실패 단계: {stage_name(task_id)}",
                f"❌ 실패 task: `{task_id}`",
                f"❌ 오류 유형: `{type(exc).__name__ if exc else 'N/A'}`",
                "",
                f"raw_object_keys_hint: {format_raw_object_keys_for_recovery(raw_keys)}",
                f"Airflow 로그: {getattr(ti, 'log_url', 'N/A')}",
            ]
        ),
        DISCORD_RED,
        f"dag_id={context['dag'].dag_id} · run_id={short_text(run_id, 180)}",
    )


def kma_dag_schedule() -> str | None:
    if "ASK_SEOUL_KMA_DAG_SCHEDULE" in os.environ:
        return os.environ["ASK_SEOUL_KMA_DAG_SCHEDULE"] or None
    return KMA_PUBLISH_CRON_KST
