"""파일 기반 run 기록 — 모든 태스크 실행을 R2 ``runs/`` 에 JSON 1개 (Trino 무관).

관측을 관측대상(Trino)과 분리한다: Trino 가 OOM 으로 죽어도 실행 기록이 남는다(맹점 해소).
데이터 품질 대시보드가 ``runs/`` 를 집계해 도메인별 잔디를 그린다.

.. note:: 이 Weather fork 에는 그 소비자가 없다(D1 적재 DAG 미이관·대시보드 폐기).
   그래서 R2 PUT 은 ``common.ops.telemetry_switch`` 로 기본 off 다 — 코드와 콜백 자리는
   되살릴 수 있게 그대로 둔다.

- ``record_run_metadata`` (Iceberg via Trino) 를 대체 — 콜백 자리는 그대로, 타겟만 R2 파일.
- ``errors/`` (실패 상세·Discord 알림) 와 ``_reports`` (bronze 수집 감사) 는 **별개로 유지**.

경로: ``ops/runs/domain=<d>/observed_date=YYYY-MM-DD(KST)/dag_id=<dag>/<run>__<task>__try<N>.json``
(ASK-Seoul#78 P-7 도메인→날짜 축 · P-8 dev·prod 공통 ops/ · 버킷은 ``r2_env_for`` — dev=seoul-dev/prod=seoul)
R2 자격증명·put 은 common.errors.sink 의 것을 재활용(같은 R2 버킷, dev 전용 — prod 는 r2_env_for 직접 사용).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from common.ops.telemetry_switch import OPS_TELEMETRY_ENV, ops_telemetry_enabled

LOGGER = logging.getLogger(__name__)

RUNS_PREFIX = "ops/runs"  # dev·prod 공통 (ASK-Seoul#78 P-8)
_KST = timezone(timedelta(hours=9))


def runs_prefix(target: str) -> str:
    """runs/ prefix — dev·prod 모두 ``ops/runs`` (ASK-Seoul#78 P-8: 개발 기록도 ops/ 안,
    개발 전용 최상위 ``runs/`` 제거). 과거 dev→'runs'(#230 중앙집중)는 P-8 로 폐기.
    target 은 시그니처 유지용 — 버킷 선택(dev=seoul-dev/prod=seoul)은 _put_r2 가 별도 처리."""
    return "ops/runs"


def _safe(value: object) -> str:
    """경로 세그먼트 안전화 — 영숫자/-_. 만 허용."""
    return "".join(c if (c.isalnum() or c in "-_.") else "_" for c in str(value))[:120]


def _iso(value: object) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def _put_r2(object_key: str, payload: bytes, *, target: str = "dev") -> None:
    # 이 fork 에는 ops/ 를 읽는 소비자가 없다(D1 적재 DAG 미이관·대시보드 폐기). 소비자
    # 없는 기록을 R2 에 영구 적재하지 않도록 **PUT 만** 건너뛴다 — 콜백 자리·레코드 조립은
    # 그대로라 스위치 하나로 되살아난다. 자세한 경계는 common.ops.telemetry_switch 참고.
    if not ops_telemetry_enabled():
        LOGGER.info("[ops] telemetry off (%s 미설정) — 기록 생략: %s",
                    OPS_TELEMETRY_ENV, object_key)
        return
    # dev: 기존 경로 그대로(errors sink 위임 — 바이트 단위 불변, #556 STOP-FIRST).
    # prod: r2_env_for 로 target-aware R2 클라이언트를 직접 build(더 이상 dev 우선 R2ErrorSink
    # 에 위임하지 않음 — 그러면 prod 관측도 seoul-dev 로 새 나간다).
    if (target or "dev").lower() != "prod":
        from common.errors.sink import R2ErrorSink

        R2ErrorSink._put_r2_object(object_key, payload)
        return

    import boto3

    from common.storage import r2_env_for

    boto3.client(
        "s3",
        endpoint_url=r2_env_for("R2_ENDPOINT", target),
        aws_access_key_id=r2_env_for("R2_ACCESS_KEY_ID", target),
        aws_secret_access_key=r2_env_for("R2_SECRET_ACCESS_KEY", target),
        region_name="auto",
    ).put_object(
        Bucket=r2_env_for("R2_BUCKET_NAME", target),
        Key=object_key,
        Body=payload,
        ContentType="application/json; charset=utf-8",
    )


def build_run_record(context: dict, *, domain: str, layer: str, status: str,
                     prefix: str = RUNS_PREFIX) -> tuple[str, dict]:
    """Airflow context → (R2 object key, run record dict). 테스트·백필에서 재사용 가능."""
    ti = context.get("task_instance") or context.get("ti")
    dag = context.get("dag")
    dag_run = context.get("dag_run")

    started = getattr(ti, "start_date", None)
    ended = getattr(ti, "end_date", None) or datetime.now(timezone.utc)
    scheduled = context.get("data_interval_end") or context.get("logical_date")
    duration_s = (
        (ended - started).total_seconds()
        if isinstance(started, datetime) and isinstance(ended, datetime)
        else None
    )
    dag_id = getattr(dag, "dag_id", None) or getattr(ti, "dag_id", None) or "unknown"
    task_id = getattr(ti, "task_id", None) or "unknown"
    run_id = getattr(ti, "run_id", None) or getattr(dag_run, "run_id", None) or "unknown"
    try_number = getattr(ti, "try_number", None)
    exc = context.get("exception")

    anchor = scheduled or started or ended
    obs_date = anchor.astimezone(_KST).strftime("%Y-%m-%d") if isinstance(anchor, datetime) else "unknown"

    record = {
        "domain": domain,
        "layer": layer,
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "try_number": try_number,
        "status": status,
        "scheduled_at": _iso(scheduled if isinstance(scheduled, datetime) else None),
        "started_at": _iso(started if isinstance(started, datetime) else None),
        "ended_at": _iso(ended if isinstance(ended, datetime) else None),
        "duration_s": round(duration_s, 3) if duration_s is not None else None,
        "error": (str(exc)[:500] if exc else None),  # 짧은 요약 — 전체 상세는 errors/
    }
    object_key = (
        f"{prefix}/domain={_safe(domain)}"
        f"/observed_date={obs_date}/dag_id={_safe(dag_id)}"
        f"/{_safe(run_id)}__{_safe(task_id)}__try{try_number}__{_safe(status)}.json"
    )
    return object_key, record


def record_run(domain: str, layer: str, *, status: str):
    """Airflow on_success/on_failure 콜백 — 태스크 1건 = R2 ``runs/`` 파일 1개.

    status: 'success' | 'failed' (콜백 붙는 자리에 맞춰 명시).
    관측 실패가 태스크를 죽이면 안 되므로 fail-open(경고만).
    """

    def _callback(context) -> None:
        try:
            target = (context.get("params") or {}).get("target", "dev")
            prefix = runs_prefix(target)
            object_key, record = build_run_record(
                context, domain=domain, layer=layer, status=status, prefix=prefix)
            _put_r2(object_key, json.dumps(record, ensure_ascii=False, sort_keys=True).encode("utf-8"),
                    target=target)
            LOGGER.info("[ops.runs] %s", object_key)
        except Exception as exc:  # noqa: BLE001 — 관측 실패로 태스크 죽이지 않음
            LOGGER.warning("[ops.runs] 기록 실패(무시): %s", type(exc).__name__)

    return _callback
