"""Airflow on_failure_callback 팩토리 — DAG 에 한 줄로 연결 (#77 수집 경로 1).

사용 (기존 콜백이 있으면 리스트로 나란히 건다 — 기존 동작 불변):

    from common.errors.airflow import problem_failure_callback
    record_problem = problem_failure_callback(domain="traffic", source_system="seoul_topis")

    PythonOperator(..., on_failure_callback=[existing_callback, record_problem])

콜백은 어떤 경우에도 예외를 밖으로 던지지 않는다 — 에러 기록 실패가
다른 콜백(manifest 기록·알림)이나 태스크 상태를 오염시키지 않게 한다.

#161: R2 Problem 기록에 더해 **Discord 에러 embed 도 전송**한다(전 도메인 합의).
- webhook 은 `<DOMAIN>_DISCORD_WEBHOOK_URL` → `DISCORD_WEBHOOK_URL` 폴백 체인.
- 같은 dag_run 의 반복 실패는 첫 1건만 전송(알림 폭풍 가드 — culture 요구).
- Weather/Traffic은 이 공통 콜백을 단일 실패 알림으로 사용하며 legacy opt-out도 무시.
- 다른 도메인은 `DISCORD_ERROR_NOTIFY_OPTOUT`으로 공통 알림을 제외 가능.
- R2 기록과 Discord 전송은 서로 독립(한쪽 실패가 다른 쪽을 막지 않음).
"""
from __future__ import annotations

import logging
import os
from datetime import timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from common.discord import COLOR_FAIL, first_notice_for_run, send_embed
from common.errors.problem import Problem
from common.errors.sink import R2ErrorSink

LOGGER = logging.getLogger(__name__)

OPTOUT_ENV = "DISCORD_ERROR_NOTIFY_OPTOUT"
_ALWAYS_NOTIFY_DOMAINS = frozenset({"weather", "traffic"})
_KST = timezone(timedelta(hours=9))


def _notify_optout(domain: str) -> bool:
    # Weather/Traffic use this common callback as their single failure notifier.
    # Keep a legacy process-level opt-out available for other domains only.
    if domain in _ALWAYS_NOTIFY_DOMAINS:
        return False
    raw = os.environ.get(OPTOUT_ENV, "")
    return domain in {d.strip() for d in raw.split(",") if d.strip()}


def _dbt_failure_summary(dbt_run_results_path: str) -> str | None:
    """dbt ``run_results.json`` 에서 실패 노드(모델/테스트)와 사유를 사람이 읽는 요약으로.

    dbt 태스크(run/test)가 실패하면 현재 실행의 ``run_results.json`` 에 노드별
    status·message 가 남는다. 이를 파싱해 "어떤 모델/테스트가 왜" 깨졌는지 알림에 넣는다
    (예: ``테스트 unique_grain_gold_seoul_ppltn_daily — 중복 121건``).

    best-effort — 파일 부재/파싱 실패/실패노드 없음이면 None(기존 메시지 유지). 한 프로젝트의
    현재 attempt-local artifact만 읽어 다른 티어의 직전 결과가 섞이지 않게 한다.
    """
    import json
    try:
        with open(dbt_run_results_path, encoding="utf-8") as fh:
            results = json.load(fh).get("results", [])
    except Exception:
        return None
    label_by_kind = {"model": "모델", "test": "테스트", "seed": "seed", "snapshot": "snapshot"}
    bad: list[str] = []
    for r in results:
        status = str(r.get("status", "")).lower()
        if status not in ("error", "fail", "runtime error"):
            continue
        parts = str(r.get("unique_id", "?")).split(".")
        kind = label_by_kind.get(parts[0], parts[0] if parts else "?")
        name = parts[-1] if parts else "?"
        msg = " ".join(str(r.get("message") or "").split())
        bad.append(f"• {kind} `{name}` — {msg[:140]}" if msg else f"• {kind} `{name}`")
    if not bad:
        return None
    extra = f"\n… 외 {len(bad) - 5}건" if len(bad) > 5 else ""
    return "**실패 노드**:\n" + "\n".join(bad[:5]) + extra


def _dbt_run_results_path_from_xcom(
    context: dict[str, Any], *, dbt_project_dir: str, xcom_key: str
) -> str | None:
    """Resolve only the current task's validated attempt-local dbt artifact."""
    ti = context.get("task_instance") or context.get("ti")
    task_id = getattr(ti, "task_id", None)
    xcom_pull = getattr(ti, "xcom_pull", None)
    if not task_id or not callable(xcom_pull):
        return None
    try:
        value = xcom_pull(task_ids=task_id, key=xcom_key)
        if not isinstance(value, str) or not value.strip():
            return None
        candidate = Path(value)
        target_root = (Path(dbt_project_dir) / "target").resolve()
        resolved = candidate.resolve()
        if (
            candidate.name != "run_results.json"
            or candidate.parent.name != "execution"
            or not resolved.is_relative_to(target_root)
        ):
            return None
        return str(resolved)
    except Exception:
        return None


def _problem_embed(problem: Problem, dbt_summary: str | None = None) -> tuple[str, str, str]:
    """Problem → (title, description, footer). 내용은 요약만 — 상세는 R2 문서가 원본."""
    title = f"❌ {problem.domain or '?'} · {problem.dag_id or '?'} 실패"
    lines = [
        f"**task**: `{problem.task_id or '?'}` (try {problem.try_number if problem.try_number is not None else '?'})",
        f"**유형**: {problem.title}",
    ]
    if dbt_summary:
        # dbt 실패면 "무엇이 왜" 를 앞에 — Bash exit 1 같은 뭉뚱그린 detail 보다 유용.
        lines.append(dbt_summary)
    elif problem.detail:
        lines.append(f"**상세**: {problem.detail[:500]}")
    lines.append("같은 run 의 추가 실패는 반복 전송하지 않습니다 — 상세는 R2 errors/ 참고.")
    occurred = problem.occurred_at.astimezone(_KST).strftime("%Y-%m-%d %H:%M:%S KST")
    footer = f"run_id={problem.run_id or '?'} · {occurred}"
    return title, "\n".join(lines), footer


def problem_from_airflow_context(context: dict[str, Any], *, domain: str,
                                 source_system: str | None = None) -> Problem:
    # dag_id/task_id 를 여러 컨텍스트 객체에서 폴백 탐색한다. task-level 콜백 외에
    # 수동 트리거·DAG-level 콜백 등 일부 객체(ti/dag/dag_run)가 빠지는 경로가 있어,
    # 그중 하나만 없어도 dag_id/task_id 가 None 이 되면 알림 카드가 '?'·R2 키가
    # dag_id=unknown/ 으로 degrade 된다(#194 리뷰). `task` 를 포함해 넓게 훑는다.
    ti = context.get("task_instance") or context.get("ti")
    task = context.get("task")
    dag = context.get("dag")
    dag_run = context.get("dag_run")

    def _pick(*values: Any) -> Any:
        for value in values:
            if value:
                return value
        return None

    dag_id = _pick(
        getattr(ti, "dag_id", None),
        getattr(task, "dag_id", None),
        getattr(dag, "dag_id", None),
        getattr(dag_run, "dag_id", None),
    )
    task_id = _pick(
        getattr(ti, "task_id", None),
        getattr(task, "task_id", None),
    )
    run_id = _pick(
        context.get("run_id"),
        getattr(dag_run, "run_id", None),
        getattr(ti, "run_id", None),
    )
    try_number = getattr(ti, "try_number", None)
    return Problem.from_exception(
        context.get("exception"),
        domain=domain,
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        try_number=try_number if isinstance(try_number, int) else None,
        source_system=source_system,
    )


def problem_failure_callback(domain: str, *, source_system: str | None = None,
                             sink: R2ErrorSink | None = None,
                             dbt_project_dir: str | None = None,
                             dbt_run_results_xcom_key: str | None = None,
                             should_notify: Callable[[BaseException | None], bool] | None = None,
                             ) -> Callable[[dict[str, Any]], None]:
    """실패 콜백 팩토리.

    ``dbt_project_dir`` 와 ``dbt_run_results_xcom_key`` 를 함께 주면 현재 task가
    XCom으로 남긴 attempt-local ``run_results.json``만 읽어 실패 모델/테스트·사유를
    알림에 붙인다. XCom key를 생략한 기존 caller는 기존 ``target/run_results.json``
    best-effort 경로를 유지한다.

    ``should_notify`` 를 주면 그 predicate가 ``context["exception"]`` 에 대해 False 를
    돌려주는 실패에 한해 Discord 전송만 건너뛴다(R2 Problem 문서 기록은 그대로 유지 —
    감시 가능성은 보존한다). 생략 시 기존 동작(항상 알림) 그대로.
    """
    error_sink = sink or R2ErrorSink()

    def record_problem(context: dict[str, Any]) -> None:
        problem: Problem | None = None
        try:
            problem = problem_from_airflow_context(
                context, domain=domain, source_system=source_system)
            error_sink.write(problem)
        except Exception as exc:
            # 예외 메시지는 찍지 않는다(시크릿 포함 가능) — 타입명만 남긴다.
            LOGGER.warning("Failed to store problem document: %s", type(exc).__name__)

        # Discord 에러 알림 (#161) — R2 기록과 독립. 어떤 실패도 밖으로 안 던진다.
        try:
            if problem is None or _notify_optout(domain):
                return
            if should_notify is not None and not should_notify(context.get("exception")):
                LOGGER.info(
                    "[discord] known-quiet 실패로 분류되어 알림 스킵 (dag_id=%s)",
                    problem.dag_id,
                )
                return
            if not first_notice_for_run(problem.dag_id, problem.run_id):
                LOGGER.info("[discord] 같은 run 의 실패 알림 이미 전송 — 스킵 (%s)",
                            problem.dag_id)
                return
            dbt_summary = None
            if dbt_project_dir and dbt_run_results_xcom_key:
                artifact_path = _dbt_run_results_path_from_xcom(
                    context,
                    dbt_project_dir=dbt_project_dir,
                    xcom_key=dbt_run_results_xcom_key,
                )
                if artifact_path:
                    dbt_summary = _dbt_failure_summary(artifact_path)
            elif dbt_project_dir:
                dbt_summary = _dbt_failure_summary(
                    os.path.join(dbt_project_dir, "target", "run_results.json")
                )
            title, description, footer = _problem_embed(problem, dbt_summary)
            send_embed(title, description, color=COLOR_FAIL, footer=footer, domain=domain)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to send discord error notice: %s", type(exc).__name__)

    return record_problem
