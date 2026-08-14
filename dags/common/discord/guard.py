"""알림 폭풍(storm) 가드 — 같은 dag_run 의 반복 실패 알림을 1건으로 묶는다 (#161).

culture 검토 의견의 v1 필수 요구: 동적 매핑 N태스크가 동시에 죽으면(예: culture_bronze
12태스크 × 자정 API 장애) 에러 embed N발이 한꺼번에 나가 rate limit 에 걸리고
정작 중요한 알림이 드랍된다. 최소 대책으로 **run 단위 집계**를 넣는다:
같은 (dag_id, run_id)의 첫 실패만 전송하고 이후 실패는 조용히 R2 기록만 남긴다.

구현은 공유 볼륨 마커 파일(`open(..., "x")` 원자 생성)이다. scheduler/worker 가
`airflow_logs` 볼륨을 공유하는 현 LocalExecutor 단일 호스트 구성에서 프로세스 간
안전하다. (멀티 호스트로 가면 DB/Variable 기반으로 교체 — 모듈 docstring 계약만
유지하면 됨.) 가드 실패는 fail-open: 중복 전송이 침묵보다 낫다.

마커는 빈 파일이고 자동 삭제하지 않는다 — run 종료 후엔 불필요하므로, `airflow_logs`
볼륨(로그) 정리 시 `_discord_notify_guard/` 도 함께 비우면 된다(culture 리뷰 #194).
"""
from __future__ import annotations

import logging
import os
import re

LOGGER = logging.getLogger(__name__)

GUARD_DIR_ENV = "DISCORD_NOTIFY_GUARD_DIR"
_DEFAULT_GUARD_DIR = "/opt/airflow/logs/_discord_notify_guard"

_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")


def _marker_path(dag_id: str, run_id: str) -> str:
    base = os.environ.get(GUARD_DIR_ENV) or _DEFAULT_GUARD_DIR
    name = _UNSAFE.sub("-", f"{dag_id}__{run_id}")
    return os.path.join(base, name)


def first_notice_for_run(dag_id: str | None, run_id: str | None) -> bool:
    """이 (dag_id, run_id)에서 첫 알림이면 True(마커 생성), 이미 보냈으면 False.

    식별자가 없거나 마커 파일시스템이 실패하면 True (fail-open).
    """
    if not dag_id or not run_id:
        return True
    path = _marker_path(dag_id, run_id)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "x"):
            pass
        return True
    except FileExistsError:
        return False
    except Exception as exc:  # noqa: BLE001 -- 가드 고장이 알림을 막으면 안 됨
        LOGGER.warning("[discord] notify guard 실패(fail-open): %s", type(exc).__name__)
        return True
