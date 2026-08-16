"""공개 Gold 를 함께 쓰는 Weather DAG 사이의 상호배제.

`weather_vilage_fcst_transform` 과 `weather_serving_snapshot_refresh` 는 같은
공개 Gold 4종을 각자 고정한 serving as-of hour 로 다시 쓴다. 공용 pool
(`trino_weather_legacy_heavy`)은 **태스크 하나 단위**로만 직렬화하므로 두 DAG 가
동시에 쓰는 것은 막아도, transform 의 `dbt_run_gold` 와 `dbt_test_gold` **사이**에
refresh 가 끼어드는 것은 막지 못한다. 그러면 transform 은 자기가 쓴 것과 다른
데이터를 검증하게 되어 reconciliation 계약이 전 장소에서 깨진다.

실측(2026-08-16):

    14:59-15:07  transform  dbt_run_gold                  (as-of H1 로 Gold 작성)
    15:07-15:14  refresh    dbt_run_serving_snapshot_...  (as-of H2 로 같은 Gold 재작성)
    15:29-15:44  transform  dbt_test_gold                 → 427 장소 전부 불일치로 실패

양보하는 쪽은 refresh 다. transform 이 더 신선한 Bronze 로 같은 제품을 만들고,
refresh 는 매시 도는 보정이라 한 번 건너뛰어도 다음 정시에 복구된다.
"""
from __future__ import annotations

from typing import Callable, Iterable

TRANSFORM_DAG_ID = "weather_vilage_fcst_transform"

#: refresh 가 양보해야 하는 상대. 같은 공개 Gold 를 쓰는 DAG 만 넣는다.
CONFLICTING_DAG_IDS = frozenset({TRANSFORM_DAG_ID})


def should_skip_serving_snapshot_refresh(
    *, running_dag_ids: Iterable[str]
) -> bool:
    """겹치는 DAG 가 실행 중이면 True. 순수 함수라 DB 없이 검증한다."""
    return bool(CONFLICTING_DAG_IDS.intersection(running_dag_ids))


def _running_dag_ids() -> tuple[str, ...]:
    """Airflow 메타DB에서 현재 running 인 충돌 대상 DAG 를 읽는다."""
    from airflow.models import DagRun
    from airflow.utils.state import DagRunState

    found: list[str] = []
    for dag_id in sorted(CONFLICTING_DAG_IDS):
        if DagRun.find(dag_id=dag_id, state=DagRunState.RUNNING):
            found.append(dag_id)
    return tuple(found)


def guard_serving_snapshot_refresh(
    *,
    running_dag_ids_provider: Callable[[], Iterable[str]] | None = None,
    **_context: object,
) -> dict[str, object]:
    """겹치면 skip, 아니면 통과. refresh DAG 의 첫 태스크로 건다.

    겹침은 정상적인 운영 상황이므로 실패가 아니라 skip 으로 끝낸다 — 실패로 두면
    매시 알림이 울리고 진짜 장애와 구분되지 않는다.
    """
    from airflow.exceptions import AirflowSkipException

    provider = running_dag_ids_provider or _running_dag_ids
    running = tuple(provider())
    if should_skip_serving_snapshot_refresh(running_dag_ids=running):
        raise AirflowSkipException(
            "weather serving snapshot refresh skipped: 공개 Gold 를 함께 쓰는 "
            f"DAG 가 실행 중이다 ({', '.join(running)}). 다음 정시에 재시도한다."
        )
    return {"skipped": False}
