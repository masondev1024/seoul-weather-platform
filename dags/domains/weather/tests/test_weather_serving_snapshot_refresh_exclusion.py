"""시간별 serving snapshot refresh 가 transform 과 겹치지 않게 막는 가드.

두 DAG 는 같은 공개 Gold 4종을 각자의 serving as-of hour 로 다시 쓴다. pool
(`trino_weather_legacy_heavy`, 1 슬롯)은 **태스크 단위**로만 직렬화하므로,
transform 이 `dbt_run_gold` 를 끝내고 슬롯을 놓는 순간 대기 중이던 refresh 가
슬롯을 잡아 **transform 의 run 과 test 사이에 끼어들어** 같은 테이블을 덮어쓴다.
그러면 transform 의 `dbt_test_gold` 는 자기가 쓴 것과 다른 데이터를 검증하게 되어
427 개 장소 전부에서 reconciliation 이 깨진다(2026-08-16 실측:
run_gold 14:59-15:07 → refresh run 15:07-15:14 → transform test_gold 15:29 실패).

refresh 는 매시 도는 보정 작업이고 transform 이 더 신선한 Bronze 로 같은 제품을
만들므로, 겹칠 때 **양보하는 쪽은 refresh** 다. 다음 정시에 다시 돌기 때문에
skip 이 안전하고 자기 치유적이다.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

WEATHER_ROOT = Path(__file__).resolve().parents[1]
if str(WEATHER_ROOT) not in sys.path:
    sys.path.insert(0, str(WEATHER_ROOT))

from weather_serving_exclusion import (  # noqa: E402
    TRANSFORM_DAG_ID,
    should_skip_serving_snapshot_refresh,
)


def test_skips_when_the_transform_dag_is_running():
    """transform 이 도는 중이면 refresh 는 Gold 를 건드리지 않고 물러난다."""
    assert should_skip_serving_snapshot_refresh(
        running_dag_ids=(TRANSFORM_DAG_ID,)
    ) is True


def test_runs_when_no_transform_is_active():
    assert should_skip_serving_snapshot_refresh(running_dag_ids=()) is False


def test_ignores_unrelated_running_dags():
    """다른 도메인/무관한 DAG 가 돈다고 시간별 보정을 멈추지 않는다."""
    assert should_skip_serving_snapshot_refresh(
        running_dag_ids=("traffic_incident_bronze", "weather_vilage_fcst_bronze")
    ) is False


def test_guard_raises_skip_so_the_dag_run_is_not_marked_failed():
    """겹침은 정상 상황이라 실패가 아니라 skip 으로 끝나야 한다."""
    from airflow.exceptions import AirflowSkipException

    from weather_serving_exclusion import guard_serving_snapshot_refresh

    with pytest.raises(AirflowSkipException):
        guard_serving_snapshot_refresh(
            running_dag_ids_provider=lambda: (TRANSFORM_DAG_ID,)
        )


def test_guard_returns_running_state_when_clear():
    from weather_serving_exclusion import guard_serving_snapshot_refresh

    assert guard_serving_snapshot_refresh(
        running_dag_ids_provider=lambda: ()
    ) == {"skipped": False}


def test_guard_is_wired_into_the_refresh_dag_before_any_write():
    """가드가 DAG 에 실제로 붙어 있고, Gold 를 쓰는 태스크보다 앞에 있어야 한다."""
    import weather_serving_snapshot_refresh as refresh_dag

    dag = refresh_dag.dag
    guard = dag.get_task("guard_conflicting_weather_transform")
    write_task = dag.get_task("dbt_run_serving_snapshot_refresh")

    downstream = guard.get_flat_relative_ids(upstream=False)
    assert write_task.task_id in downstream, (
        "가드가 Gold 쓰기 태스크보다 뒤에 있으면 이미 덮어쓴 뒤라 의미가 없다"
    )
