"""Opt-in switch for the ops telemetry writers that lost their consumer in this fork.

이 저장소는 Weather 전용으로 갈라져 나왔고, ``ops/*`` 기록을 **읽는 쪽이 하나도 없다**.
상류에서 R2 ``ops/*`` 를 Cloudflare D1 ``_ops_*`` 표로 나르던 ``common_ops_d1_load`` 와
태스크 로그를 싣던 ``common_ops_logship`` 은 이 fork 로 이관되지 않았고, ops 대시보드도
폐기됐다. 소비자가 없는 기록은 R2 에 영구 적재되며 태스크 시도마다 PUT 비용만 남긴다.

그래서 **소비자 없는 기록기만** 이 스위치로 잠근다(기본 off). 코드도 콜백 자리도 지우지
않는다 — ``run_sink`` 는 Iceberg 기반 ``record_run_metadata`` 를 대체하면서 콜백 자리를
일부러 남겨 둔 설계이고, ops 대시보드를 다시 세우면 환경변수 하나로 되살아나야 한다.

잠기는 것 (이 fork 에 소비자 없음)
    ``ops/runs``            ``run_sink.record_run`` (현재 이 fork 에는 연결된 콜백도 없음)
    ``ops/product-events``  ``product_observability.record_product_event``
    ``ops/product-health``  ``product_observability.record_product_health``
    ``ops/metrics``         ``runmetrics.MetricsR2Sink`` (dbt 노드별 실행 메트릭)
    ``common.ops.contract.emit_ops_event`` 도 같은 관문(``run_sink._put_r2``)을 지난다.
    control 계열을 이 관문으로 새로 흘리려면 스위치를 먼저 켜야 한다.

잠기지 않는 것 (읽는 쪽이 있다)
    ``ops/errors``      실패 상세 + Discord 알림 — 사람이 읽는 소비자가 살아 있다.
    ``ops/control/**``  랜딩 체크포인트·수집 슬롯 영수증 — 파이프라인이 되읽는 상태다
                        (규약 R-4: 상태 계열은 자동 삭제 금지).
    ``_reports``        bronze 수집 감사.
    ``ASAC_METRICS_DIR`` 로 고른 로컬 파일 sink — 명시적으로 켠 디버그 경로다.

되살리기
    ``ASAC_OPS_TELEMETRY_ENABLED=1`` (``true`` · ``yes`` · ``on`` 도 같음). 값은 호출마다
    읽으므로 스케줄러 재시작 없이 다음 태스크부터 적용된다.
"""

from __future__ import annotations

import os

#: 스위치 환경변수. 기존 ops sink 환경변수(``ASAC_ERRORS_PREFIX`` ·
#: ``ASAC_METRICS_PREFIX`` · ``ASAC_METRICS_DIR``)와 같은 접두어를 쓴다.
OPS_TELEMETRY_ENV = "ASAC_OPS_TELEMETRY_ENABLED"

#: 켜짐으로 인정하는 값 — 닫힌 집합. 오타를 '켜짐'으로 오해하지 않도록,
#: 여기 없는 값은 전부 꺼짐으로 본다(안전한 기본값 = off).
_TRUTHY = frozenset({"1", "true", "yes", "on"})


def ops_telemetry_enabled() -> bool:
    """소비자 없는 ops 관측 기록을 R2 에 쓸지 여부. 미설정이면 ``False``."""
    return os.environ.get(OPS_TELEMETRY_ENV, "").strip().lower() in _TRUTHY
