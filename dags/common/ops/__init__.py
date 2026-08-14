"""공용 운영(ops) 관측 — 저장소·운영 기록 규약(ASK-Seoul#78)의 **단일 관문**.

새로 기록을 남기는 코드는 :mod:`common.ops.contract` 만 쓴다. 경로를 직접 조립하거나
값 집합을 직접 정의하지 않는다 — 그러면 규약을 다시 읽지 않아도 지켜진다.

    from common.ops import OpsCategory, Grain, Layer, RowsSource, RunStatus, emit_ops_event

구성
    ``contract``  관문 — 닫힌 집합·필수 항목 검증·경로 빌더·기록 형식(F 표)
    ``d1_ops``    조회 DB 스키마 4종 + 자연키 upsert 문장(DROP 없음)
    ``ingest``    ops 존 감지 → 정규화 → 조회 DB 적재/대조
    ``run_sink``  (레거시) 태스크 실행 기록을 R2 runs/ 에 남기던 기록기
    ``product_observability``  (레거시) 제품 단계 전이·일별 상태 기록기
"""
from common.ops.contract import (  # noqa: F401
    BLOB_CATEGORIES,
    OBSERVATION_CATEGORIES,
    RECORD_FIELDS,
    RETENTION_DAYS,
    STATE_CATEGORIES,
    ControlSubtype,
    Environment,
    Grain,
    Layer,
    ManifestStatus,
    OpsCategory,
    OpsContractError,
    RowsSource,
    RunStatus,
    SinkType,
    build_ops_event,
    category_prefix,
    emit_ops_event,
    event_id_for,
    event_object_key,
    ops_key,
    resolve_environment,
)

__all__ = [
    "BLOB_CATEGORIES", "OBSERVATION_CATEGORIES", "RECORD_FIELDS", "RETENTION_DAYS",
    "STATE_CATEGORIES", "ControlSubtype", "Environment", "Grain", "Layer",
    "ManifestStatus", "OpsCategory",
    "OpsContractError", "RowsSource", "RunStatus", "SinkType", "build_ops_event",
    "category_prefix", "emit_ops_event", "event_id_for", "event_object_key", "ops_key",
    "resolve_environment",
]
