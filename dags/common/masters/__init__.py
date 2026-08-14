"""공통 마스터 수집 모듈 (#154) — 도메인 공용 마스터 데이터 적재의 순수 로직.

admin_dong: 공공데이터포털 ODcloud '행정동/법정동 연계 정보'(MODS) — 개정일자별
전국 전체 스냅샷 누적본에서 **최신 개정일자 스냅샷만** 수집한다. HTTP 는 공통
HttpCore(#78)로, R2 랜딩은 common.storage(#109)로 위임한다.
"""
from common.masters.admin_dong import (  # noqa: F401
    DATASET,
    ODCLOUD_URL,
    SOURCE_SYSTEM,
    build_core,
    fetch_page,
    iter_snapshot,
    land_snapshot,
    latest_revision,
    load_service_key,
)
