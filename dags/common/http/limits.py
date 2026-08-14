"""소스별 rate limit — 3단 계층: 코드 안전 기본값 < config 파일 < 호출측 override (#78 Q4 합의).

- 코드 기본값: 값이 없어도 깨지지 않는 안전선(아래 _CODE_DEFAULTS).
- config 파일: `dags/common/config/http_limits.yaml` (env `ASAC_HTTP_LIMITS_FILE` 로 교체)
  — 운영이 재배포 없이 튜닝. 파일/키가 없으면 조용히 코드 기본값으로.
- 호출측 override: HttpCore(rate_limit=...) 또는 어댑터 생성 인자 — 항상 최우선.

값 의미: **초당 최대 호출 수(requests/sec), 양수만 유효**. None = 제한 없음.
0/음수는 미지원 — config 파일 값이면 경고 후 무시(코드 기본값으로 후퇴),
호출측 인자면 HttpCore 가 ValueError 를 던진다("0 = 차단" 기대의 반전 사고 방지).
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

LOGGER = logging.getLogger(__name__)

_LIMITS_FILE_ENV = "ASAC_HTTP_LIMITS_FILE"
_DEFAULT_LIMITS_FILE = Path(__file__).resolve().parents[1] / "config" / "http_limits.yaml"

# 코드 안전 기본값 — 서울 열린데이터광장 일반 API 는 호출 횟수 제한이 관대하지만
# 예의상 초당 5회로 캡. 소스별 실측/키 등급은 config 파일에서 오버라이드한다.
_CODE_DEFAULTS: dict[str, float | None] = {
    "seoul_openapi": 5.0,
    "default": None,
}


def _load_file_limits() -> dict[str, float | None]:
    path = Path(os.environ.get(_LIMITS_FILE_ENV, str(_DEFAULT_LIMITS_FILE)))
    if not path.exists():
        return {}
    try:
        import yaml
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        limits: dict[str, float | None] = {}
        for key, value in data.items():
            parsed = float(value) if value is not None else None
            if parsed is not None and parsed <= 0:
                LOGGER.warning("http_limits[%s]=%s 무시 — 양수(req/s) 또는 null 만 유효", key, value)
                continue
            limits[str(key)] = parsed
        return limits
    except Exception as exc:    # config 파손이 파이프라인을 멈추지 않게 — 기본값으로 후퇴
        LOGGER.warning("http_limits config 로드 실패(코드 기본값 사용): %s", type(exc).__name__)
        return {}


def resolve_rate_limit(source: str, override: float | None = ...) -> float | None:
    """source 의 rate limit 해석. override 는 명시 시(None 포함) 최우선."""
    if override is not ...:
        return override
    file_limits = _load_file_limits()
    if source in file_limits:
        return file_limits[source]
    if source in _CODE_DEFAULTS:
        return _CODE_DEFAULTS[source]
    return file_limits.get("default", _CODE_DEFAULTS["default"])
