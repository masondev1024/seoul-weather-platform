"""공통 시크릿 마스킹 — commerce `include/security/redaction.py` 승격 사본 (#77).

commerce 통합 보안 플러그인 중 redaction 엔진만 우선 승격한다(에러 모듈의
저장 직전 마스킹 의무 때문). 나머지 가드(netio/fileio/audit …)의 승격은
commerce `docs/security/adoption.md`의 "공용 폴더 단일 제공" 계약을 따라
별도 작업으로 진행한다. `redaction.py`는 원본과 내용 동일하게 유지해
diff/동기화가 가능하게 한다(수정 금지 — 고칠 것이 있으면 commerce 원본을
고치고 재복사).
"""
from common.security.redaction import (  # noqa: F401
    PLACEHOLDER,
    Redactor,
    collect_secret_values,
    get_default_redactor,
    redact,
    refresh_env_secrets,
    register_secret,
    sanitize_log_value,
    scrub_exception,
)
