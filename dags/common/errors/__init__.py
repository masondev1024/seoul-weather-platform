"""공통 에러 모듈 — RFC 9457 Problem Details + R2 적재 (#77).

- 포맷: RFC 9457 표준 멤버 + 확장 멤버, type 은 `urn:asac:error:<slug>` (problem.py / types.py)
- 저장: 날짜-우선 파티션 per-error JSON, 저장 직전 공통 redaction (sink.py)
- 수집: ① on_failure_callback 자동 (airflow.py) ② typed 예외 ProblemError (problem.py)
- 기록 범위는 에러 전용 — network/file I/O 이력·알림 연동은 별도 기획.

기획: docs/plans/2026-07-02-feat-common-error-module.md
"""
from common.errors.problem import Problem, ProblemError, build_instance_urn  # noqa: F401
from common.errors.sink import R2ErrorSink, build_object_key, write_problem  # noqa: F401
from common.errors.types import ErrorType, classify_exception, get, register  # noqa: F401
