"""HTTP typed 예외 — 공통 에러 모듈(#77) Problem 으로 자동 변환되는 예외 (#78).

HttpCore 가 재시도 소진 후 던진다. ProblemError 를 상속하므로 태스크 실패 시
on_failure_callback(problem_failure_callback)이 휴리스틱 분류 대신 이 예외가
보유한 Problem(정확한 type·status·redacted request)을 그대로 적재한다.
"""
from __future__ import annotations

from typing import Any

from common.errors import types as error_types
from common.errors.problem import ProblemError
from common.security import redact


class HttpProblemError(ProblemError):
    """재시도 소진 후의 HTTP 오류. request 메타는 저장 전 redact 를 거친 상태."""

    def __init__(self, error_type: error_types.ErrorType, *, method: str, url: str,
                 status: int | None = None, detail: str | None = None,
                 attempts: int = 1, source_system: str | None = None) -> None:
        request: dict[str, Any] = redact({
            "method": method.upper(),
            "url": url,                 # PathKey 키가 박혀 있어도 redact 가 가린다
            "attempts": attempts,
        })
        super().__init__(error_type, redact(detail) if detail else None,
                         status=status, request=request, source_system=source_system)
        self.method = method.upper()
        self.status = status
        self.attempts = attempts


def classify_status(status: int) -> error_types.ErrorType:
    return error_types.HTTP_ERROR if status >= 400 else error_types.UNHANDLED
