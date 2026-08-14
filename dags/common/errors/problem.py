"""RFC 9457 Problem Details 문서 + 예외 → Problem 변환 (#77).

표준 멤버(type/title/status/detail/instance)에 ASAC 확장 멤버를 더한다
(RFC 9457 §3.2 허용). 값이 없는 멤버는 직렬화에서 생략한다.
`detail`/`request`의 redaction 은 저장 직전 sink 가 일괄 적용한다.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from common.errors import types as error_types

SCHEMA_VERSION = "v1"


def build_instance_urn(dag_id: str | None, run_id: str | None,
                       task_id: str | None, try_number: int | None) -> str:
    """발생 지점 URI: urn:asac:run:<dag_id>:<run_id>/<task_id>:<try_number>"""
    return f"urn:asac:run:{dag_id or 'unknown'}:{run_id or 'unknown'}" \
           f"/{task_id or 'unknown'}:{try_number if try_number is not None else 0}"


@dataclass
class Problem:
    # ── RFC 9457 표준 멤버 ──
    type: str = error_types.UNHANDLED.urn
    title: str = error_types.UNHANDLED.title
    status: int | None = None
    detail: str | None = None
    instance: str | None = None
    # ── 확장 멤버 ──
    domain: str | None = None
    dag_id: str | None = None
    task_id: str | None = None
    run_id: str | None = None
    try_number: int | None = None
    source_system: str | None = None
    request: dict[str, Any] | None = None
    extensions: dict[str, Any] | None = None
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    docs_url: str | None = None
    schema_version: str = SCHEMA_VERSION

    @property
    def type_slug(self) -> str:
        if self.type.startswith(error_types.URN_PREFIX):
            return self.type[len(error_types.URN_PREFIX):]
        return error_types.UNHANDLED.slug

    def to_dict(self) -> dict[str, Any]:
        """직렬화용 dict — None 멤버는 생략, occurred_at 은 ISO-8601(UTC)."""
        out: dict[str, Any] = {"type": self.type, "title": self.title}
        for name in ("status", "detail", "instance", "domain", "dag_id", "task_id",
                     "run_id", "try_number", "source_system", "request", "docs_url"):
            value = getattr(self, name)
            if value is not None:
                out[name] = value
        for name, value in (self.extensions or {}).items():
            if name not in out:
                out[name] = value
        out["occurred_at"] = self.occurred_at.astimezone(timezone.utc).isoformat()
        out["schema_version"] = self.schema_version
        return out

    @classmethod
    def from_exception(cls, exc: BaseException | None, *, domain: str,
                       dag_id: str | None = None, task_id: str | None = None,
                       run_id: str | None = None, try_number: int | None = None,
                       source_system: str | None = None,
                       request: dict[str, Any] | None = None) -> "Problem":
        """예외 1건을 Problem 으로 변환. ProblemError 는 자신이 보유한 Problem 을 쓰되
        실행 맥락(domain/dag_id/…)은 호출측 값으로 채운다."""
        if isinstance(exc, ProblemError):
            problem = exc.problem
        else:
            error_type = error_types.classify_exception(exc) if exc else error_types.UNHANDLED
            problem = cls(
                type=error_type.urn,
                title=error_type.title,
                docs_url=error_type.docs_url,
                status=_status_from_exception(exc),
                detail=f"{type(exc).__name__}: {exc}" if exc else None,
            )
        problem.domain = domain
        problem.dag_id = dag_id
        problem.task_id = task_id
        problem.run_id = run_id
        problem.try_number = try_number
        if source_system is not None:
            problem.source_system = source_system
        if request is not None:
            problem.request = request
        problem.instance = build_instance_urn(dag_id, run_id, task_id, try_number)
        return problem


class ProblemError(Exception):
    """Problem 을 보유한 typed 예외 — 명시적 수집 경로 (#77 수집 경로 2).

    공통 HTTP 클라이언트 등 생산자가 재시도 끝에 이 예외를 던지면
    on_failure_callback 이 휴리스틱 분류 대신 보유한 Problem 을 그대로 적재한다.
    """

    def __init__(self, error_type: error_types.ErrorType, detail: str | None = None, *,
                 status: int | None = None, request: dict[str, Any] | None = None,
                 source_system: str | None = None) -> None:
        super().__init__(detail or error_type.title)
        self.problem = Problem(
            type=error_type.urn,
            title=error_type.title,
            docs_url=error_type.docs_url,
            status=status,
            detail=detail,
            request=request,
            source_system=source_system,
        )


def _status_from_exception(exc: BaseException | None) -> int | None:
    """HTTP 유래 오류면 상태코드 추출(urllib HTTPError.code / requests response.status_code)."""
    if exc is None:
        return None
    code = getattr(exc, "code", None)
    if isinstance(code, int):
        return code
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    return status if isinstance(status, int) else None
