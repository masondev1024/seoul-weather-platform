"""에러 유형(type) 레지스트리 — 슬러그·title·docs_url을 상수로 관리한다 (#77).

`type`은 영구 불변 식별자 `urn:asac:error:<slug>`로 저장한다(검색 키).
문서 위치는 별도 멤버 `docs_url`에 저장 시점 스냅샷으로 기록한다 —
식별(type)과 문서 위치(docs_url)를 한 칸에 섞지 않는다(#77 합의).
미등록 예외는 UNHANDLED(`urn:asac:error:unhandled`)로 수렴한다.
"""
from __future__ import annotations

from dataclasses import dataclass

URN_PREFIX = "urn:asac:error:"


@dataclass(frozen=True)
class ErrorType:
    slug: str
    title: str
    docs_url: str | None = None

    @property
    def urn(self) -> str:
        return URN_PREFIX + self.slug


UNHANDLED = ErrorType("unhandled", "Unhandled exception")
API_TIMEOUT = ErrorType("api-timeout", "External API request timed out")
CONNECTION_ERROR = ErrorType("connection-error", "External API connection failed")
HTTP_ERROR = ErrorType("http-error", "External API returned an HTTP error status")
# 수집은 성공했지만 pending 적재가 장기 running 상태로 멈춘 경우. 원천·게시 실패와
# 구분해 freshness 사고의 원인을 조회할 수 있도록 명시 유형으로 남긴다(#719).
LOADER_DELAY = ErrorType("loader-delay", "Pipeline loader exceeded its runtime SLO")

_REGISTRY: dict[str, ErrorType] = {}


def register(error_type: ErrorType) -> ErrorType:
    """유형 등록(도메인 공통 + 소스별). 같은 슬러그 재등록은 마지막 정의가 이긴다."""
    _REGISTRY[error_type.slug] = error_type
    return error_type


def get(slug: str) -> ErrorType:
    """슬러그로 조회. 미등록 슬러그는 UNHANDLED 로 수렴(오타가 새 유형을 만들지 않게)."""
    return _REGISTRY.get(slug, UNHANDLED)


def all_types() -> dict[str, ErrorType]:
    return dict(_REGISTRY)


for _t in (UNHANDLED, API_TIMEOUT, CONNECTION_ERROR, HTTP_ERROR, LOADER_DELAY):
    register(_t)


# 예외 클래스명 → 유형 매핑. requests/urllib 을 임포트하지 않고 MRO 클래스명으로
# 판별한다(라이브러리 미설치 환경·테스트에서도 동작, 공통 HTTP 클라이언트의
# typed 예외(ProblemError)가 들어오면 이 휴리스틱 대신 예외가 보유한 유형을 쓴다).
_TIMEOUT_CLASS_NAMES = {"TimeoutError", "Timeout", "ConnectTimeout", "ReadTimeout", "timeout"}
_CONNECTION_CLASS_NAMES = {"ConnectionError", "URLError", "ConnectionResetError",
                           "ConnectionRefusedError", "NewConnectionError"}
_HTTP_CLASS_NAMES = {"HTTPError"}


def classify_exception(exc: BaseException) -> ErrorType:
    names = {klass.__name__ for klass in type(exc).__mro__}
    if names & _TIMEOUT_CLASS_NAMES:
        return API_TIMEOUT
    if names & _HTTP_CLASS_NAMES:      # HTTPError 는 URLError 하위라 connection 보다 먼저 본다
        return HTTP_ERROR
    if names & _CONNECTION_CLASS_NAMES:
        return CONNECTION_ERROR
    return UNHANDLED
