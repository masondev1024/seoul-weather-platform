"""공통 HTTP 클라이언트 — 소스 API 호출의 단일 통로 (#78).

합성(composition) 중심 + 얇은 계약:
- `HttpCore` — 구체 클래스. timeout 강제·재시도(backoff+jitter, Retry-After)·
  rate limit(3단 계층)·redaction 로깅. 상속하지 말고 주입받아 쓸 것(has-a).
- `auth` — 키 주입 전략(QueryKey/PathKey/HeaderKey). 키는 env 에서만 로드.
- `HttpProblemError` — 재시도 소진 시 typed 예외, 공통 에러 모듈(#77) Problem 보유.
- `contract.Transport` — 테스트 대체용 Protocol.
- `SeoulOpenApiClient` — 서울 열린데이터광장 공용 어댑터(5개 도메인 공유).

기획: docs/plans/2026-07-02-feat-common-http-client.md
"""
from common.http.auth import HeaderKey, NoAuth, PathKey, QueryKey  # noqa: F401
from common.http.contract import Transport, TransportResponse  # noqa: F401
from common.http.core import DEFAULT_TIMEOUT, OK_2XX, HttpCore, RequestsTransport  # noqa: F401
from common.http.errors import HttpProblemError  # noqa: F401
from common.http.limits import resolve_rate_limit  # noqa: F401
from common.http.seoul import SeoulOpenApiClient  # noqa: F401
