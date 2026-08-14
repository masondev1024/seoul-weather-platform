"""키 주입 전략 — 소스별 인증 방식을 HttpCore 밖에서 조립한다 (#78).

- QueryKey  : ?serviceKey=... (공공데이터포털/KMA 식)
- PathKey   : URL 경로 세그먼트에 키 (서울 열린데이터광장 식) — url 템플릿의
              `{api_key}` 자리에 치환한다(경로 조립 규칙을 전략이 소유).
- HeaderKey : Authorization 등 헤더 주입

키 값은 env 에서만 로드해 넘긴다(#70 이름 규칙) — 전략 객체는 값을 보관만
하고 절대 로그에 남기지 않는다. redaction 은 HttpCore 가 일괄 보장한다.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping
from urllib.parse import quote

API_KEY_PLACEHOLDER = "{api_key}"


@dataclass(frozen=True)
class AuthResult:
    url: str
    params: dict[str, str]
    headers: dict[str, str]


class NoAuth:
    """인증 없음 — 자리표시자가 남아 있으면 조립 오류로 즉시 실패시킨다."""

    def apply(self, url: str, params: Mapping[str, str] | None,
              headers: Mapping[str, str] | None) -> AuthResult:
        if API_KEY_PLACEHOLDER in url:
            raise ValueError("url 에 {api_key} 자리표시자가 있는데 auth 전략이 없음")
        return AuthResult(url, dict(params or {}), dict(headers or {}))


@dataclass(frozen=True)
class QueryKey:
    name: str
    key: str

    def apply(self, url: str, params: Mapping[str, str] | None,
              headers: Mapping[str, str] | None) -> AuthResult:
        merged = dict(params or {})
        merged[self.name] = self.key
        return AuthResult(url, merged, dict(headers or {}))


@dataclass(frozen=True)
class PathKey:
    key: str
    encode: bool = True     # 공공데이터포털 Decoding 키(`/`·`==` 포함) 대응

    def apply(self, url: str, params: Mapping[str, str] | None,
              headers: Mapping[str, str] | None) -> AuthResult:
        if API_KEY_PLACEHOLDER not in url:
            raise ValueError("PathKey 는 url 템플릿에 {api_key} 자리표시자가 필요")
        value = quote(self.key, safe="") if self.encode else self.key
        return AuthResult(url.replace(API_KEY_PLACEHOLDER, value),
                          dict(params or {}), dict(headers or {}))


@dataclass(frozen=True)
class HeaderKey:
    name: str
    key: str
    scheme: str | None = None   # 예: "Bearer" → "Bearer <key>"

    def apply(self, url: str, params: Mapping[str, str] | None,
              headers: Mapping[str, str] | None) -> AuthResult:
        merged = dict(headers or {})
        merged[self.name] = f"{self.scheme} {self.key}" if self.scheme else self.key
        return AuthResult(url, dict(params or {}), merged)
