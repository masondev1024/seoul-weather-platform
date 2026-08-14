"""전송 계약(Transport) — 테스트 대체용 얇은 구조적 계약 (#78).

HttpCore 는 이 Protocol 만 요구한다(합성). 기본 구현은 core.py 의
RequestsTransport(requests, #78 Q1 합의)이고, 단위 테스트는 가짜 Transport 를
주입한다 — 자바식 인터페이스 대신 typing.Protocol(구조적 타이핑)을 쓴다.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol


@dataclass
class TransportResponse:
    status: int
    content: bytes = b""
    headers: Mapping[str, str] = field(default_factory=dict)

    def header(self, name: str) -> str | None:
        for key, value in self.headers.items():
            if key.lower() == name.lower():
                return value
        return None

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")


class Transport(Protocol):
    def send(self, method: str, url: str, *, params: Mapping[str, str] | None,
             headers: Mapping[str, str] | None, timeout: float) -> TransportResponse: ...
