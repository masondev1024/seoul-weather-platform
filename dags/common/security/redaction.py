"""시크릿 마스킹(redaction) 엔진 — 로그/예외/저장 페이로드 어디든 비밀값을 가린다.

목적: API 키·R2 자격증명·토큰이 **로그, 예외 메시지, 마커 JSON, 알림**으로 새지 않게 한다.
두 가지 방어를 결합한다(둘 다 적용 = defense in depth):

1. **literal redaction** — `os.environ` 의 시크릿(이름이 KEY/SECRET/TOKEN… 인 변수)의 *실제 값* 을
   수집해, 텍스트 어디에 나오든 치환한다. 가장 정확(키가 URL 경로에 박혀도 잡는다).
2. **structural redaction** — 실제 값을 몰라도 형태로 잡는 패턴(서울 OpenAPI URL 경로 키,
   `Authorization`/`Bearer`, AWS 액세스 키, `secret=`/`token=` 류 쿼리/할당). literal 이
   비어 있는 단위테스트나 값이 갱신되기 전 상황에서도 동작.

stdlib 만 사용 — 어느 Airflow 번들에도 그대로 이식 가능(외부 의존성 없음).
"""
from __future__ import annotations

import os
import re
from typing import Any, Iterable, Mapping

PLACEHOLDER = "***REDACTED***"

# 값이 짧으면(공통 토큰 'auto','v1','local' 등) 오탐 위험 → 이 길이 미만은 literal 로 안 가린다.
_MIN_LITERAL_LEN = 6

# 시크릿으로 간주할 환경변수 *이름* 패턴(값이 아니라 이름 기준).
_SECRET_NAME_RE = re.compile(
    r"(?i)(KEY|SECRET|TOKEN|PASSWORD|PASSWD|PWD|CREDENTIAL|PRIVATE|"
    r"ACCESS[_-]?KEY|AUTH|SESSION|SIGNATURE|COOKIE)"
)
# 이름이 위에 걸려도 시크릿이 아닌 것(엔드포인트/식별자 URL 등)은 제외.
_SECRET_NAME_DENY = re.compile(r"(?i)(BASE_URL|ENDPOINT|_URL$|_URI$|_PATH$|_FILE$)")

# 미해석 참조/명백한 placeholder 는 시크릿 값으로 수집하지 않는다.
_PLACEHOLDER_VALUE_RE = re.compile(
    r"(?i)^(\$\{.*\}|<.*>|your[_-]|changeme|example|xxx+|placeholder|none|null|true|false)$"
)

# ── structural 패턴(실제 값 없이도 형태로 마스킹) ────────────────────────────────
_STRUCTURAL_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # 서울 OpenAPI 는 인증키를 URL 경로 첫 세그먼트에 박는다:
    #   http://openapi.seoul.go.kr:8088/<KEY>/json/<SERVICE>/<start>/<end>/
    (re.compile(r"(?i)(https?://openapi\.seoul\.go\.kr(?::\d+)?/)([^/\s?#]+)(/)"),
     r"\1" + PLACEHOLDER + r"\3"),
    # requests 예외는 host 없이 경로만 찍기도 한다: "url: /<KEY>/json/<SVC>/…".
    # `/json/`·`/xml/` 직전 세그먼트(=서울 인증키)를 가린다(우리 저장경로엔 /json/ 가 없다).
    (re.compile(r"(?i)([/\s\"'=:])([A-Za-z0-9%._\-]{6,})(/(?:json|xml)/)"),
     r"\1" + PLACEHOLDER + r"\3"),
    # Authorization 헤더 / Bearer 토큰
    (re.compile(r"(?i)(authorization\s*[:=]\s*)(?:bearer\s+)?\S+"),
     r"\1" + PLACEHOLDER),
    # AWS 스타일 액세스 키 ID
    (re.compile(r"\bAKIA[0-9A-Z]{16}\b"), PLACEHOLDER),
    # URL userinfo 자격증명(scheme://<userinfo>@host)의 userinfo 전체 → 통째 마스킹.
    # `user:pass` 뿐 아니라 **토큰 단독**(user 없는 https://TOKEN@host)도 가린다.
    (re.compile(r"(?<=://)[^/@\s]{1,256}(?=@)"), PLACEHOLDER),
    # 이름있는 시크릿 할당/쿼리: secret=…, token=…, api_key=…, access_key_id=…, password=…,
    # serviceKey=…(공공데이터포털/KMA 쿼리 키 — #78 리뷰에서 누락 발견)
    # 선행 \b 를 두지 않는다 → aws_secret_access_key 처럼 _ 로 이어붙은 이름도 잡는다.
    # (이름 직후의 =/: 앵커가 secretary= 같은 부분일치 오탐을 막는다.)
    (re.compile(
        r"(?i)((?:service[_-]?key|api[_-]?key|access[_-]?key[_-]?id|secret[_-]?access[_-]?key|"
        r"access[_-]?key|credential|signature|password|passwd|secret|token|pwd)"
        r"\s*[=:]\s*[\"']?)([^\s\"'&;]+)"),
     r"\1" + PLACEHOLDER),
]


def collect_secret_values(environ: Mapping[str, str] | None = None,
                          *, min_len: int = _MIN_LITERAL_LEN) -> set[str]:
    """환경변수에서 *시크릿 값* 집합을 뽑는다(이름이 시크릿 패턴인 변수의 비어있지 않은 값)."""
    env = os.environ if environ is None else environ
    out: set[str] = set()
    for name, value in env.items():
        if not value or len(value) < min_len:
            continue
        if not _SECRET_NAME_RE.search(name) or _SECRET_NAME_DENY.search(name):
            continue
        if _PLACEHOLDER_VALUE_RE.match(value.strip()):
            continue
        out.add(value)
    return out


class Redactor:
    """등록된 시크릿 literal + structural 패턴으로 텍스트/구조체를 마스킹."""

    def __init__(self, literals: Iterable[str] = (), *,
                 patterns: list[tuple[re.Pattern[str], str]] | None = None,
                 placeholder: str = PLACEHOLDER, min_literal_len: int = _MIN_LITERAL_LEN) -> None:
        self.placeholder = placeholder
        self._min = min_literal_len
        self._patterns = patterns if patterns is not None else _STRUCTURAL_PATTERNS
        self._literals: list[str] = []
        for lit in literals:
            self.add_secret(lit)

    def add_secret(self, value: str | None) -> None:
        """가릴 비밀값 1개 등록(짧은 값은 무시). 긴 것부터 치환하도록 정렬 유지."""
        if not value or len(value) < self._min or value in self._literals:
            return
        self._literals.append(value)
        self._literals.sort(key=len, reverse=True)

    def load_env_secrets(self, environ: Mapping[str, str] | None = None) -> int:
        """환경변수의 시크릿 값을 등록. 등록한 개수 반환(값 자체는 로그/반환하지 않음)."""
        before = len(self._literals)
        for value in collect_secret_values(environ, min_len=self._min):
            self.add_secret(value)
        return len(self._literals) - before

    def redact_text(self, text: str) -> str:
        if not text:
            return text
        out = text
        for lit in self._literals:          # literal 먼저(가장 정확)
            if lit in out:
                out = out.replace(lit, self.placeholder)
        for rx, repl in self._patterns:     # 형태 기반 보강
            out = rx.sub(repl, out)
        return out

    def redact(self, obj: Any) -> Any:
        """문자열/딕셔너리/리스트/튜플 재귀 마스킹(예외 객체는 메시지 문자열로)."""
        if isinstance(obj, str):
            return self.redact_text(obj)
        if isinstance(obj, Mapping):
            # 키도 마스킹한다 — 시크릿이 값이 아니라 **키**로 들어오는 경우(예: 토큰으로
            # 인덱싱된 dict) 누출 방지. literal 마스킹은 등록된 시크릿을 포함한 키만 바꾸므로
            # 일반 필드명은 그대로 남는다(구조 파손 없음).
            return {self.redact_text(k) if isinstance(k, str) else k: self.redact(v)
                    for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return type(obj)(self.redact(v) for v in obj)
        if isinstance(obj, BaseException):
            return self.redact_text(f"{type(obj).__name__}: {obj}")
        return obj

    def has_literals(self) -> bool:
        return bool(self._literals)


# 로그 인젝션(CWE-117) 무력화 — CR/LF/탭은 가시 이스케이프로, 나머지 제어문자(C0/C1/DEL,
# ANSI ESC 포함)와 U+2028/U+2029(라인 분리자)는 \uXXXX 로 치환한다.
_CTRL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f-\x9f\u2028\u2029]")


def sanitize_log_value(value: object, *, max_len: int = 2000) -> str:
    """외부 입력을 **자유형 로그**에 넣기 전 제어문자 무력화(위조 로그라인·ANSI 공격 차단).

    개행/CR/탭 → 리터럴 이스케이프(`\\n`/`\\r`/`\\t`), 그 외 제어문자 → `\\uXXXX`,
    max_len 초과분은 `…(+N)` 로 절단. events.py 의 JSON 경로는 json.dumps 가 이미
    무력화하므로 불필요 — 이 헬퍼는 `log.info("user=%s", user)` 류 직접 로깅용이다.
    """
    text = value if isinstance(value, str) else str(value)
    text = (text.replace("\\", "\\\\").replace("\r", "\\r")
                .replace("\n", "\\n").replace("\t", "\\t"))
    text = _CTRL_RE.sub(lambda m: f"\\u{ord(m.group(0)):04x}", text)
    if len(text) > max_len:
        text = text[:max_len] + f"…(+{len(text) - max_len})"
    return text


def scrub_exception(exc: BaseException, *, redactor: "Redactor | None" = None,
                    max_depth: int = 8) -> BaseException:
    """예외 **객체 자체**의 args 를 체인(__cause__/__context__)까지 따라가며 마스킹.

    타입을 바꾸지 않고 같은 예외를 그대로 다시 던질 수 있게 한다(호출측 except 절 보존).
    requests 예외처럼 메시지에 키 박힌 URL 이 들어가는 경우, 이 예외가 어디로 전파되든
    (로그·마커·알림·상위 재포장) str(exc) 에 시크릿이 남지 않는다.
    """
    red = redactor or _default
    seen: set[int] = set()
    cur: BaseException | None = exc
    depth = 0
    while cur is not None and id(cur) not in seen and depth < max_depth:
        seen.add(id(cur))
        depth += 1
        try:
            cur.args = tuple(
                red.redact(a) if isinstance(a, (str, dict, list, tuple)) else a
                for a in cur.args)
        except Exception:   # args 재할당 불가 예외(슬롯 등)는 건너뜀 — 로그 필터가 2차 방어
            pass
        cur = cur.__cause__ or cur.__context__
    return exc


# ── 모듈 전역 기본 redactor + 편의 함수 ─────────────────────────────────────────
_default = Redactor()


def get_default_redactor() -> Redactor:
    return _default


def redact(obj: Any) -> Any:
    """기본 redactor 로 마스킹. clients/notify/bronze 등 호출측에서 사용."""
    return _default.redact(obj)


def register_secret(value: str | None) -> None:
    _default.add_secret(value)


def refresh_env_secrets(environ: Mapping[str, str] | None = None) -> int:
    """현재 환경변수의 시크릿을 기본 redactor 에 (재)등록. install 시 호출."""
    return _default.load_env_secrets(environ)
