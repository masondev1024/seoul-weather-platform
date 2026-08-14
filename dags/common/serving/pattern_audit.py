# common/serving/pattern_audit.py — 패턴 SQL 정적 감사 (도메인 무관, 공유 게시기용)
#
# Serving#217 이행: 공유 게시기(publisher.py)를 쓰는 모든 도메인이 패턴 게시 시점에 같은
# 감사를 받는다(킷 §F "공유 경로 한 곳에 배선 → 4개 도메인 동시 강제"의 실물).
#
# 강제 수위는 #217 결정의 단계와 맞춘다:
#   - deny_findings(강제·게시 제외): 구조 위반 + 내부표(`_`·`sqlite_` 접두)·`pragma_*`·
#     `d1_migrations` 참조 — 게이트웨이 P0-a 와 같은 거부 목록. 게시자의 실수를 막는다.
#   - allowlist 밖(경보만): audit_pattern_sql 의 나머지 발견은 위반 제외 없이 경보한다 —
#     P0-b(허용 목록 강제)는 게시 계약·오너십 합의와 함께 2차다. 지금 강제하면 서브셋
#     게시 배치(한 DAG 이 도메인 제품 일부만 게시)에서 형제 표 참조 패턴을 오차단해
#     잘 돌던 패턴이 카탈로그에서 사라질 수 있다.
#
# 테이블 추출은 **프레임 스택 워커**다 — 킷 초판(`docs/domain-conversion-kit/pattern_audit.py`)
# 의 선형 스캐너는 괄호 친 테이블(`FROM (_keys)`)을 서브쿼리로 오인해 놓쳤고(ASK-Seoul-Serving
# prep 레드팀 확증), 전역 CTE 합집합은 서브쿼리-로컬 `WITH _keys AS(…)` 가림에 뚫렸다.
# 이 판은 게이트웨이 `src/pattern-audit.js`(적대적 3라운드 통과본)와 같은 로직이다.
# 파싱이 애매하면 과다 캡처(=거부) 쪽으로 fail-closed 한다.

from __future__ import annotations

import re
from typing import Iterable

_TOKEN_RE = re.compile(
    r"(?P<ws>\s+)"
    r"|(?P<line_comment>--[^\n]*)"
    r"|(?P<block_comment>/\*.*?\*/)"
    r"|(?P<string>'(?:[^']|'')*')"
    r'|(?P<dquote>"(?:[^"]|"")*")'
    r"|(?P<bquote>`(?:[^`]|``)*`)"
    r"|(?P<bracket>\[[^\]]*\])"
    r"|(?P<param>:[A-Za-z_][A-Za-z0-9_]*)"
    r"|(?P<number>\d+\.?\d*)"
    r"|(?P<ident>[A-Za-z_][A-Za-z0-9_]*)"
    r"|(?P<punct>[(),.;?])"
    r"|(?P<other>\S)",
    re.S,
)

ALLOWED_TABLE_FUNCS = frozenset({"json_each"})   # 배열 IN 관용구만 — pragma_* TVF 는 거부 계열
# 문장 위치에서만 위험한 쓰기/DDL 동사 — '아무 데나 토큰' 검사는 `SELECT x AS replace`·
# `REPLACE()` 스칼라 함수를 오탐한다(게이트웨이 레드팀 확증). statement_verb 자리만 본다.
WRITE_VERBS = frozenset({"insert", "update", "delete", "replace", "drop", "alter", "create",
                         "attach", "detach", "vacuum", "reindex", "analyze", "pragma"})
DANGEROUS_FN = frozenset({"load_extension"})
_INTERNAL_RE = re.compile(r"(^|\.)(_|sqlite_)", re.I)
_PRAGMA_RE = re.compile(r"^pragma_", re.I)
# 접두 규칙(`_`·`sqlite_`)이 못 잡는 게이트웨이 자산 — wrangler 마이그레이션 원장.
DENY_TABLES = frozenset({"d1_migrations"})
_BOUNDARY_KW = frozenset({"where", "group", "order", "having", "limit", "window", "union",
                          "except", "intersect", "returning", "values"})
_JOINCOND_KW = frozenset({"on", "using"})
_JOINMOD_KW = frozenset({"cross", "inner", "left", "right", "full", "outer", "natural"})
_NAME_KINDS = frozenset({"ident", "dquote", "bquote", "bracket"})
_LIMIT_PARAM_RE = re.compile(r"^(n|limit|top_n)$", re.I)


def build_allowlist(product_tables: Iterable[str],
                    cross_domain_sources: Iterable[str] = ()) -> frozenset[str]:
    """도메인 allowlist — 게시 제품 테이블(모델명=D1 테이블명) ∪ 명시 선언 크로스도메인 소스.
    게이트웨이 내부표·카탈로그/핸드오프 표는 여기 없으므로 allowlist 검사에서 자동 외부다."""
    return frozenset(str(t).lower() for t in (*product_tables, *cross_domain_sources))


def _strip_ident(v: str) -> str:
    return v.strip('"`[]')


def tokenize(sql: str) -> list[tuple[str, str]]:
    return [(m.lastgroup, m.group()) for m in _TOKEN_RE.finditer(sql or "")
            if m.lastgroup not in ("ws", "line_comment", "block_comment")]


def rewrite_audited_relation(
    sql: str, active_model: str, staging_model: str, allowed_tables: frozenset[str]
) -> str | None:
    """Rewrite exact lexical FROM/JOIN references for candidate-only verification.

    The existing full audit remains the authority for relation allowlisting.  This
    deliberately accepts only an identifier immediately after FROM/JOIN; anything
    more complex stays unverified instead of accidentally querying active data.
    """
    if audit_pattern_sql(sql, allowed_tables):
        return None
    tokens = [
        (m.lastgroup, m.group(), m.start(), m.end()) for m in _TOKEN_RE.finditer(sql or "")
        if m.lastgroup not in ("ws", "line_comment", "block_comment")
    ]
    replacements: list[tuple[int, int, str]] = []
    active = active_model.lower()
    for index, (kind, value, _start, _end) in enumerate(tokens[:-1]):
        if kind != "ident" or value.lower() not in {"from", "join"}:
            continue
        next_kind, next_value, start, end = tokens[index + 1]
        if next_kind not in _NAME_KINDS:
            return None
        if _strip_ident(next_value).lower() == active:
            quote = next_value[:1] if next_kind in {"dquote", "bquote", "bracket"} else ""
            closing = {"dquote": '"', "bquote": "`", "bracket": "]"}.get(next_kind, "")
            replacements.append((start, end, f"{quote}{staging_model}{closing}"))
    if not replacements:
        return None
    out = sql
    for start, end, value in reversed(replacements):
        out = out[:start] + value + out[end:]
    return out


def _skip_parens(toks: list[tuple[str, str]], j: int) -> int:
    depth = 0
    n = len(toks)
    while j < n:
        if toks[j] == ("punct", "("):
            depth += 1
        elif toks[j] == ("punct", ")"):
            depth -= 1
            if depth == 0:
                return j + 1
        j += 1
    return j


def _with_ctes_at(toks: list[tuple[str, str]], i: int) -> tuple[set[str], int]:
    """toks[i]=='with' 인 단일 WITH 절의 CTE 이름 집합과 절 끝 인덱스."""
    names: set[str] = set()
    n = len(toks)
    j = i + 1
    if j < n and toks[j][0] == "ident" and toks[j][1].lower() == "recursive":
        j += 1
    while j < n:
        if toks[j][0] not in _NAME_KINDS:
            break
        name = _strip_ident(toks[j][1]).lower()
        j += 1
        if j < n and toks[j] == ("punct", "("):           # 선택 컬럼목록
            j = _skip_parens(toks, j)
        if not (j < n and toks[j][0] == "ident" and toks[j][1].lower() == "as"):
            break
        j += 1
        if j < n and toks[j][0] == "ident" and toks[j][1].lower() == "not":
            j += 1
        if j < n and toks[j][0] == "ident" and toks[j][1].lower() == "materialized":
            j += 1
        if not (j < n and toks[j] == ("punct", "(")):
            break
        names.add(name)
        j = _skip_parens(toks, j)
        if j < n and toks[j] == ("punct", ","):
            j += 1
            continue
        break
    return names, j


def statement_verb(toks: list[tuple[str, str]]) -> str | None:
    """SELECT/WITH 단일문에서 실제 쓰기가 될 수 있는 유일한 자리의 동사 —
    `WITH <ctes> <VERB>` 의 VERB(없으면 첫 토큰). 현행 `^(select|with)` 정규식이 못 잡는
    `WITH x AS(…) DELETE …` 를 잡는다."""
    if not toks:
        return None
    if not (toks[0][0] == "ident" and toks[0][1].lower() == "with"):
        return toks[0][1].lower() if toks[0][0] == "ident" else None
    _, end = _with_ctes_at(toks, 0)
    if end < len(toks) and toks[end][0] == "ident":
        return toks[end][1].lower()
    return None


def _walk_table_refs(toks: list[tuple[str, str]], on_ref) -> None:
    """FROM/JOIN 테이블 자리 이름마다 on_ref(name, cte_visible) 호출 — 프레임 스택 워커.

    괄호 친 테이블 리스트(`FROM (a, _keys)`)는 재귀 프레임으로 걷고(`(` 다음이
    SELECT/WITH/VALUES 면 서브쿼리=불투명), WITH 는 **현재 프레임**에만 CTE 를 등록해
    서브쿼리-로컬 CTE 가 바깥 이름을 못 가리게 한다(레드팀 ①·② 수정 반영)."""
    n = len(toks)
    stack: list[dict] = [{"list": False, "ctes": set()}]
    expect = False
    i = 0
    while i < n:
        kind, val = toks[i]
        low = val.lower() if kind == "ident" else None
        if kind == "ident" and low == "with":
            names, _end = _with_ctes_at(toks, i)
            stack[-1]["ctes"] |= names
            i += 1
            continue
        if kind == "punct" and val == "(":
            if expect:
                nxt = toks[i + 1] if i + 1 < n else None
                is_sub = bool(nxt and nxt[0] == "ident"
                              and nxt[1].lower() in ("select", "with", "values"))
                stack.append({"list": not is_sub, "ctes": set()})
                expect = not is_sub
            else:
                stack.append({"list": False, "ctes": set()})
            i += 1
            continue
        if kind == "punct" and val == ")":
            if len(stack) > 1:
                stack.pop()
            expect = False
            i += 1
            continue
        if kind == "ident" and (low == "from" or low == "join"):
            stack[-1]["list"] = True
            expect = True
            i += 1
            continue
        if kind == "punct" and val == ",":
            if stack[-1]["list"]:
                expect = True
            i += 1
            continue
        if kind == "ident" and low in _BOUNDARY_KW:
            stack[-1]["list"] = False
            expect = False
            i += 1
            continue
        if kind == "ident" and low in _JOINCOND_KW:      # ON/USING — 리스트는 유지(뒤 콤마는 테이블)
            expect = False
            i += 1
            continue
        if kind == "ident" and low in _JOINMOD_KW:
            i += 1
            continue
        if kind == "ident" and low == "as":
            expect = False
            i += 1
            continue
        if expect and kind in _NAME_KINDS:
            name = _strip_ident(val).lower()
            j = i + 1
            while (j + 1 < n and toks[j] == ("punct", ".") and toks[j + 1][0] in _NAME_KINDS):
                name += "." + _strip_ident(toks[j + 1][1]).lower()
                j += 2
            visible = any(name in frame["ctes"] for frame in stack)
            on_ref(name, visible)
            expect = False
            i = j
            continue
        i += 1


def table_refs(toks: list[tuple[str, str]]) -> list[str]:
    refs: list[str] = []
    _walk_table_refs(toks, lambda name, _v: refs.append(name))
    return refs


def _is_denied(name: str) -> bool:
    if _INTERNAL_RE.search(name):
        return True
    return any(seg in DENY_TABLES or _PRAGMA_RE.match(seg)
               for seg in (name, *name.split(".")))


def _structural_findings(toks: list[tuple[str, str]]) -> list[str]:
    findings: list[str] = []
    if not toks or not (toks[0][0] == "ident" and toks[0][1].lower() in ("select", "with")):
        findings.append("SELECT/WITH 로 시작하지 않음(읽기 전용 아님)")
    semis = [k for k in range(len(toks)) if toks[k] == ("punct", ";")]
    if semis and not (len(semis) == 1 and semis[0] == len(toks) - 1):
        findings.append("세미콜론 내부 등장 — 복수 문장(스택 쿼리) 의심")
    verb = statement_verb(toks)
    if verb and verb in WRITE_VERBS:
        findings.append(f"쓰기/DDL 문 '{verb}' — 읽기 전용(SELECT/WITH) 아님")
    for kind, val in toks:
        if kind == "ident" and val.lower() in DANGEROUS_FN:
            findings.append(f"금지 함수 '{val.lower()}'")
            break
    return findings


def deny_findings(sql: str) -> list[str]:
    """P0-a(강제) — 구조 위반 + 내부/거부 표 참조. 거부 이름은 CTE 로도 못 가린다(fail-closed).
    형제·타 도메인 서빙표는 여기서 판정하지 않는다 — 그건 allowlist(P0-b·경보)의 몫."""
    toks = tokenize(sql)
    findings = _structural_findings(toks)
    hit: set[str] = set()

    def on_ref(name: str, _visible: bool) -> None:
        if name in ALLOWED_TABLE_FUNCS:
            return
        if _is_denied(name):
            hit.add(name)

    _walk_table_refs(toks, on_ref)
    if hit:
        findings.append(f"게이트웨이 내부 표 참조: {sorted(hit)} — 패턴 SQL 이 읽을 수 없는 표")
    return findings


def audit_pattern_sql(sql: str, allowed_tables: frozenset[str]) -> list[str]:
    """전체 감사(P0-b 포함) — 구조 + 내부표 + allowlist 밖 + LIMIT 이름 규약.
    공유 게시기는 이 중 deny 계열만 강제하고 allowlist 밖은 경보한다(모듈 머리 주석)."""
    toks = tokenize(sql)
    findings = _structural_findings(toks)
    allowed = frozenset(str(t).lower() for t in allowed_tables)
    external: set[str] = set()
    denied: set[str] = set()

    def on_ref(name: str, visible: bool) -> None:
        if name in allowed or name in ALLOWED_TABLE_FUNCS:
            return
        if _is_denied(name):
            denied.add(name)                 # 내부/거부는 CTE 로도 못 가린다
        elif not visible:
            external.add(name)

    _walk_table_refs(toks, on_ref)
    if denied:
        findings.append(f"게이트웨이 내부 표 참조: {sorted(denied)} — 패턴 SQL 이 읽을 수 없는 표")
    if external:
        findings.append(f"allowlist 밖 테이블 참조: {sorted(external)} "
                        f"(이 도메인 게시 제품 + 선언 크로스도메인 소스만 허용)")
    for k in range(len(toks) - 1):
        if toks[k][0] == "ident" and toks[k][1].lower() == "limit" and toks[k + 1][0] == "param":
            pname = toks[k + 1][1][1:]
            if not _LIMIT_PARAM_RE.match(pname):
                findings.append(f"LIMIT :{pname} — 행수 파라미터는 n/limit/top_n 이름만 상한이 걸린다")
    return findings


def audit_patterns(patterns: list[dict], allowed_tables: frozenset[str]) -> dict[str, list[str]]:
    """(pattern_id → 위반 리스트). 위반 없는 패턴은 결과에 넣지 않는다(킷 API 호환)."""
    out: dict[str, list[str]] = {}
    for p in patterns or []:
        f = audit_pattern_sql(p.get("sql") or "", allowed_tables)
        if f:
            out[str(p.get("pattern_id"))] = f
    return out
