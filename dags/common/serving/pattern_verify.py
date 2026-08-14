# common/serving/pattern_verify.py — export 시점 패턴 검증 스탬프 (Serving#217 후속)
#
# 문제: usage_patterns 를 저작해 게시하면 게이트웨이가 `verified_at` 없는 패턴을 runnable=false
#   로 막아 실행 시 409 를 준다("가져올 수 없는 항목"). verified_at 은 지금까지 별도 스크립트
#   (scripts/verify_usage_patterns.py --apply)가 dbt yml 에 손으로 찍는 단계였고, 저작 후 그 단계를
#   안 돌리면 초안이 계속 미검증으로 남았다.
#
# 조치: **각 도메인 gold→D1 export 라인**에서, 방금 게시한 D1 데이터에 패턴 SQL 을 실제로 돌려
#   통과한 패턴에 verified_at/verified_rows/verified_publication_id 를 **D1 에 직접 스탬프**한다.
#   - 미검증 패턴만 대상(yml 에 verified_at 이 이미 있으면 무접촉 — 손 검증 존중).
#   - 각 도메인 export 는 자기 제품만 다루므로 도메인 간 충돌이 없다(다중 컴퓨터 안전).
#   - 예시값(SQL 주석 `-- :n=10`)으로 바인딩해 실행. 예시 미해결·비-SELECT·실행 실패·0행(단
#     allow_empty 아님)이면 스탬프하지 않고 미검증으로 남긴다(안전망 유지).
#
# yml-소스 원칙과의 관계: yml verified_at(scripts/verify_usage_patterns.py --apply 로 커밋)이 여전히
#   정본이고, 이 스탬프는 그것이 **없는 초안**만 채운다. 나중에 --apply 를 돌리면 yml↔D1 이 수렴한다.

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Sequence
from zoneinfo import ZoneInfo

_PLACEHOLDER_RE = re.compile(r":([a-z][a-z0-9_]*)")
_RELATIVE_RE = re.compile(r"^([+-]?\d+)(d|w|M|y)$")
_RELATIVE_AS = {"date", "datetime", "ym", "year"}
_KST = ZoneInfo("Asia/Seoul")


def executable_sql(sql_text: str) -> str:
    """`--` 라인 주석·`/* :y */` 인라인 주석 제거(후자는 값이 이미 상수로 박힌 형)."""
    stripped = re.sub(r"/\*.*?\*/", " ", sql_text or "")
    return "\n".join(re.sub(r"--.*$", "", line) for line in stripped.splitlines())


def _relative_default_literal(
    default: Any,
    *,
    now: datetime | None,
) -> tuple[str | None, bool]:
    """Return (SQL literal, declared) for a relative default.

    ``declared`` distinguishes an invalid relative declaration from an absent
    declaration: invalid metadata must not fall back to a stale SQL comment.
    """
    if not isinstance(default, Mapping):
        return None, False
    if set(default) != {"rel", "as"}:
        return None, True
    rel = default.get("rel")
    as_type = default.get("as")
    match = _RELATIVE_RE.fullmatch(rel) if isinstance(rel, str) else None
    if not match or not isinstance(as_type, str) or as_type not in _RELATIVE_AS:
        return None, True

    current = datetime.now(_KST) if now is None else now
    if current.tzinfo is None:
        current = current.replace(tzinfo=_KST)
    else:
        current = current.astimezone(_KST)
    amount, unit = int(match.group(1)), match.group(2)
    try:
        if unit == "d":
            shifted = current + timedelta(days=amount)
        elif unit == "w":
            shifted = current + timedelta(weeks=amount)
        else:
            if unit == "M":
                month_index = current.year * 12 + current.month - 1 + amount
                year, month_zero_based = divmod(month_index, 12)
                anchor = current.replace(year=year, month=month_zero_based + 1, day=1)
            else:
                anchor = current.replace(year=current.year + amount, day=1)
            # JavaScript Date.setUTCMonth/setUTCFullYear overflows invalid days
            # into the following month; adding day-1 to day 1 matches that rule.
            shifted = anchor + timedelta(days=current.day - 1)
    except (OverflowError, ValueError):
        return None, True

    if as_type == "date":
        text = shifted.strftime("%Y-%m-%d")
    elif as_type == "datetime":
        clock = shifted.strftime("%H:%M:%S") if amount == 0 else "00:00:00"
        text = shifted.strftime("%Y-%m-%d") + " " + clock
    elif as_type == "ym":
        text = shifted.strftime("%Y-%m")
    else:
        text = shifted.strftime("%Y")
    return "'" + text + "'", True


def resolve_params(
    sql_text: str,
    hint_text: str = "",
    *,
    param_defaults: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> tuple[str, dict[str, str], list[str]]:
    """(치환된 실행문, 사용값, 미해결 이름) — 예시값을 sql 본문 주석 → 힌트 순으로 찾아 치환.

    verify_usage_patterns.py 의 동명 함수와 같은 규약(주석 `-- :n=10`, `:gu='성동구'` 형)이다.
    `param_defaults`에 상대 기본값이 선언된 파라미터는 SQL 주석의 정적 예시보다 우선한다.
    못 찾은 파라미터가 하나라도 있으면 실행을 포기한다(추측값 없음).
    """
    executable = executable_sql(sql_text)
    names = sorted(set(_PLACEHOLDER_RE.findall(executable)), key=len, reverse=True)
    resolved: dict[str, str] = {}
    unresolved: list[str] = []
    defaults = param_defaults if isinstance(param_defaults, Mapping) else {}
    for name in names:
        if name in defaults:
            value, declared = _relative_default_literal(defaults[name], now=now)
            if declared:
                if value is None:
                    unresolved.append(name)
                else:
                    resolved[name] = value
                continue
        value = None
        for source in (sql_text or "", hint_text or ""):
            for m in re.finditer(rf":{name}(?![a-z0-9_])", source):
                # 예시값은 항상 한 줄(주석 한 행 또는 SQL 한 행) → tail 을 그 줄 끝까지로 잡는다.
                # (60자 고정이면 `:ids=["...","...","..."]` 같은 긴 한 줄 배열이 잘려 미해결로 샜다.)
                rest = source[m.end():]
                nl = rest.find("\n")
                tail = (rest if nl < 0 else rest[:nl])[:600]
                # 🔴 예시값은 **`:이름=값` 꼴이다 — `=` 에 앵커를 건다.** 예전에는 `=` 앞으로
                #   16자까지 아무 문자나 건너뛰며 첫 숫자를 찾았는데, 그 관대함이
                #   `-- :gu=종로구, :from=2026-07-01` 에서 `:gu` 에 **다음 파라미터의 숫자
                #   2026** 을 물려 줬다. 값이 없는 파라미터는 미해결로 남아 스킵되는 게 맞지,
                #   옆칸 숫자를 주워 오면 안 된다.
                eq = re.match(r"\s*=\s*", tail)
                if not eq:
                    continue
                val = tail[eq.end():]
                # ① 따옴표 문자열 (`:gu='성동구'`)
                qm = re.match(r"'(?:[^']|'')*'", val)
                if qm:
                    value = qm.group(0)
                    break
                # ② 한 줄 배열 (`:gus=['a','b']` — json_each(:gus) 검증용, JSON 문자열로 bind)
                am = re.match(r"\[[^\]\n]*\]", val)
                if am:
                    value = "'" + am.group(0).replace("'", "''") + "'"
                    break
                # ③ 숫자 (`:n=10`) — **값 전체가 숫자일 때만.** `2026-07-01` 의 앞 네 자리를
                #   숫자로 삼키면 `event_date BETWEEN 2026 AND 2026` 이 되는데, SQLite 는
                #   TEXT↔INTEGER 를 타입 순서로 비교해 **조용히 0행**(또는 `>= 2026` 이면
                #   반대로 **필터 무력화**)이 된다. 둘 다 검증을 통과하거나 못 하게 만든다.
                #   반대로 마침표를 무조건 거부해서도 안 된다 — `-- :n = 5. 설명문…` 처럼
                #   **값 뒤에 문장이 붙는** 저작 관행이 있고(commerce 5건 실측), 그걸 숫자로
                #   못 읽으면 ④가 설명문 전체를 값으로 삼켜 `LIMIT '5. 설명문…'` 이 된다.
                #   가르는 기준은 마침표 뒤가 **숫자냐**다 — `3.5` 는 소수, `5. ` 는 문장 끝.
                num = re.match(r"[0-9]+(?:\.[0-9]+)?(?![0-9A-Za-z_\-/:]|\.[0-9])", val)
                if num:
                    value = num.group(0)
                    break
                # ④ 따옴표 없는 값 (`:gu=ALL`·`:from=2026-07-01`·`:level=약간 붐빔`) → SQL 문자열로 감싼다
                tm = re.match(r"[^,\n\]]+", val)
                if tm:
                    v = tm.group(0).strip()
                    if v:
                        value = "'" + v.replace("'", "''") + "'"
                        break
            if value is not None:
                break
        if value is None:
            unresolved.append(name)
        else:
            resolved[name] = value
    substituted = executable
    for name in names:
        if name in resolved:
            value = resolved[name]
            # P3 배열 전개형(`gu IN (:gus)` — json_each 아님): 게이트웨이는 array 파라미터를
            # ?,?,? 로 전개해 실행하므로, 검증도 JSON 배열 예시를 IN 리스트로 전개해 같은
            # 형으로 돌린다. JSON 문자열 그대로 넣으면 단일 리터럴 비교가 되어 조용히 0행.
            if (value.startswith("'[") and value.endswith("]'")
                    and re.search(rf"\bIN\s*\(\s*:{name}\s*\)", substituted, re.I)
                    and not re.search(rf"json_each\(\s*:{name}\s*\)", substituted, re.I)):
                try:
                    items = json.loads(value[1:-1].replace("''", "'"))
                    value = ", ".join(
                        str(x) if isinstance(x, (int, float)) and not isinstance(x, bool)
                        else "'" + str(x).replace("'", "''") + "'" for x in items)
                except ValueError:
                    pass                                   # 배열 파싱 실패 — 원문 유지(미해결과 동급)
            substituted = re.sub(rf":{name}(?![a-z0-9_])", value.replace("\\", r"\\"), substituted)
    return substituted.strip(), resolved, unresolved


def _hint_of(row: dict[str, Any]) -> str:
    return "\n".join(str(row.get(k) or "") for k in ("question_ko", "axes", "insight_sample_ko"))


def verify_and_stamp(
    pattern_rows: Sequence[dict[str, Any]],
    *,
    run_sql: Callable[[str], list],
    publication_id: str,
    now_iso: str | None = None,
    param_overrides: dict[str, dict[str, str]] | None = None,
    param_defaults_by_pattern: Mapping[str, Mapping[str, Any]] | None = None,
    now: datetime | None = None,
) -> dict[str, list]:
    """미검증 패턴을 실 D1 에 돌려 통과분에 스탬프한다(pattern_rows 를 제자리 수정).

    run_sql(sql) -> list[rows] (실패 시 예외). publication_id = 이 제품의 현재 게시본 id.
    이미 verified_at 이 있는 행은 건드리지 않는다(손 검증 존중). 반환: 처리 요약(로그용).
    """
    stamp_now = now_iso or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    overrides = param_overrides or {}
    defaults_by_pattern = param_defaults_by_pattern or {}
    report: dict[str, list] = {"verified": [], "failed": [], "skipped": []}
    for row in pattern_rows:
        pid = str(row.get("pattern_id"))
        if row.get("verified_at"):                       # 이미 검증(yml 스탬프) — 무접촉
            continue
        if not publication_id:                           # 게시본 id 없음 — 스탬프 근거 없음
            report["skipped"].append((pid, "no publication_id"))
            continue
        sql = row.get("sql") or ""
        substituted, resolved, unresolved = resolve_params(
            sql,
            _hint_of(row),
            param_defaults=defaults_by_pattern.get(pid),
            now=now,
        )
        ov = overrides.get(pid)
        if ov:
            resolved = {**resolved, **ov}
            unresolved = [n for n in unresolved if n not in ov]
            substituted = executable_sql(sql)
            for name in sorted(resolved, key=len, reverse=True):
                substituted = re.sub(rf":{name}(?![a-z0-9_])", resolved[name], substituted)
            substituted = substituted.strip()
        if unresolved:
            report["skipped"].append((pid, f"예시값 미해결 {unresolved}"))
            continue
        head = substituted.lstrip().lower()
        if not (head.startswith("select") or head.startswith("with")):
            report["skipped"].append((pid, "SELECT/WITH 아님"))
            continue
        try:
            rows = run_sql(substituted.rstrip(";") + ";")
        except Exception as exc:                          # noqa: BLE001 — 개별 실패는 남기고 계속
            report["failed"].append((pid, type(exc).__name__))
            continue
        measured = len(rows)
        allow_empty = bool(row.get("allow_empty"))
        if measured == 0 and not allow_empty:             # 예시값 조합은 0행 초과여야 검증(§8)
            report["skipped"].append((pid, "0행(allow_empty 아님)"))
            continue
        row["verified_rows"] = measured
        row["verified_at"] = stamp_now
        row["verified_publication_id"] = publication_id
        report["verified"].append(pid)
    return report
