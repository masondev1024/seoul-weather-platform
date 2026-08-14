"""usage_patterns 전 도메인 검증 실측 + verified_* 스탬프 (#638 §5-1 — 손 백필 금지, 재실행 실측만).

commerce 전용 verify(ASAC-DAG scripts/verify_usage_patterns.py)의 전 도메인 일반화다.
각 도메인 yml 의 **미검증 패턴**(verified_at 없음)의 sql 을 실 D1 에 실행(SELECT 만)해:

  1) >0행 → verified_rows / verified_at / verified_publication_id 를 yml 에 스탬프한다.
  2) 0행 → 예시값이 낡은 것(D1 은 최근 창만 보유)이므로 **실데이터 최빈 조합**으로
     예시를 교정(주석 갱신)한 뒤 재실행, 통과하면 스탬프한다.
  3) 그래도 0행/교정불가 → 스탬프하지 않고 보고만 한다(추측 스탬프 금지).

yml 은 텍스트 삽입만 한다(형식·주석 보존, yaml round-trip 금지). verified_at 서식은
파일의 기존 스탬프 스타일(날짜형/ISO형·pub id 인용부호)을 따른다.
verified_publication_id 는 실행 시점 _catalog 의 그 제품 현재 게시본 id 다(#638 §2.3).

실행(저장소 dbt/ 루트에서):
  python serving_contract/verify_stamp.py --env-file ../.env                 # 측정만(dry-run)
  python serving_contract/verify_stamp.py --env-file ../.env --apply         # 교정+스탬프 적용
토큰(CLOUDFLARE_API_TOKEN)은 헤더로만 쓰이고 어디에도 출력하지 않는다.
"""
from __future__ import annotations

import argparse
import collections
import glob
import io
import json
import os
import re
import sys
import urllib.request
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

DOMAINS = ("transit", "citydata", "culture", "traffic_weather", "commerce")
PLACEHOLDER_RE = re.compile(r":([a-z][a-z0-9_]*)")
PHYS_RE = re.compile(r"\bFROM\s+((?:gold|d1)_[a-z0-9_]+)", re.I)
EQ_RE = re.compile(r"\b([a-z_][a-z0-9_]*)\s*=\s*:([a-z][a-z0-9_]*)")
GE_RE = re.compile(r"\b([a-z_][a-z0-9_]*)\s*>=?\s*:([a-z][a-z0-9_]*)")
LE_RE = re.compile(r"\b([a-z_][a-z0-9_]*)\s*<=?\s*:([a-z][a-z0-9_]*)")
CONST_RE = re.compile(r"(?<!:)\b([a-z_][a-z0-9_]*)\s*=\s*([0-9]+|'(?:[^']|'')*')(?!\s*\))")
BT_RE = re.compile(r"\b([a-z_][a-z0-9_]*)\s+BETWEEN\s+:([a-z][a-z0-9_]*)\s+AND\s+:([a-z][a-z0-9_]*)", re.I)
ARR_RE = re.compile(r"\b([a-z_][a-z0-9_]*)\s+IN\s*\(\s*SELECT\s+value\s+FROM\s+json_each\(\s*:([a-z][a-z0-9_]*)\s*\)", re.I)
SENT_RE = re.compile(r":([a-z][a-z0-9_]*)\s*=\s*'")   # `:gu = 'ALL'` 센티널 — 교정 금지
RELATIVE_RE = re.compile(r"^([+-]?\d+)(d|w|M|y)$")
RELATIVE_AS = {"date", "datetime", "ym", "year"}
KST = ZoneInfo("Asia/Seoul")


# ── 예시값 해석 (dags common/serving/pattern_verify.resolve_params 와 규약 잠금) ──

def executable_sql(sql_text: str) -> str:
    stripped = re.sub(r"/\*.*?\*/", " ", sql_text or "")
    return "\n".join(re.sub(r"--.*$", "", line) for line in stripped.splitlines())


def _relative_default_literal(
    default,
    *,
    now: datetime | None,
) -> tuple[str | None, bool]:
    """Return (SQL literal, declared) for a relative default.

    ``declared`` distinguishes invalid metadata from an absent declaration so
    an invalid relative default cannot fall back to a stale SQL comment.
    """
    if not isinstance(default, Mapping):
        return None, False
    if set(default) != {"rel", "as"}:
        return None, True
    rel = default.get("rel")
    as_type = default.get("as")
    match = RELATIVE_RE.fullmatch(rel) if isinstance(rel, str) else None
    if not match or not isinstance(as_type, str) or as_type not in RELATIVE_AS:
        return None, True

    current = datetime.now(KST) if now is None else now
    if current.tzinfo is None:
        current = current.replace(tzinfo=KST)
    else:
        current = current.astimezone(KST)
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
            # Match JavaScript Date.setUTCMonth/setUTCFullYear overflow semantics.
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
    param_defaults: Mapping[str, object] | None = None,
    now: datetime | None = None,
) -> tuple[str, dict, list]:
    executable = executable_sql(sql_text)
    names = sorted(set(PLACEHOLDER_RE.findall(executable)), key=len, reverse=True)
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
                rest = source[m.end():]
                nl = rest.find("\n")
                tail = (rest if nl < 0 else rest[:nl])[:600]
                # 예시값은 `:이름=값` 꼴 — **`=` 앵커 필수**(ASAC-DAG#756/#763 pattern_verify
                # 와 규약 잠금). 값 전체를 원자적으로 읽는다(날짜 잘림·옆칸 숫자 오집 방지).
                em = re.match(r"\s*=\s*", tail)
                if not em:
                    continue
                rv = tail[em.end():]
                if rv.startswith("'"):
                    qm = re.match(r"'(?:[^']|'')*'", rv)
                    if qm:
                        value = qm.group(0)
                        break
                elif rv.startswith("["):
                    bm = re.match(r"\[[^\]\n]*\]", rv)
                    if bm:
                        value = "'" + bm.group(0).replace("'", "''") + "'"
                        break
                else:
                    # 숫자 — 값 전체가 숫자일 때만(#756: 날짜 앞자리 삼킴 방지). 단 `:n = 5. 설명…`
                    # 처럼 값 뒤 문장이 붙는 저작 관행이 있어(#763 실측) 마침표 무조건 거부도 안
                    # 된다 — 마침표 뒤가 숫자냐로 가른다(`3.5` 소수 vs `5. ` 문장 끝).
                    num = re.match(r"[0-9]+(?:\.[0-9]+)?(?![0-9A-Za-z_\-/:]|\.[0-9])", rv)
                    if num:
                        value = num.group(0)
                        break
                    token = rv.split(",")[0].strip()
                    if token:
                        value = "'" + token.replace("'", "''") + "'"
                        break
            if value is not None:
                break
        if value is None:
            unresolved.append(name)
        else:
            resolved[name] = value
    return executable, resolved, unresolved


def substitute(sql_exec: str, values: dict) -> str:
    out = sql_exec
    for name in sorted(values, key=len, reverse=True):
        v = values[name]
        # P3 배열(IN (:p) 전개형 — json_each 아님): JSON 배열 예시를 IN 리스트로 전개
        if (v.startswith("'[") and v.endswith("]'")
                and re.search(rf"\bIN\s*\(\s*:{name}\s*\)", out, re.I)
                and not re.search(rf"json_each\(\s*:{name}\s*\)", out, re.I)):
            try:
                items = json.loads(v[1:-1].replace("''", "'"))
                v = ", ".join(lit(x) for x in items)
            except ValueError:
                pass
        out = re.sub(rf":{name}(?![a-z0-9_])", v, out)
    return out.strip()


# ── D1 (stdlib urllib — serving_contract 는 외부 의존 PyYAML 뿐) ────────────────

class D1:
    def __init__(self, account_id: str, database_id: str, token: str) -> None:
        self._url = (f"https://api.cloudflare.com/client/v4/accounts/"
                     f"{account_id}/d1/database/{database_id}/query")
        self._token = token   # 헤더로만 사용, 출력 금지

    def execute(self, sql: str) -> list[dict]:
        body = json.dumps({"sql": sql}).encode("utf-8")
        req = urllib.request.Request(
            self._url, data=body, method="POST",
            headers={"Authorization": f"Bearer {self._token}",
                     "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if not payload.get("success"):
            raise RuntimeError(f"D1 오류: {[e.get('message') for e in payload.get('errors', [])][:1]}")
        results = payload.get("result") or []
        return (results[0].get("results") or []) if results else []


def lit(v) -> str:
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def repair(d1: D1, sql_raw: str, resolved: dict) -> tuple[dict | None, str]:
    """0행 패턴 예시를 실데이터 최빈 동시출현 조합으로 교정."""
    body = executable_sql(sql_raw)
    m = PHYS_RE.search(body)
    if not m:
        return None, "물리 테이블 못 찾음"
    table = m.group(1)
    sentinels = set(SENT_RE.findall(body))
    eq = [(c, p) for c, p in EQ_RE.findall(body) if p in resolved and p not in sentinels]
    ge = [(c, p) for c, p in GE_RE.findall(body) if p in resolved]
    le = [(c, p) for c, p in LE_RE.findall(body) if p in resolved]
    bt = [(c, a, b) for c, a, b in BT_RE.findall(body) if a in resolved and b in resolved]
    arr = [(c, p) for c, p in ARR_RE.findall(body) if p in resolved]
    if not (eq or ge or le or bt or arr):
        return None, "교정할 바인딩 없음"
    new = dict(resolved)
    # 패턴 자신의 상수 등호 조건(`is_raining = 1` 류)을 조합 선택에 반영한다 — 안 하면
    # 최빈 조합이 그 조건과 동시출현하지 않아 교정 후에도 0행이 난다. 못 맞추면 무조건으로 완화.
    consts = [f'"{c}" = {v}' for c, v in CONST_RE.findall(body)
              if c not in {cc for cc, _ in eq} and c in body]
    if eq:
        cols = [c for c, _ in eq]
        sel = ", ".join(f'"{c}"' for c in cols)
        base = " AND ".join(f'"{c}" IS NOT NULL' for c in cols)
        rows = []
        for where in ([base + "".join(" AND " + x for x in consts)] if consts else []) + [base]:
            try:
                rows = d1.execute(f'SELECT {sel} FROM "{table}" WHERE {where} '
                                  f"GROUP BY {sel} ORDER BY COUNT(*) DESC LIMIT 1;")
            except Exception:  # noqa: BLE001 — 상수 조건이 다른 절의 문맥일 수 있음 → 완화 재시도
                rows = []
            if rows:
                break
        if not rows:
            return None, "테이블 실데이터 없음"
        for c, p in eq:
            new[p] = lit(rows[0][c])
    eq_where = " AND ".join(f'"{c}" = {new[p]}' for c, p in eq) if eq else "1=1"
    for c, a, b in bt:
        r = d1.execute(f'SELECT MIN("{c}") lo, MAX("{c}") hi FROM "{table}" WHERE {eq_where};')
        if r and r[0]["lo"] is not None:
            new[a], new[b] = lit(r[0]["lo"]), lit(r[0]["hi"])
    for c, p in ge:
        r = d1.execute(f'SELECT MIN("{c}") v FROM "{table}" WHERE {eq_where};')
        if r and r[0]["v"] is not None:
            new[p] = lit(r[0]["v"])
    for c, p in le:
        r = d1.execute(f'SELECT MAX("{c}") v FROM "{table}" WHERE {eq_where};')
        if r and r[0]["v"] is not None:
            new[p] = lit(r[0]["v"])
    for c, p in arr:
        r = d1.execute(f'SELECT "{c}" v, COUNT(*) n FROM "{table}" WHERE {eq_where} '
                       f'AND "{c}" IS NOT NULL GROUP BY "{c}" ORDER BY n DESC LIMIT 3;')
        vals = [x["v"] for x in r]
        if vals:
            new[p] = lit(json.dumps(vals, ensure_ascii=False))
    return new, "교정됨"


# ── yml 텍스트 스탬퍼 (형식 보존 — 텍스트 삽입만) ──────────────────────────────

def display_of(sqlval: str) -> str:
    s = str(sqlval)
    if s.startswith("'") and s.endswith("'"):
        return s[1:-1].replace("''", "'")
    return s


def update_comment_line(line: str, changes: dict) -> tuple[bool, str]:
    out, hit = line, False
    for name, newval in changes.items():
        m = re.search(rf":{name}=", out)
        if not m:
            continue
        start = m.end()
        rest = out[start:]
        if rest.startswith("["):
            end = rest.find("]")
            if end < 0:
                continue
            span, new_disp = end + 1, display_of(newval)
        elif rest.startswith("'"):
            m2 = re.match(r"'(?:[^']|'')*'", rest)
            if not m2:
                continue
            span = m2.end()
            new_disp = "'" + display_of(newval).replace("'", "''") + "'"
        else:
            m2 = re.search(r",\s+:", rest)
            span = m2.start() if m2 else len(rest)
            new_disp = display_of(newval)
        out = out[:start] + new_disp + out[start + span:]
        hit = True
    return hit, out


def stamp_one(path: Path, product: str, pattern: str, rows: int, pub_id: str,
              changes: dict, at_iso: str, at_date: str) -> str | None:
    """한 패턴 스탬프. 성공 None / 실패 사유 문자열."""
    text = io.open(path, encoding="utf-8", newline="").read()
    nl = "\r\n" if "\r\n" in text else "\n"
    lines = text.split(nl)
    iso_style = any("verified_at:" in l and "T" in l for l in lines)
    q = '"' if any(re.search(r'verified_publication_id:\s*"', l) for l in lines) else ""
    at_val = at_iso if iso_style else at_date

    cur_prod, pat_i = None, None
    for i, l in enumerate(lines):
        pm = re.search(r"\bproduct_id:\s*\"?([a-z0-9_]+)\"?", l)
        if pm:
            cur_prod = pm.group(1)
        if cur_prod == product and re.search(rf"-\s*pattern_id:\s*\"?{re.escape(pattern)}\"?\s*$", l):
            pat_i = i
            break
    if pat_i is None:
        return "pattern_id 라인 못 찾음"
    item_indent = len(lines[pat_i]) - len(lines[pat_i].lstrip())
    end = len(lines)
    for j in range(pat_i + 1, len(lines)):
        l = lines[j]
        if not l.strip():
            continue
        ind = len(l) - len(l.lstrip())
        if ind < item_indent or (ind == item_indent and l.lstrip().startswith("- ")):
            end = j
            break
    block = list(range(pat_i, end))

    pend = dict(changes or {})
    if pend:
        for j in block:
            if lines[j].lstrip().startswith("--"):
                sub = {k: v for k, v in pend.items() if re.search(rf":{k}=", lines[j])}
                if sub:
                    ok, newl = update_comment_line(lines[j], sub)
                    if ok:
                        lines[j] = newl
                        for k in sub:
                            pend.pop(k, None)
        if pend:
            return f"주석 값 못 바꿈 {sorted(pend)}"

    pad = " " * (item_indent + 2)
    vr_i = at_i = pi_i = sql_i = None
    for j in block:
        ls = lines[j].lstrip()
        if ls.startswith("verified_rows:"):
            vr_i = j
        elif ls.startswith("verified_at:"):
            at_i = j
        elif ls.startswith("verified_publication_id:"):
            pi_i = j
        elif re.match(r"sql:\s*[|>]?", ls) and sql_i is None:
            sql_i = j
    vr_line = f"{pad}verified_rows: {rows}"
    at_line = f'{pad}verified_at: "{at_val}"'
    pi_line = f"{pad}verified_publication_id: {q}{pub_id}{q}"
    if vr_i is not None:
        lines[vr_i] = vr_line
        if at_i is not None:
            lines[at_i] = at_line
        else:
            lines.insert(vr_i + 1, at_line)
            at_i = vr_i + 1
            if pi_i is not None and pi_i > vr_i:
                pi_i += 1
        if pi_i is not None:
            lines[pi_i] = pi_line
        else:
            lines.insert(at_i + 1, pi_line)
    else:
        anchor = sql_i if sql_i is not None else pat_i + 1
        lines[anchor:anchor] = [vr_line, at_line, pi_line]

    io.open(path, "w", encoding="utf-8", newline="").write(nl.join(lines))
    return None


# ── main ───────────────────────────────────────────────────────────────────────

def load_env_file(path: str) -> None:
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        if k.strip():
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def hint_of(p: dict) -> str:
    return "\n".join(str(p.get(k) or "") for k in ("question_ko", "axes", "insight_sample_ko"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", default=None)
    ap.add_argument("--apply", action="store_true", help="교정+스탬프를 yml 에 기록")
    ap.add_argument("--domains", nargs="*", default=list(DOMAINS))
    args = ap.parse_args()
    if args.env_file:
        load_env_file(args.env_file)

    acct = os.environ.get("SERVING_CLOUDFLARE_ACCOUNT_ID") or os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    dbid = os.environ.get("SERVING_D1_DATABASE_ID")
    token = os.environ.get("CLOUDFLARE_API_TOKEN")
    if not (acct and dbid and token):
        print("ERROR: CLOUDFLARE_ACCOUNT_ID / SERVING_D1_DATABASE_ID / CLOUDFLARE_API_TOKEN 필요")
        return 2
    d1 = D1(acct, dbid, token)

    pub = {r["product_id"]: r["publication_id"] for r in d1.execute(
        "SELECT product_id, publication_id FROM _catalog;")}
    for r in d1.execute("SELECT product_id, publication_id FROM d1_catalog_ext;"):
        pub.setdefault(r["product_id"], r["publication_id"])

    now = datetime.now(timezone.utc)
    at_iso, at_date = now.strftime("%Y-%m-%dT%H:%M:%SZ"), now.strftime("%Y-%m-%d")
    root = Path(__file__).resolve().parents[1]
    cnt = collections.Counter()
    residual = []

    for dom in args.domains:
        for f in sorted(glob.glob(str(root / "domains" / dom / "models" / "**" / "*.yml"),
                                  recursive=True)):
            d = yaml.safe_load(io.open(f, encoding="utf-8")) or {}
            for m in d.get("models") or []:
                serving = ((m.get("config") or {}).get("meta") or {}).get("serving", {})
                prod = serving.get("product_id")
                for p in serving.get("usage_patterns") or []:
                    if p.get("verified_at"):
                        continue
                    pid = p.get("pattern_id")
                    sql = p.get("sql") or ""
                    _, resolved, unresolved = resolve_params(
                        sql,
                        hint_of(p),
                        param_defaults=p.get("param_defaults"),
                        now=now,
                    )
                    tag = f"[{dom}] {prod} :: {pid}"
                    if unresolved:
                        cnt["unresolved"] += 1
                        residual.append((tag, f"예시 미해결 {unresolved}"))
                        continue
                    if not pub.get(prod):
                        cnt["no_publication"] += 1
                        residual.append((tag, "게시본 id 없음(_catalog 미게재)"))
                        continue
                    try:
                        n = len(d1.execute(substitute(executable_sql(sql), resolved).rstrip(";") + ";"))
                    except Exception as exc:  # noqa: BLE001
                        cnt["error"] += 1
                        residual.append((tag, f"실행 실패 {type(exc).__name__}"))
                        continue
                    changes: dict = {}
                    if n == 0 and p.get("allow_empty") is True:
                        # 선언적으로 0행이 정상인 축(무강수 등) — 0행도 유효한 검증이다
                        cnt["ok_empty"] += 1
                        if args.apply:
                            err = stamp_one(Path(f), prod, pid, 0, pub[prod], {}, at_iso, at_date)
                            if err:
                                cnt["stamp_fail"] += 1
                                residual.append((tag, f"스탬프 실패: {err}"))
                            else:
                                cnt["stamped"] += 1
                        continue
                    if n == 0:
                        newvals, note = repair(d1, sql, resolved)
                        if not newvals:
                            cnt["zero_norepair"] += 1
                            residual.append((tag, f"0행·{note}"))
                            continue
                        changes = {k: v for k, v in newvals.items() if v != resolved.get(k)}
                        try:
                            n = len(d1.execute(substitute(executable_sql(sql), newvals).rstrip(";") + ";"))
                        except Exception as exc:  # noqa: BLE001
                            cnt["error"] += 1
                            residual.append((tag, f"교정 실행 실패 {type(exc).__name__}"))
                            continue
                        if n == 0:
                            cnt["still_zero"] += 1
                            residual.append((tag, "교정 후에도 0행"))
                            continue
                        cnt["repaired"] += 1
                    else:
                        cnt["ok"] += 1
                    if args.apply:
                        err = stamp_one(Path(f), prod, pid, n, pub[prod], changes, at_iso, at_date)
                        if err:
                            cnt["stamp_fail"] += 1
                            residual.append((tag, f"스탬프 실패: {err}"))
                        else:
                            cnt["stamped"] += 1

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"==== verify_stamp ({mode}) ====")
    for k, v in sorted(cnt.items()):
        print(f"  {k:12} {v}")
    if residual:
        print(f"\n-- 잔여 {len(residual)} (스탬프 안 됨 — 추측 스탬프 금지) --")
        for tag, why in residual:
            print(f"  {tag}  {why}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
