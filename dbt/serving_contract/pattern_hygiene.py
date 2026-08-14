"""usage_patterns 위생 도구 — 예시 표준화 + 상대 날짜 기본값 채움 (#217 후속).

두 가지를 일괄 처리한다(yml 텍스트 편집 — 형식·주석 보존, SQL 실행부 불변 검증):

A) **예시 주석 표준화**: 레거시 서식(`:n 예시 10` · `/* 예: 'x' */` · 트레일링 예시)을
   표준 `-- :name=value` 선행 주석으로 정규화한다. 엄격 파서(`=` 앵커, ASAC-DAG#756)가
   전 파라미터를 해석할 수 있게 되어 재검증 재현성이 복원된다.
   안전망: 편집 전후 executable_sql(주석 제거 실행부)이 완전히 같아야 적용한다.

B) **상대 날짜 기본값 채움**: 날짜형 파라미터인데 param_defaults 에 상대값({rel,as})이
   없는 패턴에, 고정 날짜 대신 실행 시점 동적 해석(게이트웨이 resolveRelativeDefault)
   기본값을 채운다. 정책:
     - `:today`류 가드(event_end_date >= :today 등) → {rel: 0d, as: date}
     - 일 단위 등호(date/event_date/chart_date/snapshot_date = :p) → {rel: -1d, as: date}
       (일 배치는 어제까지 적재가 보장 — 오늘 등호는 빈 결과가 잦다)
       단, 예보 축(forecast_date)은 오늘·미래가 본체이므로 {rel: 0d, as: date}
     - 월 축(ym) 등호 → {rel: -1M, as: ym} / 연 축(year) → {rel: -1y, as: year}
     - 범위 경계(>= :from / <= :to / BETWEEN) → from {rel: -30d} · to {rel: 0d}
       (값 형이 datetime 이면 as: datetime, 아니면 as: date)
     - **시간 버킷 등호(ts/at/time_bucket/hour_at = :p)는 제외** — rel 해석값(분·초 포함)이
       버킷 문자열 등호와 일치하지 않아 기본 실행이 조용히 0행이 된다. 목록만 보고한다.

실행(저장소 dbt/ 루트에서):
  python serving_contract/pattern_hygiene.py            # dry-run(계획 보고만)
  python serving_contract/pattern_hygiene.py --apply    # yml 반영
"""
from __future__ import annotations

import argparse
import collections
import glob
import io
import re
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from serving_contract.verify_stamp import (  # noqa: E402 — 파서 규약 단일화(#756 잠금)
    executable_sql, resolve_params, hint_of, substitute, D1, load_env_file, lit, PHYS_RE,
)

DOMAINS = ("transit", "citydata", "culture", "traffic_weather", "commerce")
DATE_VAL = re.compile(r"^'?(20\d{2})(-\d{2})?(-\d{2})?( \d{2}:\d{2}(:\d{2})?)?'?$")
YM_VAL = re.compile(r"^'?20\d{2}-\d{2}'?$")
YEAR_VAL = re.compile(r"^'?20\d{2}'?$")
DT_VAL = re.compile(r"^'?20\d{2}-\d{2}-\d{2} \d{2}:\d{2}(:\d{2})?'?$")

# 레거시 예시 서식 추출기 — commerce verify_usage_patterns.py 의 관용 문법 부분집합
LEGACY_PIPE = re.compile(r"--\s*(:[a-z][a-z0-9_]*\s+예시\s+[^|\n]+(\|\s*:[a-z][a-z0-9_]*\s+예시\s+[^|\n]+)*)\s*$")
LEGACY_ONE = re.compile(r":([a-z][a-z0-9_]*)\s+예시\s+('(?:[^']|'')*'|[0-9]+(?:\.[0-9]+)?|[^\s,|]+)")
LEGACY_INLINE = re.compile(r"/\*\s*예:?\s*('(?:[^']|'')*'|[0-9]+(?:\.[0-9]+)?|[^*]+?)\s*\*/")


def legacy_examples(sql: str) -> dict[str, str]:
    """레거시 서식에서 param→표시값 추출(따옴표 유지)."""
    out: dict[str, str] = {}
    for m in LEGACY_ONE.finditer(sql):
        out.setdefault(m.group(1), m.group(2).strip())
    # 인라인 `/* 예: V */` 는 직전 :param 에 귀속
    for m in re.finditer(r":([a-z][a-z0-9_]*)\s*(/\*\s*예:?\s*([^*]+?)\s*\*/)", sql):
        out.setdefault(m.group(1), m.group(3).strip())
    return out


def normalize_examples(sql: str, needed: list[str]) -> tuple[str | None, str]:
    """미해결 파라미터를 레거시 예시에서 찾아 표준 선행 주석으로 이관. (새 sql|None, 사유)"""
    legacy = legacy_examples(sql)
    missing = [n for n in needed if n not in legacy]
    if missing:
        return None, f"레거시 예시에도 없음 {missing}"
    lines = sql.split("\n")
    # 레거시 조각 제거: `-- :x 예시 ...` 트레일링/전용 줄, `/* 예: ... */`
    new_lines = []
    for line in lines:
        line = LEGACY_INLINE.sub("", line)
        pm = LEGACY_PIPE.search(line)
        if pm:
            line = line[:pm.start()].rstrip()
        new_lines.append(line.rstrip() if line.strip() else line)
    body = "\n".join(l for l in new_lines if l.strip() != "")
    # 표준 선행 주석 구성 — 기존 표준 주석이 이미 있으면 그 뒤에 미해결분만 덧붙인 새 줄
    pairs = ", ".join(f":{n}={legacy[n]}" for n in needed)
    new_sql = f"-- {pairs}\n{body}"
    if " ".join(executable_sql(new_sql).split()) != " ".join(executable_sql(sql).split()):
        return None, "실행부 불변 검증 실패"
    return new_sql, "ok"


def derive_examples(d1, sql: str, missing: list[str]) -> tuple[dict[str, str] | None, str]:
    """예시가 아예 없는 파라미터의 대표값을 실데이터에서 유도(표시값 반환).

    유도 규칙: LIMIT/:n류→10 · min_* 임계→1 · 컬럼 등호/LIKE→최빈값(동일 컬럼 다중
    파라미터는 상위 K 분배) · lat/lng→평균 · bbox(min/max_lat/lng)→MIN/MAX.
    호출자가 유도값으로 실제 실행해 >0행을 확인한 뒤에만 기록한다.
    """
    body = executable_sql(sql)
    m = PHYS_RE.search(body)
    table = m.group(1) if m else None
    out: dict[str, str] = {}
    col_params: dict[str, list[str]] = collections.defaultdict(list)
    for name in missing:
        if re.search(rf"LIMIT\s+:{name}(?![a-z0-9_])", body, re.I) or name in ("n", "top_n"):
            out[name] = "10"
            continue
        if name.startswith("min_") and not re.search(r"lat|lng", name):
            out[name] = "1"
            continue
        cm = (re.search(rf"([a-z_][a-z0-9_]*)\s*=\s*:{name}(?![a-z0-9_])", body)
              or re.search(rf"([a-z_][a-z0-9_]*)\s+LIKE\s+[^:\n]*:{name}(?![a-z0-9_])", body, re.I))
        if cm and table:
            col_params[cm.group(1)].append(name)
            continue
        if re.search(r"lat|lng", name) and table:
            col = "lat" if "lat" in name else "lng"
            fn = "MIN" if name.startswith("min_") else ("MAX" if name.startswith("max_") else "AVG")
            try:
                r = d1.execute(f'SELECT ROUND({fn}("{col}"), 5) v FROM "{table}";')
                if r and r[0]["v"] is not None:
                    out[name] = str(r[0]["v"])
                    continue
            except Exception:  # noqa: BLE001
                pass
        return None, f"유도 규칙 없음 :{name}"
    for col, names in col_params.items():
        try:
            r = d1.execute(f'SELECT "{col}" v, COUNT(*) c FROM "{table}" WHERE "{col}" IS NOT NULL '
                           f'AND "{col}" != \'\' GROUP BY "{col}" ORDER BY c DESC LIMIT {len(names)};')
        except Exception as exc:  # noqa: BLE001
            return None, f"컬럼 {col} 질의 실패 {type(exc).__name__}"
        vals = [x["v"] for x in r]
        if len(vals) < len(names):
            return None, f"컬럼 {col} 값 부족"
        for name, v in zip(names, vals):
            out[name] = str(v) if isinstance(v, (int, float)) else "'" + str(v).replace("'", "''") + "'"
    return out, "ok"


BUCKET_EQ_NAMES = {"ts", "at", "time_bucket", "hour_at", "hour_slot", "bucket"}


def plan_rel_default(sql: str, name: str, value: str, product: str) -> tuple[dict | None, str]:
    """파라미터 하나의 상대 기본값 정책 결정. (default|None, 분류)"""
    body = executable_sql(sql)
    as_dt = bool(DT_VAL.match(value))
    # 임계 파라미터(min_*/max_*)는 값이 연도처럼 보여도 기간 축이 아니다 — min_annual=2000 을
    # 연도 rel 로 붙였다가 임계가 '2025' 가 되는 사고(#114 실측)를 막는다.
    if name.startswith(("min_", "max_")) and not re.search(r"lat|lng", name):
        return None, "threshold_skip"
    # 시간 버킷 **등호** — 제외. `>=`/`<=` 의 `=` 를 등호로 오인하면 datetime 범위
    # 하한(from_at 류)까지 스킵된다(실측 사고 — outlook_forecast_window from_at 누락).
    if re.search(rf"(?<![<>!])=\s*:{name}(?![a-z0-9_])", body) and (name in BUCKET_EQ_NAMES or as_dt):
        return None, "bucket_eq_skip"
    if name == "today" or re.search(rf">=\s*:{name}(?![a-z0-9_])", body) and name in ("today", "now"):
        return {"rel": "0d", "as": "date"}, "today_guard"
    # 🔴 범위 판정을 ym/연도 점 규칙보다 **먼저** 본다 — 순서를 바꾸면 `ym BETWEEN :a AND :b`
    #   의 양끝이 같은 -1M 을 받아 구간이 점으로 붕괴한다(ASK-Seoul#114 실측 사고: commerce
    #   연간 추이 11패턴이 from=to=-1y). 미래축(예보/리스크/강수창/전망) 범위에 과거 창을
    #   주는 것도 같은 사고 부류다 — 미래축은 0d~+Nd 로 앞을 본다.
    lower = re.search(rf"(>=|>)\s*:{name}(?![a-z0-9_])|BETWEEN\s+:{name}\b", body, re.I)
    upper = re.search(rf"(<=|<)\s*:{name}(?![a-z0-9_])|BETWEEN\s+:[a-z0-9_]+\s+AND\s+:{name}\b", body, re.I)
    future_axis = any(t in product for t in ("forecast", "risk", "precip", "outlook"))
    if lower or upper:
        if YM_VAL.match(value):
            return ({"rel": "0M", "as": "ym"} if upper else {"rel": "-11M", "as": "ym"}), "range_ym"
        if YEAR_VAL.match(value):
            return ({"rel": "0y", "as": "year"} if upper else {"rel": "-4y", "as": "year"}), "range_year"
        as_ = "datetime" if as_dt else "date"
        if future_axis:
            # current_outlook is an hourly snapshot. A 0d datetime lower bound resolves to
            # the invocation minute and excludes the current-hour row after the hour starts.
            if lower and as_dt and product == "weather_place_current_outlook":
                return {"rel": "0d", "as": "date"}, "range_future_current_snapshot"
            return ({"rel": "+2d", "as": as_} if upper else {"rel": "0d", "as": as_}), "range_future"
        if upper:
            return {"rel": "0d", "as": as_}, "range_to"
        return {"rel": "-30d", "as": as_}, "range_from"
    if YM_VAL.match(value):
        return {"rel": "-1M", "as": "ym"}, "ym"
    if YEAR_VAL.match(value):
        return {"rel": "-1y", "as": "year"}, "year"
    if DATE_VAL.match(value) and not as_dt:                 # 일 단위 등호
        rel = "0d" if "forecast" in product else "-1d"
        return {"rel": rel, "as": "date"}, "date_eq"
    return None, "unclassified_skip"


def fmt_default(v: dict) -> str:
    return "{ rel: \"%s\", as: %s }" % (v["rel"], v["as"])


def edit_pattern_block(path: Path, product: str, pattern: str,
                       new_sql: str | None, add_defaults: dict[str, dict]) -> str | None:
    """패턴 블록에 sql 교체·param_defaults 추가 적용. 성공 None / 실패 사유."""
    text = io.open(path, encoding="utf-8", newline="").read()
    nl = "\r\n" if "\r\n" in text else "\n"
    lines = text.split(nl)
    pid_re = re.compile(rf"-\s*pattern_id:\s*\"?{re.escape(pattern)}\"?\s*$")
    flow_re = re.compile(rf"-\s*\{{\s*pattern_id:\s*\"?{re.escape(pattern)}\"?\s*[,}}]")
    cur_prod, pat_i = None, None
    flow_hits, block_hits = [], []
    for i, l in enumerate(lines):
        pm = re.search(r"\bproduct_id:\s*\"?([a-z0-9_]+)\"?", l)
        if pm:
            cur_prod = pm.group(1)
        if pid_re.search(l):
            block_hits.append(i)
            if cur_prod == product:
                pat_i = i
                break
        if flow_re.search(l):
            flow_hits.append(i)

    # flow 한 줄 매핑(`- {pattern_id: ..., sql: "..."}`) — 인라인 param_defaults 병합만 지원
    if pat_i is None and len(flow_hits) == 1 and not block_hits:
        j = flow_hits[0]
        if new_sql is not None:
            return "flow 매핑 sql 교체 미지원"
        if add_defaults:
            inject = ", ".join(f"{k}: {fmt_default(v)}" for k, v in add_defaults.items())
            if re.search(r"param_defaults:\s*\{", lines[j]):
                lines[j] = re.sub(r"param_defaults:\s*\{", f"param_defaults: {{{inject}, ", lines[j], count=1)
            else:
                lines[j] = re.sub(r"\}\s*$", f", param_defaults: {{{inject}}}}}", lines[j], count=1)
        io.open(path, "w", encoding="utf-8", newline="").write(nl.join(lines))
        return None

    # product_id 가 usage_patterns 뒤에 선언되는 파일(commerce) — 파일 내 유일하면 전역 매칭
    if pat_i is None and len(block_hits) == 1:
        pat_i = block_hits[0]
    if pat_i is None:
        return "pattern_id 못 찾음"
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
    pad = " " * (item_indent + 2)

    # sql 블록 교체(항상 블록 스칼라로 기록)
    if new_sql is not None:
        sql_i = None
        for j in range(pat_i, end):
            if re.match(r"sql:\s*(\||\"|')?", lines[j].lstrip()) and lines[j].lstrip().startswith("sql:"):
                sql_i = j
                break
        if sql_i is None:
            return "sql 키 못 찾음"
        decl = lines[sql_i].lstrip()
        sql_pad = pad + "  "
        block = [f"{pad}sql: |"] + [f"{sql_pad}{l}" if l.strip() else "" for l in new_sql.split("\n")]
        if decl.startswith("sql: |"):
            sql_end = sql_i + 1
            while sql_end < end and (not lines[sql_end].strip()
                                     or len(lines[sql_end]) - len(lines[sql_end].lstrip()) > item_indent + 2):
                sql_end += 1
        else:                                    # 한 줄 스칼라(따옴표) — 그 줄만 교체
            sql_end = sql_i + 1
        lines[sql_i:sql_end] = block
        end += len(block) - (sql_end - sql_i)

    # param_defaults 추가/병합
    if add_defaults:
        pd_i = None
        for j in range(pat_i, end):
            if lines[j].lstrip().startswith("param_defaults:"):
                pd_i = j
                break
        if pd_i is not None and "{" in lines[pd_i]:
            inner = lines[pd_i]
            inject = ", ".join(f"{k}: {fmt_default(v)}" for k, v in add_defaults.items())
            lines[pd_i] = re.sub(r"\}\s*$", f", {inject} }}", inner)
        elif pd_i is not None:                    # 블록 매핑 — 하위 줄 추가
            insert = [f"{pad}  {k}: {fmt_default(v)}" for k, v in add_defaults.items()]
            lines[pd_i + 1:pd_i + 1] = insert
        else:
            inject = ", ".join(f"{k}: {fmt_default(v)}" for k, v in add_defaults.items())
            anchor = pat_i + 1
            for j in range(pat_i, end):
                if lines[j].lstrip().startswith("sql:"):
                    anchor = j
                    break
            lines[anchor:anchor] = [f"{pad}param_defaults: {{ {inject} }}"]

    io.open(path, "w", encoding="utf-8", newline="").write(nl.join(lines))
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--env-file", default=None, help="D1 자격(예시 유도·실행 검증용)")
    ap.add_argument("--domains", nargs="*", default=list(DOMAINS))
    args = ap.parse_args()
    d1 = None
    if args.env_file:
        load_env_file(args.env_file)
        import os
        acct = os.environ.get("SERVING_CLOUDFLARE_ACCOUNT_ID") or os.environ.get("CLOUDFLARE_ACCOUNT_ID")
        dbid, token = os.environ.get("SERVING_D1_DATABASE_ID"), os.environ.get("CLOUDFLARE_API_TOKEN")
        if acct and dbid and token:
            d1 = D1(acct, dbid, token)
    root = Path(__file__).resolve().parents[1]
    cnt = collections.Counter()
    report: list[str] = []

    for dom in args.domains:
        for f in sorted(glob.glob(str(root / "domains" / dom / "models" / "**" / "*.yml"), recursive=True)):
            d = yaml.safe_load(io.open(f, encoding="utf-8")) or {}
            for m in d.get("models") or []:
                serving = ((m.get("config") or {}).get("meta") or {}).get("serving", {})
                prod = serving.get("product_id") or ""
                for p in serving.get("usage_patterns") or []:
                    pid = p.get("pattern_id")
                    sql = p.get("sql") or ""
                    _, resolved, unresolved = resolve_params(sql, hint_of(p))
                    tag = f"[{dom}] {prod} :: {pid}"

                    new_sql = None
                    if unresolved:
                        new_sql, why = normalize_examples(sql, unresolved)
                        if new_sql is None and "레거시 예시에도 없음" in why and d1 is not None:
                            # 예시 자체가 없던 패턴 — 실데이터 유도값으로 표준 예시 생성.
                            # 유도값으로 실제 실행해 >0행일 때만 채택(재현 가능한 예시 보증).
                            derived, dwhy = derive_examples(d1, sql, unresolved)
                            if derived:
                                pairs = ", ".join(f":{k}={v}" for k, v in derived.items())
                                cand = f"-- {pairs}\n{sql}"
                                _, res_c, un_c = resolve_params(cand, hint_of(p))
                                if not un_c:
                                    try:
                                        nrows = len(d1.execute(
                                            substitute(executable_sql(cand), res_c).rstrip(";") + ";"))
                                    except Exception as exc:  # noqa: BLE001
                                        nrows, dwhy = -1, f"실행 실패 {type(exc).__name__}"
                                    if nrows > 0:
                                        new_sql, why = cand, "derived"
                                        cnt["derive_ok"] += 1
                                    else:
                                        report.append(f"DERIVE-FAIL {tag} — {dwhy} rows={nrows}")
                                        cnt["derive_fail"] += 1
                                else:
                                    cnt["derive_fail"] += 1
                            else:
                                report.append(f"DERIVE-FAIL {tag} — {dwhy}")
                                cnt["derive_fail"] += 1
                        if new_sql is None:
                            cnt["normalize_fail"] += 1
                            report.append(f"NORM-FAIL {tag} — {why}")
                        else:
                            cnt["normalize"] += 1

                    pd_ = p.get("param_defaults") or {}
                    add: dict[str, dict] = {}
                    src = new_sql or sql
                    _, res2, _ = resolve_params(src, hint_of(p))
                    for name, value in res2.items():
                        if name in pd_:
                            continue
                        if not DATE_VAL.match(str(value).strip()):
                            continue
                        default, kind = plan_rel_default(src, name, str(value).strip(), prod)
                        cnt[f"rel_{kind}"] += 1
                        if default:
                            add[name] = default
                    if add:
                        cnt["rel_added_patterns"] += 1

                    if args.apply and (new_sql is not None or add):
                        err = edit_pattern_block(Path(f), prod, pid, new_sql, add)
                        if err:
                            cnt["edit_fail"] += 1
                            report.append(f"EDIT-FAIL {tag} — {err}")
                        else:
                            cnt["edited"] += 1

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"==== pattern_hygiene ({mode}) ====")
    for k, v in sorted(cnt.items()):
        print(f"  {k:22} {v}")
    for r in report:
        print("  " + r)
    return 0


if __name__ == "__main__":
    sys.exit(main())
