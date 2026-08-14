# common/serving/pattern_verify.py 단위 — export 시점 검증 스탬프 (Serving#217)
from datetime import datetime, timezone

from common.serving.pattern_verify import resolve_params, verify_and_stamp


def _row(pid, sql, **kw):
    r = {"pattern_id": pid, "sql": sql, "question_ko": "", "axes": "", "insight_sample_ko": ""}
    r.update(kw)
    return r


def test_resolve_params_from_comment():
    sql = "-- :gu='강남구', :n=10\nSELECT * FROM t WHERE gu = :gu ORDER BY x LIMIT :n"
    sub, resolved, unresolved = resolve_params(sql)
    assert unresolved == []
    assert resolved["gu"] == "'강남구'" and resolved["n"] == "10"
    assert ":gu" not in sub and ":n" not in sub and "'강남구'" in sub


# 🔴 아래 넷은 **운영에서 실제로 새어 나간** 모양이다(2026-08-10 카탈로그 실측).
#    culture 미검증 19건 중 16건, 그리고 그날 스탬프된 173건 중 87건이 이 경로였다.
def test_resolve_params_does_not_steal_next_params_number():
    # 예전 규칙은 `=종로구, :from=` 를 건너뛰고 **다음 파라미터의 2026** 을 :gu 에 물렸다.
    sql = "-- :gu=종로구, :from=2026-07-01, :to=2026-08-31\nSELECT a FROM t WHERE gu = :gu AND d BETWEEN :from AND :to"
    _, resolved, unresolved = resolve_params(sql)
    assert unresolved == []
    assert resolved["gu"] == "'종로구'"
    assert resolved["from"] == "'2026-07-01'" and resolved["to"] == "'2026-08-31'"


def test_resolve_params_keeps_date_whole():
    # 날짜의 앞 네 자리만 삼키면 `BETWEEN 2026 AND 2026` 이 되어 SQLite 가 조용히 0행을 준다.
    sub, _, _ = resolve_params("-- :from=2026-08-01\nSELECT a FROM t WHERE d >= :from")
    assert "'2026-08-01'" in sub and "2026 " not in sub


def test_resolve_params_number_followed_by_sentence():
    # 🔴 운영 실측(2026-08-11, commerce gold_env_facility_operation 5건): 값 뒤에 설명문이
    #    붙는 저작 관행이 있다. `5.` 의 마침표를 거부하면 ④가 문장 전체를 값으로 삼켜
    #    `LIMIT '5. facility_rows는…'` 이 되고, D1 이 datatype mismatch 로 죽는다.
    sql = "-- :n = 5. facility_rows는 업소당 2관측이라 /2로 환산\nSELECT gu FROM t LIMIT :n"
    sub, resolved, unresolved = resolve_params(sql)
    assert unresolved == []
    assert resolved["n"] == "5", "설명문이 값으로 딸려 들어왔다"
    assert sub.rstrip().endswith("LIMIT 5")


def test_resolve_params_decimal_still_whole():
    # 마침표 뒤가 숫자면 소수다 — 잘라내면 안 된다.
    _, resolved, _ = resolve_params("-- :ratio=3.5\nSELECT a FROM t WHERE r > :ratio")
    assert resolved["ratio"] == "3.5"


def test_resolve_params_unquoted_sentinel():
    _, resolved, _ = resolve_params("-- :gu=ALL, :n=7\nSELECT a FROM t WHERE (:gu = 'ALL' OR gu = :gu) LIMIT :n")
    assert resolved["gu"] == "'ALL'" and resolved["n"] == "7"


def test_resolve_params_requires_equals_sign():
    # 값이 없는 파라미터는 **미해결로 남는 게 맞다** — 같은 줄 다른 숫자를 주워 오면 안 된다.
    _, _, unresolved = resolve_params("SELECT a FROM t WHERE gu = :gu ORDER BY x LIMIT 10")
    assert "gu" in unresolved


def test_resolve_params_unresolved_when_no_example():
    _, _, unresolved = resolve_params("SELECT * FROM t WHERE g = :gu")
    assert "gu" in unresolved


def test_resolve_params_expands_array_for_in_list():
    # P3 전개형(IN (:gus)) — JSON 배열 예시를 IN 리스트로 전개(게이트웨이 ?,?,? 와 동형).
    # JSON 문자열 그대로 넣으면 단일 리터럴 비교가 되어 조용히 0행이 난다.
    sql = '-- :gus=["강남구","서초구"]\nSELECT g FROM t WHERE gu IN (:gus)'
    sub, _, unresolved = resolve_params(sql)
    assert unresolved == []
    assert "IN ('강남구', '서초구')" in sub


def test_resolve_params_keeps_json_string_for_json_each():
    # json_each(:gus) 형은 JSON 문자열 그대로 bind — 전개 대상이 아니다
    sql = '-- :gus=["a","b"]\nSELECT g FROM t WHERE gu IN (SELECT value FROM json_each(:gus))'
    sub, _, unresolved = resolve_params(sql)
    assert unresolved == []
    assert "json_each('[\"a\",\"b\"]')" in sub


def test_resolve_params_relative_defaults_override_stale_comment_at_kst_boundary():
    sql = "-- :from=2026-08-01, :to=2026-08-31\nSELECT a FROM t WHERE d >= :from AND d < :to"
    # 2026-08-10 15:00 UTC = 2026-08-11 00:00 KST. 기준 시각은 KST 계약으로 해석한다.
    now = datetime(2026, 8, 10, 15, 0, tzinfo=timezone.utc)
    sub, resolved, unresolved = resolve_params(
        sql,
        param_defaults={
            "from": {"rel": "-1d", "as": "date"},
            "to": {"rel": "0d", "as": "date"},
        },
        now=now,
    )

    assert unresolved == []
    assert resolved == {"from": "'2026-08-10'", "to": "'2026-08-11'"}
    assert "2026-08-01" not in sub and "'2026-08-10'" in sub and "'2026-08-11'" in sub


def test_resolve_params_relative_defaults_support_calendar_types():
    sql = "SELECT a FROM t WHERE ym = :month AND y = :year AND at < :at"
    now = datetime(2026, 8, 10, 15, 0, tzinfo=timezone.utc)
    _, resolved, unresolved = resolve_params(
        sql,
        param_defaults={
            "month": {"rel": "-1M", "as": "ym"},
            "year": {"rel": "-1y", "as": "year"},
            "at": {"rel": "0d", "as": "datetime"},
        },
        now=now,
    )

    assert unresolved == []
    assert resolved == {
        "month": "'2026-07'",
        "year": "'2025'",
        "at": "'2026-08-11 00:00:00'",
    }


def test_resolve_params_relative_datetime_nonzero_offset_uses_midnight():
    sql = "SELECT a FROM t WHERE at < :at"
    now = datetime(2026, 8, 10, 15, 30, tzinfo=timezone.utc)
    _, resolved, unresolved = resolve_params(
        sql,
        param_defaults={"at": {"rel": "-1d", "as": "datetime"}},
        now=now,
    )

    assert unresolved == []
    assert resolved["at"] == "'2026-08-10 00:00:00'"


def test_resolve_params_invalid_relative_default_stays_unresolved():
    sql = "-- :from=2026-08-01\nSELECT a FROM t WHERE d >= :from"
    _, resolved, unresolved = resolve_params(
        sql,
        param_defaults={"from": {"rel": "yesterday", "as": "date"}},
    )

    assert "from" not in resolved
    assert unresolved == ["from"]

    _, resolved, unresolved = resolve_params(
        sql,
        param_defaults={"from": {"rel": "0d", "as": []}},
    )
    assert "from" not in resolved
    assert unresolved == ["from"]


def test_verify_stamps_only_unverified_and_on_success():
    now = "2026-08-10T00:00:00Z"
    rows = [
        _row("draft", "-- :n=5\nSELECT a FROM t LIMIT :n"),                       # 미검증 → 스탬프
        _row("already", "SELECT 1", verified_at="2026-01-01T00:00:00Z", verified_rows=9),  # 검증됨 → 무접촉
    ]
    calls = []
    rep = verify_and_stamp(rows, run_sql=lambda s: (calls.append(s), [{"a": 1}, {"a": 2}])[1],
                           publication_id="pub1", now_iso=now)
    assert rep["verified"] == ["draft"]
    d = rows[0]
    assert d["verified_at"] == now and d["verified_rows"] == 2 and d["verified_publication_id"] == "pub1"
    # 이미 검증된 건 그대로 · 실행도 안 함
    assert rows[1]["verified_at"] == "2026-01-01T00:00:00Z" and rows[1]["verified_rows"] == 9
    assert all(":n" not in c for c in calls)   # 예시값으로 치환돼 실행됨


def test_verify_uses_relative_defaults_for_each_pattern():
    now = datetime(2026, 8, 10, 15, 0, tzinfo=timezone.utc)
    rows = [_row("p", "-- :at='2000-01-01'\nSELECT a FROM t WHERE d = :at")]
    calls = []

    rep = verify_and_stamp(
        rows,
        run_sql=lambda sql: (calls.append(sql), [{"a": 1}])[1],
        publication_id="pub1",
        now_iso="2026-08-11T00:00:00Z",
        now=now,
        param_defaults_by_pattern={"p": {"at": {"rel": "0d", "as": "date"}}},
    )

    assert rep["verified"] == ["p"]
    assert len(calls) == 1
    assert "'2026-08-11'" in calls[0] and "2000-01-01" not in calls[0]


def test_verify_skips_unresolved_params():
    rows = [_row("p", "SELECT a FROM t WHERE g = :gu")]   # 예시값 없음
    rep = verify_and_stamp(rows, run_sql=lambda s: [{"a": 1}], publication_id="pub1")
    assert rep["verified"] == [] and rows[0].get("verified_at") is None
    assert rep["skipped"] and "미해결" in rep["skipped"][0][1]


def test_verify_skips_zero_rows_unless_allow_empty():
    rows = [_row("p", "-- :n=5\nSELECT a FROM t LIMIT :n")]
    rep = verify_and_stamp(rows, run_sql=lambda s: [], publication_id="pub1")
    assert rep["verified"] == [] and rows[0].get("verified_at") is None
    # allow_empty=True 면 0행도 검증
    rows2 = [_row("p", "-- :n=5\nSELECT a FROM t LIMIT :n", allow_empty=True)]
    rep2 = verify_and_stamp(rows2, run_sql=lambda s: [], publication_id="pub1")
    assert rep2["verified"] == ["p"] and rows2[0]["verified_rows"] == 0


def test_verify_records_execution_failure_without_stamping():
    def boom(_sql):
        raise RuntimeError("no such column")
    rows = [_row("p", "-- :n=5\nSELECT bad FROM t LIMIT :n")]
    rep = verify_and_stamp(rows, run_sql=boom, publication_id="pub1")
    assert rep["failed"] and rep["failed"][0][0] == "p"
    assert rows[0].get("verified_at") is None      # 실패는 미검증으로 남긴다(안전망)


def test_verify_skips_non_select():
    rows = [_row("p", "-- :n=5\nDELETE FROM t")]
    rep = verify_and_stamp(rows, run_sql=lambda s: [{"a": 1}], publication_id="pub1")
    assert rep["verified"] == [] and rows[0].get("verified_at") is None


def test_verify_needs_publication_id():
    rows = [_row("p", "-- :n=5\nSELECT a FROM t LIMIT :n")]
    rep = verify_and_stamp(rows, run_sql=lambda s: [{"a": 1}], publication_id="")
    assert rep["skipped"] and rows[0].get("verified_at") is None
