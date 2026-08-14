# verify_stamp.resolve_params 단위 — ASAC-DAG pattern_verify(#756/#763)와 규약 잠금 회귀
from serving_contract.verify_stamp import resolve_params, substitute


def _sub(sql, hint=""):
    body, res, un = resolve_params(sql, hint)
    return substitute(body, res), res, un


def test_number_then_prose_takes_number_only():
    # #763: `-- :n=10 — 설명…` 에서 설명문을 값으로 삼키면 LIMIT '10 — 설명…' → datatype mismatch
    sub, res, un = _sub("-- :n=10 — 정확 일치 대신 substr 사용\nSELECT a FROM t LIMIT :n")
    assert un == []
    assert res["n"] == "10"
    assert sub.endswith("LIMIT 10")


def test_number_sentence_period_vs_decimal():
    # `5. 문장` 은 5, `3.5` 는 소수 그대로
    _, res, _ = _sub("-- :n = 5. ASC/DESC 반전 가능\nSELECT a FROM t LIMIT :n")
    assert res["n"] == "5"
    _, res2, _ = _sub("-- :ratio=3.5\nSELECT a FROM t WHERE r >= :ratio")
    assert res2["ratio"] == "3.5"


def test_date_prefix_not_swallowed_as_number():
    # #756: 비따옴표 날짜의 앞 네 자리를 숫자로 삼키지 않는다
    _, res, _ = _sub("-- :from=2026-07-01\nSELECT a FROM t WHERE d >= :from")
    assert res["from"] == "'2026-07-01'"


def test_unquoted_sentinel_and_next_number():
    _, res, un = _sub("-- :gu=ALL, :n=7\nSELECT a FROM t WHERE (:gu='ALL' OR gu=:gu) LIMIT :n")
    assert un == []
    assert res["gu"] == "'ALL'" and res["n"] == "7"


def test_in_list_array_expansion_and_json_each_kept():
    sub, _, _ = _sub('-- :gus=["강남구","서초구"]\nSELECT g FROM t WHERE gu IN (:gus)')
    assert "IN ('강남구', '서초구')" in sub
    sub2, _, _ = _sub('-- :gus=["a","b"]\nSELECT g FROM t WHERE gu IN (SELECT value FROM json_each(:gus))')
    assert "json_each('[\"a\",\"b\"]')" in sub2
