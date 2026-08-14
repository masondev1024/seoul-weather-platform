# 공용 패턴 감사기(common/serving/pattern_audit.py) 레드팀·회귀 — Serving#217 배선분.
#
# 킷 초판(docs/domain-conversion-kit/pattern_audit.py)의 두 우회(괄호 친 테이블·CTE 스코프
# 오염)를 수정한 판이다 — 게이트웨이 src/pattern-audit.js(적대적 3라운드 통과본)와 케이스 동형.
# deny(강제)와 allowlist(경보) 두 수위를 함께 고정한다.
import pytest

from common.serving.pattern_audit import (
    audit_pattern_sql, build_allowlist, deny_findings, rewrite_audited_relation, table_refs, tokenize,
)

OK = build_allowlist(["gold_ok", "gold_sibling"])

ATTACKS = [
    ("콤마 암시적 크로스 조인", "SELECT * FROM gold_ok, _keys"),
    ("명시적 JOIN 내부표", "SELECT * FROM gold_ok c JOIN _keys k ON 1=1"),
    ("파생 테이블 뒤 콤마", "SELECT * FROM (SELECT 1) x, _keys"),
    ("스칼라 서브쿼리 내부표", "SELECT (SELECT key_hash FROM _keys LIMIT 1) AS h FROM gold_ok"),
    ("스키마 한정 main._keys", "SELECT * FROM main._keys"),
    ("pragma TVF", "SELECT * FROM gold_ok c, pragma_table_info('_keys') t"),
    ("스택 쿼리", "SELECT * FROM gold_ok; DROP TABLE _keys"),
    ("CTE 본문 내부표", "WITH x AS (SELECT * FROM _keys) SELECT * FROM x"),
    ("UNION 내부표", "SELECT a FROM gold_ok UNION ALL SELECT key_hash FROM _keys"),
    ("비-SELECT (DELETE)", "DELETE FROM gold_ok"),
    ("ATTACH", "ATTACH DATABASE 'x.db' AS y; SELECT 1"),
    ("인용 식별자 우회", 'SELECT * FROM gold_ok, "_keys"'),
    ("대소문자 섞기", "SeLeCt * FrOm gold_ok , _KEYS"),
    ("주석으로 감춘 콤마조인", "SELECT * FROM gold_ok /* x */, _keys"),
    # 킷 초판이 놓치던 괄호 친 테이블 계열 (SQLite 는 `(table)` 을 허용한다)
    ("괄호 친 테이블", "SELECT * FROM (_keys)"),
    ("공백 낀 괄호 테이블", "SELECT * FROM ( _keys )"),
    ("이중 괄호 테이블", "SELECT * FROM ((_keys))"),
    ("괄호 조인 피연산자", "SELECT * FROM gold_ok JOIN (_keys) ON 1=1"),
    ("콤마 + 괄호 테이블", "SELECT * FROM gold_ok, (_keys)"),
    ("괄호 친 콤마 리스트", "SELECT * FROM (gold_ok, _keys)"),
    ("괄호 테이블 UNION", "SELECT * FROM (_keys) UNION SELECT * FROM gold_ok"),
    ("파생테이블 안 괄호테이블", "SELECT * FROM (SELECT * FROM (_keys)) x"),
    ("IN 서브쿼리 괄호테이블", "SELECT * FROM gold_ok WHERE id IN (SELECT id FROM (_keys))"),
    ("ON 뒤 콤마 테이블", "SELECT * FROM gold_ok JOIN gold_ok ON 1=1, _keys"),
    ("sqlite_master 괄호", "SELECT * FROM (sqlite_master)"),
    # CTE 스코프 오염 (서브쿼리-로컬 CTE 로 진짜 내부표를 가림)
    ("CTE 스코프 오염 콤마", "SELECT * FROM (WITH _keys AS (SELECT 1 AS k) SELECT * FROM _keys) a, _keys b"),
    ("CTE 스코프 오염 sqlite_master", "SELECT * FROM (WITH sqlite_master AS (SELECT 1) SELECT * FROM sqlite_master) a, sqlite_master b"),
    ("WITH 접두 DELETE", "WITH x AS (SELECT 1) DELETE FROM _keys"),
    ("WITH 접두 REPLACE INTO", "WITH x AS (SELECT 1) REPLACE INTO _keys VALUES (1)"),
    ("마이그레이션 원장", "SELECT * FROM gold_ok, d1_migrations"),
    ("서비스 키 표", "SELECT * FROM gold_ok JOIN _service_keys ON 1=1"),
]


@pytest.mark.parametrize("name,sql", ATTACKS, ids=[a[0] for a in ATTACKS])
def test_deny_and_allowlist_both_reject(name, sql):
    assert deny_findings(sql), f"deny 가 놓침: {sql}"
    assert audit_pattern_sql(sql, OK), f"allowlist 감사가 놓침: {sql}"


LEGIT = [
    ("필터+정렬+LIMIT :n", "SELECT a FROM gold_ok WHERE g = :gu ORDER BY a DESC LIMIT :n"),
    ("집계", "SELECT gu, SUM(x) AS s FROM gold_ok GROUP BY gu ORDER BY s DESC"),
    ("json_each 배열 IN", "SELECT * FROM gold_ok WHERE g IN (SELECT value FROM json_each(:gus))"),
    ("자기 CTE", "WITH t AS (SELECT * FROM gold_ok) SELECT * FROM t"),
    ("자기 조인", "SELECT * FROM gold_ok c JOIN gold_ok d ON c.id = d.id"),
    ("형제 조인", "SELECT * FROM gold_ok JOIN gold_sibling ON gold_ok.k = gold_sibling.k"),
    ("형제 콤마 조인", "SELECT * FROM gold_ok, gold_sibling WHERE gold_ok.k = gold_sibling.k"),
    ("서브쿼리(자기)", "SELECT * FROM (SELECT a FROM gold_ok) x WHERE x.a > :min"),
    ("끝 세미콜론 하나", "SELECT * FROM gold_ok;"),
    # 토큰 전수 검사가 오탐하던 것들 — 문장 위치 동사 검사로 살아남아야 한다
    ("REPLACE() 스칼라 함수", "SELECT REPLACE(name, '_', ' ') AS clean FROM gold_ok"),
    ("replace 별칭", "SELECT x AS replace FROM gold_ok"),
    ("재귀 CTE", "WITH RECURSIVE cnt AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM cnt WHERE x < 9) SELECT o.id FROM gold_ok o JOIN cnt ON o.id = cnt.x"),
    ("컬럼목록 CTE", "WITH t(a, b) AS (SELECT g, COUNT(*) FROM gold_ok GROUP BY g) SELECT a, b FROM t"),
    ("pragma 로 시작하는 컬럼", "SELECT pragmatic FROM gold_ok"),
]


@pytest.mark.parametrize("name,sql", LEGIT, ids=[a[0] for a in LEGIT])
def test_legit_passes_both(name, sql):
    assert deny_findings(sql) == [], f"deny 오탐: {sql}"
    assert audit_pattern_sql(sql, OK) == [], f"allowlist 오탐: {sql}"


def test_deny_allows_foreign_serving_table_but_allowlist_flags_it():
    """수위 경계 — 타 제품/도메인 서빙표: deny(강제)는 통과, allowlist 감사(경보)만 잡는다.
    #217 결정의 P0-a/P0-b 구분이 게시기에서도 유지된다는 단언."""
    sql = "SELECT * FROM gold_ok JOIN gold_other_domain ON 1=1"
    assert deny_findings(sql) == []
    assert any("allowlist 밖" in f for f in audit_pattern_sql(sql, OK))


def test_limit_name_rule_is_allowlist_level_only():
    assert deny_findings("SELECT * FROM gold_ok LIMIT :maxrows") == []
    assert any("LIMIT" in f for f in audit_pattern_sql("SELECT * FROM gold_ok LIMIT :maxrows", OK))


def test_table_refs_walks_paren_lists():
    refs = table_refs(tokenize("SELECT * FROM (gold_ok, _keys) JOIN (_usage) ON 1=1"))
    assert set(refs) == {"gold_ok", "_keys", "_usage"}


def test_build_allowlist_lowercases_and_merges_cross_domain():
    allow = build_allowlist(["Gold_A"], ["commerce_gold.gold_license_dong_summary"])
    assert "gold_a" in allow and "commerce_gold.gold_license_dong_summary" in allow


def test_rewrite_audited_relation_rewrites_only_exact_from_or_join_identifier():
    sql = 'SELECT \'gold_risk\' AS literal FROM "gold_risk" r JOIN gold_other o ON 1=1'
    assert rewrite_audited_relation(sql, "gold_risk", "gold_risk__staging", build_allowlist(["gold_risk", "gold_other"])) == (
        'SELECT \'gold_risk\' AS literal FROM "gold_risk__staging" r JOIN gold_other o ON 1=1'
    )


def test_rewrite_audited_relation_fails_closed_for_unknown_or_unreferenced_sql():
    assert rewrite_audited_relation("SELECT * FROM unknown", "gold_risk", "gold_risk__staging", build_allowlist(["gold_risk"])) is None
    assert rewrite_audited_relation("SELECT 'gold_risk'", "gold_risk", "gold_risk__staging", build_allowlist(["gold_risk"])) is None
