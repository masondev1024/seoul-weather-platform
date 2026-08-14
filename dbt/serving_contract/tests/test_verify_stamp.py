from datetime import datetime, timezone

from serving_contract.verify_stamp import resolve_params, substitute


def test_resolve_params_relative_defaults_override_stale_comment_at_kst_boundary():
    sql = "-- :from=2026-08-01, :to=2026-08-31\nSELECT a FROM t WHERE d >= :from AND d < :to"
    # 2026-08-10 15:00 UTC = 2026-08-11 00:00 KST.
    now = datetime(2026, 8, 10, 15, 0, tzinfo=timezone.utc)
    executable, resolved, unresolved = resolve_params(
        sql,
        param_defaults={
            "from": {"rel": "-1d", "as": "date"},
            "to": {"rel": "0d", "as": "date"},
        },
        now=now,
    )

    assert unresolved == []
    assert resolved == {"from": "'2026-08-10'", "to": "'2026-08-11'"}
    rendered = substitute(executable, resolved)
    assert "2026-08-01" not in rendered and "'2026-08-10'" in rendered


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
