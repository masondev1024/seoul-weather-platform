from serving_contract.pattern_hygiene import plan_rel_default


def test_current_outlook_future_lower_bound_uses_kst_day_boundary():
    sql = "SELECT * FROM t WHERE forecast_at >= :from_at AND forecast_at < :to_at"

    current_default, current_kind = plan_rel_default(
        sql,
        "from_at",
        "'2026-08-14 02:00:00'",
        "weather_place_current_outlook",
    )
    other_default, other_kind = plan_rel_default(
        sql,
        "from_at",
        "'2026-08-14 02:00:00'",
        "weather_place_precipitation_window",
    )
    grid_default, grid_kind = plan_rel_default(
        sql,
        "from_at",
        "'2026-08-14 02:00:00'",
        "weather_grid_current_outlook",
    )

    assert current_default == {"rel": "0d", "as": "date"}
    assert current_kind == "range_future_current_snapshot"
    assert other_default == {"rel": "0d", "as": "datetime"}
    assert other_kind == "range_future"
    assert grid_default == {"rel": "0d", "as": "datetime"}
    assert grid_kind == "range_future"
