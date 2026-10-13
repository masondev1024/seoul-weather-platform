from datetime import date, datetime, timezone
import pytest

from tools.weather_daily_quality import (
    ReportContract, build_report, load_grids, load_report_settings, render_markdown, save_report,
)


AS_OF = datetime(2026, 10, 13, 0, 17, tzinfo=timezone.utc)
DAY = date(2026, 10, 12)
CONTRACT = ReportContract(grids=(("g1", 60, 127), ("g2", 61, 127)), issue_times=("0200",))


def evidence(coverage=None, manifests=None, duplicates=None):
    return {
        "coverage": {"status": "measured", "reason_code": "query_completed", "rows": coverage or []},
        "manifests": {"status": "measured", "reason_code": "query_completed", "rows": manifests or []},
        "duplicates": {"status": "measured", "reason_code": "query_completed", "rows": duplicates or []},
    }


def published(run="r1", status="SUCCESS", event="2026-10-12T00:00:00+00:00"):
    return {"dag_run_id": run, "status": status, "is_publishable": status == "SUCCESS",
            "event_at": event, "expected_rows": 20, "actual_rows": 20,
            "expected_raw_objects": 2, "actual_raw_objects": 2}


def grid_row(place="g1", nx=60, run="r1", day="20261012", cycle="0200"):
    return {"base_date": day, "base_time": cycle, "place_id": place, "nx": nx, "ny": 127,
            "dag_run_id": run, "row_count": 20, "category_count": 14, "invalid_rows": 0,
            "last_collected_at_utc": "2026-10-12T00:00:00+00:00"}


def test_outage_is_unmeasured_never_zero_or_pass():
    absent = {key: {"status": "unmeasured", "reason_code": "trino_unavailable", "rows": []}
              for key in ("coverage", "manifests", "duplicates")}
    report = build_report(DAY, AS_OF, CONTRACT, absent, runtime={"docker": "unavailable"})
    assert report["status"] == "unmeasured"
    assert report["checks"]["missing"]["missing_slots"] is None
    assert report["checks"]["duplicates"]["excess_rows"] is None


def test_partial_collection_uses_contract_denominator_and_exposes_gaps():
    report = build_report(DAY, AS_OF, CONTRACT, evidence([grid_row()], [published()]))
    missing = report["checks"]["missing"]
    assert missing["expected_slots"] == 2
    assert missing["observed_slots"] == 1
    assert missing["missing_slots"] == 1
    assert missing["coverage_percent"] == 50.0
    assert missing["scheduled_population_verified"] is False
    assert missing["missing_slot_keys"] == ["20261012/0200/g2/61/127"]


def test_multiple_success_runs_are_one_observed_slot_not_duplicates():
    report = build_report(DAY, AS_OF, CONTRACT,
                          evidence([grid_row(), grid_row(run="r2")], [published(), published("r2")],
                                   [{"total_rows": 40, "duplicate_groups": 0,
                                     "duplicate_excess_rows": 0, "invalid_grain_rows": 0}]))
    assert report["checks"]["missing"]["observed_slots"] == 1
    assert report["checks"]["duplicates"]["status"] == "pass"


def test_coalesced_and_failed_runs_do_not_count_as_publishable():
    report = build_report(DAY, AS_OF, CONTRACT,
                          evidence([grid_row()], [published(), published(status="COALESCED", event="2026-10-12T01:00:00+00:00")]))
    assert report["checks"]["missing"]["observed_slots"] == 0
    assert report["checks"]["missing"]["status"] == "unmeasured"
    assert report["manifest_summary"]["coalesced_runs"] == 1


def test_manifest_timestamp_tie_is_not_arbitrarily_accepted():
    report = build_report(DAY, AS_OF, CONTRACT,
                          evidence([grid_row()], [published(), published(status="FAILED")]))
    assert report["checks"]["missing"]["status"] == "unmeasured"
    assert report["checks"]["missing"]["reason_code"] == "ambiguous_manifest_state"


def test_equal_success_timestamps_with_conflicting_counts_are_ambiguous():
    alternate = published()
    alternate["actual_rows"] = 19
    report = build_report(DAY, AS_OF, CONTRACT,
                          evidence([grid_row()], [published(), alternate]))
    assert report["checks"]["missing"]["reason_code"] == "ambiguous_manifest_state"


def test_old_forecast_recollected_now_is_still_stale():
    row = grid_row()
    row["last_collected_at_utc"] = AS_OF.isoformat()
    report = build_report(DAY, AS_OF, CONTRACT, evidence([row], [published()]))
    freshness = report["checks"]["freshness"]
    assert freshness["status"] == "fail"
    assert freshness["latest_collection_age_minutes"] == 0
    assert freshness["latest_issue_age_minutes"] > 360
    assert freshness["unmeasured_grids"] == 1


def test_duplicate_and_null_keys_fail_and_zero_row_sample_is_unmeasured():
    report = build_report(DAY, AS_OF, CONTRACT, evidence(duplicates=[{
        "total_rows": 20, "duplicate_groups": 1, "duplicate_excess_rows": 3, "invalid_grain_rows": 1}]))
    assert report["checks"]["duplicates"]["status"] == "fail"
    assert report["checks"]["duplicates"]["excess_rows"] == 3
    empty = build_report(DAY, AS_OF, CONTRACT, evidence(duplicates=[{
        "total_rows": 0, "duplicate_groups": 0, "duplicate_excess_rows": 0, "invalid_grain_rows": 0}]))
    assert empty["checks"]["duplicates"]["status"] == "unmeasured"


def test_partial_phase_failure_does_not_erase_other_checks():
    data = evidence([grid_row()], [published()])
    data["duplicates"] = {"status": "unmeasured", "reason_code": "query_failed", "rows": []}
    report = build_report(DAY, AS_OF, CONTRACT, data)
    assert report["checks"]["missing"]["missing_slots"] == 1
    assert report["checks"]["duplicates"]["status"] == "unmeasured"


def test_save_is_idempotent_and_retains_revisions(tmp_path):
    report = build_report(DAY, AS_OF, CONTRACT, evidence())
    first = save_report(report, tmp_path)
    again = save_report(report, tmp_path)
    assert first == again
    assert len(list((tmp_path / "revisions" / DAY.isoformat()).glob("*.json"))) == 1
    report["runtime"] = {"docker": "running"}
    save_report(report, tmp_path)
    assert len(list((tmp_path / "revisions" / DAY.isoformat()).glob("*.json"))) == 2
    assert "미측정" in render_markdown(report)
    assert (tmp_path / f"{DAY}.json").stat().st_mode & 0o777 == 0o600


def test_older_evaluation_is_archived_without_replacing_latest(tmp_path):
    import json
    recent = build_report(DAY, AS_OF, CONTRACT, evidence())
    save_report(recent, tmp_path)
    older = build_report(DAY, AS_OF.replace(hour=0, minute=0), CONTRACT, evidence())
    save_report(older, tmp_path)
    assert json.loads((tmp_path / f"{DAY}.json").read_text())["evaluation_as_of"] == AS_OF.isoformat()
    assert len(list((tmp_path / "revisions" / DAY.isoformat()).glob("*.json"))) == 2


def test_report_contract_rejects_unsafe_or_ambiguous_inputs(tmp_path):
    with pytest.raises(ValueError):
        ReportContract(grids=(("g1", 60, 127), ("g2", 60, 127)))
    with pytest.raises(ValueError):
        build_report(DAY, AS_OF.replace(tzinfo=None), CONTRACT, evidence())
    with pytest.raises(ValueError):
        build_report(DAY, datetime(2026, 10, 11, tzinfo=timezone.utc), CONTRACT, evidence())
    path = tmp_path / "grids.csv"
    path.write_text("place_id,nx,ny,coverage_scope\ng1,60,127,seoul_bbox\n")
    assert load_grids(path, expected_count=1) == (("g1", 60, 127),)
    with pytest.raises(ValueError):
        load_grids(path)


def test_settings_loads_actual_namespace_without_returning_secrets(tmp_path):
    path = tmp_path / "settings.env"
    path.write_text("R2_SECRET_ACCESS_KEY=sensitive-value\nASK_SEOUL_SCHEMA='weather_traffic_bronze'\nTRINO_ICEBERG_CATALOG=iceberg # comment\nBAD_SECRET='unterminated\n")
    path.chmod(0o600)
    assert load_report_settings(path) == {"ASK_SEOUL_SCHEMA": "weather_traffic_bronze", "TRINO_ICEBERG_CATALOG": "iceberg"}
    path.chmod(0o644)
    with pytest.raises(ValueError):
        load_report_settings(path)


def test_complete_forecast_population_and_fresh_issue_pass():
    as_of = datetime(2026, 10, 12, 18, 17, tzinfo=timezone.utc)
    rows = [grid_row(), grid_row("g2", 61)]
    current = [grid_row(day="20261013"), grid_row("g2", 61, day="20261013")]
    for row in current:
        row["last_collected_at_utc"] = "2026-10-12T17:30:00+00:00"
    data = evidence(rows + current, [published()], [{"total_rows": 40, "duplicate_groups": 0,
                     "duplicate_excess_rows": 0, "invalid_grain_rows": 0}])
    report = build_report(DAY, as_of, CONTRACT, data)
    assert report["status"] == "pass"
    assert report["checks"]["missing"]["missing_slots"] == 0
    assert report["checks"]["freshness"]["unmeasured_grids"] == 0


def test_cli_records_guard_block_without_query_or_restarting_services(tmp_path, monkeypatch):
    import json
    from tools import weather_daily_quality as reporting
    from tools import weather_daily_quality_evidence as reader
    monkeypatch.setattr(reporting, "runtime_status", lambda: {"docker": "unavailable", "host_guard_latched": True})
    monkeypatch.setattr(reader, "collect_evidence", lambda *args: pytest.fail("latched guard must prevent queries"))
    rc = reporting.main(["--report-date", str(DAY), "--as-of", AS_OF.isoformat(),
                         "--env-file", str(tmp_path / "absent"), "--catalog", "iceberg", "--schema", "test_bronze",
                         "--output-dir", str(tmp_path / "reports")])
    assert rc == 2
    report = json.loads((tmp_path / "reports" / f"{DAY}.json").read_text())
    assert report["checks"]["missing"]["reason_code"] == "host_guard_latched"
    assert report["measurement_window"]["bronze_relation"] == "iceberg.test_bronze.bronze_kma_vilage_fcst"
