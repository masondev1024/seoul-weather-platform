from datetime import date, datetime

import pytest

from tools import weather_coverage_comparison as comparison


BASE = date(2026, 11, 11)
ROLLOUT = datetime.fromisoformat("2026-11-18T03:23:00+00:00")
FINISHED = datetime.fromisoformat("2026-11-26T09:17:00+09:00")


def daily(day, cutoff):
    count = 400 if day < ROLLOUT.date() else 640
    return {
        "report_date_kst": day.isoformat(), "evaluation_as_of": cutoff.astimezone(comparison.timezone.utc).isoformat(),
        "measurement_window": {"reconstruction": "retained_objects_last_modified_and_manifest_completed_at_lte_cutoff"},
        "contract": {"source": "kma_vilage_fcst", "grid_count": 80, "issue_cycle_count": 8,
                     "freshness_error_minutes": 360},
        "checks": {
            "missing": {"status": "pass" if count == 640 else "fail", "reason_code": "coverage",
                        "expected_slots": 640, "observed_slots": count, "missing_slots": 640 - count},
            "freshness": {"status": "fail", "stale_grids": 80, "measured_grids": 80},
        },
    }


def test_fixed_cutoff_and_baseline_only():
    calls = []
    def read(day, cutoff):
        calls.append((day, cutoff.isoformat()))
        return daily(day, cutoff)
    result = comparison.build_comparison(BASE, read, now=ROLLOUT)
    assert len(calls) == 7
    assert calls[0] == (BASE, "2026-11-12T09:17:00+09:00")
    assert calls[-1][1] == "2026-11-18T09:17:00+09:00"
    assert result["baseline"]["aggregate"]["observed_slots"] == 2800
    assert result["baseline"]["aggregate"]["expected_slots"] == 4480
    assert result["baseline"]["days"][0]["freshness"]["stale_grids"] == 80
    assert result["followup"] is None
    assert result["delta"] is None


def test_rollout_day_excluded_and_no_reads_for_pending_days():
    calls = []
    def read(day, cutoff):
        calls.append(day)
        return daily(day, cutoff)
    before_cutoff = datetime.fromisoformat("2026-11-20T09:16:59+09:00")
    result = comparison.build_comparison(BASE, read, now=before_cutoff, rollout_at=ROLLOUT)
    assert len(calls) == 7
    followup = result["followup"]
    assert followup["days"][0]["date_kst"] == "2026-11-19"
    assert all(day["status"] == "pending_cutoff" and day["observed_slots"] is None for day in followup["days"])
    assert followup["aggregate"] is None and result["delta"] is None


def test_partial_evidence_does_not_become_zero_or_improvement():
    def read(day, cutoff):
        report = daily(day, cutoff)
        if day.day == 21:
            report["checks"]["missing"].update(status="unmeasured", reason_code="r2_evidence_unavailable",
                                               observed_slots=0, missing_slots=None)
        return report
    result = comparison.build_comparison(BASE, read, now=FINISHED, rollout_at=ROLLOUT)
    unavailable = result["followup"]["days"][2]
    assert unavailable["status"] == "unmeasured" and unavailable["observed_slots"] is None
    assert unavailable["reason"] == "r2_evidence_unavailable"
    assert result["followup"]["aggregate"] is None and result["delta"] is None


def test_complete_windows_and_frozen_baseline():
    baseline = comparison.build_comparison(BASE, daily, now=ROLLOUT)
    def read_after_only(day, cutoff):
        assert day >= date(2026, 11, 19)
        return daily(day, cutoff)
    combined = comparison.build_comparison(BASE, read_after_only, now=FINISHED,
                                            rollout_at=ROLLOUT, baseline_snapshot=baseline)
    assert combined["baseline"] == baseline["baseline"]
    assert combined["delta"] == {"observed_slots": 1680, "coverage_percentage_points": 37.5}
    baseline["method"]["cutoff_kst"] = "06:00"
    with pytest.raises(ValueError, match="baseline_method"):
        comparison.build_comparison(BASE, read_after_only, now=FINISHED,
                                    rollout_at=ROLLOUT, baseline_snapshot=baseline)


@pytest.mark.parametrize("rollout,after", [
    ("2026-11-18T09:16:59+09:00", None),
    ("2026-11-18T10:00:00+09:00", date(2026, 11, 18)),
    ("2026-11-18T10:00:00", None),
    ("2026-11-27T10:00:00+09:00", None),
])
def test_invalid_or_overlapping_rollout_rejected_before_read(rollout, after):
    def never(*args):
        pytest.fail("R2 accessed before validating window")
    with pytest.raises(ValueError):
        comparison.build_comparison(BASE, never, now=FINISHED, rollout_at=datetime.fromisoformat(rollout),
                                    after_start=after)


def test_existing_evidence_never_overwritten(tmp_path):
    output = tmp_path / "baseline.json"
    output.write_text("preserved", encoding="utf-8")
    with pytest.raises(FileExistsError):
        comparison.write_snapshot(output, {})
    assert output.read_text() == "preserved"


def test_cli_requests_historical_mode_and_frozen_pending_needs_no_r2(tmp_path, monkeypatch):
    import json

    class Clock(datetime):
        @classmethod
        def now(cls, zone):
            return ROLLOUT.astimezone(zone)
    monkeypatch.setattr(comparison, "datetime", Clock)
    for key in comparison.cloud.CREDENTIALS:
        monkeypatch.setenv(key, "fake")
    monkeypatch.setattr(comparison.cloud, "_client_from_env", lambda: object())

    calls = []
    def read(client, bucket, day, cutoff, *, historical_cutoff):
        assert historical_cutoff is True
        calls.append(day)
        return daily(day, cutoff)
    monkeypatch.setattr(comparison.cloud, "build_report", read)
    baseline = tmp_path / "baseline.json"
    assert comparison.main(["--baseline-start", BASE.isoformat(), "--output", str(baseline)]) == 0
    assert len(calls) == 7
    for key in comparison.cloud.CREDENTIALS:
        monkeypatch.delenv(key)
    after = tmp_path / "after.json"
    assert comparison.main(["--baseline-file", str(baseline), "--rollout-at", ROLLOUT.isoformat(),
                            "--output", str(after)]) == 4
    assert len(calls) == 7
    assert json.loads(after.read_bytes())["delta"] is None


@pytest.mark.parametrize("override", [
    {"report_date_kst": "1999-01-01"},
    {"evaluation_as_of": "2026-11-12T09:18:00+09:00"},
    {"evaluation_as_of": "2026-11-12T09:17:00"},
    {"measurement_window": {}},
])
def test_live_or_wrong_date_report_is_rejected(override):
    def read(day, cutoff):
        return {**daily(day, cutoff), **override}
    with pytest.raises(ValueError, match="cutoff_evidence_mismatch"):
        comparison.build_comparison(BASE, read, now=ROLLOUT)


def test_invalid_freshness_does_not_emit_nonfinite_values():
    def read(day, cutoff):
        result = daily(day, cutoff)
        result["checks"]["freshness"]["stale_grids"] = float("nan")
        return result
    with pytest.raises(ValueError, match="freshness_counts_invalid"):
        comparison.build_comparison(BASE, read, now=ROLLOUT)
