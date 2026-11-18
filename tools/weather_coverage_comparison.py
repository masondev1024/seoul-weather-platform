"""Compare two seven-day KST issue cohorts using immutable landing evidence at D+1 09:17."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from tools import weather_daily_quality_cloud as cloud
from tools.weather_daily_quality import DEFAULT_GRID_CSV, DEFAULT_ISSUE_TIMES, KST, load_grids

DAYS = 7
DAY = timedelta(days=1)
CUTOFF = time(9, 17)
EXPECTED = 640
SCHEMA = "weather-coverage-comparison/v1"
RECONSTRUCTION = "retained_objects_last_modified_and_manifest_completed_at_lte_cutoff"


def cutoff_for(day: date) -> datetime:
    return datetime.combine(day + DAY, CUTOFF, tzinfo=KST)


def _method() -> dict:
    grids = sorted((nx, ny) for _, nx, ny in load_grids(DEFAULT_GRID_CSV))
    # Pin the lookup population and reader revision with a frozen baseline.
    return {"source": cloud.SOURCE, "timezone": "Asia/Seoul", "cutoff_day_offset": 1,
            "cutoff_kst": CUTOFF.strftime("%H:%M"), "expected_slots_daily": EXPECTED,
            "issue_times_kst": list(DEFAULT_ISSUE_TIMES), "grid_count": len(grids),
            "grid_sha256": hashlib.sha256(json.dumps(grids).encode()).hexdigest(),
            "reader_sha256": hashlib.sha256(Path(cloud.__file__).read_bytes()).hexdigest(),
            "freshness_error_minutes": 360}


def _freshness(check):
    if check["status"] == "unmeasured":
        return None
    measured, stale = check["measured_grids"], check["stale_grids"]
    if (check["status"] not in {"pass", "fail"} or type(measured) is not int or type(stale) is not int
            or not 0 <= stale <= measured <= 80):
        raise ValueError("freshness_counts_invalid")
    return {key: check[key] for key in ("status", "measured_grids", "stale_grids")}


def _day(day, now, read_report):
    cutoff = cutoff_for(day)
    row = {"date_kst": day.isoformat(), "cutoff_kst": cutoff.isoformat(), "expected_slots": EXPECTED,
           "status": "pending_cutoff", "observed_slots": None, "missing_slots": None, "freshness": None}
    if now < cutoff:
        return row
    report = read_report(day, cutoff)
    if (report.get("report_date_kst") != day.isoformat()
            or datetime.fromisoformat(report["evaluation_as_of"]) != cutoff
            or report["measurement_window"].get("reconstruction") != RECONSTRUCTION):
        raise ValueError("cutoff_evidence_mismatch")
    contract = report["contract"]
    missing = report["checks"]["missing"]
    if (contract["source"] != cloud.SOURCE or contract["grid_count"] != 80 or contract["issue_cycle_count"] != 8
            or contract["freshness_error_minutes"] != 360 or missing["expected_slots"] != EXPECTED):
        raise ValueError("coverage_contract_changed")
    if missing["status"] == "unmeasured":
        row.update(status="unmeasured", reason=missing["reason_code"])
        return row
    observed = missing["observed_slots"]
    if (missing["status"] not in {"pass", "fail"} or type(observed) is not int or not 0 <= observed <= EXPECTED
            or missing["missing_slots"] != EXPECTED - observed):
        raise ValueError("coverage_counts_invalid")
    row["freshness"] = _freshness(report["checks"]["freshness"])
    row.update(status="measured", observed_slots=observed, missing_slots=EXPECTED - observed)
    return row


def _window(start, rows):
    aggregate = None
    if all(row["status"] == "measured" for row in rows):
        observed = sum(row["observed_slots"] for row in rows)
        expected = DAYS * EXPECTED
        aggregate = {"expected_slots": expected, "observed_slots": observed, "missing_slots": expected - observed,
                     "coverage_percent": round(100 * observed / expected, 4)}
    return {"start_kst": start.isoformat(), "end_kst": (start + (DAYS - 1) * DAY).isoformat(),
            "days": rows, "aggregate": aggregate}


def _frozen_baseline(snapshot, start, method):
    if snapshot.get("schema_version") != SCHEMA or snapshot.get("method") != method:
        raise ValueError("baseline_method_mismatch")
    window = snapshot["baseline"]
    rows = window["days"]
    if len(rows) != DAYS:
        raise ValueError("baseline_requires_seven_measured_days")
    for index, row in enumerate(rows):
        day = start + index * DAY
        count = row["observed_slots"]
        if row["freshness"] is not None:
            _freshness(row["freshness"])
        if (row["date_kst"] != day.isoformat() or row["cutoff_kst"] != cutoff_for(day).isoformat()
                or row["expected_slots"] != EXPECTED or row["status"] != "measured"
                or type(count) is not int or not 0 <= count <= EXPECTED or row["missing_slots"] != EXPECTED - count):
            raise ValueError("baseline_requires_seven_measured_days")
    if window != _window(start, rows):
        raise ValueError("baseline_aggregate_invalid")
    return window


def build_comparison(baseline_start, read_report, *, now, rollout_at=None, after_start=None, baseline_snapshot=None):
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("timezone_required")
    baseline_cutoff = cutoff_for(baseline_start + (DAYS - 1) * DAY)
    if baseline_cutoff > now:
        raise ValueError("baseline_not_mature")
    if rollout_at is not None:
        if rollout_at.tzinfo is None or rollout_at.utcoffset() is None:
            raise ValueError("rollout_timezone_required")
        if rollout_at < baseline_cutoff or rollout_at > now:
            raise ValueError("rollout_must_follow_baseline_cutoff_and_be_in_the_past")
        earliest = rollout_at.astimezone(KST).date() + DAY
        after_start = after_start or earliest
        if after_start < earliest:
            raise ValueError("followup_must_exclude_rollout_day")
    elif after_start is not None:
        raise ValueError("rollout_required_for_followup")

    method = _method()
    def measure(start):
        return _window(start, [_day(start + index * DAY, now, read_report) for index in range(DAYS)])

    if baseline_snapshot is None:
        baseline = measure(baseline_start)
    else:
        baseline = _frozen_baseline(baseline_snapshot, baseline_start, method)
    followup = measure(after_start) if after_start else None
    delta = None
    if followup and baseline["aggregate"] and followup["aggregate"]:
        change = followup["aggregate"]["observed_slots"] - baseline["aggregate"]["observed_slots"]
        delta = {"observed_slots": change, "coverage_percentage_points": round(100 * change / (DAYS * EXPECTED), 4)}
    return {"schema_version": SCHEMA, "generated_at": now.astimezone(timezone.utc).isoformat(), "method": method,
            "rollout_at": rollout_at.isoformat() if rollout_at else None,
            "baseline": baseline, "followup": followup, "delta": delta}


def write_snapshot(path: Path, snapshot: dict) -> None:
    """Publish a complete JSON file locally, without replacing previous evidence."""
    payload = json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n"
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        try:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
            os.link(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    origin = parser.add_mutually_exclusive_group(required=True)
    origin.add_argument("--baseline-start", type=date.fromisoformat)
    origin.add_argument("--baseline-file", type=Path, help="Use the original seven-day snapshot without re-reading baseline storage")
    parser.add_argument("--rollout-at", type=datetime.fromisoformat, help="Actual activation timestamp, including timezone")
    parser.add_argument("--after-start", type=date.fromisoformat, help="Optional later start, KST")
    parser.add_argument("--output", type=Path, required=True, help="New local JSON path")
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error("output_already_exists")
    try:
        snapshot = json.loads(args.baseline_file.read_text(encoding="utf-8")) if args.baseline_file else None
        start = date.fromisoformat(snapshot["baseline"]["start_kst"]) if snapshot is not None else args.baseline_start
        client = None

        def read(day, cutoff):
            nonlocal client
            if client is None:
                if not all(os.environ.get(key) for key in cloud.CREDENTIALS):
                    parser.error("read_only_storage_credentials_required")
                client = cloud._client_from_env()
            return cloud.build_report(client, os.environ["R2_BUCKET_NAME"], day, cutoff, historical_cutoff=True)
        result = build_comparison(start, read, now=datetime.now(timezone.utc), rollout_at=args.rollout_at,
                                  after_start=args.after_start, baseline_snapshot=snapshot)
        write_snapshot(args.output, result)
    except (KeyError, TypeError, OSError, ValueError):
        parser.error("invalid_snapshot_window_or_output")
    states = {row["status"] for window in (result["baseline"], result["followup"]) if window
              for row in window["days"]}
    if "unmeasured" in states:
        return 1
    return 4 if "pending_cutoff" in states else 0


if __name__ == "__main__":
    raise SystemExit(main())
