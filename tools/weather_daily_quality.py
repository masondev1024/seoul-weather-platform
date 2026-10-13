"""Daily, read-only quality evidence for the KMA short-range forecast collector."""
from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import json
import logging
import os
import re
import shlex
import stat
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


KST = ZoneInfo("Asia/Seoul")
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GRID_CSV = ROOT / "dags/domains/weather/config/seoul_kma_grids.csv"
DEFAULT_ISSUE_TIMES = ("0200", "0500", "0800", "1100", "1400", "1700", "2000", "2300")
LOGGER = logging.getLogger(__name__)
REPORT_SETTING_KEYS = frozenset({"TRINO_ICEBERG_CATALOG", "ASK_SEOUL_SCHEMA",
                                "ASK_SEOUL_REPORT_WEATHER_FRESHNESS_WARN_MINUTES",
                                "ASK_SEOUL_REPORT_WEATHER_FRESHNESS_ERROR_MINUTES"})


def load_report_settings(path: Path) -> dict[str, str]:
    """Read only non-secret report settings; never source a credential file as code."""
    if not path.exists():
        return {}
    if not path.is_file() or stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise ValueError("settings_file_permissions_invalid")
    settings = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.strip().removeprefix("export ").partition("=")
        if separator and key.strip() in REPORT_SETTING_KEYS:
            tokens = shlex.split(value, comments=True)
            if len(tokens) != 1:
                raise ValueError("report_setting_invalid")
            settings[key.strip()] = tokens[0]
    return settings


@dataclass(frozen=True)
class ReportContract:
    grids: tuple[tuple[str, int, int], ...]
    issue_times: tuple[str, ...] = DEFAULT_ISSUE_TIMES
    required_category_count: int = 14
    freshness_warn_minutes: int = 240
    freshness_error_minutes: int = 360

    def __post_init__(self):
        if not self.grids or len({(nx, ny) for _, nx, ny in self.grids}) != len(self.grids):
            raise ValueError("invalid_grid_population")
        if any(not re.fullmatch(r"[A-Za-z0-9_-]+", place) or nx < 1 or ny < 1
               for place, nx, ny in self.grids):
            raise ValueError("invalid_grid_identity")
        if not self.issue_times or len(set(self.issue_times)) != len(self.issue_times):
            raise ValueError("invalid_issue_times")
        for cycle in self.issue_times:
            if cycle not in DEFAULT_ISSUE_TIMES:
                raise ValueError("unsupported_issue_time")
        if not 0 < self.freshness_warn_minutes < self.freshness_error_minutes:
            raise ValueError("invalid_freshness_thresholds")
        if self.required_category_count != 14:
            raise ValueError("unsupported_category_contract")


def load_grids(path: Path, expected_count: int = 80) -> tuple[tuple[str, int, int], ...]:
    with path.open(encoding="utf-8", newline="") as handle:
        rows = tuple((row["place_id"], int(row["nx"]), int(row["ny"]))
                     for row in csv.DictReader(handle) if row.get("coverage_scope") == "seoul_bbox")
    ReportContract(rows)
    if len(rows) != expected_count:
        raise ValueError("grid_population_mismatch")
    return tuple(sorted(rows))


def _timestamp(value) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    # Existing Bronze collected_at and manifest event_at are UTC-naive timestamps.
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _phase_reason(phase: dict) -> str:
    value = str(phase.get("reason_code", "evidence_unavailable"))
    return value if re.fullmatch(r"[a-z_]{1,64}", value) else "evidence_unavailable"


def _unmeasured(reason: str, **metrics) -> dict:
    return {"status": "unmeasured", "reason_code": reason, **metrics}


def _evaluation_window(report_date: date, evaluation_as_of: datetime) -> tuple[datetime, date]:
    if evaluation_as_of.tzinfo is None:
        raise ValueError("evaluation_requires_timezone")
    as_of = evaluation_as_of.astimezone(timezone.utc)
    end_date = as_of.astimezone(KST).date()
    if not 1 <= (end_date - report_date).days <= 2:
        raise ValueError("report_requires_complete_recent_kst_date")
    return as_of, end_date


def _manifest_state(phase: dict, as_of: datetime) -> tuple[set[str], dict]:
    states: dict[str, list[dict]] = {}
    for row in phase.get("rows", []):
        if _timestamp(row["event_at"]) <= as_of:
            states.setdefault(row["dag_run_id"], []).append(row)
    summary = {"runs": len(states), "publishable_runs": 0, "failed_runs": 0,
               "coalesced_runs": 0, "ambiguous_runs": 0, "invalid_publishable_runs": 0}
    accepted = set()
    for run_id, rows in states.items():
        if any(row["status"] == "COALESCED" for row in rows):
            summary["coalesced_runs"] += 1
            continue
        latest = max(_timestamp(row["event_at"]) for row in rows)
        terminal = [row for row in rows if _timestamp(row["event_at"]) == latest]
        signature_keys = ("status", "is_publishable", "expected_rows", "actual_rows",
                          "expected_raw_objects", "actual_raw_objects")
        if len({tuple(row.get(key) for key in signature_keys) for row in terminal}) != 1:
            summary["ambiguous_runs"] += 1
            continue
        row = terminal[0]
        if row["status"] == "FAILED":
            summary["failed_runs"] += 1
        if row["status"] == "SUCCESS" and row["is_publishable"] is True:
            if (row.get("actual_rows") is None or row["actual_rows"] <= 0
                    or row.get("expected_rows") != row["actual_rows"]
                    or row.get("actual_raw_objects") is None or row["actual_raw_objects"] <= 0
                    or row.get("expected_raw_objects") != row["actual_raw_objects"]):
                summary["invalid_publishable_runs"] += 1
                continue
            accepted.add(run_id)
    summary["publishable_runs"] = len(accepted)
    return accepted, summary


def _accepted_coverage(evidence: dict, contract: ReportContract, as_of: datetime):
    accepted, summary = _manifest_state(evidence["manifests"], as_of)
    rows = []
    invalid_groups = 0
    population = set(contract.grids)
    for row in evidence["coverage"].get("rows", []):
        if row["dag_run_id"] not in accepted:
            continue
        if (_timestamp(row["last_collected_at_utc"]) > as_of or row.get("invalid_rows", 0)
                or row["row_count"] <= 0 or row["category_count"] < contract.required_category_count
                or (row["place_id"], row["nx"], row["ny"]) not in population
                or row["base_time"] not in contract.issue_times):
            invalid_groups += 1
            continue
        rows.append(row)
    summary["invalid_coverage_groups"] = invalid_groups
    return rows, summary


def _missing_check(report_date: date, contract: ReportContract, rows: list[dict], reason: str | None) -> dict:
    expected = {(report_date.strftime("%Y%m%d"), cycle, place, nx, ny)
                for cycle in contract.issue_times for place, nx, ny in contract.grids}
    observed = {(row["base_date"], row["base_time"], row["place_id"], row["nx"], row["ny"])
                for row in rows} & expected
    metrics = {"expected_slots": len(expected), "observed_slots": len(observed),
               "missing_slots": None, "coverage_percent": None, "missing_slot_keys": [],
               "denominator_basis": "configured_forecast_contract",
               "scheduled_population_verified": False}
    if reason or not observed:
        return _unmeasured(reason or "no_publishable_collection_evidence", **metrics)
    gaps = sorted(expected - observed)
    return {**metrics, "status": "fail" if gaps else "pass",
            "reason_code": "contract_slots_missing" if gaps else "contract_coverage_complete",
            "missing_slots": len(gaps), "coverage_percent": round(len(observed) / len(expected) * 100, 2),
            "missing_slot_keys": ["/".join(map(str, key)) for key in gaps]}


def _duplicate_check(phase: dict) -> dict:
    metrics = {"excess_rows": None, "duplicate_groups": None, "sample_rows": None,
               "invalid_grain_rows": None, "scope": "bronze_business_grain_within_dag_run"}
    rows = phase.get("rows", [])
    if phase["status"] != "measured" or len(rows) != 1:
        return _unmeasured(_phase_reason(phase), **metrics)
    row = rows[0]
    if row["total_rows"] == 0:
        return _unmeasured("no_forecast_rows_in_window", **metrics)
    bad = row["duplicate_excess_rows"] > 0 or row["invalid_grain_rows"] > 0
    return {**metrics, "status": "fail" if bad else "pass",
            "reason_code": "duplicate_or_invalid_grain" if bad else "unique_within_run",
            "excess_rows": row["duplicate_excess_rows"], "duplicate_groups": row["duplicate_groups"],
            "sample_rows": row["total_rows"], "invalid_grain_rows": row["invalid_grain_rows"]}


def _freshness_check(contract: ReportContract, rows: list[dict], as_of: datetime, reason: str | None) -> dict:
    metrics = {"latest_issue_at": None, "latest_issue_age_minutes": None,
               "latest_collected_at": None, "latest_collection_age_minutes": None,
               "stale_grids": None, "unmeasured_grids": len(contract.grids), "expected_latest_due_issue_at": None}
    if reason or not rows:
        return _unmeasured(reason or "no_publishable_collection_evidence", **metrics)
    latest: dict[tuple[str, int, int], datetime] = {}
    collection_times = []
    for row in rows:
        issue = datetime.strptime(row["base_date"] + row["base_time"], "%Y%m%d%H%M").replace(tzinfo=KST)
        if issue > as_of:
            continue
        key = (row["place_id"], row["nx"], row["ny"])
        latest[key] = max(latest.get(key, issue), issue)
        collection_times.append(_timestamp(row["last_collected_at_utc"]))
    if not latest:
        return _unmeasured("no_due_forecast_evidence", **metrics)
    due_cycles = [datetime.combine(as_of.astimezone(KST).date() - timedelta(days=offset),
                                   time(int(cycle[:2])), tzinfo=KST)
                  for offset in (0, 1) for cycle in contract.issue_times]
    due = max(issue for issue in due_cycles if issue + timedelta(minutes=60) <= as_of)
    ages = [(as_of - issue).total_seconds() / 60 for issue in latest.values()]
    stale = sum(age > contract.freshness_error_minutes for age in ages)
    missing = len(contract.grids) - len(latest)
    delayed = sum(issue < due for issue in latest.values())
    if stale:
        status, reason_code = "fail", "source_issue_stale"
    elif missing:
        status, reason_code = "unmeasured", "grid_freshness_incomplete"
    elif delayed or max(ages) > contract.freshness_warn_minutes:
        status, reason_code = "warn", "source_issue_delayed"
    else:
        status, reason_code = "pass", "source_issue_fresh"
    last_issue, collected = max(latest.values()), max(collection_times)
    return {**metrics, "status": status, "reason_code": reason_code,
            "latest_issue_at": last_issue.isoformat(), "latest_issue_age_minutes": round(min(ages), 2),
            "oldest_grid_issue_age_minutes": round(max(ages), 2), "latest_collected_at": collected.isoformat(),
            "latest_collection_age_minutes": round((as_of - collected).total_seconds() / 60, 2),
            "stale_grids": stale, "delayed_grids": delayed, "unmeasured_grids": missing,
            "expected_latest_due_issue_at": due.isoformat()}


def build_report(report_date: date, evaluation_as_of: datetime, contract: ReportContract,
                 evidence: dict, *, runtime: dict | None = None) -> dict:
    as_of, end_date = _evaluation_window(report_date, evaluation_as_of)
    phases = ("coverage", "manifests")
    reason = next((_phase_reason(evidence[key]) for key in phases
                   if evidence[key]["status"] != "measured"), None)
    rows, summary = _accepted_coverage(evidence, contract, as_of)
    if summary["ambiguous_runs"]:
        reason = "ambiguous_manifest_state"
    if summary["invalid_publishable_runs"] or summary["invalid_coverage_groups"]:
        reason = "invalid_collection_evidence"
    checks = {"missing": _missing_check(report_date, contract, rows, reason),
              "duplicates": _duplicate_check(evidence["duplicates"]),
              "freshness": _freshness_check(contract, rows, as_of, reason)}
    status = next((candidate for candidate in ("fail", "unmeasured", "warn", "pass")
                   if any(check["status"] == candidate for check in checks.values())), "unmeasured")
    population_hash = hashlib.sha256(json.dumps(contract.grids).encode()).hexdigest()
    return {"schema_version": "weather-daily-quality/v1", "policy_version": "weather-collection-dq/v1",
            "source_id": "kma_vilage_fcst", "report_date_kst": report_date.isoformat(),
            "evaluation_as_of": as_of.isoformat(), "status": status, "checks": checks,
            "contract": {"grid_count": len(contract.grids), "grid_population_sha256": population_hash,
                         "issue_times_kst": list(contract.issue_times), "required_category_count": 14,
                         "freshness_warn_minutes": contract.freshness_warn_minutes,
                         "freshness_error_minutes": contract.freshness_error_minutes},
            "measurement_window": {"load_date_start_kst": report_date.isoformat(),
                                   "load_date_end_kst": end_date.isoformat(), "collection_as_of_utc": as_of.isoformat()},
            "manifest_summary": summary, "runtime": runtime or {},
            "evidence_sources": {key: {"status": phase["status"], "reason_code": _phase_reason(phase),
                                       "query_ids": phase.get("query_ids", []),
                                       "duration_seconds": phase.get("duration_seconds"),
                                       "physical_input_bytes": phase.get("physical_input_bytes"),
                                       "processed_bytes": phase.get("processed_bytes")}
                                 for key, phase in evidence.items()},
            "limitations": ["Expected slots describe the configured forecast contract; active Airflow scheduling is not verified.",
                            "Missing measures accepted issue-grid coverage, not every forecast hour or variable value.",
                            "Duplicates are checked inside one Bronze DAG run; retained recollections across runs are not DQ duplicates.",
                            "Only the bounded collection window is read; later backfills and all-history Silver uniqueness are not measured.",
                            "Freshness is evaluated at evaluation_as_of, including source issue age per grid."]}


def render_markdown(report: dict) -> str:
    names = {"pass": "정상", "warn": "주의", "fail": "실패", "unmeasured": "미측정"}
    missing, duplicates, freshness = (report["checks"][key] for key in ("missing", "duplicates", "freshness"))
    def value(item):
        return "미측정" if item is None else str(item)
    lines = [f"# 서울 단기예보 일일 데이터 품질 — {report['report_date_kst']}", "",
             f"전체 상태: **{names[report['status']]}**", f"평가 시각: {report['evaluation_as_of']}", "",
             "| 검사 | 상태 | 결과 | 근거 코드 |", "|---|---|---|---|",
             f"| 누락 | {names[missing['status']]} | 관측 {missing['observed_slots']} / 계약 기대 {missing['expected_slots']}, 누락 {value(missing['missing_slots'])} | {missing['reason_code']} |",
             f"| 중복 | {names[duplicates['status']]} | 초과 행 {value(duplicates['excess_rows'])}, 검사 행 {value(duplicates['sample_rows'])} | {duplicates['reason_code']} |",
             f"| 최신성 | {names[freshness['status']]} | 최신 발표 경과 {value(freshness['latest_issue_age_minutes'])}분, 오래된 격자 {value(freshness['stale_grids'])}, 미측정 격자 {freshness['unmeasured_grids']} | {freshness['reason_code']} |", "",
             "계약 기대치는 설정된 격자 × 발표 주기입니다. 실제 DAG 활성화·예약 실행 여부는 별도 검증 대상입니다.",
             "원본을 재수집한 다른 실행은 중복 데이터로 세지 않습니다. 최신 수집 시각과 예보 발표 시각을 구분합니다.",
             "수집 근거를 읽지 못했거나 검증된 표본이 없으면 정상 또는 0으로 표시하지 않습니다.", "",
             f"읽은 수집 날짜 범위(KST): {report['measurement_window']['load_date_start_kst']} ~ {report['measurement_window']['load_date_end_kst']}",
             f"최근 수집 시각(UTC): {value(freshness['latest_collected_at'])}",
             f"최근 예보 발표 시각: {value(freshness['latest_issue_at'])}", "",
             "자동 생성된 운영 기록입니다. 예보 정확도 평가나 전체 이력의 품질 보증을 뜻하지 않습니다.", ""]
    if missing["missing_slot_keys"]:
        lines += ["누락한 발표/격자:", "", *(f"- {key}" for key in missing["missing_slot_keys"]), ""]
    return "\n".join(lines)


def _atomic_write(path: Path, payload: bytes):
    descriptor, temporary = tempfile.mkstemp(prefix=".quality-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def save_report(report: dict, output_dir: Path) -> Path:
    day = date.fromisoformat(report["report_date_kst"]).isoformat()
    payload = (json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    digest = hashlib.sha256(payload).hexdigest()
    output_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    revision_dir = output_dir / "revisions" / day
    revision_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (output_dir / ".report.lock").open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        revision = revision_dir / f"{digest}.json"
        if not revision.exists():
            _atomic_write(revision, payload)
        latest = output_dir / f"{day}.json"
        if latest.exists():
            previous = json.loads(latest.read_text(encoding="utf-8"))
            if _timestamp(previous["evaluation_as_of"]) > _timestamp(report["evaluation_as_of"]):
                return latest
        _atomic_write(output_dir / f"{day}.md", render_markdown(report).encode())
        # JSON is authoritative; Markdown is a recoverable projection. Commit JSON last.
        _atomic_write(latest, payload)
    return output_dir / f"{day}.json"


def runtime_status() -> dict:
    socket = Path.home() / ".docker/run/docker.sock"
    state = {"docker": "unavailable", "host_guard_latched": None}
    try:
        result = subprocess.run(["docker", "--host", f"unix://{socket}", "ps", "--format", "{{.Names}}"],
                                capture_output=True, text=True, timeout=5, check=False)
        if result.returncode == 0:
            names = set(result.stdout.splitlines())
            state["docker"] = "available"
            state["weather_scheduler_running"] = "seoul-weather-platform-mac-airflow-scheduler-1" in names
    except (OSError, subprocess.SubprocessError):
        pass
    path = Path.home() / ".local/state/seoul-weather-platform/host-guard/state.json"
    try:
        stored = json.loads(path.read_text())
        state["host_guard_latched"] = stored.get("latched") if isinstance(stored.get("latched"), bool) else None
    except (OSError, ValueError):
        pass
    return state


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-date", type=date.fromisoformat)
    parser.add_argument("--as-of", type=datetime.fromisoformat)
    parser.add_argument("--grid-csv", type=Path, default=DEFAULT_GRID_CSV)
    parser.add_argument("--expected-grids", type=int, default=80)
    parser.add_argument("--issue-times", default=",".join(DEFAULT_ISSUE_TIMES))
    parser.add_argument("--output-dir", type=Path, default=ROOT / "logs/weather_daily_quality")
    parser.add_argument("--trino-port", type=int, default=30586)
    parser.add_argument("--env-file", type=Path, default=ROOT / "weather-platform.prod.env")
    parser.add_argument("--catalog")
    parser.add_argument("--schema")
    parser.add_argument("--warn-minutes", type=int)
    parser.add_argument("--error-minutes", type=int)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        from tools.weather_daily_quality_evidence import EvidenceConfig, collect_evidence
        as_of = args.as_of or datetime.now(timezone.utc)
        if as_of.tzinfo is None:
            raise ValueError("evaluation_requires_timezone")
        report_date = args.report_date or as_of.astimezone(KST).date() - timedelta(days=1)
        as_of, _ = _evaluation_window(report_date, as_of)
        settings = load_report_settings(args.env_file)
        catalog = args.catalog or settings.get("TRINO_ICEBERG_CATALOG")
        schema = args.schema or settings.get("ASK_SEOUL_SCHEMA")
        if not catalog or not schema:
            raise ValueError("explicit_trino_namespace_required")
        contract = ReportContract(load_grids(args.grid_csv, args.expected_grids),
                                  tuple(args.issue_times.split(",")),
                                  freshness_warn_minutes=args.warn_minutes if args.warn_minutes is not None else int(settings.get("ASK_SEOUL_REPORT_WEATHER_FRESHNESS_WARN_MINUTES", "240")),
                                  freshness_error_minutes=args.error_minutes if args.error_minutes is not None else int(settings.get("ASK_SEOUL_REPORT_WEATHER_FRESHNESS_ERROR_MINUTES", "360")))
        config = EvidenceConfig(port=args.trino_port, catalog=catalog, schema=schema)
        runtime = runtime_status()
        if runtime["host_guard_latched"] is True:
            evidence = {key: {"status": "unmeasured", "reason_code": "host_guard_latched",
                              "rows": [], "query_ids": []}
                        for key in ("coverage", "duplicates", "manifests")}
        else:
            evidence = collect_evidence(config, report_date, as_of)
        report = build_report(report_date, as_of, contract, evidence, runtime=runtime)
        report["measurement_window"]["bronze_relation"] = f"{catalog}.{schema}.bronze_kma_vilage_fcst"
        save_report(report, args.output_dir)
        for phase, result in evidence.items():
            LOGGER.info("daily_quality phase=%s status=%s reason=%s", phase,
                        result["status"], _phase_reason(result))
        LOGGER.info("daily_quality date=%s status=%s", report_date, report["status"])
        return {"pass": 0, "warn": 0, "fail": 1, "unmeasured": 2}[report["status"]]
    except (ValueError, OSError, KeyError, TypeError, ImportError):
        LOGGER.error("daily_quality reason=configuration_or_evidence_invalid")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
