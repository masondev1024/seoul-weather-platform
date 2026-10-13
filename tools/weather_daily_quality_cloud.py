"""Bounded, read-only R2 landing evidence suitable for public daily reports.

Raw completion manifests prove landing coverage, not Bronze row uniqueness or
forecast accuracy. Every output field is constructed explicitly; private object
keys, run identifiers, bucket names and provider errors never enter the report.
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from tools.weather_daily_quality import (
    DEFAULT_GRID_CSV, DEFAULT_ISSUE_TIMES, KST, _atomic_write, load_grids,
)

SOURCE = "kma_vilage_fcst"
PREFIX = f"raw/weather/{SOURCE}/load_date="
MAX_OBJECTS = 5000
MAX_LIST_PAGES = 12
MAX_MANIFESTS = 64
MAX_MANIFEST_BYTES = 256 * 1024
MAX_TOTAL_BYTES = 4 * 1024 * 1024
RAW_KEY = re.compile(
    rf"{PREFIX}(?P<day>\d{{4}}-\d{{2}}-\d{{2}})/"
    r"(?:run_id=[\w.=-]+/)?nx=(?P<nx>\d+)/ny=(?P<ny>\d+)/"
    r"(?P<collected>\d{8}T\d{6})KST_base-(?P<issue>\d{12})_[\w.=-]+\.json$"
)
MANIFEST_KEY = re.compile(rf"{PREFIX}(?P<day>\d{{4}}-\d{{2}}-\d{{2}})/run_id=(?P<run>[\w.=-]+)/_manifest\.json$")
CREDENTIALS = ("R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME")


class EvidenceUnavailable(Exception):
    """A fixed, public-safe reason code rather than provider exception text."""


def _window(report_date: date, as_of: datetime) -> tuple[datetime, tuple[str, str]]:
    if as_of.tzinfo is None:
        raise ValueError("evaluation_requires_timezone")
    as_of = as_of.astimezone(timezone.utc)
    evaluation_day = as_of.astimezone(KST).date()
    if (evaluation_day - report_date).days != 1:
        raise ValueError("report_requires_previous_complete_kst_day")
    return as_of, (report_date.isoformat(), evaluation_day.isoformat())


def _list_objects(client, bucket: str, days: tuple[str, str]) -> dict[str, int]:
    objects = {}
    consumed = 0
    requests = 0
    for day in days:
        prefix = f"{PREFIX}{day}/"
        token = None
        tokens = set()
        while True:
            if consumed >= MAX_OBJECTS or requests >= MAX_LIST_PAGES:
                raise EvidenceUnavailable("listing_budget_exceeded")
            options = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": min(1000, MAX_OBJECTS - consumed)}
            if token:
                options["ContinuationToken"] = token
            requests += 1
            page = client.list_objects_v2(**options)
            entries = page.get("Contents", [])
            if not isinstance(entries, list) or len(entries) > options["MaxKeys"]:
                raise EvidenceUnavailable("listing_invalid")
            consumed += len(entries)
            for item in entries:
                key, size = item.get("Key"), item.get("Size")
                if not isinstance(key, str) or not key.startswith(prefix) or type(size) is not int or size < 0:
                    raise EvidenceUnavailable("listing_invalid")
                objects[key] = size
            if page.get("IsTruncated") is False:
                break
            token = page.get("NextContinuationToken")
            if not isinstance(token, str) or not token or token in tokens:
                raise EvidenceUnavailable("listing_invalid")
            tokens.add(token)
    return objects


def _read_manifest_bytes(client, bucket: str, key: str, remaining_bytes: int) -> bytes:
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        payload = body.read(min(MAX_MANIFEST_BYTES, remaining_bytes) + 1)
    finally:
        body.close()
    if len(payload) > min(MAX_MANIFEST_BYTES, remaining_bytes):
        raise EvidenceUnavailable("manifest_budget_exceeded")
    return payload


def _parse_raw(key: str, days: tuple[str, str], as_of: datetime, grids: set) -> tuple:
    match = RAW_KEY.fullmatch(key)
    if not match or match["day"] not in days:
        raise ValueError("raw_key_invalid")
    collected = datetime.strptime(match["collected"], "%Y%m%dT%H%M%S").replace(tzinfo=KST)
    issue = datetime.strptime(match["issue"], "%Y%m%d%H%M").replace(tzinfo=KST)
    grid = (int(match["nx"]), int(match["ny"]))
    if grid not in grids or issue > collected or collected > as_of or issue.strftime("%H%M") not in DEFAULT_ISSUE_TIMES:
        raise ValueError("raw_evidence_invalid")
    return grid, issue.astimezone(timezone.utc), collected.astimezone(timezone.utc)


def _validate_manifest(document: dict, key: str, objects: dict, days: tuple[str, str], as_of: datetime, grids: set) -> list:
    match = MANIFEST_KEY.fullmatch(key)
    run = document.get("run_id")
    safe_run = "".join(char if char.isalnum() or char in "._=-" else "_" for char in run) if isinstance(run, str) else ""
    keys = document.get("object_keys")
    if (not match or not run or match["run"] != safe_run or document.get("dataset") != SOURCE
            or document.get("load_date") != match["day"] or document.get("status") not in ("complete", "SUCCESS")
            or not isinstance(keys, list) or not keys or not all(isinstance(item, str) for item in keys)):
        raise ValueError("manifest_invalid")
    unique = set(keys)
    if len(unique) != len(keys):
        raise ValueError("manifest_duplicate_references")
    if (type(document.get("actual_count")) is not int or type(document.get("expected_count")) is not int
            or document["actual_count"] != len(unique) or document["expected_count"] != len(unique)):
        raise ValueError("manifest_counts_invalid")
    completed = datetime.fromisoformat(str(document.get("completed_at")).replace("Z", "+00:00"))
    if completed.tzinfo is None or completed > as_of:
        raise ValueError("manifest_time_invalid")
    evidence = []
    for raw_key in unique:
        if raw_key not in objects:
            raise ValueError("manifest_reference_unlisted")
        row = _parse_raw(raw_key, days, as_of, grids)
        if row[2] > completed:
            raise ValueError("manifest_time_invalid")
        evidence.append((raw_key, *row))
    # Recovery runs can reuse foreign-run and legacy keys. All references must
    # still be listed in the two permitted load dates and share one source issue.
    if len({row[2] for row in evidence}) != 1:
        raise ValueError("manifest_issue_mismatch")
    return evidence


def _collect(client, bucket: str, days: tuple[str, str], as_of: datetime, grids: set) -> dict:
    result = {"reason": None, "objects": None, "raw_objects": None, "manifests": 0,
              "accepted_manifests": 0, "invalid_manifests": 0, "duplicate_references": 0, "rows": []}
    try:
        objects = _list_objects(client, bucket, days)
        result["objects"] = len(objects)
        result["raw_objects"] = sum(bool(RAW_KEY.fullmatch(key)) for key in objects)
        manifests = sorted(key for key in objects if key.endswith("/_manifest.json"))
        result["manifests"] = len(manifests)
        if (len(manifests) > MAX_MANIFESTS or sum(objects[key] for key in manifests) > MAX_TOTAL_BYTES
                or any(objects[key] > MAX_MANIFEST_BYTES for key in manifests)):
            raise EvidenceUnavailable("manifest_budget_exceeded")
        bytes_read = 0
        for key in manifests:
            if not MANIFEST_KEY.fullmatch(key):
                result["invalid_manifests"] += 1
                continue
            try:
                payload = _read_manifest_bytes(client, bucket, key, MAX_TOTAL_BYTES - bytes_read)
                bytes_read += len(payload)
                document = json.loads(payload)
                if not isinstance(document, dict):
                    raise ValueError("manifest_invalid")
                keys = document.get("object_keys")
                if isinstance(keys, list) and all(isinstance(item, str) for item in keys):
                    result["duplicate_references"] += len(keys) - len(set(keys))
                result["rows"].extend(_validate_manifest(document, key, objects, days, as_of, grids))
                result["accepted_manifests"] += 1
            except (ValueError, TypeError, KeyError):
                result["invalid_manifests"] += 1
        if result["invalid_manifests"]:
            result["reason"] = "manifest_invalid"
        elif not result["accepted_manifests"]:
            result["reason"] = "no_complete_manifests"
    except EvidenceUnavailable as error:
        result["reason"] = str(error)
    except Exception:
        result["reason"] = "r2_evidence_unavailable"
    return result


def _check(status: str, reason: str, **values) -> dict:
    return {"status": status, "reason_code": reason, **values}


def build_report(client, bucket: str, report_date: date, as_of: datetime, *, grids=None, unavailable_reason=None) -> dict:
    as_of, days = _window(report_date, as_of)
    grid_set = set(grids if grids is not None else ((nx, ny) for _, nx, ny in load_grids(DEFAULT_GRID_CSV)))
    if not grid_set or any(type(nx) is not int or type(ny) is not int or nx < 1 or ny < 1 for nx, ny in grid_set):
        raise ValueError("grid_population_invalid")
    evidence = _collect(client, bucket, days, as_of, grid_set) if unavailable_reason is None else {
        "reason": "credentials_missing", "objects": None, "raw_objects": None, "manifests": 0,
        "accepted_manifests": 0, "invalid_manifests": 0, "duplicate_references": 0, "rows": []}
    reason, rows = evidence["reason"], evidence["rows"]
    complete_listing = evidence["objects"] is not None
    raw_count = evidence["raw_objects"]
    collection = _check("unmeasured", reason or "evidence_unavailable", raw_objects=raw_count)
    if complete_listing:
        collection = _check("pass" if raw_count else "fail", "raw_objects_present" if raw_count else "no_raw_collection_objects", raw_objects=raw_count)
    expected = len(grid_set) * len(DEFAULT_ISSUE_TIMES)
    observed = {(grid, issue) for _, grid, issue, _ in rows if issue.astimezone(KST).date() == report_date}
    missing = _check("unmeasured", reason or "no_report_day_evidence", expected_slots=expected,
                     observed_slots=len(observed), missing_slots=None, scheduled_population_verified=False)
    if not reason and observed:
        gaps = expected - len(observed)
        missing = _check("fail" if gaps else "pass", "contract_slots_missing" if gaps else "contract_coverage_complete",
                         expected_slots=expected, observed_slots=len(observed), missing_slots=gaps, scheduled_population_verified=False)
    duplicates = evidence["duplicate_references"]
    measured_duplicates = duplicates if evidence["manifests"] and not reason else None
    duplicate_check = _check("unmeasured", reason or "no_manifest_evidence", duplicate_references=measured_duplicates)
    if duplicates or measured_duplicates is not None:
        duplicate_check = _check("fail" if duplicates else "pass", "manifest_duplicate_references" if duplicates else "manifest_references_unique", duplicate_references=duplicates)
    latest = {}
    for _, grid, issue, _ in rows:
        latest[grid] = max(issue, latest.get(grid, issue))
    stale = sum((as_of - issued).total_seconds() > 360 * 60 for issued in latest.values())
    absent = len(grid_set - latest.keys())
    freshness = _check("unmeasured", reason or "grid_evidence_missing", measured_grids=len(latest), stale_grids=stale,
                       missing_grids=absent, latest_issue_at=max(latest.values()).isoformat() if latest else None,
                       latest_collected_at=max(row[3] for row in rows).isoformat() if rows else None)
    if stale or (not reason and not absent):
        freshness.update(status="fail" if stale else "pass", reason_code="source_issue_stale" if stale else "source_issue_current")
    checks = {"collection": collection, "missing": missing, "duplicates": duplicate_check, "freshness": freshness}
    status = next(state for state in ("fail", "unmeasured", "warn", "pass") if any(check["status"] == state for check in checks.values()))
    revision = os.environ.get("GITHUB_SHA", "local")
    verified_keys = {row[0] for row in rows}
    return {"schema_version": "weather-raw-quality/v1", "report_date_kst": days[0], "evaluation_as_of": as_of.isoformat(),
            "source_revision": revision if re.fullmatch(r"[0-9a-f]{40}", revision) else "local", "status": status,
            "contract": {"source": SOURCE, "grid_count": len(grid_set), "issue_cycle_count": len(DEFAULT_ISSUE_TIMES),
                         "freshness_error_minutes": 360, "coverage_basis": "configured_forecast_contract", "duplicate_basis": "manifest_object_references"},
            "measurement_window": {"load_date_start_kst": days[0], "load_date_end_kst": days[1]}, "checks": checks,
            "evidence": {"listed_objects": evidence["objects"], "manifest_count": evidence["manifests"],
                         "accepted_manifests": evidence["accepted_manifests"], "invalid_manifests": evidence["invalid_manifests"],
                         "verified_raw_objects": len(verified_keys), "unverified_raw_objects": max(0, raw_count - len(verified_keys)) if raw_count is not None else None}}


def render_markdown(report: dict) -> str:
    lines = [f"# 서울 기상 수집 품질 — {report['report_date_kst']}", "", f"전체 상태: **{report['status']}**",
             f"측정 시각(UTC): {report['evaluation_as_of']}", "", "| 항목 | 상태 | 근거 |", "| --- | --- | --- |"]
    labels = {"collection": "수집 가용성", "missing": "누락", "duplicates": "Manifest 참조 중복", "freshness": "최신성"}
    lines.extend(f"| {labels[name]} | {check['status']} | {check['reason_code']} |" for name, check in report["checks"].items())
    missing = report["checks"]["missing"]
    duplicates = report["checks"]["duplicates"]["duplicate_references"]
    freshness = report["checks"]["freshness"]
    def displayed(value):
        return "미측정" if value is None else str(value)
    lines += ["", f"설정된 발표·격자 조합: {missing['expected_slots']}, 확인된 조합: {missing['observed_slots']}",
              f"Manifest 참조 중복 수: {displayed(duplicates)}",
              f"오래된 발표 격자 수: {freshness['stale_grids']}, 근거가 없는 격자 수: {freshness['missing_grids']}",
              f"최근 예보 발표 시각(UTC): {displayed(freshness['latest_issue_at'])}",
              f"최근 수집 시각(UTC): {displayed(freshness['latest_collected_at'])}",
              "", "완료 manifest와 객체 존재 여부만 읽기 전용으로 검사했습니다. 격자·발표 주기는 설정 계약이며 실제 스케줄 활성화는 확인하지 않았습니다.",
              "중복 지표는 manifest 안의 같은 객체 참조이며, Bronze 행 중복이나 예보 정확도를 평가하지 않습니다.",
              "검증 근거가 부족하면 미측정으로 표시합니다. 수집 파일이 전혀 없으면 수집 가용성 실패로 기록합니다.", ""]
    return "\n".join(lines)


def _client_from_env():
    import boto3
    from botocore.config import Config

    return boto3.client("s3", region_name="auto", endpoint_url=os.environ["R2_ENDPOINT"], aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
                        config=Config(connect_timeout=5, read_timeout=15, retries={"total_max_attempts": 2, "mode": "standard"}))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("logs/weather_daily_quality_cloud"))
    parser.add_argument("--report-date", type=date.fromisoformat)
    args = parser.parse_args(argv)
    as_of = datetime.now(timezone.utc)
    day = args.report_date or as_of.astimezone(KST).date() - timedelta(days=1)
    try:
        present = all(os.environ.get(name) for name in CREDENTIALS)
        client = _client_from_env() if present else None
        report = build_report(client, os.environ.get("R2_BUCKET_NAME", ""), day, as_of,
                              unavailable_reason=None if present else "credentials_missing")
        args.output_dir.mkdir(parents=True, exist_ok=True)
        _atomic_write(args.output_dir / f"{day}.json", (json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True) + "\n").encode())
        _atomic_write(args.output_dir / f"{day}.md", render_markdown(report).encode())
    except Exception:
        print("daily_quality_configuration_or_output_error")
        return 3
    print(f"daily_quality_report date={day} status={report['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
