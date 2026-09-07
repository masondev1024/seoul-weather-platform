#!/usr/bin/env python3
"""Run the Weather recovery planner in dry-run mode.

The command consumes a sanitized candidate document and prints a deterministic
plan.  It never calls Airflow, KMA, Trino, R2, or D1.  The eventual Airflow
coordinator can reuse :func:`build_plan` after it has built candidates from
validated receipts.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping


REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_ROOT = REPO_ROOT / "dags"
if str(DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(DAGS_ROOT))

from common.recovery.planner import (  # noqa: E402
    RecoveryCandidate,
    RecoveryPlan,
    RecoveryPlannerError,
    RecoveryPolicy,
    plan_recovery,
)


CANDIDATE_SCHEMA_VERSION = "weather-recovery-candidates/v1"


def build_plan(
    payload: Mapping[str, object],
    *,
    now: datetime,
    policy: RecoveryPolicy | None = None,
) -> RecoveryPlan:
    """Validate a candidate document and return its side-effect-free plan."""
    if payload.get("schema_version") not in (None, CANDIDATE_SCHEMA_VERSION):
        raise RecoveryPlannerError("unsupported candidate schema version")
    raw_candidates = payload.get("candidates")
    if not isinstance(raw_candidates, list):
        raise RecoveryPlannerError("candidates must be a list")
    candidates = tuple(_candidate_from_mapping(item) for item in raw_candidates)
    return plan_recovery(candidates, now=now, policy=policy)


def _candidate_from_mapping(value: object) -> RecoveryCandidate:
    if not isinstance(value, Mapping):
        raise RecoveryPlannerError("candidate must be an object")
    slot_ids = value.get("slot_ids")
    if not isinstance(slot_ids, list) or any(not isinstance(item, str) for item in slot_ids):
        raise RecoveryPlannerError("candidate.slot_ids must be a string list")

    def required_text(name: str) -> str:
        raw = value.get(name)
        if not isinstance(raw, str) or not raw.strip():
            raise RecoveryPlannerError(f"candidate.{name} must be a non-empty string")
        return raw

    def required_timestamp(name: str) -> datetime:
        raw = required_text(name)
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise RecoveryPlannerError(f"candidate.{name} must be an ISO timestamp") from exc
        if parsed.tzinfo is None:
            raise RecoveryPlannerError(f"candidate.{name} must include timezone")
        return parsed

    def required_int(name: str) -> int:
        raw = value.get(name)
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise RecoveryPlannerError(f"candidate.{name} must be an integer")
        return raw

    def required_bool(name: str) -> bool:
        raw = value.get(name)
        if not isinstance(raw, bool):
            raise RecoveryPlannerError(f"candidate.{name} must be a bool")
        return raw

    optional_text = value.get("normal_run_id")
    if optional_text is not None and not isinstance(optional_text, str):
        raise RecoveryPlannerError("candidate.normal_run_id must be a string or null")
    failure_code = value.get("last_failure_code")
    if failure_code is not None and not isinstance(failure_code, str):
        raise RecoveryPlannerError("candidate.last_failure_code must be a string or null")
    normal_run_active = value.get("normal_run_active", False)
    if not isinstance(normal_run_active, bool):
        raise RecoveryPlannerError("candidate.normal_run_active must be a bool")
    attempt_count = value.get("attempt_count", 0)
    if isinstance(attempt_count, bool) or not isinstance(attempt_count, int):
        raise RecoveryPlannerError("candidate.attempt_count must be an integer")
    return RecoveryCandidate(
        domain=required_text("domain"),
        source_id=required_text("source_id"),
        slot_key=required_text("slot_key"),
        slot_ids=tuple(slot_ids),
        scheduled_at=required_timestamp("scheduled_at"),
        deadline_at=required_timestamp("deadline_at"),
        recovery_boundary=required_timestamp("recovery_boundary"),
        expected_count=required_int("expected_count"),
        covered_count=required_int("covered_count"),
        raw_manifest_verified=required_bool("raw_manifest_verified"),
        historical_query_allowed=required_bool("historical_query_allowed"),
        normal_run_active=normal_run_active,
        normal_run_id=optional_text,
        last_failure_code=failure_code,
        attempt_count=attempt_count,
    )


def _policy_from_args(args: argparse.Namespace) -> RecoveryPolicy:
    return RecoveryPolicy(
        max_jobs=args.max_jobs,
        max_api_jobs=args.max_api_jobs,
        max_recovery_age=timedelta(hours=args.max_age_hours),
    )


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must not be negative")
    return parsed


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _read_payload(path: str) -> object:
    if path == "-":
        return json.load(sys.stdin)
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _now(raw: str | None) -> datetime:
    if raw is None:
        return datetime.now(timezone.utc)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--now must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--now must include timezone")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plan Weather recovery candidates without executing mutations."
    )
    parser.add_argument(
        "--input",
        default="-",
        help="candidate JSON path, or '-' for stdin (default)",
    )
    parser.add_argument("--now", help="fixed ISO timestamp for deterministic runs")
    parser.add_argument("--max-jobs", type=_positive_int, default=3)
    parser.add_argument("--max-api-jobs", type=_positive_int, default=1)
    parser.add_argument("--max-age-hours", type=_positive_float, default=24.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        payload = _read_payload(args.input)
        if not isinstance(payload, Mapping):
            raise RecoveryPlannerError("candidate document must be an object")
        plan = build_plan(
            payload,
            now=_now(args.now),
            policy=_policy_from_args(args),
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError, RecoveryPlannerError):
        # Do not echo paths, candidate values, or exception text: an input file
        # can contain object names or other operational identifiers.
        print("weather_recovery_coordinator_error=invalid_input", file=sys.stderr)
        return 2
    print(json.dumps(plan.to_dict(), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
