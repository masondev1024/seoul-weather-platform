from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Mapping


REQUIRED_RESULTS = frozenset(
    {
        "repository-contract",
        "dbt-weather",
        "airflow-tests",
        "dagbag-policy",
        "promotion-source",
        "governance-mode",
    }
)


@dataclass(frozen=True)
class GateDecision:
    allowed: bool
    reason: str


def _blocked(reason: str) -> GateDecision:
    return GateDecision(allowed=False, reason=reason)


def decide_required_ci(
    event_name: str,
    git_ref: str,
    governance_mode: str,
    results: Mapping[str, str],
) -> GateDecision:
    if governance_mode != "public":
        return _blocked("unsupported_governance_mode")
    if event_name not in {"pull_request", "push"}:
        return _blocked("unsupported_event")
    if event_name == "pull_request":
        if not git_ref.startswith("refs/pull/") or not git_ref.endswith("/merge"):
            return _blocked("unsupported_ref")
    else:
        if git_ref not in {"refs/heads/dev", "refs/heads/main"}:
            return _blocked("unsupported_ref")

    for name in sorted(set(results) - REQUIRED_RESULTS):
        return _blocked(f"unexpected_result:{name}")
    for name in REQUIRED_RESULTS:
        if results.get(name) != "success":
            return _blocked(f"required_result_not_success:{name}")
    return GateDecision(allowed=True, reason="allowed")


def _result(value: str) -> tuple[str, str]:
    name, separator, status = value.partition("=")
    if not separator or not name or not status:
        raise argparse.ArgumentTypeError("result must use name=value")
    return name, status


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Decide whether required CI results allow promotion.")
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--git-ref", required=True)
    parser.add_argument("--governance-mode", required=True)
    parser.add_argument("--result", type=_result, action="append", default=[])
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    decision = decide_required_ci(
        args.event_name,
        args.git_ref,
        args.governance_mode,
        dict(args.result),
    )
    print(decision.reason)
    return 0 if decision.allowed else 1


if __name__ == "__main__":
    raise SystemExit(main())
