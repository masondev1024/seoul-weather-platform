from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from tools.ci_required_gate import REQUIRED_RESULTS, decide_required_ci


PUBLIC_RESULTS = {
    "repository-contract": "success",
    "dbt-weather": "success",
    "airflow-tests": "success",
    "dagbag-policy": "success",
    "promotion-source": "success",
    "governance-mode": "success",
}


def test_public_pull_request_allows_only_hosted_required_results() -> None:
    decision = decide_required_ci(
        "pull_request", "refs/pull/7/merge", "public", PUBLIC_RESULTS
    )

    assert decision.allowed
    assert decision.reason == "allowed"
    assert REQUIRED_RESULTS == frozenset(PUBLIC_RESULTS)


def test_public_main_and_dev_push_allow_only_hosted_required_results() -> None:
    for ref in ("refs/heads/main", "refs/heads/dev"):
        assert decide_required_ci("push", ref, "public", PUBLIC_RESULTS).allowed


def test_public_gate_rejects_self_hosted_runtime_result_dependency() -> None:
    decision = decide_required_ci(
        "push",
        "refs/heads/main",
        "public",
        PUBLIC_RESULTS | {"dagbag-runtime": "success"},
    )

    assert not decision.allowed
    assert decision.reason == "unexpected_result:dagbag-runtime"


def test_public_gate_rejects_non_public_governance_modes() -> None:
    for mode in ("protected", "guarded_private", "", "open"):
        decision = decide_required_ci("push", "refs/heads/main", mode, PUBLIC_RESULTS)

        assert not decision.allowed
        assert decision.reason == "unsupported_governance_mode"


def test_public_gate_fails_closed_for_missing_or_non_success_hosted_result() -> None:
    missing = decide_required_ci(
        "push",
        "refs/heads/main",
        "public",
        {key: value for key, value in PUBLIC_RESULTS.items() if key != "dbt-weather"},
    )
    malformed = decide_required_ci(
        "push",
        "refs/heads/main",
        "public",
        PUBLIC_RESULTS | {"dbt-weather": "cancelled"},
    )

    assert not missing.allowed
    assert missing.reason == "required_result_not_success:dbt-weather"
    assert not malformed.allowed
    assert malformed.reason == "required_result_not_success:dbt-weather"


def test_public_gate_rejects_unsupported_event_or_ref() -> None:
    unsupported_event = decide_required_ci(
        "workflow_dispatch", "refs/heads/main", "public", PUBLIC_RESULTS
    )
    unsupported_ref = decide_required_ci(
        "push", "refs/heads/feature", "public", PUBLIC_RESULTS
    )
    unsupported_pr_ref = decide_required_ci(
        "pull_request", "refs/heads/main", "public", PUBLIC_RESULTS
    )

    assert not unsupported_event.allowed
    assert unsupported_event.reason == "unsupported_event"
    assert not unsupported_ref.allowed
    assert unsupported_ref.reason == "unsupported_ref"
    assert not unsupported_pr_ref.allowed
    assert unsupported_pr_ref.reason == "unsupported_ref"


def test_cli_returns_expected_public_mode_exit_codes() -> None:
    root = Path(__file__).resolve().parents[2]
    command = [
        sys.executable,
        "tools/ci_required_gate.py",
        "--event-name",
        "pull_request",
        "--git-ref",
        "refs/pull/7/merge",
        "--governance-mode",
        "public",
    ]
    for name, value in PUBLIC_RESULTS.items():
        command.extend(["--result", f"{name}={value}"])

    allowed = subprocess.run(
        command, cwd=root, text=True, capture_output=True, check=False
    )
    blocked = subprocess.run(
        command[:-2] + ["--result", "governance-mode=failure"],
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
    )
    invalid = subprocess.run(
        command[:-2] + ["--result", "not-a-pair"],
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
    )

    assert allowed.returncode == 0
    assert allowed.stdout == "allowed\n"
    assert blocked.returncode == 1
    assert invalid.returncode == 2
