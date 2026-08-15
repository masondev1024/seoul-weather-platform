from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from tools.ci_required_gate import decide_required_ci


BASE = {
    "repository-contract": "success",
    "dbt-weather": "success",
    "airflow-tests": "success",
    "dagbag-policy": "success",
    "dagbag-runtime": "success",
    "promotion-source": "success",
    "governance-mode": "success",
}


def test_pull_request_requires_runtime_skip() -> None:
    assert decide_required_ci(
        "pull_request", "refs/pull/7/merge", "protected",
        BASE | {"dagbag-runtime": "skipped"},
    ).allowed


def test_protected_main_push_requires_runtime_success() -> None:
    assert decide_required_ci("push", "refs/heads/main", "protected", BASE).allowed


def test_protected_dev_push_requires_runtime_success() -> None:
    assert decide_required_ci("push", "refs/heads/dev", "protected", BASE).allowed


def test_pull_request_rejects_runtime_execution() -> None:
    assert not decide_required_ci(
        "pull_request", "refs/pull/7/merge", "protected", BASE,
    ).allowed


def test_guarded_private_push_requires_runtime_skip_and_reports_degraded() -> None:
    decision = decide_required_ci(
        "push", "refs/heads/main", "guarded_private",
        BASE | {"dagbag-runtime": "skipped"},
    )
    assert decision.allowed and decision.reason == "degraded_guarded_private"


def test_missing_or_non_success_required_result_fails_closed() -> None:
    missing = decide_required_ci(
        "push", "refs/heads/main", "protected", {key: value for key, value in BASE.items() if key != "dbt-weather"},
    )
    malformed = decide_required_ci(
        "push", "refs/heads/main", "protected", BASE | {"dbt-weather": "cancelled"},
    )

    assert not missing.allowed
    assert not malformed.allowed


def test_unsupported_event_ref_or_governance_mode_fails_closed() -> None:
    unsupported_event = decide_required_ci("workflow_dispatch", "refs/heads/main", "protected", BASE)
    unsupported_ref = decide_required_ci("push", "refs/heads/feature", "protected", BASE)
    unsupported_mode = decide_required_ci("push", "refs/heads/main", "open", BASE)

    assert not unsupported_event.allowed
    assert not unsupported_ref.allowed
    assert not unsupported_mode.allowed


def test_pull_request_requires_all_other_results_to_succeed() -> None:
    decision = decide_required_ci(
        "pull_request", "refs/pull/7/merge", "protected",
        BASE | {"dagbag-runtime": "skipped", "airflow-tests": "skipped"},
    )

    assert not decision.allowed


def test_cli_returns_expected_exit_codes() -> None:
    root = Path(__file__).resolve().parents[2]
    command = [
        sys.executable,
        "tools/ci_required_gate.py",
        "--event-name", "pull_request",
        "--git-ref", "refs/pull/7/merge",
        "--governance-mode", "protected",
    ]
    for name, value in (BASE | {"dagbag-runtime": "skipped"}).items():
        command.extend(["--result", f"{name}={value}"])

    allowed = subprocess.run(command, cwd=root, text=True, capture_output=True, check=False)
    blocked = subprocess.run(
        command[:-2] + ["--result", "governance-mode=failure"],
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
    )
    invalid = subprocess.run(command[:-2] + ["--result", "not-a-pair"], cwd=root, text=True, capture_output=True, check=False)

    assert allowed.returncode == 0
    assert blocked.returncode == 1
    assert invalid.returncode == 2
