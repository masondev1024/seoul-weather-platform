from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from tools.promotion_source import (
    validate_main_push_associated_prs,
    validate_pull_request_event,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "github"
REPOSITORY = "masondev1024/seoul-weather-platform"
PUSHED_SHA = "0123456789abcdef0123456789abcdef01234567"
BASE_SHA = "fedcba9876543210fedcba9876543210fedcba98"
ZERO_SHA = "0" * 40


def _fixture(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "tools.promotion_source", *args],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _main_push_event() -> dict[str, object]:
    return {
        "ref": "refs/heads/main",
        "before": BASE_SHA,
        "after": PUSHED_SHA,
        "created": False,
        "deleted": False,
        "repository": {"full_name": REPOSITORY},
    }


def test_pull_request_fixture_accepts_same_repository_dev_to_main() -> None:
    event = _fixture("pull-request-dev-main.json")

    decision = validate_pull_request_event(event, REPOSITORY)

    assert decision.allowed
    assert decision.reason == "allowed"


def test_pull_request_to_main_accepts_same_repository_feature_branch() -> None:
    event = _fixture("pull-request-dev-main.json")
    event["pull_request"]["head"]["ref"] = "feature/weather-copy"

    decision = validate_pull_request_event(event, REPOSITORY)

    assert decision.allowed
    assert decision.reason == "allowed"


def test_pull_request_to_main_accepts_fork_feature_branch() -> None:
    event = _fixture("pull-request-dev-main.json")
    event["pull_request"]["head"]["ref"] = "feature/weather-copy"
    event["pull_request"]["head"]["repo"]["full_name"] = "contributor/fork"

    decision = validate_pull_request_event(event, REPOSITORY)

    assert decision.allowed
    assert decision.reason == "allowed"


def test_pull_request_to_dev_is_not_a_promotion() -> None:
    event = _fixture("pull-request-dev-main.json")
    event["pull_request"]["base"]["ref"] = "dev"
    event["pull_request"]["head"]["ref"] = "feature/weather-copy"
    event["pull_request"]["head"]["repo"]["full_name"] = "contributor/fork"

    decision = validate_pull_request_event(event, REPOSITORY)

    assert decision.allowed
    assert decision.reason == "not-required"


def test_pull_request_to_main_rejects_wrong_base_repository() -> None:
    event = _fixture("pull-request-dev-main.json")
    event["pull_request"]["base"]["repo"]["full_name"] = "other/platform"

    decision = validate_pull_request_event(event, REPOSITORY)

    assert not decision.allowed


def test_main_push_fixture_accepts_exact_merged_dev_to_main_pr() -> None:
    prs = _fixture("push-main-associated-prs.json")

    decision = validate_main_push_associated_prs(
        prs, _main_push_event(), REPOSITORY, PUSHED_SHA
    )

    assert decision.allowed
    assert decision.reason == "allowed"


def test_main_push_accepts_same_repository_feature_pr_merge() -> None:
    prs = copy.deepcopy(_fixture("push-main-associated-prs.json"))
    prs[0]["head"]["ref"] = "feature/weather-copy"

    decision = validate_main_push_associated_prs(
        prs, _main_push_event(), REPOSITORY, PUSHED_SHA
    )

    assert decision.allowed
    assert decision.reason == "allowed"


def test_main_push_accepts_fork_feature_pr_merge() -> None:
    prs = copy.deepcopy(_fixture("push-main-associated-prs.json"))
    prs[0]["head"]["ref"] = "feature/weather-copy"
    prs[0]["head"]["repo"]["full_name"] = "contributor/fork"

    decision = validate_main_push_associated_prs(
        prs, _main_push_event(), REPOSITORY, PUSHED_SHA
    )

    assert decision.allowed
    assert decision.reason == "allowed"


@pytest.mark.parametrize(
    ("field_path", "value"),
    [
        (("base", "repo", "full_name"), "other/platform"),
        (("merged_at",), None),
        (("merge_commit_sha",), "fedcba9876543210fedcba9876543210fedcba98"),
    ],
)
def test_main_push_rejects_non_exact_promotion_evidence(
    field_path: tuple[str, ...], value: object
) -> None:
    prs = copy.deepcopy(_fixture("push-main-associated-prs.json"))
    target = prs[0]
    for field in field_path[:-1]:
        target = target[field]
    target[field_path[-1]] = value

    decision = validate_main_push_associated_prs(
        prs, _main_push_event(), REPOSITORY, PUSHED_SHA
    )

    assert not decision.allowed


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("ref", "refs/heads/dev"),
        ("before", ZERO_SHA),
        ("before", BASE_SHA.upper()),
        ("after", BASE_SHA),
        ("created", True),
        ("deleted", True),
        ("repository", {"full_name": "other/repo"}),
    ],
)
def test_main_push_rejects_non_current_or_bootstrap_push_event(
    field: str, value: object
) -> None:
    prs = _fixture("push-main-associated-prs.json")
    event = _main_push_event()
    event[field] = value

    decision = validate_main_push_associated_prs(
        prs, event, REPOSITORY, PUSHED_SHA
    )

    assert not decision.allowed
    assert decision.reason == "invalid-push-event"


def test_main_push_rejects_historical_merge_sha_replay() -> None:
    prs = _fixture("push-main-associated-prs.json")
    event = _main_push_event()
    event["before"] = "1111111111111111111111111111111111111111"

    decision = validate_main_push_associated_prs(
        prs, event, REPOSITORY, PUSHED_SHA
    )

    assert not decision.allowed
    assert decision.reason == "missing-promotion-evidence"


def test_main_push_requires_at_least_one_exact_associated_pr() -> None:
    prs = _fixture("push-main-associated-prs.json")
    unrelated = copy.deepcopy(prs[0])
    unrelated["base"]["sha"] = "1111111111111111111111111111111111111111"

    assert (
        validate_main_push_associated_prs(
            [], _main_push_event(), REPOSITORY, PUSHED_SHA
        ).allowed
        is False
    )
    assert validate_main_push_associated_prs(
        [unrelated, *prs], _main_push_event(), REPOSITORY, PUSHED_SHA
    ).allowed is False


def test_main_push_rejects_multiple_exact_associated_prs() -> None:
    prs = _fixture("push-main-associated-prs.json")

    decision = validate_main_push_associated_prs(
        [prs[0], copy.deepcopy(prs[0])], _main_push_event(), REPOSITORY, PUSHED_SHA
    )

    assert not decision.allowed
    assert decision.reason == "missing-promotion-evidence"


def test_main_push_rejects_malformed_entry_even_when_valid_evidence_follows() -> None:
    prs = _fixture("push-main-associated-prs.json")

    decision = validate_main_push_associated_prs(
        [{"base": "main"}, *prs], _main_push_event(), REPOSITORY, PUSHED_SHA
    )

    assert not decision.allowed
    assert decision.reason == "invalid-associated-prs"


def test_main_push_rejects_malformed_entry_after_valid_evidence() -> None:
    prs = _fixture("push-main-associated-prs.json")

    decision = validate_main_push_associated_prs(
        [*prs, {"base": "main"}], _main_push_event(), REPOSITORY, PUSHED_SHA
    )

    assert not decision.allowed
    assert decision.reason == "invalid-associated-prs"


def test_cli_accepts_fixture_files_without_network_access() -> None:
    pull_request = _run_cli(
        "pull-request",
        "--event-path",
        str(FIXTURES / "pull-request-dev-main.json"),
        "--repository",
        REPOSITORY,
    )
    main_push = _run_cli(
        "main-push",
        "--event-path",
        str(FIXTURES / "push-main-event.json"),
        "--associated-prs-path",
        str(FIXTURES / "push-main-associated-prs.json"),
        "--repository",
        REPOSITORY,
        "--sha",
        PUSHED_SHA,
    )

    assert pull_request.returncode == 0
    assert pull_request.stdout == "allowed\n"
    assert pull_request.stderr == ""
    assert main_push.returncode == 0
    assert main_push.stdout == "allowed\n"
    assert main_push.stderr == ""


def test_cli_unknown_argument_exits_two_without_printing_raw_values() -> None:
    result = _run_cli(
        "main-push",
        "--event-path",
        str(FIXTURES / "push-main-event.json"),
        "--associated-prs-path",
        str(FIXTURES / "push-main-associated-prs.json"),
        "--repository",
        REPOSITORY,
        "--sha",
        PUSHED_SHA,
        "--extra",
        r"C:\RAW_PATH_MARKER\payload.json",
    )

    output = result.stdout + result.stderr
    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "invalid-input\n"
    assert "RAW_PATH_MARKER" not in output


@pytest.mark.parametrize(
    ("payload", "expected_reason"),
    [
        ('{"pull_request":{"secret":"RAW_PAYLOAD_MARKER"', "invalid-input"),
        ('{"pull_request":{"secret":"RAW_PAYLOAD_MARKER"}}', "invalid-event"),
    ],
)
def test_pull_request_cli_fails_closed_without_printing_raw_input(
    tmp_path: Path, payload: str, expected_reason: str
) -> None:
    input_path = tmp_path / "RAW_PATH_MARKER.json"
    input_path.write_text(payload, encoding="utf-8")

    result = _run_cli(
        "pull-request",
        "--event-path",
        str(input_path),
        "--repository",
        REPOSITORY,
    )

    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert result.stdout == f"{expected_reason}\n"
    assert result.stderr == ""
    assert "RAW_PAYLOAD_MARKER" not in output
    assert "RAW_PATH_MARKER" not in output
    assert "JSONDecodeError" not in output


def test_main_push_cli_rejects_non_array_schema_without_printing_raw_input(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "RAW_PATH_MARKER.json"
    input_path.write_text('{"secret":"RAW_PAYLOAD_MARKER"}', encoding="utf-8")

    result = _run_cli(
        "main-push",
        "--event-path",
        str(input_path),
        "--associated-prs-path",
        str(input_path),
        "--repository",
        REPOSITORY,
        "--sha",
        PUSHED_SHA,
    )

    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert result.stdout == "invalid-input\n"
    assert result.stderr == ""
    assert "RAW_PAYLOAD_MARKER" not in output
    assert "RAW_PATH_MARKER" not in output
