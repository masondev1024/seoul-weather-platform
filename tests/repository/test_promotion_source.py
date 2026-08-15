from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import tools.promotion_source as promotion_source
from tools.promotion_source import (
    validate_main_push_associated_prs,
    validate_pull_request_event,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "github"
REPOSITORY = "masondev1024/seoul-weather-platform"
PUSHED_SHA = "0123456789abcdef0123456789abcdef01234567"
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


def _bootstrap_event() -> dict[str, object]:
    return {
        "ref": "refs/heads/main",
        "before": ZERO_SHA,
        "after": PUSHED_SHA,
        "created": True,
        "deleted": False,
        "repository": {"full_name": REPOSITORY},
    }


def _bootstrap_repository_readback() -> dict[str, str]:
    return {"full_name": REPOSITORY, "default_branch": "dev"}


def _bootstrap_branch_readback(name: str) -> dict[str, str]:
    return {"name": name, "sha": PUSHED_SHA}


def test_pull_request_fixture_accepts_same_repository_dev_to_main() -> None:
    event = _fixture("pull-request-dev-main.json")

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


@pytest.mark.parametrize(
    ("field_path", "value"),
    [
        (("head", "ref"), "feature/weather-copy"),
        (("head", "repo", "full_name"), "contributor/fork"),
        (("base", "repo", "full_name"), "other/platform"),
    ],
)
def test_pull_request_to_main_rejects_non_dev_or_cross_repository_source(
    field_path: tuple[str, ...], value: str
) -> None:
    event = _fixture("pull-request-dev-main.json")
    target = event["pull_request"]
    for field in field_path[:-1]:
        target = target[field]
    target[field_path[-1]] = value

    decision = validate_pull_request_event(event, REPOSITORY)

    assert not decision.allowed


def test_main_push_fixture_accepts_exact_merged_dev_to_main_pr() -> None:
    prs = _fixture("push-main-associated-prs.json")

    decision = validate_main_push_associated_prs(prs, REPOSITORY, PUSHED_SHA)

    assert decision.allowed
    assert decision.reason == "allowed"


@pytest.mark.parametrize(
    ("field_path", "value"),
    [
        (("head", "ref"), "feature/weather-copy"),
        (("head", "repo", "full_name"), "contributor/fork"),
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

    decision = validate_main_push_associated_prs(prs, REPOSITORY, PUSHED_SHA)

    assert not decision.allowed


def test_main_push_requires_at_least_one_exact_associated_pr() -> None:
    prs = _fixture("push-main-associated-prs.json")
    unrelated = copy.deepcopy(prs[0])
    unrelated["head"]["ref"] = "feature/weather-copy"

    assert (
        validate_main_push_associated_prs([], REPOSITORY, PUSHED_SHA).allowed is False
    )
    assert validate_main_push_associated_prs(
        [unrelated, *prs], REPOSITORY, PUSHED_SHA
    ).allowed


def test_main_push_rejects_malformed_entry_even_when_valid_evidence_follows() -> None:
    prs = _fixture("push-main-associated-prs.json")

    decision = validate_main_push_associated_prs(
        [{"base": "main"}, *prs], REPOSITORY, PUSHED_SHA
    )

    assert not decision.allowed
    assert decision.reason == "invalid-associated-prs"


def test_main_push_rejects_malformed_entry_after_valid_evidence() -> None:
    prs = _fixture("push-main-associated-prs.json")

    decision = validate_main_push_associated_prs(
        [*prs, {"base": "main"}], REPOSITORY, PUSHED_SHA
    )

    assert not decision.allowed
    assert decision.reason == "invalid-associated-prs"


def test_initial_main_bootstrap_accepts_only_exact_guarded_creation_evidence() -> None:
    decision = promotion_source.validate_initial_main_bootstrap_push(
        _bootstrap_event(),
        _bootstrap_repository_readback(),
        _bootstrap_branch_readback("dev"),
        _bootstrap_branch_readback("main"),
        REPOSITORY,
        PUSHED_SHA,
        "guarded_private",
    )

    assert decision.allowed
    assert decision.reason == "initial-bootstrap"


@pytest.mark.parametrize(
    "case",
    [
        "protected-mode",
        "missing-mode",
        "created-false",
        "created-string",
        "deleted-true",
        "nonzero-before",
        "wrong-ref",
        "wrong-after",
        "event-repository-mismatch",
        "remote-repository-mismatch",
        "default-already-main",
        "dev-name-mismatch",
        "dev-sha-mismatch",
        "main-name-mismatch",
        "main-sha-mismatch",
        "uppercase-sha",
        "extra-remote-field",
        "missing-event-repository",
        "missing-branch-sha",
    ],
)
def test_initial_main_bootstrap_rejects_any_weakened_identity_evidence(
    case: str,
) -> None:
    event = _bootstrap_event()
    repository_readback = _bootstrap_repository_readback()
    dev_readback = _bootstrap_branch_readback("dev")
    main_readback = _bootstrap_branch_readback("main")
    repository = REPOSITORY
    sha = PUSHED_SHA
    governance_mode = "guarded_private"

    if case == "protected-mode":
        governance_mode = "protected"
    elif case == "missing-mode":
        governance_mode = ""
    elif case == "created-false":
        event["created"] = False
    elif case == "created-string":
        event["created"] = "true"
    elif case == "deleted-true":
        event["deleted"] = True
    elif case == "nonzero-before":
        event["before"] = PUSHED_SHA
    elif case == "wrong-ref":
        event["ref"] = "refs/heads/dev"
    elif case == "wrong-after":
        event["after"] = ZERO_SHA
    elif case == "event-repository-mismatch":
        event["repository"] = {"full_name": "attacker/fork"}
    elif case == "remote-repository-mismatch":
        repository_readback["full_name"] = "attacker/fork"
    elif case == "default-already-main":
        repository_readback["default_branch"] = "main"
    elif case == "dev-name-mismatch":
        dev_readback["name"] = "main"
    elif case == "dev-sha-mismatch":
        dev_readback["sha"] = ZERO_SHA
    elif case == "main-name-mismatch":
        main_readback["name"] = "dev"
    elif case == "main-sha-mismatch":
        main_readback["sha"] = ZERO_SHA
    elif case == "uppercase-sha":
        sha = PUSHED_SHA.upper()
    elif case == "extra-remote-field":
        repository_readback["private"] = "true"
    elif case == "missing-event-repository":
        event.pop("repository")
    elif case == "missing-branch-sha":
        main_readback.pop("sha")

    decision = promotion_source.validate_initial_main_bootstrap_push(
        event,
        repository_readback,
        dev_readback,
        main_readback,
        repository,
        sha,
        governance_mode,
    )

    assert not decision.allowed
    assert decision.reason == "invalid-bootstrap-source"


def test_initial_main_bootstrap_cli_accepts_sanitized_readbacks(
    tmp_path: Path,
) -> None:
    paths = {
        "event": tmp_path / "event.json",
        "repository": tmp_path / "repository.json",
        "dev": tmp_path / "dev.json",
        "main": tmp_path / "main.json",
    }
    payloads = {
        "event": _bootstrap_event(),
        "repository": _bootstrap_repository_readback(),
        "dev": _bootstrap_branch_readback("dev"),
        "main": _bootstrap_branch_readback("main"),
    }
    for name, path in paths.items():
        path.write_text(json.dumps(payloads[name]), encoding="utf-8")

    result = _run_cli(
        "initial-main-bootstrap",
        "--event-path",
        str(paths["event"]),
        "--repository-readback-path",
        str(paths["repository"]),
        "--dev-branch-readback-path",
        str(paths["dev"]),
        "--main-branch-readback-path",
        str(paths["main"]),
        "--repository",
        REPOSITORY,
        "--sha",
        PUSHED_SHA,
        "--governance-mode",
        "guarded_private",
    )

    assert result.returncode == 0
    assert result.stdout == "initial-bootstrap\n"
    assert result.stderr == ""


def test_initial_main_bootstrap_cli_fails_closed_without_raw_input(
    tmp_path: Path,
) -> None:
    event_path = tmp_path / "RAW_PATH_MARKER-event.json"
    event_path.write_text('{"secret":"RAW_PAYLOAD_MARKER"}', encoding="utf-8")
    readback = tmp_path / "readback.json"
    readback.write_text("{}", encoding="utf-8")

    result = _run_cli(
        "initial-main-bootstrap",
        "--event-path",
        str(event_path),
        "--repository-readback-path",
        str(readback),
        "--dev-branch-readback-path",
        str(readback),
        "--main-branch-readback-path",
        str(readback),
        "--repository",
        REPOSITORY,
        "--sha",
        PUSHED_SHA,
        "--governance-mode",
        "guarded_private",
    )

    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert result.stdout == "invalid-bootstrap-source\n"
    assert result.stderr == ""
    assert "RAW_PAYLOAD_MARKER" not in output
    assert "RAW_PATH_MARKER" not in output


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
