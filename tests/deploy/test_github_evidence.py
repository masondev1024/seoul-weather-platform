from __future__ import annotations

import copy
import json
import socket
import subprocess
from pathlib import Path
from typing import Any

import pytest

from tools.github_governance import protection_payload


REPOSITORY = "masondev1024/seoul-weather-platform"
SHA = "0123456789abcdef0123456789abcdef01234567"
OTHER_SHA = "fedcba9876543210fedcba9876543210fedcba98"
RUN_ID = 90210
APP_ID = 424242
OTHER_APP_ID = 434343
CI_NAME = "CI / required"
PROMOTION_NAME = "Promotion Source / required"
WORKFLOW_REF = f"{REPOSITORY}/.github/workflows/deploy-main.yml@refs/heads/main"
CI_URL = f"https://api.github.com/repos/{REPOSITORY}/check-runs/101"
PROMOTION_URL = f"https://api.github.com/repos/{REPOSITORY}/check-runs/102"

REPO_ENDPOINT = f"/repos/{REPOSITORY}"
RUN_ENDPOINT = f"/repos/{REPOSITORY}/actions/runs/{RUN_ID}"
JOBS_ENDPOINT = f"/repos/{REPOSITORY}/actions/runs/{RUN_ID}/jobs"
CI_CHECK_ENDPOINT = f"/repos/{REPOSITORY}/check-runs/101"
PROMOTION_CHECK_ENDPOINT = f"/repos/{REPOSITORY}/check-runs/102"
DEV_PROTECTION_ENDPOINT = f"/repos/{REPOSITORY}/branches/dev/protection"
MAIN_PROTECTION_ENDPOINT = f"/repos/{REPOSITORY}/branches/main/protection"
MAIN_BRANCH_ENDPOINT = f"/repos/{REPOSITORY}/branches/main"

EXPECTED_CALLS = [
    ("GET", REPO_ENDPOINT),
    ("GET", RUN_ENDPOINT),
    ("GET", JOBS_ENDPOINT),
    ("GET", CI_CHECK_ENDPOINT),
    ("GET", PROMOTION_CHECK_ENDPOINT),
    ("GET", DEV_PROTECTION_ENDPOINT),
    ("GET", MAIN_PROTECTION_ENDPOINT),
    ("GET", MAIN_BRANCH_ENDPOINT),
]


@pytest.fixture(autouse=True)
def forbid_external_processes_and_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def blocked(*args: object, **kwargs: object) -> None:
        raise AssertionError("external side effect attempted")

    monkeypatch.setattr(subprocess, "run", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket.socket, "connect", blocked)


def _event() -> dict[str, Any]:
    return {
        "action": "completed",
        "repository": {"full_name": REPOSITORY, "private": True},
        "workflow_run": {
            "id": RUN_ID,
            "name": "CI",
            "path": ".github/workflows/ci.yml",
            "event": "push",
            "head_branch": "main",
            "head_sha": SHA,
            "status": "completed",
            "conclusion": "success",
            "display_title": "RAW_EVENT_MARKER",
        },
        "sender": {"login": "RAW_EVENT_MARKER"},
    }


def _repo_response() -> dict[str, Any]:
    return {
        "full_name": REPOSITORY,
        "default_branch": "main",
        "visibility": "private",
        "private": True,
        "html_url": "RAW_REPOSITORY_MARKER",
    }


def _run_response() -> dict[str, Any]:
    return {
        "id": RUN_ID,
        "repository": {"full_name": REPOSITORY, "id": 1},
        "name": "CI",
        "path": ".github/workflows/ci.yml",
        "event": "push",
        "head_branch": "main",
        "head_sha": SHA,
        "status": "completed",
        "conclusion": "success",
        "logs_url": "RAW_RUN_MARKER",
    }


def _job(name: str, check_url: str) -> dict[str, Any]:
    return {
        "id": 700,
        "run_id": RUN_ID,
        "name": name,
        "head_branch": "main",
        "head_sha": SHA,
        "status": "completed",
        "conclusion": "success",
        "check_run_url": check_url,
        "steps": [],
    }


def _jobs_response() -> dict[str, Any]:
    return {
        "total_count": 3,
        "jobs": [
            _job(PROMOTION_NAME, PROMOTION_URL),
            _job("lint-extra", f"https://api.github.com/repos/{REPOSITORY}/check-runs/999"),
            _job(CI_NAME, CI_URL),
        ],
    }


def _check_response(name: str, url: str, app_id: object = APP_ID) -> dict[str, Any]:
    return {
        "id": 101,
        "url": url,
        "name": name,
        "head_sha": SHA,
        "status": "completed",
        "conclusion": "success",
        "app": {"id": app_id, "slug": "github-actions", "name": "GitHub Actions"},
        "output": {"summary": "RAW_CHECK_MARKER"},
    }


def _raw_protection(branch: str) -> dict[str, Any]:
    app_ids = {CI_NAME: APP_ID, PROMOTION_NAME: APP_ID}
    value = protection_payload(branch, app_ids)  # type: ignore[arg-type]
    value["required_status_checks"]["contexts"] = [
        item["context"] for item in value["required_status_checks"]["checks"]
    ]
    value["enforce_admins"] = {"enabled": True}
    for field in (
        "required_linear_history",
        "allow_force_pushes",
        "allow_deletions",
        "block_creations",
        "required_conversation_resolution",
        "lock_branch",
        "allow_fork_syncing",
    ):
        value[field] = {"enabled": value[field]}
    value["required_pull_request_reviews"]["dismissal_restrictions"] = {
        "users": [],
        "teams": [],
        "apps": [],
    }
    value["restrictions"] = {"users": [], "teams": [], "apps": []}
    value["url"] = "RAW_PROTECTION_MARKER"
    return value


def _responses() -> dict[str, object]:
    return {
        REPO_ENDPOINT: _repo_response(),
        RUN_ENDPOINT: _run_response(),
        JOBS_ENDPOINT: _jobs_response(),
        CI_CHECK_ENDPOINT: _check_response(CI_NAME, CI_URL),
        PROMOTION_CHECK_ENDPOINT: _check_response(PROMOTION_NAME, PROMOTION_URL),
        DEV_PROTECTION_ENDPOINT: _raw_protection("dev"),
        MAIN_PROTECTION_ENDPOINT: _raw_protection("main"),
        MAIN_BRANCH_ENDPOINT: {
            "name": "main",
            "commit": {"sha": SHA, "url": "RAW_BRANCH_MARKER"},
        },
    }


class FakeGhRunner:
    def __init__(self, responses: dict[str, object]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str]] = []

    def api(self, *args: object) -> dict[str, Any]:
        if len(args) != 2:
            raise AssertionError("payload or unexpected API argument")
        method, endpoint = args
        if type(method) is not str or type(endpoint) is not str:
            raise AssertionError("invalid API argument type")
        if method != "GET" or "?" in endpoint or endpoint.startswith("http"):
            raise AssertionError("unbounded API call")
        self.calls.append((method, endpoint))
        value = self.responses.get(endpoint)
        if isinstance(value, Exception):
            raise value
        if value is None:
            raise RuntimeError("RAW_MISSING_ENDPOINT_MARKER")
        return value  # type: ignore[return-value]


def _write_event(path: Path, event: object | None = None) -> Path:
    path.write_text(
        json.dumps(_event() if event is None else event),
        encoding="utf-8",
        newline="\n",
    )
    return path


def _collect(
    tmp_path: Path,
    *,
    runner: FakeGhRunner | None = None,
    event: object | None = None,
    event_path: Path | None = None,
    workflow_ref: object = WORKFLOW_REF,
    workflow_sha: object = SHA,
    repository: object = REPOSITORY,
    gh_token: object = "TOKEN_MARKER",
) -> tuple[Any, FakeGhRunner]:
    from deployment.github_evidence import read_main_identity_inputs

    active_runner = runner or FakeGhRunner(_responses())
    active_path = event_path or _write_event(tmp_path / "event.json", event)
    inputs = read_main_identity_inputs(
        event_path=active_path,
        workflow_ref=workflow_ref,
        workflow_sha=workflow_sha,
        repository=repository,
        gh_token=gh_token,
        runner=active_runner,
    )
    return inputs, active_runner


def _assert_rejected(
    tmp_path: Path,
    *,
    runner: FakeGhRunner | None = None,
    **overrides: object,
) -> FakeGhRunner:
    from deployment.github_evidence import GithubEvidenceError

    active_runner = runner or FakeGhRunner(_responses())
    with pytest.raises(GithubEvidenceError) as caught:
        _collect(tmp_path, runner=active_runner, **overrides)
    assert caught.value.category == "invalid-github-evidence"
    assert str(caught.value) == "invalid-github-evidence"
    assert "TOKEN_MARKER" not in str(caught.value)
    assert "RAW_" not in str(caught.value)
    return active_runner


def test_valid_evidence_uses_exact_get_order_and_canonical_identity_payload(
    tmp_path: Path,
) -> None:
    from deployment.main_identity import validate_main_deploy_identity

    inputs, runner = _collect(tmp_path)

    assert runner.calls == EXPECTED_CALLS
    assert inputs.workflow_ref == WORKFLOW_REF
    assert inputs.workflow_sha == SHA
    assert inputs.repository == REPOSITORY
    assert inputs.event == {
        "action": "completed",
        "repository": {"full_name": REPOSITORY},
        "workflow_run": {"head_sha": SHA},
    }
    assert inputs.repo == {
        "full_name": REPOSITORY,
        "default_branch": "main",
        "main_branch_sha": SHA,
        "visibility": "private",
        "private": True,
    }
    assert inputs.source_run == {
        "id": RUN_ID,
        "repository": {"full_name": REPOSITORY},
        "name": "CI",
        "path": ".github/workflows/ci.yml",
        "event": "push",
        "head_branch": "main",
        "head_sha": SHA,
        "status": "completed",
        "conclusion": "success",
    }
    assert [job["name"] for job in inputs.source_jobs] == [CI_NAME, PROMOTION_NAME]
    assert [check["name"] for check in inputs.linked_checks] == [
        CI_NAME,
        PROMOTION_NAME,
    ]
    assert inputs.protections == {
        "dev": protection_payload("dev", {CI_NAME: APP_ID}),
        "main": protection_payload(
            "main", {CI_NAME: APP_ID, PROMOTION_NAME: APP_ID}
        ),
    }
    identity = validate_main_deploy_identity(**inputs.as_kwargs())
    assert identity.workflow_sha == SHA
    assert repr(inputs) == "MainIdentityInputs()"
    assert "TOKEN_MARKER" not in repr(inputs)


@pytest.mark.parametrize(
    ("visibility", "private"),
    [("private", True), ("public", False), ("internal", False)],
)
def test_repository_visibility_is_normalized_before_governance_classification(
    tmp_path: Path, visibility: str, private: bool
) -> None:
    responses = _responses()
    responses[REPO_ENDPOINT]["visibility"] = visibility  # type: ignore[index]
    responses[REPO_ENDPOINT]["private"] = private  # type: ignore[index]

    inputs, _ = _collect(tmp_path, runner=FakeGhRunner(responses))

    assert inputs.repo["visibility"] == visibility
    assert inputs.repo["private"] is private


def test_result_is_frozen_and_defensive_against_response_and_caller_mutation(
    tmp_path: Path,
) -> None:
    responses = _responses()
    inputs, _ = _collect(tmp_path, runner=FakeGhRunner(responses))

    responses[REPO_ENDPOINT]["full_name"] = "other/repo"  # type: ignore[index]
    responses[JOBS_ENDPOINT]["jobs"][0]["name"] = "mutated"  # type: ignore[index]
    returned_repo = inputs.repo
    returned_repo["full_name"] = "mutated/repo"
    returned_jobs = inputs.source_jobs
    returned_jobs[0]["name"] = "mutated"

    assert inputs.repo["full_name"] == REPOSITORY
    assert inputs.source_jobs[0]["name"] == CI_NAME
    with pytest.raises(AttributeError):
        inputs.repository = "other/repo"


@pytest.mark.parametrize(
    ("target", "value"),
    [
        ("action", "requested"),
        ("repository", "other/repo"),
        ("id", 0),
        ("id", True),
        ("name", "Other"),
        ("path", ".github/workflows/ci.yml@main"),
        ("event", "pull_request"),
        ("head_branch", "dev"),
        ("head_sha", OTHER_SHA),
        ("head_sha", "A" * 40),
        ("status", "in_progress"),
        ("conclusion", "failure"),
    ],
)
def test_wrong_local_event_envelope_is_rejected_before_api(
    tmp_path: Path, target: str, value: object
) -> None:
    event = _event()
    if target == "action":
        event["action"] = value
    elif target == "repository":
        event["repository"]["full_name"] = value
    else:
        event["workflow_run"][target] = value

    runner = _assert_rejected(tmp_path, event=event)

    assert runner.calls == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("workflow_ref", f"{REPOSITORY}/.github/workflows/deploy-main.yml@refs/heads/dev"),
        ("workflow_ref", f"{REPOSITORY}/.github/workflows/other.yml@refs/heads/main"),
        ("workflow_sha", OTHER_SHA),
        ("workflow_sha", "A" * 40),
        ("repository", "other/repo"),
        ("repository", "owner/repo?x=1"),
    ],
)
def test_wrong_local_invocation_identity_is_rejected_before_api(
    tmp_path: Path, field: str, value: object
) -> None:
    runner = _assert_rejected(tmp_path, **{field: value})

    assert runner.calls == []


@pytest.mark.parametrize(
    "token",
    [None, "", False, 1],
)
def test_missing_or_non_string_gh_token_is_rejected_before_api(
    tmp_path: Path, token: object
) -> None:
    runner = _assert_rejected(tmp_path, gh_token=token)

    assert runner.calls == []


@pytest.mark.parametrize(
    "raw",
    [
        b"\xff\xfe",
        b"not-json",
        b"[]",
        b'{"action":"completed","action":"completed"}',
        b'{"value":NaN}',
    ],
)
def test_malformed_event_seed_is_rejected_before_api(
    tmp_path: Path, raw: bytes
) -> None:
    path = tmp_path / "event.json"
    path.write_bytes(raw)

    runner = _assert_rejected(tmp_path, event_path=path)

    assert runner.calls == []


def test_oversized_event_seed_is_rejected_before_api(tmp_path: Path) -> None:
    path = tmp_path / "event.json"
    path.write_bytes(b"{" + b" " * 65536 + b"}")

    runner = _assert_rejected(tmp_path, event_path=path)

    assert runner.calls == []


def test_non_regular_event_seed_is_rejected_before_api(tmp_path: Path) -> None:
    runner = _assert_rejected(tmp_path, event_path=tmp_path)

    assert runner.calls == []


def test_symlink_event_seed_is_rejected_before_api(tmp_path: Path) -> None:
    target = _write_event(tmp_path / "target.json")
    link = tmp_path / "event-link.json"
    try:
        link.symlink_to(target)
    except OSError as exc:
        pytest.skip(f"symlink unavailable: {exc}")

    runner = _assert_rejected(tmp_path, event_path=link)

    assert runner.calls == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", RUN_ID + 1),
        ("repository", {"full_name": "other/repo"}),
        ("name", "Other"),
        ("path", ".github/workflows/ci.yml@main"),
        ("event", "pull_request"),
        ("head_branch", "dev"),
        ("head_sha", OTHER_SHA),
        ("status", "in_progress"),
        ("conclusion", "failure"),
    ],
)
def test_remote_source_run_must_match_event_run_exactly(
    tmp_path: Path, field: str, value: object
) -> None:
    responses = _responses()
    responses[RUN_ENDPOINT][field] = value  # type: ignore[index]

    runner = _assert_rejected(tmp_path, runner=FakeGhRunner(responses))

    assert runner.calls == EXPECTED_CALLS[:2]


@pytest.mark.parametrize("mutation", ["truncated", "over-100", "duplicate", "missing"])
def test_jobs_page_must_be_complete_and_required_jobs_unique(
    tmp_path: Path, mutation: str
) -> None:
    responses = _responses()
    jobs_response = responses[JOBS_ENDPOINT]
    if mutation == "truncated":
        jobs_response["total_count"] = 4  # type: ignore[index]
    elif mutation == "over-100":
        job = _job("extra", f"https://api.github.com/repos/{REPOSITORY}/check-runs/999")
        jobs_response["jobs"] = [copy.deepcopy(job) for _ in range(101)]  # type: ignore[index]
        jobs_response["total_count"] = 101  # type: ignore[index]
    elif mutation == "duplicate":
        jobs_response["jobs"].append(_job(CI_NAME, f"https://api.github.com/repos/{REPOSITORY}/check-runs/103"))  # type: ignore[index]
        jobs_response["total_count"] = 4  # type: ignore[index]
    else:
        jobs_response["jobs"] = [job for job in jobs_response["jobs"] if job["name"] != CI_NAME]  # type: ignore[index]
        jobs_response["total_count"] = 2  # type: ignore[index]

    runner = _assert_rejected(tmp_path, runner=FakeGhRunner(responses))

    assert runner.calls == EXPECTED_CALLS[:3]


def test_jobs_page_requires_builtin_object(tmp_path: Path) -> None:
    class DictSubclass(dict[str, Any]):
        pass

    responses = _responses()
    responses[JOBS_ENDPOINT] = DictSubclass(responses[JOBS_ENDPOINT])  # type: ignore[arg-type]

    runner = _assert_rejected(tmp_path, runner=FakeGhRunner(responses))

    assert runner.calls == EXPECTED_CALLS[:3]


@pytest.mark.parametrize(
    "url",
    [
        "https://api.github.com/repos/other/repo/check-runs/101",
        f"http://api.github.com/repos/{REPOSITORY}/check-runs/101",
        f"https://api.github.com/repos/{REPOSITORY}/check-runs/0",
        f"https://api.github.com/repos/{REPOSITORY}/check-runs/101?token=TOKEN_MARKER",
    ],
)
def test_foreign_or_malformed_check_run_url_is_never_called(
    tmp_path: Path, url: str
) -> None:
    responses = _responses()
    responses[JOBS_ENDPOINT]["jobs"][2]["check_run_url"] = url  # type: ignore[index]

    runner = _assert_rejected(tmp_path, runner=FakeGhRunner(responses))

    assert runner.calls == EXPECTED_CALLS[:3]


def test_duplicate_check_run_url_is_rejected_before_check_read(tmp_path: Path) -> None:
    responses = _responses()
    responses[JOBS_ENDPOINT]["jobs"][0]["check_run_url"] = CI_URL  # type: ignore[index]

    runner = _assert_rejected(tmp_path, runner=FakeGhRunner(responses))

    assert runner.calls == EXPECTED_CALLS[:3]


@pytest.mark.parametrize(
    ("endpoint", "field", "value", "call_count"),
    [
        (CI_CHECK_ENDPOINT, "url", PROMOTION_URL, 4),
        (CI_CHECK_ENDPOINT, "name", PROMOTION_NAME, 4),
        (CI_CHECK_ENDPOINT, "head_sha", OTHER_SHA, 4),
        (CI_CHECK_ENDPOINT, "status", "in_progress", 4),
        (CI_CHECK_ENDPOINT, "conclusion", "failure", 4),
        (CI_CHECK_ENDPOINT, "app.slug", "other-app", 4),
        (CI_CHECK_ENDPOINT, "app.id", 0, 4),
        (CI_CHECK_ENDPOINT, "app.id", True, 4),
        (PROMOTION_CHECK_ENDPOINT, "app.id", OTHER_APP_ID, 5),
    ],
)
def test_linked_check_fields_and_shared_app_binding_are_exact(
    tmp_path: Path,
    endpoint: str,
    field: str,
    value: object,
    call_count: int,
) -> None:
    responses = _responses()
    if field.startswith("app."):
        responses[endpoint]["app"][field.removeprefix("app.")] = value  # type: ignore[index]
    else:
        responses[endpoint][field] = value  # type: ignore[index]

    runner = _assert_rejected(tmp_path, runner=FakeGhRunner(responses))

    assert runner.calls == EXPECTED_CALLS[:call_count]


@pytest.mark.parametrize(
    ("endpoint", "branch", "call_count"),
    [
        (DEV_PROTECTION_ENDPOINT, "dev", 6),
        (MAIN_PROTECTION_ENDPOINT, "main", 7),
    ],
)
def test_weakened_protection_is_rejected_before_canonicalization(
    tmp_path: Path, endpoint: str, branch: str, call_count: int
) -> None:
    responses = _responses()
    responses[endpoint]["required_linear_history"] = {"enabled": False}  # type: ignore[index]

    runner = _assert_rejected(tmp_path, runner=FakeGhRunner(responses))

    assert runner.calls == EXPECTED_CALLS[:call_count]


def test_main_protection_must_bind_exact_linked_check_app(tmp_path: Path) -> None:
    responses = _responses()
    responses[MAIN_PROTECTION_ENDPOINT]["required_status_checks"]["checks"][0][
        "app_id"
    ] = OTHER_APP_ID  # type: ignore[index]

    runner = _assert_rejected(tmp_path, runner=FakeGhRunner(responses))

    assert runner.calls == EXPECTED_CALLS[:7]


def test_stale_final_main_head_is_rejected(tmp_path: Path) -> None:
    responses = _responses()
    responses[MAIN_BRANCH_ENDPOINT]["commit"]["sha"] = OTHER_SHA  # type: ignore[index]

    runner = _assert_rejected(tmp_path, runner=FakeGhRunner(responses))

    assert runner.calls == EXPECTED_CALLS


def test_cyclic_api_response_is_rejected_with_fixed_error(tmp_path: Path) -> None:
    responses = _responses()
    cyclic = _repo_response()
    cyclic["cycle"] = cyclic
    responses[REPO_ENDPOINT] = cyclic

    runner = _assert_rejected(tmp_path, runner=FakeGhRunner(responses))

    assert runner.calls == EXPECTED_CALLS[:1]


def test_runner_exception_and_raw_body_are_not_exposed(tmp_path: Path) -> None:
    responses = _responses()
    responses[REPO_ENDPOINT] = RuntimeError("TOKEN_MARKER RAW_API_BODY actor-login")

    runner = _assert_rejected(tmp_path, runner=FakeGhRunner(responses))

    assert runner.calls == EXPECTED_CALLS[:1]
