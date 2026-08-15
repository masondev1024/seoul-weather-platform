from __future__ import annotations

import copy
import socket
import subprocess
from collections.abc import Iterator
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


def _repo() -> dict[str, Any]:
    return {
        "full_name": REPOSITORY,
        "default_branch": "main",
        "main_branch_sha": SHA,
        "visibility": "private",
        "private": True,
    }


def _protections(app_id: int = APP_ID) -> dict[str, Any]:
    app_ids = {CI_NAME: app_id, PROMOTION_NAME: app_id}
    return {
        "dev": protection_payload("dev", app_ids),
        "main": protection_payload("main", app_ids),
    }


def _source_run() -> dict[str, Any]:
    return {
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


def _job(name: str, url: str) -> dict[str, Any]:
    return {
        "run_id": RUN_ID,
        "name": name,
        "head_branch": "main",
        "head_sha": SHA,
        "status": "completed",
        "conclusion": "success",
        "check_run_url": url,
    }


def _source_jobs() -> list[dict[str, Any]]:
    return [_job(CI_NAME, CI_URL), _job(PROMOTION_NAME, PROMOTION_URL)]


def _check(name: str, url: str, app_id: object = APP_ID) -> dict[str, Any]:
    return {
        "url": url,
        "name": name,
        "head_sha": SHA,
        "status": "completed",
        "conclusion": "success",
        "app": {"id": app_id, "slug": "github-actions"},
    }


def _linked_checks() -> list[dict[str, Any]]:
    return [_check(CI_NAME, CI_URL), _check(PROMOTION_NAME, PROMOTION_URL)]


def _event() -> dict[str, Any]:
    return {
        "action": "completed",
        "repository": {"full_name": REPOSITORY},
        "workflow_run": {"head_sha": SHA},
    }


def _valid_inputs() -> dict[str, Any]:
    return {
        "event": _event(),
        "workflow_ref": WORKFLOW_REF,
        "workflow_sha": SHA,
        "repository": REPOSITORY,
        "repo": _repo(),
        "protections": _protections(),
        "source_run": _source_run(),
        "source_jobs": _source_jobs(),
        "linked_checks": _linked_checks(),
    }


@pytest.fixture(autouse=True)
def forbid_side_effects(monkeypatch: pytest.MonkeyPatch) -> None:
    def blocked(*args: object, **kwargs: object) -> None:
        raise AssertionError("side effect attempted")

    monkeypatch.setattr(subprocess, "run", blocked)
    monkeypatch.setattr(Path, "write_text", blocked)
    monkeypatch.setattr(Path, "write_bytes", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket.socket, "connect", blocked)


def _validate(**overrides: Any) -> Any:
    from deployment.main_identity import validate_main_deploy_identity

    payload = _valid_inputs()
    payload.update(overrides)
    return validate_main_deploy_identity(**payload)


def _assert_rejected(**overrides: Any) -> None:
    from deployment.main_identity import MainIdentityError

    with pytest.raises(MainIdentityError) as caught:
        _validate(**overrides)
    assert caught.value.category
    assert str(caught.value) == caught.value.category
    unsafe_text = f"{REPOSITORY} {CI_URL} TOKEN_MARKER actor-login"
    assert not any(part in str(caught.value) for part in unsafe_text.split())


def test_accepts_exact_main_deploy_identity_and_returns_immutable_scalars() -> None:
    identity = _validate()

    assert identity.repository == REPOSITORY
    assert identity.workflow_ref == WORKFLOW_REF
    assert identity.workflow_sha == SHA
    assert identity.source_run_id == RUN_ID
    assert identity.checks == (
        (CI_NAME, "/repos/masondev1024/seoul-weather-platform/check-runs/101", APP_ID),
        (
            PROMOTION_NAME,
            "/repos/masondev1024/seoul-weather-platform/check-runs/102",
            APP_ID,
        ),
    )
    with pytest.raises(AttributeError):
        identity.repository = "other/repo"  # type: ignore[misc]


def test_accepts_required_jobs_in_any_input_order_with_deterministic_output() -> None:
    identity = _validate(source_jobs=list(reversed(_source_jobs())))

    assert identity.checks == (
        (CI_NAME, "/repos/masondev1024/seoul-weather-platform/check-runs/101", APP_ID),
        (
            PROMOTION_NAME,
            "/repos/masondev1024/seoul-weather-platform/check-runs/102",
            APP_ID,
        ),
    )


@pytest.mark.parametrize(
    "event",
    [
        {"action": "completed", "repository": {"full_name": REPOSITORY}},
        {
            "action": "completed",
            "repository": {"full_name": REPOSITORY},
            "workflow_run": {"head_sha": SHA},
            "extra": "field",
        },
    ],
)
def test_rejects_missing_or_extra_event_contract_keys(event: dict[str, Any]) -> None:
    _assert_rejected(event=event)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("action", "requested"),
        ("repository", {"full_name": "other/repo"}),
        ("workflow_run", {"head_sha": OTHER_SHA}),
    ],
)
def test_rejects_non_exact_event_identity(field: str, value: object) -> None:
    event = _event()
    event[field] = value
    _assert_rejected(event=event)


@pytest.mark.parametrize(
    ("workflow_ref", "repository"),
    [
        (f"{REPOSITORY}/.github/workflows/deploy-main.yml@refs/pull/1/merge", REPOSITORY),
        (f"{REPOSITORY}/.github/workflows/deploy-main.yml@refs/heads/dev", REPOSITORY),
        (f"{REPOSITORY}/.github/workflows/other.yml@refs/heads/main", REPOSITORY),
        (WORKFLOW_REF, "other/repo"),
    ],
)
def test_rejects_pr_or_wrong_workflow_reference(
    workflow_ref: str, repository: str
) -> None:
    _assert_rejected(workflow_ref=workflow_ref, repository=repository)


@pytest.mark.parametrize("sha", [OTHER_SHA, "A" * 40, "g" * 40, "a" * 39, True])
def test_rejects_stale_or_non_exact_workflow_sha(sha: object) -> None:
    _assert_rejected(workflow_sha=sha)


@pytest.mark.parametrize(
    "protections",
    [
        {"dev": None, "main": _protections()["main"]},
        {"dev": _protections()["dev"], "main": None},
        {"dev": _protections()["dev"], "main": {"bad": True}},
    ],
)
def test_rejects_guarded_private_or_invalid_governance(
    protections: dict[str, Any]
) -> None:
    _assert_rejected(protections=protections)


def test_rejects_main_protection_not_bound_to_linked_app_id() -> None:
    _assert_rejected(protections=_protections(OTHER_APP_ID))


@pytest.mark.parametrize(
    "mutation",
    [
        "extra-branch-key",
        "missing-top-level-key",
        "extra-status-key",
        "missing-status-strict",
        "extra-check-key",
        "missing-check-context",
        "extra-review-key",
        "missing-review-bypass",
    ],
)
def test_rejects_non_exact_nested_protection_shape(mutation: str) -> None:
    protections = _protections()
    main = protections["main"]
    if mutation == "extra-branch-key":
        protections["release"] = copy.deepcopy(main)
    elif mutation == "missing-top-level-key":
        main.pop("allow_fork_syncing")
    elif mutation == "extra-status-key":
        main["required_status_checks"]["contexts"] = [CI_NAME, PROMOTION_NAME]
    elif mutation == "missing-status-strict":
        main["required_status_checks"].pop("strict")
    elif mutation == "extra-check-key":
        main["required_status_checks"]["checks"][0]["url"] = "TOKEN_MARKER"
    elif mutation == "missing-check-context":
        main["required_status_checks"]["checks"][0].pop("context")
    elif mutation == "extra-review-key":
        main["required_pull_request_reviews"]["dismissal_restrictions"] = {
            "users": [],
            "teams": [],
            "apps": [],
        }
    else:
        main["required_pull_request_reviews"].pop("bypass_pull_request_allowances")

    _assert_rejected(protections=protections)


def test_rejects_stale_remote_main_branch_sha() -> None:
    repo = _repo()
    repo["main_branch_sha"] = OTHER_SHA

    _assert_rejected(repo=repo)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", True),
        ("id", 0),
        ("repository", {"full_name": "other/repo"}),
        ("name", "Deploy"),
        ("path", ".github/workflows/other.yml"),
        ("event", "pull_request"),
        ("head_branch", "dev"),
        ("head_sha", OTHER_SHA),
        ("status", "in_progress"),
        ("conclusion", "failure"),
    ],
)
def test_rejects_failed_incomplete_duplicate_or_wrong_source_run(
    field: str, value: object
) -> None:
    run = _source_run()
    run[field] = value
    _assert_rejected(source_run=run)


@pytest.mark.parametrize("mutation", ["missing", "extra", "duplicate"])
def test_rejects_source_run_missing_extra_or_duplicate_contract_keys(
    mutation: str,
) -> None:
    run = _source_run()
    if mutation == "missing":
        run.pop("path")
    elif mutation == "extra":
        run["html_url"] = "TOKEN_MARKER"
    else:
        _assert_rejected(source_run=[run, copy.deepcopy(run)])
        return
    _assert_rejected(source_run=run)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "duplicate-name",
        "wrong-run",
        "wrong-branch",
        "wrong-sha",
        "incomplete",
        "failed",
        "foreign-url",
        "duplicate-url",
        "extra-required",
        "bool-run",
        "not-list",
    ],
)
def test_rejects_missing_duplicate_wrong_run_or_foreign_required_job(
    mutation: str,
) -> None:
    jobs: object = _source_jobs()
    if mutation == "missing":
        jobs = [copy.deepcopy(_source_jobs()[0])]
    elif mutation == "duplicate-name":
        jobs.append(copy.deepcopy(jobs[0]))  # type: ignore[index, union-attr]
    elif mutation == "wrong-run":
        jobs[0]["run_id"] = RUN_ID + 1  # type: ignore[index]
    elif mutation == "wrong-branch":
        jobs[0]["head_branch"] = "dev"  # type: ignore[index]
    elif mutation == "wrong-sha":
        jobs[0]["head_sha"] = OTHER_SHA  # type: ignore[index]
    elif mutation == "incomplete":
        jobs[0]["status"] = "in_progress"  # type: ignore[index]
    elif mutation == "failed":
        jobs[0]["conclusion"] = "failure"  # type: ignore[index]
    elif mutation == "foreign-url":
        jobs[0]["check_run_url"] = "https://api.github.com/repos/other/repo/check-runs/101"  # type: ignore[index]
    elif mutation == "duplicate-url":
        jobs[1]["check_run_url"] = jobs[0]["check_run_url"]  # type: ignore[index]
    elif mutation == "extra-required":
        jobs.append(_job("Other / required", f"https://api.github.com/repos/{REPOSITORY}/check-runs/103"))  # type: ignore[union-attr]
    elif mutation == "bool-run":
        jobs[0]["run_id"] = True  # type: ignore[index]
    else:
        jobs = {"total_count": 2, "jobs": jobs}
    _assert_rejected(source_jobs=jobs)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "duplicate-url",
        "wrong-name",
        "wrong-sha",
        "incomplete",
        "failed",
        "wrong-slug",
        "wrong-app",
        "bool-app",
        "not-list",
        "extra",
    ],
)
def test_rejects_untrusted_or_ambiguous_linked_check(mutation: str) -> None:
    checks: object = _linked_checks()
    if mutation == "missing":
        checks = [copy.deepcopy(_linked_checks()[0])]
    elif mutation == "duplicate-url":
        checks.append(copy.deepcopy(checks[0]))  # type: ignore[index, union-attr]
    elif mutation == "wrong-name":
        checks[0]["name"] = PROMOTION_NAME  # type: ignore[index]
    elif mutation == "wrong-sha":
        checks[0]["head_sha"] = OTHER_SHA  # type: ignore[index]
    elif mutation == "incomplete":
        checks[0]["status"] = "in_progress"  # type: ignore[index]
    elif mutation == "failed":
        checks[0]["conclusion"] = "failure"  # type: ignore[index]
    elif mutation == "wrong-slug":
        checks[0]["app"]["slug"] = "other-app"  # type: ignore[index]
    elif mutation == "wrong-app":
        checks[0]["app"]["id"] = OTHER_APP_ID  # type: ignore[index]
    elif mutation == "bool-app":
        checks[0]["app"]["id"] = True  # type: ignore[index]
    elif mutation == "not-list":
        checks = {"total_count": 2, "check_runs": checks}
    else:
        checks[0]["html_url"] = "TOKEN_MARKER"  # type: ignore[index]
    _assert_rejected(linked_checks=checks)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        (
            "event",
            {
                "action": "completed",
                "repository": {"full_name": REPOSITORY},
                "workflow_run": {"head_sha": SHA, "actor": object()},
            },
        ),
        (
            "source_jobs",
            [_job(CI_NAME, CI_URL), _job(PROMOTION_NAME, PROMOTION_URL), {"name": None}],
        ),
    ],
)
def test_rejects_non_json_domain_values(name: str, value: object) -> None:
    _assert_rejected(**{name: value})


def test_snapshots_inputs_before_validation_and_ignores_caller_mutation() -> None:
    inputs = _valid_inputs()
    identity = _validate(**inputs)
    inputs["event"]["workflow_run"]["head_sha"] = OTHER_SHA
    inputs["source_jobs"][0]["name"] = "mutated"

    assert identity.workflow_sha == SHA
    assert identity.checks[0][0] == CI_NAME


class MutatingMapping(dict[str, Any]):
    def items(self) -> Iterator[tuple[str, Any]]:  # type: ignore[override]
        self["extra"] = "mutated"
        return super().items()


def test_rejects_stateful_mapping_instead_of_observing_later_state() -> None:
    event = MutatingMapping(_event())

    _assert_rejected(event=event)


@pytest.mark.parametrize("name", ["event", "source_jobs"])
def test_rejects_cyclic_json_containers_with_safe_error(name: str) -> None:
    if name == "event":
        value: Any = _event()
        value["cycle"] = value
    else:
        value = _source_jobs()
        value.append(value)

    _assert_rejected(**{name: value})
