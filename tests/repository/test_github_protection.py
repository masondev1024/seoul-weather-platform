from __future__ import annotations

import copy
import hashlib
import json
from collections import defaultdict, deque
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tools.github_governance import protection_payload
from tools.github_protection import (
    PLAN_SCHEMA_VERSION,
    GhApiError,
    ProtectionError,
    SubprocessGhRunner,
    apply_plan,
    build_plan,
    main,
    plan_sha256,
    verify_protection,
)


REPOSITORY = "masondev1024/seoul-weather-platform"
BOOTSTRAP_SHA = "0123456789abcdef0123456789abcdef01234567"
OTHER_SHA = "fedcba9876543210fedcba9876543210fedcba98"
APP_ID = 424242
OTHER_APP_ID = 434343
WORKFLOW_RUN_IDS = {"dev": 1101, "main": 1201}
EXPECTED_CHECKS = {
    "dev": ["CI / required"],
    "main": ["CI / required", "Promotion Source / required"],
}
CHECK_APP_IDS = {
    "CI / required": APP_ID,
    "Promotion Source / required": APP_ID,
}
CHECK_RUN_URLS = {
    "dev": {
        "CI / required": (f"https://api.github.com/repos/{REPOSITORY}/check-runs/2101"),
    },
    "main": {
        "CI / required": (f"https://api.github.com/repos/{REPOSITORY}/check-runs/2201"),
        "Promotion Source / required": (
            f"https://api.github.com/repos/{REPOSITORY}/check-runs/2202"
        ),
    },
}


def _endpoint(suffix: str) -> str:
    base = f"/repos/{REPOSITORY}"
    return f"{base}/{suffix}" if suffix else base


def _check_run_endpoint(branch: str, name: str) -> str:
    return CHECK_RUN_URLS[branch][name].removeprefix("https://api.github.com")


def _repo(
    *, default_branch: str = "main", visibility: str = "private"
) -> dict[str, Any]:
    return {
        "full_name": REPOSITORY,
        "default_branch": default_branch,
        "visibility": visibility,
        "private": visibility == "private",
    }


def _branch(name: str, sha: str = BOOTSTRAP_SHA) -> dict[str, Any]:
    return {"name": name, "commit": {"sha": sha}}


def _workflow_runs(
    branch: str,
    *,
    sha: str = BOOTSTRAP_SHA,
    run_id: object | None = None,
    name: object = "CI",
    path: object | None = None,
    event: object = "push",
    head_branch: object | None = None,
    status: object = "completed",
    conclusion: object = "success",
) -> dict[str, Any]:
    resolved_run_id = WORKFLOW_RUN_IDS[branch] if run_id is None else run_id
    return {
        "total_count": 1,
        "workflow_runs": [
            {
                "id": resolved_run_id,
                "name": name,
                "path": path or ".github/workflows/ci.yml",
                "event": event,
                "head_branch": head_branch or branch,
                "head_sha": sha,
                "status": status,
                "conclusion": conclusion,
            }
        ],
    }


def _jobs(
    branch: str,
    *,
    sha: str = BOOTSTRAP_SHA,
    names: tuple[str, ...] | None = None,
    check_run_urls: dict[str, str] | None = None,
) -> dict[str, Any]:
    required_names = tuple(names or EXPECTED_CHECKS[branch])
    urls = check_run_urls or CHECK_RUN_URLS[branch]
    jobs = [
        {
            "id": 3000 + index,
            "run_id": WORKFLOW_RUN_IDS[branch],
            "name": name,
            "head_branch": branch,
            "head_sha": sha,
            "status": "completed",
            "conclusion": "success",
            "check_run_url": urls[name],
        }
        for index, name in enumerate(required_names, start=1)
    ]
    jobs.append(
        {
            "id": 3999,
            "run_id": WORKFLOW_RUN_IDS[branch],
            "name": "Repository Contract",
            "head_branch": branch,
            "head_sha": sha,
            "status": "completed",
            "conclusion": "success",
            "check_run_url": (
                f"https://api.github.com/repos/{REPOSITORY}/check-runs/2999"
            ),
        }
    )
    return {"total_count": len(jobs), "jobs": jobs}


def _check_run(
    name: str,
    *,
    branch: str,
    sha: str = BOOTSTRAP_SHA,
    app_id: object = APP_ID,
    app_slug: object = "github-actions",
    status: object = "completed",
    conclusion: object = "success",
) -> dict[str, Any]:
    url = CHECK_RUN_URLS[branch][name]
    return {
        "id": int(url.rsplit("/", 1)[1]),
        "url": url,
        "name": name,
        "head_sha": sha,
        "status": status,
        "conclusion": conclusion,
        "app": {"id": app_id, "slug": app_slug},
    }


def _workflow_runs_endpoint(branch: str, sha: str = BOOTSTRAP_SHA) -> str:
    return _endpoint(
        "actions/workflows/ci.yml/runs"
        f"?branch={branch}&event=push&head_sha={sha}&status=success&per_page=100"
    )


def _jobs_endpoint(branch: str) -> str:
    return _endpoint(
        f"actions/runs/{WORKFLOW_RUN_IDS[branch]}/jobs?filter=latest&per_page=100"
    )


def _readback(
    branch: str,
    app_ids: dict[str, int] | None = None,
) -> dict[str, Any]:
    payload = protection_payload(  # type: ignore[arg-type]
        branch,
        app_ids or CHECK_APP_IDS,
    )
    result = copy.deepcopy(payload)
    result["enforce_admins"] = {"enabled": payload["enforce_admins"]}
    for field in (
        "required_linear_history",
        "allow_force_pushes",
        "allow_deletions",
        "block_creations",
        "required_conversation_resolution",
        "lock_branch",
        "allow_fork_syncing",
    ):
        result[field] = {"enabled": payload[field]}
    return result


class FakeGhRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self._responses: dict[tuple[str, str], deque[dict[str, Any] | GhApiError]] = (
            defaultdict(deque)
        )

    def add(
        self,
        method: str,
        endpoint: str,
        response: dict[str, Any] | GhApiError,
    ) -> None:
        self._responses[(method, endpoint)].append(response)

    def api(
        self,
        method: str,
        endpoint: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.calls.append((method, endpoint, copy.deepcopy(payload)))
        queued = self._responses[(method, endpoint)]
        if not queued:
            raise AssertionError(f"unexpected gh call: {method} {endpoint}")
        response = queued.popleft()
        if isinstance(response, GhApiError):
            raise response
        return copy.deepcopy(response)


def _add_branch_discovery(
    runner: FakeGhRunner,
    branch: str,
    *,
    workflow_runs: dict[str, Any] | None = None,
    jobs: dict[str, Any] | None = None,
    check_runs: dict[str, dict[str, Any]] | None = None,
    app_ids: dict[str, int] | None = None,
) -> None:
    runner.add(
        "GET",
        _workflow_runs_endpoint(branch),
        workflow_runs if workflow_runs is not None else _workflow_runs(branch),
    )
    runner.add(
        "GET",
        _jobs_endpoint(branch),
        jobs if jobs is not None else _jobs(branch),
    )
    evidence = check_runs or {}
    resolved_app_ids = app_ids or CHECK_APP_IDS
    for name in EXPECTED_CHECKS[branch]:
        runner.add(
            "GET",
            _check_run_endpoint(branch, name),
            evidence.get(
                name,
                _check_run(
                    name,
                    branch=branch,
                    app_id=resolved_app_ids[name],
                ),
            ),
        )


def _add_preflight(
    runner: FakeGhRunner,
    *,
    default_branch: str = "main",
    dev_sha: str = BOOTSTRAP_SHA,
    main_sha: str = BOOTSTRAP_SHA,
    discovery_overrides: dict[str, dict[str, Any]] | None = None,
) -> None:
    runner.add("GET", _endpoint(""), _repo(default_branch=default_branch))
    overrides = discovery_overrides or {}
    for branch, sha in (("dev", dev_sha), ("main", main_sha)):
        runner.add("GET", _endpoint(f"branches/{branch}"), _branch(branch, sha))
        _add_branch_discovery(runner, branch, **overrides.get(branch, {}))


def _add_verify_readbacks(
    runner: FakeGhRunner,
    *,
    check_app_ids: dict[str, int] | None = None,
    protection_app_ids: dict[str, int] | None = None,
    protection_errors: dict[str, GhApiError] | None = None,
) -> None:
    evidence_ids = check_app_ids or CHECK_APP_IDS
    readback_ids = protection_app_ids or CHECK_APP_IDS
    errors = protection_errors or {}
    runner.add("GET", _endpoint(""), _repo())
    for branch in ("dev", "main"):
        runner.add("GET", _endpoint(f"branches/{branch}"), _branch(branch))
        _add_branch_discovery(
            runner,
            branch,
            app_ids=evidence_ids,
        )
        protection_endpoint = _endpoint(f"branches/{branch}/protection")
        runner.add(
            "GET",
            protection_endpoint,
            errors.get(branch, _readback(branch, readback_ids)),
        )


def _expected_plan_without_checksum() -> dict[str, Any]:
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "repository": REPOSITORY,
        "bootstrap_sha": BOOTSTRAP_SHA,
        "expected_default_branch": "main",
        "required_check_runs": [
            "CI / required",
            "Promotion Source / required",
        ],
        "required_check_app_ids": CHECK_APP_IDS,
        "branches": {
            "dev": {
                "head_sha": BOOTSTRAP_SHA,
                "protection_endpoint": _endpoint("branches/dev/protection"),
                "required_checks": ["CI / required"],
                "payload": protection_payload("dev", CHECK_APP_IDS),
            },
            "main": {
                "head_sha": BOOTSTRAP_SHA,
                "protection_endpoint": _endpoint("branches/main/protection"),
                "required_checks": [
                    "CI / required",
                    "Promotion Source / required",
                ],
                "payload": protection_payload("main", CHECK_APP_IDS),
            },
        },
    }


def _seal(plan_without_checksum: dict[str, Any]) -> dict[str, Any]:
    plan = copy.deepcopy(plan_without_checksum)
    canonical = json.dumps(
        plan,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    plan["plan_sha256"] = hashlib.sha256(canonical).hexdigest()
    return plan


def _valid_plan() -> dict[str, Any]:
    return _seal(_expected_plan_without_checksum())


def test_build_plan_queries_exact_branch_workflow_jobs_and_check_runs() -> None:
    runner = FakeGhRunner()
    _add_preflight(runner)

    plan = build_plan(REPOSITORY, BOOTSTRAP_SHA, runner)

    assert plan == _valid_plan()
    assert runner.calls == [
        ("GET", _endpoint(""), None),
        ("GET", _endpoint("branches/dev"), None),
        (
            "GET",
            _workflow_runs_endpoint("dev"),
            None,
        ),
        ("GET", _jobs_endpoint("dev"), None),
        ("GET", _check_run_endpoint("dev", "CI / required"), None),
        ("GET", _endpoint("branches/main"), None),
        (
            "GET",
            _workflow_runs_endpoint("main"),
            None,
        ),
        ("GET", _jobs_endpoint("main"), None),
        ("GET", _check_run_endpoint("main", "CI / required"), None),
        (
            "GET",
            _check_run_endpoint("main", "Promotion Source / required"),
            None,
        ),
    ]
    assert not any("/commits/" in endpoint for _, endpoint, _ in runner.calls)


def test_branch_discovery_passes_validated_check_run_paths_to_gh_cli() -> None:
    runner = FakeGhRunner()
    _add_preflight(runner)

    build_plan(REPOSITORY, BOOTSTRAP_SHA, runner)

    check_run_endpoints = [
        endpoint
        for method, endpoint, _ in runner.calls
        if method == "GET" and "/check-runs/" in endpoint
    ]
    assert check_run_endpoints == [
        _endpoint("check-runs/2101"),
        _endpoint("check-runs/2201"),
        _endpoint("check-runs/2202"),
    ]
    assert not any(endpoint.startswith("https://") for endpoint in check_run_endpoints)


def test_plan_binds_discovered_app_ids_into_payload_and_checksum() -> None:
    runner = FakeGhRunner()
    _add_preflight(runner)

    plan = build_plan(REPOSITORY, BOOTSTRAP_SHA, runner)

    assert plan["required_check_app_ids"] == CHECK_APP_IDS
    assert plan["branches"]["dev"]["payload"]["required_status_checks"]["checks"] == [
        {"context": "CI / required", "app_id": APP_ID}
    ]
    assert plan["branches"]["main"]["payload"]["required_status_checks"]["checks"] == [
        {"context": "CI / required", "app_id": APP_ID},
        {"context": "Promotion Source / required", "app_id": APP_ID},
    ]
    tampered = copy.deepcopy(plan)
    tampered["required_check_app_ids"]["CI / required"] = OTHER_APP_ID
    assert plan_sha256(tampered) != plan["plan_sha256"]


@pytest.mark.parametrize(
    "mutation",
    [
        "missing-run",
        "missing-id",
        "boolean-id",
        "wrong-name",
        "wrong-path",
        "wrong-event",
        "wrong-branch",
        "wrong-head",
        "in-progress",
        "failure",
        "duplicate",
        "malformed-run",
        "missing-total-count",
        "boolean-total-count",
        "truncated-response",
        "over-page-limit",
    ],
)
def test_build_plan_rejects_untrusted_or_ambiguous_branch_workflow_run(
    mutation: str,
) -> None:
    evidence = _workflow_runs("dev")
    run = evidence["workflow_runs"][0]
    if mutation == "missing-run":
        evidence["workflow_runs"] = []
        evidence["total_count"] = 0
    elif mutation == "missing-id":
        run.pop("id")
    elif mutation == "boolean-id":
        run["id"] = True
    elif mutation == "wrong-name":
        run["name"] = "Other"
    elif mutation == "wrong-path":
        run["path"] = ".github/workflows/other.yml"
    elif mutation == "wrong-event":
        run["event"] = "pull_request"
    elif mutation == "wrong-branch":
        run["head_branch"] = "main"
    elif mutation == "wrong-head":
        run["head_sha"] = OTHER_SHA
    elif mutation == "in-progress":
        run["status"] = "in_progress"
        run["conclusion"] = None
    elif mutation == "failure":
        run["conclusion"] = "failure"
    elif mutation == "duplicate":
        evidence["workflow_runs"].append(copy.deepcopy(run))
        evidence["total_count"] = 2
    elif mutation == "malformed-run":
        evidence["workflow_runs"][0] = "not-an-object"
    elif mutation == "missing-total-count":
        evidence.pop("total_count")
    elif mutation == "boolean-total-count":
        evidence["total_count"] = True
    elif mutation == "truncated-response":
        evidence["total_count"] += 1
    else:
        evidence["workflow_runs"] = [copy.deepcopy(run) for _ in range(101)]
        evidence["total_count"] = 101
    runner = FakeGhRunner()
    _add_preflight(
        runner,
        discovery_overrides={"dev": {"workflow_runs": evidence}},
    )

    with pytest.raises(ProtectionError):
        build_plan(REPOSITORY, BOOTSTRAP_SHA, runner)

    assert all(method == "GET" for method, _, _ in runner.calls)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing-required",
        "duplicate-required",
        "wrong-run-id",
        "wrong-branch",
        "wrong-head",
        "in-progress",
        "failure",
        "missing-check-run-url",
        "malformed-check-run-url",
        "wrong-repository-check-run-url",
        "malformed-job",
        "missing-total-count",
        "boolean-total-count",
        "truncated-response",
        "over-page-limit",
    ],
)
def test_build_plan_rejects_invalid_required_job_source(mutation: str) -> None:
    evidence = _jobs("dev")
    required_job = evidence["jobs"][0]
    if mutation == "missing-required":
        evidence["jobs"].pop(0)
        evidence["total_count"] -= 1
    elif mutation == "duplicate-required":
        evidence["jobs"].append(copy.deepcopy(required_job))
        evidence["total_count"] += 1
    elif mutation == "wrong-run-id":
        required_job["run_id"] = WORKFLOW_RUN_IDS["main"]
    elif mutation == "wrong-branch":
        required_job["head_branch"] = "main"
    elif mutation == "wrong-head":
        required_job["head_sha"] = OTHER_SHA
    elif mutation == "in-progress":
        required_job["status"] = "in_progress"
        required_job["conclusion"] = None
    elif mutation == "failure":
        required_job["conclusion"] = "failure"
    elif mutation == "missing-check-run-url":
        required_job.pop("check_run_url")
    elif mutation == "malformed-check-run-url":
        required_job["check_run_url"] = "https://example.invalid/check-runs/2101"
    elif mutation == "wrong-repository-check-run-url":
        required_job["check_run_url"] = (
            "https://api.github.com/repos/other/repository/check-runs/2101"
        )
    elif mutation == "malformed-job":
        evidence["jobs"][0] = "not-an-object"
    elif mutation == "missing-total-count":
        evidence.pop("total_count")
    elif mutation == "boolean-total-count":
        evidence["total_count"] = True
    elif mutation == "truncated-response":
        evidence["total_count"] += 1
    else:
        unrelated = evidence["jobs"][1]
        evidence["jobs"] = [required_job]
        for index in range(100):
            extra = copy.deepcopy(unrelated)
            extra["id"] = 4000 + index
            extra["name"] = f"Unrelated {index}"
            extra["check_run_url"] = (
                f"https://api.github.com/repos/{REPOSITORY}/check-runs/{5000 + index}"
            )
            evidence["jobs"].append(extra)
        evidence["total_count"] = 101
    runner = FakeGhRunner()
    _add_preflight(runner, discovery_overrides={"dev": {"jobs": evidence}})

    with pytest.raises(ProtectionError):
        build_plan(REPOSITORY, BOOTSTRAP_SHA, runner)

    assert all(method == "GET" for method, _, _ in runner.calls)


def test_build_plan_rejects_duplicate_check_run_url_for_required_jobs() -> None:
    evidence = _jobs("main")
    evidence["jobs"][1]["check_run_url"] = evidence["jobs"][0]["check_run_url"]
    runner = FakeGhRunner()
    _add_preflight(runner, discovery_overrides={"main": {"jobs": evidence}})

    with pytest.raises(ProtectionError):
        build_plan(REPOSITORY, BOOTSTRAP_SHA, runner)

    assert all(method == "GET" for method, _, _ in runner.calls)


@pytest.mark.parametrize(
    "mutation",
    [
        "wrong-name",
        "wrong-head",
        "in-progress",
        "failure",
        "missing-app",
        "wrong-slug",
        "null-id",
        "negative-id",
        "boolean-id",
    ],
)
def test_build_plan_rejects_invalid_linked_check_run_source(mutation: str) -> None:
    evidence = _check_run("CI / required", branch="dev")
    if mutation == "wrong-name":
        evidence["name"] = "Promotion Source / required"
    elif mutation == "wrong-head":
        evidence["head_sha"] = OTHER_SHA
    elif mutation == "in-progress":
        evidence["status"] = "in_progress"
        evidence["conclusion"] = None
    elif mutation == "failure":
        evidence["conclusion"] = "failure"
    elif mutation == "missing-app":
        evidence.pop("app")
    elif mutation == "wrong-slug":
        evidence["app"]["slug"] = "other-app"
    elif mutation == "null-id":
        evidence["app"]["id"] = None
    elif mutation == "negative-id":
        evidence["app"]["id"] = -1
    else:
        evidence["app"]["id"] = True
    runner = FakeGhRunner()
    _add_preflight(
        runner,
        discovery_overrides={
            "dev": {"check_runs": {"CI / required": evidence}},
        },
    )

    with pytest.raises(ProtectionError):
        build_plan(REPOSITORY, BOOTSTRAP_SHA, runner)

    assert all(method == "GET" for method, _, _ in runner.calls)


def test_build_plan_rejects_conflicting_app_id_across_branch_bound_sources() -> None:
    runner = FakeGhRunner()
    _add_preflight(
        runner,
        discovery_overrides={
            "main": {
                "app_ids": {
                    "CI / required": OTHER_APP_ID,
                    "Promotion Source / required": APP_ID,
                }
            }
        },
    )

    with pytest.raises(ProtectionError):
        build_plan(REPOSITORY, BOOTSTRAP_SHA, runner)

    assert all(method == "GET" for method, _, _ in runner.calls)


def test_plan_checksum_uses_canonical_json_excluding_only_checksum() -> None:
    expected = _valid_plan()

    assert plan_sha256(expected) == expected["plan_sha256"]


@pytest.mark.parametrize(
    ("default_branch", "dev_sha", "main_sha", "missing_main_job"),
    [
        ("dev", BOOTSTRAP_SHA, BOOTSTRAP_SHA, False),
        ("main", OTHER_SHA, BOOTSTRAP_SHA, False),
        ("main", BOOTSTRAP_SHA, OTHER_SHA, False),
        ("main", BOOTSTRAP_SHA, BOOTSTRAP_SHA, True),
    ],
)
def test_build_plan_fails_closed_on_default_sha_or_workflow_source_mismatch(
    default_branch: str,
    dev_sha: str,
    main_sha: str,
    missing_main_job: bool,
) -> None:
    runner = FakeGhRunner()
    discovery_overrides = (
        {"main": {"jobs": _jobs("main", names=("CI / required",))}}
        if missing_main_job
        else None
    )
    _add_preflight(
        runner,
        default_branch=default_branch,
        dev_sha=dev_sha,
        main_sha=main_sha,
        discovery_overrides=discovery_overrides,
    )

    with pytest.raises(ProtectionError):
        build_plan(REPOSITORY, BOOTSTRAP_SHA, runner)

    assert all(method == "GET" for method, _, _ in runner.calls)


@pytest.mark.parametrize("sha", ["dev", "a" * 39, "A" * 40, "g" * 40])
def test_build_plan_rejects_non_exact_lowercase_commit_sha_without_remote_call(
    sha: str,
) -> None:
    runner = FakeGhRunner()

    with pytest.raises(ProtectionError, match="invalid-bootstrap-sha"):
        build_plan(REPOSITORY, sha, runner)

    assert runner.calls == []


def test_apply_revalidates_plan_and_remote_state_then_puts_and_reads_each_branch() -> (
    None
):
    runner = FakeGhRunner()
    _add_preflight(runner)
    for branch in ("dev", "main"):
        endpoint = _endpoint(f"branches/{branch}/protection")
        runner.add("PUT", endpoint, _readback(branch))
        runner.add("GET", endpoint, _readback(branch))

    result = apply_plan(REPOSITORY, _valid_plan(), BOOTSTRAP_SHA, runner)

    assert result.mode == "protected"
    assert result.release_enabled
    mutation_and_readback = runner.calls[10:]
    assert mutation_and_readback == [
        (
            "PUT",
            _endpoint("branches/dev/protection"),
            protection_payload("dev", CHECK_APP_IDS),
        ),
        ("GET", _endpoint("branches/dev/protection"), None),
        (
            "PUT",
            _endpoint("branches/main/protection"),
            protection_payload("main", CHECK_APP_IDS),
        ),
        ("GET", _endpoint("branches/main/protection"), None),
    ]


@pytest.mark.parametrize("tamper", ["checksum", "repo", "sha", "endpoint", "branch"])
def test_apply_rejects_tampered_or_mismatched_plan_before_remote_calls(
    tamper: str,
) -> None:
    plan = _valid_plan()
    repository = REPOSITORY
    confirm_sha = BOOTSTRAP_SHA
    if tamper == "checksum":
        plan["branches"]["dev"]["payload"]["allow_deletions"] = True
    elif tamper == "repo":
        repository = "other/platform"
    elif tamper == "sha":
        confirm_sha = OTHER_SHA
    elif tamper == "endpoint":
        plan["branches"]["dev"]["protection_endpoint"] = _endpoint(
            "branches/main/protection"
        )
        plan = _seal(
            {key: value for key, value in plan.items() if key != "plan_sha256"}
        )
    else:
        plan["branches"]["release"] = plan["branches"].pop("dev")
        plan = _seal(
            {key: value for key, value in plan.items() if key != "plan_sha256"}
        )
    runner = FakeGhRunner()

    with pytest.raises(ProtectionError):
        apply_plan(repository, plan, confirm_sha, runner)

    assert runner.calls == []


def test_apply_rejects_stale_remote_sha_before_any_put() -> None:
    runner = FakeGhRunner()
    _add_preflight(runner, main_sha=OTHER_SHA)

    with pytest.raises(ProtectionError, match="branch-sha-mismatch"):
        apply_plan(REPOSITORY, _valid_plan(), BOOTSTRAP_SHA, runner)

    assert all(method == "GET" for method, _, _ in runner.calls)


def test_apply_rejects_resealed_app_binding_when_remote_source_changed() -> None:
    plan = _expected_plan_without_checksum()
    changed_ids = {
        "CI / required": OTHER_APP_ID,
        "Promotion Source / required": OTHER_APP_ID,
    }
    plan["required_check_app_ids"] = changed_ids
    for branch in ("dev", "main"):
        plan["branches"][branch]["payload"] = protection_payload(
            branch,  # type: ignore[arg-type]
            changed_ids,
        )
    runner = FakeGhRunner()
    _add_preflight(runner)

    with pytest.raises(ProtectionError, match="required-check-app-mismatch"):
        apply_plan(REPOSITORY, _seal(plan), BOOTSTRAP_SHA, runner)

    assert runner.calls
    assert all(method == "GET" for method, _, _ in runner.calls)


def test_apply_compares_immediate_readback_to_exact_bound_app_id() -> None:
    runner = FakeGhRunner()
    _add_preflight(runner)
    endpoint = _endpoint("branches/dev/protection")
    runner.add("PUT", endpoint, _readback("dev"))
    runner.add(
        "GET",
        endpoint,
        _readback(
            "dev",
            {
                "CI / required": OTHER_APP_ID,
                "Promotion Source / required": APP_ID,
            },
        ),
    )

    with pytest.raises(ProtectionError, match="protection-readback-mismatch"):
        apply_plan(REPOSITORY, _valid_plan(), BOOTSTRAP_SHA, runner)

    assert not any(
        method == "PUT" and endpoint.endswith("/branches/main/protection")
        for method, endpoint, _ in runner.calls
    )


def test_apply_stops_before_main_put_when_dev_readback_differs() -> None:
    runner = FakeGhRunner()
    _add_preflight(runner)
    dev_endpoint = _endpoint("branches/dev/protection")
    runner.add("PUT", dev_endpoint, _readback("dev"))
    weakened = _readback("dev")
    weakened["allow_force_pushes"]["enabled"] = True
    runner.add("GET", dev_endpoint, weakened)

    with pytest.raises(ProtectionError, match="protection-readback-mismatch"):
        apply_plan(REPOSITORY, _valid_plan(), BOOTSTRAP_SHA, runner)

    assert not any(
        method == "PUT" and endpoint.endswith("/branches/main/protection")
        for method, endpoint, _ in runner.calls
    )


@pytest.mark.parametrize("status", [403, 404])
def test_private_put_denial_is_guarded_private_diagnosis_not_success(
    status: int,
) -> None:
    runner = FakeGhRunner()
    _add_preflight(runner)
    endpoint = _endpoint("branches/dev/protection")
    runner.add("PUT", endpoint, GhApiError("PUT", endpoint, status))

    with pytest.raises(ProtectionError) as caught:
        apply_plan(REPOSITORY, _valid_plan(), BOOTSTRAP_SHA, runner)

    assert caught.value.reason == "guarded_private"
    assert not caught.value.release_enabled


def test_verify_rediscovers_each_branch_head_app_source_before_protected() -> None:
    runner = FakeGhRunner()
    _add_verify_readbacks(runner)

    result = verify_protection(REPOSITORY, "main", EXPECTED_CHECKS, runner)

    assert result.mode == "protected"
    assert result.release_enabled
    assert runner.calls == [
        ("GET", _endpoint(""), None),
        ("GET", _endpoint("branches/dev"), None),
        ("GET", _workflow_runs_endpoint("dev"), None),
        ("GET", _jobs_endpoint("dev"), None),
        ("GET", _check_run_endpoint("dev", "CI / required"), None),
        ("GET", _endpoint("branches/dev/protection"), None),
        ("GET", _endpoint("branches/main"), None),
        ("GET", _workflow_runs_endpoint("main"), None),
        ("GET", _jobs_endpoint("main"), None),
        ("GET", _check_run_endpoint("main", "CI / required"), None),
        (
            "GET",
            _check_run_endpoint("main", "Promotion Source / required"),
            None,
        ),
        ("GET", _endpoint("branches/main/protection"), None),
    ]


def test_verify_rejects_protection_readback_bound_to_different_app() -> None:
    runner = FakeGhRunner()
    _add_verify_readbacks(
        runner,
        protection_app_ids={
            "CI / required": OTHER_APP_ID,
            "Promotion Source / required": OTHER_APP_ID,
        },
    )

    result = verify_protection(REPOSITORY, "main", EXPECTED_CHECKS, runner)

    assert result.mode == "invalid"
    assert not result.release_enabled


@pytest.mark.parametrize("status", [403, 404])
def test_verify_normalizes_private_protection_denial_to_guarded_diagnosis(
    status: int,
) -> None:
    runner = FakeGhRunner()
    dev_endpoint = _endpoint("branches/dev/protection")
    _add_verify_readbacks(
        runner,
        protection_errors={
            "dev": GhApiError("GET", dev_endpoint, status),
        },
    )

    result = verify_protection(REPOSITORY, "main", EXPECTED_CHECKS, runner)

    assert result.mode == "guarded_private"
    assert not result.release_enabled


def test_verify_rejects_expected_check_arguments_that_weaken_contract() -> None:
    runner = FakeGhRunner()

    with pytest.raises(ProtectionError, match="invalid-expected-checks"):
        verify_protection(
            REPOSITORY,
            "main",
            {"dev": ["CI / required"], "main": ["CI / required"]},
            runner,
        )

    assert runner.calls == []


def test_injected_runner_keeps_plan_apply_verify_off_real_subprocess(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_subprocess(*args: object, **kwargs: object) -> None:
        raise AssertionError("real subprocess attempted")

    monkeypatch.setattr("tools.github_protection.subprocess.run", unexpected_subprocess)

    plan_runner = FakeGhRunner()
    _add_preflight(plan_runner)
    plan = build_plan(REPOSITORY, BOOTSTRAP_SHA, plan_runner)

    apply_runner = FakeGhRunner()
    _add_preflight(apply_runner)
    for branch in ("dev", "main"):
        endpoint = _endpoint(f"branches/{branch}/protection")
        apply_runner.add("PUT", endpoint, _readback(branch))
        apply_runner.add("GET", endpoint, _readback(branch))
    apply_plan(REPOSITORY, plan, BOOTSTRAP_SHA, apply_runner)

    verify_runner = FakeGhRunner()
    _add_verify_readbacks(verify_runner)
    verify_protection(REPOSITORY, "main", EXPECTED_CHECKS, verify_runner)


def test_subprocess_runner_uses_only_gh_api_argv_and_stdin_without_token_or_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[list[str], dict[str, Any]]] = []

    def fake_run(argv: list[str], **kwargs: Any) -> SimpleNamespace:
        observed.append((argv, kwargs))
        return SimpleNamespace(returncode=0, stdout='{"ok":true}', stderr="")

    monkeypatch.setattr("tools.github_protection.subprocess.run", fake_run)
    runner = SubprocessGhRunner()

    assert runner.api("GET", _endpoint("")) == {"ok": True}
    assert runner.api("PUT", _endpoint("branches/dev/protection"), {"safe": True}) == {
        "ok": True
    }

    for argv, kwargs in observed:
        assert argv[:2] == ["gh", "api"]
        assert "--method" in argv
        assert "X-GitHub-Api-Version: 2022-11-28" in argv
        assert not any("token" in value.lower() for value in argv)
        assert "env" not in kwargs
        assert kwargs["shell"] is False
    assert "--input" not in observed[0][0]
    assert observed[1][0][-2:] == ["--input", "-"]
    assert observed[1][1]["input"] == '{"safe":true}'


def test_subprocess_runner_redacts_failed_command_output() -> None:
    def fake_run(argv: list[str], **kwargs: Any) -> SimpleNamespace:
        del argv, kwargs
        return SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="gh: TOKEN_MARKER private detail (HTTP 403)",
        )

    runner = SubprocessGhRunner(run=fake_run)

    with pytest.raises(GhApiError) as caught:
        runner.api("GET", _endpoint("branches/dev/protection"))

    assert caught.value.status == 403
    assert "TOKEN_MARKER" not in str(caught.value)


def test_subprocess_runner_sanitizes_process_start_failure() -> None:
    def unavailable(argv: list[str], **kwargs: Any) -> SimpleNamespace:
        del argv, kwargs
        raise OSError("TOKEN_MARKER local process detail")

    runner = SubprocessGhRunner(run=unavailable)

    with pytest.raises(GhApiError) as caught:
        runner.api("GET", _endpoint(""))

    assert caught.value.status is None
    assert "TOKEN_MARKER" not in str(caught.value)


def _completed(stdout: object) -> SimpleNamespace:
    return SimpleNamespace(returncode=0, stdout=stdout, stderr="")


def test_subprocess_runner_reads_exact_json_object_list() -> None:
    runner = SubprocessGhRunner(run=lambda *args, **kwargs: _completed('[{"number":7}]'))

    assert runner.api_list(
        "GET", f"/repos/{REPOSITORY}/commits/{BOOTSTRAP_SHA}/pulls?per_page=2&page=1"
    ) == [{"number": 7}]


@pytest.mark.parametrize(
    ("method_name", "stdout"),
    [
        ("api", '{"number":7,"number":8}'),
        ("api_list", '[{"number":7,"number":8}]'),
    ],
)
def test_subprocess_runner_rejects_duplicate_json_keys_without_raw_body(
    method_name: str, stdout: str
) -> None:
    runner = SubprocessGhRunner(run=lambda *args, **kwargs: _completed(stdout))
    call = getattr(runner, method_name)

    with pytest.raises(GhApiError) as caught:
        call(
            "GET",
            f"/repos/{REPOSITORY}/commits/{BOOTSTRAP_SHA}/pulls?per_page=2&page=1",
        )

    assert "number" not in str(caught.value)


@pytest.mark.parametrize(
    "stdout", ['7', '{"number":7}', '[7]', '[[]]', '[{"number":7},null]']
)
def test_subprocess_runner_rejects_non_object_json_list_items(stdout: str) -> None:
    runner = SubprocessGhRunner(run=lambda *args, **kwargs: _completed(stdout))

    with pytest.raises(GhApiError) as caught:
        runner.api_list("GET", f"/repos/{REPOSITORY}/commits/{BOOTSTRAP_SHA}/pulls?per_page=2&page=1")

    assert "number" not in str(caught.value)


def test_subprocess_runner_api_list_rejects_non_get_or_payload_without_response() -> None:
    called = False

    def run(*args: object, **kwargs: object) -> SimpleNamespace:
        nonlocal called
        del args, kwargs
        called = True
        return _completed('[{"number":7}]')

    runner = SubprocessGhRunner(run=run)
    with pytest.raises(GhApiError):
        runner.api_list("PUT", f"/repos/{REPOSITORY}/commits/{BOOTSTRAP_SHA}/pulls?per_page=2&page=1")
    assert not called
    with pytest.raises(TypeError):
        runner.api_list(  # type: ignore[call-arg]
            "GET",
            f"/repos/{REPOSITORY}/commits/{BOOTSTRAP_SHA}/pulls?per_page=2&page=1",
            {"unexpected": True},
        )
    assert not called


@pytest.mark.parametrize("outcome", ["{malformed", OSError("TOKEN_MARKER RAW_RESPONSE")])
def test_subprocess_runner_api_list_sanitizes_malformed_or_failed_subprocess(
    outcome: object,
) -> None:
    def run(*args: object, **kwargs: object) -> SimpleNamespace:
        del args, kwargs
        if isinstance(outcome, Exception):
            raise outcome
        return _completed(outcome)

    runner = SubprocessGhRunner(run=run)
    with pytest.raises(GhApiError) as caught:
        runner.api_list("GET", f"/repos/{REPOSITORY}/commits/{BOOTSTRAP_SHA}/pulls?per_page=2&page=1")

    assert "TOKEN_MARKER" not in str(caught.value)


def test_subprocess_runner_api_list_redacts_failed_command_response() -> None:
    def run(*args: object, **kwargs: object) -> SimpleNamespace:
        del args, kwargs
        return SimpleNamespace(
            returncode=1,
            stdout="RAW_RESPONSE_MARKER",
            stderr="TOKEN_MARKER private API response (HTTP 403)",
        )

    runner = SubprocessGhRunner(run=run)
    with pytest.raises(GhApiError) as caught:
        runner.api_list("GET", f"/repos/{REPOSITORY}/commits/{BOOTSTRAP_SHA}/pulls?per_page=2&page=1")

    assert caught.value.status == 403
    assert "TOKEN_MARKER" not in str(caught.value)
    assert "RAW_RESPONSE_MARKER" not in str(caught.value)


def test_cli_plan_apply_verify_use_injected_runner_and_sanitized_status(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    plan_path = tmp_path / "protection-plan.json"
    plan_runner = FakeGhRunner()
    _add_preflight(plan_runner)

    assert (
        main(
            [
                "plan",
                "--repo",
                REPOSITORY,
                "--bootstrap-sha",
                BOOTSTRAP_SHA,
                "--output",
                str(plan_path),
            ],
            runner=plan_runner,
        )
        == 0
    )
    assert json.loads(plan_path.read_text(encoding="utf-8")) == _valid_plan()
    assert capsys.readouterr().out == "planned\n"

    apply_runner = FakeGhRunner()
    _add_preflight(apply_runner)
    for branch in ("dev", "main"):
        endpoint = _endpoint(f"branches/{branch}/protection")
        apply_runner.add("PUT", endpoint, _readback(branch))
        apply_runner.add("GET", endpoint, _readback(branch))
    assert (
        main(
            [
                "apply",
                "--repo",
                REPOSITORY,
                "--plan",
                str(plan_path),
                "--confirm-bootstrap-sha",
                BOOTSTRAP_SHA,
            ],
            runner=apply_runner,
        )
        == 0
    )
    assert capsys.readouterr().out == "protected\n"

    verify_runner = FakeGhRunner()
    _add_verify_readbacks(verify_runner)
    assert (
        main(
            [
                "verify",
                "--repo",
                REPOSITORY,
                "--expected-default",
                "main",
                "--expected-dev-check",
                "CI / required",
                "--expected-main-check",
                "CI / required",
                "--expected-main-check",
                "Promotion Source / required",
            ],
            runner=verify_runner,
        )
        == 0
    )
    assert capsys.readouterr().out == "protected\n"
