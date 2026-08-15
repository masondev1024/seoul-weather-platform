from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from tools.github_governance import classify, protection_matches


_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_BOOTSTRAP_SHA = "0" * 40
_CHECK_RUN_URL_RE = re.compile(
    r"^https://api\.github\.com/repos/(?P<repo>[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/check-runs/(?P<id>[1-9][0-9]*)$"
)
_REQUIRED_CHECKS = ("CI / required", "Promotion Source / required")
_DAGBAG_RUNTIME_JOB = "dagbag-runtime"
_SAFE_CATEGORY = "invalid-main-deploy-identity"
_PROTECTION_TOP_LEVEL_KEYS = {
    "required_status_checks",
    "enforce_admins",
    "required_pull_request_reviews",
    "restrictions",
    "required_linear_history",
    "allow_force_pushes",
    "allow_deletions",
    "block_creations",
    "required_conversation_resolution",
    "lock_branch",
    "allow_fork_syncing",
}
_PROTECTION_FLAGS = {
    "required_linear_history": True,
    "allow_force_pushes": False,
    "allow_deletions": False,
    "block_creations": False,
    "required_conversation_resolution": True,
    "lock_branch": False,
    "allow_fork_syncing": False,
}


class MainIdentityError(RuntimeError):
    def __init__(self, category: str = _SAFE_CATEGORY) -> None:
        self.category = category
        super().__init__(category)


@dataclass(frozen=True)
class MainDeployIdentity:
    repository: str
    workflow_ref: str
    workflow_sha: str
    source_run_id: int
    checks: tuple[tuple[str, str, int], ...]


def _reject() -> None:
    raise MainIdentityError()


def _snapshot(value: object, seen: set[int] | None = None) -> object:
    active = seen if seen is not None else set()
    if type(value) is dict:
        value_id = id(value)
        if value_id in active:
            _reject()
        active.add(value_id)
        result = {key: _snapshot(child, active) for key, child in value.items()}
        active.remove(value_id)
        return result
    if type(value) is list:
        value_id = id(value)
        if value_id in active:
            _reject()
        active.add(value_id)
        result = [_snapshot(child, active) for child in value]
        active.remove(value_id)
        return result
    if value is None or type(value) in {str, int, bool}:
        return value
    _reject()


def _exact_mapping(value: object, keys: set[str]) -> dict[str, Any]:
    if type(value) is not dict or set(value) != keys:
        _reject()
    return value


def _exact_list(value: object, *, length: int | None = None) -> list[Any]:
    if type(value) is not list:
        _reject()
    if length is not None and len(value) != length:
        _reject()
    return value


def _string(value: object) -> str:
    if type(value) is not str or not value:
        _reject()
    return value


def _positive_int(value: object) -> int:
    if type(value) is not int or value <= 0:
        _reject()
    return value


def _sha(value: object) -> str:
    text = _string(value)
    if _SHA_RE.fullmatch(text) is None:
        _reject()
    return text


def _deploy_sha(value: object) -> str:
    sha = _sha(value)
    if sha == _BOOTSTRAP_SHA:
        _reject()
    return sha


def _repo_full_name(value: object) -> str:
    mapping = _exact_mapping(value, {"full_name"})
    return _string(mapping["full_name"])


def _check_run_endpoint(repository: str, value: object) -> str:
    url = _string(value)
    match = _CHECK_RUN_URL_RE.fullmatch(url)
    if match is None or match.group("repo") != repository:
        _reject()
    return f"/repos/{repository}/check-runs/{match.group('id')}"


def _validate_event(event: object, repository: str, sha: str) -> None:
    event_map = _exact_mapping(event, {"action", "repository", "workflow_run"})
    if event_map["action"] != "completed":
        _reject()
    if _repo_full_name(event_map["repository"]) != repository:
        _reject()
    workflow_run = _exact_mapping(event_map["workflow_run"], {"head_sha"})
    if _sha(workflow_run["head_sha"]) != sha:
        _reject()


def _validate_repo(repo: object, repository: str, sha: str) -> dict[str, Any]:
    repo_map = _exact_mapping(
        repo, {"full_name", "default_branch", "main_branch_sha", "visibility", "private"}
    )
    if repo_map["full_name"] != repository:
        _reject()
    if repo_map["default_branch"] != "main":
        _reject()
    if _sha(repo_map["main_branch_sha"]) != sha:
        _reject()
    if type(repo_map["private"]) is not bool:
        _reject()
    visibility = _string(repo_map["visibility"])
    if (
        visibility not in {"private", "public", "internal"}
        or repo_map["private"] is not (visibility == "private")
    ):
        _reject()
    return repo_map


def _validate_source_run(source_run: object, repository: str, sha: str) -> int:
    run = _exact_mapping(
        source_run,
        {
            "id",
            "repository",
            "name",
            "path",
            "event",
            "head_branch",
            "head_sha",
            "status",
            "conclusion",
        },
    )
    run_id = _positive_int(run["id"])
    if (
        _repo_full_name(run["repository"]) != repository
        or run["name"] != "CI"
        or run["path"] != ".github/workflows/ci.yml"
        or run["event"] != "push"
        or run["head_branch"] != "main"
        or _sha(run["head_sha"]) != sha
        or run["status"] != "completed"
        or run["conclusion"] != "success"
    ):
        _reject()
    return run_id


def _validate_jobs(
    source_jobs: object,
    repository: str,
    run_id: int,
    sha: str,
    governance_mode: object,
) -> dict[str, str]:
    jobs = _exact_list(source_jobs, length=len(_REQUIRED_CHECKS) + 1)
    endpoints: dict[str, str] = {}
    urls: set[str] = set()
    runtime_seen = False
    expected_runtime_conclusion = (
        "success" if governance_mode == "protected" else "skipped"
    )
    for job_value in jobs:
        job = _exact_mapping(
            job_value,
            {
                "run_id",
                "name",
                "head_branch",
                "head_sha",
                "status",
                "conclusion",
                "check_run_url",
            },
        )
        name = _string(job["name"])
        if name in endpoints or (name == _DAGBAG_RUNTIME_JOB and runtime_seen):
            _reject()
        url = _string(job["check_run_url"])
        if url in urls:
            _reject()
        urls.add(url)
        endpoint = _check_run_endpoint(repository, url)
        if (
            _positive_int(job["run_id"]) != run_id
            or job["head_branch"] != "main"
            or _sha(job["head_sha"]) != sha
            or job["status"] != "completed"
        ):
            _reject()
        if name == _DAGBAG_RUNTIME_JOB:
            if job["conclusion"] != expected_runtime_conclusion:
                _reject()
            runtime_seen = True
            continue
        if name not in _REQUIRED_CHECKS or job["conclusion"] != "success":
            _reject()
        endpoints[name] = endpoint
    if set(endpoints) != set(_REQUIRED_CHECKS) or not runtime_seen:
        _reject()
    return endpoints


def _validate_linked_checks(
    linked_checks: object,
    endpoints_by_name: dict[str, str],
    repository: str,
    sha: str,
) -> tuple[tuple[str, str, int], ...]:
    checks = _exact_list(linked_checks, length=len(_REQUIRED_CHECKS))
    by_endpoint: dict[str, tuple[str, int]] = {}
    for check_value in checks:
        check = _exact_mapping(
            check_value,
            {"url", "name", "head_sha", "status", "conclusion", "app"},
        )
        name = _string(check["name"])
        endpoint = _check_run_endpoint(repository, check["url"])
        app = _exact_mapping(check["app"], {"id", "slug"})
        app_id = _positive_int(app["id"])
        if (
            name not in _REQUIRED_CHECKS
            or endpoints_by_name.get(name) != endpoint
            or endpoint in by_endpoint
            or _sha(check["head_sha"]) != sha
            or check["status"] != "completed"
            or check["conclusion"] != "success"
            or app["slug"] != "github-actions"
        ):
            _reject()
        by_endpoint[endpoint] = (name, app_id)
    result = tuple(
        (name, endpoints_by_name[name], by_endpoint[endpoints_by_name[name]][1])
        for name in _REQUIRED_CHECKS
    )
    if len({app_id for _, _, app_id in result}) != 1:
        _reject()
    return result


def _validate_governance(
    repo: dict[str, Any],
    governance_mode: object,
    protections: object,
    expected_app_ids: dict[str, int],
) -> None:
    if governance_mode == "guarded_private":
        if (
            repo["visibility"] != "private"
            or repo["private"] is not True
            or protections is not None
        ):
            _reject()
        return
    if governance_mode != "protected":
        _reject()
    protections_map = _exact_mapping(protections, {"dev", "main"})
    _validate_exact_protection("dev", protections_map["dev"], expected_app_ids)
    _validate_exact_protection("main", protections_map["main"], expected_app_ids)
    if classify(repo, protections_map) != "protected":
        _reject()
    main = protections_map["main"]
    if not protection_matches("main", main, expected_app_ids):
        _reject()


def _validate_promotion_pr(value: object, repository: str, sha: str) -> None:
    pull_request = _exact_mapping(
        value, {"number", "merged_at", "merge_commit_sha", "base", "head"}
    )
    base = _exact_mapping(pull_request["base"], {"ref", "repo"})
    head = _exact_mapping(pull_request["head"], {"ref", "repo"})
    if (
        _positive_int(pull_request["number"]) <= 0
        or _string(pull_request["merged_at"]) == ""
        or _sha(pull_request["merge_commit_sha"]) != sha
        or base["ref"] != "main"
        or _repo_full_name(base["repo"]) != repository
        or head["ref"] != "dev"
        or _repo_full_name(head["repo"]) != repository
    ):
        _reject()


def _validate_exact_protection(
    branch: str, protection: object, expected_app_ids: dict[str, int]
) -> None:
    protection_map = _exact_mapping(protection, _PROTECTION_TOP_LEVEL_KEYS)
    status = _exact_mapping(
        protection_map["required_status_checks"], {"strict", "checks"}
    )
    if status["strict"] is not True:
        _reject()
    expected_contexts = (
        ("CI / required",)
        if branch == "dev"
        else ("CI / required", "Promotion Source / required")
    )
    checks = _exact_list(status["checks"], length=len(expected_contexts))
    for check, context in zip(checks, expected_contexts, strict=True):
        check_map = _exact_mapping(check, {"context", "app_id"})
        if check_map["context"] != context:
            _reject()
        if _positive_int(check_map["app_id"]) != expected_app_ids[context]:
            _reject()
    if protection_map["enforce_admins"] is not True:
        _reject()
    reviews = _exact_mapping(
        protection_map["required_pull_request_reviews"],
        {
            "dismiss_stale_reviews",
            "require_code_owner_reviews",
            "required_approving_review_count",
            "require_last_push_approval",
            "bypass_pull_request_allowances",
        },
    )
    if (
        reviews["dismiss_stale_reviews"] is not True
        or reviews["require_code_owner_reviews"] is not False
        or type(reviews["required_approving_review_count"]) is not int
        or reviews["required_approving_review_count"] != 0
        or reviews["require_last_push_approval"] is not False
    ):
        _reject()
    bypass = _exact_mapping(
        reviews["bypass_pull_request_allowances"], {"users", "teams", "apps"}
    )
    if any(_exact_list(bypass[key], length=0) != [] for key in bypass):
        _reject()
    if protection_map["restrictions"] is not None:
        _reject()
    for field, expected in _PROTECTION_FLAGS.items():
        if protection_map[field] is not expected:
            _reject()


def validate_main_deploy_identity(
    *,
    event: object,
    workflow_ref: object,
    workflow_sha: object,
    repository: object,
    repo: object,
    governance_mode: object,
    promotion_pr: object,
    protections: object,
    source_run: object,
    source_jobs: object,
    linked_checks: object,
) -> MainDeployIdentity:
    event_s = _snapshot(event)
    workflow_ref_s = _snapshot(workflow_ref)
    workflow_sha_s = _snapshot(workflow_sha)
    repository_s = _snapshot(repository)
    repo_s = _snapshot(repo)
    governance_mode_s = _snapshot(governance_mode)
    promotion_pr_s = _snapshot(promotion_pr)
    protections_s = _snapshot(protections)
    source_run_s = _snapshot(source_run)
    source_jobs_s = _snapshot(source_jobs)
    linked_checks_s = _snapshot(linked_checks)

    repository_text = _string(repository_s)
    workflow_ref_text = _string(workflow_ref_s)
    workflow_sha_text = _deploy_sha(workflow_sha_s)
    if (
        workflow_ref_text
        != f"{repository_text}/.github/workflows/deploy-main.yml@refs/heads/main"
    ):
        _reject()

    _validate_event(event_s, repository_text, workflow_sha_text)
    repo_map = _validate_repo(repo_s, repository_text, workflow_sha_text)
    _validate_promotion_pr(promotion_pr_s, repository_text, workflow_sha_text)
    run_id = _validate_source_run(source_run_s, repository_text, workflow_sha_text)
    endpoints_by_name = _validate_jobs(
        source_jobs_s,
        repository_text,
        run_id,
        workflow_sha_text,
        governance_mode_s,
    )
    checks = _validate_linked_checks(
        linked_checks_s, endpoints_by_name, repository_text, workflow_sha_text
    )
    expected_app_ids = {name: app_id for name, _, app_id in checks}
    _validate_governance(
        repo_map, governance_mode_s, protections_s, expected_app_ids
    )
    return MainDeployIdentity(
        repository=repository_text,
        workflow_ref=workflow_ref_text,
        workflow_sha=workflow_sha_text,
        source_run_id=run_id,
        checks=checks,
    )
