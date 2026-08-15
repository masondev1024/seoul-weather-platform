from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn, Protocol

from tools.github_governance import (
    GovernanceMode,
    classify,
    protection_matches,
    protection_payload,
    release_enabled,
)


PLAN_SCHEMA_VERSION = "weather-github-protection-plan/v1"
GH_API_VERSION = "2026-03-10"

_BRANCHES = ("dev", "main")
_REQUIRED_CHECKS: dict[str, tuple[str, ...]] = {
    "dev": ("CI / required",),
    "main": ("CI / required", "Promotion Source / required"),
}
_REQUIRED_CHECK_RUNS = tuple(
    sorted({context for checks in _REQUIRED_CHECKS.values() for context in checks})
)
_REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_HTTP_STATUS_RE = re.compile(r"\bHTTP\s+(\d{3})\b", re.IGNORECASE)


class ProtectionError(RuntimeError):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        self.release_enabled = False
        super().__init__(reason)


class GhApiError(RuntimeError):
    def __init__(self, method: str, endpoint: str, status: int | None) -> None:
        self.method = method
        self.endpoint = endpoint
        self.status = status
        super().__init__("gh-api-failed")


@dataclass(frozen=True)
class ProtectionVerification:
    mode: GovernanceMode
    release_enabled: bool


class GhRunner(Protocol):
    def api(
        self,
        method: str,
        endpoint: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...

    def api_list(self, method: str, endpoint: str) -> list[dict[str, Any]]: ...


class SubprocessGhRunner:
    def __init__(self, *, run: Callable[..., object] | None = None) -> None:
        self._run = run

    def api(
        self,
        method: str,
        endpoint: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if method not in {"GET", "PUT"}:
            raise GhApiError(method, endpoint, None)
        if method == "GET" and payload is not None:
            raise GhApiError(method, endpoint, None)
        if method == "PUT" and payload is None:
            raise GhApiError(method, endpoint, None)

        value = self._read_json(method, endpoint, payload)
        if not isinstance(value, dict):
            raise GhApiError(method, endpoint, None)
        return value

    def api_list(self, method: str, endpoint: str) -> list[dict[str, Any]]:
        if method != "GET":
            raise GhApiError(method, endpoint, None)
        value = self._read_json(method, endpoint, None)
        if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
            raise GhApiError(method, endpoint, None)
        return value

    def _read_json(
        self,
        method: str,
        endpoint: str,
        payload: dict[str, Any] | None,
    ) -> Any:

        argv = [
            "gh",
            "api",
            "--method",
            method,
            endpoint,
            "--header",
            "Accept: application/vnd.github+json",
            "--header",
            f"X-GitHub-Api-Version: {GH_API_VERSION}",
        ]
        input_text: str | None = None
        if payload is not None:
            argv.extend(["--input", "-"])
            input_text = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )

        run = self._run or subprocess.run
        try:
            result = run(
                argv,
                input=input_text,
                capture_output=True,
                text=True,
                encoding="utf-8",
                check=False,
                shell=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise GhApiError(method, endpoint, None) from exc
        returncode = getattr(result, "returncode", None)
        stdout = getattr(result, "stdout", "")
        stderr = getattr(result, "stderr", "")
        if returncode != 0:
            match = _HTTP_STATUS_RE.search(stderr if isinstance(stderr, str) else "")
            status = int(match.group(1)) if match else None
            raise GhApiError(method, endpoint, status)
        try:
            return json.loads(
                stdout,
                object_pairs_hook=_unique_json_object,
                parse_constant=_reject_json_constant,
            )
        except (TypeError, json.JSONDecodeError) as exc:
            raise GhApiError(method, endpoint, None) from exc


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise GhApiError("GET", "<json>", None)
        result[key] = value
    return result


def _reject_json_constant(value: str) -> NoReturn:
    del value
    raise GhApiError("GET", "<json>", None)


class _SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        self.exit(2, "invalid-input\n")


def _validate_repository(repository: str) -> None:
    if not isinstance(repository, str) or not _REPOSITORY_RE.fullmatch(repository):
        raise ProtectionError("invalid-repository")


def _validate_sha(sha: str) -> None:
    if not isinstance(sha, str) or not _SHA_RE.fullmatch(sha):
        raise ProtectionError("invalid-bootstrap-sha")


def _repo_endpoint(repository: str) -> str:
    return f"/repos/{repository}"


def _branch_endpoint(repository: str, branch: str) -> str:
    return f"{_repo_endpoint(repository)}/branches/{branch}"


def _protection_endpoint(repository: str, branch: str) -> str:
    return f"{_branch_endpoint(repository, branch)}/protection"


def _workflow_runs_endpoint(repository: str, branch: str, sha: str) -> str:
    return (
        f"{_repo_endpoint(repository)}/actions/workflows/ci.yml/runs"
        f"?branch={branch}&event=push&head_sha={sha}&status=success&per_page=100"
    )


def _workflow_jobs_endpoint(repository: str, run_id: int) -> str:
    return (
        f"{_repo_endpoint(repository)}/actions/runs/{run_id}/jobs"
        "?filter=latest&per_page=100"
    )


def _required_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _repository_is_private(repository: Mapping[str, object]) -> bool:
    visibility = repository.get("visibility")
    if isinstance(visibility, str):
        return visibility.casefold() == "private"
    return repository.get("private") is True


def _validate_repository_readback(
    repository: str, metadata: Mapping[str, object]
) -> None:
    if _required_string(metadata.get("full_name")) != repository:
        raise ProtectionError("repository-mismatch")
    if _required_string(metadata.get("default_branch")) != "main":
        raise ProtectionError("default-branch-mismatch")
    visibility = metadata.get("visibility")
    if (
        not (
            isinstance(visibility, str)
            and visibility.casefold() in {"private", "public", "internal"}
        )
        and type(metadata.get("private")) is not bool
    ):
        raise ProtectionError("invalid-repository-readback")


def _branch_sha(branch: str, response: Mapping[str, object]) -> str | None:
    if response.get("name") != branch:
        return None
    commit = response.get("commit")
    if not isinstance(commit, Mapping):
        return None
    return _required_string(commit.get("sha"))


def _positive_integer(value: object) -> int | None:
    return value if type(value) is int and value > 0 else None


def _complete_page(
    response: Mapping[str, object],
    collection_name: str,
    invalid_reason: str,
    incomplete_reason: str,
) -> list[object]:
    items = response.get(collection_name)
    if not isinstance(items, list):
        raise ProtectionError(invalid_reason)
    total_count = response.get("total_count")
    if type(total_count) is not int or total_count != len(items) or len(items) > 100:
        raise ProtectionError(incomplete_reason)
    return items


def _validate_workflow_run(
    response: Mapping[str, object],
    branch: str,
    sha: str,
) -> int:
    workflow_runs = _complete_page(
        response,
        "workflow_runs",
        "invalid-workflow-runs",
        "incomplete-workflow-runs",
    )
    if not workflow_runs:
        raise ProtectionError("missing-workflow-run")
    if len(workflow_runs) != 1:
        raise ProtectionError("duplicate-workflow-run")
    workflow_run = workflow_runs[0]
    if not isinstance(workflow_run, Mapping):
        raise ProtectionError("invalid-workflow-run")
    run_id = _positive_integer(workflow_run.get("id"))
    if (
        run_id is None
        or workflow_run.get("name") != "CI"
        or workflow_run.get("path") != ".github/workflows/ci.yml"
        or workflow_run.get("event") != "push"
        or workflow_run.get("head_branch") != branch
        or workflow_run.get("head_sha") != sha
        or workflow_run.get("status") != "completed"
        or workflow_run.get("conclusion") != "success"
    ):
        raise ProtectionError("invalid-workflow-run")
    return run_id


def _check_run_endpoint(repository: str, value: object) -> str | None:
    if not isinstance(value, str):
        return None
    pattern = (
        rf"https://api\.github\.com/repos/{re.escape(repository)}"
        r"/check-runs/([1-9][0-9]*)"
    )
    match = re.fullmatch(pattern, value)
    if match is None:
        return None
    return f"{_repo_endpoint(repository)}/check-runs/{match.group(1)}"


def _validate_required_jobs(
    response: Mapping[str, object],
    repository: str,
    run_id: int,
    branch: str,
    sha: str,
    required_names: Sequence[str],
) -> dict[str, str]:
    jobs = _complete_page(
        response,
        "jobs",
        "invalid-workflow-jobs",
        "incomplete-workflow-jobs",
    )
    required = set(required_names)
    sources: dict[str, str] = {}
    for job in jobs:
        if not isinstance(job, Mapping):
            raise ProtectionError("invalid-workflow-jobs")
        name = job.get("name")
        if not isinstance(name, str) or not name:
            raise ProtectionError("invalid-workflow-jobs")
        if name not in required:
            continue
        if name in sources:
            raise ProtectionError("duplicate-required-workflow-job")
        if (
            _positive_integer(job.get("run_id")) != run_id
            or job.get("head_branch") != branch
            or job.get("head_sha") != sha
            or job.get("status") != "completed"
            or job.get("conclusion") != "success"
        ):
            raise ProtectionError("invalid-required-workflow-job")
        check_run_endpoint = _check_run_endpoint(repository, job.get("check_run_url"))
        if check_run_endpoint is None or check_run_endpoint in sources.values():
            raise ProtectionError("invalid-required-check-run-url")
        sources[name] = check_run_endpoint
    if set(sources) != required:
        raise ProtectionError("missing-required-workflow-job")
    return {name: sources[name] for name in required_names}


def _validate_linked_check_run(
    response: Mapping[str, object],
    name: str,
    sha: str,
) -> int:
    if response.get("name") != name or response.get("head_sha") != sha:
        raise ProtectionError("invalid-required-check-run")
    if response.get("status") != "completed" or response.get("conclusion") != "success":
        raise ProtectionError("required-check-run-not-successful")
    app = response.get("app")
    if not isinstance(app, Mapping) or app.get("slug") != "github-actions":
        raise ProtectionError("invalid-required-check-app")
    app_id = _positive_integer(app.get("id"))
    if app_id is None:
        raise ProtectionError("invalid-required-check-app")
    return app_id


def _discover_branch_check_app_ids(
    repository: str,
    branch: str,
    sha: str,
    required_names: Sequence[str],
    runner: GhRunner,
) -> dict[str, int]:
    workflow_runs = runner.api("GET", _workflow_runs_endpoint(repository, branch, sha))
    run_id = _validate_workflow_run(workflow_runs, branch, sha)
    jobs = runner.api("GET", _workflow_jobs_endpoint(repository, run_id))
    check_run_urls = _validate_required_jobs(
        jobs,
        repository,
        run_id,
        branch,
        sha,
        required_names,
    )
    sources: dict[str, int] = {}
    for name in required_names:
        check_run = runner.api("GET", check_run_urls[name])
        sources[name] = _validate_linked_check_run(check_run, name, sha)
    return sources


def _merge_check_app_ids(
    discovered: dict[str, int], branch_sources: Mapping[str, int]
) -> None:
    for name, app_id in branch_sources.items():
        previous = discovered.setdefault(name, app_id)
        if previous != app_id:
            raise ProtectionError("conflicting-required-check-app")


def _validate_check_app_ids(check_app_ids: Mapping[str, int]) -> dict[str, int]:
    required = set(_REQUIRED_CHECK_RUNS)
    if set(check_app_ids) != required:
        raise ProtectionError("missing-required-check-run")
    sources: dict[str, int] = {}
    for name in _REQUIRED_CHECK_RUNS:
        app_id = _positive_integer(check_app_ids.get(name))
        if app_id is None:
            raise ProtectionError("invalid-required-check-app")
        sources[name] = app_id
    return sources


def _remote_preflight(
    repository: str, bootstrap_sha: str, runner: GhRunner
) -> tuple[dict[str, Any], dict[str, int]]:
    metadata = runner.api("GET", _repo_endpoint(repository))
    _validate_repository_readback(repository, metadata)
    discovered_app_ids: dict[str, int] = {}
    for branch in _BRANCHES:
        response = runner.api("GET", _branch_endpoint(repository, branch))
        if _branch_sha(branch, response) != bootstrap_sha:
            raise ProtectionError("branch-sha-mismatch")
        branch_app_ids = _discover_branch_check_app_ids(
            repository,
            branch,
            bootstrap_sha,
            _REQUIRED_CHECKS[branch],
            runner,
        )
        _merge_check_app_ids(discovered_app_ids, branch_app_ids)
    return metadata, _validate_check_app_ids(discovered_app_ids)


def plan_sha256(plan: Mapping[str, object]) -> str:
    unsigned = {key: value for key, value in plan.items() if key != "plan_sha256"}
    canonical = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def build_plan(repository: str, bootstrap_sha: str, runner: GhRunner) -> dict[str, Any]:
    _validate_repository(repository)
    _validate_sha(bootstrap_sha)
    _, check_app_ids = _remote_preflight(repository, bootstrap_sha, runner)

    plan: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "repository": repository,
        "bootstrap_sha": bootstrap_sha,
        "expected_default_branch": "main",
        "required_check_runs": list(_REQUIRED_CHECK_RUNS),
        "required_check_app_ids": check_app_ids,
        "branches": {
            branch: {
                "head_sha": bootstrap_sha,
                "protection_endpoint": _protection_endpoint(repository, branch),
                "required_checks": list(_REQUIRED_CHECKS[branch]),
                "payload": protection_payload(branch, check_app_ids),
            }
            for branch in _BRANCHES
        },
    }
    plan["plan_sha256"] = plan_sha256(plan)
    return plan


def _validate_plan(
    repository: str,
    plan: Mapping[str, object],
    confirm_bootstrap_sha: str,
) -> tuple[str, dict[str, int]]:
    _validate_repository(repository)
    _validate_sha(confirm_bootstrap_sha)
    expected_top_level = {
        "schema_version",
        "repository",
        "bootstrap_sha",
        "expected_default_branch",
        "required_check_runs",
        "required_check_app_ids",
        "branches",
        "plan_sha256",
    }
    if set(plan) != expected_top_level:
        raise ProtectionError("invalid-plan-schema")
    if plan.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise ProtectionError("invalid-plan-schema")
    if plan.get("repository") != repository:
        raise ProtectionError("plan-repository-mismatch")
    bootstrap_sha = plan.get("bootstrap_sha")
    if bootstrap_sha != confirm_bootstrap_sha:
        raise ProtectionError("bootstrap-sha-mismatch")
    if not isinstance(bootstrap_sha, str):
        raise ProtectionError("invalid-bootstrap-sha")
    _validate_sha(bootstrap_sha)
    if plan.get("expected_default_branch") != "main":
        raise ProtectionError("default-branch-mismatch")
    if plan.get("required_check_runs") != list(_REQUIRED_CHECK_RUNS):
        raise ProtectionError("invalid-required-check-runs")
    raw_check_app_ids = plan.get("required_check_app_ids")
    if not isinstance(raw_check_app_ids, Mapping) or set(raw_check_app_ids) != set(
        _REQUIRED_CHECK_RUNS
    ):
        raise ProtectionError("invalid-required-check-app-ids")
    check_app_ids: dict[str, int] = {}
    for context in _REQUIRED_CHECK_RUNS:
        app_id = _positive_integer(raw_check_app_ids.get(context))
        if app_id is None:
            raise ProtectionError("invalid-required-check-app-ids")
        check_app_ids[context] = app_id

    checksum = plan.get("plan_sha256")
    if (
        not isinstance(checksum, str)
        or not re.fullmatch(r"[0-9a-f]{64}", checksum)
        or checksum != plan_sha256(plan)
    ):
        raise ProtectionError("plan-checksum-mismatch")

    branches = plan.get("branches")
    if not isinstance(branches, Mapping) or set(branches) != set(_BRANCHES):
        raise ProtectionError("invalid-plan-branches")
    expected_branch_fields = {
        "head_sha",
        "protection_endpoint",
        "required_checks",
        "payload",
    }
    for branch in _BRANCHES:
        branch_plan = branches.get(branch)
        if (
            not isinstance(branch_plan, Mapping)
            or set(branch_plan) != expected_branch_fields
        ):
            raise ProtectionError("invalid-plan-branch")
        if branch_plan.get("head_sha") != bootstrap_sha:
            raise ProtectionError("branch-sha-mismatch")
        if branch_plan.get("protection_endpoint") != _protection_endpoint(
            repository, branch
        ):
            raise ProtectionError("branch-endpoint-mismatch")
        if branch_plan.get("required_checks") != list(_REQUIRED_CHECKS[branch]):
            raise ProtectionError("required-checks-mismatch")
        if branch_plan.get("payload") != protection_payload(branch, check_app_ids):
            raise ProtectionError("protection-payload-mismatch")
    return bootstrap_sha, check_app_ids


def _diagnosis_for_api_failure(
    metadata: Mapping[str, object], error: GhApiError
) -> str:
    if _repository_is_private(metadata) and error.status in {403, 404}:
        return "guarded_private"
    return "github-api-error"


def apply_plan(
    repository: str,
    plan: Mapping[str, object],
    confirm_bootstrap_sha: str,
    runner: GhRunner,
) -> ProtectionVerification:
    bootstrap_sha, planned_app_ids = _validate_plan(
        repository, plan, confirm_bootstrap_sha
    )
    try:
        metadata, current_app_ids = _remote_preflight(repository, bootstrap_sha, runner)
    except GhApiError as exc:
        raise ProtectionError("github-api-error") from exc
    if current_app_ids != planned_app_ids:
        raise ProtectionError("required-check-app-mismatch")

    readbacks: dict[str, Mapping[str, object] | None] = {}
    for branch in _BRANCHES:
        endpoint = _protection_endpoint(repository, branch)
        payload = protection_payload(branch, planned_app_ids)
        try:
            runner.api("PUT", endpoint, payload)
            readback = runner.api("GET", endpoint)
        except GhApiError as exc:
            raise ProtectionError(_diagnosis_for_api_failure(metadata, exc)) from exc
        if not protection_matches(branch, readback, planned_app_ids):
            raise ProtectionError("protection-readback-mismatch")
        readbacks[branch] = readback

    mode = classify(metadata, readbacks)
    if mode != "protected":
        raise ProtectionError(mode)
    return ProtectionVerification(mode=mode, release_enabled=release_enabled(mode))


def _expected_checks_are_exact(
    expected_checks: Mapping[str, Sequence[str]],
) -> bool:
    if set(expected_checks) != set(_BRANCHES):
        return False
    return all(
        tuple(expected_checks.get(branch, ())) == _REQUIRED_CHECKS[branch]
        for branch in _BRANCHES
    )


def verify_protection(
    repository: str,
    expected_default: str,
    expected_checks: Mapping[str, Sequence[str]],
    runner: GhRunner,
) -> ProtectionVerification:
    _validate_repository(repository)
    if expected_default != "main":
        raise ProtectionError("invalid-expected-default")
    if not _expected_checks_are_exact(expected_checks):
        raise ProtectionError("invalid-expected-checks")

    try:
        metadata = runner.api("GET", _repo_endpoint(repository))
    except GhApiError as exc:
        raise ProtectionError("github-api-error") from exc
    try:
        _validate_repository_readback(repository, metadata)
    except ProtectionError:
        return ProtectionVerification(mode="invalid", release_enabled=False)

    protections: dict[str, Mapping[str, object] | None] = {}
    discovered_app_ids: dict[str, int] = {}
    for branch in _BRANCHES:
        try:
            branch_readback = runner.api("GET", _branch_endpoint(repository, branch))
        except GhApiError as exc:
            raise ProtectionError("github-api-error") from exc
        head_sha = _branch_sha(branch, branch_readback)
        if head_sha is None or _SHA_RE.fullmatch(head_sha) is None:
            return ProtectionVerification(mode="invalid", release_enabled=False)
        try:
            branch_app_ids = _discover_branch_check_app_ids(
                repository,
                branch,
                head_sha,
                _REQUIRED_CHECKS[branch],
                runner,
            )
        except GhApiError as exc:
            raise ProtectionError("github-api-error") from exc
        for context, app_id in branch_app_ids.items():
            previous = discovered_app_ids.setdefault(context, app_id)
            if previous != app_id:
                return ProtectionVerification(mode="invalid", release_enabled=False)
        endpoint = _protection_endpoint(repository, branch)
        try:
            protections[branch] = runner.api("GET", endpoint)
        except GhApiError as exc:
            if _repository_is_private(metadata) and exc.status in {403, 404}:
                protections[branch] = None
            else:
                raise ProtectionError("github-api-error") from exc
        if protections[branch] is not None and not protection_matches(
            branch,
            protections[branch],
            branch_app_ids,
        ):
            return ProtectionVerification(mode="invalid", release_enabled=False)
    mode = classify(metadata, protections)
    return ProtectionVerification(mode=mode, release_enabled=release_enabled(mode))


def _read_plan(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ProtectionError("invalid-plan") from exc
    if not isinstance(value, dict):
        raise ProtectionError("invalid-plan")
    return value


def _write_plan(path: Path, plan: Mapping[str, object]) -> None:
    text = json.dumps(plan, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    try:
        path.write_text(text, encoding="utf-8", newline="\n")
    except OSError as exc:
        raise ProtectionError("invalid-output") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = _SanitizedArgumentParser(
        description="Plan, apply, or verify native GitHub branch protection."
    )
    commands = parser.add_subparsers(dest="command", required=True)

    plan = commands.add_parser("plan")
    plan.add_argument("--repo", required=True)
    plan.add_argument("--bootstrap-sha", required=True)
    plan.add_argument("--output", type=Path, required=True)

    apply = commands.add_parser("apply")
    apply.add_argument("--repo", required=True)
    apply.add_argument("--plan", type=Path, required=True)
    apply.add_argument("--confirm-bootstrap-sha", required=True)

    verify = commands.add_parser("verify")
    verify.add_argument("--repo", required=True)
    verify.add_argument("--expected-default", required=True)
    verify.add_argument("--expected-dev-check", action="append", default=[])
    verify.add_argument("--expected-main-check", action="append", default=[])
    return parser


def main(
    argv: list[str] | None = None,
    *,
    runner: GhRunner | None = None,
) -> int:
    args = build_parser().parse_args(argv)
    active_runner = runner or SubprocessGhRunner()
    try:
        if args.command == "plan":
            plan = build_plan(args.repo, args.bootstrap_sha, active_runner)
            _write_plan(args.output, plan)
            print("planned")
            return 0
        if args.command == "apply":
            result = apply_plan(
                args.repo,
                _read_plan(args.plan),
                args.confirm_bootstrap_sha,
                active_runner,
            )
        else:
            result = verify_protection(
                args.repo,
                args.expected_default,
                {
                    "dev": args.expected_dev_check,
                    "main": args.expected_main_check,
                },
                active_runner,
            )
    except GhApiError:
        print("github-api-error")
        return 1
    except ProtectionError as exc:
        print(exc.reason)
        return 1
    print(result.mode)
    return 0 if result.release_enabled else 1


if __name__ == "__main__":
    raise SystemExit(main())
