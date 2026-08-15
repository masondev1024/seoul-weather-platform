from __future__ import annotations

import json
import math
import os
import re
import stat
from dataclasses import dataclass
from typing import Any, NoReturn

from tools.github_governance import protection_matches, protection_payload
from tools.github_protection import GhRunner


_SAFE_CATEGORY = "invalid-github-evidence"
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_BOOTSTRAP_SHA = "0" * 40
_REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_REQUIRED_CHECKS = ("CI / required", "Promotion Source / required")
_DEFAULT_MAX_EVENT_BYTES = 65_536


class GithubEvidenceError(RuntimeError):
    def __init__(self, category: str = _SAFE_CATEGORY) -> None:
        self.category = category
        super().__init__(category)


def _reject() -> NoReturn:
    raise GithubEvidenceError() from None


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            _reject()
        result[key] = value
    return result


def _reject_json_constant(value: str) -> NoReturn:
    del value
    _reject()


def load_event_seed(
    path: str | os.PathLike[str], max_bytes: int = _DEFAULT_MAX_EVENT_BYTES
) -> dict[str, Any]:
    if type(max_bytes) is not int or max_bytes <= 0:
        _reject()
    try:
        path_value = os.fspath(path)
        before = os.lstat(path_value)
        if not stat.S_ISREG(before.st_mode) or before.st_size > max_bytes:
            _reject()
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path_value, flags)
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_size > max_bytes
                or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
            ):
                _reject()
            chunks: list[bytes] = []
            remaining = max_bytes + 1
            while remaining > 0:
                chunk = os.read(descriptor, min(remaining, 8192))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            raw = b"".join(chunks)
            if len(raw) > max_bytes:
                _reject()
        finally:
            os.close(descriptor)
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except GithubEvidenceError:
        raise
    except (OSError, TypeError, ValueError, UnicodeError, json.JSONDecodeError):
        _reject()
    if type(value) is not dict:
        _reject()
    return value


def _snapshot_json(value: object, active: set[int] | None = None) -> object:
    seen = active if active is not None else set()
    if type(value) is dict:
        identity = id(value)
        if identity in seen:
            _reject()
        seen.add(identity)
        result: dict[str, object] = {}
        for key, child in value.items():
            if type(key) is not str:
                _reject()
            result[key] = _snapshot_json(child, seen)
        seen.remove(identity)
        return result
    if type(value) is list:
        identity = id(value)
        if identity in seen:
            _reject()
        seen.add(identity)
        result = [_snapshot_json(child, seen) for child in value]
        seen.remove(identity)
        return result
    if value is None or type(value) in {str, int, bool}:
        return value
    if type(value) is float and math.isfinite(value):
        return value
    _reject()


def _mapping(value: object) -> dict[str, Any]:
    if type(value) is not dict:
        _reject()
    return value


def _sequence(value: object) -> list[Any]:
    if type(value) is not list:
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


def _repository(value: object) -> str:
    text = _string(value)
    if _REPOSITORY_RE.fullmatch(text) is None:
        _reject()
    return text


def _repository_name(value: object) -> str:
    return _string(_mapping(value).get("full_name"))


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError):
        _reject()


@dataclass(frozen=True, slots=True, repr=False)
class MainIdentityInputs:
    workflow_ref: str
    workflow_sha: str
    repository: str
    governance_mode: str
    _event_json: str
    _repo_json: str
    _protections_json: str
    _promotion_pr_json: str
    _source_run_json: str
    _source_jobs_json: str
    _linked_checks_json: str

    def __repr__(self) -> str:
        return "MainIdentityInputs()"

    @staticmethod
    def _decode(value: str) -> Any:
        return json.loads(value)

    @property
    def event(self) -> dict[str, Any]:
        return self._decode(self._event_json)

    @property
    def repo(self) -> dict[str, Any]:
        return self._decode(self._repo_json)

    @property
    def protections(self) -> dict[str, Any] | None:
        return self._decode(self._protections_json)

    @property
    def promotion_pr(self) -> dict[str, Any]:
        return self._decode(self._promotion_pr_json)

    @property
    def source_run(self) -> dict[str, Any]:
        return self._decode(self._source_run_json)

    @property
    def source_jobs(self) -> list[dict[str, Any]]:
        return self._decode(self._source_jobs_json)

    @property
    def linked_checks(self) -> list[dict[str, Any]]:
        return self._decode(self._linked_checks_json)

    def as_kwargs(self) -> dict[str, object]:
        return {
            "event": self.event,
            "workflow_ref": self.workflow_ref,
            "workflow_sha": self.workflow_sha,
            "repository": self.repository,
            "repo": self.repo,
            "governance_mode": self.governance_mode,
            "promotion_pr": self.promotion_pr,
            "protections": self.protections,
            "source_run": self.source_run,
            "source_jobs": self.source_jobs,
            "linked_checks": self.linked_checks,
        }


def _build_inputs(
    *,
    event: dict[str, Any],
    workflow_ref: str,
    workflow_sha: str,
    repository: str,
    governance_mode: str,
    repo: dict[str, Any],
    protections: dict[str, Any] | None,
    promotion_pr: dict[str, Any],
    source_run: dict[str, Any],
    source_jobs: list[dict[str, Any]],
    linked_checks: list[dict[str, Any]],
) -> MainIdentityInputs:
    return MainIdentityInputs(
        workflow_ref=workflow_ref,
        workflow_sha=workflow_sha,
        repository=repository,
        governance_mode=governance_mode,
        _event_json=_canonical_json(event),
        _repo_json=_canonical_json(repo),
        _protections_json=_canonical_json(protections),
        _promotion_pr_json=_canonical_json(promotion_pr),
        _source_run_json=_canonical_json(source_run),
        _source_jobs_json=_canonical_json(source_jobs),
        _linked_checks_json=_canonical_json(linked_checks),
    )


def _validate_local_envelope(
    event: dict[str, Any],
    workflow_ref: object,
    workflow_sha: object,
    repository: object,
) -> tuple[str, str, int, dict[str, Any]]:
    repository_text = _repository(repository)
    workflow_sha_text = _deploy_sha(workflow_sha)
    if (
        _string(workflow_ref)
        != f"{repository_text}/.github/workflows/deploy-main.yml@refs/heads/main"
    ):
        _reject()
    repository_event = _mapping(event.get("repository"))
    workflow_run = _mapping(event.get("workflow_run"))
    run_id = _positive_int(workflow_run.get("id"))
    if (
        event.get("action") != "completed"
        or _repository_name(repository_event) != repository_text
        or workflow_run.get("name") != "CI"
        or workflow_run.get("path") != ".github/workflows/ci.yml"
        or workflow_run.get("event") != "push"
        or workflow_run.get("head_branch") != "main"
        or _sha(workflow_run.get("head_sha")) != workflow_sha_text
        or workflow_run.get("status") != "completed"
        or workflow_run.get("conclusion") != "success"
    ):
        _reject()
    normalized_event = {
        "action": "completed",
        "repository": {"full_name": repository_text},
        "workflow_run": {"head_sha": workflow_sha_text},
    }
    return repository_text, workflow_sha_text, run_id, normalized_event


def _api_get(runner: GhRunner, endpoint: str) -> dict[str, Any]:
    if not endpoint.startswith("/repos/") or "?" in endpoint:
        _reject()
    try:
        response = runner.api("GET", endpoint)
        snapshot = _snapshot_json(response)
    except GithubEvidenceError:
        raise
    except Exception:
        _reject()
    return _mapping(snapshot)


def _api_list(runner: GhRunner, endpoint: str) -> list[dict[str, Any]]:
    try:
        response = runner.api_list("GET", endpoint)
        snapshot = _snapshot_json(response)
    except GithubEvidenceError:
        raise
    except Exception:
        _reject()
    return [_mapping(value) for value in _sequence(snapshot)]


def _normalize_repo(response: dict[str, Any], repository: str) -> dict[str, Any]:
    visibility = _string(response.get("visibility"))
    private = response.get("private")
    if (
        _repository_name(response) != repository
        or response.get("default_branch") != "main"
        or visibility not in {"private", "public", "internal"}
        or type(private) is not bool
        or private is not (visibility == "private")
    ):
        _reject()
    return {
        "full_name": repository,
        "default_branch": "main",
        "visibility": visibility,
        "private": private,
    }


def _normalize_source_run(
    response: dict[str, Any], repository: str, run_id: int, sha: str
) -> dict[str, Any]:
    if (
        _positive_int(response.get("id")) != run_id
        or _repository_name(response.get("repository")) != repository
        or response.get("name") != "CI"
        or response.get("path") != ".github/workflows/ci.yml"
        or response.get("event") != "push"
        or response.get("head_branch") != "main"
        or _sha(response.get("head_sha")) != sha
        or response.get("status") != "completed"
        or response.get("conclusion") != "success"
    ):
        _reject()
    return {
        "id": run_id,
        "repository": {"full_name": repository},
        "name": "CI",
        "path": ".github/workflows/ci.yml",
        "event": "push",
        "head_branch": "main",
        "head_sha": sha,
        "status": "completed",
        "conclusion": "success",
    }


def _check_run_url(repository: str, value: object) -> tuple[str, str]:
    url = _string(value)
    pattern = re.compile(
        rf"^https://api\.github\.com/repos/{re.escape(repository)}/check-runs/([1-9][0-9]*)$"
    )
    match = pattern.fullmatch(url)
    if match is None:
        _reject()
    return url, f"/repos/{repository}/check-runs/{match.group(1)}"


def _normalize_jobs(
    response: dict[str, Any], repository: str, run_id: int, sha: str
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    jobs = _sequence(response.get("jobs"))
    total_count = response.get("total_count")
    if (
        type(total_count) is not int
        or total_count != len(jobs)
        or len(jobs) > 100
    ):
        _reject()
    required: dict[str, dict[str, Any]] = {}
    endpoints: dict[str, str] = {}
    urls: set[str] = set()
    for value in jobs:
        job = _mapping(value)
        name = job.get("name")
        if name not in _REQUIRED_CHECKS:
            continue
        if name in required:
            _reject()
        url, endpoint = _check_run_url(repository, job.get("check_run_url"))
        if url in urls:
            _reject()
        urls.add(url)
        if (
            _positive_int(job.get("run_id")) != run_id
            or job.get("head_branch") != "main"
            or _sha(job.get("head_sha")) != sha
            or job.get("status") != "completed"
            or job.get("conclusion") != "success"
        ):
            _reject()
        required[name] = {
            "run_id": run_id,
            "name": name,
            "head_branch": "main",
            "head_sha": sha,
            "status": "completed",
            "conclusion": "success",
            "check_run_url": url,
        }
        endpoints[name] = endpoint
    if set(required) != set(_REQUIRED_CHECKS):
        _reject()
    return [required[name] for name in _REQUIRED_CHECKS], endpoints


def _normalize_check(
    response: dict[str, Any], name: str, url: str, sha: str
) -> tuple[dict[str, Any], int]:
    app = _mapping(response.get("app"))
    app_id = _positive_int(app.get("id"))
    if (
        response.get("url") != url
        or response.get("name") != name
        or _sha(response.get("head_sha")) != sha
        or response.get("status") != "completed"
        or response.get("conclusion") != "success"
        or app.get("slug") != "github-actions"
    ):
        _reject()
    return (
        {
            "url": url,
            "name": name,
            "head_sha": sha,
            "status": "completed",
            "conclusion": "success",
            "app": {"id": app_id, "slug": "github-actions"},
        },
        app_id,
    )


def _normalize_protection(
    branch: str, response: dict[str, Any], app_ids: dict[str, int]
) -> dict[str, Any]:
    if not protection_matches(branch, response, app_ids):
        _reject()
    return protection_payload(branch, app_ids)  # type: ignore[arg-type]


def _normalize_promotion_pr(
    values: list[dict[str, Any]], repository: str, sha: str
) -> dict[str, Any]:
    if len(values) != 1:
        _reject()
    value = values[0]
    base = _mapping(value.get("base"))
    head = _mapping(value.get("head"))
    number = _positive_int(value.get("number"))
    merged_at = _string(value.get("merged_at"))
    merge_commit_sha = _sha(value.get("merge_commit_sha"))
    if (
        merge_commit_sha != sha
        or base.get("ref") != "main"
        or _repository_name(base.get("repo")) != repository
        or head.get("ref") != "dev"
        or _repository_name(head.get("repo")) != repository
    ):
        _reject()
    return {
        "number": number,
        "merged_at": merged_at,
        "merge_commit_sha": sha,
        "base": {"ref": "main", "repo": {"full_name": repository}},
        "head": {"ref": "dev", "repo": {"full_name": repository}},
    }


def _validate_main_branch(response: dict[str, Any], sha: str) -> None:
    commit = _mapping(response.get("commit"))
    if response.get("name") != "main" or _sha(commit.get("sha")) != sha:
        _reject()


def read_main_identity_inputs(
    *,
    event_path: str | os.PathLike[str],
    workflow_ref: object,
    workflow_sha: object,
    repository: object,
    governance_mode: object,
    gh_token: object,
    runner: GhRunner,
    max_event_bytes: int = _DEFAULT_MAX_EVENT_BYTES,
) -> MainIdentityInputs:
    try:
        event_seed = load_event_seed(event_path, max_bytes=max_event_bytes)
        repository_text, sha, run_id, event = _validate_local_envelope(
            event_seed, workflow_ref, workflow_sha, repository
        )
        mode = governance_mode if type(governance_mode) is str else ""
        if mode not in {"protected", "guarded_private"}:
            _reject()
        if type(gh_token) is not str or not gh_token:
            _reject()

        repo = _normalize_repo(
            _api_get(runner, f"/repos/{repository_text}"), repository_text
        )
        if mode == "guarded_private" and repo["private"] is not True:
            _reject()
        source_run = _normalize_source_run(
            _api_get(runner, f"/repos/{repository_text}/actions/runs/{run_id}"),
            repository_text,
            run_id,
            sha,
        )
        source_jobs, check_endpoints = _normalize_jobs(
            _api_get(
                runner, f"/repos/{repository_text}/actions/runs/{run_id}/jobs"
            ),
            repository_text,
            run_id,
            sha,
        )

        linked_checks: list[dict[str, Any]] = []
        check_app_ids: dict[str, int] = {}
        for name in _REQUIRED_CHECKS:
            endpoint = check_endpoints[name]
            check, app_id = _normalize_check(
                _api_get(runner, endpoint),
                name,
                source_jobs[_REQUIRED_CHECKS.index(name)]["check_run_url"],
                sha,
            )
            linked_checks.append(check)
            check_app_ids[name] = app_id
        if len(set(check_app_ids.values())) != 1:
            _reject()

        promotion_pr = _normalize_promotion_pr(
            _api_list(
                runner,
                f"/repos/{repository_text}/commits/{sha}/pulls?per_page=2&page=1",
            ),
            repository_text,
            sha,
        )
        protections: dict[str, Any] | None = None
        if mode == "protected":
            protections = {
                branch: _normalize_protection(
                    branch,
                    _api_get(
                        runner,
                        f"/repos/{repository_text}/branches/{branch}/protection",
                    ),
                    check_app_ids,
                )
                for branch in ("dev", "main")
            }
        _validate_main_branch(
            _api_get(runner, f"/repos/{repository_text}/branches/main"), sha
        )
        repo["main_branch_sha"] = sha
        return _build_inputs(
            event=event,
            workflow_ref=_string(workflow_ref),
            workflow_sha=sha,
            repository=repository_text,
            governance_mode=mode,
            repo=repo,
            protections=protections,
            promotion_pr=promotion_pr,
            source_run=source_run,
            source_jobs=source_jobs,
            linked_checks=linked_checks,
        )
    except GithubEvidenceError:
        raise
    except Exception:
        _reject()
