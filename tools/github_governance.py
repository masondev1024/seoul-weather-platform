from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from typing import Literal


GovernanceMode = Literal["protected", "guarded_private", "invalid"]
ProtectedBranch = Literal["dev", "main"]

_BRANCHES = ("dev", "main")
_EXPECTED_CHECKS: dict[str, tuple[str, ...]] = {
    "dev": ("CI / required",),
    "main": ("CI / required", "Promotion Source / required"),
}


def _positive_app_id(value: object) -> int | None:
    return value if type(value) is int and value > 0 else None


def _required_app_ids(
    branch: str, check_app_ids: Mapping[str, object]
) -> dict[str, int]:
    required: dict[str, int] = {}
    for context in _EXPECTED_CHECKS[branch]:
        app_id = _positive_app_id(check_app_ids.get(context))
        if app_id is None:
            raise ValueError("invalid-check-app-id")
        required[context] = app_id
    return required


def protection_payload(
    branch: ProtectedBranch,
    check_app_ids: Mapping[str, object],
) -> dict[str, object]:
    if branch not in _BRANCHES:
        raise ValueError("unsupported-branch")
    app_ids = _required_app_ids(branch, check_app_ids)
    payload: dict[str, object] = {
        "required_status_checks": {
            "strict": True,
            "checks": [
                {"context": context, "app_id": app_ids[context]}
                for context in _EXPECTED_CHECKS[branch]
            ],
        },
        "enforce_admins": True,
        "required_pull_request_reviews": {
            "dismiss_stale_reviews": True,
            "require_code_owner_reviews": False,
            "required_approving_review_count": 0,
            "require_last_push_approval": False,
            "bypass_pull_request_allowances": {
                "users": [],
                "teams": [],
                "apps": [],
            },
        },
        "restrictions": None,
        "required_linear_history": True,
        "allow_force_pushes": False,
        "allow_deletions": False,
        "block_creations": False,
        "required_conversation_resolution": True,
        "lock_branch": False,
        "allow_fork_syncing": False,
    }
    return copy.deepcopy(payload)


def _exact_bool(value: object) -> bool | None:
    if type(value) is bool:
        return value
    if isinstance(value, Mapping) and type(value.get("enabled")) is bool:
        return value["enabled"]
    return None


def _empty_actor_lists(value: object, *, absent_is_empty: bool) -> bool:
    if value is None:
        return absent_is_empty
    if not isinstance(value, Mapping):
        return False
    for actor_kind in ("users", "teams", "apps"):
        actors = value.get(actor_kind, [])
        if (
            not isinstance(actors, Sequence)
            or isinstance(actors, (str, bytes, bytearray))
            or len(actors) != 0
        ):
            return False
    return True


def _status_check_bindings(
    value: object,
) -> tuple[tuple[str, int], ...] | None:
    if not isinstance(value, Mapping) or value.get("strict") is not True:
        return None
    checks = value.get("checks")
    if not isinstance(checks, Sequence) or isinstance(checks, (str, bytes, bytearray)):
        return None
    bindings: list[tuple[str, int]] = []
    for check in checks:
        if not isinstance(check, Mapping):
            return None
        context = check.get("context")
        app_id = _positive_app_id(check.get("app_id"))
        if not isinstance(context, str) or not context or app_id is None:
            return None
        bindings.append((context, app_id))
    return tuple(bindings)


def _pull_request_rule_matches(value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    if value.get("dismiss_stale_reviews") is not True:
        return False
    if value.get("require_code_owner_reviews") is not False:
        return False
    review_count = value.get("required_approving_review_count")
    if type(review_count) is not int or review_count != 0:
        return False
    if value.get("require_last_push_approval") is not False:
        return False
    if not _empty_actor_lists(
        value.get("bypass_pull_request_allowances"), absent_is_empty=True
    ):
        return False
    dismissal_restrictions = value.get("dismissal_restrictions")
    return _empty_actor_lists(dismissal_restrictions, absent_is_empty=True)


def protection_matches(
    branch: str,
    protection: object,
    expected_app_ids: Mapping[str, object] | None = None,
) -> bool:
    if branch not in _BRANCHES or not isinstance(protection, Mapping):
        return False

    bindings = _status_check_bindings(protection.get("required_status_checks"))
    expected_contexts = _EXPECTED_CHECKS[branch]
    if (
        bindings is None
        or len(bindings) != len({context for context, _ in bindings})
        or tuple(sorted(context for context, _ in bindings)) != expected_contexts
    ):
        return False
    if expected_app_ids is not None:
        try:
            required_app_ids = _required_app_ids(branch, expected_app_ids)
        except ValueError:
            return False
        if dict(bindings) != required_app_ids:
            return False
    if _exact_bool(protection.get("enforce_admins")) is not True:
        return False
    if not _pull_request_rule_matches(protection.get("required_pull_request_reviews")):
        return False
    if not _empty_actor_lists(protection.get("restrictions"), absent_is_empty=True):
        return False

    expected_flags = {
        "required_linear_history": True,
        "allow_force_pushes": False,
        "allow_deletions": False,
        "block_creations": False,
        "required_conversation_resolution": True,
        "lock_branch": False,
        "allow_fork_syncing": False,
    }
    return all(
        _exact_bool(protection.get(field)) is expected
        for field, expected in expected_flags.items()
    )


def _default_branch(repo: Mapping[str, object]) -> str | None:
    default_branch_ref = repo.get("defaultBranchRef")
    if isinstance(default_branch_ref, Mapping):
        name = default_branch_ref.get("name")
        return name if isinstance(name, str) and name else None
    default_branch = repo.get("default_branch")
    return (
        default_branch if isinstance(default_branch, str) and default_branch else None
    )


def _visibility(repo: Mapping[str, object]) -> str | None:
    visibility = repo.get("visibility")
    if isinstance(visibility, str) and visibility:
        normalized = visibility.casefold()
        return normalized if normalized in {"private", "public", "internal"} else None
    private = repo.get("private")
    if type(private) is bool:
        return "private" if private else "public"
    return None


def classify(
    repo: Mapping[str, object],
    protections: Mapping[str, Mapping[str, object] | None],
) -> GovernanceMode:
    if not isinstance(repo, Mapping) or _default_branch(repo) != "main":
        return "invalid"
    visibility = _visibility(repo)
    if visibility is None or set(protections) != set(_BRANCHES):
        return "invalid"

    for branch in _BRANCHES:
        protection = protections.get(branch)
        if protection is not None and not protection_matches(branch, protection):
            return "invalid"
    if any(protections.get(branch) is None for branch in _BRANCHES):
        return "guarded_private" if visibility == "private" else "invalid"
    check_sources: dict[str, int] = {}
    for branch in _BRANCHES:
        protection = protections.get(branch)
        if not isinstance(protection, Mapping):
            return "invalid"
        bindings = _status_check_bindings(protection.get("required_status_checks"))
        if bindings is None:
            return "invalid"
        for context, app_id in bindings:
            previous = check_sources.setdefault(context, app_id)
            if previous != app_id:
                return "invalid"
    return "protected"


def release_enabled(mode: str) -> bool:
    return mode == "protected"
