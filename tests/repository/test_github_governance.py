from __future__ import annotations

import copy
from typing import Any

import pytest

from tools.github_governance import (
    classify,
    protection_matches,
    protection_payload,
    release_enabled,
)


APP_ID = 424242
CHECK_APP_IDS = {
    "CI / required": APP_ID,
    "Promotion Source / required": APP_ID,
}
DEV_PAYLOAD = {
    "required_status_checks": {
        "strict": True,
        "checks": [{"context": "CI / required", "app_id": APP_ID}],
    },
    "enforce_admins": True,
    "required_pull_request_reviews": {
        "dismiss_stale_reviews": True,
        "require_code_owner_reviews": False,
        "required_approving_review_count": 0,
        "require_last_push_approval": False,
        "bypass_pull_request_allowances": {"users": [], "teams": [], "apps": []},
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

MAIN_PAYLOAD = copy.deepcopy(DEV_PAYLOAD)
MAIN_PAYLOAD["required_status_checks"]["checks"] = [
    {"context": "CI / required", "app_id": APP_ID},
    {"context": "Promotion Source / required", "app_id": APP_ID},
]


def _repo(
    *, default_branch: str = "main", visibility: str = "PRIVATE"
) -> dict[str, Any]:
    return {
        "nameWithOwner": "masondev1024/seoul-weather-platform",
        "defaultBranchRef": {"name": default_branch},
        "visibility": visibility,
    }


def _readback(branch: str, *, include_bypass: bool = True) -> dict[str, Any]:
    checks = {
        "dev": ["CI / required"],
        "main": ["CI / required", "Promotion Source / required"],
    }[branch]
    pull_request_reviews: dict[str, Any] = {
        "dismiss_stale_reviews": True,
        "require_code_owner_reviews": False,
        "required_approving_review_count": 0,
        "require_last_push_approval": False,
    }
    if include_bypass:
        pull_request_reviews["bypass_pull_request_allowances"] = {
            "users": [],
            "teams": [],
            "apps": [],
        }
    return {
        "url": f"https://api.github.test/branches/{branch}/protection",
        "required_status_checks": {
            "url": "https://api.github.test/status-checks",
            "strict": True,
            "contexts": checks,
            "checks": [
                {"context": context, "app_id": CHECK_APP_IDS[context]}
                for context in checks
            ],
        },
        "enforce_admins": {"url": "https://api.github.test/admins", "enabled": True},
        "required_pull_request_reviews": pull_request_reviews,
        "restrictions": None,
        "required_linear_history": {"enabled": True},
        "allow_force_pushes": {"enabled": False},
        "allow_deletions": {"enabled": False},
        "block_creations": {"enabled": False},
        "required_conversation_resolution": {"enabled": True},
        "lock_branch": {"enabled": False},
        "allow_fork_syncing": {"enabled": False},
    }


def _protections() -> dict[str, dict[str, Any] | None]:
    return {"dev": _readback("dev"), "main": _readback("main")}


def test_dev_protection_payload_is_exact() -> None:
    assert protection_payload("dev", CHECK_APP_IDS) == DEV_PAYLOAD


def test_main_protection_payload_adds_only_sorted_promotion_check() -> None:
    assert protection_payload("main", CHECK_APP_IDS) == MAIN_PAYLOAD


def test_protection_payload_rejects_unknown_branch() -> None:
    with pytest.raises(ValueError, match="unsupported-branch"):
        protection_payload("feature", CHECK_APP_IDS)  # type: ignore[arg-type]


@pytest.mark.parametrize("app_id", [None, -1, 0, False, "424242"])
def test_context_without_positive_pinned_app_id_is_never_protected(
    app_id: object,
) -> None:
    protections = _protections()
    protections["dev"]["required_status_checks"]["checks"][0]["app_id"] = app_id

    assert not protection_matches("dev", protections["dev"])
    assert classify(_repo(), protections) == "invalid"


def test_legacy_context_only_readback_is_never_protected() -> None:
    protections = _protections()
    protections["dev"]["required_status_checks"].pop("checks")

    assert classify(_repo(), protections) == "invalid"


def test_same_required_context_cannot_bind_conflicting_apps_across_branches() -> None:
    protections = _protections()
    protections["main"]["required_status_checks"]["checks"][0]["app_id"] = APP_ID + 1

    assert classify(_repo(), protections) == "invalid"


def test_valid_main_default_and_both_exact_readbacks_are_protected() -> None:
    assert classify(_repo(), _protections()) == "protected"


def test_initial_dev_default_branch_is_invalid_even_when_protections_match() -> None:
    assert classify(_repo(default_branch="dev"), _protections()) == "invalid"


@pytest.mark.parametrize("branch", ["dev", "main"])
@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_required_checks_must_match_each_branch_exactly(
    branch: str, mutation: str
) -> None:
    protections = _protections()
    checks = protections[branch]["required_status_checks"]["checks"]
    if mutation == "missing":
        checks.pop()
    else:
        checks.append({"context": "Unexpected / check", "app_id": None})

    assert not protection_matches(branch, protections[branch])
    assert classify(_repo(), protections) == "invalid"


@pytest.mark.parametrize(
    ("field_path", "unsafe_value"),
    [
        (("enforce_admins", "enabled"), False),
        (("required_status_checks", "strict"), False),
        (("required_pull_request_reviews", "dismiss_stale_reviews"), False),
        (("required_pull_request_reviews", "require_code_owner_reviews"), True),
        (("required_pull_request_reviews", "required_approving_review_count"), 1),
        (("required_pull_request_reviews", "required_approving_review_count"), False),
        (("required_pull_request_reviews", "require_last_push_approval"), True),
        (("required_linear_history", "enabled"), False),
        (("allow_force_pushes", "enabled"), True),
        (("allow_deletions", "enabled"), True),
        (("required_conversation_resolution", "enabled"), False),
        (("lock_branch", "enabled"), True),
        (("allow_fork_syncing", "enabled"), True),
    ],
)
def test_unsafe_or_weakened_readback_is_invalid(
    field_path: tuple[str, ...], unsafe_value: object
) -> None:
    protections = _protections()
    target: dict[str, Any] = protections["dev"]
    for field in field_path[:-1]:
        target = target[field]
    target[field_path[-1]] = unsafe_value

    assert classify(_repo(), protections) == "invalid"


def test_absent_pull_request_rule_is_invalid() -> None:
    protections = _protections()
    protections["dev"].pop("required_pull_request_reviews")

    assert classify(_repo(), protections) == "invalid"


@pytest.mark.parametrize("actor_kind", ["users", "teams", "apps"])
def test_any_bypass_actor_is_invalid(actor_kind: str) -> None:
    protections = _protections()
    bypass = protections["dev"]["required_pull_request_reviews"][
        "bypass_pull_request_allowances"
    ]
    bypass[actor_kind] = [{"login": "unexpected-actor"}]

    assert classify(_repo(), protections) == "invalid"


def test_absent_or_individually_omitted_empty_bypass_fields_normalize_to_empty() -> (
    None
):
    absent = _protections()
    absent["dev"]["required_pull_request_reviews"].pop("bypass_pull_request_allowances")
    partial_empty = _protections()
    partial_empty["main"]["required_pull_request_reviews"][
        "bypass_pull_request_allowances"
    ] = {"users": []}

    assert classify(_repo(), absent) == "protected"
    assert classify(_repo(), partial_empty) == "protected"


def test_non_empty_push_restriction_is_invalid() -> None:
    protections = _protections()
    protections["dev"]["restrictions"] = {
        "users": [{"login": "unexpected-actor"}],
        "teams": [],
        "apps": [],
    }

    assert classify(_repo(), protections) == "invalid"


def test_private_unavailable_protection_is_guarded_diagnosis_only() -> None:
    protections = _protections()
    protections["main"] = None

    mode = classify(_repo(visibility="private"), protections)

    assert mode == "guarded_private"
    assert not release_enabled(mode)


def test_unavailable_protection_is_invalid_for_non_private_or_wrong_default() -> None:
    protections = {"dev": None, "main": None}

    assert classify(_repo(visibility="PUBLIC"), protections) == "invalid"
    assert classify(_repo(default_branch="dev"), protections) == "invalid"


def test_private_misconfigured_readback_is_invalid_not_guarded() -> None:
    protections = _protections()
    protections["main"]["allow_deletions"]["enabled"] = True

    assert classify(_repo(visibility="PRIVATE"), protections) == "invalid"


def test_only_protected_mode_enables_release() -> None:
    assert release_enabled("protected")
    assert not release_enabled("guarded_private")
    assert not release_enabled("invalid")
