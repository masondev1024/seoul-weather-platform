from __future__ import annotations

import argparse
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn


_SHA_RE = re.compile(r"[0-9a-f]{40}")
_ZERO_SHA = "0" * 40


@dataclass(frozen=True)
class PromotionDecision:
    allowed: bool
    reason: str


class _SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        self.exit(2, "invalid-input\n")


def _blocked(reason: str) -> PromotionDecision:
    return PromotionDecision(allowed=False, reason=reason)


def _required_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _promotion_fields(
    pull_request: object,
) -> tuple[str, str | None, str, str, str] | None:
    if not isinstance(pull_request, Mapping):
        return None
    base = pull_request.get("base")
    head = pull_request.get("head")
    if not isinstance(base, Mapping) or not isinstance(head, Mapping):
        return None
    base_repo = base.get("repo")
    head_repo = head.get("repo")
    if not isinstance(base_repo, Mapping) or not isinstance(head_repo, Mapping):
        return None

    base_ref = _required_string(base.get("ref"))
    base_sha = base.get("sha")
    base_repository = _required_string(base_repo.get("full_name"))
    head_ref = _required_string(head.get("ref"))
    head_repository = _required_string(head_repo.get("full_name"))
    if (
        base_ref is None
        or base_repository is None
        or head_ref is None
        or head_repository is None
    ):
        return None
    if base_sha is not None and (
        not isinstance(base_sha, str) or _SHA_RE.fullmatch(base_sha) is None
    ):
        return None
    return base_ref, base_sha, base_repository, head_ref, head_repository


def validate_pull_request_event(
    event: Mapping[str, object], repository: str
) -> PromotionDecision:
    if not repository or not isinstance(event, Mapping):
        return _blocked("invalid-event")
    fields = _promotion_fields(event.get("pull_request"))
    if fields is None:
        return _blocked("invalid-event")
    base_ref, _, base_repository, _, _ = fields
    if base_repository != repository:
        return _blocked("invalid-promotion-source")
    if base_ref == "dev":
        return PromotionDecision(allowed=True, reason="not-required")
    if base_ref != "main":
        return _blocked("unsupported-base")
    return PromotionDecision(allowed=True, reason="allowed")


def validate_main_push_associated_prs(
    prs: Sequence[Mapping[str, object]],
    event: Mapping[str, object],
    repository: str,
    sha: str,
) -> PromotionDecision:
    event_repository = event.get("repository") if isinstance(event, Mapping) else None
    if (
        not repository
        or not sha
        or _SHA_RE.fullmatch(sha) is None
        or sha == _ZERO_SHA
        or not isinstance(prs, Sequence)
        or isinstance(prs, (str, bytes, bytearray))
        or not isinstance(event, Mapping)
        or event.get("ref") != "refs/heads/main"
        or event.get("created") is not False
        or event.get("deleted") is not False
        or event.get("after") != sha
        or not isinstance(event_repository, Mapping)
        or event_repository.get("full_name") != repository
    ):
        return _blocked("invalid-push-event")
    before = event.get("before")
    if (
        not isinstance(before, str)
        or _SHA_RE.fullmatch(before) is None
        or before == _ZERO_SHA
    ):
        return _blocked("invalid-push-event")

    exact_promotion_count = 0
    for pull_request in prs:
        if not isinstance(pull_request, Mapping):
            return _blocked("invalid-associated-prs")
        fields = _promotion_fields(pull_request)
        if fields is None:
            return _blocked("invalid-associated-prs")
        base_ref, base_sha, base_repository, _, _ = fields
        merged_at = _required_string(pull_request.get("merged_at"))
        merge_commit_sha = _required_string(pull_request.get("merge_commit_sha"))
        if (
            base_ref == "main"
            and base_sha == before
            and base_repository == repository
            and merged_at is not None
            and merge_commit_sha == sha
        ):
            exact_promotion_count += 1
    if len(prs) != 1:
        return _blocked("missing-promotion-evidence")
    if exact_promotion_count == 1:
        return PromotionDecision(allowed=True, reason="allowed")
    return _blocked("missing-promotion-evidence")


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid-input") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = _SanitizedArgumentParser(
        description="Validate dev-to-main promotion evidence from local GitHub JSON."
    )
    commands = parser.add_subparsers(dest="command", required=True)

    pull_request = commands.add_parser("pull-request")
    pull_request.add_argument("--event-path", type=Path, required=True)
    pull_request.add_argument("--repository", required=True)

    main_push = commands.add_parser("main-push")
    main_push.add_argument("--event-path", type=Path, required=True)
    main_push.add_argument("--associated-prs-path", type=Path, required=True)
    main_push.add_argument("--repository", required=True)
    main_push.add_argument("--sha", required=True)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "pull-request":
            payloads = (_read_json(args.event_path),)
        else:
            payloads = (_read_json(args.associated_prs_path), _read_json(args.event_path))
    except ValueError:
        print("invalid-input")
        return 1

    if args.command == "pull-request":
        (payload,) = payloads
        if not isinstance(payload, Mapping):
            decision = _blocked("invalid-input")
        else:
            decision = validate_pull_request_event(payload, args.repository)
    elif args.command == "main-push":
        payload, event = payloads
        if not isinstance(payload, list) or not isinstance(event, Mapping):
            decision = _blocked("invalid-input")
        else:
            decision = validate_main_push_associated_prs(
                payload, event, args.repository, args.sha
            )

    print(decision.reason)
    return 0 if decision.allowed else 1


if __name__ == "__main__":
    raise SystemExit(main())
