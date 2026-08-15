from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum

from deployment.canonical_json import canonical_bytes, sha256_hex


_SHA40 = re.compile(r"^[0-9a-f]{40}$")
_SHA64 = re.compile(r"^[0-9a-f]{64}$")


class DeploymentOutcome(StrEnum):
    STARTED = "started"
    SUCCESS = "success"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"
    ROLLBACK_FAILED = "rollback_failed"


class DeploymentTerminalCategory(StrEnum):
    PAUSE_STATE_UNVERIFIED = "pause_state_unverified"


@dataclass(frozen=True)
class DagStateSnapshot:
    paused: tuple[tuple[str, bool], ...]
    run_counts: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class WriterRunCounts:
    running: int
    queued: int

    def __post_init__(self) -> None:
        for value in (self.running, self.queued):
            if type(value) is not int or value < 0:
                raise ValueError("writer run counts must be non-negative integers")


@dataclass(frozen=True)
class DeploymentRecord:
    schema_version: str
    deployment_id: str
    repository: str
    candidate_sha: str
    target_fingerprint: str
    started_at: str
    completed_at: str | None
    outcome: DeploymentOutcome
    health: str | None
    overlay_content_b64: str | None
    overlay_sha256: str | None
    terminal_category: DeploymentTerminalCategory | None = None


@dataclass(frozen=True)
class BaselineRecord:
    schema_version: str
    baseline_id: str
    target_fingerprint: str
    captured_at: str
    rehearsal: str
    overlay_content_b64: str
    overlay_sha256: str


@dataclass(frozen=True)
class DeploymentResult:
    deployment_id: str
    outcome: DeploymentOutcome
    health: str | None
    idempotent: bool


def _require_sha(value: str, pattern: re.Pattern[str], field: str) -> None:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ValueError(f"{field} must be lowercase hex")


def deployment_id(repository: str, candidate_sha: str, target_fingerprint: str) -> str:
    _require_sha(candidate_sha, _SHA40, "candidate_sha")
    _require_sha(target_fingerprint, _SHA64, "target_fingerprint")
    if type(repository) is not str or not repository:
        raise ValueError("repository must be non-empty")
    return sha256_hex(
        canonical_bytes(
            {
                "repository": repository,
                "candidate_sha": candidate_sha,
                "target_fingerprint": target_fingerprint,
            }
        )
    )
