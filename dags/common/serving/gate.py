"""Publication Gate — zero / partial / sample-reliability policy from the contract.

Pure decision functions so every branch (fail, retain-last-good, degraded, publish)
is unit-tested without any D1/Trino. The publisher acts on ``GateResult`` and never
re-derives policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from common.serving.contract import ServingContract


class GateDecision(str, Enum):
    PUBLISH = "publish"
    PUBLISH_DEGRADED = "publish_degraded"
    SKIP_RETAIN = "skip_retain"  # keep last-known-good, do not write
    FAIL = "fail"


# serving_status values recorded in _catalog / publication metadata.
STATUS_PUBLISHED = "published"
STATUS_DEGRADED = "degraded"
STATUS_SKIPPED = "skipped_retained"
STATUS_FAILED = "failed"


@dataclass(frozen=True)
class GateResult:
    decision: GateDecision
    serving_status: str
    reason: str


def evaluate_gate(contract: ServingContract, this_count: int, last_good_count: int | None) -> GateResult:
    """Decide whether to publish ``this_count`` rows given the last good publication."""
    if this_count == 0:
        policy = contract.zero_policy
        if policy == "fail":
            return GateResult(GateDecision.FAIL, STATUS_FAILED, "zero_policy=fail 이며 0행")
        if policy == "allow":
            return GateResult(GateDecision.PUBLISH, STATUS_PUBLISHED, "zero_policy=allow — 0행 정상")
        if policy == "warn":
            return GateResult(GateDecision.PUBLISH_DEGRADED, STATUS_DEGRADED, "zero_policy=warn — 0행 degraded 게시")
        # retain_last_good (default): skip + keep last good
        return GateResult(GateDecision.SKIP_RETAIN, STATUS_SKIPPED, "zero_policy=retain_last_good — 0행, 직전본 유지")

    if contract.partial_min_ratio is not None and last_good_count:
        threshold = last_good_count * contract.partial_min_ratio
        if this_count < threshold:
            return GateResult(
                GateDecision.SKIP_RETAIN,
                STATUS_SKIPPED,
                f"partial 절단: {this_count} < 직전 {last_good_count}×{contract.partial_min_ratio} — 직전본 유지",
            )

    return GateResult(GateDecision.PUBLISH, STATUS_PUBLISHED, "정상 게시")


def apply_reliability(contract: ServingContract, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool]:
    """Apply rollup sample-reliability policy at row level.

    Returns ``(rows_to_write, degraded)``. ``suppress_row`` drops under-sampled rows,
    ``flag_degraded`` keeps them but marks the publication degraded, ``allow`` is a
    no-op. Non-rollup contracts (no ``reliability``) pass rows through unchanged.
    """
    reliability = contract.reliability
    if not reliability:
        return rows, False

    field = reliability.get("sample_count_field")
    minimum = reliability.get("minimum_sample_count")
    policy = reliability.get("insufficient_sample_policy", "allow")
    if not field or minimum is None:
        return rows, False

    def sufficient(row: dict[str, Any]) -> bool:
        value = row.get(field)
        return isinstance(value, (int, float)) and not isinstance(value, bool) and value >= minimum

    if policy == "suppress_row":
        kept = [row for row in rows if sufficient(row)]
        return kept, len(kept) != len(rows)
    if policy == "flag_degraded":
        degraded = any(not sufficient(row) for row in rows)
        return rows, degraded
    return rows, False  # allow
