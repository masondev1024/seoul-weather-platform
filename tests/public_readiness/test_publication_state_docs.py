from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DOC_PATHS = (
    "docs/operations/public-release-readiness.md",
    "docs/legal/publication-blockers.md",
    "docs/architecture/public-private-operations-boundary.md",
)


def _docs() -> dict[str, str]:
    return {
        path: (REPO_ROOT / path).read_text(encoding="utf-8")
        for path in DOC_PATHS
    }


def test_publication_docs_separate_repo_pass_from_external_cutover_pending() -> None:
    docs = _docs()
    combined = "\n".join(docs.values())

    for stale_current_blocker in (
        "internal_private_snapshot_only",
        "there is no authorized root license",
        "no visibility change is authorized",
        "Legacy self-hosted workflow routes remain",
        "deploy-main workflow remains present",
        "Full Git history and GitHub Release scans still require separate evidence",
        "Not yet completed against every reachable object",
        "Bodies and downloadable assets not yet fully scanned",
    ):
        assert stale_current_blocker not in combined

    readiness = docs["docs/operations/public-release-readiness.md"]
    assert "Repo-local publication gates" in readiness
    assert "PASS" in readiness
    assert "GitHub external visibility cutover" in readiness
    assert "PENDING" in readiness
    assert "User authorization" in readiness
    assert "full visibility cutover authorized on 2026-08-21" in readiness

    for pending_item in (
        "fresh public-readiness preflight",
        "repository variables disabled/public",
        "exact offline runner removal",
        "PR CI and merge",
        "public visibility readback",
        "post-public branch protection readback",
    ):
        assert pending_item in readiness

    for completed_audit in (
        "0 GitHub Releases",
        "0 downloadable Release artifacts",
        "121 GitHub Actions logs scanned clean",
        "reachable-object scan passed",
    ):
        assert completed_audit in combined
    assert "fresh delta/full scan immediately before visibility" in combined

    architecture = docs["docs/architecture/public-private-operations-boundary.md"]
    assert "disabled manual no-op deploy workflow is hosted-only and inert" in architecture
    assert "hosted-only CI has no self-hosted route" in architecture
    assert "Public visibility has not yet been applied" in architecture
    assert "Branch protection has not yet been applied after public visibility" in architecture
