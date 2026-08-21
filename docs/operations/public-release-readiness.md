# Public release readiness

Status: **REPO-LOCAL PASS / GITHUB EXTERNAL VISIBILITY CUTOVER PENDING**

Decision date: 2026-08-21

User authorization: full visibility cutover authorized on 2026-08-21.

Scope: repository-local publication gates, audit evidence, and remaining external
GitHub cutover steps. The user authorized the full visibility cutover on
2026-08-21, but public visibility has not yet been applied and must still be
confirmed by fresh GitHub readbacks.

## Current state separation

| Gate | Current evidence | Status |
|---|---|---|
| Repo-local publication gates | Apache-2.0 `LICENSE`, `NOTICE`, provenance authorization, secretless example config, and public-release validator are current | **PASS** |
| Secretless example | `.env.example` contains Weather-only placeholders and the Mac Trino envelope | Pass |
| Runtime separation | Compose project `seoul-weather-platform-mac`; personal secrets remain Git-ignored | Pass for local design |
| Runtime lineage cost | Marquez inactive; Airflow/dbt OpenLineage disabled; file provenance retained | Pass |
| Redistribution rights | `provenance/source-files.jsonl` records approved 2026-08-21 public republication authorization while retaining lineage | Pass |
| Authorized license/notice | Root `LICENSE` and `NOTICE` record Apache-2.0 terms and attribution | Pass |
| Hosted CI / runner isolation | Hosted-only CI has no self-hosted route; disabled manual no-op deploy workflow is safe, hosted-only, and inert | Pass |
| Prior audit evidence | 0 GitHub Releases, 0 downloadable Release artifacts, 121 GitHub Actions logs scanned clean, and reachable-object scan passed at its audit point | Pass for audit point |
| GitHub external visibility cutover | Awaiting fresh preflight, repository variables disabled/public, exact offline runner removal, PR CI and merge, public visibility readback, and post-public branch protection readback | **PENDING** |

## Pending external cutover checklist

Complete these immediately before or during the GitHub visibility operation:

- Run a fresh public-readiness preflight.
- Confirm repository variables disabled/public.
- Confirm exact offline runner removal.
- Run PR CI and merge from the reviewed branch.
- Apply visibility only after the approved preflight path.
- Collect public visibility readback.
- Collect post-public branch protection readback.
- Run a fresh delta/full scan immediately before visibility.

## Required final report

Before applying the external GitHub visibility change, report the exact reviewed
commit, redistribution and license evidence, fresh delta/full secret scan,
Release/artifact delta evidence, hosted-CI and fork event matrix, no-runner
readbacks, branch protection plan, public runtime reproducibility, Worker
exposure, and rollback.

Past Releases become public together with the repository. They are part of the
publication surface even when their assets are not present in the current tree.
The audit point recorded 0 GitHub Releases and 0 downloadable Release artifacts,
but that result must be checked again before visibility. Do not infer readiness
from a clean working tree alone.

There is intentionally no `gh repo edit --visibility public` command, visibility
API client, or automatic publication workflow in this repository.
