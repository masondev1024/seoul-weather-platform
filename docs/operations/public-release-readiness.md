# Public release readiness

Status: **REPO-LOCAL PASS / GITHUB PUBLIC CUTOVER COMPLETE**

Decision date: 2026-08-21

User authorization: full visibility cutover authorized on 2026-08-21.

Scope: repository-local publication gates, audit evidence, and the completed
external GitHub cutover. The user authorized the full visibility cutover on
2026-08-21. Fresh readbacks on 2026-08-22 confirmed public visibility, hosted-only
CI, no registered repository runner, disabled repository-driven deployment, and
post-public branch protection on `main`.

## Current state separation

| Gate | Current evidence | Status |
|---|---|---|
| Repo-local publication gates | Apache-2.0 `LICENSE`, `NOTICE`, provenance authorization, secretless example config, and public-release validator are current | **PASS** |
| Secretless example | `.env.example` contains Weather-only placeholders and the local Trino envelope | Pass |
| Runtime separation | `docker-compose.local.yml`; compatibility project ID `seoul-weather-platform-mac`; personal secrets remain Git-ignored | Pass for local design |
| Runtime lineage cost | Marquez inactive; Airflow/dbt OpenLineage disabled; file provenance retained | Pass |
| Redistribution rights | `provenance/source-files.jsonl` records approved 2026-08-21 public republication authorization while retaining lineage | Pass |
| Authorized license/notice | Root `LICENSE` and `NOTICE` record Apache-2.0 terms and attribution | Pass |
| Hosted CI / runner isolation | Hosted-only CI has no self-hosted route; disabled manual no-op deploy workflow is safe, hosted-only, and inert | Pass |
| Prior audit evidence | 0 GitHub Releases, 0 downloadable Release artifacts, 121 GitHub Actions logs scanned clean, and reachable-object scan passed at its audit point | Pass for audit point |
| GitHub external visibility cutover | Repository visibility `PUBLIC`; `WEATHER_DEPLOYMENT_ENABLED=disabled`; `WEATHER_GOVERNANCE_MODE=public`; 0 registered repository runners; `CI / required` protected on `main` | **COMPLETE** |

## Completed external cutover evidence

Read back on 2026-08-22:

- Repository visibility is `PUBLIC` and the default branch is `main`.
- Repository variables are deployment `disabled` and governance `public`.
- No repository runner is registered.
- The `main` protection rule requires `CI / required` and pull-request review.
- GitHub has 0 Releases and 0 downloadable Release assets.

## Ongoing public-repository controls

For every release or trust-boundary change, re-run the repository validators and
report the exact reviewed commit, redistribution and license evidence, secret
scan, Release/artifact delta, hosted-CI and fork event matrix, runner readback,
branch protection readback, public runtime reproducibility, Worker exposure, and
rollback path.

Past Releases become public together with the repository. They are part of the
publication surface even when their assets are not present in the current tree.
The 2026-08-22 readback recorded 0 GitHub Releases and 0 downloadable Release
artifacts. This is point-in-time evidence and must be checked again before a
future release. Do not infer readiness from a clean working tree alone.

There is intentionally no `gh repo edit --visibility public` command, visibility
API client, or automatic publication workflow in this repository.
