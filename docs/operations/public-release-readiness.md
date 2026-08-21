# Public release readiness

Status: **BLOCKED**

Decision date: 2026-08-21

Scope: architecture and preconditions only; no visibility change is authorized.

| Gate | Current evidence | Status |
|---|---|---|
| Repository visibility | GitHub readback reports `PRIVATE` | Pass for current cutover |
| Secretless example | `.env.example` contains Weather-only placeholders and the Mac Trino envelope | Pass |
| Runtime separation | Compose project `seoul-weather-platform-mac`; personal secrets remain Git-ignored | Pass for local design |
| Runtime lineage cost | Marquez inactive; Airflow/dbt OpenLineage disabled; file provenance retained | Pass |
| Redistribution rights | `provenance/source-files.jsonl` records approved 2026-08-21 public republication authorization while retaining lineage | Pass |
| Authorized license/notice | Root `LICENSE` and `NOTICE` record Apache-2.0 terms and attribution | Pass |
| Full Git history scan | Not yet completed against every reachable object | **Blocked** |
| GitHub Release scan | Bodies and downloadable assets not yet fully scanned | **Blocked** |
| Fork/runner isolation | Legacy self-hosted `dagbag-runtime` and `deploy-main` definitions still exist | **Blocked** |
| Public branch governance | Final public branch protection and required-check readbacks not collected | **Blocked** |

## Required final report

Before a visibility change, report the exact reviewed commit, redistribution and
license evidence, redacted history/Release scan results, hosted-CI and fork event
matrix, self-hosted/deployment disablement readbacks, branch protection, public
runtime reproducibility, Worker exposure, and rollback. The user must then grant a
new, explicit approval for the visibility change.

Past Releases become public together with the repository. They are part of the
publication surface even when their assets are not present in the current tree.
Do not infer readiness from a clean working tree alone.

There is intentionally no `gh repo edit --visibility public` command, visibility
API client, or automatic publication workflow in this repository.
