# Public Release and Scheduling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish the reviewed Weather-only repository under Apache-2.0, remove public-to-private execution paths, and restore the organization-equivalent Weather schedules on the personal Mac runtime.

**Architecture:** GitHub-hosted CI remains the only public automation surface. Personal R2, D1, Airflow, Trino, and Docker credentials and execution stay on the Mac; no public workflow can select a self-hosted runner or deploy to the laptop. Airflow restores the organization cron for KMA Bronze and serving snapshots, while asset-triggered DAGs retain their existing dependencies and manual recovery DAGs remain paused.

**Tech Stack:** GitHub Actions, Python 3.11.15, Apache Airflow 3.2.2, Docker Compose, Trino 482, dbt 1.10.22, Cloudflare R2/D1/Workers.

**Spec:** `docs/architecture/public-private-operations-boundary.md` and `docs/operations/public-release-readiness.md`

## Global Constraints

- Keep Apache Airflow pinned at `3.2.2`.
- Apply the Apache License 2.0 to repository-owned and authorized team-derived code; preserve MIT attribution for `NomaDamas/k-skill` material in `NOTICE`.
- Never commit populated environment files or resource identifiers.
- Public workflows must use GitHub-hosted runners only and must never deploy to the personal Mac.
- Restore KMA schedule `20 2,5,8,11,14,17,20,23 * * *` and serving snapshot schedule `0 * * * *` in KST.
- Keep `weather_vilage_fcst_bronze_backfill` and `weather_vilage_fcst_recollect` paused.
- Abort automatic activation on any Trino restart, OOM, failed health check, failed publication, or non-clean Git state.

---

### Task 1: License and local public-release gate

**Files:**
- Create: `LICENSE`
- Create: `NOTICE`
- Create: `tools/public_release_contract.py`
- Create: `tests/public_readiness/test_public_release_contract.py`
- Modify: `tools/refresh_provenance.py`
- Modify: `tests/repository/test_refresh_provenance.py`
- Modify: `provenance/source-files.jsonl`
- Modify: `docs/legal/publication-blockers.md`
- Modify: `docs/operations/public-release-readiness.md`

**Interfaces:**
- Produces: `validate_public_release_contract(repo_root: Path) -> list[str]`, returning an empty list only when license, notice, provenance, workflow, and example-config boundaries are publishable.

- [ ] **Step 1: Write failing public-release and provenance tests**

  Assert that the repository has an Apache-2.0 root license, an attribution notice, zero `internal_private_snapshot_only` provenance records, no self-hosted workflow route, no enabled deploy workflow, and a secretless example environment.

- [ ] **Step 2: Run the tests and verify RED**

  Run: `.venv/bin/python -m pytest tests/public_readiness/test_public_release_contract.py tests/repository/test_refresh_provenance.py -q`

  Expected: failures for missing `LICENSE`, missing `NOTICE`, locked provenance, and legacy workflow routes.

- [ ] **Step 3: Add Apache-2.0/NOTICE and minimal contract implementation**

  Use the canonical Apache License 2.0 text. Record the approved 2026-08-21 team-code republication decision and the MIT third-party attribution without claiming ownership of upstream work.

- [ ] **Step 4: Reclassify provenance without losing source lineage**

  Preserve every source repository, commit, path, hash, derivation, and validator field; change only the license authorization status and explanatory reason required by the approval.

- [ ] **Step 5: Verify GREEN and refresh the manifest**

  Run the targeted tests, `python -m tools.refresh_provenance --check`, and `python -m tools.verify_provenance`.

### Task 2: Public-safe GitHub Actions boundary

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/deploy-main.yml`
- Modify: `tools/ci_required_gate.py`
- Modify: `tools/workflow_policy.py`
- Modify: `tests/repository/test_ci_required_gate.py`
- Modify: `tests/repository/test_workflow_policy.py`
- Modify: `tests/deploy/test_deploy_main_workflow.py`

**Interfaces:**
- Consumes: repository variable `WEATHER_GOVERNANCE_MODE=public`.
- Produces: a hosted-only CI required gate and a manual no-op deployment workflow with no self-hosted labels or deployment commands.

- [ ] **Step 1: Write failing tests for public mode**

  Require public-mode pull request and main-push CI to pass only hosted checks. Require any `self-hosted` label, `deploy-main` command, automatic deployment trigger, or `WEATHER_DEPLOYMENT_ENABLED=enabled` dependency to fail the repository policy.

- [ ] **Step 2: Verify RED**

  Run: `.venv/bin/python -m pytest tests/repository/test_ci_required_gate.py tests/repository/test_workflow_policy.py tests/deploy/test_deploy_main_workflow.py -q`

- [ ] **Step 3: Implement the minimal hosted-only workflow**

  Remove `dagbag-runtime`, its promotion evidence, and its required result. Permit only governance mode `public`. Replace `Deploy Main` with a `workflow_dispatch` no-op job on `ubuntu-latest` guarded by `if: false`.

- [ ] **Step 4: Verify GREEN and run workflow policy**

  Run the targeted tests plus `.venv/bin/python -m tools.workflow_policy --repo-root .`.

### Task 3: Restore organization-equivalent Mac schedules

**Files:**
- Modify: `docker-compose.mac.yml`
- Modify: `tools/mac_runtime_contract.py`
- Modify: `tests/deploy/test_mac_runtime_contract.py`
- Modify: `provenance/source-files.jsonl`

**Interfaces:**
- Produces: `ASK_SEOUL_KMA_DAG_SCHEDULE=20 2,5,8,11,14,17,20,23 * * *` and `ASK_SEOUL_WEATHER_SERVING_SNAPSHOT_DAG_SCHEDULE=0 * * * *` in the Mac Airflow environment.

- [ ] **Step 1: Change tests to require the exact organization cron values**

- [ ] **Step 2: Verify RED against the current blank overrides**

- [ ] **Step 3: Set the two exact cron values and update the runtime validator**

- [ ] **Step 4: Verify GREEN, DagBag timetables, and the complete regression suite**

  Run targeted tests, dbt parse/contract validation, Airflow DagBag, repository/provenance checks, and `.venv/bin/python -m pytest -q`.

### Task 4: Publish the reviewed commit safely

**Files:**
- Modify: `docs/operations/public-release-readiness.md` with the redacted audit evidence.

**Interfaces:**
- Consumes: clean local commit, GitHub owner permission, zero-secret history/log audit, zero Releases/assets, disabled deployment variables.
- Produces: merged `main`, public visibility readback, and protected `main` requiring `CI / required`.

- [ ] **Step 1: Set GitHub deployment variables fail-closed**

  Set `WEATHER_DEPLOYMENT_ENABLED=disabled` and `WEATHER_GOVERNANCE_MODE=public`; verify both values without printing credentials.

- [ ] **Step 2: Commit only reviewed paths and push the feature branch**

- [ ] **Step 3: Open a non-draft PR, wait for hosted CI, and merge only on success**

- [ ] **Step 4: Remove the exact offline repository self-hosted runner registration**

- [ ] **Step 5: Change visibility to public and read it back**

- [ ] **Step 6: Apply main branch protection after visibility change**

  Require strict `CI / required`, block force pushes and deletion, require conversation resolution, and retain owner recovery by not requiring an unavailable second reviewer.

### Task 5: Activate and observe automatic Weather operation

**Files:**
- No repository source changes.

**Interfaces:**
- Consumes: the published commit and exact Mac runtime schedules.
- Produces: nine active production DAGs, two paused recovery DAGs, and a successful automatic R2/Iceberg/D1/Worker cycle.

- [ ] **Step 1: Recreate only Airflow runtime services with the reviewed compose/env**

- [ ] **Step 2: Verify Trino health, OOM/restart counters, timetable values, and zero active Weather tasks**

- [ ] **Step 3: Unpause nine production DAGs in dependency-safe stages**

  Unpause reference/maintenance, Bronze/reconciliation/asset transforms, then snapshot/export/watchdog. Keep backfill and recollect paused.

- [ ] **Step 4: Observe the first automatically scheduled cycle**

  Require Airflow success, Trino restart `0`, OOM `false`, bounded memory, personal R2/Iceberg/D1 publication success, and a read-only Worker HTTP 200 response.

- [ ] **Step 5: Record final public/runtime readbacks and rollback instructions**

