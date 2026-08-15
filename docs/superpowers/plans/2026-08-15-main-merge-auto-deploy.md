# Main Merge Weather Auto-Deploy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** private 단일 소유자 `guarded_private` 또는 더 강한 `protected` 경계에서 exact `main` merge의 CI 성공 SHA를 기존 로컬 Airflow에 자동 배포하고 실패 시 직전 성공 SHA 또는 승인된 baseline으로 rollback한다.

**Architecture:** GitHub `workflow_run`은 thin dispatcher다. GitHub-hosted `verify-main`이 원격 identity를 read-only 검증하고, 성공한 경우에만 self-hosted `deploy-main`이 동일 검증을 반복한 뒤 상태를 변경한다. 두 경로는 `deployment.main_cli` 하나로 진입한다. identity, ledger, overlay, orchestration, concrete argv adapter를 분리하고 L0에서는 injected fake만 사용한다. 최초 cutover 전에는 runner와 workflow를 비활성 상태로 유지한다.

**Tech Stack:** Python 3.11.15, pytest 9.0.3, GitHub Actions, GitHub CLI REST, Docker Compose, Airflow 3.2.2 CLI, Windows atomic replace/exclusive file lock.

## Global Constraints

- 운영 승격은 same-repository `dev → main` PR merge만 허용한다.
- 배포 trigger는 `CI`의 `workflow_run.completed`이고 source event/branch/conclusion은 `push`/`main`/`success`다.
- `guarded_private`에서는 private 단일 소유자 경계, current remote `main`, same-repository exact `dev → main` merged PR, exact branch-bound required checks를 mutation 전에 모두 재검증한다. public/internal 전환 또는 추가 writer면 guarded 배포를 중단한다.
- `protected`에서는 위 exact merge 증거에 더해 `dev`·`main` native protection과 branch-bound required check readback이 mutation 전에 모두 일치해야 한다.
- `WEATHER_DEPLOYMENT_ENABLED=enabled`만 자동배포를 예약한다. 최초 승인 전에는 이 값을 설정하지 않으며 runner도 offline 상태로 둔다.
- PR, `workflow_dispatch`, `repository_dispatch`, Release event는 self-hosted deploy에 도달하지 않는다. guarded_private도 별도 최초 cutover 승인과 deployment flag 전에는 self-hosted deploy에 도달하지 않는다.
- 실제 Airflow pause/deploy와 runner 활성화는 최초 cutover 보고·승인 전까지 금지한다.
- L0 test는 fake/injected runner만 사용하고 Docker·Airflow·GitHub subprocess를 실행하지 않는다.
- DAG trigger/backfill/clear/retry/mark-success, dbt run/build, Trino·D1·R2 write는 금지한다.
- data service 재시작, `compose down`, `--force-recreate`는 금지한다.
- `airflow-init`은 자동배포 대상이 아니다. normal code-service allowlist는 api-server, scheduler, dag-processor, triggerer이며 generated overlay는 `!override` 없이 DAG/dbt mount target만 교체해 plugins와 writable logs volume을 보존한다.
- 기존 `ask-seoul-sample` checkout은 수정하지 않는다. 배포 SHA는 repository 밖 `runtime_root/releases/<sha>`에 detached checkout하고, exact Airflow code service의 `/opt/airflow/dags`·`/opt/airflow/dbt`만 generated Compose overlay로 교체한다. release는 두 source를 read-only로 하고 exact SHA별 `ASK_SEOUL_DBT_ARTIFACT_ROOT`를 기존 writable logs volume 아래에 둔다. baseline rollback은 기존 executor 호환을 위해 dbt mount를 read-write로 유지하고 이 환경 변수를 주입하지 않는다.
- stable generated overlay는 repository 밖 절대경로이며 temp write, `flush`, `fsync`, `os.replace`로만 교체한다. rollback은 checksum-valid 직전 성공 overlay 또는 승인·rehearsal 완료 baseline overlay만 사용할 수 있다.
- stage·commit·push·PR은 사용자 별도 승인 전까지 금지한다.

---

### Task 1: exact `main` deploy identity gate

**Files:**
- Create: `deployment/main_identity.py`
- Test: `tests/deploy/test_main_deploy_identity.py`

**Interfaces:**
- Reuses: `tools.github_governance.classify(repo, protections) == "protected"` and `protection_matches("main", protection, expected_app_ids)`
- Produces: frozen `MainDeployIdentity` and `validate_main_deploy_identity(*, event, workflow_ref, workflow_sha, repository, repo, protections, source_run, source_jobs, linked_checks) -> MainDeployIdentity`

- [ ] **Step 1: strict event/SHA RED tests**

```python
def test_exact_successful_main_ci_is_accepted():
    identity = validate_main_deploy_identity(**valid_identity_case())
    assert identity.candidate_sha == SHA

@pytest.mark.parametrize("mutation", [
    "pull_request_source", "public_guarded", "stale_main", "wrong_workflow",
    "wrong_repo", "failed_ci", "missing_required_check", "duplicate_check",
])
def test_identity_fails_closed_before_mutation(mutation):
    with pytest.raises(MainIdentityError):
        validate_main_deploy_identity(**mutated_case(mutation))
```

- [ ] **Step 2: RED 확인**

Run: `python -m pytest tests/deploy/test_main_deploy_identity.py -q`

Expected: `ModuleNotFoundError: deployment.main_identity`.

- [ ] **Step 3: immutable identity 최소 구현**

Require exact lowercase 40-hex equality across `workflow_run.head_sha`, trusted deploy workflow SHA, remote `main`, source job and linked check SHA. Require source workflow `CI`, suffix 없는 path `.github/workflows/ci.yml`, 별도 `head_branch=main`, event `push`, status/conclusion `completed/success`, same-repository `dev → main` merged PR evidence, and unique `CI / required` plus `Promotion Source / required` jobs from that exact source run. `guarded_private` accepts only `private=true`; public/internal or additional-writer operational evidence stops deployment. `protected` additionally requires `classify(...) == "protected"` and exact protection app bindings. Each linked check must be `completed/success`, `app.slug == "github-actions"`, and have the same positive app id. Snapshot JSON-native input once; expose only frozen scalar/tuple fields. Errors contain a fixed category only.

- [ ] **Step 4: GREEN**

Run: `python -m pytest tests/deploy/test_main_deploy_identity.py -q`

Expected: PASS; monkeypatched subprocess/network sentinel remains unused.

---

### Task 2: target overlay contract, atomic ledger, lock, and deterministic protocols

**Files:**
- Create: `deployment/models.py`
- Create: `deployment/ledger.py`
- Create: `deployment/overlay.py`
- Create: `deployment/adapters.py`
- Create: `deployment/fake_adapters.py`
- Modify: `deployment/target.py`
- Modify: `runtime/deploy-target.schema.json`
- Modify: `runtime/deploy-target.example.json`
- Modify: `tests/deploy/test_deploy_target_contract.py`
- Modify: `tests/deploy/test_release_inventory.py`
- Test: `tests/deploy/test_main_deploy_ledger.py`
- Test: `tests/deploy/test_generated_overlay.py`
- Test: `tests/deploy/test_main_adapter_protocols.py`

**Interfaces:**
- Produces: `DeploymentRecord`, `DeploymentOutcome`, `DagStateSnapshot`, `BaselineRecord`
- Produces: `deployment_id(repository, candidate_sha, target_fingerprint) -> str`
- Produces: `DeploymentLedger.acquire_lock()`, `.begin()`, `.complete()`, `.previous_success()`, `.baseline()`
- Produces: `OverlayArtifact`, `render_release_overlay(target, checkout_root, candidate_sha)`, `render_baseline_overlay(target)`, `AtomicOverlayStore.install(artifact)` and `.restore(content, checksum)`
- Produces: `AirflowReadAdapter`, `AirflowMutationAdapter`, `ComposeAdapter`, `GitAdapter`, `HealthAdapter`, `Clock` protocols and deterministic fakes

- [ ] **Step 1: ledger/protocol RED tests**

Add isolated tests named `test_same_successful_sha_is_idempotent_noop`, `test_live_lock_rejects_concurrent_deploy`, `test_corrupt_partial_and_rollback_failed_records_are_not_candidates`, and `test_protocols_expose_no_trigger_backfill_clear_or_unrestricted_shell`. The tests must assert exact return values, exact fake event logs, and absence of subprocess calls; no empty bodies or placeholder assertions are allowed.

Add target/schema RED cases for required `local_state.generated_overlay_file`, repository-external `mounts.runtime_root`, normalized path alias rejection, and collision between overlay/ledger/lock paths. Add overlay RED cases that parse generated YAML and require exactly the target code services and two long-syntax bind mounts per service, targets `/opt/airflow/dags` and `/opt/airflow/dbt`, release sources exactly `<runtime_root>/releases/<lowercase-sha>/{dags,dbt}` with both read-only, exact SHA별 `ASK_SEOUL_DBT_ARTIFACT_ROOT`, no credential fields, and deterministic bytes/checksum. Baseline sources are exactly the validated current `dags_host_path` and `dbt_host_path`, with DAG read-only, dbt read-write, and no artifact environment.

Require `.tmp → flush → os.fsync → os.replace` for record and index. Checksum excludes only top-level `record_sha256`. A reader ignores `.tmp`, bad checksum, partial and current failed records.

- [ ] **Step 2: RED 확인**

Run: `python -m pytest tests/deploy/test_deploy_target_contract.py tests/deploy/test_generated_overlay.py tests/deploy/test_main_deploy_ledger.py tests/deploy/test_main_adapter_protocols.py -q`

- [ ] **Step 3: minimal implementation**

Extend `local_state` with required `generated_overlay_file`; validate it and `runtime_root` as absolute repository-external non-root paths distinct from ledger and lock paths. Use an OS-exclusive lock at the validated target lock file. `previous_success` requires outcome `success`, health `passed`, completed timestamp, exact code SHA, target fingerprint, overlay checksum, and restorable private overlay bytes. `baseline-candidate://existing-local` is not rollback-eligible; only checksum-valid `baseline://existing-local` with rehearsal `passed` is eligible. Overlay and ledger writes use sibling temp files plus `flush → fsync → os.replace`; partial temp files are ignored.

- [ ] **Step 4: GREEN**

Run the four test files; expected PASS with no subprocess.

---

### Task 3: pause·drain·deploy·health·rollback state machine

**Files:**
- Create: `deployment/main_orchestrator.py`
- Test: `tests/deploy/test_main_orchestrator.py`
- Test: `tests/deploy/test_main_rollback.py`

**Interfaces:**
- Consumes: Task 2 protocols, `DeployTarget`, candidate and previous-success SHA
- Produces: `MainDeploymentOrchestrator.deploy(identity, target) -> DeploymentResult`

- [ ] **Step 1: ordered state-machine RED tests**

Create separate deterministic tests for these exact cases: success event order; capture before first pause; only the exact ten DAGs change; bounded drain polls only the writer allowlist; checkout failure restores the snapshot without overlay install/deploy; dry-run failure restores without stable overlay replacement; post-install/deploy/health failure restores the prior eligible overlay and redeploys it; rollback failure leaves every Weather DAG paused; first cutover without an approved rehearsed baseline fails before pause. Every test asserts the complete fake event log rather than a subsequence.

- [ ] **Step 2: RED 확인**

Run: `python -m pytest tests/deploy/test_main_orchestrator.py tests/deploy/test_main_rollback.py -q`

- [ ] **Step 3: exact state machine 구현**

Order is `verify → lock → eligible rollback lookup → capture → pause → drain → detached checkout → render candidate overlay → candidate config/dry-run → atomic stable overlay install → deploy exact code services → health → restore pause snapshot → success record`. Poll interval/deadline come from validated target. Failures before stable overlay install restore the pause snapshot and do not deploy. Failures after install keep all Weather DAGs paused, atomically restore the latest eligible previous-success or baseline overlay, redeploy through the same allowlist, verify health, then restore the snapshot. Rollback failure records `rollback_failed` and keeps all ten DAGs paused.

- [ ] **Step 4: GREEN**

Run the two test files; expected all failure-injection paths PASS.

---

### Task 4: concrete argv adapters and single `main_cli`

**Files:**
- Create: `deployment/airflow_adapter.py`
- Create: `deployment/compose_adapter.py`
- Create: `deployment/git_adapter.py`
- Create: `deployment/github_evidence.py`
- Create: `deployment/health_adapter.py`
- Create: `deployment/main_cli.py`
- Create: `deployment/cutover_cli.py`
- Test: `tests/deploy/test_main_adapter_boundaries.py`
- Test: `tests/deploy/test_main_cli.py`
- Test: `tests/deploy/test_cutover_cli.py`

**Interfaces:**
- Concrete adapters consume validated `DeployTarget` and injected `CommandRunner`
- Read-only CLI: `python -m deployment.main_cli verify-main --event-path "$env:GITHUB_EVENT_PATH" --workflow-ref "$env:GITHUB_WORKFLOW_REF" --workflow-sha "$env:GITHUB_WORKFLOW_SHA"`
- State-changing CLI: `python -m deployment.main_cli deploy-main --event-path "$env:GITHUB_EVENT_PATH" --workflow-ref "$env:GITHUB_WORKFLOW_REF" --workflow-sha "$env:GITHUB_WORKFLOW_SHA"`
- One-time operator CLI, never a workflow command: `python -m deployment.cutover_cli inspect|activate ...`

- [ ] **Step 1: command boundary RED tests**

Airflow mutations are only exact `dags pause|unpause -o json -y <dag_id>`. All Airflow commands use base Compose files plus the stable overlay. Candidate validation uses base files plus the temp candidate overlay for `config` and `up --dry-run`; deployment uses base files plus the stable overlay for `up -d --no-deps` with the sorted exact Airflow code-service set. Parsed config/dry-run must contain no mutation target outside that set. Release config additionally requires exact SHA artifact environment and one unchanged writable `/opt/airflow/logs` mount per code service. Git creates `<runtime_root>/releases/<sha>` as a repository-external detached checkout at exact candidate SHA and verifies `HEAD`, `dags/`, and `dbt/`. Health first proves stable overlay bytes/checksum equal the expected candidate or rollback artifact, then checks only exact code-service Compose health and Airflow DAG inventory/import errors. dbt parse·serving contract is not regenerated on the runner; it is already mandatory inside the exact source SHA's `CI / required`. GitHub evidence performs only bounded GET endpoints and never prints bodies.

Tests reject shell strings/metacharacters, option injection, path escape, duplicate options, `down`, `restart`, `--force-recreate`, data service, trigger/backfill/dbt/Trino/wrangler commands, unknown Airflow CLI fingerprint, and stale SHA. Invalid local event/ref/SHA envelopes perform no subprocess call. After that local envelope passes and before identity validation, only exact bounded `gh api --method GET` evidence reads are allowed; Docker/Airflow/Git/Compose subprocess calls remain zero.

- [ ] **Step 2: RED 확인**

Run: `python -m pytest tests/deploy/test_main_adapter_boundaries.py tests/deploy/test_main_cli.py -q`

- [ ] **Step 3: adapters/CLI 구현**

`verify-main` order is size-bounded event parse → read governance/branch/source-run/job/linked-check evidence → validate identity → print only `verify-main:identity:ok`. It has no target, Docker, Airflow, filesystem-write or unrestricted command adapter. `deploy-main` repeats the same identity validation, then loads target from runner-local `WEATHER_DEPLOY_TARGET_PATH` → verifies Airflow CLI contract using the current stable/baseline overlay → constructs ledger and explicit adapters → calls orchestrator. Both reject invocation unless workflow ref/path and event are exact. Errors print `<command>:<stage>:<category>` only. Target paths, API bodies, raw subprocess output and credentials are never CLI arguments or output.

`cutover_cli inspect` is read-only and reports only sanitized target/CLI/inventory/baseline digests and booleans. `activate` rejects `GITHUB_ACTIONS=true`, requires exact user-confirmed target and baseline SHA-256 values, reruns all read-only checks under the cutover lock, validates the baseline candidate with Compose config/dry-run, atomically installs and restores the same baseline overlay bytes as a rehearsal, records only `baseline://existing-local` with `rehearsal=passed`, and installs the reviewed target file last. It does not start the runner, set GitHub state, pause/unpause DAGs, or deploy code services. `main_cli` exposes only `verify-main` and `deploy-main`; every workflow route rejects `cutover_cli`.

- [ ] **Step 4: GREEN**

Run the three test files plus `python -m compileall -q deployment`.

---

### Task 5: thin workflow, policy cleanup, first-cutover gate, and full L0

**Files:**
- Create: `.github/workflows/deploy-main.yml`
- Modify: `tools/workflow_policy.py`
- Modify: `tests/repository/test_workflow_policy.py`
- Create: `tests/deploy/test_deploy_main_workflow.py`
- Create: `docs/operations/main-auto-deploy-first-cutover.md`
- Create: `tests/repository/test_main_auto_deploy_docs.py`
- Modify: `docs/operations/predeployment-approval-gate.md`
- Modify: `docs/operations/github-bootstrap.md`
- Modify: `docs/superpowers/plans/2026-08-14-ci-bootstrap.md`
- Modify: `docs/superpowers/plans/2026-08-14-public-readiness.md`
- Delete: `deployment/plan.py`
- Delete: `tests/deploy/test_deployment_plan.py`
- Delete: `docs/superpowers/plans/2026-08-14-release-preflight.md`
- Delete: `docs/superpowers/plans/2026-08-14-deployment-engine.md`
- Delete: `docs/superpowers/specs/2026-08-14-ci-cd-release-public-design.md`
- Modify: `tools/verify_repository.ps1`
- Modify: `tests/repository/test_runtime_safety.py`
- Modify: `provenance/source-files.jsonl`

**Interfaces:**
- Workflow has one GitHub-hosted read-only entrypoint `verify-main` and one self-hosted state-changing entrypoint `deploy-main`, both with exact event/workflow arguments.

- [ ] **Step 1: workflow/policy RED tests**

Require only `workflow_run` for `CI`/`completed`/`main`, read-only workflow permissions including `pull-requests: read`, `concurrency.group: weather-main-deploy`, `cancel-in-progress: false`, and a job-level event guard that checks allowed governance mode, `WEATHER_DEPLOYMENT_ENABLED == enabled`, and source name/path/event/branch/status/conclusion before either job runs. The `ubuntu-latest` preflight uses only pinned checkout/setup-python plus stdlib-only exact `verify-main`; the `[self-hosted, windows, weather-prod]` deploy job has `needs: verify-main`, timeout 60, pinned checkout와 exact `deploy-main` 두 step만 사용한다. self-hosted Python `3.11`·PyYAML은 최초 승인된 cutover에서 workflow 밖에 사전 준비하며 어떤 deploy workflow도 package install이나 `setup-python`으로 환경을 바꾸지 않는다. guarded_private uses the workflow read token and private exact merged-PR revalidation; protected uses exact `GH_TOKEN: ${{ secrets.WEATHER_GOVERNANCE_READ_TOKEN }}` with only `Administration: read`, `Actions: read`, `Checks: read`, `Contents: read`, and `Pull requests: read`. Checkout ref is `${{ github.workflow_sha }}`, never event text. Ban all other events, local/unpinned actions, direct Docker/Airflow commands and extra self-hosted steps.

- [ ] **Step 2: thin workflow GREEN**

Run: `python -m pytest tests/deploy/test_deploy_main_workflow.py tests/repository/test_workflow_policy.py -q`

- [ ] **Step 3: first-cutover document/test**

Document exact STOP report fields: target main SHA, code services, 10 DAG pause states, writer running/queued counts, target/CLI fingerprints, sanitized candidate baseline-overlay fingerprint, stable overlay path presence only as a boolean, protected mode의 `WEATHER_GOVERNANCE_READ_TOKEN` existence/permission result without its value, zero dbt/Trino/D1/R2 writes, rollback and rollback-failure pause behavior. The approved cutover procedure is `read-only inspect → report/STOP → user approval → install target and baseline overlay → config/dry-run plus baseline restore rehearsal → protected인 경우 least-privilege governance secret configure/readback → start runner → set and read back WEATHER_DEPLOYMENT_ENABLED=enabled`. guarded_private에는 secret을 설치하지 않으며 public/internal 또는 extra writer가 있으면 배포를 중단한다. State deploy jobs remain skipped before that exact enable flag; later eligible guarded_private or protected main merges require no additional approval.

- [ ] **Step 4: remove superseded Release artifacts**

Delete only the listed Release-plan files and replace Draft/Publish/deploy-release references in `docs/operations/predeployment-approval-gate.md`, `docs/operations/github-bootstrap.md`, `2026-08-14-ci-bootstrap.md`, `2026-08-14-public-readiness.md`, and workflow-policy fixtures with the main-CI model. Keep `deployment/redaction.py`, target, inventory and CLI compatibility security fixes. Update workflow policy so absent Release workflows are no longer modeled or accepted, and so the actual tracked `deploy-main.yml` is the sole allowed self-hosted mutation route.

- [ ] **Step 5: repository verifier and provenance**

Add pure `tests/deploy` to verifier only after subprocess sentinels prove no runtime call. Refresh provenance once after source stability.

Run:

```powershell
python -m pytest tests/deploy tests/repository -q
python -m tools.workflow_policy --repo-root .
python -m tools.refresh_provenance --repo-root . --manifest provenance/source-files.jsonl
powershell -NoProfile -ExecutionPolicy Bypass -File tools\verify_repository.ps1
```

Expected: all PASS; no Docker/Airflow/GitHub process.

## Stop Gate

- 이 계획 완료는 로컬 자동배포 코드·fake L0 완료다.
- stage·commit·push·PR, repository variable/protection 변경, runner 활성화는 별도 Git 승인 전까지 실행하지 않는다.
- 실제 target/baseline 설치와 Airflow state change는 `docs/operations/main-auto-deploy-first-cutover.md` 보고 후 사용자 승인 전까지 실행하지 않는다.
