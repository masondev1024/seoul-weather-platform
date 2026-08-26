# 비공개 보호형 Weather 자동 배포 구현 계획

> 사람용 안내: 비공개 개인 저장소에서 실수로 배포하지 않도록 만든 상세 작업표다.
> 설정 키와 검사 이름은 실행 코드와 맞춰야 하므로 원래 표기를 유지한다.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** private 개인 저장소에서 exact same-repository `dev → main` 병합 PR과 성공한 main CI SHA만 `guarded_private` 자동 배포로 연결한다.

**Architecture:** 기존 protected identity·Airflow 상태 머신은 유지하고 GitHub evidence 계층에 governance mode와 commit-associated merged PR 증거를 추가한다. Workflow는 두 mode의 token source를 분리하며, guarded mode는 protection readback 없이 private repository·merged PR·branch/SHA-bound GitHub Actions checks를 검증한다.

**Tech Stack:** Python 3.11, pytest, GitHub Actions YAML, `gh api`, PyYAML, repository provenance manifest.

## Global Constraints

- `guarded_private`는 native protection과 같은 보안 경계가 아니라 sole-owner private repository의 실수 방지 모델이다.
- `guarded_private`는 exact `private == true`, `visibility == private`, `default_branch == main`에서만 허용한다.
- protected와 guarded 모두 exact same-repository merged `dev → main` PR, exact current main SHA, `CI / required`, `Promotion Source / required`, linked `github-actions` check-run을 검증한다.
- `protected`만 dev/main protection readback을 수행하며, `guarded_private` evidence는 `protections=null`이어야 한다.
- guarded token은 `${{ github.token }}`, protected token은 `WEATHER_GOVERNANCE_READ_TOKEN`이며 서로 fallback하지 않는다.
- Workflow permission은 exact `actions: read`, `checks: read`, `contents: read`, `pull-requests: read`다.
- CI의 self-hosted `dagbag-runtime` route는 계속 protected branch push 전용이다.
- `--force-recreate`, 전체 stack, `airflow-init`, Postgres, Trino, Marquez, dbt run/build, DAG trigger/backfill은 자동배포에서 금지한다.
- 구현 중 Airflow, Docker, GitHub variable/secret, runner, dbt, Trino, D1, R2 state를 변경하지 않는다.
- 모든 production 변경은 실패하는 regression test를 먼저 확인한 뒤 최소 구현한다.
- 각 task는 수정 전에 대상 파일의 `git log --follow -p`를 읽고 기존 fail-closed 의도를 report에 한두 문장으로 남긴다.

---

### Task 1: Mode-aware GitHub evidence와 merged PR identity

**Files:**
- Modify: `tools/github_protection.py:59-132`
- Modify: `deployment/github_evidence.py:178-532`
- Modify: `deployment/main_identity.py:265-374`
- Test: `tests/repository/test_github_protection.py`
- Test: `tests/deploy/test_github_evidence.py`
- Test: `tests/deploy/test_main_deploy_identity.py`

**Interfaces:**
- Produces: `GhRunner.api_list(method: str, endpoint: str) -> list[dict[str, Any]]`
- Produces: `read_main_identity_inputs(..., governance_mode: object, ...) -> MainIdentityInputs`
- Produces: `MainIdentityInputs.as_kwargs()` keys `governance_mode`, `promotion_pr`, `protections`
- Produces: `validate_main_deploy_identity(..., governance_mode: object, promotion_pr: object, protections: object, ...) -> MainDeployIdentity`

- [ ] **Step 1: Add failing runner JSON-list tests**

Add tests showing `SubprocessGhRunner.api_list("GET", endpoint)` accepts only a top-level list of JSON objects and rejects scalar, mapping, nested non-object item, PUT, payload, malformed JSON and subprocess failure without printing response bodies.

```python
def test_subprocess_runner_reads_exact_json_object_list() -> None:
    runner = SubprocessGhRunner(run=_completed(stdout='[{"number":7}]'))
    assert runner.api_list("GET", "/repos/o/r/commits/" + SHA + "/pulls?per_page=2&page=1") == [{"number": 7}]
```

- [ ] **Step 2: Run the runner tests and verify RED**

Run: `python -m pytest tests/repository/test_github_protection.py -k "api_list" -q`

Expected: FAIL because `api_list` does not exist.

- [ ] **Step 3: Implement the bounded list transport**

Factor the existing subprocess invocation into a private JSON decoder. Preserve `api()` as dict-only and add `api_list()` as GET-only/list-of-dict-only. Never accept arbitrary method/payload or expose raw stdout/stderr.

```python
class GhRunner(Protocol):
    def api(self, method: str, endpoint: str, payload: dict[str, Any] | None = None) -> dict[str, Any]: ...
    def api_list(self, method: str, endpoint: str) -> list[dict[str, Any]]: ...
```

- [ ] **Step 4: Add failing evidence and identity tests**

Add otherwise-valid fixtures for both modes. The guarded positive fixture must use `governance_mode="guarded_private"`, private repo, `protections=None`, exact one merged PR and no protection endpoint calls. The protected positive must retain exact dev/main protections and also include the merged PR.

```python
PROMOTION_PR = {
    "number": 7,
    "merged_at": "2026-08-15T00:00:00Z",
    "merge_commit_sha": SHA,
    "base": {"ref": "main", "repo": {"full_name": REPOSITORY}},
    "head": {"ref": "dev", "repo": {"full_name": REPOSITORY}},
}
```

Add isolated negative cases for zero/two PRs, bootstrap SHA, feature/fork/unmerged/wrong-SHA PR, public/internal guarded repo, guarded protections present, protected protections absent, invalid mode, stale main, wrong source/check/app evidence and defensive snapshot immutability.

- [ ] **Step 5: Run evidence/identity tests and verify RED**

Run: `python -m pytest tests/deploy/test_github_evidence.py tests/deploy/test_main_deploy_identity.py -q`

Expected: FAIL because governance mode, PR evidence and guarded path are not implemented.

- [ ] **Step 6: Implement canonical mode and PR evidence**

Use exact endpoint `/repos/{repository}/commits/{sha}/pulls?per_page=2&page=1`. Require exactly one returned item and normalize only `number`, `merged_at`, `merge_commit_sha`, `base.ref/repo.full_name`, `head.ref/repo.full_name`. Both modes read repo → source run → jobs → linked checks → PR → optional protections → main HEAD. `protected` retains existing exact protection validation; `guarded_private` requires private repo and canonical `protections=None`.

```python
def _normalize_promotion_pr(values: list[dict[str, Any]], repository: str, sha: str) -> dict[str, Any]:
    if len(values) != 1:
        _reject()
    # Validate exact dev -> main, same repo, merged_at, merge_commit_sha.
```

- [ ] **Step 7: Run Task 1 GREEN verification**

Run:

```powershell
python -m pytest tests/repository/test_github_protection.py tests/deploy/test_github_evidence.py tests/deploy/test_main_deploy_identity.py -q
python -m compileall -q tools/github_protection.py deployment/github_evidence.py deployment/main_identity.py
python -m ruff check tools/github_protection.py deployment/github_evidence.py deployment/main_identity.py tests/repository/test_github_protection.py tests/deploy/test_github_evidence.py tests/deploy/test_main_deploy_identity.py
```

- [ ] **Step 8: Commit Task 1**

```powershell
git add -- tools/github_protection.py deployment/github_evidence.py deployment/main_identity.py tests/repository/test_github_protection.py tests/deploy/test_github_evidence.py tests/deploy/test_main_deploy_identity.py
git commit -m "feat(deploy): guarded private 승격 증거 검증"
```

---

### Task 2: CLI governance gate와 mutation-before-identity 차단

**Files:**
- Modify: `deployment/main_cli.py:112-149`
- Test: `tests/deploy/test_main_cli.py`

**Interfaces:**
- Consumes: Task 1 `read_main_identity_inputs(..., governance_mode=...)`
- Consumes: Task 1 `MainIdentityInputs.as_kwargs()`
- Produces: `_validate_gate(invocation) -> tuple[repository, token, governance_mode]`

- [ ] **Step 1: Add failing guarded CLI tests**

Cover `verify-main` and `deploy-main` guarded success using a non-empty read token supplied as `GH_TOKEN`, exact mode propagation to evidence, and no target/runtime imports before identity. Keep protected success. Add invalid/missing mode, token, repository, flag, workflow ref/SHA/event file negatives that make zero GitHub or runtime calls.

```python
def test_guarded_verify_passes_mode_to_evidence(monkeypatch, event_path) -> None:
    monkeypatch.setenv("GOVERNANCE_MODE", "guarded_private")
    # Capture read_main_identity_inputs kwargs and assert exact mode.
```

- [ ] **Step 2: Run CLI tests and verify RED**

Run: `python -m pytest tests/deploy/test_main_cli.py -q`

Expected: guarded positive FAILS at `invalid-environment`.

- [ ] **Step 3: Implement minimal mode propagation**

Allow only `protected` and `guarded_private`, return the validated mode from `_validate_gate`, and pass it to the evidence reader. Do not change CLI commands, output categories, target loading order or runtime adapter construction.

```python
repository, token, governance_mode = _validate_gate(invocation)
inputs = read_main_identity_inputs(..., governance_mode=governance_mode, ...)
```

- [ ] **Step 4: Run Task 2 GREEN verification**

Run:

```powershell
python -m pytest tests/deploy/test_main_cli.py tests/deploy/test_github_evidence.py tests/deploy/test_main_deploy_identity.py -q
python -m compileall -q deployment/main_cli.py
python -m ruff check deployment/main_cli.py tests/deploy/test_main_cli.py
```

- [ ] **Step 5: Commit Task 2**

```powershell
git add -- deployment/main_cli.py tests/deploy/test_main_cli.py
git commit -m "feat(deploy): guarded private CLI gate 허용"
```

---

### Task 3: Deploy Main workflow와 workflow-policy 계약

**Files:**
- Modify: `.github/workflows/deploy-main.yml`
- Modify: `tools/workflow_policy.py:52-80,942-981,1228-1369`
- Test: `tests/deploy/test_deploy_main_workflow.py`
- Test: `tests/repository/test_workflow_policy.py`

**Interfaces:**
- Consumes: Task 2 CLI accepts exact `GOVERNANCE_MODE` and `GH_TOKEN`.
- Produces: guarded/protected two-clause Deploy Main workflow contract.

- [ ] **Step 1: Add failing workflow behavior tests**

Parse the real workflow and assert exact permissions include `pull-requests: read`, both job guards accept only the two mode clauses with all existing source constraints, guarded steps use `${{ github.token }}`, protected steps use `${{ secrets.WEATHER_GOVERNANCE_READ_TOKEN }}`, checkout/setup/action pins and order remain exact, and self-hosted job still needs hosted verification.

Add mutations that remove PR read, weaken either clause, swap/fallback token expressions, add package install/setup-python to self-hosted, add direct Docker/Airflow command, alter checkout ref, or allow PR/manual/release events.

- [ ] **Step 2: Run workflow tests and verify RED**

Run: `python -m pytest tests/deploy/test_deploy_main_workflow.py tests/repository/test_workflow_policy.py -q`

Expected: FAIL because current workflow is protected-only and lacks PR permission/mode-specific steps.

- [ ] **Step 3: Implement exact workflow and policy**

Keep the two jobs `verify-main` and `deploy-main`. Their job guards must be exact DNF over `protected` and `guarded_private` while every non-mode atom remains identical. Keep checkout first; hosted setup-python second. Add mode-specific CLI steps whose `if`, `GH_TOKEN`, `GOVERNANCE_MODE`, `DEPLOYMENT_ENABLED`, command and shell are exact. Protected secret absence must fail its CLI step and never select `${{ github.token }}`.

```yaml
permissions:
  actions: read
  checks: read
  contents: read
  pull-requests: read
```

The mode portion of each job guard is exactly:

```yaml
(
  vars.WEATHER_GOVERNANCE_MODE == 'protected' ||
  vars.WEATHER_GOVERNANCE_MODE == 'guarded_private'
) &&
vars.WEATHER_DEPLOYMENT_ENABLED == 'enabled'
```

Hosted mode steps use these exact contracts after checkout/setup-python:

```yaml
- name: Verify guarded private main identity
  if: vars.WEATHER_GOVERNANCE_MODE == 'guarded_private'
  run: python -m deployment.main_cli verify-main --event-path "$env:GITHUB_EVENT_PATH" --workflow-ref "$env:GITHUB_WORKFLOW_REF" --workflow-sha "$env:GITHUB_WORKFLOW_SHA"
  shell: pwsh
  env:
    GH_TOKEN: ${{ github.token }}
    GOVERNANCE_MODE: ${{ vars.WEATHER_GOVERNANCE_MODE }}
    DEPLOYMENT_ENABLED: ${{ vars.WEATHER_DEPLOYMENT_ENABLED }}
- name: Verify protected main identity
  if: vars.WEATHER_GOVERNANCE_MODE == 'protected'
  run: python -m deployment.main_cli verify-main --event-path "$env:GITHUB_EVENT_PATH" --workflow-ref "$env:GITHUB_WORKFLOW_REF" --workflow-sha "$env:GITHUB_WORKFLOW_SHA"
  shell: pwsh
  env:
    GH_TOKEN: ${{ secrets.WEATHER_GOVERNANCE_READ_TOKEN }}
    GOVERNANCE_MODE: ${{ vars.WEATHER_GOVERNANCE_MODE }}
    DEPLOYMENT_ENABLED: ${{ vars.WEATHER_DEPLOYMENT_ENABLED }}
```

Self-hosted mode steps have the same conditions/env split and exact `deploy-main` command, after one pinned checkout step and with no setup/package-install step.

Update policy parsing so Deploy Main expects exactly two approved guard clauses while CI protected self-hosted clauses remain unchanged.

- [ ] **Step 4: Run Task 3 GREEN verification**

Run:

```powershell
python -m pytest tests/deploy/test_deploy_main_workflow.py tests/repository/test_workflow_policy.py -q
python -m tools.workflow_policy --repo-root .
python -m compileall -q tools/workflow_policy.py
python -m ruff check tools/workflow_policy.py tests/deploy/test_deploy_main_workflow.py tests/repository/test_workflow_policy.py
```

- [ ] **Step 5: Commit Task 3**

```powershell
git add -- .github/workflows/deploy-main.yml tools/workflow_policy.py tests/deploy/test_deploy_main_workflow.py tests/repository/test_workflow_policy.py
git commit -m "feat(ci): guarded private main 자동 배포 허용"
```

---

### Task 4: 운영 문서, provenance와 전체 검증

**Files:**
- Modify: `docs/operations/github-bootstrap.md`
- Modify: `docs/operations/main-auto-deploy-first-cutover.md`
- Modify: `docs/operations/predeployment-approval-gate.md`
- Modify: `docs/superpowers/specs/2026-08-15-main-merge-auto-deploy-design.md`
- Modify: `docs/superpowers/plans/2026-08-14-ci-bootstrap.md`
- Modify: `docs/superpowers/plans/2026-08-14-public-readiness.md`
- Modify: `docs/superpowers/plans/2026-08-15-main-merge-auto-deploy.md`
- Modify: `provenance/source-files.jsonl`

**Interfaces:**
- Consumes: Tasks 1-3 exact guarded/private and protected behavior.
- Produces: one consistent first-cutover procedure with deployment disabled until separate approval.

- [ ] **Step 1: Align operational documents**

State that guarded mode is sole-owner/private/accident-prevention only; exact merged PR is revalidated; public/internal or extra writer stops guarded deployment; protected mode remains stronger; first cutover still requires runner-local target/baseline/capability report and explicit user approval. Remove requirements to install the governance secret for guarded mode, but retain it for protected mode with Pull requests read.

Human-only prose gets no source-text regression test. Any executable workflow snippet retained in documentation must be parsed by the existing workflow-policy fixtures and match Task 3's production contract.

- [ ] **Step 2: Run full secretless verification before provenance refresh**

Run:

```powershell
python -m pytest tests/deploy tests/repository -q
python -m tools.workflow_policy --repo-root .
python -m compileall -q deployment tools tests
python -m ruff check deployment tools tests
git diff --check
```

- [ ] **Step 3: Refresh provenance once and run root verifier**

Run:

```powershell
python -m tools.refresh_provenance --repo-root .
powershell -NoProfile -ExecutionPolicy Bypass -File tools/verify_repository.ps1
```

Expected: provenance current and all repository/deploy tests pass; Windows symlink privilege skips may remain explicitly reported.

- [ ] **Step 4: Commit Task 4**

```powershell
git add -- docs/operations/github-bootstrap.md docs/operations/main-auto-deploy-first-cutover.md docs/operations/predeployment-approval-gate.md docs/superpowers/specs/2026-08-15-main-merge-auto-deploy-design.md docs/superpowers/plans/2026-08-14-ci-bootstrap.md docs/superpowers/plans/2026-08-14-public-readiness.md docs/superpowers/plans/2026-08-15-main-merge-auto-deploy.md provenance/source-files.jsonl tests/repository/test_workflow_policy.py docs/superpowers/plans/2026-08-15-guarded-private-auto-deploy.md
git commit -m "docs(deploy): guarded private 전환 절차 정렬"
```

---

## Final Review and Handoff

- [ ] Generate a whole-branch review package from merge-base `b01acb5ab07d426b0f2dc26754a37caa0600d28c` to HEAD.
- [ ] Obtain independent spec and code-quality approval; route any Critical/Important finding through one fix wave and scoped re-review.
- [ ] Re-run `powershell -NoProfile -ExecutionPolicy Bypass -File tools/verify_repository.ps1` after the final reviewed diff.
- [ ] Push the feature branch and open a ready PR to `dev` only after fresh verification.
- [ ] Do not set `WEATHER_DEPLOYMENT_ENABLED`, install/start a runner, pause DAGs, or deploy Airflow in this implementation PR.
