# CI·Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Weather 전용 저장소에 fork-safe CI, `dev → main` 승격 증거, fail-closed required check, CODEOWNERS, 최초 `main`/default-branch bootstrap 절차를 구축한다.

**Architecture:** 정책 판정은 네트워크 없는 Python 모듈로 만들고 GitHub workflow는 그 판정기를 호출하는 얇은 adapter로 둔다. 모든 PR CI는 GitHub-hosted에서 실행하고, 실제 Airflow image의 DagBag은 native-protected `dev`·`main` push에서만 격리된 self-hosted Docker job으로 확인한다. 원격 branch 생성·default 변경·protection 설정은 로컬 구현과 검증이 끝난 뒤 별도 Git 승인 구간에서만 수행한다.

**Tech Stack:** Python 3.11.15, pytest 9.0.3, PyYAML 6.0.2, PowerShell, GitHub Actions, dbt-core 1.10.22, dbt-trino 1.10.2, Airflow 3.2.2 Docker image.

## Global Constraints

- 개발 통합 base는 `dev`, 운영·default branch는 bootstrap 이후 `main`이다.
- `main` 승격은 같은 저장소의 `dev → main` PR만 허용한다.
- 최초 CI PR을 열거나 재실행하기 전에 별도 승인으로 repository variable을 `WEATHER_GOVERNANCE_MODE=guarded_private`로 설정하고 exact readback한다. missing·`protected`·그 밖의 값이면 최초 CI와 bootstrap을 중단한다.
- 어떤 PR 코드도 self-hosted runner에서 실행하지 않는다.
- native protection API readback과 최초 cutover 승인 전에는 production runner 등록·활성화와 `main` 자동 배포를 금지한다. `guarded_private`는 진단 mode일 뿐 배포 fallback이 아니다.
- `pull_request_target`은 모든 workflow에서 금지한다.
- CI에서 Airflow/Docker state mutation, dbt `run`/`build`, Trino·D1·R2 write를 실행하지 않는다.
- non-local action ref는 아래 검증된 full commit SHA만 사용한다.
  - `actions/checkout@11d5960a326750d5838078e36cf38b85af677262`
  - `actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065`
- local `uses: ./...` action은 금지한다. self-hosted job의 `uses:`는 위 두 action만 허용한다.
- required job name `CI / required`와 `Promotion Source / required`는 전체 workflow에서 각각 한 번만 존재하고 `.github/workflows/ci.yml`만 소유한다.
- stage·commit·push·PR·원격 GitHub 설정은 각각 실행 시점의 사용자 명시 승인 뒤에만 수행한다.
- Airflow 배포·DAG pause/unpause·pipeline 가동은 이 계획 범위가 아니다.

---

### Task 1: CI required 집계 판정기

**Files:**
- Create: `tools/ci_required_gate.py`
- Test: `tests/repository/test_ci_required_gate.py`

**Interfaces:**
- Produces: `GateDecision(allowed: bool, reason: str)`
- Produces: `decide_required_ci(event_name: str, git_ref: str, governance_mode: str, results: Mapping[str, str]) -> GateDecision`
- Required result keys: `repository-contract`, `dbt-weather`, `airflow-tests`, `dagbag-policy`, `dagbag-runtime`, `promotion-source`, `governance-mode`

- [ ] **Step 1: PR·protected push·guarded mode 규칙의 실패 테스트 작성**

```python
BASE = {
    "repository-contract": "success",
    "dbt-weather": "success",
    "airflow-tests": "success",
    "dagbag-policy": "success",
    "dagbag-runtime": "success",
    "promotion-source": "success",
    "governance-mode": "success",
}

def test_pull_request_requires_runtime_skip():
    assert decide_required_ci(
        "pull_request", "refs/pull/7/merge", "protected",
        BASE | {"dagbag-runtime": "skipped"},
    ).allowed

def test_protected_main_push_requires_runtime_success():
    assert decide_required_ci("push", "refs/heads/main", "protected", BASE).allowed

def test_pull_request_rejects_runtime_execution():
    assert not decide_required_ci(
        "pull_request", "refs/pull/7/merge", "protected", BASE,
    ).allowed

def test_guarded_private_push_requires_runtime_skip_and_reports_degraded():
    decision = decide_required_ci(
        "push", "refs/heads/main", "guarded_private",
        BASE | {"dagbag-runtime": "skipped"},
    )
    assert decision.allowed and decision.reason == "degraded_guarded_private"
```

- [ ] **Step 2: RED 확인**

Run: `python -m pytest tests/repository/test_ci_required_gate.py -q`

Expected: `ModuleNotFoundError: No module named 'tools.ci_required_gate'`.

- [ ] **Step 3: 최소 구현**

`dagbag-runtime`은 모든 PR에서 정확히 `skipped`, protected `dev|main` push에서 `success`, `guarded_private` push에서 `skipped`여야 한다. guarded 결과는 `degraded_guarded_private`를 출력하며 자동 배포 활성화 증거로 사용할 수 없다. 나머지는 모두 `success`만 통과시키고 missing/cancelled/skipped/failure를 fail-closed 처리한다. CLI는 반복 `--result name=value`와 `--governance-mode`를 받고 허용 시 `0`, 차단 시 `1`, 잘못된 인자는 `2`를 반환한다.

- [ ] **Step 4: GREEN 확인**

Run: `python -m pytest tests/repository/test_ci_required_gate.py -q`

Expected: PASS.

- [ ] **Step 5: 승인 후 경로 지정 commit**

```powershell
git add tools/ci_required_gate.py tests/repository/test_ci_required_gate.py
git commit -m "test(ci): required 집계 판정을 fail-closed로 고정"
```

---

### Task 2: `dev → main` promotion evidence

**Files:**
- Create: `tools/promotion_source.py`
- Create: `tests/fixtures/github/pull-request-dev-main.json`
- Create: `tests/fixtures/github/push-main-associated-prs.json`
- Test: `tests/repository/test_promotion_source.py`

**Interfaces:**
- Produces: `validate_pull_request_event(event: Mapping[str, object], repository: str) -> PromotionDecision`
- Produces: `validate_main_push_associated_prs(prs: Sequence[Mapping[str, object]], repository: str, sha: str) -> PromotionDecision`
- Produces: `validate_initial_main_bootstrap_push(event, repository_readback, dev_branch_readback, main_branch_readback, repository, sha, governance_mode) -> PromotionDecision`
- CLI performs no network; workflow supplies `$GITHUB_EVENT_PATH` or sanitized `gh api repos/$GITHUB_REPOSITORY/commits/$GITHUB_SHA/pulls` JSON.

- [ ] **Step 1: fixtures와 RED 테스트 작성**

Valid PR fixture must contain:

```json
{
  "pull_request": {
    "base": {"ref": "main", "repo": {"full_name": "masondev1024/seoul-weather-platform"}},
    "head": {"ref": "dev", "repo": {"full_name": "masondev1024/seoul-weather-platform"}}
  }
}
```

Push fixture must contain a merged PR whose `base.ref == "main"`, `head.ref == "dev"`, both repo names match, and `merge_commit_sha == pushed sha`. Add negative tests for feature head, fork head, missing `merged_at`, wrong SHA, malformed JSON. Separately reproduce that the first creation of absent `main` cannot have an associated merged `dev → main` PR. The one-time bootstrap path must reject any mismatch in `guarded_private`, raw payload `created/deleted/before/ref/after/repository`, lowercase 40-hex SHA, remote repository/default branch or exact `dev`·`main` head readback.

- [ ] **Step 2: RED 확인**

Run: `python -m pytest tests/repository/test_promotion_source.py -q`

Expected: missing module failure.

- [ ] **Step 3: pure validator와 CLI 구현**

PR to `dev` is not a promotion and returns success with `not-required`; PR to `main` accepts only same-repository `dev`. 일반 push to `main` accepts only an associated merged `dev → main` PR for the exact SHA. A separate `initial-main-bootstrap` CLI may return `initial-bootstrap` only for a `guarded_private` branch-creation payload with `created == true`, `deleted == false`, `before == 40자리 zero SHA`, exact main ref/after/repository, remote default `dev`, and remote `dev`·`main` heads both equal to that SHA. Raw payload and path are never printed.

- [ ] **Step 4: GREEN 및 승인 후 commit**

Run: `python -m pytest tests/repository/test_promotion_source.py -q`

```powershell
git add tools/promotion_source.py tests/fixtures/github/pull-request-dev-main.json tests/fixtures/github/push-main-associated-prs.json tests/repository/test_promotion_source.py
git commit -m "feat(ci): main 승격 출처를 dev PR로 제한"
```

---

### Task 3: workflow/CODEOWNERS 정적 보안 정책

**Files:**
- Modify: `pyproject.toml`
- Create: `tools/workflow_policy.py`
- Test: `tests/repository/test_workflow_policy.py`

**Interfaces:**
- Produces: `WorkflowFinding(path: str, rule: str, summary: str)`
- Produces: `audit_workflows(repo_root: Path) -> list[WorkflowFinding]`
- Uses `yaml.load(text, Loader=yaml.BaseLoader)` so the key `on` stays a string.

- [ ] **Step 1: PyYAML을 exact pin으로 추가하고 RED 테스트 작성**

Add `PyYAML==6.0.2` to `[project.optional-dependencies].dev`. Tests create temporary workflows proving the scanner rejects:

```text
pull_request_target
uses: actions/checkout@v4
self-hosted on pull_request or workflow_dispatch
self-hosted push without protected mode and exact dev/main ref
Deploy Main workflow_run without exact `CI`/completed/main trigger, protected+enabled mode, source name `CI`, suffix-free path `.github/workflows/ci.yml`, separate `head_branch=main`, event/status/conclusion and workflow/head SHA equality
repository_dispatch, Release, workflow_dispatch 또는 PR에서 self-hosted deploy 진입
runtime mutation commands
docker compose exec/run container indirection
local actions and self-hosted actions outside the two pinned actions
self-hosted run commands outside the exact route allowlist
duplicate required check names or ownership outside `.github/workflows/ci.yml`
missing `if: always()` on CI / required
missing CODEOWNERS coverage
```

Mutation patterns include `docker compose up`, `docker build`, `--force-recreate`, `airflow dags pause`, `airflow dags unpause`, `airflow dags trigger`, `dbt run`, `dbt build`, `wrangler d1 execute`.

- [ ] **Step 2: RED 확인**

Run: `python -m pytest tests/repository/test_workflow_policy.py -q`

Expected: missing module failure.

- [ ] **Step 3: scanner와 CLI 구현**

Every external `uses:` must match `^[^@]+@[0-9a-f]{40}$`; local actions are forbidden. Self-hosted policy is event-aware: PR와 `workflow_dispatch`는 금지하고, CI runtime은 `protected` `push`의 exact `refs/heads/dev|main`만 허용한다. 자동배포는 `.github/workflows/deploy-main.yml` 하나에서만 허용한다. trigger는 `workflow_run`의 exact `CI`/`completed`/`main`, guard는 `protected`+`enabled`, source workflow name `CI`, suffix 없는 path `.github/workflows/ci.yml`, 별도 `head_branch=main`, event/status/conclusion과 `github.workflow_sha == workflow_run.head_sha`를 모두 요구한다. GitHub-hosted `verify-main` 성공 뒤 `[self-hosted, windows, weather-prod]`의 `deploy-main`만 실행할 수 있다. `repository_dispatch`, Release, manual dispatch 또는 다른 workflow path가 self-hosted deploy에 도달하면 finding이다. `docker compose exec`와 `run`은 내부 argv와 무관하게 mutation으로 판정하며 direct `docker compose config`와 `ps`만 기존 read-only 판정을 유지한다.

Self-hosted executable surface는 pinned checkout action 하나로 고정하고 route별 action/run step 순서와 argv가 정확히 같아야 한다. protected push checkout input은 `{persist-credentials: false}`만, Deploy Main checkout input은 `{ref: ${{ github.workflow_sha }}, persist-credentials: false}`만 허용한다. GitHub-hosted preflight만 pinned `setup-python`의 `{python-version: '3.11.15'}`를 사용한다. Self-hosted Deploy Main에서 `setup-python`, package install, checkout repository/ref/path override, 추가 input, 누락된 `persist-credentials: false` 또는 action 순서 변경은 모두 finding이다.

Self-hosted workflow/job에는 `defaults.run`을 두지 않고 job `container`와 `services`를 금지한다. `working-directory`를 지정할 수 없고, Deploy Main run step의 shell은 exact `pwsh`다. workflow/job `env`는 `PATH`, `PYTHONPATH`, `PYTHONHOME`, `PIP_*`, `PSModulePath`, `LD_*`, `NODE_*`, `WEATHER_DEPLOY_TARGET_PATH`처럼 executable/import/target을 바꾸는 이름을 정의할 수 없다. Deploy Main에는 package-install run step이 없어야 하고 verify/deploy CLI step에는 아래 mapping만 정확히 허용한다.

```yaml
env:
  GH_TOKEN: ${{ secrets.WEATHER_GOVERNANCE_READ_TOKEN }}
  GOVERNANCE_MODE: ${{ vars.WEATHER_GOVERNANCE_MODE }}
  DEPLOYMENT_ENABLED: ${{ vars.WEATHER_DEPLOYMENT_ENABLED }}
```

```text
protected push CI:
  python -m tools.dagbag_check --repo-root .

GitHub-hosted preflight:
  python -m deployment.main_cli verify-main --event-path "$env:GITHUB_EVENT_PATH" --workflow-ref "$env:GITHUB_WORKFLOW_REF" --workflow-sha "$env:GITHUB_WORKFLOW_SHA"

self-hosted deploy:
  python -m deployment.main_cli deploy-main --event-path "$env:GITHUB_EVENT_PATH" --workflow-ref "$env:GITHUB_WORKFLOW_REF" --workflow-sha "$env:GITHUB_WORKFLOW_SHA"
```

Hosted `verify-main`은 stdlib-only import path를 유지한다. Self-hosted runner의 Python `3.11`과 PyYAML은 최초 cutover 승인 뒤 workflow 밖에서 사전 준비하고 sanitized version/capability proof를 남긴다. Workflow는 package install로 drift를 교정하지 않는다.

The scanner also requires CODEOWNERS entries for `.github/workflows/**`, `tools/**`, `deployment/**`, `runtime/**`, `provenance/**`, `docs/operations/**`, and `release/**`.

- [ ] **Step 4: GREEN 및 승인 후 commit**

Run: `python -m pytest tests/repository/test_workflow_policy.py -q`

```powershell
git add pyproject.toml tools/workflow_policy.py tests/repository/test_workflow_policy.py
git commit -m "test(ci): workflow와 runner 보안 정책 고정"
```

---

### Task 4: CI workflow와 CODEOWNERS

**Files:**
- Create: `.github/CODEOWNERS`
- Create: `.github/workflows/ci.yml`
- Modify: `tools/verify_repository.ps1`
- Modify: `tests/repository/test_runtime_safety.py`
- Test: `tests/repository/test_workflow_policy.py`

**Interfaces:**
- Produces check names: `Repository Contract`, `dbt-weather`, `airflow-tests`, `dagbag-policy`, `dagbag-runtime`, `Promotion Source / required`, `governance-mode`, `CI / required`
- Consumes Task 1–3 CLIs and `tools/dagbag_check.py`.

- [ ] **Step 1: CODEOWNERS 작성**

```text
.github/workflows/** @masondev1024
tools/** @masondev1024
deployment/** @masondev1024
runtime/** @masondev1024
provenance/** @masondev1024
docs/operations/** @masondev1024
release/** @masondev1024
```

- [ ] **Step 2: workflow trigger/permission skeleton 작성**

```yaml
name: CI
on:
  pull_request:
    branches: [dev, main]
  push:
    branches: [dev, main]
permissions:
  contents: read
  pull-requests: read
concurrency:
  group: ci-${{ github.event.pull_request.number || github.sha }}
  cancel-in-progress: true
```

- [ ] **Step 3: GitHub-hosted jobs 추가**

Use `ubuntu-latest`, pinned checkout/setup-python, Python `3.11.15`. Exact commands:

```text
Repository Contract:
  python -m pip install jsonschema==4.26.0 PyYAML==6.0.2 pytest==9.0.3
  pwsh -File tools/verify_repository.ps1

dbt-weather:
  python -m pip install -r runtime/requirements-dbt.lock.txt
  dbt deps --project-dir dbt/domains/traffic_weather --profiles-dir dbt/domains/traffic_weather
  dbt parse --project-dir dbt/domains/traffic_weather --profiles-dir dbt/domains/traffic_weather --target ci --no-partial-parse
  python dbt/serving_contract/validate_serving_contract.py --source dbt/domains/traffic_weather/models/weather/transform/gold/gold_weather_place_current_outlook.yml dbt/domains/traffic_weather/models/weather/transform/gold/gold_weather_place_precipitation_window.yml dbt/domains/traffic_weather/models/weather/transform/gold/gold_weather_place_risk_window.yml dbt/domains/traffic_weather/models/weather/transform/gold/gold_weather_place_forecast_change_daily.yml --manifest dbt/domains/traffic_weather/target/manifest.json --format text
  python -m pytest dbt/serving_contract/tests dbt/domains/traffic_weather/tests/weather tests/contracts -q

airflow-tests:
  python -m compileall dags tools release
  python -m pytest dags/common/serving/tests dags/domains/weather/tests tests/repository/test_airflow_boundary.py -q

dagbag-policy:
  python -m pytest tests/repository/test_dagbag_harness.py tests/repository/test_scaffold_contract.py -q
```

Install Airflow test dependencies on Ubuntu from `runtime/requirements-airflow.lock.txt`; never run a scheduler or metadata DB service.

- [ ] **Step 4: protected-push 전용 DagBag runtime job 추가**

```yaml
name: dagbag-runtime
if: >
  vars.WEATHER_GOVERNANCE_MODE == 'protected' &&
  github.event_name == 'push' &&
  (github.ref == 'refs/heads/dev' || github.ref == 'refs/heads/main')
runs-on: [self-hosted, windows, weather-prod]
steps:
  - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262
    with: {persist-credentials: false}
  - run: python -m tools.dagbag_check --repo-root .
```

This runs only the pinned digest container with `--network none`, read-only mounts and tmpfs as already built by `tools/dagbag_check.py`.

- [ ] **Step 5: promotion/governance/required jobs 추가**

`Promotion Source / required` calls Task 2. A `dev → main` PR additionally requires a successful `dagbag-runtime` check on the exact protected `dev` head SHA; an ordinary main push fetches associated PRs using read-only `gh api`. Only a `created == true` main push takes the separate bootstrap CLI, which revalidates raw event identity and sanitized read-only repository/branch state before allowing the one-time creation. `governance-mode` accepts only repository variable `WEATHER_GOVERNANCE_MODE` equal to `protected` or, while private, `guarded_private`; any missing value fails. `CI / required` uses `if: always()` and passes every `needs.*.result`, event, ref and governance mode to Task 1. PR runtime is always skipped; protected branch push runtime must succeed; guarded push is explicitly degraded and cannot enable `Deploy Main`.

- [ ] **Step 6: repository verifier에 workflow policy 추가**

Add before pytest:

```powershell
Invoke-SecretlessPythonCheck @("-m", "tools.workflow_policy", "--repo-root", $resolvedRepo)
```

Update `test_repository_verifier_runs_only_secretless_python_checks` expected calls and retain the assertion that no Docker/Airflow executable is called.

- [ ] **Step 7: GREEN 확인**

```powershell
python -m pytest tests/repository/test_ci_required_gate.py tests/repository/test_promotion_source.py tests/repository/test_workflow_policy.py tests/repository/test_runtime_safety.py -q
powershell -NoProfile -ExecutionPolicy Bypass -File tools\verify_repository.ps1
```

Expected: PASS; no Docker/Airflow state-changing command.

- [ ] **Step 8: 승인 후 commit**

```powershell
git add .github/CODEOWNERS .github/workflows/ci.yml tools/verify_repository.ps1 tests/repository/test_runtime_safety.py
git commit -m "ci(weather): fork-safe 필수 게이트 구성"
```

---

### Task 5: bootstrap·native protection 적용/readback과 운영 문서

**Files:**
- Create: `tools/github_governance.py`
- Create: `tools/github_protection.py`
- Create: `docs/operations/github-bootstrap.md`
- Test: `tests/repository/test_github_governance.py`
- Test: `tests/repository/test_github_protection.py`

**Interfaces:**
- Produces: `classify(repo: Mapping[str, object], protections: Mapping[str, Mapping[str, object] | None]) -> Literal["protected", "guarded_private", "invalid"]`
- Produces: `protection_payload(branch: Literal["dev", "main"], check_app_ids: Mapping[str, object]) -> dict[str, object]`
- CLIs: `python -m tools.github_protection plan|apply|verify`. `apply` is the only remote mutation entrypoint and shells out only to authenticated `gh api` argv without a token argument.

- [ ] **Step 1: RED fixtures/tests 작성**

Cover initial `defaultBranchRef.name == "dev"`, post-bootstrap `main`, private protection 403, valid protection on both branches, missing/extra required checks, `enforce_admins=false`, absent PR rule, non-empty bypass actor, force-push/deletion allowed, conversation resolution disabled and SHA mismatch. For app discovery, cover missing/duplicate/malformed/truncated branch workflow runs, wrong CI name/path/event/branch/SHA/status/conclusion, jobs from a different run, missing/duplicate/failed required jobs, foreign or reused `check_run_url`, linked check-run identity/status/app mismatch, non-positive app ID and cross-branch conflicting source. `guarded_private` is permitted only as a diagnosis when visibility is private; it must never set a deployment-enabled result.

Assert exact payloads:

```json
{
  "required_status_checks": {"strict": true, "checks": [{"context": "CI / required", "app_id": "<discovered-positive-id>"}]},
  "enforce_admins": true,
  "required_pull_request_reviews": {
    "dismiss_stale_reviews": true,
    "require_code_owner_reviews": false,
    "required_approving_review_count": 0,
    "require_last_push_approval": false,
    "bypass_pull_request_allowances": {"users": [], "teams": [], "apps": []}
  },
  "restrictions": null,
  "required_linear_history": true,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "block_creations": false,
  "required_conversation_resolution": true,
  "lock_branch": false,
  "allow_fork_syncing": false
}
```

For `main`, `required_status_checks.checks` contains both `CI / required` and `Promotion Source / required` in sorted order, each bound to the dynamically discovered positive GitHub Actions `app_id`. No numeric App ID is hard-coded. If GitHub omits empty personal-repository bypass fields in the response, verification accepts only absent or all-empty fields; any actor is a failure. CODEOWNERS coverage remains mandatory in Task 3, while code-owner approval stays disabled for a solo maintainer.

- [ ] **Step 2: RED/GREEN**

Run: `python -m pytest tests/repository/test_github_governance.py tests/repository/test_github_protection.py -q`

Implement pure classification/payload/readback validation and injected `GhRunner`, then rerun expecting PASS. Unit tests must fail if a real subprocess or network call is attempted.

- [ ] **Step 3: bootstrap document 작성**

Document this exact remote sequence, each mutation gated by fresh user approval:

```powershell
gh auth status -h github.com
gh repo view masondev1024/seoul-weather-platform --json defaultBranchRef,visibility
gh variable set WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform --body guarded_private
gh variable get WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform
# STOP: exact guarded_private readback 뒤 최초 CI PR을 열거나 재실행하기 전에 secretless CI만 활성화한다.
git fetch origin dev
$bootstrapSha = git rev-parse origin/dev
gh variable get WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform
git push origin "${bootstrapSha}:refs/heads/main"
# STOP: default가 dev인 동안 exact initial-main-bootstrap source와 main CI success를 확인한다.
gh repo edit masondev1024/seoul-weather-platform --default-branch main
python -m tools.github_protection plan --repo masondev1024/seoul-weather-platform --bootstrap-sha $bootstrapSha --output "$env:TEMP\weather-protection-plan.json"
python -m tools.github_protection apply --repo masondev1024/seoul-weather-platform --plan "$env:TEMP\weather-protection-plan.json" --confirm-bootstrap-sha $bootstrapSha
python -m tools.github_protection verify --repo masondev1024/seoul-weather-platform --expected-default main --expected-dev-check "CI / required" --expected-main-check "CI / required" --expected-main-check "Promotion Source / required"
gh variable set WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform --body protected
```

Set and exact-read `guarded_private` before the first CI PR; it permits only GitHub-hosted secretless diagnostics, never production runner or `Deploy Main`. Before the main-creation push, read it again and record `bootstrap_sha = git rev-parse origin/dev`; after it, require both `git ls-remote origin refs/heads/main` and branch API SHA to match. Keep the default branch at `dev` until the exact initial bootstrap CI succeeds, then change it to `main`. Before `apply`, query each branch's `/actions/workflows/ci.yml/runs` with exact branch/head SHA/push/success filters, require one complete matching `CI` run and follow its `/actions/runs/<id>/jobs` response to the exact required jobs and repository-owned `check_run_url`; validate the absolute URL then pass only its exact repository API path to `gh api`. Linked check-runs must match name/SHA/success and expose `app.slug == github-actions` with positive `app.id`. All paginated lists require exact `total_count == returned length <= 100`; duplicates, malformed values and cross-branch app conflicts fail closed. Bind discovered IDs into the plan payload and checksum. `apply` re-discovers them, issues only `PUT /repos/masondev1024/seoul-weather-platform/branches/dev/protection` and the corresponding `main` endpoint, then immediately GETs both resources and compares normalized fields including exact `app_id`. `verify` repeats discovery against each current branch head and rejects context-only protection. It never enables code-owner review, bypass actor, force push or deletion.

If either PUT returns 403/404 or readback differs, keep/set `guarded_private`, delete the temp plan, and stop. Do not register/start a production runner, install the governance read secret, or enable `Deploy Main`. Only a successful readback permits the final `protected` variable write. Runner registration and `WEATHER_DEPLOYMENT_ENABLED=enabled` are later, separately approved first-cutover steps.

- [ ] **Step 4: 승인 후 commit**

```powershell
git add tools/github_governance.py tools/github_protection.py docs/operations/github-bootstrap.md tests/repository/test_github_governance.py tests/repository/test_github_protection.py
git commit -m "docs(ci): main bootstrap와 governance 판정 고정"
```

---

### Task 6: provenance와 전체 검증

**Files:**
- Modify: `provenance/source-files.jsonl`

- [ ] **Step 1: provenance 갱신**

Run: `python -m tools.refresh_provenance --repo-root . --manifest provenance/source-files.jsonl`

- [ ] **Step 2: 전체 L0 검증**

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools\verify_repository.ps1
python -m pytest tests/repository tests/contracts release/weather/tests dbt/serving_contract/tests -q
```

Expected: all pass. `git status --short` must contain only this plan의 owned paths and `provenance/source-files.jsonl`.

- [ ] **Step 3: 승인 후 provenance commit**

```powershell
git add provenance/source-files.jsonl
git commit -m "chore(provenance): CI bootstrap 산출물 등록"
```

## Stop Gate

- 이 계획 완료는 local CI 구현 완료이지 remote bootstrap 승인이나 Airflow 배포 승인이 아니다.
- `main` 생성, default-branch 변경, repository variable 설정, branch protection 변경, push, PR 생성은 사용자에게 실제 대상 SHA와 변경을 보고한 뒤 별도 승인을 받아 실행한다.
- `guarded_private`이면 production runner 또는 `Deploy Main` 활성화로 넘어갈 수 없다. protected readback이 성공해도 runner 설치·Airflow 상태 변경은 최초 cutover의 별도 승인 전까지 수행하지 않는다.
