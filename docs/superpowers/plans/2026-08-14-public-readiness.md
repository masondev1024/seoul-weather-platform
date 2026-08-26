# Public-readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 저장소 공개 가능 여부를 license·history secret·Release asset·runtime reproducibility·fork runner 보안 증거로 판정하고, 현재 권리 blocker를 정확히 실패로 보고하는 read-only gate를 구축한다.

**Architecture:** 각 gate는 pure scanner와 redacted finding을 반환하고 하나의 report builder가 `weather-public-readiness/v1`로 집계한다. GitHub workflow는 수동 실행·GitHub-hosted·read-only이며 보고서 artifact만 올린다. visibility 변경 기능은 저장소 코드에 만들지 않고 모든 blocker 해소 뒤 just-in-time 사용자 승인으로 별도 수행한다.

**Tech Stack:** Python 3.11.15, pytest 9.0.3, Git object scanning, GitHub Actions, OCI image digest lock, Markdown public docs.

## Global Constraints

- 현재 `ASAC-DAG`, `ASAC-DBT`, `ASK-Seoul-Serving` source는 `internal_private_snapshot_only`이므로 readiness의 올바른 결과는 `blocked`다.
- 권리 문서 확보 또는 clean-room 교체 전 `LICENSE`를 임의 선택하거나 visibility를 public으로 바꾸지 않는다.
- secret finding은 값·원문·raw line을 출력하지 않는다.
- `public-readiness.yml`은 `workflow_dispatch`만 사용하고 GitHub-hosted에서 실행한다.
- Airflow/Docker/dbt/Trino/D1/R2 mutation, self-hosted runner, `pull_request_target`, visibility API write를 금지한다.
- public-readiness PASS도 visibility 변경 승인이 아니다. 별도 사용자 승인이 필요하다.
- stage·commit·push·PR은 실행 시점의 사용자 승인 뒤에만 수행한다.

## 2026-08-21 Mac cutover architecture decision

- Architecture source: `docs/architecture/public-private-operations-boundary.md`.
- Current evidence/status: `docs/operations/public-release-readiness.md`.
- The repository is the public-candidate **code plane**. Populated env files,
  personal Cloudflare/KMA credentials, Docker volumes, Airflow metadata/logs, and
  deployment approval evidence are the private **Mac operations plane**.
- The personal Mac must never be registered as a runner for a public repository.
  Fork and pull-request checks remain GitHub-hosted, read-only, and secretless.
- Before visibility changes, the legacy self-hosted `dagbag-runtime` and
  `deploy-main` route must be disabled or moved to a separately private operations
  repository and verified by GitHub readback.
- The current implementation phase adds only architecture, a secretless
  Weather-only example, and fail-closed evidence. Tasks below remain the future
  scanner/governance implementation plan and do not authorize publication.

---

### Task 1: readiness report model과 현재 license blocker

**Files:**
- Create: `tools/public_readiness.py`
- Create: `tests/public_readiness/test_public_readiness_report.py`
- Create: `tests/public_readiness/test_license_gate.py`

**Interfaces:**
- Produces: `GateResult(name: str, status: Literal["passed", "blocked"], blockers: Sequence[Finding], notices: Sequence[Finding])`
- Produces: `evaluate_license_gate(source_refs: Mapping[str, object], source_records: Sequence[Mapping[str, object]]) -> GateResult`
- Produces: `build_report(repo_root: Path, checked_at: datetime, adapters: ReadinessAdapters) -> dict[str, object]`
- CLI: `python -m tools.public_readiness --repo-root . --output public-readiness-report.json`

- [ ] **Step 1: RED 테스트 작성**

Write these exact tests: `test_internal_private_snapshot_only_is_a_blocker`, `test_k_skill_mit_agpl_boundary_is_notice_not_automatic_blocker`, `test_report_status_is_blocked_when_any_gate_blocks`, and `test_finding_contains_source_id_commit_and_status_but_no_source_content`.

- [ ] **Step 2: RED 확인**

Run: `python -m pytest tests/public_readiness/test_license_gate.py tests/public_readiness/test_public_readiness_report.py -q`

Expected: missing module.

- [ ] **Step 3: license/report 최소 구현**

Current source refs must produce exactly three redistribution blockers. Schema:

```json
{
  "schema_version": "weather-public-readiness/v1",
  "status": "blocked",
  "checked_at": "2026-08-14T00:00:00Z",
  "gates": {},
  "blockers": [],
  "notices": []
}
```

CLI always writes the report atomically; returns `0` only for passed and `1` for blocked, `2` for malformed input.

- [ ] **Step 4: GREEN 및 승인 후 commit**

```powershell
python -m pytest tests/public_readiness/test_license_gate.py tests/public_readiness/test_public_readiness_report.py -q
git add tools/public_readiness.py tests/public_readiness/test_license_gate.py tests/public_readiness/test_public_readiness_report.py
git commit -m "feat(public): 권리 blocker를 fail-closed로 보고"
```

---

### Task 2: 전체 Git history secret scan

**Files:**
- Create: `tools/history_secret_scan.py`
- Test: `tests/public_readiness/test_history_secret_scan.py`

**Interfaces:**
- Produces: `scan_history(repo_root: Path, git: GitObjectReader, rules=SECRET_RULES) -> GateResult`
- `GitObjectReader` enumerates `git rev-list --objects --all` and reads blob bytes via `git cat-file --batch`; it never invokes `git grep` that prints matching lines.

- [ ] **Step 1: temporary repository RED 테스트 작성**

Create a temp Git repo with a credential-like value in commit 1, remove it in commit 2, then assert the current tree is clean but history gate blocks. Assert finding exposes only object id, normalized path, rule and fixed summary; the secret substring is absent from stdout, repr and report.

- [ ] **Step 2: RED/GREEN**

Run: `python -m pytest tests/public_readiness/test_history_secret_scan.py -q`

Reuse `SECRET_RULES` from `tools.repository_policy`; treat binary/non-UTF-8 blobs as skipped with a notice unless their bytes match a byte-safe private-key/API-key signature. Add blob size limit and report a blocker when an unscannable oversized text candidate exists.

- [ ] **Step 3: 승인 후 commit**

```powershell
git add tools/history_secret_scan.py tests/public_readiness/test_history_secret_scan.py
git commit -m "feat(public): Git 이력 secret을 redacted scan"
```

---

### Task 3: Release body/asset disclosure scan

**Files:**
- Create: `tools/release_asset_scan.py`
- Create: `tests/fixtures/public-readiness/releases.json`
- Test: `tests/public_readiness/test_release_asset_scan.py`

**Interfaces:**
- Produces: `scan_release_text(release_id: str, body: str, assets: Sequence[ReleaseAsset]) -> GateResult`
- Produces: `GitHubReleaseReader` protocol; real adapter is GET/download only, fake adapter reads fixtures.

- [ ] **Step 1: RED fixtures/tests 작성**

Block Windows/macOS/Linux home paths, private/local IP, `.env`, ASK/KMA/Cloudflare credential patterns, bearer/private keys, and invalid `deployment-plan.json` checksum. Verify a sanitized plan/body passes and finding never contains matched text.

- [ ] **Step 2: RED/GREEN**

Run: `python -m pytest tests/public_readiness/test_release_asset_scan.py -q`

Downloaded assets are scanned in a temp directory and deleted after use. Unknown binary assets or failed downloads block readiness; no asset is executed.

- [ ] **Step 3: 승인 후 commit**

```powershell
git add tools/release_asset_scan.py tests/fixtures/public-readiness/releases.json tests/public_readiness/test_release_asset_scan.py
git commit -m "feat(public): 과거 Release 공개정보 누출 검사"
```

---

### Task 4: workflow/fork/default-branch public security gate

**Files:**
- Modify: `tools/workflow_policy.py`
- Create: `tests/public_readiness/test_workflow_policy.py`

**Interfaces:**
- Consumes plan 1 workflow audit.
- Produces gate requiring default branch `main`, native protection mode, no `pull_request_target`, full action SHA pins, no PR self-hosted execution, deploy workflow identity guard and CODEOWNERS coverage.

- [ ] **Step 1: RED 테스트 작성**

Fixtures cover fork PR, same-repository PR, guarded private/public/internal visibility, protected branch push, exact `CI` workflow_run success/failure/wrong source name/suffix-free path `.github/workflows/ci.yml`/separate head branch/event/status/SHA, disabled deployment flag, repository_dispatch, Release/manual events, unpinned action and visibility write command. guarded_private is permitted only for a private single-owner repository with exact merged-PR revalidation; public/internal visibility or an extra writer stops guarded deployment and public readiness. `WEATHER_DEPLOYMENT_ENABLED != enabled` also blocks the production runner.

- [ ] **Step 2: RED/GREEN**

Run: `python -m pytest tests/public_readiness/test_workflow_policy.py -q`

The scanner rejects `gh repo edit`, `PATCH /repos/masondev1024/seoul-weather-platform`, `visibility`, `self-hosted` in public-readiness workflow and any deployment route outside `.github/workflows/deploy-main.yml`. Event-aware rules require: no self-hosted PR/workflow_dispatch/repository_dispatch/Release; CI runtime only on `protected` `dev|main` push; Deploy Main only on exact `CI`/`completed`/`main` workflow_run with an allowed governance mode plus `enabled`, source name `CI`, suffix-free path `.github/workflows/ci.yml`, separate `head_branch=main`, event/status/conclusion and workflow/head SHA equality. guarded_private must additionally be private, sole-owner, and exact merged-PR revalidated; public/internal or extra writer fails closed. GitHub-hosted `verify-main` must succeed before the self-hosted `deploy-main`; both use pinned checkout at `${{ github.workflow_sha }}`, only hosted verify uses pinned `setup-python`, and neither runs a package install. Self-hosted deploy calls only the exact `deployment.main_cli` entrypoint against its approval-time pre-provisioned Python `3.11`/PyYAML environment. Protected CLI steps receive the repo-scoped read-only credential through `GH_TOKEN` with `Administration`, `Actions`, `Checks`, `Contents`, and `Pull requests` read plus exact `GOVERNANCE_MODE` and `DEPLOYMENT_ENABLED`; guarded steps use the workflow read token and install no governance secret. Missing or mismatched gate input fails closed.

- [ ] **Step 3: 승인 후 commit**

```powershell
git add tools/workflow_policy.py tests/public_readiness/test_workflow_policy.py
git commit -m "test(public): fork runner와 visibility 보안 gate 고정"
```

---

### Task 5: reproducible public Airflow runtime gate

**Files:**
- Create: `runtime/public-runtime.schema.json`
- Create: `runtime/public-runtime.lock.json`
- Create: `runtime/airflow/Dockerfile`
- Create: `tools/public_runtime.py`
- Test: `tests/public_readiness/test_reproducible_airflow_runtime.py`

**Interfaces:**
- Produces: `validate_public_runtime(repo_root: Path) -> GateResult`
- Runtime lock records public base image repository, immutable digest, Airflow/Python versions and requirements checksums.

- [ ] **Step 1: current-state RED 테스트 작성**

Require Dockerfile, public image repository, a base ref matching `@sha256:[0-9a-f]{64}`, Airflow `3.2.2`, Python `3.11`, `runtime/requirements-airflow.lock.txt` checksum, no `latest`, no `COPY .env|credentials|secrets`, and a secretless DagBag build recipe.

- [ ] **Step 2: resolve public base digest read-only**

Run only after Docker registry read approval:

```powershell
$manifest = docker buildx imagetools inspect apache/airflow:3.2.2-python3.11 --format '{{json .Manifest}}' | ConvertFrom-Json
python -m tools.public_runtime write-lock --repository apache/airflow --tag 3.2.2-python3.11 --digest $manifest.digest
```

The command validates the immutable digest and writes `runtime/public-runtime.lock.json`; no image build or container start occurs.

- [ ] **Step 3: Dockerfile/validator 구현과 GREEN**

The Dockerfile `FROM` value is formed from the exact repository and digest in the committed lock; dependencies install from `runtime/requirements-airflow.lock.txt` with no credential ARG/ENV. Run:

`python -m pytest tests/public_readiness/test_reproducible_airflow_runtime.py -q`

- [ ] **Step 4: 승인 후 commit**

```powershell
git add runtime/public-runtime.schema.json runtime/public-runtime.lock.json runtime/airflow/Dockerfile tools/public_runtime.py tests/public_readiness/test_reproducible_airflow_runtime.py
git commit -m "build(public): 재현 가능한 Airflow runtime 계약 추가"
```

---

### Task 6: public documentation gate without inventing a license

**Files:**
- Create: `SECURITY.md`
- Create: `CONTRIBUTING.md`
- Create: `docs/legal/publication-blockers.md`
- Test: `tests/public_readiness/test_public_docs.py`

**Interfaces:**
- Produces gate requiring `README.md`, `LICENSE`, `SECURITY.md`, `CONTRIBUTING.md`; current expected blocker is missing authorized `LICENSE` plus three private snapshot rights.

- [ ] **Step 1: RED 테스트 작성**

Verify SECURITY has a private vulnerability reporting route without an embedded email/token, CONTRIBUTING targets feature PRs to `dev`, reserves `dev → main` for promotion, states fork code never runs self-hosted, and forbids credentials. Verify `publication-blockers.md` names the three source IDs/statuses but copies no code.

- [ ] **Step 2: docs 작성과 expected-blocked GREEN**

Run: `python -m pytest tests/public_readiness/test_public_docs.py -q`

The test must pass by asserting the gate reports `missing_authorized_license`; do not create `LICENSE` until redistribution rights or clean-room completion establishes an actual license choice.

- [ ] **Step 3: 승인 후 commit**

```powershell
git add SECURITY.md CONTRIBUTING.md docs/legal/publication-blockers.md tests/public_readiness/test_public_docs.py
git commit -m "docs(public): 공개 기여 보안과 권리 blocker 문서화"
```

---

### Task 7: read-only public-readiness workflow

**Files:**
- Create: `.github/workflows/public-readiness.yml`
- Test: `tests/public_readiness/test_public_readiness_workflow.py`
- Modify: `tools/public_readiness.py`

**Interfaces:**
- Produces `public-readiness-report.json` artifact and a final pass/fail exit.

- [ ] **Step 1: workflow RED 테스트 작성**

Require only `workflow_dispatch`, `permissions.contents: read`, `ubuntu-latest`, no self-hosted/Docker/Airflow/dbt/visibility write, pinned checkout/setup/upload actions, report upload even when blocked, and a final step preserving the scanner exit code.

- [ ] **Step 2: minimal workflow 작성**

```yaml
name: Public Readiness
on:
  workflow_dispatch:
permissions:
  contents: read
jobs:
  report:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262
        with: {fetch-depth: 0, persist-credentials: false}
      - uses: actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065
        with: {python-version: '3.11.15'}
      - run: python -m pip install jsonschema==4.26.0 PyYAML==6.0.2 pytest==9.0.3
      - id: readiness
        continue-on-error: true
        run: python -m tools.public_readiness --repo-root . --output public-readiness-report.json
      - if: always()
        uses: actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02
        with: {name: public-readiness-report, path: public-readiness-report.json}
      - if: steps.readiness.outcome != 'success'
        run: exit 1
```

- [ ] **Step 3: GREEN 및 승인 후 commit**

```powershell
python -m pytest tests/public_readiness/test_public_readiness_workflow.py -q
git add .github/workflows/public-readiness.yml tests/public_readiness/test_public_readiness_workflow.py tools/public_readiness.py
git commit -m "ci(public): read-only 공개 준비 보고서 workflow 추가"
```

---

### Task 8: provenance·full verification·expected blocker report

**Files:**
- Create: `docs/operations/public-release-readiness.md`
- Test: `tests/public_readiness/test_visibility_approval.py`
- Modify: `provenance/source-files.jsonl`

- [ ] **Step 1: 별도 visibility 승인 문서/RED 테스트 작성**

The document states readiness PASS is not approval, past Releases become public, visibility change needs a fresh user report/approval, and no repository script performs the change. Static test rejects `gh repo edit --visibility public` and GitHub API visibility writes anywhere except quoted forbidden examples in tests/docs.

- [ ] **Step 2: 전체 검증**

```powershell
python -m pytest tests/public_readiness -q
python -m tools.refresh_provenance --repo-root . --manifest provenance/source-files.jsonl
powershell -NoProfile -ExecutionPolicy Bypass -File tools\verify_repository.ps1
python -m tools.public_readiness --repo-root . --output public-readiness-report.json
```

Expected: tests and repository verification PASS; final CLI returns `1` with exactly the unresolved license/source blockers and a redacted report.

- [ ] **Step 3: 승인 후 commit**

```powershell
git add docs/operations/public-release-readiness.md tests/public_readiness/test_visibility_approval.py provenance/source-files.jsonl
git commit -m "chore(public): 공개 준비 gate와 승인 경계 등록"
```

## Stop Gate

- 이 계획의 성공 결과는 “공개 gate가 정확히 blocked를 보고한다”이다. 현재 저장소를 public으로 바꾸지 않는다.
- 다음 단계는 세 private source의 재배포 권리 확보 또는 clean-room 교체를 별도 설계·검증하는 일이다.
- 모든 blocker가 해소되고 authorized `LICENSE`가 추가된 뒤에도 사용자에게 history/Release/runtime/branch protection 최종 증거를 보고하고 별도 승인을 받아야 visibility를 변경할 수 있다.
