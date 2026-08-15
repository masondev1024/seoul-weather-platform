# `guarded_private` Weather 자동 배포 설계

## 1. 결정

현재 `masondev1024/seoul-weather-platform`은 private 개인 저장소이고, private branch protection을 사용할 수 없는 상태다. 이 조건에서는 `WEATHER_GOVERNANCE_MODE=guarded_private`에서도 검증된 `dev → main` 병합 commit을 자동 배포할 수 있게 한다.

이 fallback의 목적은 개인 저장소 운영 중 다음 실수를 차단하는 것이다.

- PR 없이 `main`에 직접 push한 commit 배포
- 다른 branch 또는 fork에서 온 PR 배포
- 실패·취소·과거 SHA의 CI 결과 재사용
- 검증된 SHA와 다른 checkout 배포
- 허용되지 않은 Compose service 또는 Airflow DAG 변경
- writer가 실행 중인 상태에서 DAG·dbt mount 교체

이 설계는 native protection과 같은 강한 보안 경계를 주장하지 않는다. 저장소 write 권한과 GitHub 계정이 탈취되거나, 권한을 가진 사용자가 workflow·검증 코드를 함께 악의적으로 변경하는 상황은 보호 범위 밖이다. 현재 private 저장소의 write 권한을 신뢰 경계로 두고, 오작동과 우발적인 직접 배포를 막는 운영 안전장치로 사용한다.

## 2. 기존 설계와의 관계

이 문서는 `2026-08-15-main-merge-auto-deploy-design.md`의 다음 조항만 대체한다.

- `guarded_private`에서는 배포 job을 항상 차단한다는 조건
- identity 검증에 dev/main branch protection readback이 항상 필요하다는 조건
- 모든 배포 CLI가 `WEATHER_GOVERNANCE_READ_TOKEN`만 사용한다는 조건

다음 계약은 그대로 유지한다.

- `workflow_run`의 exact `CI`/`completed`/`main` trigger
- GitHub-hosted preflight 성공 후에만 self-hosted job 실행
- exact main SHA checkout과 detached runtime checkout
- Airflow DAG 열 개의 pause snapshot·bounded drain·restore
- Airflow 코드 service 네 개만 `--no-deps`로 갱신
- `docker compose down`, `restart`, `--force-recreate`, data service mutation 금지
- candidate health 실패 시 이전 overlay 또는 baseline rollback
- 최초 cutover 전 사용자 STOP 보고와 별도 승인

native protection을 사용할 수 있게 되면 `WEATHER_GOVERNANCE_MODE=protected`로 전환하고 기존 protection readback 경로를 그대로 사용한다.

## 3. 검토한 대안

### A. native branch protection만 허용

가장 강한 기존 설계다. GitHub가 `dev`·`main`의 PR·required check·force push 규칙을 mutation 전에 강제한다. 현재 private 저장소에서는 사용할 수 없어 자동 배포를 시작할 수 없다.

### B. private 저장소의 증거 기반 fallback

이번에 채택한다. GitHub-hosted preflight와 self-hosted CLI가 exact SHA의 CI, GitHub Actions check, 병합 PR, remote main을 다시 검증한다. 별도 유료 기능 없이 현재 저장소에서 구현할 수 있고 기존 배포 상태 머신을 재사용한다.

한계는 same-repo workflow와 검증 코드도 보호되지 않은 `main`에 있다는 점이다. 따라서 악의적인 writer 또는 탈취된 write credential에 대한 방어가 아니라 sole-owner 운영의 실수 방지 모델이다.

### C. 저장소 밖 trusted deploy controller

branch protection 없이도 더 강한 경계를 만들려면 self-hosted runner가 저장소 workflow를 실행하지 않게 하고, 로컬 고정 service가 GitHub를 read-only polling한 뒤 검증된 SHA만 배포해야 한다. 보안성은 높지만 별도 설치·업데이트·감사·복구 체계가 필요해 현재 빠른 전환 범위에서는 제외한다. 다중 writer가 생기거나 공격자 모델을 확대할 때 이 방식 또는 native protection으로 전환한다.

## 4. 신뢰 경계와 활성화 조건

`guarded_private` 자동 배포는 다음 조건을 모두 만족할 때만 허용한다.

1. repository가 정확히 `private == true`, `visibility == private`, `default_branch == main`이다.
2. `WEATHER_GOVERNANCE_MODE == guarded_private`이고 `WEATHER_DEPLOYMENT_ENABLED == enabled`이다.
3. `Deploy Main`은 default branch의 `.github/workflows/deploy-main.yml`에서 실행된다.
4. source는 `.github/workflows/ci.yml`의 `push` run이며 branch가 `main`, status/conclusion이 `completed/success`다.
5. `github.workflow_sha`, source run `head_sha`, remote `main` HEAD가 모두 같은 lowercase 40자리 SHA다.
6. source run의 exact `CI / required`와 `Promotion Source / required` job이 같은 SHA에서 성공했고, 각 linked check-run이 `github-actions` app 소유다.
7. 해당 SHA에 연결된 병합 PR을 GitHub API로 다시 읽어 exact same-repository `dev → main` merge임을 검증한다.
8. self-hosted job은 GitHub-hosted `verify-main` 성공 뒤에만 실행되고 같은 identity 검증을 반복한다.

`guarded_private`에서는 dev/main protection endpoint를 호출하거나 protection 부재를 성공으로 위장하지 않는다. 검증 결과에는 `governance_mode=guarded_private`와 `protections=null`을 명시해 `protected` 증거와 구분한다.

## 5. 병합 PR 증거

현재 `Promotion Source / required` check만 신뢰하면 최초 `main` 생성용 `initial-main-bootstrap` 성공 run을 일반 승격과 구분할 수 없다. 따라서 배포 identity는 commit-associated PR을 독립적으로 다시 검증한다.

GitHub REST `GET /repos/{owner}/{repo}/commits/{sha}/pulls` 응답에서 다음 조건을 모두 만족하는 PR이 정확히 하나여야 한다.

- `merged_at`이 존재한다.
- `base.ref == main`
- `head.ref == dev`
- `base.repo.full_name == repository`
- `head.repo.full_name == repository`
- `merge_commit_sha == candidate_sha`

0개, 2개 이상, fork, feature branch, open/closed-but-unmerged PR, 다른 merge SHA는 모두 차단한다. 이 조회에는 `pull-requests: read` permission을 명시한다. GitHub 문서상 commit-associated PR endpoint는 Pull requests read 권한을 요구한다.

이 검증으로 최초 bootstrap SHA, 로컬 merge push, commit message만 위조한 push는 배포 후보가 되지 않는다.

## 6. workflow와 credential 분리

`deploy-main.yml`은 두 governance mode를 명시적으로 분리한다.

- `guarded_private`: read-only workflow permission의 `${{ github.token }}` 사용
- `protected`: repository-scoped `WEATHER_GOVERNANCE_READ_TOKEN` 사용

두 token을 `A || B` 형태의 단일 expression으로 fallback하지 않는다. hosted verify와 self-hosted deploy에 mode별 conditional step을 두고, 각 step의 token source와 `GOVERNANCE_MODE`를 workflow policy가 exact하게 검사한다. `protected`에서 secret이 없으면 `${{ github.token }}`으로 대체하지 않고 실패한다.

`WEATHER_GOVERNANCE_READ_TOKEN`의 최소 권한은 기존 `Administration: read`, `Actions: read`, `Checks: read`, `Contents: read`에 `Pull requests: read`를 추가한다. 다른 repository 접근과 write 권한은 허용하지 않는다.

workflow permission은 다음 read-only 집합만 허용한다.

```yaml
permissions:
  actions: read
  checks: read
  contents: read
  pull-requests: read
```

checkout은 계속 `persist-credentials: false`이며 `ref: ${{ github.workflow_sha }}`만 허용한다. `workflow_dispatch`, `repository_dispatch`, Release, PR event, 임의 SHA 입력은 허용하지 않는다.

## 7. 코드 계약 변경

### `deployment.github_evidence`

- `governance_mode`를 명시적으로 입력받고 canonical evidence에 보존한다.
- 양 mode에서 repository, source run, jobs, linked checks, associated merged PR, remote main HEAD를 동일하게 검증한다.
- `protected`에서만 dev/main protection을 조회하고 exact payload를 정규화한다.
- `guarded_private`에서는 private repository를 강제하고 `protections=null`을 반환한다.
- API list 응답은 별도 bounded parser로 처리하고 pagination/extra page 또는 malformed payload를 fail-closed한다.

### `deployment.main_identity`

- `governance_mode`와 normalized merged PR evidence를 pure validator 입력에 포함한다.
- `protected`는 현재 exact protection 검증을 유지한다.
- `guarded_private`는 private/default-main/current-SHA와 `protections is None`을 강제한다.
- source run·linked check·merged PR 검증은 두 mode에서 동일하며 완화하지 않는다.

### `deployment.main_cli`

- gate는 `protected`와 `guarded_private`만 허용하고 다른 값은 GitHub 조회 전에 거부한다.
- mode를 evidence reader와 identity validator에 그대로 전달한다.
- `verify-main`은 filesystem target·Docker·Airflow adapter를 만들지 않는다.
- `deploy-main`은 identity 성공 이후에만 target과 mutation adapter를 구성한다.

### workflow policy

- Deploy Main job guard는 두 exact DNF clause만 허용한다.
- guarded/private와 protected step의 token·env·순서가 다르면 finding을 만든다.
- CI의 `dagbag-runtime` self-hosted route는 계속 `protected` branch push에서만 허용한다. guarded CI는 GitHub-hosted L0만 수행한다.
- deploy workflow 외 self-hosted route, direct Docker/Airflow command, local action, unpinned action, package install은 계속 금지한다.

## 8. runtime 안전 계약

governance fallback은 배포 상태 머신을 단순화하거나 우회하지 않는다.

1. exact identity 검증
2. exclusive lock
3. 현재 Weather DAG pause 상태 snapshot
4. exact 열 개 DAG pause와 readback
5. writer `running`·`queued == 0` bounded drain
6. candidate SHA detached checkout
7. overlay 생성과 Compose config·dry-run
8. stable overlay atomic install
9. exact 네 Airflow code service에만 `docker compose up -d --no-deps`
10. overlay·service health·DAG inventory 검증
11. 원 pause snapshot 복원과 readback
12. durable success record

`--force-recreate`, 전체 stack, `airflow-init`, Postgres, Trino, Marquez, dbt run/build, DAG trigger/backfill은 자동배포 경로에서 금지한다. rollback은 재-pause·writer re-drain 뒤 이전 overlay를 복원하며, pause 상태를 증명하지 못하면 durable `pause_state_unverified`로 남긴다.

## 9. 테스트 전략

동작 코드는 RED → GREEN 순서로 변경한다.

### positive

- private repository + guarded mode + exact merged `dev → main` PR
- protected mode의 기존 exact protection path
- 두 mode 모두 exact main SHA, source run, required jobs/checks 성공
- guarded step은 `${{ github.token }}`, protected step은 governance secret 사용
- 성공 배포와 동일 SHA idempotent no-op

### negative

- 직접 main push, local merge push, bootstrap-created main SHA
- feature branch·fork·다른 repository·미병합 PR
- associated PR 0개·중복·truncated/paginated 응답·wrong merge SHA
- CI failure/cancelled/skipped/neutral 또는 다른 SHA의 성공 check
- source run/job/check-run의 branch·run ID·app 불일치
- public/internal repository의 guarded mode
- guarded mode에 protection payload가 섞이거나 protected mode에 protection이 없음
- workflow guard/token source/step order 완화
- floating main checkout, arbitrary SHA, package install, self-hosted PR route
- `--force-recreate`, data service, DAG allowlist 밖 mutation

### 완료 검증

- focused deployment identity/evidence/CLI/workflow tests
- 전체 `tests/deploy`
- 전체 `tests/repository`
- repository verifier와 provenance integrity
- Ruff, compile, workflow policy CLI
- 실제 GitHub PR에서 L0 CI 성공

fixture 성공은 실제 GitHub 권한, self-hosted runner, Airflow 배포 성공의 증거로 표현하지 않는다.

## 10. rollout과 중단 조건

구현 PR은 `feat/* → dev`로 올리고 CI가 성공한 뒤 `dev`에 병합한다. 이후 별도 `dev → main` PR로 승격한다.

다음 조건이 갖춰지기 전에는 `WEATHER_DEPLOYMENT_ENABLED`를 생성하거나 `enabled`로 바꾸지 않는다.

- guarded fallback 구현 PR과 main 승격 CI 성공
- runner-local target·baseline·ledger 준비
- Python/PyYAML과 Docker/Airflow read-only capability 확인
- 기존 조직 Weather DAG 열 개와 running/queued writer 현황 보고
- pause → drain → deploy → health → rollback 순서 보고
- dbt·Trino·D1·R2 write가 없다는 확인
- 사용자 최종 cutover 승인

최초 전환 전 기존 조직 파이프라인을 수동 all-pause하지 않는다. 배포 orchestrator가 원 pause 상태를 먼저 snapshot한 뒤 pause·drain해야 성공 후 원 상태를 복원할 수 있다.

repo visibility가 public/internal로 바뀌거나, writer가 추가되거나, GitHub credential 침해가 의심되거나, associated PR/read-only evidence를 얻지 못하면 guarded 배포는 즉시 fail-closed한다. 그 경우 native protection 또는 저장소 밖 trusted controller로 전환하기 전까지 자동 배포를 재개하지 않는다.

## 11. 완료 조건

- `guarded_private`가 protection 성공으로 기록되지 않는다.
- exact same-repo merged `dev → main` PR 없는 SHA는 self-hosted runner에 도달하지 않는다.
- bootstrap·직접 push·stale SHA·failed CI가 모두 차단된다.
- guarded/private와 protected token·evidence 경로가 명시적으로 분리된다.
- runtime service·DAG·rollback 안전 계약은 기존보다 약해지지 않는다.
- 최초 cutover 전 실제 Airflow·Docker state change는 없다.
- 위협 모델의 한계와 native protection/trusted controller 전환 조건이 문서와 운영 절차에 남는다.
