# `main` CI 성공 기반 Weather 자동 배포 설계

## 1. 결정과 적용 범위

Weather Platform의 운영 배포 승인은 보호된 `dev → main` PR merge로 단일화한다. `main` merge 자체로 배포하지 않고, 그 merge commit의 `CI` workflow가 성공한 직후 별도 `Deploy Main` workflow가 같은 SHA를 자동 배포한다.

이 설계는 다음 기존 안을 폐기한다.

- Draft Release와 배포 보고서 자동 생성
- CalVer tag와 `Publish Release` 수동 승인
- 60분 `deployment-plan.json`
- Release asset·tag·workflow SHA의 다중 identity gate

다음 구현은 유지하고 재사용한다.

- `dev` PR CI와 `dev → main` 승격 검증
- `dev`·`main` native protection과 required check의 GitHub Actions App 결속
- 최초 `main` bootstrap 예외의 `guarded_private` 제한
- `DeployTarget`, 민감정보 차단, read-only inventory, Airflow CLI 호환성 계약

## 2. 목표와 비목표

### 목표

1. 보호된 `main`의 정확한 merge SHA만 자동 배포한다.
2. 기존 Weather DAG 상태와 실행 중 writer를 보존하며 Airflow 코드 서비스만 교체한다.
3. 실패 시 직전 성공 SHA로 자동 rollback한다.
4. 최초 전환 전에만 현재 로컬 Airflow 상태를 보고하고 사용자 승인을 받는다.
5. 최초 활성화 이후에는 추가 클릭이나 Release 발행 없이 `main` merge가 최종 배포 승인이다.

### 비목표

- DAG 수동 trigger, backfill, clear, retry, mark-success
- dbt `run`·`build` 또는 transform 실행
- Trino·Iceberg·D1·R2 write
- Postgres·Trino·Marquez 등 데이터 서비스 재시작
- `docker compose down`, `--force-recreate`, 전체 stack 재생성
- 일반 Marketplace, K-Skill proxy 또는 public 전환 배포

## 3. 배포 trigger와 신뢰 경계

`.github/workflows/deploy-main.yml`은 `workflow_run`만 받는다.

```yaml
on:
  workflow_run:
    workflows: [CI]
    types: [completed]
    branches: [main]
```

GitHub-hosted `preflight` job은 event와 원격 readback을 검증하고, self-hosted `deploy` job은 그 성공 뒤 동일 검증을 다시 수행한다. 다음 조건을 모두 만족할 때만 self-hosted runner에 도달한다.

- `WEATHER_GOVERNANCE_MODE == protected`
- `WEATHER_DEPLOYMENT_ENABLED == enabled`
- source workflow 이름은 `CI`, path는 suffix 없는 정본 `.github/workflows/ci.yml`, branch는 별도 `head_branch=main`
- source event가 `push`
- source branch가 `main`
- source status/conclusion이 `completed/success`
- source head SHA가 현재 원격 `refs/heads/main`과 같음
- 실행 중인 deploy workflow가 기본 브랜치 `main`의 정본 workflow임
- exact source SHA의 `CI / required`와 `Promotion Source / required`가 branch-bound GitHub Actions check로 성공

branch protection readback은 기본 `GITHUB_TOKEN`으로 대체하지 않는다. GitHub REST의 해당 endpoint에 필요한 repository `Administration: read`를 포함하고 `Actions: read`, `Checks: read`, `Contents: read`만 추가한 repository-scoped fine-grained token을 `WEATHER_GOVERNANCE_READ_TOKEN` secret으로 보관한다. 두 CLI step은 이 값을 `GH_TOKEN` 환경 변수로만 전달하며 argv·로그·artifact에는 포함하지 않는다. secret 누락, 권한 부족, 403/404는 모두 mutation 전 identity 실패다.

PR, `workflow_dispatch`, `repository_dispatch`, `pull_request_target`, Release event는 배포 trigger가 아니다. `guarded_private`이거나 `WEATHER_DEPLOYMENT_ENABLED != enabled`이면 preflight와 runner job 모두 실행하지 않는다.

## 4. 구성 요소

### 4.1 thin workflow

`deploy-main.yml`의 GitHub-hosted job은 pinned `checkout`·`setup-python` 뒤 stdlib-only identity preflight만 수행한다. self-hosted job은 pinned `checkout` 뒤 exact `deploy-main`만 호출하며 `setup-python`·패키지 설치를 실행하지 않는다. Python `3.11`·PyYAML은 최초 승인된 cutover에서 workflow 밖에 사전 준비하고, self-hosted job만 유일한 state-changing entrypoint가 된다.

```text
python -m deployment.main_cli verify-main
python -m deployment.main_cli deploy-main
```

Docker·Airflow 명령을 workflow YAML에 직접 쓰지 않는다. 두 job의 checkout은 기본 브랜치의 trusted workflow SHA를 사용한다. `preflight`가 source CI SHA·원격 main·protection·branch-bound checks를 검증해야 `deploy`가 예약되며, self-hosted CLI가 같은 검증을 반복한 뒤 별도 runtime directory에 exact source SHA를 detached checkout한다.

### 4.2 main identity gate

`deployment.main_cli`는 mutation adapter를 만들기 전에 다음을 검증한다.

- repository/default branch/protection readback
- workflow event와 source CI identity
- source SHA가 현재 remote `main` HEAD와 동일
- source SHA가 `dev → main` merge 증거를 가짐
- local `DeployTarget` fingerprint와 Airflow CLI `3.2.2` capability fingerprint
- 동일 SHA의 성공 배포가 이미 있으면 idempotent no-op
- 다른 배포가 진행 중이면 exclusive lock 실패

오류는 고정 stage/category만 출력한다. event, API body, 로컬 경로, credential reference, CLI raw output은 출력하지 않는다.

### 4.3 로컬 ledger

runner-local repository 밖 경로에 checksum이 포함된 atomic JSON record를 저장한다.

- 현재 진행 중 lock
- candidate SHA와 시작·완료 시각
- 배포 outcome과 health 결과
- 직전 성공 SHA
- rollback outcome

`.tmp → fsync → os.replace` 순서로 기록한다. 동일 성공 SHA는 재배포하지 않는다. checksum이 틀리거나 partial인 record는 rollback 후보가 아니다.

### 4.4 기존 Compose와 Release checkout 연결

기존 `ask-seoul-sample`의 `dags/`·`dbt/`를 덮어쓰거나 이동하지 않는다. runner-local repository 밖 경로에 release SHA별 detached checkout을 만들고, Airflow 코드 서비스의 `/opt/airflow/dags`와 `/opt/airflow/dbt` mount만 재정의하는 generated Compose overlay를 사용한다.

- local target은 stable `generated_overlay_file` 절대경로를 가진다.
- candidate overlay는 exact release checkout의 `dags/`와 `dbt/`를 read-only bind mount하고 기존 writable `/opt/airflow/logs` volume을 보존한다. 네 코드 서비스에는 exact `ASK_SEOUL_DBT_ARTIFACT_ROOT=/opt/airflow/logs/weather-dbt/releases/<candidate SHA>`만 추가한다.
- overlay의 service key는 승인된 `airflow_code_services`와 정확히 같고 data service는 포함할 수 없다.
- candidate overlay는 temp file로 만들고 Compose config·dry-run 검증에 사용한다.
- 검증 후 stable overlay를 `fsync → os.replace`로 교체하고 코드 서비스만 재배포한다.
- rollback은 ledger가 보존한 직전 성공 overlay checksum/content 또는 최초 baseline overlay를 원자 복원한 뒤 같은 코드 서비스만 재배포한다.

최초 baseline overlay는 현재 로컬 harness의 기존 `dags/`·`dbt/` mount를 그대로 가리킨다. 기존 조직 dbt executor의 rollback 호환을 위해 baseline은 DAG read-only, dbt read-write이며 새 artifact 환경 변수를 주입하지 않는다. 최초 승인 전에는 생성·설치하지 않고 sanitized candidate fingerprint와 mount seam만 보고한다.

## 5. 자동 배포 순서

배포 순서는 고정한다.

1. identity·protection·target·CLI 호환성 검증
2. exclusive lock 획득과 현재 DAG pause 상태 snapshot
3. 정확한 10개 Weather DAG만 pause
4. writer allowlist의 `running`·`queued` run이 0이 될 때까지 bounded drain
5. source SHA를 repository 밖 runtime directory에 detached checkout
6. candidate generated overlay를 temp file로 작성
7. candidate overlay를 포함한 Compose config·`up --dry-run`으로 mount와 대상이 Airflow 코드 서비스에만 한정되는지 검증
8. stable overlay를 원자 교체하고 `docker compose up -d --no-deps <airflow code services>` 실행
9. stable overlay가 expected artifact bytes/checksum과 같은지 확인하고 Compose code-service health, Airflow DAG list/import-error를 read-only 검증
10. 원래 unpaused였던 DAG만 unpause
11. overlay checksum을 포함한 성공 record 원자 기록과 lock 해제

일반 자동배포 code-service 후보는 `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`뿐이다. `airflow-init`은 DB migration/init 성격의 one-shot 서비스이므로 자동배포에서 금지하고 별도 maintenance 승인 범위로 남긴다. 실제 집합은 승인된 local `DeployTarget`과 Compose config가 모두 일치해야 한다. Generated overlay는 Compose의 volume target merge를 사용해 DAG/dbt target만 교체하며 `!override`를 사용하지 않아 기존 plugins/logs mount를 보존한다. release config는 logs mount가 정확히 하나이고 writable이며 base와 동일한지, artifact 환경 변수가 exact SHA root인지 추가로 검증한다.

## 6. 실패와 rollback

- pause 전 실패: 상태 변경 없이 종료한다.
- drain timeout: code deploy 없이 원래 pause 상태를 복원한다.
- deploy 또는 health 실패: Weather DAG는 paused 상태를 유지한 채 직전 성공 overlay 또는 승인된 baseline overlay를 원자 복원하고 같은 allowlist로 재배포한다.
- rollback health 성공: 원래 pause 상태를 복원하고 `rolled_back`으로 기록한다.
- rollback 실패: 모든 Weather DAG를 paused로 유지하고 `rollback_failed`를 기록한다. 성공으로 보고하지 않으며 자동 재시도하지 않는다.
- 직전 성공 SHA가 없는 최초 cutover: 승인된 기존 로컬 baseline 없이는 pause 전에 실패한다.

## 7. 최초 cutover 승인

자동 CD 구현과 테스트만으로 runner를 활성화하지 않는다. 최초 1회 다음 내용을 read-only로 수집해 사용자에게 보고한다.

1. 배포 대상 commit과 변경되는 Airflow 코드 서비스
2. 정확한 10개 DAG의 pause 상태
3. writer DAG의 running·queued run 수와 drain 예상
4. 현재 mount·Compose project·Airflow CLI fingerprint
5. 기존 로컬 mount를 가리키는 baseline overlay candidate와 rollback 경로
6. dbt·Trino·D1·R2 write가 0임
7. pause → drain → dry-run → deploy → health → restore 순서

승인 뒤에만 local target/baseline을 설치하고 self-hosted runner를 시작한 뒤 `WEATHER_DEPLOYMENT_ENABLED=enabled`를 exact readback으로 활성화한다. 그 이후에는 보호된 `main` merge마다 같은 절차가 자동 실행된다.

최초 실제 전환에서 말하는 “main release”는 GitHub Release 발행이 아니라 개인 저장소의 보호된 `dev → main` PR merge다. 전환 직전 read-only STOP 보고 후 사용자가 승인하면 baseline·runner·gate를 준비하고 첫 main CI 성공 SHA의 orchestrator가 기존 pause 상태를 먼저 capture한 뒤 Weather DAG 10개만 pause·drain한다(data/container service 전체 중지는 금지). 성공 시 새 Weather-only 코드로 capture한 pause snapshot을 복원하고, 실패 시 기존 조직 mount를 가리키는 baseline으로 rollback한다. 전환 전에 운영자가 10개를 수동 all-pause하면 원 snapshot을 잃으므로 금지한다.

최초 1회 준비는 workflow에서 호출할 수 없는 별도 `deployment.cutover_cli`로 수행한다. `inspect`는 target·Compose·Airflow inventory와 in-memory baseline fingerprint만 읽고 sanitized 결과만 출력한다. 승인 후 `activate`는 `GITHUB_ACTIONS=true`를 거부하고, 사용자가 확인한 target/baseline fingerprint를 다시 대조한 뒤 baseline overlay config/dry-run, stable overlay atomic install·동일 bytes restore rehearsal, baseline ledger record, 최종 target file atomic install 순서까지만 수행한다. runner 시작, repository secret/variable 변경, DAG pause/unpause, code-service deploy는 이 CLI가 수행하지 않는다.

## 8. 테스트와 검증

### L0 — 로컬·CI, state change 없음

- workflow event/branch/SHA/protection negative fixtures
- target·Airflow CLI contract와 command argv allowlist
- ledger·generated overlay atomicity, lock, idempotency
- pause/drain/restore state machine의 deterministic fake tests
- deploy/health/rollback failure injection
- 모든 subprocess를 injected fake runner로 차단
- workflow policy로 self-hosted exact action/input/order/command 검증

### L1 — 최초 승인 후, pause 전

- 실제 target load와 fingerprint
- read-only inventory
- Compose `config`·`ps`
- Airflow list/help/version
- `docker compose up --dry-run`의 code-service 한정 검증
- baseline restore rehearsal

dbt parse·serving contract는 exact source SHA의 `dbt-weather`를 포함한 `CI / required` 성공 증거로 충족한다. CI는 dbt source를 read-only로 만든 뒤 외부 target/log/packages 경로에서 pinned dbt `deps`·`parse`를 실행하고 source diff 0과 serving contract를 검증한다. 배포 checkout에는 생성물인 `dbt_packages`·`target/manifest.json`을 포함하지 않으며 runtime health에서 이를 다시 만들지 않는다. Weather dbt 실행기는 source checkout을 cwd/project/profiles로만 읽고 `ASK_SEOUL_DBT_ARTIFACT_ROOT` 아래 SHA별 attempt path에서 `dbt deps`를 self-heal한다. manifest를 만든 성공 phase만 `<artifact root>/target/manifest.json`을 원자 교체하며 serving factory는 이 stable manifest만 읽는다. env가 없는 baseline은 기존 project-local 경로를 그대로 사용한다.

실제 pause와 deploy는 최초 승인 이후에만 수행한다.

## 9. 폐기·정리 대상

구현 변경에서 다음 산출물을 제거하거나 새 설계에 맞게 교체한다.

- `deployment/plan.py`와 `tests/deploy/test_deployment_plan.py`
- Release preflight·deployment-plan 전용 문서와 workflow fixture
- `prepare-release.yml`, `deploy-prod.yml`, Release identity/CalVer/asset 관련 계획
- 기존 설계서의 Draft Release·Publish gate 조항

`deployment/redaction.py`의 file URI·token 차단처럼 자동 배포에서도 유효한 보안 보강은 유지한다.

## 10. 완료 조건

- `dev → main` 이외의 main 승격은 계속 거부된다.
- 성공한 exact main CI SHA만 deploy workflow를 시작한다.
- PR 코드·guarded mode·stale main SHA는 self-hosted runner에 도달하지 않는다.
- 코드 서비스 외 Compose 대상은 mutation 전에 차단된다.
- 성공 시 원래 pause 상태가 복원된다.
- 실패 시 rollback 또는 paused fail-closed 상태가 증명된다.
- 최초 cutover 전에는 실제 Airflow/Docker state change가 없다.
- 최초 승인 뒤에는 `main` merge 외 추가 수동 승인 없이 배포가 완료된다.
