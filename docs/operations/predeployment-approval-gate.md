# Airflow 사전 승인 게이트

## 적용 범위

Airflow 코드 서비스 배포, DAG pause/unpause, pipeline start/stop에는 승인된 전환 경계가 필요하다. 수동 trigger, backfill, clear, retry, mark-success와 dbt·Trino·D1·R2 write는 자동 배포 범위가 아니다.

최초 전환 전에는 모든 Airflow state change에 사용자 사전 승인이 필요하다. 최초 전환이 성공한 뒤에는 보호된 같은 저장소의 `dev → main` PR merge와 exact merge SHA의 `CI` 성공이 해당 SHA 배포의 승인이다. GitHub Release, tag, Draft 또는 Publish 클릭은 사용하지 않는다.

## 승인 전 허용 작업

승인 전에는 상태를 바꾸지 않는 secretless L0 검증과 read-only inventory만 수행한다.

```powershell
./tools/verify_repository.ps1
```

표준 L0 경로는 repository policy, provenance, workflow policy와 unit test이며 Airflow·Docker·pipeline을 호출하지 않는다. 최초 전환 보고를 준비할 때만 검증된 read adapter로 다음을 읽을 수 있다.

- local deploy-target schema·permission과 sanitized fingerprint
- `docker compose config --services`, `docker compose ps`
- Airflow version/help, DAG 목록과 exact 열 개 DAG의 pause 상태
- writer allowlist의 running·queued run 수
- ledger의 baseline·직전 성공 SHA/checksum 요약
- Airflow CLI capability fingerprint
- self-hosted runner Python `3.11`과 PyYAML의 sanitized version/capability proof

절대경로, 로컬 IP, credential reference, token 값, `.env` 값과 raw inspect payload는 보고하지 않는다.

## 저장소 보호와 자동 배포 비활성 기본값

최초 cutover의 명시 승인 전에는 어떤 mode도 production runner를 시작하지 않는다. `guarded_private`는 private 단일 소유자 저장소의 사고 방지 경계이며, 승인 후에도 같은 저장소의 exact `dev → main` merged PR과 CI 증거를 매번 재검증해야 한다. public/internal visibility 또는 추가 writer가 확인되면 guarded 배포를 중단한다. `protected`는 native protection readback까지 요구하는 더 강한 경계다.

최초 전환 승인 전에는 `WEATHER_DEPLOYMENT_ENABLED`를 unset으로 두고 `[self-hosted, windows, weather-prod]` runner를 offline으로 유지한다. 보호 규칙, required checks의 GitHub Actions App 결속, read credential 또는 target 계약이 달라지면 runner와 deployment enable flag를 먼저 비활성화한다.

`WEATHER_GOVERNANCE_READ_TOKEN`은 protected mode의 protection readback용으로만 대상 저장소 하나에 제한해 설치하는 fine-grained read-only token이다. `Administration`, `Actions`, `Checks`, `Contents`, `Pull requests`의 read만 부여하고 repository secret으로 저장한다. 값은 읽거나 출력하지 않으며 만료와 rotation을 관리한다. protected mode에서 secret이 없거나 비어 있으면 GitHub-hosted preflight가 실패하고 self-hosted job은 실행되지 않아야 한다. guarded mode에는 이 secret을 설치하지 않는다.

## 최초 전환 보고와 STOP

실제 변경 전에 [main 자동 배포 최초 전환 절차](./main-auto-deploy-first-cutover.md)의 항목을 사용자에게 보고하고 STOP한다. 핵심 보고 내용은 다음과 같다.

1. 대상 `main` SHA와 변경되는 네 Airflow 코드 서비스
2. exact 열 개 Weather DAG의 pause snapshot과 writer running·queued 수
3. target/CLI/baseline candidate의 sanitized fingerprint, stable overlay 존재 여부 boolean, runner Python `3.11`·PyYAML capability proof
4. baseline 설치·rehearsal → protected인 경우에만 read secret 준비 → runner Python/PyYAML 사전 준비 → runner·flag 활성화 → eligible guarded_private 또는 protected `dev → main` merge 재검증 → deploy기가 원 상태 capture → exact 열 개 pause·drain → deploy·health → capture 상태 복원 순서
5. rollback 성공과 rollback 실패 시 fail-closed pause 동작
6. 전체 stack/data service stop이 없고 dbt·Trino·D1·R2 write가 0이라는 확인

사용자 승인 전에는 Docker `up`·`up --dry-run`, DAG pause/unpause, pipeline stop/start, local target/baseline 설치, runner Python/PyYAML 설치·업그레이드, runner 시작, secret/variable write를 실행하지 않는다.

## 승인 후 최초 전환

승인 뒤에는 local target과 baseline overlay를 설치하고 config/dry-run 및 baseline restore rehearsal을 통과시킨다. protected mode를 선택한 경우에만 repo-scoped governance read token을 `WEATHER_GOVERNANCE_READ_TOKEN` repository secret으로 등록하며 `Pull requests: read`를 포함한 최소권한을 확인한다. guarded mode에는 secret을 등록하지 않는다. 값은 화면·명령 인자·로그·보고서에 출력하지 않는다. 이어 workflow 밖 runner 관리 절차로 Python `3.11` 환경과 PyYAML을 한 번 사전 설치하고 sanitized version/capability readback을 확인한다. 그 다음 runner를 시작하고 `WEATHER_DEPLOYMENT_ENABLED=enabled`를 exact readback한 뒤, private 단일 소유자 guarded 경계 또는 protected 경계의 same-repository `dev → main` PR merge와 successful main CI를 재검증해 첫 자동 배포를 시작한다.

운영자가 전환 전에 열 개 DAG를 수동 pause하거나 drain하지 않는다. 첫 self-hosted 배포기가 기존 조직 Weather DAG의 현재 pause 상태를 캡처한 뒤 exact 열 개만 pause하고 writer를 bounded drain해야 성공 후 같은 상태로 새 pipeline을 시작할 수 있다. cutover activation은 filesystem baseline 준비만 수행하며 Airflow state를 바꾸지 않는다.

배포는 `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`에만 `up -d --no-deps`를 적용한다. `airflow-init`, Postgres, Trino, Marquez, `compose down`, `restart`, `--force-recreate`는 금지한다. 성공하면 최초 snapshot에서 원래 unpaused였던 DAG만 복원한다. 실패하면 승인·rehearsal된 baseline으로 rollback하고, rollback도 실패하면 exact 열 개 DAG를 모두 paused로 유지한다.

`Deploy Main` workflow 안에서는 `pip` 또는 다른 package install을 실행하지 않는다. hosted preflight만 pinned `setup-python`을 사용하며 self-hosted job은 runner의 사전 준비된 Python/PyYAML을 그대로 사용한다. capability drift가 있으면 workflow가 환경을 고치지 않고 fail-closed해야 한다.

## 전환 후 승인 모델

최초 cutover 이후 추가 수동 Release 승인은 없다. eligible guarded_private 또는 protected same-repository `dev → main` merge와 exact main CI 성공을 매번 다시 검증한 결과가 배포 증거이며, `Deploy Main`은 GitHub-hosted identity preflight 뒤에만 self-hosted 배포를 실행한다. stale SHA, 다른 workflow/event/branch, public/internal guarded repository, extra writer, disabled flag, protected mode의 missing read secret, invalid target/ledger/baseline은 모두 mutation 전에 fail-closed한다.
