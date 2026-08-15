# `main` 자동 배포 최초 전환 절차

## 한 번만 필요한 승인

이 절차의 “첫 `main` release”는 GitHub Release 생성이나 `Publish Release` 클릭이 아니다. 같은 저장소의 exact `dev → main` PR merge와 그 merge SHA의 `CI` 성공을 매번 재검증한 결과가 배포 증거다. 최초 전환에서만 현재 조직 Weather 파이프라인을 안전하게 넘기기 위해 target·baseline·capability의 read-only 보고 후 사용자 명시 승인을 받는다. 전환이 끝난 뒤에는 그 검증을 통과한 `main` merge가 별도 클릭 없이 자동 배포를 시작한다.

승인 전에는 다음 상태를 유지한다.

- repository variable `WEATHER_DEPLOYMENT_ENABLED`는 unset이다.
- `[self-hosted, windows, weather-prod]` runner는 offline이다.
- runner Python/PyYAML 설치·업그레이드 또는 executable 환경 변경을 하지 않는다.
- local deploy target, stable generated overlay와 baseline rollback record를 설치하지 않는다.
- Airflow pause/unpause, Docker `up`, pipeline stop/start와 dbt·Trino·D1·R2 write를 실행하지 않는다.

`Deploy Main`의 두 job은 `WEATHER_DEPLOYMENT_ENABLED=enabled`와 허용 governance mode가 모두 정확할 때만 예약된다. `guarded_private`는 private 단일 소유자·사고 방지 경계에서만 사용할 수 있고, public/internal visibility 또는 추가 writer가 있으면 배포를 중단한다. `protected`는 이보다 강한 native protection readback을 요구한다. protected mode에서 `WEATHER_GOVERNANCE_READ_TOKEN` secret이 없거나 비어 있으면 GitHub-hosted `verify-main`이 실패해야 하며 self-hosted `deploy-main`은 실행되지 않는다. guarded mode에는 이 secret을 설치하지 않는다.

## 고정 배포 범위

자동 배포가 교체할 수 있는 서비스는 아래 네 개뿐이다.

- `airflow-apiserver`
- `airflow-scheduler`
- `airflow-dag-processor`
- `airflow-triggerer`

`airflow-init`, Postgres, Trino, Marquez와 전체 stack은 대상이 아니다. `docker compose down`, `restart`, `--force-recreate`, data service stop은 금지한다. release 배포는 DAG와 dbt source를 read-only bind mount하고 기존 writable `/opt/airflow/logs` volume을 그대로 보존한 채 네 코드 서비스에만 `up -d --no-deps`를 적용한다. release overlay는 `ASK_SEOUL_DBT_ARTIFACT_ROOT=/opt/airflow/logs/weather-dbt/releases/<main SHA>`를 네 서비스에 정확히 주입해 `target`, `logs`, `dbt_packages`를 immutable source 밖에 둔다. 최초 rollback용 baseline overlay는 기존 조직 executor 호환을 위해 DAG만 read-only, dbt mount는 read-write로 유지하며 이 환경 변수를 주입하지 않는다.

상태를 보존할 Weather DAG는 아래 정확한 열 개다.

1. `common_admin_dong_bronze`
2. `weather_serving_export`
3. `weather_serving_freshness_watchdog`
4. `weather_serving_snapshot_refresh`
5. `weather_vilage_fcst_bronze`
6. `weather_vilage_fcst_bronze_backfill`
7. `weather_vilage_fcst_collection_slot_reconciliation`
8. `weather_vilage_fcst_recollect`
9. `weather_vilage_fcst_transform`
10. `weather_w2_canonical_transform`

수동 trigger, backfill, clear, retry, mark-success는 전환 중에도 금지한다.

## 승인 전 read-only 보고서

다음 항목을 읽어 사용자에게 먼저 보고하고 STOP한다. 절대경로, credential reference, token 값, raw Compose/Airflow payload는 보고하지 않는다.

1. 배포 후보인 개인 저장소 `main` SHA, same-repository `dev → main` merged PR와 source `CI` identity. guarded_private이면 private·단일 소유자 경계도 함께 확인
2. 위 네 코드 서비스의 논리 이름과 현재 health/state
3. 위 열 개 DAG 각각의 pause 상태
4. writer allowlist 전체의 `running`·`queued` run 수와 drain timeout/poll 계약
5. local target fingerprint, Airflow `3.2.2` CLI capability fingerprint, self-hosted runner의 Python `3.11`·PyYAML version/capability proof. executable 경로나 package 목록 전체는 보고하지 않음
6. 현재 조직 checkout의 DAG/dbt mount를 가리키는 baseline candidate checksum·fingerprint
7. stable generated overlay 파일의 존재 여부만 나타내는 boolean. 경로 문자열은 보고하지 않음
8. release 후보 overlay가 `/opt/airflow/dags`·`/opt/airflow/dbt`를 read-only로 바꾸고, 기존 writable `/opt/airflow/logs` mount와 exact SHA별 dbt artifact 환경 변수를 보존한다는 검증 결과. baseline 후보는 기존 조직 executor를 위해 dbt read-write·artifact 환경 변수 없음으로 구분함
9. rollback 시 복원할 baseline과 rollback health 실패 시 열 개 DAG를 모두 paused로 유지한다는 동작
10. dbt `run/build`, Trino·Iceberg·D1·R2 write가 모두 0이며 전체 stack/data services를 중지하지 않는다는 확인
11. protected mode인 경우에만 `WEATHER_GOVERNANCE_READ_TOKEN` secret 이름의 존재 여부, 만료일/rotation 담당과 최소권한을 확인. guarded_private에는 이 항목을 요구하지 않으며 값은 어느 mode에서도 조회하거나 출력하지 않음
12. `credential_source_kind=existing_local_env` target이면 runner 프로세스의 `COMPOSE_ENV_FILES`와 `ASK_SEOUL_PROD_ENV_FILE`이 동일한 기존 Compose credential 단일 파일을 가리키는지 확인. 값·절대경로·파일 내용은 출력하지 않음

보고 시점에는 `compose config`, `compose ps`, Airflow list/help/version, ledger read처럼 검증된 read-only adapter만 사용한다. `compose up --dry-run`도 실제 전환 승인 뒤에 실행한다.

## protected mode의 governance read credential

`WEATHER_GOVERNANCE_READ_TOKEN`은 protected mode에서만 `masondev1024/seoul-weather-platform` 한 저장소로 제한해 설치하는 fine-grained token이다. repository permissions는 identity GET에 필요한 `Administration: read`, `Actions: read`, `Checks: read`, `Contents: read`, `Pull requests: read`만 허용하고 write 권한과 다른 저장소 접근은 주지 않는다. 만료일을 두고 담당자가 만료 전에 rotation하며, 교체 뒤 다음 preflight의 성공/실패만 확인한다. 값은 workflow argv, 로그, report, local target 또는 repository 파일에 기록하지 않는다.

protected mode의 GitHub Actions repository secret에는 위 token을 `WEATHER_GOVERNANCE_READ_TOKEN` 이름으로 저장한다. workflow는 이를 CLI step의 `GH_TOKEN`으로만 전달한다. checkout은 계속 `persist-credentials: false`이며 `${{ github.token }}`으로 protected protection readback을 대체하지 않는다. guarded mode는 private repository와 exact merged-PR evidence를 다시 읽고 workflow의 read-only `github.token` 권한(`Pull requests: read` 포함)으로 검증하므로 이 secret을 설치하지 않는다.

## 승인 후 최초 전환 순서

사용자가 STOP 보고를 확인하고 최초 전환을 명시 승인한 뒤 아래 순서만 사용한다. 운영자가 열 개 DAG를 미리 수동 pause하거나 drain하지 않는다. 그러면 자동 배포기가 원래 pause 상태를 잃어 성공 뒤 새 pipeline을 복원할 수 없다.

1. 기존 Compose credential 파일은 복사하거나 repository에 넣지 않고 runner 관리 환경의 `COMPOSE_ENV_FILES`와 `ASK_SEOUL_PROD_ENV_FILE`이 동일한 단일 파일을 참조하게 한다. repository 밖 `WEATHER_DEPLOY_TARGET_PATH`와 함께 값·절대경로를 로그에 남기지 않은 채 존재 여부를 확인한다.
2. repository 밖 local target과 현재 조직 DAG/dbt mount를 가리키는 baseline overlay를 설치한다. 이 cutover activation은 filesystem baseline 준비만 수행하고 DAG pause/unpause나 코드 서비스 배포를 하지 않는다.
3. baseline checksum record를 원자 기록하고 Compose `config`·`up --dry-run`으로 네 코드 서비스 외 대상이 없는지 확인한다. baseline restore를 실제 stable overlay 교체와 같은 원자 경로로 rehearsal하고 health를 확인한다.
4. protected mode를 선택한 경우에만 repo-scoped governance read token을 GitHub Actions repository secret `WEATHER_GOVERNANCE_READ_TOKEN`으로 등록하고 이름·만료·최소권한(`Pull requests: read` 포함)을 확인한다. guarded_private에는 이 단계를 수행하지 않으며 입력값은 화면·명령 인자·로그·보고서에 출력하지 않는다.
5. workflow 밖의 승인된 runner 관리 절차로 Python `3.11` 환경과 PyYAML을 사전 설치한다. 이어 version/capability를 read-only로 다시 확인해 보고한 기대값과 일치시킨다. workflow는 `pip install`을 실행하지 않고 self-hosted job에서 `setup-python`도 사용하지 않는다.
6. self-hosted runner를 시작한다. runner 서비스는 승인된 `WEATHER_DEPLOY_TARGET_PATH`, `COMPOSE_ENV_FILES`, `ASK_SEOUL_PROD_ENV_FILE`을 상속해야 하며 workflow YAML이 이 값을 덮어쓰지 않는다.
7. `WEATHER_DEPLOYMENT_ENABLED=enabled`를 설정하고 exact readback한다. 이 전까지 deploy job은 계속 비활성이다.
8. 개인 private 단일 소유자 guarded 경계 또는 protected 경계에서 같은 저장소의 `dev → main` PR을 merge한다. GitHub Release나 tag를 만들지 않는다.
9. exact merge SHA의 `CI`가 성공하면 `workflow_run`의 GitHub-hosted `verify-main`이 source workflow name `CI`, suffix 없는 path `.github/workflows/ci.yml`, 별도 `head_branch=main`, event/status/conclusion, remote `main`, same-repository merged PR와 branch-bound required checks를 검증한다. guarded_private에서는 private·단일 소유자 경계와 workflow `github.token`을 확인하고, protected에서는 추가로 governance secret과 native protection readback을 확인한다.
10. preflight 성공 뒤에만 self-hosted `deploy-main`이 같은 identity를 다시 검증하고 기존 조직 Weather DAG 열 개의 현재 pause snapshot을 캡처한다. 이어 정확한 열 개만 pause하고 writer allowlist의 `running`·`queued`가 모두 0이 될 때까지 bounded drain한다. timeout이면 snapshot을 복원하고 배포를 중단한다.
11. drain 성공 뒤 exact SHA checkout·candidate overlay config/dry-run·네 코드 서비스 배포·health를 수행한다. Compose config는 release source가 read-only이고, 기존 logs volume이 writable이며, artifact root가 exact SHA에 결속됐는지 확인한다.
12. 성공하면 deploy 시작 시 캡처한 snapshot에서 원래 unpaused였던 DAG만 복원해 개인 저장소 코드 기반 새 Weather pipeline을 시작한다. 원래 paused였던 DAG는 paused로 유지한다.

각 단계가 실패하면 다음 단계로 넘어가지 않는다. 전체 stack이나 data service를 중지해 우회하지 않는다.

## rollback과 실패 상태

- stable overlay 설치 전 실패: 코드 서비스를 재배포하지 않고 최초 pause snapshot을 복원한다.
- stable overlay 설치 뒤 배포 또는 health 실패: 열 개 DAG를 paused로 유지하고 승인·rehearsal된 baseline overlay를 원자 복원한 뒤 같은 네 코드 서비스만 재배포하고 health를 다시 확인한다.
- baseline rollback health 성공: 최초 pause snapshot을 복원하고 outcome을 `rolled_back`으로 기록한다.
- baseline rollback 자체가 실패: 열 개 DAG를 모두 paused로 유지하고 `rollback_failed`를 기록한다. 자동 재시도하거나 성공으로 보고하지 않는다.
- 승인·rehearsal된 baseline record가 없으면 첫 자동 배포는 pause 전에 fail-closed해야 한다.
- 첫 `main` promotion, source `CI` 또는 GitHub-hosted preflight가 self-hosted mutation 전에 실패하면 아직 pause snapshot이나 Airflow mutation이 없으므로 baseline overlay와 기존 코드 서비스·DAG 상태를 그대로 둔다. 원인을 고치는 동안 `WEATHER_DEPLOYMENT_ENABLED`를 unset하고 runner를 offline으로 되돌린다.

## 전환 후 운영 계약

최초 전환이 성공한 뒤 private 단일 소유자 `guarded_private` 또는 더 강한 `protected` 경계에서 같은 저장소의 exact `dev → main` merge가 배포 증거다. public/internal visibility 또는 추가 writer는 guarded 배포를 즉시 중단하고 protected mode 전환을 요구한다. 별도 Draft, Release, tag, 배포 보고서 확인 또는 Publish 클릭은 없다. 각 배포는 같은 pause snapshot·bounded drain·code-service-only deploy·health·restore·rollback 절차를 반복한다. governance/protection readback, runner isolation, 필요한 mode의 credential, target fingerprint 또는 baseline/직전 성공 record가 유효하지 않으면 자동 배포는 fail-closed한다.

`Deploy Main`은 package manager를 실행하지 않는다. GitHub-hosted `verify-main`은 pinned checkout과 pinned `setup-python` 뒤 stdlib-only 검증 CLI만 호출하고, self-hosted `deploy-main`은 pinned checkout 뒤 승인 시점에 사전 준비한 Python/PyYAML 환경으로 배포 CLI만 호출한다. runner 환경의 version/capability drift는 workflow 실행 중 설치로 고치지 않고 deployment flag를 비활성화한 뒤 다음 승인된 maintenance에서 교정한다.

Weather dbt 실행은 source checkout을 수정하지 않는다. release SHA별 artifact root 아래에서 attempt-local `target`, `logs`, `dbt_packages`를 사용하고, 성공한 dbt phase의 `manifest.json`만 같은 root의 stable `target/manifest.json`으로 원자 게시한다. `weather_serving_export`는 이 stable manifest가 준비된 성공 phase 이후에만 정상 동작하며, manifest 부재를 fixture나 이전 checkout의 파일로 대체하지 않고 fail-closed한다. CI의 `dbt-weather` job은 source를 read-only로 만든 상태에서 외부 artifact 경로로 `dbt deps`·`dbt parse`를 실행하고 source diff가 0인지 검증한다.
