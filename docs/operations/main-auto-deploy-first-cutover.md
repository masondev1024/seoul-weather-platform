# `main` 자동 배포 최초 전환 절차

## 이 전환에서 말하는 “release”

GitHub Release나 `Publish` 버튼을 누르는 절차가 아니다. 같은 저장소의 정확한
`dev → main` PR 병합과 그 병합 SHA의 `CI` 성공을 배포 증거로 삼는다. 최초 한 번만
개인 운영 환경의 대상·기준선·실행기 준비를 읽기 전용으로 확인하고, 사용자 승인을
받아 전환한다. 이후에는 같은 검사를 통과한 main 병합이 자동 배포를 시작한다.

## 승인 전 상태

- `WEATHER_DEPLOYMENT_ENABLED`는 비워 둔다.
- `[self-hosted, windows, weather-prod]` runner는 offline이다.
- runner Python/PyYAML 설치·업그레이드와 실행 환경 변경을 하지 않는다.
- 개인 배포 대상, 안정 overlay, 되돌리기 기준선을 설치하지 않는다.
- Airflow 일시정지 변경, Docker `up`, 파이프라인 시작·중지, `dbt`·Trino·D1·R2 쓰기를
  하지 않는다.

`guarded_private`는 비공개 단일 소유자 저장소에서만 쓸 수 있다. 공개 저장소나 작성자가
늘어난 상태라면 배포 플래그를 끄고 GitHub 보호 규칙을 실제로 확인하는 `protected`나
저장소 밖의 신뢰된 제어기로 바꾼다. protected 모드에서
`WEATHER_GOVERNANCE_READ_TOKEN`이 없으면 호스팅 검사가 실패하고 self-hosted 작업은
실행되지 않아야 한다.

## 자동 배포 대상

아래 네 코드 서비스만 교체한다.

- `airflow-apiserver`
- `airflow-scheduler`
- `airflow-dag-processor`
- `airflow-triggerer`

`airflow-init`, Postgres, Trino, Marquez, 전체 stack은 대상이 아니다. `down`,
`restart`, `--force-recreate`, 데이터 서비스 중지는 사용하지 않는다. 기존 로그 volume은
보존하고 DAG·`dbt` 원본은 읽기 전용으로 연결한다.

## 상태를 보존할 Weather DAG 열 개

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

전환 중 수동 trigger, backfill, clear, retry, mark-success는 하지 않는다.

## 승인 전 읽기 보고서

다음 항목을 비밀값 없이 읽어 보고하고 중단한다.

1. 개인 저장소 `main` SHA, `Promotion Source / required`, 같은 저장소 `dev → main` 병합과
   source `CI`의 정체성
2. 네 코드 서비스의 상태와 health
3. 열 개 DAG의 일시정지 상태
4. writer의 실행 중·대기 중 작업 수와 비우는 제한 시간
5. 배포 대상·Airflow `3.2.2`·runner Python `3.11`/PyYAML 기능 지문
6. 현재 조직 코드와 연결된 기준선 후보의 checksum
7. 안정 overlay가 있는지 여부(Boolean)
8. release overlay는 원본 읽기 전용·로그 쓰기 가능·커밋별 `dbt` 산출물 경로인지,
   기준선은 기존 실행기를 위해 구성이 다른지
9. 되돌리기 때 기준선을 복원하고 health 실패 시 DAG 열 개를 모두 멈추는지
10. `dbt` 실행과 Trino·Iceberg·D1·R2 쓰기가 0인지, 전체 stack을 멈추지 않는지
11. protected 모드일 때만 governance token의 존재·만료·최소권한을 확인(값은 금지)
12. 기존 Compose가 하나의 개인 환경 파일만 읽는지 확인(값·절대경로 출력 금지)

## 승인 후 전환 순서

1. 개인 환경 파일을 복사하지 않고 runner의 `COMPOSE_ENV_FILES`와
   `ASK_SEOUL_PROD_ENV_FILE`이 같은 파일을 가리키는지만 확인한다.
2. 저장소 밖의 배포 대상과 현재 조직 코드/`dbt`를 가리키는 기준선 overlay를 준비한다.
3. 기준선 checksum을 원자적으로 기록하고 Compose `config`·`up --dry-run` 결과에 네
   코드 서비스만 있는지 확인한다. 기준선 복원도 실제로 연습한다.
4. protected 모드일 때만 `WEATHER_GOVERNANCE_READ_TOKEN`을 최소권한 secret으로 등록한다.
5. runner 관리 절차에서 Python `3.11`과 PyYAML을 미리 준비하고 버전·기능을 확인한다.
6. runner를 시작하고 승인된 대상·환경 파일을 상속하는지 확인한다.
7. `WEATHER_DEPLOYMENT_ENABLED=enabled`를 설정하고 정확히 읽어 확인한다.
8. 같은 저장소 `dev → main` PR을 병합한다. Release나 tag는 만들지 않는다.
9. 병합 SHA의 `CI`와 승격 증거가 성공하는지, protected 모드라면 보호 규칙도 확인한다.
10. 배포기가 열 개 DAG의 현재 상태를 저장하고 정확히 열 개만 멈춘 뒤 writer 실행·대기가
    0이 될 때까지 제한된 시간만 기다린다.
11. 기준선·release 설정을 읽기 전용으로 확인한 뒤 네 코드 서비스만
    `up -d --no-deps`로 반영하고 health를 확인한다.
12. 성공하면 원래 켜져 있던 DAG만 복원한다. 원래 멈춘 DAG는 그대로 둔다.

어느 단계든 실패하면 다음 단계로 넘어가지 않는다. 전체 stack을 중지해 우회하지 않는다.

## 되돌리기

- 준비 전 실패: 서비스를 바꾸지 않고 최초 DAG 상태를 유지한다.
- 배포·health 실패: 열 개 DAG를 모두 멈추고 연습한 기준선 overlay와 네 코드 서비스만
  복원한다.
- 기준선 health 성공: 최초 상태를 복원하고 `rolled_back`으로 기록한다.
- 기준선 health도 실패: 열 개 DAG를 계속 멈추고 `rollback_failed`로 기록한다. 자동 재시도
  하지 않는다.
- 기준선 기록이 없으면 DAG를 멈추기 전부터 안전하게 거부한다.

## 전환 후 계약

매 배포마다 정확한 `dev → main` 병합, 병합 SHA의 `CI`, 대상 지문, 기준선 기록, 보호
규칙을 다시 확인한다. 오래된 SHA, 다른 branch/fork, 필수 검사 누락은 변경 전에 거부한다.
workflow 안에서는 `pip install`을 실행하지 않는다. runner 의존성 차이는 별도 승인된
정비에서 고친다.
