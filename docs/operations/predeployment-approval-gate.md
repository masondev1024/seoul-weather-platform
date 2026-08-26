# Airflow 사전 승인 게이트

## 어디에 적용하는가

Airflow 코드 배포, DAG 일시정지 해제, 파이프라인 시작·중지, 수동 실행, backfill,
재시도, 상태 강제 변경, `dbt`·Trino·D1·R2 쓰기는 승인된 전환 경계를 거쳐야 한다.

처음 전환할 때는 모든 상태 변경에 사용자 사전 승인이 필요하다. 첫 전환이 끝난 뒤에는
보호된 같은 저장소의 `dev → main` 병합과 정확한 병합 SHA의 `CI` 성공이 해당 SHA 배포의
증거가 된다. GitHub Release나 tag를 만들 필요는 없다.

## 승인 전 허용 범위

승인 전에는 비밀값 없는 저장소 검사와 읽기 전용 조회만 한다.

```powershell
./tools/verify_repository.ps1
```

최초 보고를 만들 때에만 다음을 가린 값으로 읽는다.

- 배포 대상 형식과 안전한 지문
- `docker compose config --services`, `docker compose ps`
- Airflow 버전·도움말, DAG 목록과 정확한 열 개 DAG의 일시정지 상태
- 허용된 기록의 실행 중·대기 중 개수
- 기준선과 직전 성공 커밋·checksum 요약
- Airflow 명령 기능 지문
- self-hosted runner의 Python `3.11`과 PyYAML 기능 확인

절대경로, 로컬 IP, 비밀값 이름, token, `.env` 값, 원문 inspect 결과는 보고하지 않는다.

## 자동 배포 기본값

최초 승인 전에는 production runner를 시작하지 않는다. `guarded_private`는 비공개·단일
소유자 저장소에서만 쓰는 사고 방지 모드이고, `protected`는 GitHub 보호 규칙을 실제로
다시 읽어 확인하는 더 강한 모드다. 저장소가 공개됐거나 작성자가 늘면 guarded를 중단하고
protected 또는 저장소 밖의 신뢰된 제어기로 바꾼다.

승인 전에는 `WEATHER_DEPLOYMENT_ENABLED`를 비워 두고
`[self-hosted, windows, weather-prod]` runner를 offline으로 둔다. 보호 규칙, 필수 검사,
읽기 권한, 대상 계약이 바뀌면 먼저 배포 플래그를 끈다.

protected 모드의 `WEATHER_GOVERNANCE_READ_TOKEN`은 한 저장소에만 읽기 권한을 주는
fine-grained token이다. `Administration`, `Actions`, `Checks`, `Contents`,
`Pull requests`의 read만 허용하고 값을 읽거나 출력하지 않는다. guarded 모드에는 이
secret을 만들지 않는다.

## 최초 전환 보고와 중단 지점

실제 변경 전에 [main 자동 배포 최초 전환 절차](./main-auto-deploy-first-cutover.md)의
다음 내용을 보고하고 중단한다.

1. 대상 `main` SHA와 바뀌는 네 Airflow 코드 서비스
2. 열 개 Weather DAG의 일시정지 상태, 실행 중·대기 중 작업 수
3. 대상·명령·기준선의 가린 지문과 runner 기능 확인
4. 기준선 준비 → runner 준비 → 병합 검증 → 상태 보존 → 일시정지·비우기 → 배포·health →
   상태 복원 순서
5. 되돌리기 성공과 실패 시 열 개 DAG를 모두 멈춤 상태로 두는 동작
6. 전체 stack을 중단하지 않으며 데이터 쓰기가 0이라는 확인

승인 전에는 Docker `up`, DAG 변경, pipeline 변경, 기준선 설치, runner 설치·시작,
secret/variable 쓰기를 실행하지 않는다.

## 승인 후 최초 전환

승인 뒤에만 기준선과 배포 대상을 설치하고 설정·모의 실행·되돌리기 연습을 한다.
그 다음 runner의 Python `3.11`/PyYAML 기능을 확인하고, 배포 플래그를 정확히
`enabled`로 읽은 뒤 같은 저장소의 `dev → main` 병합과 성공한 `CI`를 재확인한다.

배포기가 기존 Weather DAG 열 개의 상태를 먼저 저장하고, 정확히 그 열 개만 멈춘 뒤
실행 중·대기 중 writer가 0이 될 때까지 제한된 시간만 기다린다. 시간 초과면 상태를
복원하고 배포하지 않는다.

배포 대상은 `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`,
`airflow-triggerer` 네 개뿐이다. `airflow-init`, Postgres, Trino, Marquez와 전체 stack,
`docker compose down`, `restart`, `--force-recreate`는 대상이 아니다.

성공하면 원래 활성화였던 DAG만 다시 활성화하고, 원래 멈춰 있던 DAG는 그대로 둔다.
실패하면 연습한 기준선을 복원하고, 되돌리기마저 실패하면 열 개 DAG를 모두 멈춤 상태로
유지한다.

## 전환 후 운영 규칙

매 배포마다 같은 저장소의 정확한 `dev → main` 병합, 해당 SHA의 `CI`, 대상 지문,
기준선 기록, 보호 규칙 확인을 다시 한다. 다른 branch·fork·오래된 SHA·필수 검사 누락은
변경 전에 안전하게 거부한다. workflow 안에서 `pip install`을 하지 않으며, runner의
의존성 차이는 다음 승인된 정비에서 고친다.
