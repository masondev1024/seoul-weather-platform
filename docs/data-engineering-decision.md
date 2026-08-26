# 데이터 플랫폼 설계 결정 기록

이 문서는 개인 Seoul Weather Platform을 운영하면서 **왜 지금의 설계를 골랐는지**를
남긴다. 다음 장애가 나도 같은 증거를 모으고 같은 기준으로 판단할 수 있게 하는 것이
목적이다. 자격증명, 계정 번호, 실제 객체 경로, Airflow 기록 원문은 넣지 않는다.

## 2026-08-23 — 장애의 전파를 분리한다

### 확인한 사실

| 관찰 | 근거 | 분류 |
|---|---|---|
| scheduler, API server, Trino, Postgres가 살아 있고 DAG 읽기 오류가 없었다. | 컨테이너 상태와 `airflow dags list-import-errors` | 제어 영역 정상 |
| 주간 `weather_iceberg_maintenance`가 Trino 한 자리를 약 10시간 차지했다. | Airflow 작업 시작·종료 시각과 대기열 | 용량·일정 설계 문제 |
| 긴 `dbt` 단계가 작업 상태를 보고할 때 Execution API 토큰 만료로 403을 받았다. | `Invalid auth token: Signature has expired` 기록 | Airflow 내부 인증 설정 |
| 실황 원본의 R2 조건부 기록이 DNS 해석 실패로 멈췄다. | `EndpointConnectionError`, `NameResolutionError` | 외부 네트워크 일시 장애 |
| 신선도 감시와 D1 발행이 오래된 Gold를 감지하고 실패했다. | 감시 기준과 발행 검사 결과 | 하류 보호 기능 정상 |

마지막 두 실패는 별도의 근본 원인이 아니다. 위 단계가 늦어졌을 때 오래된 자료를
사용자에게 보여 주지 않도록 안전하게 중단한 결과다.

### 결정 1 — 서비스 제공보다 유지보수가 앞서지 않게 한다

`weather_iceberg_maintenance`는 기본 schedule을 없애고
`ASK_SEOUL_WEATHER_MAINTENANCE_DAG_SCHEDULE`에 값이 있을 때만 실행한다. 개인 노트북의
Trino는 한 번에 쿼리 하나만 처리하므로 전체 테이블 정리가 수집·Gold 갱신·D1 발행을
막을 수 있다.

- 유지보수도 `trino_weather_heavy` 한 자리 대기열을 함께 쓴다.
- 각 변경 작업은 8분 실행 제한, 우선순위 1, 자동 재시도 0이다.
- 한 테이블 안의 `OPTIMIZE → EXPIRE_SNAPSHOTS → REMOVE_ORPHAN_FILES` 순서는 유지한다.
- 신선도와 대기열이 정상인 시간에 사람 승인을 받아서만 실행한다.

전용 대기열을 따로 만들어 동시에 돌리는 방법은 버렸다. Trino 자체가 한 자리라서
메모리 문제를 없애지 못하고 대기만 숨기기 때문이다.

### 결정 2 — 두 종류의 Airflow 토큰을 구분한다

Airflow 3의 `[execution_api] jwt_expiration_time`은 작업 실행기가 XCom·heartbeat·상태를
보고할 때 쓰는 토큰이다. `[api_auth] jwt_expiration_time`은 화면과 외부 API용이라서
긴 `dbt` 작업 문제를 해결하지 못한다.

개인 Compose의 Airflow 서비스에는 `AIRFLOW__EXECUTION_API__JWT_EXPIRATION_TIME=7200`을
명시했다. 관찰된 긴 단계보다 길지만 무제한은 아니다. 2시간 동안 API와 완전히 끊기면
성공으로 꾸미지 않고 실패시킨다.

### 결정 3 — R2 재시도는 멱등 쓰기 안에서만 허용한다

원본 파일과 주기 마침표는 바꾸지 않거나 `If-None-Match: *` 조건부로 쓴다. 그래서
같은 내용의 재시도는 중복·덮어쓰기를 만들지 않는다.

- boto3 `standard`, 최대 3회 시도, 연결 3초·읽기 30초, TCP keepalive를 고정한다.
- SDK가 모두 실패한 뒤 네트워크 예외에만 1초·2초의 바깥 재시도를 더한다.
- 실황 DAG는 30초 뒤 한 번만 Airflow 재시도를 하며 40분 전체 제한은 그대로 둔다.
- 권한·스키마·완전성·payload 충돌은 재시도하지 않는다.

모든 예외를 재시도하는 방식은 버렸다. 계약 위반이나 잘못된 자격증명을 늦게 발견하고
API 비용만 늘리기 때문이다.

### 결정 4 — Trino 버려진 쿼리 제한을 느슨하게 하지 않는다

Trino `query.client.timeout`은 결과를 읽지 않는 쿼리를 취소하는 회복 장치다. 이번
유지보수 쿼리는 client polling이 멈춘 뒤 `ABANDONED_QUERY`가 됐다.

제한을 늘리면 멈춘 DDL이 메모리와 유일한 쿼리 자리를 더 오래 잡는다. 따라서 유지보수를
기본 수동 실행으로 바꾸고 작업 시간 상한을 둔다. 신선도와 메모리를 우선한 결정이다.

## 반영 전 검사 기준

별도 Airflow 배포 승인이 있을 때만 다음을 수행한다.

1. 새 Compose를 재생성하고 모든 Airflow 서비스에서 `execution_api.jwt_expiration_time=7200`
   을 읽기 전용으로 확인한다.
2. DAG 읽기 오류가 0인지 확인한다.
3. 수집 → Bronze → 변환 → 시점 저장 → D1 발행 → 감시 작업이 세 주기 연속 성공하고
   신선도 기준이 회복되는지 본다.
4. 유지보수 DAG schedule이 `None`인지 확인하고 승인 전에는 실행하지 않는다.
5. 실패하면 기록을 보존하고 원인을 다시 분류한다. 오래된 제품을 내보내기 위해 감시나
   D1 검사를 약하게 만들지 않는다.

## 참고한 공식 자료

- [Apache Airflow 3.2 설정 문서](https://airflow.apache.org/docs/apache-airflow/3.2.1/configurations-ref.html) — Execution API 토큰 설정
- [Apache Airflow JWT 인증](https://airflow.apache.org/docs/apache-airflow/stable/security/jwt_token_authentication.html) — 작업 토큰 갱신
- [Trino 쿼리 관리 설정](https://trino.io/docs/current/admin/properties-query-management.html) — `query.client.timeout`
- [Boto3 재시도 안내](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) — 표준 재시도와 시도 횟수

## 2026-08-26 — 자동 복구는 계획·소유권·실행으로 나눈다

### 새로 확인한 사실

- 노트북과 Docker를 다시 켜도 `catchup=False`인 DAG가 빠진 logical date를 스스로 만들지 않는다.
- “복구 후보를 찾았다”와 “복구 작업을 누가 맡았다”는 서로 다른 기록이어야 한다.
- Airflow 3.2.2 기본 `DAG_IGNORE_FILE_SYNTAX=regexp`와 snapshot의 glob 패턴이 충돌하면
  테스트 파일이 DAG로 읽혀 중복·가져오기 오류가 난다.

### 결정 5 — 첫 단계는 쓰지 않는 계획기와 사전 점검이다

`dags/common/recovery/planner.py`는 원본 재처리를 API 재수집보다 먼저 고르고,
24시간 경과·부분 자료·명백한 오류는 안전하게 막는다. `weather_recovery_coordinator`는
생성 시 멈춤, schedule 없음, `catchup=False`다. `tools/weather_startup_preflight.py`는
Docker·Compose·서비스·Trino·Mac 메모리만 읽고 비밀값을 가린 JSON 증거를 남긴다.

### 결정 6 — 작업 소유권은 원자적인 비교·교환 뒤에 둔다

같은 `job_key`를 두 번 실행하지 않도록 `RecoveryLeaseRegistry`를 사용한다. 만료된
소유권을 넘길 때는 이전 소유자가 더 이상 쓸 수 없도록 `owner`와 `lease_id`를 함께
확인한다. 메모리용 구현은 계약 검사 전용이고, 운영용 `PostgresLeaseBackend`는
Airflow metadata DB에서 만들기·인계·갱신·종료를 한 트랜잭션으로 처리한다.

테이블 생성은 모듈 import나 생성자에서 몰래 하지 않고 별도 승인된 migration에서만 한다.

### 결정 7 — 실행 요청은 컴파일러가 만들고 실행기는 따로 연다

`dispatch.py`는 검증된 원본 manifest를 기존 backfill DAG의 `conf`로, 과거 후보를
recollect DAG의 `base_date/base_time`으로 바꾼다. manifest와 완전성 증거가 없으면
만들지 않는다. Airflow API를 직접 부르지 않아 계획과 실행의 경계를 검사할 수 있다.

### 결정 8 — local 설정에서 파일 무시 문법을 고정한다

원본 snapshot을 고치지 않고 `docker-compose.local.yml`에서
`AIRFLOW__CORE__DAG_IGNORE_FILE_SYNTAX=glob`을 명시한다. 개인 실행 환경에서만 필요한
호환성을 선언해 출처 경계를 지킨다.

### 결정 9 — 실행 직전에 활성 작업과 대기열을 다시 읽는다

소유권을 얻었다고 바로 실행 요청을 보내지 않는다. `airflow_snapshot.py`가 Airflow
DagRun과 `Pool.slots_stats()`를 읽고, `admission.py`가 정상 Weather 실행·대기 작업·
KMA/Trino 대기열을 확인한다. 기본은 한 번에 한 복구 작업이며, 대기열이 차 있으면
`defer`한다. 이중 확인으로 재기동 순간의 경합과 저사양 노트북의 무한 대기를 줄인다.

### 결정 10 — Trino 상태 확인과 데이터 준비 확인을 나눈다

`SELECT 1` healthcheck가 무거운 쿼리 뒤에서 대기하면 Trino가 살아 있어도 Docker가
비정상으로 보일 수 있다. local 설정은 `/v1/info` HTTP liveness를 사용하고,
데이터 준비 여부와 메모리 예산은 `weather_startup_preflight.py`가 별도로 판단한다.

## 현재 진행 상태

- **완료:** 계획기·작업 소유권·실행 전 승인·실행 요청 컴파일, 비밀값 없는 사전 점검,
  DAG 가져오기 검사, 단위·회귀 검사
- **아직 안 함:** 실제 Postgres 표 생성, Airflow 실행 요청 실행기, launchd 설치·활성화,
  R2/Iceberg/D1 복구 쓰기

실제 운영으로 넘어가기 전에는 쓰지 않는 계획을 세 번 연속 관측하고, 중복 실행·소유권
만료·원본 재처리 0 API 호출·Bronze hash 일치를 증명해야 한다.
