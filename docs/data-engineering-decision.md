# Data Engineering Decisions

이 문서는 개인 Seoul Weather Platform에서 내린 운영 설계 선택을 기록한다. 목표는
"이번에는 통과"가 아니라, 다음 장애에서 같은 증거를 다시 얻고 같은 안전한 결정을
내릴 수 있게 하는 것이다. 자격증명, 계정 식별자, 실제 객체 키, Airflow metadata와
실행 로그 원문은 넣지 않는다.

## 2026-08-23 — Weather 운영 장애의 실패 전파를 분리한다

### 관찰된 사실

2026-08-23의 Airflow metadata와 task log를 읽기 전용으로 조사했다.

| 관찰 | 근거 | 분류 |
|---|---|---|
| Airflow scheduler, API server, Trino, Postgres는 healthy였고 DAG import error는 없었다. | 컨테이너 health 및 `airflow dags list-import-errors` | 제어 plane 정상 |
| 주간 `weather_iceberg_maintenance`가 단일 Weather Trino slot을 약 10시간 점유했다. | Airflow task instance의 pool/start/end 시간 | 용량·스케줄 설계 결함 |
| 장시간 dbt phase가 XCom/task state를 보고할 때 Execution API JWT 만료로 403을 받았다. | task log의 `Invalid auth token: Signature has expired` | Airflow 내부 인증 설정 결함 |
| 실황 Raw landing의 R2 conditional checkpoint write가 DNS resolution 실패로 중단됐다. | `EndpointConnectionError`와 `NameResolutionError` | 외부/네트워크 일시 장애 |
| freshness watchdog과 D1 publish는 stale Gold를 감지하고 실패했다. | watchdog SLO, publication smoke test 결과 | 하류 보호장치 정상 작동 |

따라서 watchdog과 D1 publish의 실패는 별도 근본 원인이 아니다. upstream freshness가
깨졌을 때 stale 데이터를 서빙하지 않도록 fail-closed한 결과다.

### 결정 1 — Serving path보다 maintenance를 우선하지 않는다

`weather_iceberg_maintenance`는 기본 schedule을 없애고, 명시적인
`ASK_SEOUL_WEATHER_MAINTENANCE_DAG_SCHEDULE`이 있을 때만 스케줄된다. 개인 노트북의
단일 Trino는 resource group concurrency가 1이므로, full-table compaction은 신선한
수집·Gold refresh·D1 publication을 막을 수 있다.

- maintenance는 같은 canonical `trino_weather_heavy` pool을 쓴다.
- 각 mutation은 8분 `execution_timeout`, priority 1, 자동 retry 0을 갖는다. 이는
  stalled action의 상한이며 Trino client timeout을 대체하거나 연장하지 않는다.
- `OPTIMIZE → EXPIRE_SNAPSHOTS → REMOVE_ORPHAN_FILES`의 같은 테이블 내 순서는
  보존한다. mutation 재시도나 순서 변경은 데이터 안전성 위험이 더 크다.
- maintenance가 필요하면 ingestion/serving freshness가 정상이고 queued heavy task가
  없는 유지보수 창에 사람의 승인으로 실행한다.

**기각한 대안:** maintenance 전용 pool을 추가해 동시 실행시키는 방식. 노트북 Trino의
메모리 상한에서는 queue 지연을 OOM/abort로 바꾸기만 한다.

### 결정 2 — Execution API JWT와 public API JWT를 구분한다

Airflow 3의 `[execution_api] jwt_expiration_time`은 task runner가 내부 Execution API에
XCom, heartbeat, state를 보고할 때 쓰는 토큰이다. UI/public API의
`[api_auth] jwt_expiration_time`을 늘려도 장시간 dbt subprocess 문제를 해결하지 못한다.

개인 Compose의 모든 Airflow service는
`AIRFLOW__EXECUTION_API__JWT_EXPIRATION_TIME=7200`을 명시한다.

- 2시간은 관찰된 장시간 dbt phase보다 충분히 길고, 무제한 토큰보다 짧다.
- 정상 task는 더 자주 API를 호출해 token refresh middleware를 사용한다.
- task가 2시간 동안 API와 완전히 단절되면 성공으로 위장하지 않고 실패한다.

**기각한 대안:** public/UI JWT만 늘리는 방식. 이는 다른 토큰의 lifetime을 바꾸므로
원인과 효과가 맞지 않는다.

### 결정 3 — R2 retry는 idempotency 경계 안에서만 추가한다

Raw object와 cycle deadline checkpoint는 immutable 또는 `If-None-Match: *` conditional
write다. 이 경계 안에서는 같은 payload 재시도가 중복·덮어쓰기를 만들지 않는다.

- boto3 client는 `standard`, 총 3회 attempt, connect 3초/read 30초, TCP keepalive를
  명시한다.
- SDK attempt가 모두 실패한 뒤에는 transport 예외에만 1초, 2초의 bounded outer retry를
  추가한다.
- KMA observation Raw task는 30초 후 단 한 번 Airflow retry한다. 40분 DAG deadline은
  그대로 hard upper bound다.
- permission, schema, completeness, payload conflict 오류는 retry하지 않는다.

**기각한 대안:** 모든 예외를 task retry하는 방식. 계약 위반과 자격증명 오류를 늦게
실패시키고 API 호출 비용만 증가시킨다.

### 결정 4 — Trino abandoned-query timeout을 느슨하게 하지 않는다

Trino의 `query.client.timeout` 기본값은 client가 결과를 읽지 않으면 query를 취소하는
회복 장치다. 이번 maintenance query는 client polling이 멈춘 뒤 `ABANDONED_QUERY`로
종료됐다.

그 값을 크게 늘리면 stalled DDL이 더 오래 메모리와 유일한 query slot을 점유한다.
따라서 timeout을 늘리는 대신 maintenance를 manual-by-default로 바꾸고 action runtime을
상한한다. 이 선택은 OOM 회피와 serving freshness를 우선한다.

## 운영 반영 전 검증 기준

다음은 별도 Airflow 배포 승인 후에만 수행한다.

1. 새 Compose 설정으로 code service를 재생성하고 모든 Airflow container에서
   `execution_api.jwt_expiration_time=7200`을 읽기 전용으로 확인한다.
2. DAG import error가 0인지 확인한다.
3. 수집 → Bronze → transform → serving snapshot → D1 export → watchdog의 연속 세 cycle이
   success인지, freshness SLO가 회복되는지 확인한다.
4. maintenance DAG는 schedule이 `None`인지 확인하고, 별도 유지보수 승인 전에는
   trigger하지 않는다.
5. 실패하면 task log와 Airflow metadata를 먼저 보존한 뒤 원인 분류를 반복한다. stale
   product를 publish하기 위해 watchdog/D1 smoke test를 완화하지 않는다.

## 참고한 1차 문서

- [Apache Airflow 3.2 configuration reference](https://airflow.apache.org/docs/apache-airflow/3.2.1/configurations-ref.html) — Execution API JWT 설정
- [Apache Airflow JWT authentication](https://airflow.apache.org/docs/apache-airflow/stable/security/jwt_token_authentication.html) — task token refresh 동작
- [Trino query management properties](https://trino.io/docs/current/admin/properties-query-management.html) — `query.client.timeout`의 abandoned-query 의미
- [Boto3 retry guide](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) — standard retry와 `total_max_attempts`
