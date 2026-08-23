# Lesson Run — 2026-08-23 Weather Pipeline Incident

## 한 줄 결론

"컨테이너가 healthy"는 데이터 제품이 healthy라는 뜻이 아니다. 이번 사례에서는
container health와 DAG import는 정상이었지만, 단일 Trino slot의 장시간 maintenance,
Airflow 내부 토큰 만료, R2 DNS 단절이 합쳐져 freshness와 D1 publication까지 영향을
주었다.

## 내가 따라갈 조사 순서

### 1. 증상과 원인을 분리한다

먼저 최근 24시간의 DAG run과 task instance를 `dag_id`, `task_id`, `state`, `pool`,
`start_date`, `end_date`로 본다. 실패 task를 바로 고치지 않고 upstream/downstream
관계를 그린다.

```text
maintenance / execution-token / R2 DNS
               │
               ▼
Raw·Bronze·Gold freshness 지연
               │
               ├── serving snapshot stale
               ├── D1 smoke test fails closed
               └── freshness watchdog alerts
```

watchdog의 실패가 보이면 "watchdog을 고칠" 것이 아니라, 마지막 성공 publication과
upstream slot 점유를 먼저 확인한다.

### 2. 시간 축에서 경쟁을 증명한다

pool 이름만 보고 동시성 문제라고 추측하지 않는다. 실제 Airflow metadata에서
`start_date/end_date`를 비교한다.

- 모든 Weather Trino mutation은 1-slot `trino_weather_heavy`를 사용한다.
- long-running task 하나가 slot을 점유하면 뒤의 task는 OOM 없이 기다릴 수 있다.
- 그러나 waiting은 freshness SLO를 넘길 수 있으므로, maintenance에는 서비스보다 작은
  priority와 짧은 execution bound가 필요하다.

여기서 얻은 교훈은 **OOM 방지용 serialization만으로는 product freshness를 보장하지
않는다**는 것이다. resource isolation에는 queueing policy와 deadline도 필요하다.

### 3. Airflow 3의 두 JWT를 혼동하지 않는다

다음 두 설정은 이름이 비슷하지만 목적이 다르다.

| 설정 | 용도 | 이번 영향 |
|---|---|---|
| `[api_auth] jwt_expiration_time` | UI/public REST API | 원인 아님 |
| `[execution_api] jwt_expiration_time` | worker task의 state/XCom/heartbeat | dbt subprocess 완료 후 보고 실패 원인 |

장시간 subprocess가 실행되는 동안 Execution API 요청이 없으면 token reissue가 일어나지
않을 수 있다. 그러므로 dbt phase의 worst-case runtime보다 긴, 그러나 무한대는 아닌
TTL을 운영 구성에 명시해야 한다.

### 4. retry는 쓰기 의미론 뒤에 둔다

R2 DNS failure는 외부 일시 장애다. 하지만 retry가 안전한 이유는 DNS가 아니라
**conditional immutable write** 때문이다.

- 같은 key의 다른 payload를 overwrite할 수 있으면 retry하지 않는다.
- `If-None-Match: *`와 payload checksum이 있으면 같은 raw/checkpoint의 재시도는
  idempotent하다.
- permission/validation/completeness 오류는 retryable transport error가 아니다.

따라서 retry policy는 API의 편의 기능이 아니라 data contract의 일부로 테스트한다.

### 5. fail-closed downstream을 제거하지 않는다

D1 publish가 `quality_freshness_stale`로 막힌 것은 실패가 아니라 보호다. 이 gate를
끄면 사용자에게 더 오래된 forecast를 새 데이터처럼 제공하게 된다. 올바른 recovery는
upstream Bronze/Gold를 정상화한 뒤 새 publication을 만드는 것이다.

### 6. 테스트 runner도 production compatibility contract다

호스트 Python 3.14에 설치된 별도 Airflow/SQLAlchemy 조합에서는 DAG를 subprocess로
import하는 두 테스트가 `MappedAnnotationError`로 시작조차 못 했다. 이는 변경 코드의
실패가 아니라 runner dependency가 production Airflow 3.2.2 image와 다르다는 뜻이다.

- unit test는 host에서 실행하되, Airflow DAG import와 pool 해석은 실제 scheduler image에서
  읽기 전용으로 확인한다.
- local host의 Airflow를 임의로 upgrade/downgrade해 통과시키지 않는다. 재현 가능한
  production image의 test target을 CI에 추가하는 것이 올바른 후속 조치다.
- 이번에는 실제 runtime에서 forecast DAG의 guard off/on pool 해석과 전체 DAG import error
  0을 확인했다.

## 이번에 추가한 재발 방지 테스트

- R2 endpoint transport failure가 1초/2초 bounded retry 후 성공하는지
- persistent R2 failure가 무한 retry하지 않는지
- R2 client retry mode, attempt 수, timeout, TCP keepalive가 고정되는지
- observation landing DAG의 short retry가 40분 run deadline을 바꾸지 않는지
- maintenance가 default schedule 없이 explicit opt-in일 때만 실행되는지
- maintenance mutation이 canonical pool, priority 1, 8분 timeout, retry 0을 유지하는지
- local Compose의 Airflow 서비스 모두가 Execution API JWT TTL을 명시하는지
- production Airflow image에서 DAG import와 guard-on/off pool 해석이 가능한지

## 다음 장애에서의 Stop/Go 기준

| 상태 | 판단 | 행동 |
|---|---|---|
| DAG import error, secret/config validation error | Stop | 배포·trigger 금지, configuration 복구 |
| R2/KMA의 짧은 transport error, idempotent retry 후 success | Observe | retry 횟수와 cycle duration 기록 |
| freshness watchdog failure | Stop serving promotion | 마지막 successful publication과 upstream pool/blocker 확인 |
| maintenance가 heavy slot을 장시간 점유 | Stop maintenance | task log와 Trino query state 보존, serving 회복 우선 |
| D1 smoke test failure due freshness | Do not bypass | Gold freshness 회복 뒤 정상 export 재시도 |

## 아직 하지 않은 것

이 기록과 코드 수정은 로컬 working tree에만 있다. Docker 재생성, Airflow 재시작,
DAG unpause/trigger/backfill, R2/Trino/D1 write는 수행하지 않았다. 운영 반영은
`README.md`의 Airflow 배포 gate와 별도 승인 절차를 따른다.
