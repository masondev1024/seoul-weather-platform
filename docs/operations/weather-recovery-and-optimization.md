# Weather 파이프라인 장애 분석과 자동 복구 설계

분석 기준일: 2026-08-26 (Asia/Seoul)
범위: 개인 Mac, KMA 수집, Iceberg Bronze/Silver/Gold, D1 제공, Trino 메모리·파일
캐시, Airflow 재시작과 누락 주기 복구
현재 상태: **계획기·사전 점검·작업 소유권·실행 전 판정·실행 요청 변환 완료 / 실제
실행기는 아직 닫힘**

이 문서는 이번 노트북 중단에서 확인한 사실과 재발 방지 설계를 포트폴리오에 남기는
기록이다. 자격증명, 계정 번호, 실제 객체 경로, Airflow 기록 원문은 넣지 않는다.

## 1. 결론

이번 문제를 Trino 하나의 문제로 단정할 수 없다.

1. 노트북이 꺼진 동안 Docker와 Airflow가 멈춰 예약 주기가 빠졌다.
2. 주요 DAG가 `catchup=False`라서 스케줄러는 과거 실행을 자동으로 만들지 않았다.
3. 기존 `backfill`, `recollect`, `collection_slot_reconciliation`은 통제된 수동 경계다.
   누락을 기록할 뿐 수집 DAG를 자동 실행하지 않는다.
4. 복구 중 R2 DNS/시간 오차/시간 초과, Trino `ABANDONED_QUERY`, Airflow 작업
   `SIGKILL(-9)`가 서로 다른 층에서 나타났다.
5. Trino의 Docker OOM이라는 증거는 없었다(`OOMKilled=false`). 호스트 메모리 압박과
   PyIceberg·Arrow 페이지를 한 작업에 오래 보관한 문제가 먼저 드러났다.
6. D1 발행과 최신성 감시의 실패는 오래된 Gold를 제공하지 않기 위한 안전한 중단이었다.
   이 보호 장치를 약하게 만들면 안 된다.

따라서 목표는 단순히 `docker compose up -d`를 자동 실행하는 것이 아니다. 다음 흐름을
한 제어 절차로 만든다.

```text
재기동 → 빠진 주기 찾기 → 복구 가능 여부 판단
      → 제한·멱등 복구 → 최신 시점 저장·D1 발행 → 최신성 확인
```

이번 단계에서는 이 흐름의 판단·경계 계층만 코드로 고정했다. 실제 Airflow 실행 요청,
KMA 재수집, Iceberg/R2/D1 쓰기를 담당하는 실행기는 다음 단계다.

## 2. 조사 방법과 신뢰도

### 직접 읽은 증거

| 영역 | 확인 방법 | 확인한 사실 |
|---|---|---|
| 런타임 | Docker inspect/stats, Airflow 기록, 작업 로그 | Trino `healthy`, `OOMKilled=false`, 자동 재시작, scheduler 작업 `SIGKILL` 뒤 재시도 성공 |
| 수집 | `weather_vilage_fcst_bronze` 실행·작업 상태 | 원본, Bronze 적재·검사·발행의 성공·재시도·소요시간 |
| 복구 | 예보 원본 재처리와 실황 복구 실행 | 완전한 원본 manifest로 API 없이 재적재 가능, 80개 객체·69,680행 성공 |
| 제공 | 시점 저장·발행·최신성 감시 로그 | 최신 발행 성공, 감시 `healthy=true`, 발행 나이와 행 수 확인 |
| 코드 계약 | `dags/domains/weather`, `dags/common/collection_slots`, `dags/common/serving` | `catchup=False`, 일시정지 문턱, 영수증 멱등성, 오래된 자료 차단, 재시도·마감 정책 |
| 검사 | `.venv` Weather 검사 묶음 | `497 passed`, 메모리·재시도·원본 전송 검사 통과 |

### 추론과 아직 모르는 것

- 자동으로 누락 주기가 채워지지 않은 구조적 이유는 `catchup=False`와 자동 복구 조정기의
  부재가 함께 있기 때문이다.
- `SIGKILL(-9)`은 Trino OOM보다 Airflow 작업의 페이지·XCom·Arrow 메모리와 Mac swap
  압박이 합쳐진 결과로 보는 것이 타당하다.
- 앞 단계 변환이 끝나기 전에 시점 저장·D1 발행을 시작하면 새 예보가 바로 보이지 않을 수
  있다. 데이터 손상 대신 안전한 지연을 택한 것이며 최신성 기준에 명시해야 한다.

정확한 중단 시간이 종료인지 sleep인지, KMA 과거 조회 보존 기간·요금·재배포 조건,
Docker 로그인 시작 설정, daemon 준비 p95, 외부 launchd와 Airflow 조정기 중 최종 선택은
추가 읽기 확인이 필요하다.

## 3. 장애 흐름

| 단계 | 증상 | 직접 원인 | 영향 | 판단 |
|---|---|---|---|---|
| 노트북 중단 | 예약 실행 공백 | Docker/Airflow 정지 또는 네트워크 단절 | 관측·예보 주기 누락 | 첫 원인 |
| 재기동 직후 | R2 DNS, timeout, `RequestTimeTooSkewed` | 외부 주소·Docker DNS·시계 불안정 | 원본 마침표·저장 실패 | 일시 장애 |
| 복구 중 | `ABANDONED_QUERY`, 무거운 대기열 | 한 자리 Trino와 긴 쿼리·유지보수 경쟁 | 변환·검사 지연 | 용량·일정 결함 |
| 실황 실행 | 40분 마감 도달 | `trino_weather_heavy` 대기와 적재·검사 경합 | 주기 실패 | 마감 보호 작동 |
| 예보 적재 | `SIGKILL(-9)` 뒤 재시도 성공 | Airflow 작업 메모리 압박과 원본 페이지 중첩 | 첫 시도 실패 | 응용 메모리 압박 |
| 시점 저장 | 오래된 시각 | 앞 Gold가 제때 끝나지 않음 | 새 발행 보류 | 안전 중단 |
| D1/감시 | DNS 또는 최신성 오류 | 앞 단계·Cloudflare 지연 | 오래된 본문 공개 방지 | 보호 정상 |
| 안정화 | Trino 자동 재시작 뒤 정상 | `restart: unless-stopped`, 메모리 회복 | 이후 실행 정상화 | 프로세스만 복구 |

### 3.1 2026-08-26 복구 후 시점 기록

아래는 05:35~05:48 UTC에 읽은 기록이다. 시간이 지나면 최신 실행이 바뀌므로 당시
사고 증거로만 보관한다.

| 경로 | 확인 결과 | 의미 |
|---|---|---|
| 실황 Bronze | 02:45·03:45·04:45 정기 실행 성공 | 재기동 뒤 시간별 세 주기 확인 |
| 예보 Bronze | 05:20 실행 성공(두 번째 시도 포함) | 원본 80개·69,680행, 검사·발행 완료 |
| Gold 변환 | 05:33 자산 연결 실행이 검사 단계 진행 중 | 실패가 아니라 하류 계산 중 |
| 시점 저장 | 05:00 정기 실행 성공 | 직전 저장본 정상 |
| D1 발행 | 05:24 자산 연결 실행 성공 | 공개 네 제품 최신 발행 성공 |
| 최신성 감시 | 05:35 성공, `healthy=true`, 위험 제품 427행 | 발행 나이 9~11분, 기준 안 |
| Trino | `healthy`, `OOMKilled=false`, 약 2.6/5 GiB | 자동 재시작 뒤 안정 |

07:02 UTC 추가 관측에서는 실행 중 시점 쿼리 때문에 예전 SQL healthcheck가 10초
시간 초과를 기록했지만 Trino 프로세스는 살아 있었다. 메모리는 3.49/5 GiB까지 올랐다가
쿼리 종료 뒤 2.79/5 GiB로 내려왔다. 그래서 local Compose에는 `/v1/info` HTTP liveness
검사를 넣었다. 실행 중인 컨테이너는 재생성하지 않았으므로 새 검사가 적용됐다고 주장하지
않는다.

수집·Bronze·감시는 정상이고 새 예보 변환이 진행 중이라는 뜻이다. 새 예보가 D1에 보이는
시점은 변환 완료와 다음 시점 저장·발행 성공 뒤다.

## 4. 근본 원인 분류

### P0 — 누락 주기를 자동으로 만드는 제어 영역이 없다

예보 수집·backfill·recollect와 실황 DAG는 `catchup=False` 또는 `schedule=None`이다.
`collection_slot_reconciliation`도 API를 부르지 않고 예상·결과 영수증만 기록한다.
Compose `restart: unless-stopped`는 프로세스만 되살릴 뿐 빠진 logical date를 만들지 않는다.

그래서 노트북을 다시 켜면 서비스는 정상인데 누락 슬롯은 그대로 빈칸일 수 있다.

### P0 — Trino와 Airflow 작업의 메모리를 따로 관리해야 한다

Trino가 `OOMKilled`되지 않았는데도 작업이 죽은 이유는 원본 manifest의 페이지와 행을
한 작업 메모리에 유지했기 때문이다. 80개 격자는 PyIceberg/Arrow 변환 때 최고점이
커진다. 현재 `bronze_batch.py`는 사전 점검과 저장을 나누고 페이지 묶음을 8개로 제한한다.
재시도하면 같은 `dag_run_id`의 첫 묶음을 지우고 전체 묶음을 다시 만들어 부분 자료를
남기지 않는다.

남은 위험은 XCom에 원본 목록을 전달하는 구조, `dbt`와 scheduler child process의 동시
메모리, 묶음 크기 8을 고정한 점이다.

### P1 — 한 자리 Trino는 OOM을 줄이지만 최신성을 보장하지 않는다

`trino/resource-groups.json`은 `hardConcurrencyLimit=1`, `maxQueued=10`이다.
유지보수·Bronze·변환·시점 저장이 같은 coordinator를 공유하면 대기 시간이 API timeout과
최신성 기준을 함께 잡아먹는다. 예전 `SELECT 1` healthcheck도 대기열에 들어가 프로세스가
살아 있어도 Docker가 비정상으로 보이는 false negative를 만들었다.

### P1 — 외부 오류마다 다른 시계를 쓴다

- R2 `EndpointConnectionError`/DNS: 짧은 전송 재시도
- `RequestTimeTooSkewed`: 재시도보다 시계·Docker 사전 점검
- KMA 429: `Retry-After`, 공유 API 대기열, 하루 실제 시도 기록과 함께 제한
- 스키마·완전성·권한·hash 충돌: 재시도하지 않고 즉시 중단

현재 코드에는 예보 429 `(60, 180, 300)`초, 실황 `(5, 10, 20)`초 backoff·회로·실제
시도 기록, R2 바깥 재시도 `(1, 2)`초가 있다. 하지만 누락 주기를 고르고 남은 예산을
계산하는 자동 조정 단계가 없었다.

### P1 — 하류 보호 기능은 원인을 고치지 않는다

감시 작업은 D1과 품질 정보를 따로 읽어 오래됨·발행 불일치·가용성 불일치를 실패로
낸다. 발행기의 마지막 정상본 보존과 0/부분 자료 차단도 의도된 기능이다.

순서는 **앞 단계 복구 → Gold 계약 검사 → 시점 마침표 → D1 발행 → 감시**다. 감시
재시도를 늘리거나 오래된 자료 문턱을 끄지 않는다.

### P2 — 원하는 설정과 실제 상태가 달라질 수 있다

설정 파일에서 Marquez/OpenLineage를 꺼도 Airflow 기록에는 과거 실행이 남을 수 있다.
Compose, 현재 컨테이너 환경, DAG 상태를 하나의 원하는 상태 증거로 묶어야 재기동 때
이전 설정이 되살아나지 않는다.

### P2 — Marquez는 개인 실행에서 쓰지 않는 설정이다

기존 profile 호환 때문에 정의는 남아 있지만 local에서는 꺼져 있다. 나중에 profile을
완전히 지우거나 “정의는 있으나 꺼져 있음”을 검사하는 계약을 유지한다.

## 5. 이미 효과가 확인된 보호 기능

| 보호 기능 | 구현 위치 | 역할 | 한계 |
|---|---|---|---|
| Compose 재시작 | `docker-compose.yml` | 프로세스·컨테이너 자동 재기동 | 빠진 DAG 실행은 만들지 않음 |
| Airflow 실행 마감 | 예보 60분, 실황 40분 | 멈춘 실행이 다음 주기를 영구 차단하지 않음 | 대기열이 길면 정상 주기도 실패 |
| 멱등 원본·마침표 | `weather_ingest/landing.py`, `runtime.py` | 불변·조건부 쓰기와 재개 | 원천 주소가 사라지면 복구 불가 |
| R2 원본 임시 보관 | `weather_ingest/raw_spool.py` | 짧은 네트워크 단절 때 재전송 비용 절감 | 24시간 보존, 디스크 한도 필요 |
| 제한 재시도 | `weather_ingest/runtime.py`, `kma_observation_http.py` | 429/5xx/DNS를 제한해서 재시도 | 누락 선택기는 없음 |
| 슬롯 영수증 | `common/collection_slots` | 예상·종료 결과를 한 번만 기록 | 자동 복구 상태는 없음 |
| 안전한 제공 문턱 | `common/serving/gate.py`, `watchdog.py` | 오래됨·부분·0건 제품 공개 방지 | 앞 단계를 자동 고치지 않음 |
| Trino spill·파일 캐시 | `trino/config.properties`, `iceberg.properties` | 낮은 메모리 집계와 반복 읽기 절감 | 파티션·증분 계산을 대신하지 않음 |
| 묶음 Bronze 적재 | `weather_ingest/bronze_batch.py` | 페이지 8개씩 처리해 최고 메모리 낮춤 | 사전 점검과 저장에서 페이지를 두 번 읽음 |

## 6. 목표 구조: 재기동 뒤 자동 누락 복구

### 6.1 노트북 시작과 데이터 복구를 분리한다

```text
macOS launchd / Docker Desktop
        │  (프로세스만 시작, 데이터 쓰기 금지)
        ▼
시작 사전 점검
  daemon · 환경 지문 · 시계 · 컨테이너 health · Trino 준비 상태
        │
        ▼
weather_recovery_coordinator (5~10분 주기, max_active_runs=1)
  1. 빠진 슬롯 목록 만들기
  2. 복구 방법·근거 분류
  3. 영구 작업 소유권 + 멱등 키 확보
  4. 제한된 실행 요청 또는 복구 묶음 생성
        │
        ├─ 검증된 R2 manifest → API 없는 Bronze 재처리
        ├─ 예보 원본 없음 → KMA 재수집(호출 한도 안에서)
        ├─ 실황 없음 → 과거 원천 계약 확인 후 요청 또는 복구 불가
        └─ 최신 정상 묶음 → Gold 변환 한 번으로 합치기
                                      │
                                      ▼
                              시점 저장 → D1 → 감시
```

### 6.2 macOS 시작 층

launchd는 다음만 한다.

1. Docker Desktop이 준비될 때까지 최대 5분 기다린다.
2. 저장소 절대경로, Compose project/network 지문을 확인한다.
3. 환경 파일은 존재·권한·checksum만 가린 값으로 기록하고 비밀값은 읽어 출력하지 않는다.
4. 기존 volume을 재사용해 `docker compose ... up -d --no-build`를 실행한다.
5. Postgres/Trino/Airflow health를 기다린 뒤 조정기가 판단하게 한다.
6. 실패하면 재시도 폭주 없이 알림과 `BOOT_BLOCKED`를 남긴다.

launchd에서 `airflow dags trigger`를 반복 호출하지 않는다. daemon이 준비되지 않았거나
두 번 실행될 때 중복 작업을 만들 수 있기 때문이다.

### 6.3 복구 작업의 영구 계약

슬롯 영수증만으로는 “누락을 발견했다”와 “복구를 맡았다”를 구분할 수 없다.

```text
recovery_job_key = domain | source_id | expected_slot_id | strategy | population_revision
state ∈ {planned, leased, running, succeeded, skipped, unrecoverable, blocked}
```

필수 기록은 `job_key`, 슬롯 ID, 방법, 원본 수정본, 계획·소유권 만료 시각, 시도 횟수,
원본 manifest 근거, 남은 API 예산, 마지막 오류, Bronze/Gold/발행 ID, 정책 버전,
판정 이유다.

R2 제어 영역에는 바뀌지 않는 사건을, Airflow Postgres에는 실행 색인과 소유권을 둔다.
소유권은 TTL과 주인을 가지며, 조정기가 죽으면 만료 뒤 인계할 수 있어야 한다. 같은
`job_key`를 두 번 계획해도 복구 실행은 하나만 생겨야 한다. 정상·수동·복구 실행은
출처에서 구분한다.

### 6.4 누락 분류

| 우선순위 | 조건 | 행동 |
|---|---|---|
| 0 | 제공 최신성 기준을 이미 넘김 | 새 API보다 마지막 정상본 유지, 알림 |
| 1 | 완전한 R2 manifest와 payload hash | API 없이 Bronze 재처리 |
| 2 | 원본이 없지만 KMA 과거 조회 범위 안 | `kma_api_requests`와 하루 기록을 먼저 예약한 뒤 재수집 |
| 3 | 실황 과거 원천과 이용 권리 확인 | 별도 실황 복구 규칙 적용 |
| 4 | 보존 기간 밖, manifest 불완전, hash·계약 충돌 | `unrecoverable` 또는 `blocked`, 추정값 금지 |

현재 제공을 회복할 때는 최신 복구 가능 슬롯부터 처리한다. 품질 분석은 제공이 정상으로
돌아온 뒤 오래된 슬롯부터 시간순으로 처리해 누락 구간을 분명히 한다.

### 6.5 복구 예산과 동시성

```text
MAX_RECOVERY_SLOTS_PER_COORDINATOR_RUN = 3
MAX_RECOVERY_FORECAST_API_SLOTS        = 1
MAX_RECOVERY_OBSERVATION_API_SLOTS     = 1
MAX_RECOVERY_WALL_TIME                 = 45 minutes
MAX_RECOVERY_AGE                       = 24 hours
```

KMA의 `Retry-After`·backoff·회로·실제 시도 기록을 그대로 쓰고, API 복구는
`kma_api_requests` 한 자리만 허용한다. Bronze·검사·변환은 실제 Trino 한 자리와 같은
대기열을 사용한다. 별도 Airflow 대기열을 추가해 Trino 동시성을 몰래 늘리지 않는다.
정상 실행과 같은 슬롯이면 정상 실행을 먼저 처리하고, `max_active_runs=1`을 유지한다.

### 6.6 하류 계산을 한 번으로 합치기

누락 슬롯마다 변환·시점 저장·D1 발행을 만들면 같은 계산을 반복한다.

1. 복구로 성공한 Bronze manifest를 모은다.
2. 최신 제공 가능한 Bronze 식별자를 하나 고른다.
3. 영향받은 파티션만 Silver/Gold를 증분 계산한다.
4. Gold 마침표는 제공 시각과 Bronze 식별자가 같을 때만 낸다.
5. 마침표 뒤 한 번만 시점 저장·D1 발행을 한다.
6. 감시가 발행 ID, 최신성, 427개 장소 가용성을 확인한다.

과거 품질 분석과 현재 제공 제품을 같은 D1 발행에 섞지 않는다. 품질 Gold는 R2/Iceberg에
두고 D1에는 현재 공개 네 제품만 둔다.

### 6.7 상태 흐름

```text
BOOTING
  └─> PREFLIGHT_FAILED ──(알림·중지)
  └─> PREFLIGHT_OK
         └─> INVENTORY
                ├─ gap 없음 ──> STEADY
                ├─ 막힘 ─────> DEGRADED (마지막 정상본 유지)
                └─ 복구 가능 ─> LEASED
                                      └─> RUNNING
                                             ├─ 재시도 가능 오류 ─> RETRY_WAIT
                                             ├─ 계약 오류 ───────> BLOCKED
                                             ├─ 성공 ────────────> COALESCE_TRANSFORM
                                             │                    └─> PUBLISH_VERIFY
                                             └─> STEADY/DEGRADED
```

모든 전이는 사건과 지표를 남기며 재시도해도 같은 `job_key`를 유지한다.

## 7. 다음 최적화 순서

| 우선순위 | 작업 | 기대 효과 | 반드시 확인할 위험 |
|---|---|---|---|
| P0 | 복구 조정기와 영구 작업·소유권 | 재기동 뒤 누락을 수동 명령 없이 처리 | 중복 실행, 호출 한도, 오래된 발행 |
| P0 | 시작 사전 점검·시계·원하는 상태 증명 | Docker 준비와 데이터 준비를 구분 | 비밀값 가림, launchd 중복 |
| P0 | Bronze 메모리 수정 정식 반영 | Airflow 작업 강제 종료 감소 | 묶음 크기별 최고 메모리·R2 읽기량 |
| P0 | 실행 ID와 추적 ID 통일 | 원인부터 복구까지 한 번에 추적 | XCom에 원문을 넣지 않음 |
| P1 | health를 liveness/readiness로 분리 | 대기열 때문에 잘못 비정상으로 보이는 현상 제거 | local 설정과 사전 점검 일치 |
| P1 | Airflow 동시성·작업 수 상한 측정 | scheduler 메모리 압박 완화 | 너무 낮아 최신성 초과 |
| P1 | 변환·시점 저장 증분 파티션 | 한 시간 주기 안에 끝낼 가능성 향상 | Iceberg 늦은 자료·시점 정확성 |
| P1 | 대기열·쿼리·재시작·OOM 지표 | 용량 원인을 추측하지 않음 | 지표 수 폭증·비밀값 노출 |
| P1 | 원본 임시 보관 디스크 한도·나이 | 노트북 디스크 고갈 방지 | Bronze 확인 전 삭제 금지 |
| P2 | 파일 캐시 적중·퇴출 관측 | R2 전송과 지연 조정 | 캐시는 정확성 계층이 아님 |
| P2 | Marquez 사용 안 하는 설정 정리 | 메모리와 설정 잡음 감소 | Compose 호환성 |
| P2 | KMA 과거 보존·권리 계약 | 복구 가능 여부를 정확히 판단 | API 신청·정책 확인 |
| P3 | 품질 Gold와 AI 근거 봉투 | 단순 위험 조회를 근거 있는 분석으로 확장 | D1 제공 계약 버전 분리 |

### Trino 원칙

- 현재 5 GiB 컨테이너, `MaxRAMPercentage=55`, 노드별 쿼리 800 MB, heap 여유 1.5 GB,
  동시 쿼리 1개, spill 사용은 노트북에 맞춘 보수값이다.
- `TRINO_TASK_CONCURRENCY`를 올리거나 `query.client.timeout`을 늘리지 않는다. 대기만
  OOM 또는 버려진 쿼리로 바뀔 수 있다.
- 파티션 조건과 증분 모델이 파일 캐시보다 먼저다. 캐시는 같은 파일의 재읽기만 줄인다.
- 유지보수는 기본 꺼짐으로 두고, 대기열이 없고 제공 최신성이 정상인 시간에 승인 실행한다.

### Bronze·HTTP 원칙

- KMA 80개 격자의 논리 호출, 실제 HTTP 시도, 페이지·행 수, 429 재시도를 따로 기록한다.
- 원본 payload를 XCom으로 옮기지 않고 manifest·pointer·hash만 전달한다.
- 사전 점검이 끝나기 전에는 Iceberg 쓰기를 열지 않는다.
- `delete_existing`는 같은 `dag_run_id` 재시도의 멱등 경계다.
- 현재 페이지를 두 번 읽는 방식은 메모리와 R2 읽기량의 교환이다. 다음 단계에서는
  임시 보관 파일 → parser → Arrow 묶음 → Iceberg 추가를 한 흐름으로 연결한다.

## 8. 구현 현황과 다음 단계

### 이번 단계에 구현한 P0 경계

- `dags/common/recovery/planner.py`: 원본 재처리 우선, 과거 API 예산, 오래됨·부분 자료·
  명백한 오류 차단, 결정적인 `job_key`/`plan_id`
- `dags/common/recovery/lease.py`, `postgres.py`: 원자적 작업 소유권, 만료 인계·fencing,
  Airflow metadata DB용 Postgres adapter. 표 생성은 별도 migration에서만 수행
- `dags/common/recovery/airflow_snapshot.py`, `admission.py`: Airflow 실행·대기열을
  읽고 충돌이 없을 때만 실행 요청을 한 번 허용. 잘못된 snapshot이면 거부·연기
- `docker-compose.local.yml`: `SELECT 1` 대신 `/v1/info` HTTP liveness. 준비 상태와
  메모리는 사전 점검에서 따로 판단
- `dags/common/recovery/dispatch.py`: 승인된 원본 재처리·재수집 작업을 기존 DAG `conf`로
  바꾸지만 trigger API는 호출하지 않음
- `dags/domains/weather/weather_recovery_candidates.py`: 슬롯 영수증을 발표 주기 후보로
  묶고 원본 manifest가 맞을 때만 재처리 증거로 승격
- `tools/weather_recovery_coordinator.py`: 입력 JSON을 받아 계획만 출력하는 CLI
- `dags/domains/weather/weather_recovery_coordinator.py`: 기본 schedule 없음·생성 시 일시정지,
  계획만 출력하는 Airflow control DAG
- `tools/weather_startup_preflight.py`, `scripts/weather_startup.sh`: Docker·Compose·필수
  서비스·5 GiB Trino 예산 확인. 명시 opt-in 없이는 시작만 하지 않음
- `runtime/launchd/com.mason.seoul-weather-platform.plist.example`: `RunAtLoad`·`KeepAlive`
  예시, 기본 `WEATHER_STARTUP_AUTOSTART=disabled`

비밀값 없는 검사 결과는 전체 pytest `1133 passed`, 공통 복구 `111 passed`, Weather
`497 passed`, CI 회귀 `947 passed, 1 skipped`, DAG 14개·`import_errors=0`, local runtime
계약 통과다. 실제 사전 점검도 필수 서비스 6개와 Trino 메모리 약 `52.61%`를 통과했고
`mutation_performed=false`였다.

이는 “실행 직전 계획과 차단을 안전하게 계산할 수 있다”는 뜻이지 “백로그 자동 복구가
운영 중”이라는 뜻이 아니다. 실제 Postgres 표, Airflow 실행기, 쓰기 권한, 장애주입을
추가해야 한다.

### 다음 단계

1. **관측만:** 후보 수, 가장 오래된 누락, 원본 근거 비율, 남은 API 예산, 대기 시간,
   Trino 최고 메모리를 지표로 남긴다.
2. **재기동 사전 점검:** Docker/launchd 시작, 대상 지문, 시계·DNS·Trino·Postgres·Airflow
   준비를 확인하고 실패하면 모든 복구 DAG를 계속 멈춘다.
3. **원본 재처리:** 완전한 manifest만 결정적인 작업으로 만들고, API 호출 0건·Bronze
   행/hash 일치·프로세스 중단 후 재개를 증명한다.
4. **재수집 정책:** KMA 과거 조회와 권리를 확인한 뒤에만 호출을 연다. 실황을 억지로
   채우지 말고 복구 불가 누락으로 남긴다.
5. **하류 합치기:** 여러 Bronze 성공을 한 변환으로 묶고 source identity 일치를 확인한다.
6. **장애주입·전환:** Docker 중단, scheduler 종료, Trino 재시작, R2 DNS, 시계 오차,
   KMA 429, 부분 manifest, 중복 조정을 재현한 뒤 처음에는 알림 전용으로 운영한다.

## 9. 자동 복구 완료 기준

- 노트북을 세 시간 껐다 켜도 5분 안에 누락 목록이 생긴다.
- 조정기가 두 개 떠도 같은 작업은 하나만 생긴다.
- 완전한 R2 manifest는 KMA API를 부르지 않고 Bronze hash/행 수가 같다.
- 재수집이 대기열·하루 한도·재시도·주기 마감을 넘지 않는다.
- 프로세스가 죽어도 작업 소유권 만료 뒤 같은 작업을 안전하게 이어 간다.
- 부분·hash·스키마·권한 오류는 재시도 폭주 없이 `blocked`로 남는다.
- 새 Gold 검사와 source identity가 맞기 전에는 마지막 정상 발행본을 유지한다.
- 복구 묶음당 D1 발행은 한 번이고 발행 ID·내용 hash가 맞다.
- Trino `OOMKilled=false`, 무제한 대기열 없음, 해결되지 않은 버려진 쿼리 없음
- 실황 한 주기 p95가 한 시간보다 짧고, 추적 ID로 원본→DAG→Bronze→Gold→D1을 연결한다.
- 로그와 문서에 secret, 서비스 키, 원문 URL query key가 없다.

## 10. 포트폴리오용 요약

저사양 Mac에서 80개 KMA 격자 날씨 레이크하우스를 운영하던 중 노트북 중단으로 예약
주기가 누락되고 R2 네트워크 오류, Trino 대기, Airflow 작업 메모리 압박이 겹쳤다.
단순 재시작은 컨테이너만 살리고 데이터 누락은 복구하지 못했다. 원본을 불변·hash
manifest로 보관하고, 대기열·재시도·마감·멱등성을 함께 제한했다. 이어 계획기, 작업
소유권, 실행 전 판정을 나눠 다음 단계에서 안전한 자동 복구를 열 수 있는 제어 영역을
만들었다.

정직하게 말하면 launchd 설치·활성화, 실제 실행 요청, Postgres migration, R2/Iceberg/D1
복구 쓰기는 아직 하지 않았다. 이 문서는 구현 완료가 아니라 운영 설계와 검증 기준을
기록한 것이다.

## 11. 중단·진행 기준

| 상태 | 판단 | 행동 |
|---|---|---|
| DAG 읽기 오류, 비밀값·설정 검사 오류 | 중단 | 배포·실행 요청 금지, 설정 복구 |
| 짧은 R2/KMA 전송 오류가 제한 재시도 후 성공 | 관찰 | 재시도 횟수와 주기 시간 기록 |
| 최신성 감시 실패 | 발행 중단 | 마지막 정상 발행과 앞 단계 원인 확인 |
| 유지보수가 무거운 자리를 오래 점유 | 유지보수 중단 | 기록과 Trino 상태 보존, 제공 회복 우선 |
| 신선도 때문에 D1 검사 실패 | 우회 금지 | Gold 회복 뒤 정상 발행 재시도 |
| 시계·비밀값·대상 설정이 어긋남 | 모든 쓰기 중단 | 환경 복구와 증거 보존 후 재검토 |

## 12. 관련 파일

- `docker-compose.yml`, `docker-compose.local.yml` — 재시작, 의존성, 노트북 자원 제한
- `README-LOCAL.md` — 개인 실행과 실황 활성화 문턱
- `dags/domains/weather/weather_vilage_fcst_bronze.py` — 예보 일정·마감·재처리 DAG
- `dags/domains/weather/weather_ultra_srt_ncst_bronze.py` — 시간별 실황·80개 격자 완전성
- `dags/domains/weather/weather_vilage_fcst_collection_slot_reconciliation.py` — API 없는 누락 확인
- `dags/common/collection_slots/` — 한 번만 쓰는 예상·결과 영수증
- `dags/domains/weather/weather_ingest/bronze_batch.py` — 묶음 적재 메모리 개선
- `dags/domains/weather/weather_ingest/runtime.py` — KMA/R2 재시도·backoff·호출 예산
- `dags/domains/weather/weather_ingest/kma_observation_http.py` — 실황 429·회로·마감
- `dags/common/serving/gate.py`, `watchdog.py`, `dag_factory.py` — 마지막 정상본·안전 발행
- `trino/config.properties`, `resource-groups.json`, `iceberg.properties` — spill·메모리·동시성·파일 캐시
- `docs/data-engineering-decision.md`, `docs/lessonrun.md` — 결정과 장애 복기
