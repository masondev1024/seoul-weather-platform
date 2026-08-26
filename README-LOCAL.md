# 개인 노트북에서 날씨 파이프라인 실행하기

이 문서는 개인 Cloudflare R2·D1을 바라보는 로컬 실행 방법을 설명한다. 실제 값이 들어간
`weather-platform.prod.env`는 Git, 클라우드 동기화 폴더, 메신저에 올리지 않는다.

## 준비 파일

- `weather-platform.prod.env` — 개인 R2, Data Catalog, D1, Worker, Airflow 값
- `docker-compose.yml` — 공통 서비스 정의
- `docker-compose.local.yml` — 노트북용 메모리·health·OpenLineage 설정
- `Dockerfile.airflow` — 로컬 Airflow 이미지 빌드 파일
- `dags/`, `dbt/` — 현재 Weather 파이프라인 코드
- `trino/`, `scripts/` — Iceberg/R2 설정과 안전한 실행 보조 도구

개인 실행에서는 Marquez와 OpenLineage를 사용하지 않는다. 과거 구성과의 호환성을 위해
파일 일부는 남아 있지만 local 설정에서 꺼져 있다. 파일 출처와 변경 이력은
`provenance/`에 남긴다.

또한 local 설정은 `AIRFLOW__CORE__DAG_IGNORE_FILE_SYNTAX=glob`을 명시한다. 이 값은
`.airflowignore` 패턴과 맞지 않는 테스트 파일이 DAG로 잘못 읽히는 일을 막는다.

## 구성만 먼저 검사하기

자격증명이나 컨테이너를 건드리지 않고 Compose 합성 결과만 확인한다.

```bash
ASK_SEOUL_PROD_ENV_FILE=.env.example docker compose \
  --env-file .env.example \
  -f docker-compose.yml \
  -f docker-compose.local.yml \
  config --quiet
```

## 처음 실행할 때

Docker Desktop을 켠 뒤 저장소 루트에서 실행한다.

```bash
export ASK_SEOUL_PROD_ENV_FILE="$PWD/weather-platform.prod.env"

docker compose \
  --env-file "$ASK_SEOUL_PROD_ENV_FILE" \
  -f docker-compose.yml \
  -f docker-compose.local.yml \
  config --services

docker compose \
  --env-file "$ASK_SEOUL_PROD_ENV_FILE" \
  -f docker-compose.yml \
  -f docker-compose.local.yml \
  up -d --build

docker compose \
  --env-file "$ASK_SEOUL_PROD_ENV_FILE" \
  -f docker-compose.yml \
  -f docker-compose.local.yml \
  ps
```

처음 만드는 Airflow 메타데이터 DB에서는 DAG가 일시정지 상태로 시작한다. 개인 R2·D1을
가리키는지 읽기 전용으로 확인한 뒤 Weather DAG만 따로 활성화한다. Traffic 등 다른
영역의 DAG는 건드리지 않는다.

## 실황 DAG는 별도 승인 대상

`weather_ultra_srt_ncst_bronze`는 한 시간 주기의 실황 수집이다. 코드가 있어도 기본
schedule은 비어 있고 `ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED=false`가 기본값이다.
전체 Weather DAG를 한꺼번에 활성화하지 않는다.

다음 순서를 모두 확인한 뒤 별도 승인을 받는다.

1. 80개 격자 수동 canary가 성공하는지 확인한다.
2. `kma_api_requests` 대기열이 한 자리인지 확인한다.
3. 공유 시도 기록과 누락 슬롯 복구 기록을 초기화·검사한다.
4. 한 주기 실행 시간이 1시간보다 짧은지 확인한다.

정확한 사전 점검 순서는 `docs/operations/kma-observation-predeployment-plan.md`를 따른다.

## 다른 컴퓨터에서 가져오지 않는 것

다른 호스트의 Docker volume, Trino 캐시, Postgres 메타데이터, Airflow 로그는 이관하지
않는다. 새 호스트에서는 새 volume으로 시작하고, 운영 데이터만 환경 파일이 가리키는
개인 Cloudflare 저장소에서 읽는다.

## 노트북에 맞춘 자원 제한

local 설정은 운영 이미지 대신 `Dockerfile.airflow`로 이미지를 만든다. 첫 실행에는
기본 이미지와 Python 의존성 다운로드 시간이 필요하다.

- Trino 컨테이너: 5 GiB
- JVM heap: 약 2.75 GiB
- 동시에 처리하는 쿼리: 1개
- 대기열: 최대 10개
- 파일 시스템 캐시: 최근 Iceberg 파일 재읽기 완화용
- `SELECT 1` 대신 `/v1/info` HTTP liveness 확인

처음 켠 뒤 Trino 유휴 메모리 3회와 작은 읽기 전용 쿼리를 측정한다. Trino가 5 GiB의
65%를 넘거나 전체 컨테이너가 Docker 메모리의 80%를 넘으면 DAG를 활성화하지 않는다.
캐시는 반복 읽기를 줄일 뿐이며, 날짜 파티션 조건과 증분 계산을 대신하지 않는다.

Compose 내부 호환성을 위해 프로젝트명 `seoul-weather-platform-mac`과 네트워크명
`seoul-weather-platform-mac-net`은 유지한다. 이름을 바꾸려면 별도 상태 이관이 필요하다.

## 노트북을 다시 켰을 때

현재 자동 복구 코드는 **계획과 사전 점검만** 한다.

- `tools/weather_startup_preflight.py` — Docker, Compose, 필수 서비스, Trino liveness,
  Mac 메모리 예산을 읽고 비밀값을 가린 JSON 증거를 남긴다.
- `tools/weather_recovery_coordinator.py` — 누락 슬롯과 원본 기록을 보고 복구 계획만 만든다.
- `dags/common/recovery/lease.py` — 중복 작업을 막는 작업 소유권 계약이다.
- `dags/common/recovery/dispatch.py` — 검증된 원본 기록을 기존 backfill/recollect 설정으로
  바꾸지만 실제 Airflow 실행 요청은 보내지 않는다.
- `scripts/weather_startup.sh` — 기본은 사전 점검만 한다. `--start`와
  `WEATHER_STARTUP_AUTOSTART=enabled`를 함께 줘야 기존 이미지로 서비스만 시작한다.
- `runtime/launchd/com.mason.seoul-weather-platform.plist.example` — 설치 예시이며
  기본값은 `WEATHER_STARTUP_AUTOSTART=disabled`다.

실제 backfill, API 재수집, R2·Iceberg·D1 쓰기, launchd 설치·활성화는 아직 열지 않았다.
계획을 세 번 연속 관측하고, 원본 재처리 canary와 중복 방지 검사를 통과한 뒤 별도 승인을
받아 단계적으로 연다.

## 승인 경계

저장소 검사만으로는 이미지 빌드, 서비스 재시작, DAG 실행·활성화, R2·D1 쓰기를 허용하지
않는다. 모든 변경은 `AGENTS.md`의 사전 보고와 사용자 승인을 먼저 거친다.
