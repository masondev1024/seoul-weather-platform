# Weather 로컬 실행 패키지

개인 Cloudflare R2/D1을 대상으로 노트북에서 Weather 파이프라인을 운영하는 구성이다. `weather-platform.prod.env`에는 운영 자격증명이 들어 있으므로 Git, 클라우드 동기화 폴더, 메신저에 올리지 않는다.

## 포함된 실행 필수 항목

- `weather-platform.prod.env` — 개인 R2, Data Catalog, D1, Worker/API, Airflow 운영 환경값
- `docker-compose.yml`, `docker-compose.local.yml` — 공통 정의와 로컬 운영 override
- `Dockerfile.airflow` — Airflow 이미지를 로컬 빌드할 Dockerfile
- `dags/`, `dbt/` — 현재 배포 중인 Weather 파이프라인 소스 스냅샷
- `trino/`, `scripts/` — Trino Iceberg/R2 설정과 안전한 DAG 트리거 스크립트

Marquez는 개인 런타임에서 사용하지 않는다. Compose 정의는 과거 구성 호환성을 위해
프로파일 뒤에 남아 있지만, 로컬 실행 명령은 해당 프로파일을 활성화하지 않으며 Airflow와
dbt의 OpenLineage 방출도 `docker-compose.local.yml`에서 명시적으로 끈다.

실제 자격증명이나 컨테이너 기동 없이 공개 구성을 검증하려면 다음을 실행한다.

```bash
ASK_SEOUL_PROD_ENV_FILE=.env.example docker compose \
  --env-file .env.example \
  -f docker-compose.yml \
  -f docker-compose.local.yml \
  config --quiet
```

## 로컬에서 처음 실행

아래 명령은 이미지 빌드와 서비스를 가동하므로 운영 승인을 받은 뒤에만 실행한다. Docker Desktop을 실행한 뒤 저장소 루트에서 진행한다.

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

새 Airflow 메타데이터 DB에서는 DAG를 정지 상태로 생성한다. 개인 R2/D1 대상과 실행 중인 작업을 확인한 뒤 승인된 Weather DAG만 개별적으로 활성화한다. 기존 메타데이터를 사용하면 이전 활성 상태가 유지될 수 있으며, 다른 도메인의 DAG는 변경하지 않는다.

시간별 실황 DAG `weather_ultra_srt_ncst_bronze`의 설정 예제는 공유 호출 제한이
꺼져 있고 스케줄이 비어 있지만, 현재 `docker-compose.local.yml`은
`ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED=true`와 매시 45분 스케줄
`ASK_SEOUL_KMA_OBSERVATION_DAG_SCHEDULE="45 * * * *"`를 정의한다.
스케줄 정의와 실제 DAG 활성 상태는 다르므로 전체 Weather DAG를 일괄 활성화하지 않는다.
공유 SQLite 호출 기록 초기화, `kma_api_requests` 1-slot pool 확인, 80개 격자의
제한된 수집 검증과 별도 승인을 거친다. 초기 활성화 절차는
`docs/operations/kma-observation-predeployment-plan.md`를 참고하되 현재 설정은
`docker-compose.local.yml`과 `tools/local_runtime_contract.py`를 기준으로 확인한다.

다른 호스트의 Docker named volume, Trino cache, Postgres 메타데이터, Airflow 로그는 이관하지 않는다. 새 호스트에서는 새 volume으로 시작하며, R2·Iceberg·D1의 운영 데이터는 환경 파일이 가리키는 개인 Cloudflare 저장소를 그대로 사용한다.

`docker-compose.local.yml`은 운영 이미지 digest를 사용하지 않고 현재 `Dockerfile.airflow`로 로컬 이미지를 빌드한다. 따라서 첫 실행에는 base image 및 Python/dbt 의존성 다운로드 시간이 필요하다.

로컬 override의 파일명은 호스트 종류와 무관하게 `docker-compose.local.yml`이다. Compose
프로젝트명 `seoul-weather-platform-mac`과 네트워크명 `seoul-weather-platform-mac-net`은
현재 컨테이너와 named volume의 상태 호환성을 위해 유지한다. 이 내부 식별자 변경은 별도의
상태 마이그레이션으로 진행해야 한다. Trino는 5GiB
컨테이너, 쿼리 2개 동시 실행, 대기열 10개로 제한한다. 현재 Weather 작업 풀은
2개 슬롯이며, 일반 변환 분기는 1개 슬롯을 사용하고 다른 쓰기 작업과 겹치면 안 되는
작업은 2개 슬롯을 요청한다. 메모리와 동시 실행 수의 현재 기준은
`docker-compose.local.yml`, `trino/resource-groups.json`, `dags/common/pools.py`다. 최초
기동 후에는 idle 메모리 3회와 작은 read-only 쿼리를 측정하고, Trino가 5GiB의 65% 또는
전체 core stack이 Docker 메모리의 80%를 넘으면 DAG를 활성화하지 않는다.

저장소 검증만으로는 Airflow 이미지 build, 서비스 재시작, DAG 활성화·트리거, R2/D1
write를 허용하지 않는다. `docs/operations/airflow-deployment-approval.md`의 배포
사전 보고와 운영 승인을 먼저 거친다.
