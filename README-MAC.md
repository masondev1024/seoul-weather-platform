# Weather MacBook 실행 패키지

이 ZIP은 현재 Windows에서 검증한 개인 Cloudflare R2/D1 대상의 Weather 운영 구성이다. `weather-platform.prod.env`에는 운영 자격증명이 들어 있으므로 Git, 클라우드 동기화 폴더, 메신저에 올리지 않는다.

## 포함된 실행 필수 항목

- `weather-platform.prod.env` — 개인 R2, Data Catalog, D1, Worker/API, Airflow 운영 환경값
- `docker-compose.yml`, `docker-compose.prod.yml`, `docker-compose.mac.yml` — MacBook용 Compose 조합
- `Dockerfile.airflow` — Mac에서 Airflow 이미지를 로컬 빌드할 Dockerfile
- `dags/`, `dbt/` — 현재 배포 중인 Weather 파이프라인 소스 스냅샷
- `trino/`, `scripts/` — Trino Iceberg/R2 설정과 안전한 DAG 트리거 스크립트

Marquez는 개인 런타임에서 사용하지 않는다. Compose 정의는 과거 구성 호환성을 위해
프로파일 뒤에 남아 있지만, Mac 실행 명령은 해당 프로파일을 활성화하지 않으며 Airflow와
dbt의 OpenLineage 방출도 `docker-compose.mac.yml`에서 명시적으로 끈다.

## MacBook에서 처음 실행

Docker Desktop을 실행한 뒤, 압축을 푼 폴더에서 아래 명령을 실행한다.

```bash
export ASK_SEOUL_PROD_ENV_FILE="$PWD/weather-platform.prod.env"

docker compose \
  --env-file "$ASK_SEOUL_PROD_ENV_FILE" \
  -f docker-compose.yml \
  -f docker-compose.prod.yml \
  -f docker-compose.mac.yml \
  config --services

docker compose \
  --env-file "$ASK_SEOUL_PROD_ENV_FILE" \
  -f docker-compose.yml \
  -f docker-compose.prod.yml \
  -f docker-compose.mac.yml \
  up -d --build

docker compose \
  --env-file "$ASK_SEOUL_PROD_ENV_FILE" \
  -f docker-compose.yml \
  -f docker-compose.prod.yml \
  -f docker-compose.mac.yml \
  ps
```

처음에는 새 Airflow 메타데이터 DB가 생성되므로 DAG는 pause 상태로 시작한다. 개인 R2/D1 대상이 맞는지 확인한 뒤 Weather DAG family만 unpause한다. Traffic DAG는 건드리지 않는다.

Windows의 Docker named volume, Trino cache, Postgres 메타데이터, Airflow 로그는 이관하지 않는다. MacBook에서 새 volume으로 시작하며, R2·Iceberg·D1의 운영 데이터는 환경 파일이 가리키는 개인 Cloudflare 저장소를 그대로 사용한다.

`docker-compose.mac.yml`은 운영 이미지 digest를 사용하지 않고 현재 `Dockerfile.airflow`로 Mac 로컬 이미지를 빌드한다. 따라서 첫 실행에는 base image 및 Python/dbt 의존성 다운로드 시간이 필요하다.

Mac override의 Compose 프로젝트명은 `seoul-weather-platform-mac`이며, 애플리케이션
네트워크도 `seoul-weather-platform-mac-net`으로 고정해 폐기한 `elt-infra` 런타임과
공유하지 않는다. named volume은 Mac 프로젝트명 아래 새로 생성한다. Trino는 5GiB
컨테이너, 약 2.75GiB JVM heap, 쿼리 1개 동시 실행, 대기열 10개로 제한한다. 최초
기동 후에는 idle 메모리 3회와 작은 read-only 쿼리를 측정하고, Trino가 5GiB의 65% 또는
전체 core stack이 Docker 메모리의 80%를 넘으면 DAG를 활성화하지 않는다.

저장소 검증만으로는 Airflow 이미지 build, 서비스 재시작, DAG 활성화·트리거, R2/D1
write를 허용하지 않는다. `AGENTS.md`의 배포 사전 보고와 사용자 승인을 먼저 거친다.
