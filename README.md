# Seoul Weather Platform

기상청 예보를 수집하고 R2·Iceberg에 적재한 뒤, dbt로 변환해 D1과 Weather Risk K-Skill에 연결하는 데이터 플랫폼이다. ASK Seoul 팀 프로젝트에서 분리한 Weather 코드에 복구 계획, 예보 품질 평가와 공간 데이터 제품을 추가했다.

주요 기술: Python, SQL, Airflow, dbt, Trino, Apache Iceberg, Cloudflare R2·D1·Workers, GitHub Actions.

운영 환경은 개인 로컬 노트북과 개인 Cloudflare 계정에서 관리한다. 자격증명, Airflow metadata, Docker volume과 실행 로그는 저장소에 포함하지 않는다.

## 구현 내용과 검증 기록

- **팀 코드의 출처**: [고정 원본](provenance/source-refs.lock.json), [파일별 이관·변경 이력](provenance/source-files.jsonl), [NOTICE](NOTICE)로 이관한 팀 코드와 개인 후속 변경을 구분한다.
- **장애 복구 계획**: [복구 판단 로직](dags/common/recovery/planner.py)은 수집 결과를 기준으로 Raw 재처리·재수집·보류·차단을 구분하고 작업 수와 복구 기간을 제한한다. [Coordinator DAG](dags/domains/weather/weather_recovery_coordinator.py)는 기본 정지 상태이며, 계획만 생성하고 재처리를 직접 실행하지 않는다.
- **예보 품질 평가**: [품질 설계](docs/architecture/weather-forecast-quality.md)와 [Gold 구조](docs/architecture/forecast-quality-gold.md)에 예보 시점과 실황 기준, D-1·D-2·D-3 비교, 커버리지와 품질 지표 계산을 정리했다. [리포트 도구](tools/forecast_quality_report.py)와 [운영 절차](docs/runbooks/WEATHER_FORECAST_QUALITY_RUNBOOK.md)에서 결과 확인과 재처리 기준을 확인할 수 있다.
- **공간 데이터 제품**: [장소·격자 조인](tools/spatial_quality_product.py)은 기존 매핑과 격자별 품질 결과를 연결하고, 지표가 없는 장소를 `NO_METRICS`로 남긴다. [설계](docs/architecture/forecast-quality-spatial-product.md)와 [테스트](tests/forecast_quality/test_spatial_product.py)를 함께 관리한다.
- **운영 판단과 장애 기록**: [운영 경계](docs/architecture/platform-boundaries.md), [설계 판단과 근거](docs/data-engineering-decision.md), [장애 기록](docs/lessonrun.md)에 의존 관계와 복구 기준을 정리했다.

검증용 80개 격자 fixture는 합성 데이터다. 테스트 통과가 실제 서울 예보 정확도, 현재 배포 상태나 데이터 신선도를 뜻하지는 않는다. 품질 평가 대상인 80개 격자와 K-Skill 호환용 427개 장소도 서로 다른 기준이다.

[기상청 실황 수집 설계](docs/architecture/kma-observation-truth.md)를 바탕으로 `getUltraSrtNcst` 파싱, 호출 횟수 제한, 제한된 재시도, 변경 불가능한 Raw 적재와 전용 Iceberg Bronze를 구현했다.

설정 예제와 로컬 운영 설정은 다르다. `.env.example`은 공유 호출 제한 설정이 `false`이고 관측 수집 스케줄이 비어 있지만, [`docker-compose.local.yml`](docker-compose.local.yml)은 공유 호출 제한을 켜고 매시 45분(`45 * * * *`) 스케줄을 정의한다. DAG의 기본 정지는 최초 생성 시에만 적용되며 기존 DAG의 활성 상태를 바꾸지 않는다. 초기 설계 문서의 비활성 기본값과 현재 로컬 설정을 혼동하지 않아야 한다. 실제 수집·적재, 재처리와 DAG 상태 변경은 아래 Airflow 배포 승인 절차를 별도로 따른다.

## 현재 경계

이 저장소가 관리하는 범위:

- KMA Weather 수집·Bronze 적재 DAG와 필요한 `common` import closure
- Weather dbt graph와 `asac_axes` pinned package
- Weather D1 publication compatibility lane
- Origin/proxy contract fixture
- 개인 Weather origin 앞의 최소 권한 K-Skill proxy와 회귀 테스트
- `seoul-weather-risk` upstream PR에 넣을 장소 artifact generator와 snapshot

이 저장소가 소유하지 않는 범위:

- 개인 Cloudflare 자격증명과 실제 R2/D1 데이터
- Weather origin의 범용 Marketplace/OAuth 구현
- NomaDamas `k-skill` runtime
- Marketplace UI/OAuth/quota/MCP
- Traffic, Citydata, Culture, Commerce, Transit domain

## Product 경계

Weather Platform public product는 4개다.

| product_id | dbt producer | 현재 K-Skill 노출 |
|---|---|---:|
| `weather_place_current_outlook` | `gold_weather_place_current_outlook` | 아니오 |
| `weather_place_precipitation_window` | `gold_weather_place_precipitation_window` | 아니오 |
| `weather_place_risk_window` | `gold_weather_place_risk_window` | 예 |
| `weather_place_forecast_change_daily` | `gold_weather_place_forecast_change_daily` | 아니오 |

현재 설치되는 `seoul-weather-risk` K-Skill은 `weather_place_risk_window` 하나만 사용자에게 노출한다. K-Skill runtime의 정본은 이 저장소가 아니라 upstream `NomaDamas/k-skill`이다.

기본 hosted proxy가 조직 시절 origin을 가리키는 동안에는 `KSKILL_PROXY_BASE_URL`을 이 저장소의 개인 proxy Worker origin으로 설정한다. proxy는 고정된 세 경로만 개인 Weather origin으로 전달하고 서비스 토큰은 Worker secret으로만 보관한다.

## 고정 원본

source snapshot은 working tree가 아니라 다음 commit에서만 가져온다.

| source id | repository | commit |
|---|---|---|
| `airflow_weather` | `ASAC-DE-bigkk/ASAC-DAG` | `73ff5665ffd5526c59de8be2969cf65dffaf468b` |
| `weather_dbt` | `ASAC-DE-bigkk/ASAC-DBT` | `a64292d50bd8c2a19784388828de38d2b4a8c525` |
| `weather_origin_contract` | `ASAC-DE-bigkk/ASK-Seoul-Serving` | `efe393e7a925d5798867424993daf0dbe5d55902` |
| `kskill_runtime` | `NomaDamas/k-skill` | `43edf3c0f1037a4e510b21de61e26965212b6620` |

정본 파일:

- `provenance/source-refs.lock.json`
- `provenance/source-inventory.json`
- `provenance/source-files.jsonl`

## Secretless 검증

Airflow, Docker, 기존 파이프라인을 건드리지 않는 기본 검증:

```powershell
./tools/verify_repository.ps1
```

이 명령은 다음만 수행한다.

```text
python -m tools.repository_policy --repo-root <repository>
python -m tools.verify_provenance --repo-root <repository>
python -m tools.refresh_provenance --repo-root <repository> --check
python -m tools.workflow_policy --repo-root <repository>
python -m pytest tests/repository tests/deploy
```

개별 확인이 필요하면:

```powershell
python -m pytest tests\contracts release\weather\tests -q
python -m pytest dbt\serving_contract\tests -q
python -m pytest dags\common\serving\tests -q
python -m pytest dags\domains\weather\tests -q
python -m pytest dbt\domains\traffic_weather\tests\weather -q
python -m pytest tests\forecast_quality -q
cd k-skill-proxy && npm test
```

고정 Airflow image에서 실행 중인 compose와 분리된 DagBag import를 확인한다. 이 명령은 network 없이 read-only one-off container만 사용하며 DAG를 실행하지 않는다.

```powershell
powershell -File tools\verify_dagbag.ps1 -PrintCommand
powershell -File tools\verify_dagbag.ps1
```

dbt 계약 검증은 `runtime/requirements-dbt.lock.txt`와 같은 Linux runtime 또는 dbt Core 1.10.22/dbt-trino 1.10.2 격리 환경에서 실행한다.

```powershell
dbt deps --project-dir dbt/domains/traffic_weather
dbt parse --project-dir dbt/domains/traffic_weather --profiles-dir dbt/domains/traffic_weather --target ci --no-partial-parse
dbt ls --project-dir dbt/domains/traffic_weather --profiles-dir dbt/domains/traffic_weather --target ci --selector ask_seoul_weather_d1_public_products --resource-type model --output name
python dbt/serving_contract/validate_serving_contract.py --source dbt/domains/traffic_weather/models/weather/transform/gold/gold_weather_place_current_outlook.yml dbt/domains/traffic_weather/models/weather/transform/gold/gold_weather_place_precipitation_window.yml dbt/domains/traffic_weather/models/weather/transform/gold/gold_weather_place_risk_window.yml dbt/domains/traffic_weather/models/weather/transform/gold/gold_weather_place_forecast_change_daily.yml --manifest dbt/domains/traffic_weather/target/manifest.json --format text
```

## Airflow 배포 gate

사용자 명시 승인 전에는 다음을 실행하지 않는다.

- Airflow image build/deploy
- scheduler, dag-processor, api-server, triggerer restart/recreate
- DAG enable, unpause, trigger, backfill
- collection/transform/publication pipeline start/stop
- 기존 로컬 파이프라인 pause/stop/restart

배포가 필요하면 먼저 다음을 보고하고 승인받는다.

1. 배포 대상 commit과 변경 서비스
2. 기존 로컬 파이프라인에서 중지할 DAG와 running/queued run
3. pause, drain, 배포, health check, rollback 순서
4. dbt/Trino/D1 영향과 데이터 write 여부

자세한 기준은 `docs/operations/predeployment-approval-gate.md`를 따른다.

## 개인 운영 storage

런타임은 개인 R2, Iceberg Data Catalog, D1과 개인 Worker를 사용한다. 저장소는 adapter와 계약을 소유하지만 실제 cloud resource와 비밀값은 private operations plane에 둔다.

리소스 owner, 장애 영향과 복구 경계는 `docs/operations/current-resource-dependencies.md`에 secret 없이 기록한다.

로컬 실행은 공통 `docker-compose.yml`과 `docker-compose.local.yml` 두 파일만 조합한다. 명령과 리소스 제한은 `README-LOCAL.md`를 따른다.

## 주요 문서

- `CONTEXT.md` — 저장소 용어와 정본 경계
- `README-LOCAL.md` — 로컬 Compose 실행과 메모리 운영 기준
- `docs/superpowers/specs/2026-08-14-weather-repository-separation-design.md` — 최종 분리 설계
- `docs/architecture/platform-boundaries.md` — runtime ownership seam
- `docs/architecture/weather-forecast-quality.md` — internal forecast-quality Gold architecture
- `docs/operations/weather-forecast-quality-runbook.md` — Quality Gold shadow/activation/rollback gate
- `docs/runbooks/WEATHER_FORECAST_QUALITY_RUNBOOK.md` — 예보 품질 Gold 리포트 실행·재처리 기준
- `docs/architecture/kma-observation-truth.md` — KMA 실황 정답 데이터 계약과 배포 gate
- `docs/operations/kma-observation-predeployment-plan.md` — 관측 파이프라인 승인 전 배포·복구 설계
- `docs/operations/current-resource-dependencies.md` — 기존 resource 의존성
- `docs/operations/predeployment-approval-gate.md` — Airflow 변경 승인 gate
- `docs/data-engineering-decision.md` — 운영 선택, 대안, 근거가 남는 decision log
- `docs/lessonrun.md` — 실제 장애를 data flow와 contract 관점에서 복기한 lesson run

## Git 주의

기능·문서 변경은 `feat/` 브랜치에서 `dev` 대상 PR로 반영하고, 검증된 `dev`를 별도 PR로 `main`에 반영한다. 필수 CI를 통과한 뒤 병합하며 브랜치 보호 규칙을 우회하지 않는다. `dev`는 선형 이력을 유지하므로 동기화 완료 여부는 커밋 번호가 아니라 `git diff origin/main origin/dev`로 파일 내용까지 확인한다.

아직 사용자 승인 전이면 stage, commit, push, PR을 수행하지 않는다. stage가 필요할 때도 `git add .` 또는 `git add -A`를 쓰지 않고 경로 지정 stage만 사용한다.
