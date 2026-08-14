# Seoul Weather Platform

Weather 수집, dbt 변환, D1 publication 호환 코드, Weather Risk K-Skill 장소 artifact를 한 저장소에서 관리하기 위한 개인 private vertical slice다.

현재 상태는 **L0 repository extraction 진행 중**이다. 이 저장소가 만들어졌다고 해서 개인 R2/D1 운영 독립, Airflow 배포, 실서비스 전환이 완료된 것은 아니다.

## 현재 경계

이 저장소가 관리하는 범위:

- KMA Weather 수집·Bronze 적재 DAG와 필요한 `common` import closure
- Weather dbt graph와 `asac_axes` pinned package
- Weather D1 publication compatibility lane
- Origin/proxy contract fixture
- `seoul-weather-risk` upstream PR에 넣을 장소 artifact generator와 snapshot

이 저장소가 1차에서 소유하지 않는 범위:

- 기존 R2, Iceberg/Trino, D1, Weather origin
- 개인 R2/D1/Iceberg catalog
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
python -m pytest tests/repository
```

개별 확인이 필요하면:

```powershell
python -m pytest tests\contracts release\weather\tests -q
python -m pytest dbt\serving_contract\tests -q
python -m pytest dags\common\serving\tests -q
python -m pytest dags\domains\weather\tests -q
python -m pytest dbt\domains\traffic_weather\tests\weather -q
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

## 기존 storage 의존성

1차에서는 기존 R2, Iceberg/Trino, D1, Weather origin을 외부 의존성으로 계속 사용한다. 새 저장소가 해당 resource를 소유한다는 뜻은 아니다.

리소스 owner, 사용 승인, 종료 리스크는 `docs/operations/current-resource-dependencies.md`에 secret 없이 기록한다. 개인 R2/D1 이관은 repository boundary가 안정된 뒤 별도 milestone로 진행한다.

## 주요 문서

- `CONTEXT.md` — 저장소 용어와 정본 경계
- `docs/superpowers/specs/2026-08-14-weather-repository-separation-design.md` — 최종 분리 설계
- `docs/architecture/platform-boundaries.md` — runtime ownership seam
- `docs/operations/current-resource-dependencies.md` — 기존 resource 의존성
- `docs/operations/predeployment-approval-gate.md` — Airflow 변경 승인 gate

## Git 주의

아직 사용자 승인 전이면 stage, commit, push, PR을 수행하지 않는다. stage가 필요할 때도 `git add .` 또는 `git add -A`를 쓰지 않고 경로 지정 stage만 사용한다.
