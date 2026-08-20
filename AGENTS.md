# Seoul Weather Platform 작업 규약

## 목적과 소유 경계

- 이 저장소는 Weather 수집 → R2 raw → Trino/Iceberg Bronze → dbt Silver/Gold → D1 publication → K-Skill artifact의 수직 slice를 관리한다.
- Weather Platform public product는 `weather_place_current_outlook`, `weather_place_precipitation_window`, `weather_place_risk_window`, `weather_place_forecast_change_daily` 네 개다. 현재 `seoul-weather-risk` K-Skill이 노출하는 제품은 `weather_place_risk_window` 하나다.
- Traffic, Citydata, Culture, Commerce, Transit, Marketplace UI/OAuth/quota/MCP는 기본 쓰기 범위가 아니다.
- 기존 R2, Iceberg catalog, Trino cluster, D1, Weather origin은 1차에서 외부 의존성이다. 이 저장소가 해당 운영 리소스를 소유하거나 자동으로 재구성한다고 가정하지 않는다.
- 설계와 용어는 `docs/superpowers/specs/2026-08-14-weather-repository-separation-design.md`와 `CONTEXT.md`를 따른다.

## 고정 원본과 provenance

- source snapshot은 `provenance/source-refs.lock.json`의 고정 commit에서만 읽는다.
- dirty working tree를 복사하지 않는다. `git show`, `git archive`, `git ls-tree`, `git cat-file`을 사용한다.
- imported/derived/generated 파일은 `provenance/source-files.jsonl`로 추적한다.
- 원본 commit 갱신은 source lock, inventory, checksum, 검증 결과를 같은 변경에서 갱신한다.
- working tree의 `.env*`, `target`, `logs`, `dbt_packages`, `.omc`, `.omx`, `.superpowers`와 개인 실행 기록은 snapshot과 provenance에 포함하지 않는다.

## Weather 모델·서빙 불변식

- Bronze는 원본 payload와 redacted request metadata, result, collected/load time, payload hash, raw object key를 보존한다. Silver는 표준화·캐스팅·dedup을 담당하고 native id를 보존한다. Gold는 분석·서빙 제품을 담당한다.
- Weather public product의 grain, 시간축, primary key, null/zero 의미, freshness와 `meta.serving` 계약을 임의로 바꾸지 않는다.
- incremental 모델은 unique key, late-arrival/repair 범위, merge 대상 window가 명시되어야 한다. 편의를 위해 full-refresh 또는 무제한 historical scan을 기본 경로로 바꾸지 않는다.
- forecast window는 제품 계약이 정한 범위로 제한하고, `forecast_at`/day 파티션과 predicate pruning을 보존한다. 필요한 컬럼만 선택하고 불필요한 `select *`, 전체 history `row_number`, 무제한 join을 추가하지 않는다.
- 최신 `issued_at` 선택, snapshot pin, dedup, last-known-good, zero/partial-row gate를 제거하거나 우회하지 않는다.
- D1 게시가 필요한 변경은 `Contract Load → Gate → Write → row-count verify → _catalog/publication evidence → API smoke` 순서를 보존한다. 적재 성공만으로 publication 성공을 선언하지 않는다.

## Trino 외부 read·OOM 비용 가드레일

이번 작업의 우선순위는 “원격에서 덜 읽기 → 같은 파일을 재사용하기 → 폭주를 격리하기 → 정확히 측정하기”다.

- 원격 read 감소가 1차다. bounded incremental model, partition pruning, predicate/column pushdown, selective join과 serving snapshot을 우선한다.
- FS cache는 cache miss 이후의 동일 파일 재조회 방어선이다. 캐시를 켰다는 이유로 unbounded scan이나 잘못된 모델을 허용하지 않는다.
- Airflow Pool, DAG `max_active_runs`, Trino Resource Group, memory cap, spill, memory revoking은 동시성·메모리 폭주 방어선이다. 이들은 원격 scan bytes를 자동으로 줄이는 기능이 아니므로 역할을 혼동하지 않는다.
- query scan-byte limit, execution-time limit, workload별 resource group 같은 외부 Trino 설정은 후보와 검증 기준만 문서화한다. 이 저장소의 코드 변경만으로 prod Trino 설정·credential·catalog를 바꾸지 않는다.
- 측정은 Windows 전체 RX/TX를 주 지표로 삼지 않는다. 가능한 경우 Trino `physicalInputBytes`/`processedBytes`, FS cache hit/miss·external read, R2 Class B operation, Data Catalog operation, query queue/실행시간을 별도로 기록한다. 접근할 수 없는 지표는 임의 수치로 대체하지 않는다.
- 멀티노드 추가는 이번 범위에 포함하지 않는다. 유효한 bounded query에서 OOM 재발, Worker 메모리 지속 포화, queue SLO 초과, ETL/serving 격리 필요성이 관측될 때만 별도 capacity 검토 이슈로 분리한다.
- Retry와 fault tolerance는 신뢰성 기능이지 비용 절감 기능이 아니다. 재시도가 외부 read를 반복할 수 있으므로 idempotency, checkpoint, backoff와 함께 검증한다.

## AIRFLOW_DEPLOYMENT_APPROVAL_REQUIRED

Airflow 관련 state change 전에는 반드시 사용자에게 먼저 보고하고 명시 승인을 받는다.

사전 승인 없이 금지되는 작업:

- Airflow image build 또는 deploy
- scheduler, dag-processor, api-server, triggerer 재생성·재시작
- DAG 활성화 또는 unpause
- 수동 트리거와 backfill
- collection/transform/publication pipeline 가동
- 기존 로컬 파이프라인 중지·재시작
- R2/Iceberg/Trino/D1에 대한 write 또는 대량 재처리

배포가 필요하면 먼저 다음을 보고하고 승인받는다.

1. 배포 대상 commit과 변경 service
2. 중지·drain 대상 DAG와 running/queued run
3. pause, drain, deploy, health check, rollback 순서
4. dbt/Trino/R2/D1 영향과 data write 여부

승인 전에는 repository test, secretless static check, read-only inspection만 수행한다.

## 보안·권리

- `.env*`, token, key, password, KMA `serviceKey`, Cloudflare/R2/D1 credential을 출력·문서화·커밋하지 않는다.
- credential은 존재 여부와 필요한 환경변수 이름만 기록한다.
- 원본 DAG·dbt·Serving code와 seed/fixture의 재사용 권리가 확인되기 전에는 public 전환이나 제3자 재배포를 하지 않는다.
- private repository라는 사실은 코드·데이터의 재사용 권리를 부여하지 않는다.

## Git·이슈·PR

- 개발·PR base는 `dev`, 기능 branch는 `feat/<issue-number>-<short-description>` 형식을 사용한다.
- 사용자 승인 없이 stage, commit, push, PR 생성, destructive git 작업을 하지 않는다.
- `git add .`, `git add -A`를 사용하지 않고 경로 지정 stage만 사용한다.
- 사용자 변경과 unrelated dirty state를 되돌리지 않는다.
- 이슈와 PR 본문에는 근본 원인, 변경 동작, 관측 가능한 결과, 데이터·비용 영향, rollback 범위를 기록한다.

## 검증

- 동작 코드는 가능하면 test-first로 변경하고 RED → GREEN을 확인한다.
- 최소 L0 검증은 repository policy, provenance, DagBag import, dbt parse, serving contract, 427-place artifact determinism이다.
- Weather 변경은 대상 selector와 모델 범위를 좁혀 `dbt parse`, 필요한 `dbt test`, serving-contract validator를 실행한다. 전체 재빌드와 전체 test를 기본값으로 삼지 않는다.
- 실제 storage를 읽거나 쓰는 L1 검증은 resource 사용 승인과 deployment gate 이후에만 수행한다.
- fixture/unit test 성공은 runtime 배포, 최신 데이터, D1 publication의 증거가 아니다.
- 실행 보고에는 명령, selector, DAG run/task 상태, relation/table, row count, freshness, 측정 범위와 미측정 사유를 남긴다. secret과 개인 기록은 남기지 않는다.
