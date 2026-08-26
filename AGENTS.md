# Seoul Weather Platform 작업 규약

이 문서는 저장소를 운영·변경할 때 지킬 기준이다. 빠른 데모보다 신뢰성, 자료 품질,
유지보수성, 확장성, 관측 가능성, 성능, 자동화, 비용을 먼저 본다.

## 목적과 소유 경계

- 이 저장소는 Weather 수집 → R2 raw → Trino/Iceberg Bronze → `dbt` Silver/Gold → D1 publication → K-Skill artifact의 수직 흐름을 관리한다.
- 공개 Weather 제품은 `weather_place_current_outlook`, `weather_place_precipitation_window`, `weather_place_risk_window`, `weather_place_forecast_change_daily` 네 개다. `seoul-weather-risk`가 공개하는 제품은 `weather_place_risk_window` 하나다.
- Traffic, Citydata, Culture, Commerce, Transit, Marketplace UI/OAuth/quota/MCP는 기본 변경 범위가 아니다.
- 기존 R2, Iceberg catalog, Trino cluster, D1, Weather origin은 외부 의존성이다. 이 저장소가 해당 운영 리소스를 자동으로 다시 만들거나 소유한다고 가정하지 않는다.
- 설계와 용어는 `docs/superpowers/specs/2026-08-14-weather-repository-separation-design.md`, `docs/README.md`, `CONTEXT.md`를 따른다.

## 고정 원본과 출처 기록

- source snapshot은 `provenance/source-refs.lock.json`의 고정 commit에서만 읽는다.
- dirty working tree를 복사하지 않는다. `git show`, `git archive`, `git ls-tree`, `git cat-file`을 사용한다.
- 가져온·파생·생성 파일은 `provenance/source-files.jsonl`로 추적한다.
- 원본 commit을 갱신하면 source lock, inventory, checksum, 검증 결과를 같은 변경에 넣는다.
- `.env*`, `target`, `logs`, `dbt_packages`, `.omc`, `.omx`, `.superpowers`와 개인 실행 기록은 snapshot/provenance에 넣지 않는다.

## Weather 자료와 제공 불변식

- Bronze는 원본 payload, 가린 요청 정보, 결과, 수집·적재 시각, payload hash, 원본 객체 경로를 보존한다. Silver는 표준화·형 변환·중복 제거를 담당하고 원본 ID를 유지한다. Gold는 분석·제공 제품을 담당한다.
- 공개 제품의 grain, 시간축, 기본 키, null/zero 의미, 최신성, `meta.serving` 계약을 임의로 바꾸지 않는다.
- 증분 모델은 고유 키, 늦게 도착한 자료의 보수 범위, merge 범위를 명시한다. 편의를 위해 전체 재생성이나 범위 없는 이력 읽기를 기본으로 바꾸지 않는다.
- 예보 시간 범위와 `forecast_at`/날짜 파티션, predicate pruning을 보존한다. 불필요한 `select *`, 전체 이력 `row_number`, 범위 없는 join을 추가하지 않는다.
- 최신 `issued_at` 선택, snapshot 고정, 중복 제거, 마지막 정상본, 0/부분 행 문턱을 제거하거나 우회하지 않는다.
- D1 변경은 `Contract Load → Gate → Write → row-count verify → _catalog/publication evidence → API smoke` 순서를 지킨다. 적재 성공만으로 발행 성공을 선언하지 않는다.

## Trino 읽기·OOM·비용 기준

우선순위는 “원격에서 덜 읽기 → 같은 파일 재사용 → 폭주 격리 → 정확한 측정”이다.

- 제한된 증분 모델, 날짜 파티션, 조건·열 pushdown, 필요한 조인, 제공 시점 저장을 먼저 적용한다.
- FS cache는 같은 불변 파일의 재읽기를 줄이는 보조 수단이다. 캐시를 켰다는 이유로 범위 없는 읽기를 허용하지 않는다.
- Airflow Pool, DAG `max_active_runs`, Trino Resource Group, memory cap, spill, memory revoking은 동시성과 메모리 폭주 방어선이다. 원격 읽기량을 자동으로 줄이는 기능은 아니다.
- query scan-byte/time limit과 workload별 resource group은 후보·검증 기준만 문서화한다. 코드 변경만으로 운영 Trino 설정·자격증명·catalog를 바꾸지 않는다.
- Windows 전체 RX/TX를 주 지표로 삼지 않는다. 가능하면 Trino `physicalInputBytes`/`processedBytes`, FS cache 적중·실패, 외부 읽기, R2 Class B, Data Catalog 작업, 대기·실행 시간을 따로 기록한다. 모르는 값은 추정하지 않는다.
- 멀티노드 추가는 현재 범위가 아니다. 유효한 제한 쿼리에서도 OOM, Worker 메모리 포화, 대기 SLO 초과, ETL/제공 분리가 관측될 때 별도 용량 검토로 분리한다.
- Retry와 fault tolerance는 신뢰성 기능이지 비용 절감 기능이 아니다. 외부 읽기를 반복할 수 있으므로 멱등성·checkpoint·backoff와 함께 검증한다.

## AIRFLOW_DEPLOYMENT_APPROVAL_REQUIRED

Airflow 상태를 바꾸기 전에는 사용자에게 먼저 보고하고 명시 승인을 받는다.

승인 없이 금지하는 작업:

- Airflow 이미지 build/deploy
- scheduler, dag-processor, api-server, triggerer 재생성·재시작
- DAG 활성화·unpause
- 수동 trigger·backfill·clear·retry·mark-success
- collection/transform/publication pipeline 가동
- 기존 로컬 파이프라인 중지·재시작
- R2/Iceberg/Trino/D1 write 또는 대량 재처리

보고에는 대상 commit·service, 중지·비우기 대상 DAG와 실행, pause·drain·배포·health·
rollback 순서, dbt/Trino/R2/D1 영향과 데이터 쓰기 여부를 포함한다. 승인 전에는
repository test, 비밀값 없는 정적 검사, 읽기 전용 확인만 한다.

자동 복구 제어면도 같은 경계를 따른다.

- recovery planner/coordinator는 기본 dry-run, schedule 없음, 생성 시 pause다.
- startup wrapper는 `--start`와 `WEATHER_STARTUP_AUTOSTART=enabled`가 모두 있어야 Compose core stack만 시작한다. DAG 실행 요청과 데이터 쓰기는 소유하지 않는다.
- planner 결과를 실제 replay/recollect executor로 승격하기 전 active-run 대조, durable lease/idempotency, API·Trino 예산, rollback/마지막 정상본 문턱, 장애주입 증거와 별도 승인을 확인한다.

## 보안·권리

- `.env*`, token, key, password, KMA `serviceKey`, Cloudflare/R2/D1 credential을 출력·문서화·커밋하지 않는다.
- 비밀값은 존재 여부와 필요한 환경 변수 이름만 기록한다.
- 원본 DAG·`dbt`·Serving code와 seed/fixture의 재사용 권리를 확인하기 전에는 공개 전환이나 제3자 재배포를 하지 않는다.
- private repository라는 사실만으로 코드·자료의 재사용 권리가 생기지 않는다.

## Git·이슈·PR

- 개발·PR base는 `dev`, 기능 branch는 `feat/<issue-number>-<short-description>` 형식을 사용한다.
- 사용자 승인 없이 stage, commit, push, PR 생성, 위험한 Git 작업을 하지 않는다.
- `git add .`, `git add -A`를 사용하지 않고 경로를 명시해 stage한다.
- 사용자 변경과 unrelated dirty state를 되돌리지 않는다.
- 이슈·PR에는 근본 원인, 바뀐 동작, 관측 결과, 자료·비용 영향, rollback 범위를 적는다.

### `main` promotion은 PR-only

public repository에서 merged pull request만 `main`의 정상적인 source다.
`Promotion Source / required`가 그 증거를 확인한다.

- documentation, provenance, CI, 긴급 fix도 `main`에 직접 commit/push하지 않는다.
- 기능 변경은 `feat/` branch에서 검사한 뒤 `dev` 대상 PR로 올린다.
- 검증된 `dev → main` PR과 required check 성공 뒤에만 main에 병합한다.
- 빠른 반영을 위해 branch protection이나 required check를 우회하지 않는다. 수정과 provenance 갱신은 같은 feature PR에 넣는다.

#### 명시적 긴급 예외

직접 main push는 사용자가 `Promotion Source / required`와 `CI / required`가 merged-PR
증거 없이 의도적으로 실패한다는 사실을 들은 뒤 명시적으로 승인한 경우에만 허용한다.
근거와 runtime 증거를 `docs/lessonrun.md`에 남기고, 다음 변경부터 branch-and-PR 경로로
돌아온다.

## 구현과 검증

- 동작 코드는 가능하면 test-first로 바꾸고 RED → GREEN을 확인한다.
- 최소 L0 검사는 repository policy, provenance, DagBag import, `dbt` parse, 제공 계약,
  427-place 산출물 결정성이다.
- Weather 변경은 대상 selector와 모델 범위를 좁혀 필요한 검사만 실행한다. 전체 재빌드·전체 검사를 기본값으로 삼지 않는다.
- 실제 storage를 읽거나 쓰는 L1 검사는 리소스 사용 승인과 배포 게이트 뒤에만 한다.
- fixture/unit test 성공은 운영 배포·최신 자료·D1 발행의 증거가 아니다.
- 보고서에는 명령, selector, DAG run/task 상태, relation/table, 행 수, 최신성, 측정 범위와 미측정 사유를 남긴다. 비밀값과 개인 기록은 남기지 않는다.
