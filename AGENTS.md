# Seoul Weather Platform 작업 규약

## 목적과 경계

- 이 저장소는 Weather 수집 → R2 raw → Trino/Iceberg Bronze → dbt Silver/Gold → D1 publication → K-Skill artifact의 수직 slice를 관리한다.
- Weather Platform의 공개 제품은 네 개이며, 현재 `seoul-weather-risk` K-Skill이 노출하는 제품은 `weather_place_risk_window` 하나다.
- Traffic, Citydata, Culture, Commerce, Transit, Marketplace UI/OAuth/quota/MCP는 기본 쓰기 범위가 아니다.
- 설계와 용어는 `docs/superpowers/specs/2026-08-14-weather-repository-separation-design.md`와 `CONTEXT.md`를 따른다.

## 고정 원본과 이관

- source snapshot은 `provenance/source-refs.lock.json`의 고정 commit에서만 읽는다.
- dirty working tree를 복사하지 않는다. `git show`, `git archive`, `git ls-tree`, `git cat-file`을 사용한다.
- imported/derived/generated 파일은 `provenance/source-files.jsonl`로 추적한다.
- 원본 commit 갱신은 source lock, inventory, checksum, 검증 결과를 같은 변경에서 갱신한다.

## AIRFLOW_DEPLOYMENT_APPROVAL_REQUIRED

Airflow 관련 state change 전에는 반드시 사용자에게 먼저 보고하고 명시 승인을 받는다.

사전 승인 없이 금지되는 작업:

- Airflow 이미지 build 또는 배포
- scheduler, dag-processor, api-server, triggerer 재생성·재시작
- DAG 활성화 또는 unpause
- 수동 트리거와 backfill
- collection/transform/publication 파이프라인 가동
- 기존 로컬 파이프라인 중지·재시작

보고에는 다음을 포함한다.

1. 배포 대상 commit과 변경 서비스
2. 기존 로컬 파이프라인에서 중지해야 할 DAG와 running/queued run
3. drain·pause·배포·health check·rollback 순서
4. dbt/Trino/D1 영향과 데이터 write 여부

사용자가 기존 로컬 파이프라인을 멈출 수 있도록 사전 보고한 뒤 승인받기 전까지는 repository/secretless test와 read-only inspection만 수행한다.

## 보안

- `.env*`, token, key, password, KMA `serviceKey`, Cloudflare credential을 출력·문서화·커밋하지 않는다.
- credential은 존재 여부와 필요한 환경변수 이름만 기록한다.
- `.omc`, `.omx`, `.superpowers`, `LessonRun.md`, `engineering-decision-log.md`는 원격에 올리지 않는다.
- public 전환과 제3자 재배포는 원본 코드·seed·fixture의 권리 확인 후 별도 승인으로 진행한다.

## Git

- 개발·PR base는 `dev`, 기능 branch는 `feat/` prefix를 사용한다.
- 사용자 승인 없이 stage, commit, push, PR 생성, destructive git 작업을 하지 않는다.
- `git add .`, `git add -A`를 사용하지 않고 경로 지정 stage만 사용한다.
- 사용자 변경과 unrelated dirty state를 되돌리지 않는다.

## 구현과 검증

- 동작 코드는 test-first로 변경하고 RED → GREEN을 확인한다.
- 최소 L0 검증은 repository policy, provenance, DagBag import, dbt parse, serving contract, 427-place artifact determinism이다.
- 실제 storage를 읽거나 쓰는 L1 검증은 별도 승인 이후에만 수행한다.
- fixture/unit test 성공을 runtime 배포나 최신 데이터의 증거로 표현하지 않는다.
