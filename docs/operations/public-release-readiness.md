# 공개 전환 준비 상태

상태: **저장소 내부 검사 통과 / GitHub 공개 전환 완료**
기준일: 2026-08-21

2026-08-21에 전체 공개 전환을 승인했고, 2026-08-22에 읽기 전용으로 다시 확인했다.
공개 전환은 코드 저장소에만 적용되며 개인 운영 환경을 공개하지 않는다.

## 현재 확인표

| 항목 | 확인 결과 | 상태 |
|---|---|---|
| 저장소 공개 검사 | `LICENSE`, `NOTICE`, 출처 승인, 비밀값 없는 예시 파일, 공개 검사기 | 통과 |
| 예시 환경 파일 | `.env.example`에 Weather 전용 빈칸과 로컬 Trino 제한이 있음 | 통과 |
| 개인 실행 분리 | `docker-compose.local.yml`, 호환 프로젝트 ID, 비밀값은 Git 무시 | 통과 |
| 계보 비용 | Marquez 비활성, Airflow/`dbt` OpenLineage 비활성, 파일 출처 유지 | 통과 |
| 재배포 권리 | `provenance/source-files.jsonl`에 2026-08-21 승인 기록 | 통과 |
| 라이선스·고지 | Apache-2.0 `LICENSE`와 `NOTICE` 존재 | 통과 |
| CI·runner 분리 | GitHub 호스팅 검사만 사용, 수동 배포 workflow는 꺼짐·무동작 | 통과 |
| 당시 감사 자료 | Release 0개, 내려받을 산출물 0개, Actions 기록 121개 검사 | 당시 기준 통과 |
| GitHub 공개 전환 | `PUBLIC`, 배포 비활성, 거버넌스 `public`, runner 0개, `main` 보호 | 완료 |

## 외부 확인값

2026-08-22 읽기 결과는 다음과 같다.

- 저장소 공개 상태 `PUBLIC`, 기본 브랜치 `main`
- 저장소 변수 `WEATHER_DEPLOYMENT_ENABLED=disabled`,
  `WEATHER_GOVERNANCE_MODE=public`
- 등록된 runner 0개
- `main`에 `CI / required`와 PR 검토 규칙 적용
- GitHub Release와 내려받을 수 있는 Release 산출물 0개

이는 확인한 시점의 증거다. 다음 변경이나 Release 전에는 다시 읽어야 하며, 깨끗한
작업 폴더만 보고 공개 준비가 끝났다고 판단하지 않는다.

## 앞으로 지킬 것

공개·신뢰 경계를 바꿀 때마다 검토한 커밋, 재배포 권리, 비밀값 검사, Release 차이,
호스팅 CI·fork 동작, runner·branch 보호 상태, 개인 runtime 재현 방법, Worker 노출,
되돌리기 경로를 다시 기록한다. 공개된 과거 Release가 생기면 그것도 공개 범위에 포함된다.

저장소 코드나 workflow가 저장소 공개 여부를 자동으로 바꾸는 명령은 의도적으로 넣지 않았다.

## 검사기와 대조하는 원문 근거

사람이 읽는 문장은 위와 같이 한국어로 썼지만, 공개 검사기가 찾는 증거 문구는 다음처럼
남긴다. 이 문구는 배포 명령이 아니라 당시 확인 결과를 가리키는 표식이다.

- `Repo-local publication gates` — `PASS`
- `GitHub external visibility cutover` — `COMPLETE`
- `User authorization: full visibility cutover authorized on 2026-08-21`
- Repository visibility is `PUBLIC`
- deployment `disabled` and governance `public`
- No repository runner is registered
- `CI / required` and pull-request review
- `0 GitHub Releases`, `0 downloadable Release artifacts`, `121 GitHub Actions logs scanned clean`, `reachable-object scan passed`
- `future release` 전에는 다시 확인한다.
