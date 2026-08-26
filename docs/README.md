# 문서 읽는 순서

이 저장소의 문서는 사람이 읽는 운영 문서와 구현 당시의 상세 계약으로 나뉜다.

## 먼저 읽을 문서

1. `../README.md` — 저장소가 하는 일과 공개·개인 영역
2. `../README-LOCAL.md` — 개인 노트북에서 시작하는 방법
3. `architecture/` — 데이터 흐름, KMA 실황, 예보 품질
4. `operations/weather-recovery-and-optimization.md` — 장애 원인과 자동 복구 설계
5. `data-engineering-decision.md` — 선택한 방법과 버린 대안
6. `lessonrun.md` — 실제 장애를 조사한 순서와 배운 점

위 문서는 제목과 설명을 한국어로 쓰고, 처음 나오는 기술 용어는 뜻을 풀어 쓴다.

## 운영 전에 읽을 문서

- `operations/predeployment-approval-gate.md` — Airflow 변경 승인 경계
- `operations/kma-observation-predeployment-plan.md` — 실황 수집 배포 전 확인표
- `operations/main-auto-deploy-first-cutover.md` — `main` 자동 배포 최초 전환
- `operations/github-bootstrap.md` — GitHub 브랜치와 보호 규칙 최초 설정
- `operations/current-resource-dependencies.md` — 개인 리소스와 중단 영향
- `operations/secretless-dagbag-validation.md` — 비밀값 없는 DAG 읽기 검사
- `operations/public-release-readiness.md`, `legal/publication-blockers.md` — 공개 전환 증거

## 상세 계획·명세

`superpowers/plans/`와 `superpowers/specs/`는 구현 도구와 리뷰어가 같은 계약을 보도록
남긴 작업 원문이다. 체크박스, 코드 식별자, 설정 키, 명령어, 상태 값은 자동 검사와
대조해야 하므로 영어 표기를 일부 유지한다. 사람이 전체 흐름을 볼 때는 위 운영 문서와
결정·복기 문서를 먼저 읽으면 된다.

## 공통 표기 원칙

- 제품명·도구명·DAG ID·파일명·환경 변수·API 경로는 원래 표기를 유지한다.
- 일반 설명은 한국어로 쓴다.
- “정상”은 컨테이너가 살아 있다는 뜻과 데이터가 최신이라는 뜻을 구분한다.
- 수치·시각·검사 결과는 확인한 날짜와 함께 기록한다.
- 비밀값, token, 실제 객체 경로와 원문 로그는 문서에 넣지 않는다.
