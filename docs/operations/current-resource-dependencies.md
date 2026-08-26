# 개인 Weather 운영 리소스 의존성

이 문서는 비밀값을 저장하지 않는다. 아래 리소스는 Mason 개인 계정에 있고, 공개
저장소와 분리된 개인 운영 영역에서만 사용한다.

| 리소스 | 역할 | 현재 상태 | 중단 시 영향 |
|---|---|---|---|
| 개인 R2·Data Catalog | KMA 원본과 Iceberg 파일 저장 | 운영 중 | 새 Bronze 적재와 `dbt` 조회 불가 |
| 로컬 Trino | Bronze·Silver·Gold 조회 | 5 GiB 제한, 파일 캐시 사용 | 변환·시점 저장 중단 |
| 개인 D1 | 공개 제품 발행 | 운영 중 | origin에 최신 발행본이 안 보임 |
| 개인 Weather origin | `/skill/v1/...` 응답 | 운영 중 | proxy가 upstream에 연결하지 못함 |
| 개인 K-Skill proxy | 세 읽기 전용 경로 전달 | 이 저장소에서 관리 | helper가 개인 origin을 조회하지 못함 |
| NomaDamas hosted proxy | 기본 K-Skill 경로 | 조직 origin을 가리킬 수 있음 | 기본 조회가 503 또는 오래된 결과 |

## Airflow 변경 승인

새 저장소의 코드를 실제 Airflow에 올리거나 DAG를 가동하기 전에는 사용자에게 다음을
먼저 보고한다.

1. 멈추거나 비워야 할 기존 DAG와 실행 중인 작업
2. 바꿀 컨테이너와 연결할 파일
3. 변경 전·후 health, DAG 읽기, 최신성 검사 순서
4. 실패했을 때 되돌릴 방법

사용자 승인이 없으면 기존 파이프라인을 일시정지·중지·재시작하거나 새 DAG를 활성화·
실행하지 않는다.

## 운영 완료 기준

아래 네 가지를 모두 확인해야 전체 경로가 정상이라고 판단한다.

- Docker health, 재시작 횟수, OOM 상태가 정상이다.
- 최신 Bronze → Silver → Gold 실행이 성공했다.
- D1 발행과 감시 작업이 성공하고 API 응답의 제품 ID가 맞다.
- 개인 proxy를 거친 K-Skill 조회가 200을 반환하고 발행 ID가 일치한다.
