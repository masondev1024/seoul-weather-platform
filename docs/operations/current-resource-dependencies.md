# 기존 Weather 운영 리소스 의존성

## 상태

이 문서는 secret 값을 저장하지 않는다. 1차 저장소 분리 동안 기존 ASK Seoul 리소스는 외부 의존성으로 남아 있으며 새 저장소가 소유하지 않는다.

| 논리 리소스 | 역할 | 현재 상태 | owner/사용 승인 | 종료 시 영향 |
|---|---|---|---|---|
| Existing R2 raw | KMA raw·manifest 저장 | 확인 필요 | 확인 필요 | 신규 Bronze replay 불가 |
| Existing Iceberg/Trino | Bronze·Silver·Gold query | 로컬 서비스 running 관찰 | 확인 필요 | dbt run·product 검증 불가 |
| Existing D1 | public product publication | 확인 필요 | 확인 필요 | origin이 최신 publication을 읽지 못함 |
| Existing Weather origin | `/skill/v1/...` 제공 | 확인 필요 | 확인 필요 | hosted proxy upstream 실패 |
| NomaDamas hosted proxy | K-Skill public route | 외부 upstream | NomaDamas | 설치된 skill query 실패 |

## Airflow 배포 승인 gate

현재 로컬 Airflow 파이프라인이 별도 compose project에서 실행 중인 것이 read-only inspection으로 관찰됐다. 새 저장소의 Airflow 코드를 배포하거나 DAG를 가동하기 전에는 반드시 사용자에게 먼저 다음을 보고한다.

1. 중지·drain해야 할 기존 로컬 DAG와 run
2. 변경할 container와 mount
3. 배포 전후 health/import/freshness 검증
4. 실패 시 rollback 경로

사용자의 명시 승인 전에는 기존 파이프라인을 pause·stop·restart하거나 새 DAG를 enable·trigger하지 않는다.

## Exit criterion

다음 조건이 확인돼야 기존 리소스 의존성을 제거할 수 있다.

- 개인 R2/Iceberg/D1 생성과 권한 검증
- raw checksum copy 또는 KMA 재수집 결정
- 개인 환경 Bronze → Silver → Gold replay
- origin shadow comparison과 hosted proxy rollback 준비
