# 개인 Weather 운영 리소스 의존성

## 상태

이 문서는 secret 값을 저장하지 않는다. Cloud resource는 Mason의 개인 계정에 있지만 public repository와 분리된 private operations plane이다.

| 논리 리소스 | 역할 | 현재 상태 | owner/사용 승인 | 종료 시 영향 |
|---|---|---|---|---|
| Personal R2 raw/Data Catalog | KMA raw·Iceberg 저장 | 운영 중 | Mason | 신규 Bronze와 dbt query 불가 |
| Local Trino | Bronze·Silver·Gold query | 5 GiB limit, FS cache 적용 | Mason의 로컬 노트북 | transform·snapshot 중단 |
| Personal D1 | public product publication | 운영 중 | Mason Cloudflare | origin 최신 발행본 미제공 |
| Personal Weather origin | `/skill/v1/...` 제공 | 운영 중 | Mason Cloudflare | proxy upstream 실패 |
| Personal K-Skill proxy | 공개 3-route forwarding | 이 저장소에서 관리 | Mason Cloudflare | 기본 helper의 개인 origin 조회 실패 |
| NomaDamas hosted proxy | 기본 K-Skill route | 조직 origin stale 관측 | NomaDamas | 기본 설정 query가 503 |

## Airflow 배포 승인 gate

현재 로컬 Airflow 파이프라인이 별도 compose project에서 실행 중인 것이 read-only inspection으로 관찰됐다. 새 저장소의 Airflow 코드를 배포하거나 DAG를 가동하기 전에는 반드시 사용자에게 먼저 다음을 보고한다.

1. 중지·drain해야 할 기존 로컬 DAG와 run
2. 변경할 container와 mount
3. 배포 전후 health/import/freshness 검증
4. 실패 시 rollback 경로

사용자의 명시 승인 전에는 기존 파이프라인을 pause·stop·restart하거나 새 DAG를 enable·trigger하지 않는다.

## 운영 완료 기준

다음 조건을 함께 확인해야 end-to-end 정상으로 판정한다.

- Docker health, restart, OOM 상태 정상
- 최신 Bronze → Silver → Gold run 성공
- D1 publication/watchdog 성공과 API readback 일치
- 개인 proxy 경유 K-Skill query 200 및 publication identity 확인
