# Airflow 운영 변경 승인 기준

이 문서는 저장소의 코드 검증과 실제 운영 변경을 구분하기 위한 공통 기준이다. 저장소 테스트가 통과해도 외부 R2, Iceberg, Trino, D1 또는 로컬 Airflow에 자동으로 쓰기 권한이 생기지는 않는다.

## AIRFLOW_DEPLOYMENT_APPROVAL_REQUIRED

Airflow 관련 상태 변경 전에는 변경 대상, 영향 범위, 복구 방법을 먼저 공유하고 명시적인 운영 승인을 받는다.

사전 승인 없이 수행하지 않는 작업:

- Airflow 이미지 빌드 또는 배포
- scheduler, dag-processor, api-server, triggerer 재생성·재시작
- DAG 활성화 또는 unpause
- 수동 트리거와 backfill
- 기존 로컬 파이프라인 중지·재시작
- R2, Iceberg, Trino, D1에 대한 write 또는 대량 재처리

배포가 필요할 때 기록할 항목:

1. 대상 commit과 변경 service
2. 중지·drain 대상 DAG와 running/queued run
3. pause, drain, deploy, health check, rollback 순서
4. dbt/Trino/R2/D1 영향과 data write 여부

승인 전에는 저장소 테스트, secretless 정적 검사, 읽기 전용 점검만 수행한다. 이 문서는 실행 권한이 아니라 운영 변경을 통제하기 위한 체크 기준이다.
