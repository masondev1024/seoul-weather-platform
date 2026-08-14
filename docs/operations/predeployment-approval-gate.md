# Airflow 사전 승인 게이트

## 적용 범위

Airflow 이미지 build/deploy, scheduler·dag-processor·api-server·triggerer의 recreate/restart, DAG enable/unpause, trigger, backfill, collection/transform/publication pipeline의 start/stop에는 사용자 사전 승인이 필요하다.

## 승인 전 허용 작업

승인 전에는 repository policy, provenance, unit test 같은 secretless·read-only 검증만 수행한다. 표준 검증 경로는 다음이며 Airflow·Docker·컨테이너·파이프라인을 호출하지 않는다.

```powershell
./tools/verify_repository.ps1
```

이 스크립트는 `runtime/toolchain.lock.json`의 Python/Airflow/dbt/adapter/Node 고정값을 읽고, 현재 Python minor가 계약과 일치하는지 확인한 다음 아래 경로만 실행한다.

```text
python -m tools.repository_policy --repo-root <repository>
python -m tools.verify_provenance --repo-root <repository>
python -m pytest tests/repository
```

## 필수 사전 통지와 승인

운영 변경을 요청하기 전에 사용자에게 기존 로컬 파이프라인을 중지할 수 있도록 먼저 알린다. 보고에는 다음을 포함한다.

1. 배포 대상 commit과 변경될 서비스
2. 기존 로컬 파이프라인에서 중지할 DAG 및 running/queued run
3. pause·drain·배포·health check·rollback의 순서
4. dbt/Trino/D1 영향과 데이터 write 여부

사용자가 위 통지를 확인하고 명시적으로 승인할 때까지 어떤 Airflow state change도 수행하지 않는다. 사전 승인이 없으면 deploy/restart/enable/unpause/trigger/backfill/start/stop을 실행하지 않는다.

## 승인 후에도 필요한 절차

승인 자체는 안전한 전환을 대체하지 않는다. 기존 writer를 pause하고 running transform을 drain한 뒤, 변경 대상만 배포한다. health·DAG import·manifest를 확인한 다음 writer 충돌이 없을 때에만 정해진 전환 절차로 DAG를 활성화한다. 실패하면 last-known-good 상태로 rollback한다.
