# 비밀값 없는 DAG 읽기 검사

`tools/verify_dagbag.ps1`는 `runtime/toolchain.lock.json`에 고정한 Airflow 이미지로
네트워크 없는 일회성 컨테이너를 만든다. 기존 Compose나 실행 중인 Airflow와 분리되어
있다.

컨테이너에는 다음 제한을 건다.

- `--network none`, 자격증명 환경 변수 없음
- `--read-only`, 쓰기 가능한 곳은 임시 `tmpfs` 하나뿐
- 저장소의 `dags/`, `dbt/`, 검사 도구를 읽기 전용으로 연결
- `PYTHONDONTWRITEBYTECODE=1`, 격리된 `/tmp`, `AIRFLOW_HOME=/tmp/airflow`
- 메타데이터 DB 변경이나 Airflow CLI 없이 `DagBag` 객체만 읽음
- 가져오기 오류 0개와 Weather DAG ID 정확한 집합을 함께 확인

먼저 실행할 명령만 출력한다.

```powershell
powershell -File tools/verify_dagbag.ps1 -PrintCommand
```

출력을 확인하고 Docker daemon이 준비된 뒤에만 실제 검사를 실행한다.

```powershell
powershell -File tools/verify_dagbag.ps1
```

이 검사는 Compose, 기존 Airflow 서비스, DAG 실행·활성화·backfill·배포·중지·재시작을
건드리지 않는다.
