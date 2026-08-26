# Secretless DagBag validation

`tools/verify_dagbag.ps1` creates one isolated, one-off container from the Airflow digest in `runtime/toolchain.lock.json`. It is deliberately separate from `tools/verify_repository.ps1`.

The command uses `docker run` with all of the following boundaries:

- `--network none` and no credential environment variables;
- `--read-only`, with `/tmp` as the only writable `tmpfs`;
- repository `dags/`, `dbt/`, 검증용 `tools/`를 read-only로 mount;
- `PYTHONDONTWRITEBYTECODE=1`, 격리된 `/tmp`, `AIRFLOW_HOME=/tmp/airflow`;
- metadata DB migration이나 Airflow CLI 없이 `DagBag` 객체만 직접 load;
- import error 0개와 versioned Weather DAG ID allowlist를 함께 검증.

Print the fully resolved command without starting Docker:

```powershell
powershell -File tools/verify_dagbag.ps1 -PrintCommand
```

Run the check only after reviewing that output and confirming the local Docker daemon is available:

```powershell
powershell -File tools/verify_dagbag.ps1
```

이 harness는 Docker Compose나 기존 Airflow service를 제어하지 않는다. DAG trigger, unpause, backfill, deploy, stop, restart도 수행하지 않는다.
