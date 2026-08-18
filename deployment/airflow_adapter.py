from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath

from deployment.command import CommandRunner, CompletedCommand
from deployment.models import WriterRunCounts
from deployment.output_contracts import (
    is_airflow_322_noop,
    parse_airflow_bool,
    parse_airflow_json_rows,
)
from deployment.target import DeployTarget


class AirflowAdapterError(RuntimeError):
    """A fixed, redacted Airflow adapter failure category."""


_UNSAFE = re.compile(r"[|;&`$<>\x00-\x1f\x7f]")
_IDEMPOTENT_NOOP_MESSAGES = {
    ("pause", True): "No unpaused DAGs were found",
    ("unpause", False): "No paused DAGs were found",
}


def _safe_atom(value: object) -> str:
    if type(value) is not str or not value or value.startswith("-") or _UNSAFE.search(value):
        raise AirflowAdapterError("airflow_adapter_input_rejected")
    return value


def _safe_path(value: PurePath) -> str:
    raw = _safe_atom(str(value))
    parts = re.split(r"[\\/]", raw)
    if "." in parts or ".." in parts:
        raise AirflowAdapterError("airflow_adapter_input_rejected")
    windows = PureWindowsPath(raw)
    posix = PurePosixPath(raw)
    if not windows.is_absolute() and not posix.is_absolute():
        raise AirflowAdapterError("airflow_adapter_input_rejected")
    return raw


def _json_rows(stdout: str) -> list[Mapping[str, object]]:
    try:
        return parse_airflow_json_rows(stdout)
    except Exception:
        raise AirflowAdapterError("airflow_adapter_invalid_output") from None


class AirflowCommandAdapter:
    def __init__(self, target: DeployTarget, runner: CommandRunner) -> None:
        self._target = target
        self._runner = runner
        project = _safe_atom(target.project_name)
        control = _safe_atom(target.control_service)
        files = tuple(_safe_path(path) for path in target.compose_files)
        stable = _safe_path(target.generated_overlay_file)
        for dag_id in target.dag_allowlist | target.writer_dag_allowlist:
            _safe_atom(dag_id)
        prefix: list[str] = ["docker", "compose", "-p", project]
        for compose_file in files:
            prefix.extend(("-f", compose_file))
        prefix.extend(("-f", stable, "exec", "-T", control, "airflow"))
        self._prefix = tuple(prefix)
        self._cwd = Path(_safe_path(target.working_directory))
        self._dags = tuple(sorted(target.dag_allowlist))
        self._writers = tuple(sorted(target.writer_dag_allowlist))

    def _checked(self, argv: Sequence[str]) -> str:
        try:
            result: CompletedCommand = self._runner.run(argv, self._cwd)
        except Exception:
            raise AirflowAdapterError("airflow_adapter_command_failed") from None
        if result.returncode != 0 or result.stderr:
            raise AirflowAdapterError("airflow_adapter_command_failed")
        return result.stdout

    def capture_pause_state(self, dag_ids: tuple[str, ...]) -> dict[str, bool]:
        if type(dag_ids) is not tuple or dag_ids != self._dags:
            raise AirflowAdapterError("airflow_adapter_input_rejected")
        rows = _json_rows(
            self._checked((*self._prefix, "dags", "list", "-o", "json"))
        )
        paused: dict[str, bool] = {}
        for row in rows:
            dag_id = row.get("dag_id")
            try:
                is_paused = parse_airflow_bool(row.get("is_paused"))
            except Exception:
                raise AirflowAdapterError("airflow_adapter_invalid_output") from None
            if type(dag_id) is not str or not dag_id:
                raise AirflowAdapterError("airflow_adapter_invalid_output")
            if dag_id not in self._target.dag_allowlist:
                continue
            if dag_id in paused:
                if paused[dag_id] != is_paused:
                    raise AirflowAdapterError("airflow_adapter_invalid_output")
                continue
            paused[dag_id] = is_paused
        # allowlist 에 있지만 아직 배포되지 않은 새 DAG 는 running Airflow 의 dagbag 에
        # 없을 수 있다(DAG 를 추가하는 배포). 없는 DAG 는 되돌릴 이전 상태가 없으므로
        # 안전측인 paused=True 로 채워 스냅샷을 완성한다 — 복원은 이 스냅샷을 따르므로
        # 새 DAG 는 배포 후에도 계속 paused 로 남고(운영자가 의도적으로 unpause),
        # 없는 DAG 에 대한 pause/unpause 는 Airflow 가 idempotent noop 으로 처리한다
        # (실측: "No unpaused DAGs were found"). 단 allowlist DAG 가 단 하나도 존재하지
        # 않으면 dagbag 이 통째로 깨진 것으로 보고 거부한다(기존 strict 검사의 최소 안전선).
        if not paused:
            raise AirflowAdapterError("airflow_adapter_invalid_output")
        return {dag_id: paused.get(dag_id, True) for dag_id in self._dags}

    def _present_allowlisted_dag_ids(self) -> frozenset[str]:
        """running Airflow dagbag 에 실제로 존재하는 allowlist DAG 집합."""
        rows = _json_rows(
            self._checked((*self._prefix, "dags", "list", "-o", "json"))
        )
        present: set[str] = set()
        for row in rows:
            dag_id = row.get("dag_id")
            if type(dag_id) is str and dag_id in self._target.dag_allowlist:
                present.add(dag_id)
        return frozenset(present)

    def writer_run_counts(self, dag_ids: tuple[str, ...]) -> WriterRunCounts:
        if type(dag_ids) is not tuple or dag_ids != self._writers:
            raise AirflowAdapterError("airflow_adapter_input_rejected")
        # 아직 배포되지 않은 새 writer DAG 는 dagbag 에 없어 `dags list-runs` 가
        # "does not exist" 를 뱉는다(실측). 없는 DAG 는 run 이 있을 수 없으므로
        # drain 대상에서 건너뛴다.
        present = self._present_allowlisted_dag_ids()
        totals = {"running": 0, "queued": 0}
        for dag_id in self._writers:
            if dag_id not in present:
                continue
            for state in ("running", "queued"):
                rows = _json_rows(
                    self._checked(
                        (
                            *self._prefix,
                            "dags",
                            "list-runs",
                            "--state",
                            state,
                            "-o",
                            "json",
                            dag_id,
                        )
                    )
                )
                run_ids: set[str] = set()
                for row in rows:
                    run_id = row.get("run_id")
                    if (
                        row.get("dag_id") != dag_id
                        or row.get("state") != state
                        or type(run_id) is not str
                        or not run_id
                        or run_id in run_ids
                    ):
                        raise AirflowAdapterError("airflow_adapter_invalid_output")
                    run_ids.add(run_id)
                totals[state] += len(run_ids)
        return WriterRunCounts(running=totals["running"], queued=totals["queued"])

    def pause_dag(self, target: DeployTarget, dag_id: str) -> None:
        self._set_pause_state(target, dag_id, operation="pause", expected=True)

    def unpause_dag(self, target: DeployTarget, dag_id: str) -> None:
        self._set_pause_state(target, dag_id, operation="unpause", expected=False)

    def _set_pause_state(
        self, target: DeployTarget, dag_id: str, *, operation: str, expected: bool
    ) -> None:
        if target != self._target or dag_id not in self._target.dag_allowlist:
            raise AirflowAdapterError("airflow_adapter_input_rejected")
        expected_noop = _IDEMPOTENT_NOOP_MESSAGES.get((operation, expected))
        if expected_noop is None:
            raise AirflowAdapterError("airflow_adapter_input_rejected")
        stdout = self._checked(
            (
                *self._prefix,
                "dags",
                operation,
                "-o",
                "json",
                "-y",
                dag_id,
            )
        )
        if is_airflow_322_noop(stdout, expected_noop):
            return
        rows = _json_rows(stdout)
        if (
            len(rows) != 1
            or rows[0].get("dag_id") != dag_id
        ):
            raise AirflowAdapterError("airflow_adapter_invalid_output")
        try:
            observed = parse_airflow_bool(rows[0].get("is_paused"))
        except Exception:
            raise AirflowAdapterError("airflow_adapter_invalid_output") from None
        # Airflow 3.2.2 reports the state before a successful pause/unpause
        # transition. The orchestrator verifies the resulting state separately
        # with capture_pause_state after the mutation batch.
        if observed is expected:
            raise AirflowAdapterError("airflow_adapter_invalid_output")
