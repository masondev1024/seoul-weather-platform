from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePath
from typing import Any, Iterator

from deployment.models import WriterRunCounts


@dataclass(frozen=True)
class FakeAdapterEvent:
    operation: str
    payload: tuple[tuple[str, Any], ...]


class _FakeBase:
    def __init__(
        self,
        events: list[FakeAdapterEvent],
        failures: dict[str, BaseException] | None = None,
        scripts: dict[str, list[object]] | None = None,
    ):
        self.events = events
        self.failures = failures or {}
        self.scripts = {operation: list(values) for operation, values in (scripts or {}).items()}

    def _event(self, operation: str, *, default: object = None, **payload: Any) -> object:
        self.events.append(FakeAdapterEvent(operation, tuple(sorted(payload.items()))))
        if operation in self.failures:
            raise self.failures[operation]
        scripted = self.scripts.get(operation)
        if scripted:
            result = scripted.pop(0)
            if isinstance(result, BaseException):
                raise result
            return result
        return default


class FakeAirflowAdapter(_FakeBase):
    def __init__(
        self,
        events: list[FakeAdapterEvent],
        paused: dict[str, bool] | None = None,
        run_counts: object = None,
        failures: dict[str, BaseException] | None = None,
        scripts: dict[str, list[object]] | None = None,
    ):
        super().__init__(events, failures, scripts)
        self.paused = dict(paused or {})
        self.run_counts = run_counts

    def capture_pause_state(self, dag_ids: tuple[str, ...]) -> dict[str, bool]:
        result = self._event(
            "airflow.capture_pause_state", default=dict(self.paused), dag_ids=dag_ids
        )
        return dict(result) if type(result) is dict else result

    def writer_run_counts(self, dag_ids: tuple[str, ...]) -> WriterRunCounts:
        return self._event(
            "airflow.writer_run_counts", default=self.run_counts, dag_ids=dag_ids
        )

    def pause_dag(self, target, dag_id: str) -> None:
        self._event("airflow.pause_dag", dag_id=dag_id)
        self.paused[dag_id] = True

    def unpause_dag(self, target, dag_id: str) -> None:
        self._event("airflow.unpause_dag", dag_id=dag_id)
        self.paused[dag_id] = False


class FakeComposeAdapter(_FakeBase):
    def validate_candidate(self, target, overlay_file: PurePath) -> None:
        self._event("compose.validate_candidate", overlay_file=overlay_file)

    def deploy_code_services(self, target, overlay_file: PurePath, services: tuple[str, ...]) -> None:
        self._event("compose.deploy_code_services", overlay_file=overlay_file, services=services)


class FakeGitAdapter(_FakeBase):
    def __init__(self, events: list[FakeAdapterEvent], checkout_root: PurePath, failures: dict[str, BaseException] | None = None):
        super().__init__(events, failures)
        self.checkout_root = checkout_root

    def detached_checkout(self, repository: str, candidate_sha: str, checkout_root: PurePath) -> PurePath:
        self._event("git.detached_checkout", repository=repository, candidate_sha=candidate_sha, checkout_root=checkout_root)
        return self.checkout_root


class FakeHealthAdapter(_FakeBase):
    def __init__(
        self,
        events: list[FakeAdapterEvent],
        result: str,
        failures: dict[str, BaseException] | None = None,
        scripts: dict[str, list[object]] | None = None,
    ):
        super().__init__(events, failures, scripts)
        self.result = result

    def read_health(self, target, expected_overlay) -> str:
        return self._event(
            "health.read_health",
            default=self.result,
            expected_overlay=expected_overlay,
        )


class FakeClock(_FakeBase):
    def __init__(
        self,
        events: list[FakeAdapterEvent],
        utc_values: list[str],
        monotonic_values: list[float],
        failures: dict[str, BaseException] | None = None,
    ):
        super().__init__(events, failures)
        self.utc_values = utc_values
        self.monotonic_values = monotonic_values

    def utc_now(self) -> str:
        self._event("clock.utc_now")
        return self.utc_values.pop(0)

    def monotonic(self) -> float:
        self._event("clock.monotonic")
        return self.monotonic_values.pop(0)

    def sleep(self, seconds: float) -> None:
        self._event("clock.sleep", seconds=seconds)


class FakeOverlayStore(_FakeBase):
    def __init__(
        self,
        events: list[FakeAdapterEvent],
        staged_path: Path | None = None,
        failures: dict[str, BaseException] | None = None,
        scripts: dict[str, list[object]] | None = None,
    ):
        super().__init__(events, failures, scripts)
        self.staged_path = staged_path or Path("fake-staged-overlay.tmp")

    def stage(self, artifact) -> Path:
        self._event("overlay.stage", artifact=artifact)
        return self.staged_path

    def install(self, staged: Path, artifact) -> None:
        self._event("overlay.install", staged=staged, artifact=artifact)

    def verify_installed(self, expected) -> None:
        self._event("overlay.verify_installed", expected=expected)

    def restore(self, content: bytes, sha256: str) -> None:
        self._event("overlay.restore", content=content, sha256=sha256)

    def discard(self, staged: Path) -> None:
        self._event("overlay.discard", staged=staged)


class FakeDeploymentLedger(_FakeBase):
    def __init__(
        self,
        events: list[FakeAdapterEvent],
        previous=None,
        baseline_record=None,
        already_successful: bool = False,
        summary: dict[str, object] | None = None,
        failures: dict[str, BaseException] | None = None,
        scripts: dict[str, list[object]] | None = None,
    ):
        super().__init__(events, failures, scripts)
        self.previous = previous
        self.baseline_record = baseline_record
        self.already_successful_value = already_successful
        self.summary = summary or {"baseline": None, "previous_success": None}

    @contextmanager
    def acquire_lock(self, deployment_id: str) -> Iterator[None]:
        self._event("ledger.acquire_lock.enter", deployment_id=deployment_id)
        try:
            yield
        finally:
            self._event("ledger.acquire_lock.exit", deployment_id=deployment_id)

    def begin(self, record) -> None:
        self._event("ledger.begin", record=record)

    def complete(self, record) -> None:
        self._event("ledger.complete", record=record)

    def record_baseline(self, record) -> None:
        self._event("ledger.record_baseline", record=record)

    def previous_success(self, target_fingerprint: str):
        self._event("ledger.previous_success", target_fingerprint=target_fingerprint)
        return self.previous

    def baseline(self, target_fingerprint: str):
        self._event("ledger.baseline", target_fingerprint=target_fingerprint)
        return self.baseline_record

    def already_successful(self, deployment_id: str) -> bool:
        self._event("ledger.already_successful", deployment_id=deployment_id)
        return self.already_successful_value

    def read_summary(self) -> dict[str, object]:
        self._event("ledger.read_summary")
        return dict(self.summary)
