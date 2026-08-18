from __future__ import annotations

import base64
import binascii
import re
from dataclasses import dataclass
from pathlib import Path

from deployment.adapters import (
    AirflowMutationAdapter,
    AirflowReadAdapter,
    Clock,
    ComposeAdapter,
    DeploymentLedgerAdapter,
    GitAdapter,
    HealthAdapter,
    OverlayStore,
)
from deployment.main_identity import MainDeployIdentity
from deployment.models import (
    BaselineRecord,
    DagStateSnapshot,
    DeploymentOutcome,
    DeploymentRecord,
    DeploymentResult,
    DeploymentTerminalCategory,
    WriterRunCounts,
    deployment_id,
)
from deployment.overlay import OverlayArtifact, render_release_overlay, validate_overlay_content
from deployment.target import DeployTarget, target_fingerprint


_UTC_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_SAFE_CATEGORIES = frozenset(
    {
        "main-deploy-lock-failed",
        "main-deploy-ledger-failed",
        "main-deploy-rollback-unavailable",
        "main-deploy-rollback-candidate-invalid",
        "main-deploy-snapshot-invalid",
        "main-deploy-pause-verification-failed",
        "main-deploy-writer-counts-invalid",
        "main-deploy-drain-timeout",
        "main-deploy-drain-failed",
        "main-deploy-checkout-failed",
        "main-deploy-candidate-stage-failed",
        "main-deploy-candidate-validation-failed",
        "main-deploy-failed",
        "main-deploy-rolled-back",
        "main-deploy-rollback-failed",
        "main-deploy-pause-state-unverified",
    }
)


class MainDeploymentError(RuntimeError):
    def __init__(self, category: str) -> None:
        if category not in _SAFE_CATEGORIES:
            category = "main-deploy-failed"
        self.category = category
        super().__init__(category)


class _StageFailure(Exception):
    def __init__(self, category: str) -> None:
        self.category = category
        super().__init__(category)


@dataclass(frozen=True)
class _RollbackCandidate:
    artifact: OverlayArtifact


class MainDeploymentOrchestrator:
    def __init__(
        self,
        *,
        airflow_read: AirflowReadAdapter,
        airflow_mutation: AirflowMutationAdapter,
        compose: ComposeAdapter,
        git: GitAdapter,
        health: HealthAdapter,
        clock: Clock,
        overlay_store: OverlayStore,
        ledger: DeploymentLedgerAdapter,
    ) -> None:
        self.airflow_read = airflow_read
        self.airflow_mutation = airflow_mutation
        self.compose = compose
        self.git = git
        self.health = health
        self.clock = clock
        self.overlay_store = overlay_store
        self.ledger = ledger

    def deploy(
        self, identity: MainDeployIdentity, target: DeployTarget
    ) -> DeploymentResult:
        try:
            fingerprint = target_fingerprint(target)
            current_id = deployment_id(
                identity.repository, identity.workflow_sha, fingerprint
            )
        except Exception:
            raise MainDeploymentError("main-deploy-failed") from None

        try:
            with self.ledger.acquire_lock(current_id):
                return self._deploy_locked(identity, target, fingerprint, current_id)
        except MainDeploymentError:
            raise
        except Exception:
            raise MainDeploymentError("main-deploy-lock-failed") from None

    def _deploy_locked(
        self,
        identity: MainDeployIdentity,
        target: DeployTarget,
        fingerprint: str,
        current_id: str,
    ) -> DeploymentResult:
        try:
            if self.ledger.already_successful(current_id):
                return DeploymentResult(
                    deployment_id=current_id,
                    outcome=DeploymentOutcome.SUCCESS,
                    health="passed",
                    idempotent=True,
                )
            rollback = self._rollback_candidate(identity, target, fingerprint)
        except MainDeploymentError:
            raise
        except Exception:
            raise MainDeploymentError("main-deploy-ledger-failed") from None

        dags = tuple(sorted(target.dag_allowlist))
        try:
            snapshot_raw = self.airflow_read.capture_pause_state(dags)
        except Exception:
            raise MainDeploymentError("main-deploy-snapshot-invalid") from None
        snapshot = self._snapshot(snapshot_raw, dags)

        try:
            started_at = self.clock.utc_now()
            self._require_timestamp(started_at)
            started = self._record(
                identity=identity,
                fingerprint=fingerprint,
                deployment_id_value=current_id,
                started_at=started_at,
                completed_at=None,
                outcome=DeploymentOutcome.STARTED,
                health=None,
                artifact=None,
            )
            self.ledger.begin(started)
        except Exception:
            raise MainDeploymentError("main-deploy-ledger-failed") from None

        staged: Path | None = None
        install_started = False
        try:
            # 순서가 중요하다. pause 를 먼저 걸면 Airflow 가 **이미 도는 run 의 남은
            # 태스크까지** 스케줄하지 않아 그 run 이 그 자리에서 멈추고, drain 은
            # running=0 을 영원히 못 봐서 반드시 timeout 난다(2026-08-16 실측:
            # bronze 가 land 와 load 사이에서 23분 멈춘 채 배포가 drain-timeout).
            # 그래서 아직 안 멈춘 파이프라인을 먼저 비우고(drain), 그 다음 새 run 을
            # 막는다(pause).
            self._drain(target)
            self._pause_all(target, dags)
            expected_checkout = target.runtime_root / "releases" / identity.workflow_sha
            try:
                checkout = self.git.detached_checkout(
                    identity.repository, identity.workflow_sha, expected_checkout
                )
            except Exception:
                raise _StageFailure("main-deploy-checkout-failed") from None
            if type(checkout) is not type(expected_checkout) or checkout != expected_checkout:
                raise _StageFailure("main-deploy-checkout-failed")
            try:
                candidate = render_release_overlay(
                    target, checkout, identity.workflow_sha
                )
                staged = self.overlay_store.stage(candidate)
            except Exception:
                raise _StageFailure("main-deploy-candidate-stage-failed") from None
            try:
                self.compose.validate_candidate(target, staged)
            except Exception:
                raise _StageFailure(
                    "main-deploy-candidate-validation-failed"
                ) from None

            install_started = True
            self.overlay_store.install(staged, candidate)
            self.overlay_store.verify_installed(candidate)
            self.compose.deploy_code_services(
                target,
                target.generated_overlay_file,
                tuple(sorted(target.airflow_code_services)),
            )
            if self.health.read_health(target, candidate) != "passed":
                raise _StageFailure("main-deploy-failed")
            self._restore_snapshot(target, snapshot)
            completed_at = self.clock.utc_now()
            self._require_timestamp(completed_at)
            completed = self._record(
                identity=identity,
                fingerprint=fingerprint,
                deployment_id_value=current_id,
                started_at=started_at,
                completed_at=completed_at,
                outcome=DeploymentOutcome.SUCCESS,
                health="passed",
                artifact=candidate,
            )
            self.ledger.complete(completed)
            return DeploymentResult(
                deployment_id=current_id,
                outcome=DeploymentOutcome.SUCCESS,
                health="passed",
                idempotent=False,
            )
        except _StageFailure as error:
            if install_started:
                self._rollback(
                    identity,
                    target,
                    fingerprint,
                    current_id,
                    started_at,
                    snapshot,
                    rollback,
                )
            self._fail_before_install(
                identity,
                target,
                fingerprint,
                current_id,
                started_at,
                snapshot,
                staged,
                error.category,
            )
        except Exception:
            if install_started:
                self._rollback(
                    identity,
                    target,
                    fingerprint,
                    current_id,
                    started_at,
                    snapshot,
                    rollback,
                )
            self._fail_before_install(
                identity,
                target,
                fingerprint,
                current_id,
                started_at,
                snapshot,
                staged,
                "main-deploy-failed",
            )
        raise AssertionError("unreachable")

    def _rollback_candidate(
        self,
        identity: MainDeployIdentity,
        target: DeployTarget,
        fingerprint: str,
    ) -> _RollbackCandidate:
        previous = self.ledger.previous_success(fingerprint)
        if previous is not None:
            artifact = self._validate_previous(previous, identity, target, fingerprint)
            return _RollbackCandidate(artifact)
        baseline = self.ledger.baseline(fingerprint)
        if baseline is None:
            raise MainDeploymentError("main-deploy-rollback-unavailable")
        artifact = self._validate_baseline(baseline, target, fingerprint)
        return _RollbackCandidate(artifact)

    def _validate_previous(
        self,
        record: DeploymentRecord,
        identity: MainDeployIdentity,
        target: DeployTarget,
        fingerprint: str,
    ) -> OverlayArtifact:
        try:
            if (
                type(record) is not DeploymentRecord
                or record.schema_version
                not in {
                    "weather-local-deployment-record/v1",
                    "weather-local-deployment-record/v2",
                }
                or record.repository != identity.repository
                or record.target_fingerprint != fingerprint
                or record.candidate_sha == identity.workflow_sha
                or record.outcome is not DeploymentOutcome.SUCCESS
                or record.health != "passed"
                or record.completed_at is None
                or record.overlay_content_b64 is None
                or record.overlay_sha256 is None
            ):
                raise ValueError
            self._require_timestamp(record.started_at)
            self._require_timestamp(record.completed_at)
            if record.deployment_id != deployment_id(
                record.repository, record.candidate_sha, fingerprint
            ):
                raise ValueError
            content = self._decode_overlay(record.overlay_content_b64)
            artifact = validate_overlay_content(
                target, content, record.overlay_sha256
            )
            if (
                artifact.kind != "release"
                or artifact.candidate_sha != record.candidate_sha
            ):
                raise ValueError
            return artifact
        except Exception:
            raise MainDeploymentError(
                "main-deploy-rollback-candidate-invalid"
            ) from None

    def _validate_baseline(
        self,
        record: BaselineRecord,
        target: DeployTarget,
        fingerprint: str,
    ) -> OverlayArtifact:
        try:
            if (
                type(record) is not BaselineRecord
                or record.schema_version != "weather-local-baseline-record/v1"
                or record.baseline_id != "baseline://existing-local"
                or record.target_fingerprint != fingerprint
                or record.rehearsal != "passed"
            ):
                raise ValueError
            self._require_timestamp(record.captured_at)
            content = self._decode_overlay(record.overlay_content_b64)
            artifact = validate_overlay_content(
                target, content, record.overlay_sha256
            )
            if artifact.kind != "baseline" or artifact.candidate_sha is not None:
                raise ValueError
            return artifact
        except Exception:
            raise MainDeploymentError(
                "main-deploy-rollback-candidate-invalid"
            ) from None

    @staticmethod
    def _decode_overlay(value: str) -> bytes:
        if type(value) is not str:
            raise ValueError
        try:
            return base64.b64decode(value.encode("ascii"), validate=True)
        except (UnicodeEncodeError, binascii.Error):
            raise ValueError from None

    @staticmethod
    def _require_timestamp(value: str) -> None:
        if type(value) is not str or _UTC_TIMESTAMP.fullmatch(value) is None:
            raise ValueError("invalid timestamp")

    @staticmethod
    def _snapshot(
        value: object, dags: tuple[str, ...]
    ) -> DagStateSnapshot:
        if (
            type(value) is not dict
            or set(value) != set(dags)
            or any(type(value[dag_id]) is not bool for dag_id in dags)
        ):
            raise MainDeploymentError("main-deploy-snapshot-invalid")
        return DagStateSnapshot(
            paused=tuple((dag_id, value[dag_id]) for dag_id in dags),
            run_counts=(),
        )

    def _pause_all(self, target: DeployTarget, dags: tuple[str, ...]) -> None:
        try:
            for dag_id in dags:
                self.airflow_mutation.pause_dag(target, dag_id)
        except Exception:
            raise _StageFailure("main-deploy-failed") from None
        if not self._pause_state_matches(dags, {dag_id: True for dag_id in dags}):
            raise _StageFailure("main-deploy-pause-verification-failed")

    def _drain(self, target: DeployTarget) -> None:
        writers = tuple(sorted(target.writer_dag_allowlist))
        try:
            deadline = self.clock.monotonic() + target.drain_timeout_seconds
            first_poll = True
            while True:
                if not first_poll and self.clock.monotonic() >= deadline:
                    raise _StageFailure("main-deploy-drain-timeout")
                counts = self.airflow_read.writer_run_counts(writers)
                if type(counts) is not WriterRunCounts:
                    raise _StageFailure("main-deploy-writer-counts-invalid")
                if counts.running == 0 and counts.queued == 0:
                    return
                remaining = deadline - self.clock.monotonic()
                if remaining <= 0:
                    raise _StageFailure("main-deploy-drain-timeout")
                self.clock.sleep(min(float(target.poll_interval_seconds), remaining))
                first_poll = False
        except _StageFailure:
            raise
        except Exception:
            raise _StageFailure("main-deploy-drain-failed") from None

    def _restore_snapshot(
        self, target: DeployTarget, snapshot: DagStateSnapshot
    ) -> None:
        for dag_id, was_paused in snapshot.paused:
            if was_paused is False:
                self.airflow_mutation.unpause_dag(target, dag_id)
        expected = dict(snapshot.paused)
        if not self._pause_state_matches(tuple(expected), expected):
            raise RuntimeError("pause state mismatch")

    def _pause_state_matches(
        self, dags: tuple[str, ...], expected: dict[str, bool]
    ) -> bool:
        try:
            readback = self.airflow_read.capture_pause_state(dags)
        except Exception:
            return False
        return (
            type(readback) is dict
            and set(readback) == set(dags)
            and all(
                type(readback[dag_id]) is bool
                and readback[dag_id] is expected[dag_id]
                for dag_id in dags
            )
        )

    def _fail_before_install(
        self,
        identity: MainDeployIdentity,
        target: DeployTarget,
        fingerprint: str,
        current_id: str,
        started_at: str,
        snapshot: DagStateSnapshot,
        staged: Path | None,
        category: str,
    ) -> None:
        if staged is not None:
            try:
                self.overlay_store.discard(staged)
            except Exception:
                pass
        try:
            self._restore_snapshot(target, snapshot)
        except Exception:
            self._fail_closed(
                identity,
                target,
                fingerprint,
                current_id,
                started_at,
            )
        try:
            completed_at = self.clock.utc_now()
            self._require_timestamp(completed_at)
            self.ledger.complete(
                self._record(
                    identity=identity,
                    fingerprint=fingerprint,
                    deployment_id_value=current_id,
                    started_at=started_at,
                    completed_at=completed_at,
                    outcome=DeploymentOutcome.FAILED,
                    health=None,
                    artifact=None,
                )
            )
        except Exception:
            raise MainDeploymentError("main-deploy-ledger-failed") from None
        raise MainDeploymentError(category)

    def _rollback(
        self,
        identity: MainDeployIdentity,
        target: DeployTarget,
        fingerprint: str,
        current_id: str,
        started_at: str,
        snapshot: DagStateSnapshot,
        rollback: _RollbackCandidate,
    ) -> None:
        dags = tuple(sorted(target.dag_allowlist))
        services = tuple(sorted(target.airflow_code_services))
        try:
            for dag_id in dags:
                self.airflow_mutation.pause_dag(target, dag_id)
            if not self._pause_state_matches(
                dags, {dag_id: True for dag_id in dags}
            ):
                raise RuntimeError
            self._drain(target)
            self.overlay_store.restore(
                rollback.artifact.content, rollback.artifact.sha256
            )
            self.overlay_store.verify_installed(rollback.artifact)
            self.compose.deploy_code_services(
                target, target.generated_overlay_file, services
            )
            if self.health.read_health(target, rollback.artifact) != "passed":
                raise RuntimeError
            self._restore_snapshot(target, snapshot)
            completed_at = self.clock.utc_now()
            self._require_timestamp(completed_at)
            self.ledger.complete(
                self._record(
                    identity=identity,
                    fingerprint=fingerprint,
                    deployment_id_value=current_id,
                    started_at=started_at,
                    completed_at=completed_at,
                    outcome=DeploymentOutcome.ROLLED_BACK,
                    health="failed",
                    artifact=rollback.artifact,
                )
            )
        except Exception:
            self._fail_closed(
                identity,
                target,
                fingerprint,
                current_id,
                started_at,
            )
        raise MainDeploymentError("main-deploy-rolled-back")

    def _fail_closed(
        self,
        identity: MainDeployIdentity,
        target: DeployTarget,
        fingerprint: str,
        current_id: str,
        started_at: str,
    ) -> None:
        dags = tuple(sorted(target.dag_allowlist))
        for dag_id in dags:
            try:
                self.airflow_mutation.pause_dag(target, dag_id)
            except Exception:
                pass
        pause_state_proven = self._pause_state_matches(
            dags, {dag_id: True for dag_id in dags}
        )
        try:
            completed_at = self.clock.utc_now()
            self._require_timestamp(completed_at)
            self.ledger.complete(
                self._record(
                    identity=identity,
                    fingerprint=fingerprint,
                    deployment_id_value=current_id,
                    started_at=started_at,
                    completed_at=completed_at,
                    outcome=DeploymentOutcome.ROLLBACK_FAILED,
                    health="failed",
                    artifact=None,
                    terminal_category=(
                        None
                        if pause_state_proven
                        else DeploymentTerminalCategory.PAUSE_STATE_UNVERIFIED
                    ),
                )
            )
        except Exception:
            pass
        category = (
            "main-deploy-rollback-failed"
            if pause_state_proven
            else "main-deploy-pause-state-unverified"
        )
        raise MainDeploymentError(category)

    @staticmethod
    def _record(
        *,
        identity: MainDeployIdentity,
        fingerprint: str,
        deployment_id_value: str,
        started_at: str,
        completed_at: str | None,
        outcome: DeploymentOutcome,
        health: str | None,
        artifact: OverlayArtifact | None,
        terminal_category: DeploymentTerminalCategory | None = None,
    ) -> DeploymentRecord:
        return DeploymentRecord(
            schema_version="weather-local-deployment-record/v2",
            deployment_id=deployment_id_value,
            repository=identity.repository,
            candidate_sha=identity.workflow_sha,
            target_fingerprint=fingerprint,
            started_at=started_at,
            completed_at=completed_at,
            outcome=outcome,
            health=health,
            overlay_content_b64=(
                base64.b64encode(artifact.content).decode("ascii")
                if artifact is not None
                else None
            ),
            overlay_sha256=artifact.sha256 if artifact is not None else None,
            terminal_category=terminal_category,
        )
