from __future__ import annotations

import base64
from dataclasses import replace
from pathlib import Path

import pytest

from deployment.fake_adapters import (
    FakeAdapterEvent,
    FakeAirflowAdapter,
    FakeClock,
    FakeComposeAdapter,
    FakeDeploymentLedger,
    FakeGitAdapter,
    FakeHealthAdapter,
    FakeOverlayStore,
)
from deployment.main_identity import MainDeployIdentity
from deployment.main_orchestrator import MainDeploymentError, MainDeploymentOrchestrator
from deployment.models import (
    DeploymentOutcome,
    DeploymentRecord,
    DeploymentResult,
    DeploymentTerminalCategory,
    WriterRunCounts,
    deployment_id,
)
from deployment.overlay import render_baseline_overlay, render_release_overlay
from deployment.target import DeployTarget, load_deploy_target, target_fingerprint


SHA = "a" * 40
PREVIOUS_SHA = "b" * 40
STARTED_AT = "2026-08-15T00:00:00Z"
COMPLETED_AT = "2026-08-15T00:01:00Z"


def _event(operation: str, **payload: object) -> FakeAdapterEvent:
    return FakeAdapterEvent(operation, tuple(sorted(payload.items())))


def _target() -> DeployTarget:
    repo_root = Path(__file__).resolve().parents[2]
    return load_deploy_target(repo_root / "runtime" / "deploy-target.example.json", repo_root)


def _identity() -> MainDeployIdentity:
    return MainDeployIdentity(
        repository="owner/seoul-weather-platform",
        workflow_ref=(
            "owner/seoul-weather-platform/.github/workflows/"
            "deploy-main.yml@refs/heads/main"
        ),
        workflow_sha=SHA,
        source_run_id=17,
        checks=(
            ("CI / required", "/checks/1", 1),
            ("Promotion Source / required", "/checks/2", 1),
        ),
    )


def _previous_success(target: DeployTarget) -> tuple[DeploymentRecord, object]:
    fingerprint = target_fingerprint(target)
    artifact = render_release_overlay(
        target,
        target.runtime_root / "releases" / PREVIOUS_SHA,
        PREVIOUS_SHA,
    )
    record = DeploymentRecord(
        schema_version="weather-local-deployment-record/v1",
        deployment_id=deployment_id(
            "owner/seoul-weather-platform", PREVIOUS_SHA, fingerprint
        ),
        repository="owner/seoul-weather-platform",
        candidate_sha=PREVIOUS_SHA,
        target_fingerprint=fingerprint,
        started_at="2026-08-14T00:00:00Z",
        completed_at="2026-08-14T00:01:00Z",
        outcome=DeploymentOutcome.SUCCESS,
        health="passed",
        overlay_content_b64=base64.b64encode(artifact.content).decode("ascii"),
        overlay_sha256=artifact.sha256,
    )
    return record, artifact


def _started_record(target: DeployTarget) -> DeploymentRecord:
    fingerprint = target_fingerprint(target)
    return DeploymentRecord(
        schema_version="weather-local-deployment-record/v2",
        deployment_id=deployment_id(
            "owner/seoul-weather-platform", SHA, fingerprint
        ),
        repository="owner/seoul-weather-platform",
        candidate_sha=SHA,
        target_fingerprint=fingerprint,
        started_at=STARTED_AT,
        completed_at=None,
        outcome=DeploymentOutcome.STARTED,
        health=None,
        overlay_content_b64=None,
        overlay_sha256=None,
    )


def _completed_record(
    target: DeployTarget,
    *,
    outcome: DeploymentOutcome,
    health: str | None,
    artifact=None,
    terminal_category: DeploymentTerminalCategory | None = None,
) -> DeploymentRecord:
    fingerprint = target_fingerprint(target)
    return DeploymentRecord(
        schema_version="weather-local-deployment-record/v2",
        deployment_id=deployment_id(
            "owner/seoul-weather-platform", SHA, fingerprint
        ),
        repository="owner/seoul-weather-platform",
        candidate_sha=SHA,
        target_fingerprint=fingerprint,
        started_at=STARTED_AT,
        completed_at=COMPLETED_AT,
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


def _build(
    *,
    target: DeployTarget,
    paused: dict[str, bool] | None = None,
    previous: DeploymentRecord | None = None,
    baseline=None,
    already_successful: bool = False,
    airflow_scripts: dict[str, list[object]] | None = None,
    airflow_failures: dict[str, BaseException] | None = None,
    compose_scripts: dict[str, list[object]] | None = None,
    compose_failures: dict[str, BaseException] | None = None,
    git_failures: dict[str, BaseException] | None = None,
    health_scripts: dict[str, list[object]] | None = None,
    health_result: str = "passed",
    overlay_failures: dict[str, BaseException] | None = None,
    overlay_scripts: dict[str, list[object]] | None = None,
    ledger_failures: dict[str, BaseException] | None = None,
    ledger_scripts: dict[str, list[object]] | None = None,
    utc_values: list[str] | None = None,
    monotonic_values: list[float] | None = None,
):
    events: list[FakeAdapterEvent] = []
    dags = sorted(target.dag_allowlist)
    initial = paused if paused is not None else {dag_id: False for dag_id in dags}
    airflow = FakeAirflowAdapter(
        events,
        paused=initial,
        run_counts=WriterRunCounts(running=0, queued=0),
        failures=airflow_failures,
        scripts=airflow_scripts,
    )
    compose = FakeComposeAdapter(
        events, failures=compose_failures, scripts=compose_scripts
    )
    checkout = target.runtime_root / "releases" / SHA
    git = FakeGitAdapter(events, checkout_root=checkout, failures=git_failures)
    health = FakeHealthAdapter(
        events,
        result=health_result,
        scripts=health_scripts,
    )
    clock = FakeClock(
        events,
        utc_values=list(utc_values or [STARTED_AT, COMPLETED_AT]),
        monotonic_values=list(monotonic_values or [0.0, 100.0]),
    )
    overlay = FakeOverlayStore(
        events,
        staged_path=Path("candidate-overlay.tmp"),
        failures=overlay_failures,
        scripts=overlay_scripts,
    )
    ledger = FakeDeploymentLedger(
        events,
        previous=previous,
        baseline_record=baseline,
        already_successful=already_successful,
        failures=ledger_failures,
        scripts=ledger_scripts,
    )
    orchestrator = MainDeploymentOrchestrator(
        airflow_read=airflow,
        airflow_mutation=airflow,
        compose=compose,
        git=git,
        health=health,
        clock=clock,
        overlay_store=overlay,
        ledger=ledger,
    )
    return orchestrator, events, airflow


def _lookup_events(target: DeployTarget, previous: DeploymentRecord | None):
    fingerprint = target_fingerprint(target)
    dep_id = deployment_id("owner/seoul-weather-platform", SHA, fingerprint)
    events = [
        _event("ledger.acquire_lock.enter", deployment_id=dep_id),
        _event("ledger.already_successful", deployment_id=dep_id),
        _event("ledger.previous_success", target_fingerprint=fingerprint),
    ]
    if previous is None:
        events.append(_event("ledger.baseline", target_fingerprint=fingerprint))
    return events


def _success_events(
    target: DeployTarget,
    previous: DeploymentRecord,
    paused: dict[str, bool],
) -> list[FakeAdapterEvent]:
    dags = tuple(sorted(target.dag_allowlist))
    writers = tuple(sorted(target.writer_dag_allowlist))
    services = tuple(sorted(target.airflow_code_services))
    stable = target.generated_overlay_file
    checkout = target.runtime_root / "releases" / SHA
    candidate = render_release_overlay(target, checkout, SHA)
    started = _started_record(target)
    success = _completed_record(
        target,
        outcome=DeploymentOutcome.SUCCESS,
        health="passed",
        artifact=candidate,
    )
    dep_id = started.deployment_id
    expected = _lookup_events(target, previous)
    expected.extend(
        [
            _event("airflow.capture_pause_state", dag_ids=dags),
            _event("clock.utc_now"),
            _event("ledger.begin", record=started),
        ]
    )
    # drain 이 pause 보다 먼저다 — pause 를 먼저 걸면 이미 도는 run 이 멈춰서
    # drain 이 영원히 0 을 못 본다.
    expected.extend(
        [
            _event("clock.monotonic"),
            _event("airflow.writer_run_counts", dag_ids=writers),
        ]
    )
    expected.extend(_event("airflow.pause_dag", dag_id=dag_id) for dag_id in dags)
    expected.append(_event("airflow.capture_pause_state", dag_ids=dags))
    expected.extend(
        [
            _event(
                "git.detached_checkout",
                repository="owner/seoul-weather-platform",
                candidate_sha=SHA,
                checkout_root=checkout,
            ),
            _event("overlay.stage", artifact=candidate),
            _event(
                "compose.validate_candidate",
                overlay_file=Path("candidate-overlay.tmp"),
            ),
            _event(
                "overlay.install",
                staged=Path("candidate-overlay.tmp"),
                artifact=candidate,
            ),
            _event("overlay.verify_installed", expected=candidate),
            _event(
                "compose.deploy_code_services",
                overlay_file=stable,
                services=services,
            ),
            _event("health.read_health", expected_overlay=candidate),
        ]
    )
    expected.extend(
        _event("airflow.unpause_dag", dag_id=dag_id)
        for dag_id in dags
        if paused[dag_id] is False
    )
    expected.append(_event("airflow.capture_pause_state", dag_ids=dags))
    expected.extend(
        [
            _event("clock.utc_now"),
            _event("ledger.complete", record=success),
            _event("ledger.acquire_lock.exit", deployment_id=dep_id),
        ]
    )
    return expected


def test_success_event_log_is_exact_and_deterministic():
    target = _target()
    previous, _ = _previous_success(target)
    dags = sorted(target.dag_allowlist)
    paused = {dag_id: index == 0 for index, dag_id in enumerate(dags)}
    orchestrator, events, airflow = _build(
        target=target,
        previous=previous,
        paused=paused,
    )

    result = orchestrator.deploy(_identity(), target)

    dep_id = deployment_id(
        "owner/seoul-weather-platform", SHA, target_fingerprint(target)
    )
    assert result == DeploymentResult(
        deployment_id=dep_id,
        outcome=DeploymentOutcome.SUCCESS,
        health="passed",
        idempotent=False,
    )
    assert events == _success_events(target, previous, paused)
    assert airflow.paused == paused
    deploy_events = [
        event for event in events if event.operation == "compose.deploy_code_services"
    ]
    assert all(
        dict(event.payload)["overlay_file"] == target.generated_overlay_file
        for event in deploy_events
    )


def test_same_successful_deployment_is_noop_inside_lock():
    target = _target()
    orchestrator, events, _ = _build(target=target, already_successful=True)
    dep_id = deployment_id(
        "owner/seoul-weather-platform", SHA, target_fingerprint(target)
    )

    result = orchestrator.deploy(_identity(), target)

    assert result == DeploymentResult(
        deployment_id=dep_id,
        outcome=DeploymentOutcome.SUCCESS,
        health="passed",
        idempotent=True,
    )
    assert events == [
        _event("ledger.acquire_lock.enter", deployment_id=dep_id),
        _event("ledger.already_successful", deployment_id=dep_id),
        _event("ledger.acquire_lock.exit", deployment_id=dep_id),
    ]


def test_live_lock_rejects_before_any_read_or_mutation():
    target = _target()
    orchestrator, events, _ = _build(
        target=target,
        ledger_failures={"ledger.acquire_lock.enter": FileExistsError("private")},
    )
    dep_id = deployment_id(
        "owner/seoul-weather-platform", SHA, target_fingerprint(target)
    )

    with pytest.raises(MainDeploymentError, match="^main-deploy-lock-failed$"):
        orchestrator.deploy(_identity(), target)

    assert events == [_event("ledger.acquire_lock.enter", deployment_id=dep_id)]


@pytest.mark.parametrize("invalid_input", ["identity", "target"])
def test_invalid_identity_or_target_uses_fixed_error_before_adapter_calls(invalid_input):
    target = _target()
    identity = _identity()
    if invalid_input == "identity":
        identity = replace(identity, repository=object())
    else:
        # target_fingerprint 는 이제 canonical_rollback_bytes 를 해시하므로,
        # 이 필드를 비-bytes 로 손상시켜 fingerprint 계산 실패를 유발한다.
        target = replace(target, canonical_rollback_bytes=object())
    orchestrator, events, _ = _build(target=_target())

    with pytest.raises(MainDeploymentError, match="^main-deploy-failed$"):
        orchestrator.deploy(identity, target)

    assert events == []


def test_first_cutover_without_rehearsed_baseline_fails_before_pause():
    target = _target()
    orchestrator, events, airflow = _build(target=target)
    dep_id = deployment_id(
        "owner/seoul-weather-platform", SHA, target_fingerprint(target)
    )

    with pytest.raises(
        MainDeploymentError, match="^main-deploy-rollback-unavailable$"
    ):
        orchestrator.deploy(_identity(), target)

    assert events == _lookup_events(target, None) + [
        _event("ledger.acquire_lock.exit", deployment_id=dep_id)
    ]
    assert not any(airflow.paused.values())


@pytest.mark.parametrize(
    "snapshot",
    [
        {},
        {"extra": False},
        {dag_id: (0 if index == 0 else False) for index, dag_id in enumerate(sorted(_target().dag_allowlist))},
    ],
    ids=["missing", "extra", "non-bool"],
)
def test_pause_snapshot_rejects_missing_extra_and_non_bool_before_mutation(snapshot):
    target = _target()
    previous, _ = _previous_success(target)
    orchestrator, events, _ = _build(
        target=target,
        previous=previous,
        airflow_scripts={"airflow.capture_pause_state": [snapshot]},
    )
    dep_id = deployment_id(
        "owner/seoul-weather-platform", SHA, target_fingerprint(target)
    )
    dags = tuple(sorted(target.dag_allowlist))

    with pytest.raises(MainDeploymentError, match="^main-deploy-snapshot-invalid$"):
        orchestrator.deploy(_identity(), target)

    assert events == _lookup_events(target, previous) + [
        _event("airflow.capture_pause_state", dag_ids=dags),
        _event("ledger.acquire_lock.exit", deployment_id=dep_id),
    ]


def test_pause_snapshot_is_exact_and_precedes_first_mutation():
    target = _target()
    previous, _ = _previous_success(target)
    dags = tuple(sorted(target.dag_allowlist))
    paused = {dag_id: index % 2 == 0 for index, dag_id in enumerate(dags)}
    orchestrator, events, airflow = _build(
        target=target,
        previous=previous,
        paused=paused,
        airflow_failures={"airflow.pause_dag": RuntimeError("private")},
    )
    started = _started_record(target)
    failed = _completed_record(
        target,
        outcome=DeploymentOutcome.FAILED,
        health=None,
    )
    dep_id = started.deployment_id

    with pytest.raises(MainDeploymentError, match="^main-deploy-failed$"):
        orchestrator.deploy(_identity(), target)

    expected = _lookup_events(target, previous) + [
        _event("airflow.capture_pause_state", dag_ids=dags),
        _event("clock.utc_now"),
        _event("ledger.begin", record=started),
        # drain 이 pause 보다 먼저 돈다.
        _event("clock.monotonic"),
        _event(
            "airflow.writer_run_counts",
            dag_ids=tuple(sorted(target.writer_dag_allowlist)),
        ),
        _event("airflow.pause_dag", dag_id=dags[0]),
    ]
    expected.extend(
        _event("airflow.unpause_dag", dag_id=dag_id)
        for dag_id in dags
        if paused[dag_id] is False
    )
    expected.append(_event("airflow.capture_pause_state", dag_ids=dags))
    expected.extend(
        [
            _event("clock.utc_now"),
            _event("ledger.complete", record=failed),
            _event("ledger.acquire_lock.exit", deployment_id=dep_id),
        ]
    )
    assert events == expected
    assert airflow.paused == paused


def test_pause_readback_mismatch_cannot_record_success_or_deploy_code():
    target = _target()
    previous, _ = _previous_success(target)
    dags = tuple(sorted(target.dag_allowlist))
    paused = {dag_id: False for dag_id in dags}
    orchestrator, events, airflow = _build(
        target=target,
        previous=previous,
        paused=paused,
        airflow_scripts={
            "airflow.capture_pause_state": [paused, paused]
        },
    )
    started = _started_record(target)
    failed = _completed_record(
        target,
        outcome=DeploymentOutcome.FAILED,
        health=None,
    )

    with pytest.raises(
        MainDeploymentError, match="^main-deploy-pause-verification-failed$"
    ):
        orchestrator.deploy(_identity(), target)

    expected = _lookup_events(target, previous) + [
        _event("airflow.capture_pause_state", dag_ids=dags),
        _event("clock.utc_now"),
        _event("ledger.begin", record=started),
        # drain 이 pause 보다 먼저 돈다.
        _event("clock.monotonic"),
        _event(
            "airflow.writer_run_counts",
            dag_ids=tuple(sorted(target.writer_dag_allowlist)),
        ),
    ]
    expected.extend(_event("airflow.pause_dag", dag_id=dag_id) for dag_id in dags)
    expected.append(_event("airflow.capture_pause_state", dag_ids=dags))
    expected.extend(_event("airflow.unpause_dag", dag_id=dag_id) for dag_id in dags)
    expected.extend(
        [
            _event("airflow.capture_pause_state", dag_ids=dags),
            _event("clock.utc_now"),
            _event("ledger.complete", record=failed),
            _event("ledger.acquire_lock.exit", deployment_id=started.deployment_id),
        ]
    )
    assert events == expected
    assert airflow.paused == paused
    assert not any(
        event.operation == "compose.deploy_code_services" for event in events
    )


def test_drain_polls_only_writer_allowlist_with_bounded_sleep():
    target = _target()
    previous, _ = _previous_success(target)
    dags = {dag_id: True for dag_id in target.dag_allowlist}
    orchestrator, events, _ = _build(
        target=target,
        previous=previous,
        paused=dags,
        airflow_scripts={
            "airflow.writer_run_counts": [
                WriterRunCounts(running=1, queued=2),
                WriterRunCounts(running=0, queued=0),
            ]
        },
        monotonic_values=[0.0, 1.0, 2.0],
    )

    result = orchestrator.deploy(_identity(), target)

    expected = _success_events(target, previous, dags)
    first_counts = expected.index(
        _event(
            "airflow.writer_run_counts",
            dag_ids=tuple(sorted(target.writer_dag_allowlist)),
        )
    )
    expected[first_counts + 1:first_counts + 1] = [
        _event("clock.monotonic"),
        _event("clock.sleep", seconds=target.poll_interval_seconds),
        _event("clock.monotonic"),
        _event(
            "airflow.writer_run_counts",
            dag_ids=tuple(sorted(target.writer_dag_allowlist)),
        ),
    ]
    assert result.outcome is DeploymentOutcome.SUCCESS
    assert events == expected


def test_drain_deadline_after_bounded_sleep_does_not_poll_again():
    target = _target()
    previous, _ = _previous_success(target)
    dags = tuple(sorted(target.dag_allowlist))
    paused = {dag_id: False for dag_id in dags}
    orchestrator, events, airflow = _build(
        target=target,
        previous=previous,
        paused=paused,
        airflow_scripts={
            "airflow.writer_run_counts": [WriterRunCounts(running=1, queued=0)]
        },
        monotonic_values=[
            0.0,
            float(target.drain_timeout_seconds - 10),
            float(target.drain_timeout_seconds),
        ],
    )
    started = _started_record(target)
    failed = _completed_record(
        target,
        outcome=DeploymentOutcome.FAILED,
        health=None,
    )

    with pytest.raises(MainDeploymentError, match="^main-deploy-drain-timeout$"):
        orchestrator.deploy(_identity(), target)

    expected = _lookup_events(target, previous) + [
        _event("airflow.capture_pause_state", dag_ids=dags),
        _event("clock.utc_now"),
        _event("ledger.begin", record=started),
    ]
    expected.extend(
        [
            _event("clock.monotonic"),
            _event(
                "airflow.writer_run_counts",
                dag_ids=tuple(sorted(target.writer_dag_allowlist)),
            ),
        ]
    )
    # drain 이 먼저 돌다 timeout 났으므로 pause 는 아예 일어나지 않는다.
    expected.extend(
        [
            _event("clock.monotonic"),
            _event("clock.sleep", seconds=10.0),
            _event("clock.monotonic"),
        ]
    )
    expected.extend(_event("airflow.unpause_dag", dag_id=dag_id) for dag_id in dags)
    expected.append(_event("airflow.capture_pause_state", dag_ids=dags))
    expected.extend(
        [
            _event("clock.utc_now"),
            _event("ledger.complete", record=failed),
            _event("ledger.acquire_lock.exit", deployment_id=started.deployment_id),
        ]
    )
    assert events == expected
    assert airflow.paused == paused
    assert sum(
        event.operation == "airflow.writer_run_counts" for event in events
    ) == 1


def test_drain_timeout_restores_snapshot_without_overlay_mutation():
    target = _target()
    previous, _ = _previous_success(target)
    dags = tuple(sorted(target.dag_allowlist))
    paused = {dag_id: index == 0 for index, dag_id in enumerate(dags)}
    orchestrator, events, airflow = _build(
        target=target,
        previous=previous,
        paused=paused,
        airflow_scripts={
            "airflow.writer_run_counts": [WriterRunCounts(running=1, queued=0)]
        },
        monotonic_values=[0.0, float(target.drain_timeout_seconds)],
    )
    started = _started_record(target)
    failed = _completed_record(
        target,
        outcome=DeploymentOutcome.FAILED,
        health=None,
    )

    with pytest.raises(MainDeploymentError, match="^main-deploy-drain-timeout$"):
        orchestrator.deploy(_identity(), target)

    expected = _lookup_events(target, previous) + [
        _event("airflow.capture_pause_state", dag_ids=dags),
        _event("clock.utc_now"),
        _event("ledger.begin", record=started),
    ]
    expected.extend(
        [
            _event("clock.monotonic"),
            _event(
                "airflow.writer_run_counts",
                dag_ids=tuple(sorted(target.writer_dag_allowlist)),
            ),
        ]
    )
    # drain 이 먼저 돌다 timeout 났으므로 pause 는 아예 일어나지 않는다.
    expected.extend(
        [
            _event("clock.monotonic"),
        ]
    )
    expected.extend(
        _event("airflow.unpause_dag", dag_id=dag_id)
        for dag_id in dags
        if paused[dag_id] is False
    )
    expected.append(_event("airflow.capture_pause_state", dag_ids=dags))
    expected.extend(
        [
            _event("clock.utc_now"),
            _event("ledger.complete", record=failed),
            _event("ledger.acquire_lock.exit", deployment_id=started.deployment_id),
        ]
    )
    assert events == expected
    assert airflow.paused == paused
    assert not any(event.operation.startswith("overlay.") for event in events)
    assert not any(
        event.operation == "compose.deploy_code_services" for event in events
    )


def test_malformed_writer_counts_restore_snapshot_and_fail_closed():
    target = _target()
    previous, _ = _previous_success(target)
    dags = tuple(sorted(target.dag_allowlist))
    paused = {dag_id: False for dag_id in dags}
    orchestrator, events, airflow = _build(
        target=target,
        previous=previous,
        paused=paused,
        airflow_scripts={"airflow.writer_run_counts": [{"running": 0, "queued": 0}]},
    )
    started = _started_record(target)
    failed = _completed_record(
        target,
        outcome=DeploymentOutcome.FAILED,
        health=None,
    )

    with pytest.raises(MainDeploymentError, match="^main-deploy-writer-counts-invalid$"):
        orchestrator.deploy(_identity(), target)

    expected = _lookup_events(target, previous) + [
        _event("airflow.capture_pause_state", dag_ids=dags),
        _event("clock.utc_now"),
        _event("ledger.begin", record=started),
    ]
    expected.extend(
        [
            _event("clock.monotonic"),
            _event(
                "airflow.writer_run_counts",
                dag_ids=tuple(sorted(target.writer_dag_allowlist)),
            ),
        ]
    )
    # drain 이 먼저 돌다 실패했으므로 pause 는 아예 일어나지 않는다.
    expected.extend(_event("airflow.unpause_dag", dag_id=dag_id) for dag_id in dags)
    expected.append(_event("airflow.capture_pause_state", dag_ids=dags))
    expected.extend(
        [
            _event("clock.utc_now"),
            _event("ledger.complete", record=failed),
            _event("ledger.acquire_lock.exit", deployment_id=started.deployment_id),
        ]
    )
    assert events == expected
    assert airflow.paused == paused


def test_checkout_failure_restores_snapshot_without_install_or_deploy():
    target = _target()
    previous, _ = _previous_success(target)
    dags = tuple(sorted(target.dag_allowlist))
    paused = {dag_id: index == 0 for index, dag_id in enumerate(dags)}
    orchestrator, events, airflow = _build(
        target=target,
        previous=previous,
        paused=paused,
        git_failures={"git.detached_checkout": RuntimeError("private path")},
    )
    started = _started_record(target)
    failed = _completed_record(
        target,
        outcome=DeploymentOutcome.FAILED,
        health=None,
    )
    checkout = target.runtime_root / "releases" / SHA

    with pytest.raises(MainDeploymentError, match="^main-deploy-checkout-failed$"):
        orchestrator.deploy(_identity(), target)

    expected = _lookup_events(target, previous) + [
        _event("airflow.capture_pause_state", dag_ids=dags),
        _event("clock.utc_now"),
        _event("ledger.begin", record=started),
    ]
    expected.extend(
        [
            _event("clock.monotonic"),
            _event(
                "airflow.writer_run_counts",
                dag_ids=tuple(sorted(target.writer_dag_allowlist)),
            ),
        ]
    )
    expected.extend(_event("airflow.pause_dag", dag_id=dag_id) for dag_id in dags)
    expected.append(_event("airflow.capture_pause_state", dag_ids=dags))
    expected.extend(
        [
            _event(
                "git.detached_checkout",
                repository="owner/seoul-weather-platform",
                candidate_sha=SHA,
                checkout_root=checkout,
            ),
        ]
    )
    expected.extend(
        _event("airflow.unpause_dag", dag_id=dag_id)
        for dag_id in dags
        if paused[dag_id] is False
    )
    expected.append(_event("airflow.capture_pause_state", dag_ids=dags))
    expected.extend(
        [
            _event("clock.utc_now"),
            _event("ledger.complete", record=failed),
            _event("ledger.acquire_lock.exit", deployment_id=started.deployment_id),
        ]
    )
    assert events == expected
    assert airflow.paused == paused
    assert not any(event.operation == "overlay.install" for event in events)
    assert not any(
        event.operation == "compose.deploy_code_services" for event in events
    )


def test_candidate_validation_failure_discards_stage_and_preserves_stable_overlay():
    target = _target()
    previous, _ = _previous_success(target)
    dags = tuple(sorted(target.dag_allowlist))
    paused = {dag_id: True for dag_id in dags}
    orchestrator, events, _ = _build(
        target=target,
        previous=previous,
        paused=paused,
        compose_failures={"compose.validate_candidate": RuntimeError("private")},
    )
    started = _started_record(target)
    failed = _completed_record(
        target,
        outcome=DeploymentOutcome.FAILED,
        health=None,
    )
    checkout = target.runtime_root / "releases" / SHA
    candidate = render_release_overlay(target, checkout, SHA)

    with pytest.raises(
        MainDeploymentError, match="^main-deploy-candidate-validation-failed$"
    ):
        orchestrator.deploy(_identity(), target)

    expected = _lookup_events(target, previous) + [
        _event("airflow.capture_pause_state", dag_ids=dags),
        _event("clock.utc_now"),
        _event("ledger.begin", record=started),
    ]
    expected.extend(
        [
            _event("clock.monotonic"),
            _event(
                "airflow.writer_run_counts",
                dag_ids=tuple(sorted(target.writer_dag_allowlist)),
            ),
        ]
    )
    expected.extend(_event("airflow.pause_dag", dag_id=dag_id) for dag_id in dags)
    expected.append(_event("airflow.capture_pause_state", dag_ids=dags))
    expected.extend(
        [
            _event(
                "git.detached_checkout",
                repository="owner/seoul-weather-platform",
                candidate_sha=SHA,
                checkout_root=checkout,
            ),
            _event("overlay.stage", artifact=candidate),
            _event(
                "compose.validate_candidate",
                overlay_file=Path("candidate-overlay.tmp"),
            ),
            _event("overlay.discard", staged=Path("candidate-overlay.tmp")),
            _event("airflow.capture_pause_state", dag_ids=dags),
            _event("clock.utc_now"),
            _event("ledger.complete", record=failed),
            _event("ledger.acquire_lock.exit", deployment_id=started.deployment_id),
        ]
    )
    assert events == expected
    assert not any(event.operation == "overlay.install" for event in events)
    assert not any(
        event.operation == "compose.deploy_code_services" for event in events
    )


def test_previous_success_prevents_baseline_lookup():
    target = _target()
    previous, _ = _previous_success(target)
    paused = {dag_id: True for dag_id in target.dag_allowlist}
    orchestrator, events, _ = _build(
        target=target,
        previous=previous,
        paused=paused,
    )

    orchestrator.deploy(_identity(), target)

    assert events == _success_events(target, previous, paused)
    assert not any(event.operation == "ledger.baseline" for event in events)


@pytest.mark.parametrize("candidate_kind", ["previous", "baseline"])
def test_release_and_baseline_semantic_mismatch_rejected_before_pause(candidate_kind):
    target = _target()
    previous, _ = _previous_success(target)
    baseline_artifact = render_baseline_overlay(target)
    fingerprint = target_fingerprint(target)
    if candidate_kind == "previous":
        previous = DeploymentRecord(
            **{
                **previous.__dict__,
                "overlay_content_b64": base64.b64encode(
                    baseline_artifact.content
                ).decode("ascii"),
                "overlay_sha256": baseline_artifact.sha256,
            }
        )
        baseline = None
    else:
        previous = None
        release_artifact = render_release_overlay(
            target,
            target.runtime_root / "releases" / PREVIOUS_SHA,
            PREVIOUS_SHA,
        )
        from deployment.models import BaselineRecord

        baseline = BaselineRecord(
            schema_version="weather-local-baseline-record/v1",
            baseline_id="baseline://existing-local",
            target_fingerprint=fingerprint,
            captured_at="2026-08-14T00:00:00Z",
            rehearsal="passed",
            overlay_content_b64=base64.b64encode(release_artifact.content).decode(
                "ascii"
            ),
            overlay_sha256=release_artifact.sha256,
        )
    orchestrator, events, _ = _build(
        target=target,
        previous=previous,
        baseline=baseline,
    )
    dep_id = deployment_id("owner/seoul-weather-platform", SHA, fingerprint)

    with pytest.raises(
        MainDeploymentError, match="^main-deploy-rollback-candidate-invalid$"
    ):
        orchestrator.deploy(_identity(), target)

    assert events == _lookup_events(target, previous) + [
        _event("ledger.acquire_lock.exit", deployment_id=dep_id)
    ]


def test_drain_completes_before_pausing_so_in_flight_runs_can_finish():
    """Pausing first deadlocks: Airflow stops scheduling the remaining tasks of a
    run that is already going, so it can never reach zero and drain always times
    out. Observed 2026-08-16: a deploy paused mid-run, the bronze run stalled for
    23 minutes between land and load, and the deploy died on drain-timeout.

    Draining first lets the in-flight run finish; the pause that follows is what
    keeps new runs from starting.
    """
    target = _target()
    previous, _ = _previous_success(target)
    dags = {dag_id: True for dag_id in target.dag_allowlist}
    orchestrator, events, _ = _build(
        target=target, previous=previous, paused=dags
    )

    orchestrator.deploy(_identity(), target)

    names = [event.operation for event in events]
    first_drain = names.index("airflow.writer_run_counts")
    first_pause = names.index("airflow.pause_dag")
    assert first_drain < first_pause, (
        "drain must observe an unpaused pipeline; pausing first freezes the "
        "in-flight run and drain can never reach zero"
    )
