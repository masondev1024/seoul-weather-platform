from __future__ import annotations

import base64
from pathlib import Path

import pytest

from deployment.fake_adapters import FakeAdapterEvent
from deployment.main_orchestrator import MainDeploymentError
from deployment.models import (
    BaselineRecord,
    DeploymentOutcome,
    DeploymentRecord,
    DeploymentTerminalCategory,
    WriterRunCounts,
    deployment_id,
)
from deployment.overlay import render_baseline_overlay, render_release_overlay
from deployment.target import target_fingerprint
from tests.deploy.test_main_orchestrator import (
    COMPLETED_AT,
    SHA,
    STARTED_AT,
    _build,
    _completed_record,
    _event,
    _identity,
    _lookup_events,
    _previous_success,
    _started_record,
    _target,
)


def _candidate_artifact(target):
    checkout = target.runtime_root / "releases" / SHA
    return render_release_overlay(target, checkout, SHA)


def _pre_install_events(
    target,
    previous: DeploymentRecord | None,
    paused: dict[str, bool],
    *,
    verify_candidate: bool = True,
) -> list[FakeAdapterEvent]:
    dags = tuple(sorted(target.dag_allowlist))
    writers = tuple(sorted(target.writer_dag_allowlist))
    checkout = target.runtime_root / "releases" / SHA
    candidate = _candidate_artifact(target)
    expected = _lookup_events(target, previous)
    expected.extend(
        [
            _event("airflow.capture_pause_state", dag_ids=dags),
            _event("clock.utc_now"),
            _event("ledger.begin", record=_started_record(target)),
        ]
    )
    # drain 이 pause 보다 먼저다 — pause 를 먼저 걸면 이미 도는 run 의 남은 태스크가
    # 스케줄되지 않아 drain 이 영원히 0 을 못 본다(main_orchestrator._deploy_locked 주석).
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
        ]
    )
    if verify_candidate:
        expected.append(_event("overlay.verify_installed", expected=candidate))
    return expected


def _rollback_events(
    target,
    paused: dict[str, bool],
    rollback_artifact,
    *,
    terminal_outcome: DeploymentOutcome = DeploymentOutcome.ROLLED_BACK,
) -> list[FakeAdapterEvent]:
    dags = tuple(sorted(target.dag_allowlist))
    services = tuple(sorted(target.airflow_code_services))
    expected = [_event("airflow.pause_dag", dag_id=dag_id) for dag_id in dags]
    expected.append(_event("airflow.capture_pause_state", dag_ids=dags))
    expected.extend(
        [
            _event("clock.monotonic"),
            _event(
                "airflow.writer_run_counts",
                dag_ids=tuple(sorted(target.writer_dag_allowlist)),
            ),
            _event(
                "overlay.restore",
                content=rollback_artifact.content,
                sha256=rollback_artifact.sha256,
            ),
            _event("overlay.verify_installed", expected=rollback_artifact),
            _event(
                "compose.deploy_code_services",
                overlay_file=target.generated_overlay_file,
                services=services,
            ),
            _event("health.read_health", expected_overlay=rollback_artifact),
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
            _event(
                "ledger.complete",
                record=_completed_record(
                    target,
                    outcome=terminal_outcome,
                    health="failed",
                    artifact=(
                        rollback_artifact
                        if terminal_outcome is DeploymentOutcome.ROLLED_BACK
                        else None
                    ),
                ),
            ),
        ]
    )
    return expected


def _fail_closed_events(
    target,
    *,
    terminal_category: DeploymentTerminalCategory | None = None,
) -> list[FakeAdapterEvent]:
    dags = tuple(sorted(target.dag_allowlist))
    expected = [_event("airflow.pause_dag", dag_id=dag_id) for dag_id in dags]
    expected.extend(
        [
            _event("airflow.capture_pause_state", dag_ids=dags),
            _event("clock.utc_now"),
            _event(
                "ledger.complete",
                record=_completed_record(
                    target,
                    outcome=DeploymentOutcome.ROLLBACK_FAILED,
                    health="failed",
                    terminal_category=terminal_category,
                ),
            ),
        ]
    )
    return expected


def _assert_all_paused_and_stable_only(events, airflow, target):
    assert airflow.paused == {dag_id: True for dag_id in target.dag_allowlist}
    deploy_events = [
        event for event in events if event.operation == "compose.deploy_code_services"
    ]
    assert all(
        dict(event.payload)["overlay_file"] == target.generated_overlay_file
        for event in deploy_events
    )


def test_install_exception_is_treated_as_post_install_unknown():
    target = _target()
    previous, rollback_artifact = _previous_success(target)
    paused = {dag_id: index == 0 for index, dag_id in enumerate(sorted(target.dag_allowlist))}
    orchestrator, events, airflow = _build(
        target=target,
        previous=previous,
        paused=paused,
        overlay_failures={"overlay.install": RuntimeError("private path")},
    )
    dep_id = deployment_id(
        "owner/seoul-weather-platform", SHA, target_fingerprint(target)
    )

    with pytest.raises(MainDeploymentError, match="^main-deploy-rolled-back$"):
        orchestrator.deploy(_identity(), target)

    expected = _pre_install_events(
        target, previous, paused, verify_candidate=False
    )
    expected.extend(_rollback_events(target, paused, rollback_artifact))
    expected.append(_event("ledger.acquire_lock.exit", deployment_id=dep_id))
    assert events == expected
    assert airflow.paused == paused
    assert not any(event.operation == "overlay.discard" for event in events)


@pytest.mark.parametrize("failure_point", ["deploy", "health"])
def test_candidate_deploy_and_health_failure_restore_previous_overlay(failure_point):
    target = _target()
    previous, rollback_artifact = _previous_success(target)
    paused = {dag_id: index % 2 == 0 for index, dag_id in enumerate(sorted(target.dag_allowlist))}
    compose_scripts = None
    health_scripts = None
    if failure_point == "deploy":
        compose_scripts = {
            "compose.deploy_code_services": [RuntimeError("private"), None]
        }
    else:
        health_scripts = {"health.read_health": ["failed", "passed"]}
    orchestrator, events, airflow = _build(
        target=target,
        previous=previous,
        paused=paused,
        compose_scripts=compose_scripts,
        health_scripts=health_scripts,
    )
    candidate = _candidate_artifact(target)
    services = tuple(sorted(target.airflow_code_services))
    dep_id = deployment_id(
        "owner/seoul-weather-platform", SHA, target_fingerprint(target)
    )

    with pytest.raises(MainDeploymentError, match="^main-deploy-rolled-back$"):
        orchestrator.deploy(_identity(), target)

    expected = _pre_install_events(target, previous, paused)
    expected.append(
        _event(
            "compose.deploy_code_services",
            overlay_file=target.generated_overlay_file,
            services=services,
        )
    )
    if failure_point == "health":
        expected.append(_event("health.read_health", expected_overlay=candidate))
    expected.extend(_rollback_events(target, paused, rollback_artifact))
    expected.append(_event("ledger.acquire_lock.exit", deployment_id=dep_id))
    assert events == expected
    assert airflow.paused == paused


def test_candidate_health_failure_uses_rehearsed_baseline_when_no_previous_success():
    target = _target()
    rollback_artifact = render_baseline_overlay(target)
    fingerprint = target_fingerprint(target)
    baseline = BaselineRecord(
        schema_version="weather-local-baseline-record/v1",
        baseline_id="baseline://existing-local",
        target_fingerprint=fingerprint,
        captured_at="2026-08-14T00:00:00Z",
        rehearsal="passed",
        overlay_content_b64=base64.b64encode(rollback_artifact.content).decode("ascii"),
        overlay_sha256=rollback_artifact.sha256,
    )
    paused = {dag_id: False for dag_id in target.dag_allowlist}
    orchestrator, events, airflow = _build(
        target=target,
        baseline=baseline,
        paused=paused,
        health_scripts={"health.read_health": ["failed", "passed"]},
    )
    candidate = _candidate_artifact(target)
    services = tuple(sorted(target.airflow_code_services))
    dep_id = deployment_id("owner/seoul-weather-platform", SHA, fingerprint)

    with pytest.raises(MainDeploymentError, match="^main-deploy-rolled-back$"):
        orchestrator.deploy(_identity(), target)

    expected = _pre_install_events(target, None, paused)
    expected.extend(
        [
            _event(
                "compose.deploy_code_services",
                overlay_file=target.generated_overlay_file,
                services=services,
            ),
            _event("health.read_health", expected_overlay=candidate),
        ]
    )
    expected.extend(_rollback_events(target, paused, rollback_artifact))
    expected.append(_event("ledger.acquire_lock.exit", deployment_id=dep_id))
    assert events == expected
    assert airflow.paused == paused


@pytest.mark.parametrize(
    "failure_point", ["restore", "verify", "deploy", "health"]
)
def test_rollback_restore_deploy_and_health_fail_closed_without_retry(failure_point):
    target = _target()
    previous, rollback_artifact = _previous_success(target)
    paused = {dag_id: False for dag_id in target.dag_allowlist}
    overlay_failures = None
    overlay_scripts = None
    compose_scripts = None
    health_scripts = {"health.read_health": ["failed", "passed"]}
    if failure_point == "restore":
        overlay_failures = {"overlay.restore": RuntimeError("private")}
    elif failure_point == "verify":
        overlay_scripts = {
            "overlay.verify_installed": [None, RuntimeError("private")]
        }
    elif failure_point == "deploy":
        compose_scripts = {
            "compose.deploy_code_services": [None, RuntimeError("private")]
        }
    else:
        health_scripts = {"health.read_health": ["failed", "failed"]}
    orchestrator, events, airflow = _build(
        target=target,
        previous=previous,
        paused=paused,
        overlay_failures=overlay_failures,
        overlay_scripts=overlay_scripts,
        compose_scripts=compose_scripts,
        health_scripts=health_scripts,
    )
    candidate = _candidate_artifact(target)
    services = tuple(sorted(target.airflow_code_services))
    dep_id = deployment_id(
        "owner/seoul-weather-platform", SHA, target_fingerprint(target)
    )

    with pytest.raises(MainDeploymentError, match="^main-deploy-rollback-failed$"):
        orchestrator.deploy(_identity(), target)

    expected = _pre_install_events(target, previous, paused)
    expected.extend(
        [
            _event(
                "compose.deploy_code_services",
                overlay_file=target.generated_overlay_file,
                services=services,
            ),
            _event("health.read_health", expected_overlay=candidate),
        ]
    )
    dags = tuple(sorted(target.dag_allowlist))
    expected.extend(_event("airflow.pause_dag", dag_id=dag_id) for dag_id in dags)
    expected.append(_event("airflow.capture_pause_state", dag_ids=dags))
    expected.extend(
        [
            _event("clock.monotonic"),
            _event(
                "airflow.writer_run_counts",
                dag_ids=tuple(sorted(target.writer_dag_allowlist)),
            ),
        ]
    )
    expected.append(
        _event(
            "overlay.restore",
            content=rollback_artifact.content,
            sha256=rollback_artifact.sha256,
        )
    )
    if failure_point != "restore":
        expected.append(_event("overlay.verify_installed", expected=rollback_artifact))
    if failure_point not in {"restore", "verify"}:
        expected.append(
            _event(
                "compose.deploy_code_services",
                overlay_file=target.generated_overlay_file,
                services=services,
            )
        )
    if failure_point == "health":
        expected.append(
            _event("health.read_health", expected_overlay=rollback_artifact)
        )
    expected.extend(_fail_closed_events(target))
    expected.append(_event("ledger.acquire_lock.exit", deployment_id=dep_id))
    assert events == expected
    _assert_all_paused_and_stable_only(events, airflow, target)
    assert sum(event.operation == "overlay.restore" for event in events) == 1
    expected_deploys = 1 if failure_point in {"restore", "verify"} else 2
    assert (
        sum(event.operation == "compose.deploy_code_services" for event in events)
        == expected_deploys
    )


def test_success_record_write_failure_rolls_back_previous_overlay():
    target = _target()
    previous, rollback_artifact = _previous_success(target)
    paused = {dag_id: index == 0 for index, dag_id in enumerate(sorted(target.dag_allowlist))}
    orchestrator, events, airflow = _build(
        target=target,
        previous=previous,
        paused=paused,
        airflow_scripts={
            "airflow.writer_run_counts": [
                WriterRunCounts(0, 0),
                WriterRunCounts(1, 1),
                WriterRunCounts(0, 0),
            ]
        },
        ledger_scripts={"ledger.complete": [RuntimeError("private"), None]},
        utc_values=[STARTED_AT, COMPLETED_AT, COMPLETED_AT],
        monotonic_values=[0.0, 100.0, 101.0, 102.0],
    )
    candidate = _candidate_artifact(target)
    services = tuple(sorted(target.airflow_code_services))
    dags = tuple(sorted(target.dag_allowlist))
    dep_id = deployment_id(
        "owner/seoul-weather-platform", SHA, target_fingerprint(target)
    )

    with pytest.raises(MainDeploymentError, match="^main-deploy-rolled-back$"):
        orchestrator.deploy(_identity(), target)

    expected = _pre_install_events(target, previous, paused)
    expected.extend(
        [
            _event(
                "compose.deploy_code_services",
                overlay_file=target.generated_overlay_file,
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
            _event(
                "ledger.complete",
                record=_completed_record(
                    target,
                    outcome=DeploymentOutcome.SUCCESS,
                    health="passed",
                    artifact=candidate,
                ),
            ),
        ]
    )
    rollback_events = _rollback_events(target, paused, rollback_artifact)
    count_event = _event(
        "airflow.writer_run_counts",
        dag_ids=tuple(sorted(target.writer_dag_allowlist)),
    )
    count_index = rollback_events.index(count_event)
    rollback_events[count_index + 1:count_index + 1] = [
        _event("clock.monotonic"),
        _event("clock.sleep", seconds=target.poll_interval_seconds),
        _event("clock.monotonic"),
        count_event,
    ]
    expected.extend(rollback_events)
    expected.append(_event("ledger.acquire_lock.exit", deployment_id=dep_id))
    assert events == expected
    assert airflow.paused == paused


def test_rollback_redrain_timeout_never_restores_previous_overlay():
    target = _target()
    previous, _ = _previous_success(target)
    paused = {dag_id: False for dag_id in target.dag_allowlist}
    orchestrator, events, airflow = _build(
        target=target,
        previous=previous,
        paused=paused,
        airflow_scripts={
            "airflow.writer_run_counts": [
                WriterRunCounts(0, 0),
                WriterRunCounts(1, 0),
            ]
        },
        ledger_scripts={"ledger.complete": [RuntimeError("private"), None]},
        utc_values=[STARTED_AT, COMPLETED_AT, COMPLETED_AT],
        monotonic_values=[
            0.0,
            100.0,
            100.0 + target.drain_timeout_seconds,
        ],
    )
    candidate = _candidate_artifact(target)
    services = tuple(sorted(target.airflow_code_services))
    dags = tuple(sorted(target.dag_allowlist))
    writers = tuple(sorted(target.writer_dag_allowlist))
    dep_id = deployment_id(
        "owner/seoul-weather-platform", SHA, target_fingerprint(target)
    )

    with pytest.raises(MainDeploymentError, match="^main-deploy-rollback-failed$"):
        orchestrator.deploy(_identity(), target)

    expected = _pre_install_events(target, previous, paused)
    expected.extend(
        [
            _event(
                "compose.deploy_code_services",
                overlay_file=target.generated_overlay_file,
                services=services,
            ),
            _event("health.read_health", expected_overlay=candidate),
        ]
    )
    expected.extend(_event("airflow.unpause_dag", dag_id=dag_id) for dag_id in dags)
    expected.extend(
        [
            _event("airflow.capture_pause_state", dag_ids=dags),
            _event("clock.utc_now"),
            _event(
                "ledger.complete",
                record=_completed_record(
                    target,
                    outcome=DeploymentOutcome.SUCCESS,
                    health="passed",
                    artifact=candidate,
                ),
            ),
        ]
    )
    expected.extend(_event("airflow.pause_dag", dag_id=dag_id) for dag_id in dags)
    expected.extend(
        [
            _event("airflow.capture_pause_state", dag_ids=dags),
            _event("clock.monotonic"),
            _event("airflow.writer_run_counts", dag_ids=writers),
            _event("clock.monotonic"),
        ]
    )
    expected.extend(_fail_closed_events(target))
    expected.append(_event("ledger.acquire_lock.exit", deployment_id=dep_id))
    assert events == expected
    assert not any(event.operation == "overlay.restore" for event in events)
    _assert_all_paused_and_stable_only(events, airflow, target)


def test_candidate_snapshot_restore_failure_enters_rollback_and_restores_again():
    target = _target()
    previous, rollback_artifact = _previous_success(target)
    paused = {dag_id: False for dag_id in target.dag_allowlist}
    orchestrator, events, airflow = _build(
        target=target,
        previous=previous,
        paused=paused,
        airflow_scripts={"airflow.unpause_dag": [RuntimeError("private")]},
    )
    candidate = _candidate_artifact(target)
    services = tuple(sorted(target.airflow_code_services))
    dags = tuple(sorted(target.dag_allowlist))
    dep_id = deployment_id(
        "owner/seoul-weather-platform", SHA, target_fingerprint(target)
    )

    with pytest.raises(MainDeploymentError, match="^main-deploy-rolled-back$"):
        orchestrator.deploy(_identity(), target)

    expected = _pre_install_events(target, previous, paused)
    expected.extend(
        [
            _event(
                "compose.deploy_code_services",
                overlay_file=target.generated_overlay_file,
                services=services,
            ),
            _event("health.read_health", expected_overlay=candidate),
            _event("airflow.unpause_dag", dag_id=dags[0]),
        ]
    )
    expected.extend(_rollback_events(target, paused, rollback_artifact))
    expected.append(_event("ledger.acquire_lock.exit", deployment_id=dep_id))
    assert events == expected
    assert airflow.paused == paused


@pytest.mark.parametrize("failure_point", ["snapshot-restore", "record"])
def test_rollback_snapshot_restore_and_record_failure_leave_all_dags_paused(
    failure_point,
):
    target = _target()
    previous, rollback_artifact = _previous_success(target)
    paused = {dag_id: False for dag_id in target.dag_allowlist}
    airflow_scripts = None
    ledger_scripts = None
    if failure_point == "snapshot-restore":
        airflow_scripts = {
            "airflow.unpause_dag": [RuntimeError("private")]
        }
    else:
        ledger_scripts = {"ledger.complete": [RuntimeError("private"), None]}
    orchestrator, events, airflow = _build(
        target=target,
        previous=previous,
        paused=paused,
        airflow_scripts=airflow_scripts,
        health_scripts={"health.read_health": ["failed", "passed"]},
        ledger_scripts=ledger_scripts,
        utc_values=[STARTED_AT, COMPLETED_AT, COMPLETED_AT],
    )
    candidate = _candidate_artifact(target)
    services = tuple(sorted(target.airflow_code_services))
    dags = tuple(sorted(target.dag_allowlist))
    dep_id = deployment_id(
        "owner/seoul-weather-platform", SHA, target_fingerprint(target)
    )

    with pytest.raises(MainDeploymentError, match="^main-deploy-rollback-failed$"):
        orchestrator.deploy(_identity(), target)

    expected = _pre_install_events(target, previous, paused)
    expected.extend(
        [
            _event(
                "compose.deploy_code_services",
                overlay_file=target.generated_overlay_file,
                services=services,
            ),
            _event("health.read_health", expected_overlay=candidate),
        ]
    )
    expected.extend(_event("airflow.pause_dag", dag_id=dag_id) for dag_id in dags)
    expected.append(_event("airflow.capture_pause_state", dag_ids=dags))
    expected.extend(
        [
            _event("clock.monotonic"),
            _event(
                "airflow.writer_run_counts",
                dag_ids=tuple(sorted(target.writer_dag_allowlist)),
            ),
            _event(
                "overlay.restore",
                content=rollback_artifact.content,
                sha256=rollback_artifact.sha256,
            ),
            _event("overlay.verify_installed", expected=rollback_artifact),
            _event(
                "compose.deploy_code_services",
                overlay_file=target.generated_overlay_file,
                services=services,
            ),
            _event("health.read_health", expected_overlay=rollback_artifact),
        ]
    )
    if failure_point == "snapshot-restore":
        expected.append(_event("airflow.unpause_dag", dag_id=dags[0]))
    else:
        expected.extend(
            _event("airflow.unpause_dag", dag_id=dag_id) for dag_id in dags
        )
        expected.append(_event("airflow.capture_pause_state", dag_ids=dags))
        expected.extend(
            [
                _event("clock.utc_now"),
                _event(
                    "ledger.complete",
                    record=_completed_record(
                        target,
                        outcome=DeploymentOutcome.ROLLED_BACK,
                        health="failed",
                        artifact=rollback_artifact,
                    ),
                ),
            ]
        )
    expected.extend(_fail_closed_events(target))
    expected.append(_event("ledger.acquire_lock.exit", deployment_id=dep_id))
    assert events == expected
    _assert_all_paused_and_stable_only(events, airflow, target)


def test_rollback_failed_record_is_attempted_once_even_when_ledger_rejects_it():
    target = _target()
    previous, rollback_artifact = _previous_success(target)
    paused = {dag_id: False for dag_id in target.dag_allowlist}
    orchestrator, events, airflow = _build(
        target=target,
        previous=previous,
        paused=paused,
        overlay_failures={"overlay.restore": RuntimeError("private")},
        health_scripts={"health.read_health": ["failed"]},
        ledger_failures={"ledger.complete": RuntimeError("private ledger")},
    )
    candidate = _candidate_artifact(target)
    services = tuple(sorted(target.airflow_code_services))
    dags = tuple(sorted(target.dag_allowlist))
    dep_id = deployment_id(
        "owner/seoul-weather-platform", SHA, target_fingerprint(target)
    )

    with pytest.raises(MainDeploymentError, match="^main-deploy-rollback-failed$"):
        orchestrator.deploy(_identity(), target)

    expected = _pre_install_events(target, previous, paused)
    expected.extend(
        [
            _event(
                "compose.deploy_code_services",
                overlay_file=target.generated_overlay_file,
                services=services,
            ),
            _event("health.read_health", expected_overlay=candidate),
        ]
    )
    expected.extend(_event("airflow.pause_dag", dag_id=dag_id) for dag_id in dags)
    expected.append(_event("airflow.capture_pause_state", dag_ids=dags))
    expected.extend(
        [
            _event("clock.monotonic"),
            _event(
                "airflow.writer_run_counts",
                dag_ids=tuple(sorted(target.writer_dag_allowlist)),
            ),
        ]
    )
    expected.append(
        _event(
            "overlay.restore",
            content=rollback_artifact.content,
            sha256=rollback_artifact.sha256,
        )
    )
    expected.extend(_fail_closed_events(target))
    expected.append(_event("ledger.acquire_lock.exit", deployment_id=dep_id))
    assert events == expected
    assert sum(event.operation == "ledger.complete" for event in events) == 1
    _assert_all_paused_and_stable_only(events, airflow, target)


def test_fail_closed_readback_mismatch_uses_unverified_fixed_category():
    target = _target()
    previous, rollback_artifact = _previous_success(target)
    dags = tuple(sorted(target.dag_allowlist))
    paused = {dag_id: False for dag_id in dags}
    all_paused = {dag_id: True for dag_id in dags}
    unverified = dict(all_paused)
    unverified[dags[0]] = False
    orchestrator, events, _ = _build(
        target=target,
        previous=previous,
        paused=paused,
        airflow_scripts={
            "airflow.capture_pause_state": [
                paused,
                all_paused,
                all_paused,
                unverified,
            ]
        },
        overlay_failures={"overlay.restore": RuntimeError("private")},
        health_scripts={"health.read_health": ["failed"]},
    )
    candidate = _candidate_artifact(target)
    services = tuple(sorted(target.airflow_code_services))
    dep_id = deployment_id(
        "owner/seoul-weather-platform", SHA, target_fingerprint(target)
    )

    with pytest.raises(
        MainDeploymentError, match="^main-deploy-pause-state-unverified$"
    ):
        orchestrator.deploy(_identity(), target)

    expected = _pre_install_events(target, previous, paused)
    expected.extend(
        [
            _event(
                "compose.deploy_code_services",
                overlay_file=target.generated_overlay_file,
                services=services,
            ),
            _event("health.read_health", expected_overlay=candidate),
        ]
    )
    expected.extend(_event("airflow.pause_dag", dag_id=dag_id) for dag_id in dags)
    expected.extend(
        [
            _event("airflow.capture_pause_state", dag_ids=dags),
            _event("clock.monotonic"),
            _event(
                "airflow.writer_run_counts",
                dag_ids=tuple(sorted(target.writer_dag_allowlist)),
            ),
            _event(
                "overlay.restore",
                content=rollback_artifact.content,
                sha256=rollback_artifact.sha256,
            ),
        ]
    )
    expected.extend(
        _fail_closed_events(
            target,
            terminal_category=DeploymentTerminalCategory.PAUSE_STATE_UNVERIFIED,
        )
    )
    expected.append(_event("ledger.acquire_lock.exit", deployment_id=dep_id))
    assert events == expected
    terminal = [
        dict(event.payload)["record"]
        for event in events
        if event.operation == "ledger.complete"
    ]
    assert terminal[-1].outcome is DeploymentOutcome.ROLLBACK_FAILED
    assert (
        terminal[-1].terminal_category
        is DeploymentTerminalCategory.PAUSE_STATE_UNVERIFIED
    )
