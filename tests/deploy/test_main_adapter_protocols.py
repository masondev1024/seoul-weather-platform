from __future__ import annotations

from pathlib import PurePosixPath
from typing import get_type_hints

import pytest

import deployment.adapters as adapters
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
from deployment.models import WriterRunCounts


def test_protocol_surface_exposes_no_shell_or_runtime_escape_hatches():
    forbidden = {"run", "argv", "trigger", "backfill", "clear", "retry", "mark_success", "dbt", "trino", "d1", "r2"}
    protocols = [
        adapters.AirflowReadAdapter,
        adapters.AirflowMutationAdapter,
        adapters.ComposeAdapter,
        adapters.GitAdapter,
        adapters.HealthAdapter,
        adapters.Clock,
        adapters.OverlayStore,
        adapters.DeploymentLedgerAdapter,
    ]

    for protocol in protocols:
        names = {
            name
            for name, value in protocol.__dict__.items()
            if callable(value) and not name.startswith("_")
        }
        assert not names & forbidden


def test_protocol_methods_are_narrow_and_typed():
    assert set(get_type_hints(adapters.ComposeAdapter.deploy_code_services)) == {
        "target",
        "overlay_file",
        "services",
        "return",
    }
    assert set(get_type_hints(adapters.AirflowMutationAdapter.pause_dag)) == {
        "target",
        "dag_id",
        "return",
    }
    assert set(get_type_hints(adapters.OverlayStore.stage)) == {"artifact", "return"}
    assert set(get_type_hints(adapters.OverlayStore.verify_installed)) == {
        "expected",
        "return",
    }
    assert set(get_type_hints(adapters.DeploymentLedgerAdapter.previous_success)) == {
        "target_fingerprint",
        "return",
    }
    assert set(get_type_hints(adapters.DeploymentLedgerAdapter.acquire_lock)) == {
        "deployment_id",
        "return",
    }
    assert set(get_type_hints(adapters.HealthAdapter.read_health)) == {
        "target",
        "expected_overlay",
        "return",
    }


def test_fakes_append_typed_events_in_order_without_filesystem_or_subprocess_access():
    events = []
    airflow = FakeAirflowAdapter(events, paused={"dag": False}, run_counts={"running": 0, "queued": 0})
    compose = FakeComposeAdapter(events)
    git = FakeGitAdapter(events, checkout_root=PurePosixPath("/runtime/releases/" + "a" * 40))
    health = FakeHealthAdapter(events, result="passed")
    clock = FakeClock(events, utc_values=["2026-08-15T00:00:00Z"], monotonic_values=[1.0])
    overlay = FakeOverlayStore(events)
    ledger = FakeDeploymentLedger(events)

    airflow.capture_pause_state(("dag",))
    airflow.pause_dag(None, "dag")
    compose.validate_candidate(None, PurePosixPath("/overlay.yml"))
    git.detached_checkout("repo", "a" * 40, PurePosixPath("/runtime/releases/" + "a" * 40))
    health.read_health(None, "artifact")
    clock.utc_now()
    clock.monotonic()
    clock.sleep(0.1)
    overlay.stage("artifact")
    overlay.verify_installed("artifact")
    with ledger.acquire_lock("deployment-1"):
        pass
    ledger.read_summary()

    assert [event.operation for event in events] == [
        "airflow.capture_pause_state",
        "airflow.pause_dag",
        "compose.validate_candidate",
        "git.detached_checkout",
        "health.read_health",
        "clock.utc_now",
        "clock.monotonic",
        "clock.sleep",
        "overlay.stage",
        "overlay.verify_installed",
        "ledger.acquire_lock.enter",
        "ledger.acquire_lock.exit",
        "ledger.read_summary",
    ]


def test_writer_run_counts_reject_bool_negative_and_non_integer_values():
    assert WriterRunCounts(running=0, queued=1) == WriterRunCounts(0, 1)
    for values in [(True, 0), (0, -1), (0, 1.0)]:
        with pytest.raises(ValueError, match="non-negative integers"):
            WriterRunCounts(running=values[0], queued=values[1])


def test_fakes_script_repeated_drain_health_and_deploy_calls_in_order():
    events = []
    airflow = FakeAirflowAdapter(
        events,
        scripts={
            "airflow.writer_run_counts": [
                WriterRunCounts(1, 0),
                WriterRunCounts(0, 0),
            ]
        },
    )
    health = FakeHealthAdapter(
        events,
        result="passed",
        scripts={"health.read_health": ["failed", "passed"]},
    )
    compose = FakeComposeAdapter(
        events,
        scripts={"compose.deploy_code_services": [RuntimeError("first"), None]},
    )

    assert airflow.writer_run_counts(("writer",)) == WriterRunCounts(1, 0)
    assert airflow.writer_run_counts(("writer",)) == WriterRunCounts(0, 0)
    assert health.read_health(None, "candidate") == "failed"
    assert health.read_health(None, "rollback") == "passed"
    with pytest.raises(RuntimeError, match="first"):
        compose.deploy_code_services(None, PurePosixPath("/stable.yml"), ("svc",))
    compose.deploy_code_services(None, PurePosixPath("/stable.yml"), ("svc",))

    assert [event.operation for event in events] == [
        "airflow.writer_run_counts",
        "airflow.writer_run_counts",
        "health.read_health",
        "health.read_health",
        "compose.deploy_code_services",
        "compose.deploy_code_services",
    ]


def test_fakes_support_explicit_failure_injection():
    events = []
    airflow = FakeAirflowAdapter(events, failures={"airflow.pause_dag": RuntimeError("boom")})

    with pytest.raises(RuntimeError, match="boom"):
        airflow.pause_dag(None, "dag")

    assert events[-1].operation == "airflow.pause_dag"


def test_fake_airflow_copies_initial_pause_state():
    events = []
    initial = {"dag": False}
    airflow = FakeAirflowAdapter(events, paused=initial)

    airflow.pause_dag(None, "dag")

    assert initial == {"dag": False}
    assert airflow.paused == {"dag": True}
    assert events == [FakeAdapterEvent("airflow.pause_dag", (("dag_id", "dag"),))]


def test_fake_ledger_lock_supports_failure_injection():
    events = []
    ledger = FakeDeploymentLedger(
        events,
        failures={"ledger.acquire_lock.enter": RuntimeError("lock-denied")},
    )

    with pytest.raises(RuntimeError, match="lock-denied"):
        with ledger.acquire_lock("deployment-1"):
            pass

    assert [event.operation for event in events] == ["ledger.acquire_lock.enter"]
