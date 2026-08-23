from __future__ import annotations

import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


DOMAIN_ROOT = Path(__file__).resolve().parents[1]
DAGS_ROOT = DOMAIN_ROOT.parents[1]
sys.path.insert(0, str(DOMAIN_ROOT))
sys.path.insert(0, str(DAGS_ROOT))

from weather_ingest.kma_coordination import (  # noqa: E402
    CycleDeadline,
    CycleDeadlineConfigurationError,
    CycleDeadlineExceeded,
    CycleIdentity,
    deadline_checkpoint_key,
    initialize_cycle_deadline,
    serialize_deadline_anchor,
)


START = datetime(2026, 8, 22, 0, 45, tzinfo=timezone.utc)


class MemoryCheckpointStore:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.lock = threading.Lock()

    def read_bytes(self, key: str) -> bytes:
        with self.lock:
            return self.objects[key]

    def write_bytes_if_absent(
        self,
        key: str,
        payload: bytes,
        content_type: str,
    ) -> bool:
        assert content_type == "application/json"
        with self.lock:
            if key in self.objects:
                return False
            self.objects[key] = payload
            return True


def _identity() -> CycleIdentity:
    return CycleIdentity(
        dag_id="weather_ultra_srt_ncst_bronze",
        dag_run_id="scheduled__2026-08-22T00:45:00+00:00",
        source_id="kma_ultra_srt_ncst",
        observed_slot="2026-08-22T09:00:00+09:00",
    )


def test_first_initialization_persists_one_immutable_40_minute_anchor():
    store = MemoryCheckpointStore()

    anchor = initialize_cycle_deadline(store, _identity(), now=lambda: START)
    repeated = initialize_cycle_deadline(
        store,
        _identity(),
        now=lambda: START + timedelta(minutes=5),
    )

    assert anchor.started_at == START
    assert anchor.deadline_at == START + timedelta(minutes=40)
    assert repeated == anchor
    assert len(store.objects) == 1


def test_new_process_reads_only_the_original_remaining_budget():
    store = MemoryCheckpointStore()
    anchor = initialize_cycle_deadline(store, _identity(), now=lambda: START)

    deadline = CycleDeadline(
        anchor,
        wall_clock=lambda: START + timedelta(minutes=12),
        monotonic_clock=lambda: 100.0,
    )

    assert deadline.remaining_seconds() == pytest.approx(28 * 60)


def test_concurrent_initialization_converges_on_one_anchor():
    store = MemoryCheckpointStore()
    identity = _identity()

    def initialize(index: int):
        return initialize_cycle_deadline(
            store,
            identity,
            now=lambda: START + timedelta(seconds=index),
        )

    with ThreadPoolExecutor(max_workers=10) as executor:
        anchors = list(executor.map(initialize, range(10)))

    assert len(set(anchors)) == 1
    assert len(store.objects) == 1


def test_conflicting_checkpoint_identity_fails_closed():
    store = MemoryCheckpointStore()
    identity = _identity()
    anchor = initialize_cycle_deadline(store, identity, now=lambda: START)
    conflicting = replace(anchor, source_id="foreign_source")
    store.objects[deadline_checkpoint_key(identity)] = serialize_deadline_anchor(
        conflicting
    )

    with pytest.raises(CycleDeadlineConfigurationError, match="identity"):
        initialize_cycle_deadline(store, identity, now=lambda: START)


def test_malformed_or_non_40_minute_anchor_fails_closed():
    store = MemoryCheckpointStore()
    identity = _identity()
    anchor = initialize_cycle_deadline(store, identity, now=lambda: START)
    malformed = replace(anchor, deadline_at=START + timedelta(minutes=60))
    store.objects[deadline_checkpoint_key(identity)] = serialize_deadline_anchor(
        malformed
    )

    with pytest.raises(CycleDeadlineConfigurationError, match="40 minutes"):
        initialize_cycle_deadline(store, identity, now=lambda: START)


def test_remaining_time_never_increases_when_wall_clock_moves_backward():
    store = MemoryCheckpointStore()
    anchor = initialize_cycle_deadline(store, _identity(), now=lambda: START)
    wall = [START]
    monotonic = [100.0]
    deadline = CycleDeadline(
        anchor,
        wall_clock=lambda: wall[0],
        monotonic_clock=lambda: monotonic[0],
    )

    assert deadline.remaining_seconds() == pytest.approx(2400)
    wall[0] += timedelta(seconds=10)
    monotonic[0] += 10
    assert deadline.remaining_seconds() == pytest.approx(2390)
    wall[0] -= timedelta(minutes=5)
    monotonic[0] += 5
    assert deadline.remaining_seconds() == pytest.approx(2385)


def test_request_admission_reserves_timeout_and_headroom_at_exact_boundary():
    store = MemoryCheckpointStore()
    anchor = initialize_cycle_deadline(store, _identity(), now=lambda: START)
    deadline = CycleDeadline(
        anchor,
        wall_clock=lambda: anchor.deadline_at - timedelta(seconds=35),
        monotonic_clock=lambda: 100.0,
    )

    deadline.require_request(request_timeout_seconds=30, headroom_seconds=5)


def test_request_is_rejected_when_timeout_and_headroom_cross_deadline():
    store = MemoryCheckpointStore()
    anchor = initialize_cycle_deadline(store, _identity(), now=lambda: START)
    deadline = CycleDeadline(
        anchor,
        wall_clock=lambda: anchor.deadline_at - timedelta(seconds=34.9),
        monotonic_clock=lambda: 100.0,
    )

    with pytest.raises(CycleDeadlineExceeded, match="request"):
        deadline.require_request(request_timeout_seconds=30, headroom_seconds=5)


def test_retry_sleep_is_rejected_when_sleep_plus_request_headroom_will_not_fit():
    store = MemoryCheckpointStore()
    anchor = initialize_cycle_deadline(store, _identity(), now=lambda: START)
    deadline = CycleDeadline(
        anchor,
        wall_clock=lambda: anchor.deadline_at - timedelta(seconds=39),
        monotonic_clock=lambda: 100.0,
    )

    with pytest.raises(CycleDeadlineExceeded, match="retry sleep"):
        deadline.require_retry_sleep(sleep_seconds=10, request_headroom_seconds=30)


def test_expired_cycle_fails_before_http_admission():
    store = MemoryCheckpointStore()
    anchor = initialize_cycle_deadline(store, _identity(), now=lambda: START)
    deadline = CycleDeadline(
        anchor,
        wall_clock=lambda: anchor.deadline_at,
        monotonic_clock=lambda: 100.0,
    )

    with pytest.raises(CycleDeadlineExceeded, match="request"):
        deadline.require_request(request_timeout_seconds=1, headroom_seconds=0)
