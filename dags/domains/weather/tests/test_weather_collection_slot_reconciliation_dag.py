from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from common.raw_manifest import build_raw_manifest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import weather_vilage_fcst_collection_slot_reconciliation as module  # noqa: E402
from weather_ingest.collection_slots import weather_vilage_fcst_slots  # noqa: E402
from weather_ingest.landing import KmaGrid  # noqa: E402


@dataclass
class _Storage:
    documents: dict[str, object]

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(key for key in self.documents if key.startswith(prefix))

    def read_json(self, key: str) -> object:
        return self.documents[key]

    def write_json_if_absent(self, key: str, value: object) -> bool:
        if key in self.documents:
            return False
        self.documents[key] = value
        return True


def test_weather_reconciliation_dag_is_paused_api_free_control_plane():
    source = (
        Path(__file__).resolve().parents[1]
        / "weather_vilage_fcst_collection_slot_reconciliation.py"
    ).read_text(encoding="utf-8")
    task = module.dag.get_task("reconcile_due_weather_collection_slots")

    assert module.dag.is_paused_upon_creation is True
    assert module.dag.catchup is False
    assert module.dag.max_active_runs == 1
    schedule = getattr(module.dag, "schedule", None)
    if schedule is None:
        schedule = getattr(module.dag, "schedule_interval", None)
    assert schedule == "20 3,6,9,12,15,18,21,0 * * *"
    assert task.python_callable is module.reconcile_due_weather_collection_slots
    assert "build_weather_landing" not in source
    assert "KmaHttpAdapter" not in source
    assert "fetch_url" not in source


def test_weather_due_policy_stays_historical_or_unrecoverable_without_manifest():
    slot = weather_vilage_fcst_slots(
        "20260808",
        "0800",
        (KmaGrid("jongno", 60, 127),),
        recovery_boundary="2026-08-01T00:00:00+00:00",
    )[0]
    common = {
        "event_at": datetime(2026, 8, 8, 0, 5, tzinfo=timezone.utc),
        "dag_id": module.DAG_ID,
        "dag_run_id": "scheduled__2026-08-08T00:20:00+00:00",
    }

    historical = module.weather_missing_outcome(
        slot,
        raw_manifest_key="raw/weather/diagnostic.json",
        raw_object_count=1,
        raw_manifest_verified=False,
        **common,
    )
    assert historical.collection_state == "missing_unknown"
    assert historical.recovery_state == "pending"
    assert historical.recovery_class == "historical_query"
    assert historical.raw_manifest_key is None
    assert historical.recovery_evidence_code == "weather_apihub_historical_range"

    old_slot = weather_vilage_fcst_slots(
        "20260701",
        "0800",
        (KmaGrid("jongno", 60, 127),),
        recovery_boundary="2026-08-01T00:00:00+00:00",
    )[0]
    unrecoverable = module.weather_missing_outcome(
        old_slot,
        raw_manifest_key=None,
        raw_object_count=None,
        raw_manifest_verified=False,
        **common,
    )
    assert unrecoverable.recovery_state == "unrecoverable"
    assert unrecoverable.recovery_class == "none"


def test_weather_verified_raw_manifest_maps_only_to_the_matching_issue_grid_slot(
    monkeypatch,
):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    raw_object_key = (
        "raw/weather/kma_vilage_fcst/load_date=2026-08-08/"
        "run_id=scheduled__weather/nx=60/ny=127/"
        "20260808T080500KST_base-202608080800_request-1.json"
    )
    manifest_key = (
        "raw/weather/kma_vilage_fcst/load_date=2026-08-08/"
        "run_id=scheduled__weather/_manifest.json"
    )
    manifest = build_raw_manifest(
        run_id="scheduled__weather",
        dataset="kma_vilage_fcst",
        load_date="2026-08-08",
        object_keys=[raw_object_key],
        expected_count=1,
        actual_count=1,
        completed_at="2026-08-08T08:06:00+09:00",
    )
    slot = weather_vilage_fcst_slots(
        "20260808",
        "0800",
        (KmaGrid("jongno", 60, 127),),
        recovery_boundary="2026-08-01T00:00:00+00:00",
    )[0]

    evidence = module.weather_raw_manifest_evidence_by_slot(
        _Storage({manifest_key: manifest}),
        (slot,),
    )

    assert evidence == {
        slot.expected_slot_id: {
            "raw_manifest_key": manifest_key,
            "raw_object_count": 1,
            "raw_manifest_verified": True,
        }
    }


def test_weather_subset_raw_manifest_is_not_replay_evidence_for_all_planned_grids(
    monkeypatch,
):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    raw_object_key = (
        "raw/weather/kma_vilage_fcst/load_date=2026-08-08/"
        "run_id=scheduled__weather/nx=60/ny=127/"
        "20260808T080500KST_base-202608080800_request-1.json"
    )
    manifest_key = (
        "raw/weather/kma_vilage_fcst/load_date=2026-08-08/"
        "run_id=scheduled__weather/_manifest.json"
    )
    manifest = build_raw_manifest(
        run_id="scheduled__weather",
        dataset="kma_vilage_fcst",
        load_date="2026-08-08",
        object_keys=[raw_object_key],
        expected_count=1,
        actual_count=1,
        completed_at="2026-08-08T08:06:00+09:00",
    )
    slots = weather_vilage_fcst_slots(
        "20260808",
        "0800",
        (
            KmaGrid("jongno", 60, 127),
            KmaGrid("jung", 61, 127),
        ),
        recovery_boundary="2026-08-01T00:00:00+00:00",
    )

    evidence = module.weather_raw_manifest_evidence_by_slot(
        _Storage({manifest_key: manifest}),
        slots,
    )

    assert evidence == {}


def test_weather_reconciliation_declares_the_issue_cycle_that_is_due_after_grace(
    monkeypatch,
):
    monkeypatch.setattr(
        module,
        "load_kma_grids",
        lambda: [{"place_id": "jongno", "nx": 60, "ny": 127}],
    )

    slots = module.weather_reconciliation_candidate_slots(
        {
            "data_interval_end": datetime(
                2026,
                8,
                8,
                3,
                20,
                tzinfo=timezone.utc,
            )
        },
        recovery_boundary="2026-08-01T00:00:00+00:00",
    )

    assert len(slots) == 1
    assert slots[0].collection_slot_at == "2026-08-08T02:00:00+00:00"
    assert slots[0].deadline_at == "2026-08-08T03:00:00+00:00"


def test_weather_reconciliation_finalizes_a_missed_due_slot_without_source_io(
    monkeypatch,
):
    storage = _Storage({})
    monkeypatch.setenv("ASK_SEOUL_TARGET", "dev")
    monkeypatch.setenv(
        "ASK_SEOUL_COLLECTION_SLOT_ACTIVATION_AT",
        "2026-08-01T00:00:00+00:00",
    )
    monkeypatch.setenv(
        "ASK_SEOUL_WEATHER_API_HUB_HISTORICAL_EARLIEST_ISSUED_AT",
        "2026-08-01T00:00:00+00:00",
    )
    monkeypatch.setattr(module, "build_weather_collection_slot_storage", lambda: storage)
    monkeypatch.setattr(
        module,
        "load_kma_grids",
        lambda: [{"place_id": "jongno", "nx": 60, "ny": 127}],
    )
    monkeypatch.setattr(
        module,
        "utc_now",
        lambda: datetime(2026, 8, 8, 3, 30, tzinfo=timezone.utc),
    )

    result = module.reconcile_due_weather_collection_slots(
        run_id="scheduled__2026-08-08T03:20:00+00:00",
        data_interval_end=datetime(2026, 8, 8, 3, 20, tzinfo=timezone.utc),
    )

    assert result == {
        "declared": 1,
        "not_due": 0,
        "already_terminal": 0,
        "finalized": 1,
    }
    event = next(
        value
        for key, value in storage.documents.items()
        if "/events/" in key
    )
    assert event["collection_state"] == "missing_unknown"
    assert event["recovery_class"] == "historical_query"
    assert event["recovery_evidence_code"] == "weather_apihub_historical_range"
