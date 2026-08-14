from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.collection_slots import (  # noqa: E402
    WeatherCollectionSlotError,
    weather_collection_failure_outcomes,
    weather_collection_success_outcomes,
    weather_vilage_fcst_slots,
)
from weather_ingest.landing import KmaGrid  # noqa: E402
from weather_ingest import runtime  # noqa: E402
from weather_ingest.runtime import (  # noqa: E402
    weather_collection_slots_for_issue,
    weather_raw_manifest_is_verified,
)
from common.collection_slots.activation import CollectionSlotActivationError  # noqa: E402
from common.raw_manifest import build_raw_manifest  # noqa: E402


KST = ZoneInfo("Asia/Seoul")
BOUNDARY = "2026-08-01T00:00:00+00:00"


def _slots():
    return weather_vilage_fcst_slots(
        "20260808",
        "0800",
        (
            KmaGrid("jongno", 60, 127),
            KmaGrid("gangnam", 61, 126),
        ),
        recovery_boundary=BOUNDARY,
    )


def test_weather_slots_use_kst_issue_cycle_and_grid_identity_stably():
    slots = _slots()

    assert [slot.collection_slot_at for slot in slots] == [
        "2026-08-07T23:00:00+00:00",
        "2026-08-07T23:00:00+00:00",
    ]
    assert [slot.deadline_at for slot in slots] == [
        "2026-08-08T00:00:00+00:00",
        "2026-08-08T00:00:00+00:00",
    ]
    assert [dict(slot.grain) for slot in slots] == [
        {"place_id": "jongno", "nx": 60, "ny": 127},
        {"place_id": "gangnam", "nx": 61, "ny": 126},
    ]
    assert all(slot.recovery_boundary == BOUNDARY for slot in slots)
    assert [slot.expected_slot_id for slot in slots] == [
        slot.expected_slot_id
        for slot in weather_vilage_fcst_slots(
            "20260808",
            "0800",
            (
                KmaGrid("jongno", 60, 127),
                KmaGrid("gangnam", 61, 126),
            ),
            recovery_boundary=BOUNDARY,
        )
    ]


def test_weather_slots_reject_duplicate_grids_and_invalid_issue_cycle():
    with pytest.raises(WeatherCollectionSlotError, match="duplicate grid"):
        weather_vilage_fcst_slots(
            "20260808",
            "0800",
            (KmaGrid("jongno", 60, 127), KmaGrid("another", 60, 127)),
            recovery_boundary=BOUNDARY,
        )

    with pytest.raises(WeatherCollectionSlotError, match="base_time"):
        weather_vilage_fcst_slots(
            "20260808",
            "0815",
            (KmaGrid("jongno", 60, 127),),
            recovery_boundary=BOUNDARY,
        )


def test_weather_runtime_slot_gate_requires_an_active_concrete_historical_boundary(
    monkeypatch,
):
    grids = (KmaGrid("jongno", 60, 127),)
    monkeypatch.delenv("ASK_SEOUL_COLLECTION_SLOT_ACTIVATION_AT", raising=False)
    monkeypatch.delenv(
        "ASK_SEOUL_WEATHER_API_HUB_HISTORICAL_EARLIEST_ISSUED_AT",
        raising=False,
    )

    assert weather_collection_slots_for_issue("20260808", "0800", grids) == ()

    monkeypatch.setenv(
        "ASK_SEOUL_COLLECTION_SLOT_ACTIVATION_AT",
        "2026-08-07T22:00:00+00:00",
    )
    with pytest.raises(CollectionSlotActivationError, match="HISTORICAL_EARLIEST"):
        weather_collection_slots_for_issue("20260808", "0800", grids)

    monkeypatch.setenv(
        "ASK_SEOUL_WEATHER_API_HUB_HISTORICAL_EARLIEST_ISSUED_AT",
        BOUNDARY,
    )
    slots = weather_collection_slots_for_issue("20260808", "0800", grids)

    assert len(slots) == 1
    assert slots[0].recovery_boundary == BOUNDARY


def test_weather_raw_manifest_verification_requires_the_complete_planned_grid(
    monkeypatch,
):
    slots = _slots()
    first_key = (
        "raw/weather/kma_vilage_fcst/load_date=2026-08-08/"
        "run_id=scheduled__weather/nx=60/ny=127/"
        "20260808T080500KST_base-202608080800_request-1.json"
    )
    second_key = (
        "raw/weather/kma_vilage_fcst/load_date=2026-08-08/"
        "run_id=scheduled__weather/nx=61/ny=126/"
        "20260808T080500KST_base-202608080800_request-2.json"
    )

    class Storage:
        def __init__(self, document: dict[str, object]) -> None:
            self.document = document

        def read_json(self, _key: str) -> dict[str, object]:
            return self.document

    def raw_result(*object_keys: str) -> dict[str, object]:
        return {
            "manifest_key": "raw/weather/kma_vilage_fcst/_manifest.json",
            "raw_objects": [
                {"raw_object_key": object_key} for object_key in object_keys
            ],
        }

    incomplete_manifest = build_raw_manifest(
        run_id="scheduled__weather",
        dataset="kma_vilage_fcst",
        load_date="2026-08-08",
        object_keys=[first_key],
        expected_count=1,
        actual_count=1,
        completed_at="2026-08-08T08:06:00+09:00",
    )
    monkeypatch.setattr(
        runtime,
        "build_weather_collection_slot_storage",
        lambda: Storage(incomplete_manifest),
    )

    assert not weather_raw_manifest_is_verified(
        raw_result(first_key),
        dag_run_id="scheduled__weather",
        slots=slots,
    )

    complete_manifest = build_raw_manifest(
        run_id="scheduled__weather",
        dataset="kma_vilage_fcst",
        load_date="2026-08-08",
        object_keys=[first_key, second_key],
        expected_count=2,
        actual_count=2,
        completed_at="2026-08-08T08:06:00+09:00",
    )
    monkeypatch.setattr(
        runtime,
        "build_weather_collection_slot_storage",
        lambda: Storage(complete_manifest),
    )

    assert weather_raw_manifest_is_verified(
        raw_result(first_key, second_key),
        dag_run_id="scheduled__weather",
        slots=slots,
    )


def test_weather_success_outcomes_require_exact_verified_grid_evidence():
    slots = _slots()
    outcomes = weather_collection_success_outcomes(
        slots,
        raw_manifest_key="raw/weather/kma/_manifest.json",
        raw_objects=(
            {
                "raw_object_key": "raw/weather/kma/jongno-page-1.json",
                "place_id": "jongno",
                "nx": 60,
                "ny": 127,
                "base_date": "20260808",
                "base_time": "0800",
                "row_count": 2,
            },
            {
                "raw_object_key": "raw/weather/kma/gangnam-page-1.json",
                "place_id": "gangnam",
                "nx": 61,
                "ny": 126,
                "base_date": "20260808",
                "base_time": "0800",
                "row_count": 1,
            },
        ),
        verified_rows=3,
        event_at=datetime(2026, 8, 8, 0, 5, tzinfo=KST),
        dag_id="weather_vilage_fcst_bronze",
        dag_run_id="scheduled__2026-08-08T08:00:00+09:00",
        task_id="verify_kma_bronze_runtime",
    )

    assert [outcome.collection_state for outcome in outcomes] == [
        "observed",
        "observed",
    ]
    assert [outcome.row_count for outcome in outcomes] == [2, 1]
    assert all(outcome.source_result_code == "00" for outcome in outcomes)
    assert all(outcome.recovery_state == "not_required" for outcome in outcomes)

    with pytest.raises(WeatherCollectionSlotError, match="requires forecast rows"):
        weather_collection_success_outcomes(
            slots,
            raw_manifest_key="raw/weather/kma/_manifest.json",
            raw_objects=(
                {
                    "raw_object_key": "raw/weather/kma/jongno-page-1.json",
                    "place_id": "jongno",
                    "nx": 60,
                    "ny": 127,
                    "base_date": "20260808",
                    "base_time": "0800",
                    "row_count": 2,
                },
                {
                    "raw_object_key": "raw/weather/kma/gangnam-page-1.json",
                    "place_id": "gangnam",
                    "nx": 61,
                    "ny": 126,
                    "base_date": "20260808",
                    "base_time": "0800",
                    "row_count": 0,
                },
            ),
            verified_rows=2,
            event_at=datetime(2026, 8, 8, 0, 5, tzinfo=KST),
            dag_id="weather_vilage_fcst_bronze",
            dag_run_id="scheduled__2026-08-08T08:00:00+09:00",
            task_id="verify_kma_bronze_runtime",
        )

    with pytest.raises(WeatherCollectionSlotError, match="duplicate raw_object_key"):
        weather_collection_success_outcomes(
            slots,
            raw_manifest_key="raw/weather/kma/_manifest.json",
            raw_objects=(
                {
                    "raw_object_key": "raw/weather/kma/jongno-page-1.json",
                    "place_id": "jongno",
                    "nx": 60,
                    "ny": 127,
                    "base_date": "20260808",
                    "base_time": "0800",
                    "row_count": 1,
                },
                {
                    "raw_object_key": "raw/weather/kma/jongno-page-1.json",
                    "place_id": "jongno",
                    "nx": 60,
                    "ny": 127,
                    "base_date": "20260808",
                    "base_time": "0800",
                    "row_count": 1,
                },
            ),
            verified_rows=2,
            event_at=datetime(2026, 8, 8, 0, 5, tzinfo=KST),
            dag_id="weather_vilage_fcst_bronze",
            dag_run_id="scheduled__2026-08-08T08:00:00+09:00",
            task_id="verify_kma_bronze_runtime",
        )


def test_weather_failure_outcomes_never_promote_diagnostic_raw_to_raw_replay():
    slots = _slots()
    common = {
        "event_at": datetime(2026, 8, 8, 0, 5, tzinfo=KST),
        "dag_id": "weather_vilage_fcst_bronze",
        "dag_run_id": "scheduled__2026-08-08T08:00:00+09:00",
        "task_id": "land_kma_raw",
    }

    raw_replay = weather_collection_failure_outcomes(
        slots,
        raw_manifest_key="raw/weather/kma/_manifest.json",
        raw_object_count=2,
        raw_manifest_verified=True,
        **common,
    )
    assert all(outcome.recovery_state == "pending" for outcome in raw_replay)
    assert all(outcome.recovery_class == "raw_replay" for outcome in raw_replay)
    assert all(
        outcome.recovery_evidence_code == "raw_manifest_verified"
        for outcome in raw_replay
    )

    historical = weather_collection_failure_outcomes(
        slots,
        raw_manifest_key="raw/weather/kma/diagnostic.json",
        raw_object_count=1,
        raw_manifest_verified=False,
        **common,
    )
    assert all(outcome.recovery_state == "pending" for outcome in historical)
    assert all(outcome.recovery_class == "historical_query" for outcome in historical)
    assert all(
        outcome.recovery_evidence_code == "weather_apihub_historical_range"
        for outcome in historical
    )

    old_slot = weather_vilage_fcst_slots(
        "20260701",
        "0800",
        (KmaGrid("jongno", 60, 127),),
        recovery_boundary=BOUNDARY,
    )
    unrecoverable = weather_collection_failure_outcomes(
        old_slot,
        raw_manifest_key=None,
        raw_object_count=None,
        raw_manifest_verified=False,
        **common,
    )
    assert unrecoverable[0].recovery_state == "unrecoverable"
    assert unrecoverable[0].recovery_class == "none"
    assert unrecoverable[0].recovery_evidence_code is None
