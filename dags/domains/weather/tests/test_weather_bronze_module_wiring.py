from __future__ import annotations

import sys
from pathlib import Path

import pytest
from airflow.exceptions import AirflowSkipException

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import weather_vilage_fcst_bronze as dag_module  # noqa: E402
from weather_ingest.landing import (  # noqa: E402
    KmaGrid,
    KmaLandingRequest,
    RunIdentity,
)
from weather_ingest.collection_slots import weather_vilage_fcst_slots  # noqa: E402
from weather_ingest.run_manifest import WeatherRun  # noqa: E402


class Dag:
    dag_id = "weather_vilage_fcst_bronze"


class DagRun:
    conf = {"load_date": "2026-07-10"}


class Result:
    def __init__(self, document: dict) -> None:
        self._document = document

    def to_xcom(self) -> dict:
        return self._document


def test_expected_raw_object_count_key_has_one_entrypoint_owner():
    source = (
        Path(__file__).resolve().parents[1] / "weather_vilage_fcst_bronze.py"
    ).read_text(encoding="utf-8")

    assert dag_module.EXPECTED_RAW_OBJECT_COUNT_KEY == "expected_raw_object_count"
    assert source.count('"expected_raw_object_count"') == 1
    assert source.count("EXPECTED_RAW_OBJECT_COUNT_KEY") >= 6


def test_bronze_batch_depends_on_airflow_free_raw_contract():
    domain_root = Path(__file__).resolve().parents[1]
    batch_source = (domain_root / "weather_ingest" / "bronze_batch.py").read_text(
        encoding="utf-8"
    )
    contract_path = domain_root / "weather_ingest" / "raw_contract.py"

    assert contract_path.is_file()
    contract_source = contract_path.read_text(encoding="utf-8")
    assert "bronze_dag_support" not in batch_source
    assert "from airflow" not in batch_source
    assert "import airflow" not in batch_source
    assert "from airflow" not in contract_source
    assert "import airflow" not in contract_source
    assert "from weather_ingest.raw_contract import raw_object_page_no" in batch_source


def test_live_landing_wrapper_only_maps_airflow_context_to_domain_module(monkeypatch):
    captured: dict[str, object] = {}

    class Landing:
        def collect(self, run, request):
            captured.update(run=run, request=request)
            return Result({"raw_object_keys": ["raw/weather/page.json"]})

    monkeypatch.setattr(dag_module, "build_weather_landing", lambda: Landing())
    monkeypatch.setattr(
        dag_module, "resolve_kma_base_datetime", lambda: ("20260714", "0800")
    )
    monkeypatch.setattr(
        dag_module,
        "load_kma_grids",
        lambda: [{"place_id": "jongno", "nx": 60, "ny": 127}],
    )
    monkeypatch.setattr(dag_module, "kma_num_of_rows", lambda: 500)

    result = dag_module.land_kma_raw(
        dag=Dag(),
        dag_run=DagRun(),
        run_id="manual__weather",
    )

    assert result == {"raw_object_keys": ["raw/weather/page.json"]}
    assert captured["run"] == RunIdentity(
        Dag.dag_id,
        "manual__weather",
        landing_load_date="2026-07-10",
    )
    assert captured["request"] == KmaLandingRequest(
        base_date="20260714",
        base_time="0800",
        grids=(KmaGrid("jongno", 60, 127),),
        num_of_rows=500,
    )


def test_replay_wrapper_delegates_raw_keys_and_grid_identity(monkeypatch):
    raw_keys = ["raw/weather/page.json"]
    captured: dict[str, object] = {}

    class Landing:
        def replay(self, keys, *, grids, run):
            captured.update(keys=keys, grids=grids, run=run)
            return Result({"raw_object_keys": keys})

    class BackfillDagRun:
        conf = {"raw_object_keys": raw_keys, "load_date": "2026-07-10"}

    monkeypatch.setattr(dag_module, "build_weather_landing", lambda: Landing())
    monkeypatch.setattr(
        dag_module,
        "load_kma_grids",
        lambda: [{"place_id": "jongno", "nx": 60, "ny": 127}],
    )

    result = dag_module.land_kma_raw_object_keys(
        dag=Dag(),
        dag_run=BackfillDagRun(),
        run_id="manual__backfill",
    )

    assert captured == {
        "keys": raw_keys,
        "grids": (KmaGrid("jongno", 60, 127),),
        "run": RunIdentity(
            Dag.dag_id,
            "manual__backfill",
            landing_load_date="2026-07-10",
        ),
    }
    assert result == {"raw_object_keys": raw_keys}


def test_manifest_wrappers_use_weather_owned_contract(monkeypatch):
    calls: list[tuple[str, WeatherRun, dict]] = []

    class Manifest:
        def start(self, run, **metrics):
            calls.append(("start", run, metrics))
            return "manifest-table"

        def complete(self, run, **metrics):
            calls.append(("complete", run, metrics))
            return "manifest-table"

    monkeypatch.setattr(dag_module, "build_weather_manifest", lambda: Manifest())
    monkeypatch.setattr(
        dag_module,
        "load_kma_grids",
        lambda: [{"place_id": "jongno", "nx": 60, "ny": 127}],
    )
    monkeypatch.setattr(dag_module, "verify_kma_bronze_rows", lambda **_kwargs: 3)

    assert (
        dag_module.record_kma_run_started(dag=Dag(), run_id="manual__weather")
        == "manifest-table"
    )
    verified = dag_module.verify_kma_bronze_runtime(
        dag=Dag(),
        run_id="manual__weather",
        ti=type(
            "TI",
            (),
            {
                "xcom_pull": lambda _self, **_kwargs: {
                    "raw_object_keys": ["raw/weather/page.json"],
                    "inserted": 3,
                    "expected_rows": 3,
                    "expected_raw_object_count": 1,
                }
            },
        )(),
    )

    run = WeatherRun(Dag.dag_id, "manual__weather")
    assert verified == 3
    assert calls == [
        ("start", run, {"expected_raw_objects": 1}),
        (
            "complete",
            run,
            {
                "expected_rows": 3,
                "actual_rows": 3,
                "expected_raw_objects": 1,
                "actual_raw_objects": 1,
                "is_publishable": True,
            },
        ),
    ]


def test_weather_product_events_use_raw_and_bronze_manifest_rows(monkeypatch):
    captured: list[dict] = []
    monkeypatch.setattr(
        dag_module,
        "record_product_event",
        lambda _context, **kwargs: captured.append(kwargs) or kwargs,
    )
    monkeypatch.setattr(
        dag_module,
        "pull_kma_raw_result",
        lambda _context: {
            "raw_objects": [
                {"row_count": 3},
                {"row_count": 4},
            ]
        },
    )

    dag_module.record_weather_raw_product_event({"run_id": "run-1"})
    dag_module.record_weather_bronze_product_event(
        {
            "run_id": "run-1",
            "ti": type(
                "TI",
                (),
                {
                    "xcom_pull": lambda _self, **_kwargs: 7,
                },
            )(),
        }
    )

    assert captured == [
        {
            "domain": "weather",
            "layer": "raw",
            "row_count": 7,
            "rows_source": "raw_manifest",
        },
        {
            "domain": "weather",
            "layer": "bronze",
            "row_count": 7,
            "rows_source": "bronze_run_manifest",
        },
    ]


def test_weather_product_event_keeps_malformed_rows_unknown(monkeypatch):
    captured: list[dict] = []
    monkeypatch.setattr(
        dag_module,
        "record_product_event",
        lambda _context, **kwargs: captured.append(kwargs) or kwargs,
    )
    monkeypatch.setattr(
        dag_module,
        "pull_kma_raw_result",
        lambda _context: {"raw_objects": [{"row_count": "7"}]},
    )

    dag_module.record_weather_raw_product_event({"run_id": "run-1"})

    assert captured == [
        {
            "domain": "weather",
            "layer": "raw",
            "row_count": None,
            "rows_source": "not_observed",
        }
    ]


def test_verify_records_subset_backfill_as_success_nonpublishable(monkeypatch):
    calls: list[tuple[WeatherRun, dict]] = []

    class Manifest:
        def complete(self, run, **metrics):
            calls.append((run, metrics))
            return "manifest-table"

    monkeypatch.setattr(dag_module, "build_weather_manifest", lambda: Manifest())
    monkeypatch.setattr(dag_module, "verify_kma_bronze_rows", lambda **_kwargs: 3)

    verified = dag_module.verify_kma_bronze_runtime(
        dag=Dag(),
        run_id="manual__subset-backfill",
        ti=type(
            "TI",
            (),
            {
                "xcom_pull": lambda _self, **_kwargs: {
                    "raw_object_keys": ["raw/weather/page.json"],
                    "inserted": 3,
                    "expected_rows": 3,
                    "expected_raw_object_count": 1,
                    "is_publishable": False,
                }
            },
        )(),
    )

    assert verified == 3
    assert calls == [
        (
            WeatherRun(Dag.dag_id, "manual__subset-backfill"),
            {
                "expected_rows": 3,
                "actual_rows": 3,
                "expected_raw_objects": 1,
                "actual_raw_objects": 1,
                "is_publishable": False,
            },
        )
    ]


def test_publish_weather_bronze_asset_records_snapshot_identity_without_secrets():
    class OutletEvent:
        extra = None

    event = OutletEvent()

    class TI:
        def xcom_pull(self, *, task_ids):
            if task_ids == "load_kma_bronze":
                return {"inserted": 3, "is_publishable": True}
            return {
                "raw_objects": [
                    {
                        "raw_hash": "b" * 64,
                        "collected_at": "2026-07-15T12:00:00+09:00",
                    }
                ]
            }

    result = dag_module.publish_weather_bronze_asset(
        run_id="scheduled__weather-1",
        ti=TI(),
        outlet_events={dag_module.WEATHER_BRONZE_ASSET_REF: event},
    )

    assert result == "scheduled__weather-1"
    assert event.extra == {
        "source_id": "kma_vilage_fcst",
        "bronze_run_id": "scheduled__weather-1",
        "bronze_dag_run_id": "scheduled__weather-1",
        "event_at": "2026-07-15T12:00:00+09:00",
        "load_date": "2026-07-15",
        "row_count": 3,
        "payload_hash": "b" * 64,
        "is_publishable": True,
    }
    assert "serviceKey" not in repr(event.extra)


def test_publish_weather_bronze_asset_does_not_emit_nonpublishable_event():
    class OutletEvent:
        extra = None

    event = OutletEvent()

    class TI:
        def xcom_pull(self, *, task_ids):
            if task_ids == "load_kma_bronze":
                return {"inserted": 3, "is_publishable": False}
            return {"raw_objects": []}

    with pytest.raises(AirflowSkipException, match="not publishable"):
        dag_module.publish_weather_bronze_asset(
            run_id="manual__subset",
            ti=TI(),
            outlet_events={dag_module.WEATHER_BRONZE_ASSET_REF: event},
        )

    assert event.extra is None


def test_weather_bronze_asset_is_owned_by_publish_gate_after_verification():
    verify = dag_module.dag.get_task("verify_kma_bronze_runtime")
    record_slot_outcome = dag_module.dag.get_task(
        "record_weather_collection_slot_success"
    )
    publish = dag_module.dag.get_task("publish_weather_bronze_asset")

    assert verify.outlets == []
    assert publish.outlets == [dag_module.WEATHER_BRONZE_ASSET_REF]
    assert record_slot_outcome.task_id in verify.downstream_task_ids
    assert publish.task_id in record_slot_outcome.downstream_task_ids


def test_weather_slot_plan_locks_the_issue_cycle_for_landing(monkeypatch):
    expected_slots = []
    captured: dict[str, object] = {}

    class SlotReceipts:
        def record_expected(self, slot):
            expected_slots.append(slot)

    class Landing:
        def collect(self, run, request):
            captured.update(run=run, request=request)
            return Result({"raw_object_keys": ["raw/weather/page.json"]})

    grids = (KmaGrid("jongno", 60, 127),)
    monkeypatch.setattr(
        dag_module,
        "build_weather_collection_slot_receipt_ports",
        lambda: (
            SlotReceipts(),
            lambda base_date, base_time, actual_grids: weather_vilage_fcst_slots(
                base_date,
                base_time,
                actual_grids,
                recovery_boundary="2026-08-01T00:00:00+00:00",
            ),
        ),
    )
    monkeypatch.setattr(dag_module, "resolve_kma_base_datetime", lambda: ("20260808", "0800"))
    monkeypatch.setattr(
        dag_module,
        "load_kma_grids",
        lambda: [
            {
                "place_id": grids[0].place_id,
                "nx": grids[0].nx,
                "ny": grids[0].ny,
            }
        ],
    )
    monkeypatch.setattr(dag_module, "kma_num_of_rows", lambda: 500)
    monkeypatch.setattr(dag_module, "build_weather_landing", lambda: Landing())

    plan = dag_module.plan_weather_collection_slots(
        dag=Dag(), dag_run=DagRun(), run_id="scheduled__weather"
    )

    class TI:
        def xcom_pull(self, *, task_ids):
            if task_ids == "plan_weather_collection_slots":
                return plan
            return None

    result = dag_module.land_kma_raw(
        dag=Dag(), dag_run=DagRun(), run_id="scheduled__weather", ti=TI()
    )

    assert plan["base_date"] == "20260808"
    assert plan["base_time"] == "0800"
    assert len(plan["slots"]) == 1
    assert [slot.expected_slot_id for slot in expected_slots] == [
        weather_vilage_fcst_slots(
            "20260808",
            "0800",
            grids,
            recovery_boundary="2026-08-01T00:00:00+00:00",
        )[0].expected_slot_id
    ]
    assert result == {"raw_object_keys": ["raw/weather/page.json"]}
    assert captured["request"] == KmaLandingRequest(
        base_date="20260808",
        base_time="0800",
        grids=grids,
        num_of_rows=500,
    )


def test_weather_slot_outcomes_require_verified_manifest_and_keep_diagnostic_raw_unreplayable(
    monkeypatch,
):
    slots = weather_vilage_fcst_slots(
        "20260808",
        "0800",
        (KmaGrid("jongno", 60, 127),),
        recovery_boundary="2026-08-01T00:00:00+00:00",
    )
    plan = {
        "base_date": "20260808",
        "base_time": "0800",
        "slots": [slot.to_create_kwargs() for slot in slots],
    }
    outcomes = []

    class SlotReceipts:
        def record_expected(self, _slot):
            pytest.fail("plan has already declared the expected receipt")

        def record_outcome(self, outcome):
            outcomes.append(outcome)

    raw_result = {
        "manifest_key": "raw/weather/kma/_manifest.json",
        "raw_objects": [
            {
                "raw_object_key": "raw/weather/kma/page-1.json",
                "place_id": "jongno",
                "nx": 60,
                "ny": 127,
                "base_date": "20260808",
                "base_time": "0800",
                "row_count": 2,
            }
        ],
    }

    monkeypatch.setattr(
        dag_module,
        "build_weather_collection_slot_receipt_ports",
        lambda: (SlotReceipts(), lambda *_args: slots),
    )
    monkeypatch.setattr(dag_module, "weather_raw_manifest_is_verified", lambda *_args, **_kwargs: True)

    class SuccessTI:
        task_id = "record_weather_collection_slot_success"

        def xcom_pull(self, *, task_ids):
            if task_ids == "plan_weather_collection_slots":
                return plan
            if task_ids == "land_kma_raw":
                return raw_result
            if task_ids == "load_kma_bronze":
                return {"is_publishable": True}
            if task_ids == "verify_kma_bronze_runtime":
                return 2
            return None

    assert (
        dag_module.record_weather_collection_slot_success(
            dag=Dag(), run_id="scheduled__weather", ti=SuccessTI()
        )
        == 1
    )
    assert outcomes[0].collection_state == "observed"
    assert outcomes[0].recovery_class == "none"

    outcomes.clear()
    monkeypatch.setattr(dag_module, "weather_raw_manifest_is_verified", lambda *_args, **_kwargs: False)

    class FailureTI:
        task_id = "land_kma_raw"

        def xcom_pull(self, *, task_ids):
            if task_ids == "plan_weather_collection_slots":
                return plan
            if task_ids == "land_kma_raw":
                return raw_result
            return None

    dag_module.record_weather_collection_slot_failure(
        {"dag": Dag(), "run_id": "scheduled__weather", "ti": FailureTI()}
    )

    assert outcomes[0].collection_state == "collection_failed"
    assert outcomes[0].recovery_class == "historical_query"
    assert outcomes[0].raw_manifest_key is None
    assert outcomes[0].recovery_evidence_code == "weather_apihub_historical_range"
