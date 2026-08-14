"""KMA Bronze atomic PyIceberg write tests."""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import weather_ingest.bronze_pyiceberg as bronze_pyiceberg  # noqa: E402
from weather_bronze_test_support import RecordingTable  # noqa: E402
from weather_ingest.bronze import (  # noqa: E402
    append_kma_bronze_row_batches_pyiceberg,
)


def test_kma_pyiceberg_batches_delete_and_appends_in_one_transaction(monkeypatch):
    table = RecordingTable()
    monkeypatch.setattr(
        bronze_pyiceberg,
        "_kma_pyiceberg_delete_filter",
        lambda run_id: ("same-run", run_id),
    )
    monkeypatch.setattr(
        bronze_pyiceberg, "_arrow_table", lambda rows: [dict(row) for row in rows]
    )

    inserted = append_kma_bronze_row_batches_pyiceberg(
        schema="weather_traffic_bronze",
        dag_run_id="manual__pyiceberg",
        chunk_rows=2,
        table=table,
        row_batches=[
            {
                "metadata": {
                    "result_code": "00",
                    "result_msg": "NORMAL_SERVICE",
                    "total_count": 3,
                    "row_count": 3,
                },
                "rows": [
                    {
                        "baseDate": "20260701",
                        "baseTime": "0800",
                        "nx": "60",
                        "ny": "127",
                        "category": category,
                        "fcstDate": "20260701",
                        "fcstTime": "0900",
                        "fcstValue": "25",
                    }
                    for category in ("TMP", "REH", "WSD")
                ],
                "request_id": "request-page-1",
                "place_id": "seoul-test-grid",
                "base_date": "20260701",
                "base_time": "0800",
                "nx": 60,
                "ny": 127,
                "raw_object_key": "raw/weather/kma/request-1.json",
                "raw_hash": "abc",
                "http_status": 200,
                "collected_at": datetime(2026, 7, 1, 0, 20, tzinfo=timezone.utc),
                "page_no": 1,
                "num_of_rows": 1000,
            }
        ],
    )

    appends = [event for event in table.txn.events if event[0] == "append"]
    assert inserted == 3
    assert table.txn.commits == 1
    assert table.txn.events[1] == ("delete", ("same-run", "manual__pyiceberg"))
    assert len(appends) == 2
    assert len(appends[0][1]) == 2
    assert len(appends[1][1]) == 1
    assert all(record["page_no"] == 1 for record in appends[0][1] + appends[1][1])


def test_refresh_failure_is_observable_without_replacing_commit_failure(
    monkeypatch,
    caplog,
):
    class CommitConflict(RuntimeError):
        pass

    commit_failure = CommitConflict("original append failure")

    class FailingTable:
        def transaction(self):
            raise commit_failure

        def refresh(self):
            raise RuntimeError("refresh credential must not be logged")

    monkeypatch.setattr(bronze_pyiceberg, "CommitFailedException", CommitConflict)
    monkeypatch.setattr(
        bronze_pyiceberg, "_pyiceberg_table", lambda _schema: FailingTable()
    )
    monkeypatch.setattr(
        bronze_pyiceberg, "validate_kma_bronze_row_batch", lambda _batch: None
    )

    with pytest.raises(CommitConflict) as exc_info:
        append_kma_bronze_row_batches_pyiceberg(
            schema="weather_traffic_bronze",
            row_batches=[{}],
            dag_run_id="manual__refresh-failure",
            max_retry_attempts=2,
            retry_base_delay_seconds=0,
        )

    assert exc_info.value is commit_failure
    assert "operation=refresh_after_commit_conflict" in caplog.text
    assert "error_type=RuntimeError" in caplog.text
    assert "refresh credential must not be logged" not in caplog.text
