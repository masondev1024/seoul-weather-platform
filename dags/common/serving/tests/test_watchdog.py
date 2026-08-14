from datetime import datetime, timezone

import pytest

from common.serving.contract import ServingContract
from common.serving.watchdog import (
    ServingWatchdogError,
    evaluate_watchdog,
    raise_for_watchdog_failures,
)


class _D1Evidence:
    def __init__(self, rows, availability_rows=()):
        self._rows = rows
        self._availability_rows = availability_rows
        self.sql = None

    def execute(self, sql):
        self.sql = sql
        return self._availability_rows if "d1_product_query_availability" in sql else self._rows


def _contract(*, slo=75, trigger=None):
    return ServingContract(
        product_id="transit_parking_full_risk",
        model_name="gold_transit_parking_full_risk",
        enabled=True,
        external=True,
        publication_mode="snapshot",
        zero_policy="retain_last_good",
        primary_key=("parking_id",),
        event_time="last_event_at",
        freshness_slo_minutes=slo,
        publication_trigger=trigger or {"schedule_cron": "*/15 * * * *"},
    )


def _risk_contract():
    return ServingContract(
        product_id="weather_place_risk_window", model_name="gold_weather_place_risk_window",
        enabled=True, external=True, publication_mode="snapshot", zero_policy="fail",
        primary_key=("product_row_id",), event_time="forecast_at", freshness_slo_minutes=75,
        publication_trigger={"max_interval_minutes": 60},
        query_availability_relation="gold_weather_place_risk_query_availability",
    )


def _risk_evidence_row():
    return _evidence_row(
        model_name="gold_weather_place_risk_window", product_id="weather_place_risk_window",
        catalog_freshness="2026-08-12T00:00:00+00:00", quality_freshness="2026-08-12T00:00:00+00:00",
        published_at="2026-08-12T00:30:00+00:00",
    )


def _availability_aggregate(publication_id="publication-1", **overrides):
    row = {
        "product_id": "weather_place_risk_window", "publication_id": publication_id,
        "row_count": 427, "distinct_place_count": 427, "null_place_count": 0,
        "incomplete_count": 0, "invalid_count": 0, "fingerprint_count": 1,
        "forecast_collected_at_min": "2026-08-12T00:00:00+00:00",
    }
    row.update(overrides)
    return row


def _evidence_row(**overrides):
    row = {
        "model_name": "gold_transit_parking_full_risk",
        "product_id": "transit_parking_full_risk",
        "catalog_publication_id": "publication-1",
        "published_at": "2026-08-06T12:55:00+00:00",
        "catalog_freshness": "2026-08-06 20:39:00",
        "catalog_serving_status": "published",
        "quality_publication_id": "publication-1",
        "quality_freshness": "2026-08-06 20:39:00",
        "quality_freshness_slo_minutes": 75,
        "quality_serving_status": "published",
        "quality_measured_at": "2026-08-06T12:55:00+00:00",
    }
    row.update(overrides)
    return row


def test_kst_wall_clock_freshness_is_measured_and_breaches_source_slo():
    d1 = _D1Evidence([_evidence_row()])

    report = evaluate_watchdog(
        d1,
        [_contract()],
        checked_at=datetime(2026, 8, 6, 13, 1, tzinfo=timezone.utc),
        naive_freshness_timezones={"transit_parking_full_risk": "Asia/Seoul"},
    )

    result = report.products[0]
    assert result.freshness_age_minutes == 82
    assert result.publication_age_minutes == 6
    assert result.issues == ("freshness_slo_breached",)
    assert "d1_product_quality" in d1.sql


def test_opted_in_risk_watchdog_requires_matching_complete_sidecar():
    report = evaluate_watchdog(
        _D1Evidence([_risk_evidence_row()], [_availability_aggregate()]), [_risk_contract()],
        checked_at=datetime(2026, 8, 12, 1, 0, tzinfo=timezone.utc),
    )
    assert report.products[0].healthy is True
    assert report.products[0].query_availability_row_count == 427


@pytest.mark.parametrize("rows,issue", [
    ([], "query_availability_missing"),
    ([_availability_aggregate("previous")], "query_availability_publication_mismatch"),
    ([_availability_aggregate(row_count=426)], "query_availability_row_count_mismatch"),
    ([_availability_aggregate(incomplete_count=1)], "query_availability_incomplete"),
])
def test_opted_in_risk_watchdog_reports_sidecar_issues(rows, issue):
    report = evaluate_watchdog(
        _D1Evidence([_risk_evidence_row()], rows), [_risk_contract()],
        checked_at=datetime(2026, 8, 12, 1, 0, tzinfo=timezone.utc),
    )
    assert issue in report.products[0].issues


def test_timezone_aware_freshness_passes_when_both_contract_clocks_are_current():
    d1 = _D1Evidence(
        [
            _evidence_row(
                published_at="2026-08-06T13:00:00Z",
                catalog_freshness="2026-08-06T12:58:00+00:00",
                quality_freshness="2026-08-06T12:58:00+00:00",
            )
        ]
    )

    report = evaluate_watchdog(
        d1,
        [_contract()],
        checked_at=datetime(2026, 8, 6, 13, 1, tzinfo=timezone.utc),
    )

    assert report.healthy is True
    assert report.products[0].freshness_age_minutes == 3
    assert report.products[0].publication_age_minutes == 1


def test_naive_source_timestamp_without_contract_timezone_fails_closed():
    report = evaluate_watchdog(
        _D1Evidence([_evidence_row()]),
        [_contract()],
        checked_at=datetime(2026, 8, 6, 13, 1, tzinfo=timezone.utc),
    )

    assert report.products[0].issues == (
        "freshness_timestamp_timezone_unknown",
        "catalog_freshness_timestamp_timezone_unknown",
    )


def test_malformed_freshness_is_not_silently_ignored():
    report = evaluate_watchdog(
        _D1Evidence([_evidence_row(catalog_freshness="bad", quality_freshness="bad")]),
        [_contract()],
        checked_at=datetime(2026, 8, 6, 13, 1, tzinfo=timezone.utc),
        naive_freshness_timezones={"transit_parking_full_risk": "Asia/Seoul"},
    )

    assert report.products[0].issues == (
        "freshness_timestamp_invalid",
        "catalog_freshness_timestamp_invalid",
    )


def test_published_status_does_not_hide_a_missed_publication_interval():
    report = evaluate_watchdog(
        _D1Evidence(
            [
                _evidence_row(
                    published_at="2026-08-06T13:00:00+00:00",
                    catalog_freshness="2026-08-06T13:30:00+00:00",
                    quality_freshness="2026-08-06T13:30:00+00:00",
                )
            ]
        ),
        [_contract()],
        checked_at=datetime(2026, 8, 6, 13, 31, tzinfo=timezone.utc),
    )

    assert report.products[0].issues == ("publication_interval_breached",)


def test_evidence_identity_and_slo_mismatch_are_reported_separately():
    report = evaluate_watchdog(
        _D1Evidence(
            [
                _evidence_row(
                    quality_publication_id="publication-previous",
                    quality_freshness_slo_minutes=90,
                    catalog_freshness="2026-08-06T12:58:00+00:00",
                    quality_freshness="2026-08-06T12:58:00+00:00",
                )
            ]
        ),
        [_contract()],
        checked_at=datetime(2026, 8, 6, 13, 1, tzinfo=timezone.utc),
    )

    assert report.products[0].issues == ("publication_id_mismatch", "freshness_slo_mismatch")


def test_failure_summary_carries_all_failed_product_reasons():
    report = evaluate_watchdog(
        _D1Evidence([]),
        [_contract()],
        checked_at=datetime(2026, 8, 6, 13, 1, tzinfo=timezone.utc),
    )

    with pytest.raises(ServingWatchdogError, match="transit_parking_full_risk:catalog_missing"):
        raise_for_watchdog_failures(report)
