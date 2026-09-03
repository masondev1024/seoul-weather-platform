from __future__ import annotations

from tools.forecast_quality_report import render_report


def test_report_contains_measured_metrics_and_evidence_boundary() -> None:
    report = render_report(
        [
            {
                "evaluation_date_kst": "2026-09-02",
                "forecast_horizon": "D-1",
                "expected_count": 80,
                "matched_count": 76,
                "temperature_mae": 1.25,
                "temperature_rmse": 1.80,
                "temperature_bias": -0.10,
                "precipitation_brier_score": 0.12,
                "precipitation_ece_10bin": 0.06,
                "pty_accuracy": 0.91,
                "evidence_state": "measured",
            }
        ]
    )

    assert "1.2500" in report
    assert "0.9500" in report
    assert "provisional" in report
    assert "measured" in report


def test_report_order_is_date_then_horizon() -> None:
    report = render_report(
        [
            {"evaluation_date_kst": "2026-09-02", "forecast_horizon": "D-2", "evidence_state": "insufficient"},
            {"evaluation_date_kst": "2026-09-01", "forecast_horizon": "D-1", "evidence_state": "measured"},
        ]
    )

    assert report.index("2026-09-01") < report.index("2026-09-02")
