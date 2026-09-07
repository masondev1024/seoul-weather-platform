"""Render a deterministic, portfolio-friendly forecast quality report.

The input is an export of the Forecast-Quality Gold daily model.  The renderer does
not call a weather API or alter a serving product; it only turns measured Gold rows
into a reviewable artifact with explicit evidence boundaries.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Iterable


HORIZON_ORDER = {"D-1": 1, "D-2": 2, "D-3": 3}


def _number(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key)
    if value in (None, "", "null", "NULL"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_number(value: float | None, digits: int = 4) -> str:
    return "—" if value is None else f"{value:.{digits}f}"


def load_rows(path: Path) -> list[dict[str, Any]]:
    """Load CSV, JSON array, or JSONL without changing source values."""

    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".csv":
        return list(csv.DictReader(text.splitlines()))
    if path.suffix.lower() == ".jsonl":
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    payload = json.loads(text)
    if not isinstance(payload, list):
        raise ValueError("quality input JSON must be an array of rows")
    return [dict(row) for row in payload]


def _ordered(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("evaluation_date_kst", "")),
            HORIZON_ORDER.get(str(row.get("forecast_horizon", "")), 99),
            str(row.get("forecast_horizon", "")),
        ),
    )


def render_report(rows: Iterable[dict[str, Any]], title: str = "날씨 예보 품질 검증 리포트") -> str:
    ordered = _ordered(rows)
    if not ordered:
        raise ValueError("quality input contains no rows")

    lines = [f"# {title}", "", "Forecast-Quality Gold에서 산출한 값을 읽어 재현 가능한 문서로 만든 결과다.", ""]
    lines.extend(
        [
            "## 측정 범위",
            "",
            f"- 평가 행 수: **{len(ordered)}**",
            f"- 평가 날짜: `{ordered[0].get('evaluation_date_kst', '미기록')}` ~ `{ordered[-1].get('evaluation_date_kst', '미기록')}`",
            "- 예보 시점 기준을 고정한 재실행 결과만 비교하며, 관측값이 provisional이면 수치의 확정성을 낮춘다.",
            "",
            "## 날짜·예보 거리별 결과",
            "",
            "| 평가일 | 예보 거리 | 표본 | 일치 비율 | 기온 MAE | 기온 RMSE | 기온 편향 | 강수 Brier | 강수 ECE | PTY 정확도 | 증거 상태 |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in ordered:
        expected = _number(row, "expected_count")
        matched = _number(row, "matched_count")
        coverage = _number(row, "matched_coverage")
        if coverage is None and expected:
            coverage = (matched or 0.0) / expected
        lines.append(
            "| {date} | {horizon} | {sample} | {coverage} | {mae} | {rmse} | {bias} | {brier} | {ece} | {pty} | {state} |".format(
                date=row.get("evaluation_date_kst", "—"),
                horizon=row.get("forecast_horizon", "—"),
                sample=int(matched) if matched is not None and matched.is_integer() else _format_number(matched, 0),
                coverage=_format_number(coverage),
                mae=_format_number(_number(row, "temperature_mae")),
                rmse=_format_number(_number(row, "temperature_rmse")),
                bias=_format_number(_number(row, "temperature_bias")),
                brier=_format_number(_number(row, "precipitation_brier_score")),
                ece=_format_number(_number(row, "precipitation_ece_10bin")),
                pty=_format_number(_number(row, "pty_accuracy")),
                state=row.get("evidence_state", "미기록"),
            )
        )

    evidence_states = sorted({str(row.get("evidence_state", "미기록")) for row in ordered})
    lines.extend(
        [
            "",
            "## 해석 기준",
            "",
            "- `temperature_mae`·`temperature_rmse`는 기온 오차의 크기, `temperature_bias`는 과대·과소 예측 방향을 나타낸다.",
            "- `precipitation_brier_score`와 `precipitation_ece_10bin`은 강수 확률의 예측 품질과 보정 정도를 함께 본다.",
            "- `pty_accuracy`는 강수 형태 범주 일치율이다. 표본 수와 `matched_coverage`가 낮으면 수치를 성능 개선 근거로 사용하지 않는다.",
            f"- 이번 입력에서 확인된 증거 상태: `{', '.join(evidence_states)}`",
            "",
            "## 한계와 다음 검증",
            "",
            "- 이 파일은 Gold 결과를 시각적으로 정리한 리포트이며, 원천 관측값의 정확성 자체를 보증하지 않는다.",
            "- 관측 truth가 provisional/degraded인 날짜는 별도 표시하고, 확정 관측으로 다시 계산해 비교한다.",
            "- 다음 단계는 날짜·예보 거리·격자별 표본 수를 동일하게 맞춘 기준선 비교와 신뢰구간 계산이다.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render_report(load_rows(args.input)), encoding="utf-8")
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
