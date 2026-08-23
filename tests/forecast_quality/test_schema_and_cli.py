from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from weather_quality.cli import main


ROOT = Path(__file__).resolve().parents[2]
CONTRACT_ROOT = ROOT / "contracts/weather-forecast-quality"
SCENARIO = CONTRACT_ROOT / "fixtures/reference-scenario-v1.json"
EVIDENCE = CONTRACT_ROOT / "fixtures/reference-evidence-v1.json"
GRID_CSV = ROOT / "dags/domains/weather/config/seoul_kma_grids.csv"


def _load(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_checked_in_fixtures_satisfy_versioned_json_schemas() -> None:
    scenario_schema = _load(CONTRACT_ROOT / "schema/scenario-v1.schema.json")
    evidence_schema = _load(CONTRACT_ROOT / "schema/evidence-v1.schema.json")
    forecast_schema = _load(CONTRACT_ROOT / "schema/forecast-vintage-v1.schema.json")
    truth_schema = _load(CONTRACT_ROOT / "schema/observation-truth-v1.schema.json")

    for schema in (scenario_schema, evidence_schema, forecast_schema, truth_schema):
        Draft202012Validator.check_schema(schema)
    format_checker = FormatChecker()
    Draft202012Validator(scenario_schema, format_checker=format_checker).validate(_load(SCENARIO))
    Draft202012Validator(evidence_schema, format_checker=format_checker).validate(_load(EVIDENCE))
    Draft202012Validator(forecast_schema, format_checker=format_checker).validate(
        _load(CONTRACT_ROOT / "fixtures/forecast-vintage-v1.json")
    )
    Draft202012Validator(truth_schema, format_checker=format_checker).validate(
        _load(CONTRACT_ROOT / "fixtures/observation-truth-v1.json")
    )


def test_cli_generates_and_checks_canonical_reference_evidence(tmp_path: Path) -> None:
    output = tmp_path / "evidence.json"
    args = [
        "--grid-csv",
        str(GRID_CSV),
        "--scenario",
        str(SCENARIO),
        "--output",
        str(output),
    ]

    assert main(args) == 0
    assert output.read_bytes() == EVIDENCE.read_bytes()
    assert main([*args, "--check"]) == 0

    output.write_text("{}\n", encoding="utf-8")
    assert main([*args, "--check"]) == 1
