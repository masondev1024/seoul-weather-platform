from __future__ import annotations

import io
import json
from datetime import datetime, timezone

from tools.weather_recovery_coordinator import CANDIDATE_SCHEMA_VERSION, build_plan, main


NOW = datetime(2026, 8, 26, 6, 0, tzinfo=timezone.utc)


def _payload(**overrides: object) -> dict[str, object]:
    candidate: dict[str, object] = {
        "domain": "weather",
        "source_id": "kma_vilage_fcst",
        "slot_key": "2026-08-26T04:20:00+00:00",
        "slot_ids": ["slot-1", "slot-2"],
        "scheduled_at": "2026-08-26T04:20:00+00:00",
        "deadline_at": "2026-08-26T05:20:00+00:00",
        "recovery_boundary": "2026-08-19T00:00:00+00:00",
        "expected_count": 2,
        "covered_count": 2,
        "raw_manifest_verified": True,
        "historical_query_allowed": False,
    }
    candidate.update(overrides)
    return {"schema_version": CANDIDATE_SCHEMA_VERSION, "candidates": [candidate]}


def test_build_plan_accepts_sanitized_candidates_without_io() -> None:
    plan = build_plan(_payload(), now=NOW)

    assert plan.status == "ready"
    assert len(plan.jobs) == 1
    assert plan.jobs[0].action.value == "raw_replay"
    assert plan.to_dict()["mutation_performed"] is False


def test_cli_emits_json_and_does_not_echo_invalid_input(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps(_payload(raw_manifest_verified=False, historical_query_allowed=True))),
    )

    assert main(["--now", NOW.isoformat()]) == 0

    output = json.loads(capsys.readouterr().out)
    assert output["schema_version"] == "weather-recovery-plan/v1"
    assert output["mutation_performed"] is False
    assert output["jobs"][0]["action"] == "recollect"


def test_cli_rejects_malformed_candidate_without_leaking_values(monkeypatch, capsys) -> None:
    secret_like = "private-placeholder-value"
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps({"candidates": [{"slot_key": secret_like}]})),
    )

    assert main([]) == 2

    captured = capsys.readouterr()
    assert "invalid_input" in captured.err
    assert secret_like not in captured.err
    assert secret_like not in captured.out
