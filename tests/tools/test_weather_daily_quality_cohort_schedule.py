"""The observation is read-only and neither records early days nor polls forever."""
from datetime import datetime
from pathlib import Path

import pytest
import yaml

from tools.weather_coverage_schedule import artifact_name

ROLLOUT = datetime.fromisoformat("2026-11-18T23:10:00+09:00")


@pytest.mark.parametrize(("stamp", "suffix"), [
    ("2026-11-19T20:30:00+09:00", None),
    ("2026-11-20T09:16:59+09:00", None),
    ("2026-11-20T09:17:00+09:00", "1of7"),
    ("2026-11-20T21:43:00+09:00", "1of7"),
    ("2026-11-25T00:17:00+00:00", "6of7"),
    ("2026-11-26T00:17:00+00:00", "7of7"),
    ("2026-12-03T09:16:59+09:00", "7of7"),
    ("2026-12-03T09:17:00+09:00", None),
])
def test_only_mature_days_observed_with_bounded_retries(stamp, suffix):
    result = artifact_name(ROLLOUT, datetime.fromisoformat(stamp))
    expected = f"weather-coverage-20261118T141000000000Z-{suffix}" if suffix else None
    assert result == expected


def test_overnight_activation_advances_the_first_full_day():
    rollout = datetime.fromisoformat("2026-11-18T15:01:00+00:00")
    assert artifact_name(rollout, datetime.fromisoformat("2026-11-20T09:17:00+09:00")) is None


def test_bad_rollout_time_fails_before_any_storage_access():
    with pytest.raises(ValueError, match="invalid_rollout_time"):
        artifact_name(ROLLOUT.replace(tzinfo=None), ROLLOUT)
    with pytest.raises(ValueError, match="invalid_rollout_time"):
        artifact_name(ROLLOUT, ROLLOUT.replace(month=10))


def test_only_scheduled_main_reads_evidence_and_saves_immutable_artifacts():
    root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((root / ".github/workflows/weather-daily-quality.yml").read_text())
    job = workflow["jobs"]["coverage-observation"]
    assert all(term in job["if"] for term in ("'refs/heads/main'", "== 'schedule'", "vars.WEATHER_COVERAGE_ROLLOUT_AT != ''", "'masondev1024/seoul-weather-platform'"))
    assert "needs" not in job
    assert job["permissions"] == {"actions": "read", "contents": "read"}
    steps = {step["id"]: step for step in job["steps"] if "id" in step}
    assert steps["archive"]["if"] == "steps.plan.outputs.name != ''"
    assert steps["archive"]["shell"] == "bash"  # fail closed if the artifact API is unavailable
    assert steps["compare"]["if"] == "steps.archive.outputs.due == 'true'"
    assert "--baseline-file docs/operations/evidence/weather-coverage-baseline-20261111.json" in steps["compare"]["run"]
    assert 'secrets.WEATHER_QUALITY_R2_ACCESS_KEY_ID' in steps["compare"]["env"]["R2_ACCESS_KEY_ID"]
    artifact = next(step for step in job["steps"] if step.get("uses", "").startswith("actions/upload-artifact@"))
    assert artifact["with"]["retention-days"] == 90
    assert artifact["with"]["if-no-files-found"] == "error"
