from __future__ import annotations

from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "deploy-main.yml"


def _workflow() -> dict[str, object]:
    loaded = yaml.load(WORKFLOW_PATH.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    assert isinstance(loaded, dict)
    return loaded


def test_deploy_main_is_manual_strict_disabled_noop() -> None:
    workflow = _workflow()

    assert workflow["name"] == "Deploy Main"
    assert workflow["on"] == "workflow_dispatch"
    assert workflow["permissions"] == {"contents": "read"}
    assert set(workflow) == {"name", "on", "permissions", "jobs"}

    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    assert set(jobs) == {"disabled"}
    disabled = jobs["disabled"]
    assert isinstance(disabled, dict)
    assert disabled == {
        "name": "disabled",
        "if": "${{ false }}",
        "runs-on": "ubuntu-latest",
        "steps": [
            {
                "name": "Deployment disabled",
                "run": "echo disabled",
            }
        ],
    }


def test_deploy_main_contains_no_runtime_authority_or_deployment_command() -> None:
    workflow = _workflow()
    text = WORKFLOW_PATH.read_text(encoding="utf-8")
    job_text = text.split("jobs:", 1)[1]

    assert workflow["on"] == "workflow_dispatch"
    assert "WEATHER_DEPLOYMENT_ENABLED" not in text
    assert "self-hosted" not in text
    assert "environment:" not in text
    assert "secrets." not in text
    assert "uses:" not in text
    assert "deployment.main_cli" not in text
    assert "deploy-main" not in job_text
