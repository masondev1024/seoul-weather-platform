from __future__ import annotations

from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "deploy-main.yml"
CHECKOUT_USE = "actions/checkout@11d5960a326750d5838078e36cf38b85af677262"
SETUP_PYTHON_USE = (
    "actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065"
)
VERIFY_COMMAND = (
    "python -m deployment.main_cli verify-main "
    '--event-path "$env:GITHUB_EVENT_PATH" '
    '--workflow-ref "$env:GITHUB_WORKFLOW_REF" '
    '--workflow-sha "$env:GITHUB_WORKFLOW_SHA"'
)
DEPLOY_COMMAND = VERIFY_COMMAND.replace("verify-main", "deploy-main", 1)
EXACT_GUARD = (
    "vars.WEATHER_GOVERNANCE_MODE == 'protected' && "
    "vars.WEATHER_DEPLOYMENT_ENABLED == 'enabled' && "
    "github.event_name == 'workflow_run' && "
    "github.event.action == 'completed' && "
    "github.event.workflow_run.name == 'CI' && "
    "github.event.workflow_run.path == '.github/workflows/ci.yml' && "
    "github.event.workflow_run.event == 'push' && "
    "github.event.workflow_run.head_branch == 'main' && "
    "github.event.workflow_run.status == 'completed' && "
    "github.event.workflow_run.conclusion == 'success' && "
    "github.workflow_sha == github.event.workflow_run.head_sha"
)


def _workflow() -> dict[str, object]:
    loaded = yaml.load(WORKFLOW_PATH.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    assert isinstance(loaded, dict)
    return loaded


def _normalized_expression(value: object) -> str:
    assert isinstance(value, str)
    expression = " ".join(value.split())
    if expression.startswith("${{") and expression.endswith("}}"):
        expression = expression[3:-2].strip()
    return expression


def _steps(job: object) -> list[dict[str, object]]:
    assert isinstance(job, dict)
    steps = job.get("steps")
    assert isinstance(steps, list)
    assert all(isinstance(step, dict) for step in steps)
    return steps


def test_deploy_main_has_only_exact_main_ci_workflow_run_trigger() -> None:
    """Adding a manual, Release, PR, or non-main trigger could bypass promotion."""
    workflow = _workflow()

    assert workflow["name"] == "Deploy Main"
    assert workflow["on"] == {
        "workflow_run": {
            "workflows": ["CI"],
            "types": ["completed"],
            "branches": ["main"],
        }
    }
    assert workflow["permissions"] == {
        "actions": "read",
        "checks": "read",
        "contents": "read",
    }
    assert workflow["concurrency"] == {
        "group": "weather-main-deploy",
        "cancel-in-progress": "false",
    }
    assert set(workflow) == {
        "name",
        "on",
        "permissions",
        "concurrency",
        "jobs",
    }


def test_both_jobs_require_the_exact_protected_enabled_source_identity() -> None:
    """Weakening either guard could schedule code for an untrusted or stale source."""
    workflow = _workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    assert set(jobs) == {"verify-main", "deploy-main"}

    verify = jobs["verify-main"]
    deploy = jobs["deploy-main"]
    assert isinstance(verify, dict)
    assert isinstance(deploy, dict)
    assert verify["name"] == "verify-main"
    assert deploy["name"] == "deploy-main"
    assert _normalized_expression(verify["if"]) == EXACT_GUARD
    assert _normalized_expression(deploy["if"]) == EXACT_GUARD


def test_self_hosted_deploy_is_ordered_after_hosted_preflight() -> None:
    """Removing the dependency would reserve the production runner before preflight."""
    workflow = _workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    verify = jobs["verify-main"]
    deploy = jobs["deploy-main"]
    assert isinstance(verify, dict)
    assert isinstance(deploy, dict)

    assert verify["runs-on"] == "ubuntu-latest"
    assert "needs" not in verify
    assert deploy["runs-on"] == ["self-hosted", "windows", "weather-prod"]
    assert deploy["needs"] == "verify-main"
    assert deploy["timeout-minutes"] == "60"
    assert set(verify) == {"name", "if", "runs-on", "steps"}
    assert set(deploy) == {
        "name",
        "needs",
        "if",
        "runs-on",
        "timeout-minutes",
        "steps",
    }


def test_jobs_use_only_pre_gate_safe_steps_and_exact_cli_entrypoints() -> None:
    """No package code may execute before hosted verification or on the runner."""
    workflow = _workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    verify_steps = _steps(jobs["verify-main"])
    deploy_steps = _steps(jobs["deploy-main"])
    assert len(verify_steps) == 3
    assert len(deploy_steps) == 2

    verify_checkout, setup_python, verify = verify_steps
    deploy_checkout, deploy = deploy_steps
    for checkout in (verify_checkout, deploy_checkout):
        assert checkout.get("uses") == CHECKOUT_USE
        assert checkout.get("with") == {
            "ref": "${{ github.workflow_sha }}",
            "persist-credentials": "false",
        }
        assert set(checkout) == {"name", "uses", "with"}

    assert setup_python.get("uses") == SETUP_PYTHON_USE
    assert setup_python.get("with") == {"python-version": "3.11.15"}
    assert set(setup_python) == {"name", "uses", "with"}

    expected_env = {
        "GH_TOKEN": "${{ secrets.WEATHER_GOVERNANCE_READ_TOKEN }}",
        "GOVERNANCE_MODE": "${{ vars.WEATHER_GOVERNANCE_MODE }}",
        "DEPLOYMENT_ENABLED": "${{ vars.WEATHER_DEPLOYMENT_ENABLED }}",
    }
    for invoke, command in ((verify, VERIFY_COMMAND), (deploy, DEPLOY_COMMAND)):
        assert invoke.get("run") == command
        assert invoke.get("shell") == "pwsh"
        assert invoke.get("env") == expected_env
        assert set(invoke) == {"name", "run", "shell", "env"}

    serialized = WORKFLOW_PATH.read_text(encoding="utf-8").lower()
    assert "pip install" not in serialized
    assert "setup-python" not in str(deploy_steps).lower()
