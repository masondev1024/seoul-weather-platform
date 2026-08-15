from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

import pytest
import yaml

from tools.workflow_policy import audit_workflows


REPO_ROOT = Path(__file__).resolve().parents[2]
PINNED_SHA = "11d5960a326750d5838078e36cf38b85af677262"
SETUP_PYTHON_SHA = "a26af69be951a213d495a4c3e4e4022e16d87065"
CHECKOUT_USE = f"actions/checkout@{PINNED_SHA}"
SETUP_PYTHON_USE = f"actions/setup-python@{SETUP_PYTHON_SHA}"
DAGBAG_COMMAND = "python -m tools.dagbag_check --repo-root ."
LEGACY_PACKAGE_INSTALL_COMMAND = "python -m pip install -e .[dev]"
DEV_DEPENDENCY_INSTALL_COMMAND = (
    "python -m pip install jsonschema==4.26.0 PyYAML==6.0.2 pytest==9.0.3"
)
LEGACY_PREPARE_COMMAND = (
    "python -m deployment.prepare_cli "
    "--repository masondev1024/seoul-weather-platform "
    '--workflow-sha "$env:GITHUB_WORKFLOW_SHA" '
    '--target-path "$env:WEATHER_DEPLOY_TARGET_PATH" '
    '--event-path "$env:GITHUB_EVENT_PATH" '
    '--output-directory "$env:RUNNER_TEMP\\weather-release"'
)
LEGACY_RELEASE_DEPLOY_COMMAND = (
    "python -m deployment.cli deploy-release "
    '--event-path "$env:GITHUB_EVENT_PATH" '
    '--workflow-ref "$env:GITHUB_WORKFLOW_REF" '
    '--workflow-sha "$env:GITHUB_WORKFLOW_SHA"'
)
FORBIDDEN_MAIN_INSTALL_COMMAND = 'python -m pip install -e ".[dev]"'
VERIFY_MAIN_COMMAND = (
    "python -m deployment.main_cli verify-main "
    '--event-path "$env:GITHUB_EVENT_PATH" '
    '--workflow-ref "$env:GITHUB_WORKFLOW_REF" '
    '--workflow-sha "$env:GITHUB_WORKFLOW_SHA"'
)
DEPLOY_MAIN_COMMAND = VERIFY_MAIN_COMMAND.replace("verify-main", "deploy-main", 1)
PROTECTED_CHECKOUT_STEP = (
    f"      - uses: {CHECKOUT_USE}\n"
    "        with:\n"
    "          persist-credentials: false\n"
)
TRUSTED_CHECKOUT_STEP = (
    f"      - uses: {CHECKOUT_USE}\n"
    "        with:\n"
    "          ref: ${{ github.workflow_sha }}\n"
    "          persist-credentials: false\n"
)
SETUP_PYTHON_STEP = (
    f"      - uses: {SETUP_PYTHON_USE}\n"
    "        with:\n"
    "          python-version: '3.11.15'\n"
)
LEGACY_GITHUB_TOKEN_ENV = (
    "        env:\n          GITHUB_TOKEN: ${{ github.token }}\n"
)
GUARDED_MAIN_CLI_ENV = (
    "        env:\n"
    "          GH_TOKEN: ${{ github.token }}\n"
    "          GOVERNANCE_MODE: ${{ vars.WEATHER_GOVERNANCE_MODE }}\n"
    "          DEPLOYMENT_ENABLED: ${{ vars.WEATHER_DEPLOYMENT_ENABLED }}\n"
)
PROTECTED_MAIN_CLI_ENV = (
    "        env:\n"
    "          GH_TOKEN: ${{ secrets.WEATHER_GOVERNANCE_READ_TOKEN }}\n"
    "          GOVERNANCE_MODE: ${{ vars.WEATHER_GOVERNANCE_MODE }}\n"
    "          DEPLOYMENT_ENABLED: ${{ vars.WEATHER_DEPLOYMENT_ENABLED }}\n"
)
MAIN_CLI_ENV = PROTECTED_MAIN_CLI_ENV
TWO_MODE_GUARD = (
    "(vars.WEATHER_GOVERNANCE_MODE == 'protected' ||\n"
    "      vars.WEATHER_GOVERNANCE_MODE == 'guarded_private')"
)
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
CODEOWNERS = REPO_ROOT / ".github" / "CODEOWNERS"
GITHUB_BOOTSTRAP_DOC = REPO_ROOT / "docs" / "operations" / "github-bootstrap.md"
CI_BOOTSTRAP_PLAN = (
    REPO_ROOT / "docs" / "superpowers" / "plans" / "2026-08-14-ci-bootstrap.md"
)
REQUIRED_CODEOWNERS = """\
.github/workflows/** @maintainer
tools/** @maintainer
deployment/** @maintainer
runtime/** @maintainer
provenance/** @maintainer
docs/operations/** @maintainer
release/** @maintainer
"""


VALID_CI = f"""\
name: CI
on:
  pull_request:
    branches: [dev, main]
  push:
    branches: [dev, main]
jobs:
  dagbag-runtime:
    if: >-
      vars.WEATHER_GOVERNANCE_MODE == 'protected' &&
      github.event_name == 'push' &&
      (github.ref == 'refs/heads/dev' || github.ref == 'refs/heads/main')
    runs-on: [self-hosted, windows, weather-prod]
    steps:
      - uses: {CHECKOUT_USE}
        with:
          persist-credentials: false
      - run: {DAGBAG_COMMAND}
  promotion-source:
    name: Promotion Source / required
    runs-on: ubuntu-latest
    steps:
      - run: echo safe
  required:
    name: CI / required
    if: always()
    runs-on: ubuntu-latest
    steps:
      - run: echo safe
"""


LEGACY_DRAFT_WORKFLOW_RUN = f"""\
name: Prepare Release
on:
  workflow_run:
    workflows: [CI]
    types: [completed]
jobs:
  draft:
    if: >-
      vars.WEATHER_GOVERNANCE_MODE == 'protected' &&
      github.event_name == 'workflow_run' &&
      github.event.workflow_run.conclusion == 'success' &&
      github.event.workflow_run.event == 'push' &&
      github.event.workflow_run.head_branch == 'main' &&
      github.workflow_sha == github.event.workflow_run.head_sha
    runs-on: [self-hosted, windows, weather-prod]
    steps:
      - uses: {CHECKOUT_USE}
        with:
          ref: ${{{{ github.workflow_sha }}}}
          persist-credentials: false
      - uses: {SETUP_PYTHON_USE}
        with:
          python-version: '3.11.15'
      - run: {LEGACY_PACKAGE_INSTALL_COMMAND}
      - run: {LEGACY_PREPARE_COMMAND}
{LEGACY_GITHUB_TOKEN_ENV}"""


LEGACY_REPOSITORY_DISPATCH = f"""\
name: Refresh Release
on:
  repository_dispatch:
    types: [refresh-weather-release]
jobs:
  draft:
    if: >-
      vars.WEATHER_GOVERNANCE_MODE == 'protected' &&
      github.event_name == 'repository_dispatch' &&
      github.ref == 'refs/heads/main' &&
      github.workflow_sha == github.sha
    runs-on: [self-hosted, windows, weather-prod]
    steps:
      - uses: {CHECKOUT_USE}
        with:
          ref: ${{{{ github.workflow_sha }}}}
          persist-credentials: false
      - uses: {SETUP_PYTHON_USE}
        with:
          python-version: '3.11.15'
      - run: {LEGACY_PACKAGE_INSTALL_COMMAND}
      - run: {LEGACY_PREPARE_COMMAND}
{LEGACY_GITHUB_TOKEN_ENV}"""


LEGACY_PUBLISHED_RELEASE = f"""\
name: Deploy Prod
on:
  release:
    types: [published]
jobs:
  deploy:
    if: >-
      vars.WEATHER_GOVERNANCE_MODE == 'protected' &&
      github.event_name == 'release' &&
      github.event.action == 'published' &&
      github.event.release.draft == false &&
      github.event.release.prerelease == false
    runs-on: [self-hosted, windows, weather-prod]
    steps:
      - uses: {CHECKOUT_USE}
        with:
          ref: ${{{{ github.workflow_sha }}}}
          persist-credentials: false
      - uses: {SETUP_PYTHON_USE}
        with:
          python-version: '3.11.15'
      - run: {LEGACY_PACKAGE_INSTALL_COMMAND}
      - run: {LEGACY_RELEASE_DEPLOY_COMMAND}
{LEGACY_GITHUB_TOKEN_ENV}"""


VALID_DEPLOY_MAIN = f"""\
name: Deploy Main
on:
  workflow_run:
    workflows: [CI]
    types: [completed]
    branches: [main]
permissions:
  actions: read
  checks: read
  contents: read
  pull-requests: read
concurrency:
  group: weather-main-deploy
  cancel-in-progress: false
jobs:
  verify-main:
    name: verify-main
    if: >-
      (vars.WEATHER_GOVERNANCE_MODE == 'protected' ||
      vars.WEATHER_GOVERNANCE_MODE == 'guarded_private') &&
      vars.WEATHER_DEPLOYMENT_ENABLED == 'enabled' &&
      github.event_name == 'workflow_run' &&
      github.event.action == 'completed' &&
      github.event.workflow_run.name == 'CI' &&
      github.event.workflow_run.path == '.github/workflows/ci.yml' &&
      github.event.workflow_run.event == 'push' &&
      github.event.workflow_run.head_branch == 'main' &&
      github.event.workflow_run.status == 'completed' &&
      github.event.workflow_run.conclusion == 'success' &&
      github.workflow_sha == github.event.workflow_run.head_sha
    runs-on: ubuntu-latest
    steps:
      - uses: {CHECKOUT_USE}
        with:
          ref: ${{{{ github.workflow_sha }}}}
          persist-credentials: false
      - uses: {SETUP_PYTHON_USE}
        with:
          python-version: '3.11.15'
      - if: vars.WEATHER_GOVERNANCE_MODE == 'guarded_private'
        run: {VERIFY_MAIN_COMMAND}
        shell: pwsh
{GUARDED_MAIN_CLI_ENV}      - if: vars.WEATHER_GOVERNANCE_MODE == 'protected'
        run: {VERIFY_MAIN_COMMAND}
        shell: pwsh
{MAIN_CLI_ENV}  deploy-main:
    name: deploy-main
    needs: verify-main
    if: >-
      (vars.WEATHER_GOVERNANCE_MODE == 'protected' ||
      vars.WEATHER_GOVERNANCE_MODE == 'guarded_private') &&
      vars.WEATHER_DEPLOYMENT_ENABLED == 'enabled' &&
      github.event_name == 'workflow_run' &&
      github.event.action == 'completed' &&
      github.event.workflow_run.name == 'CI' &&
      github.event.workflow_run.path == '.github/workflows/ci.yml' &&
      github.event.workflow_run.event == 'push' &&
      github.event.workflow_run.head_branch == 'main' &&
      github.event.workflow_run.status == 'completed' &&
      github.event.workflow_run.conclusion == 'success' &&
      github.workflow_sha == github.event.workflow_run.head_sha
    runs-on: [self-hosted, windows, weather-prod]
    timeout-minutes: 60
    steps:
      - uses: {CHECKOUT_USE}
        with:
          ref: ${{{{ github.workflow_sha }}}}
          persist-credentials: false
      - if: vars.WEATHER_GOVERNANCE_MODE == 'guarded_private'
        run: {DEPLOY_MAIN_COMMAND}
        shell: pwsh
{GUARDED_MAIN_CLI_ENV}      - if: vars.WEATHER_GOVERNANCE_MODE == 'protected'
        run: {DEPLOY_MAIN_COMMAND}
        shell: pwsh
{MAIN_CLI_ENV}"""


def _write_repo(
    tmp_path: Path,
    workflow: str,
    *,
    codeowners: str | None = REQUIRED_CODEOWNERS,
    filename: str = "ci.yml",
) -> Path:
    workflow_path = tmp_path / ".github" / "workflows" / filename
    workflow_path.parent.mkdir(parents=True)
    workflow_path.write_text(dedent(workflow), encoding="utf-8")
    if codeowners is not None:
        codeowners_path = tmp_path / ".github" / "CODEOWNERS"
        codeowners_path.write_text(codeowners, encoding="utf-8")
    return tmp_path


def _rules(repo_root: Path) -> list[str]:
    return [finding.rule for finding in audit_workflows(repo_root)]


def _replace_last(value: str, old: str, new: str) -> str:
    before, separator, after = value.rpartition(old)
    assert separator == old
    return before + new + after


def _simple_workflow(*, event: str = "push", body: str = "run: echo safe") -> str:
    return f"""\
name: Policy
on: {event}
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - {body}
"""


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "tools.workflow_policy", *args],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _repository_ci() -> dict[str, object]:
    loaded = yaml.load(CI_WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    assert isinstance(loaded, dict)
    return loaded


def _run_commands(job: object) -> list[str]:
    assert isinstance(job, dict)
    steps = job.get("steps")
    assert isinstance(steps, list)
    return [
        command.strip()
        for step in steps
        if isinstance(step, dict) and isinstance((command := step.get("run")), str)
    ]


def _normalized_expression(value: object) -> str:
    assert isinstance(value, str)
    return " ".join(value.split())


def test_repository_tracks_ci_workflow_and_codeowners() -> None:
    """Deleting either governance artifact would remove the enforced CI boundary."""
    assert CI_WORKFLOW.is_file()
    assert CODEOWNERS.is_file()
    entries = {
        fields[0]: fields[1:]
        for line in CODEOWNERS.read_text(encoding="utf-8").splitlines()
        if (fields := line.split())
    }
    assert entries == {
        pattern: ["@masondev1024"]
        for pattern in (
            ".github/workflows/**",
            "tools/**",
            "deployment/**",
            "runtime/**",
            "provenance/**",
            "docs/operations/**",
            "release/**",
        )
    }


def test_repository_ci_and_codeowners_satisfy_workflow_policy() -> None:
    """Weakening the checked-in CI boundary must make the root audit fail."""
    assert audit_workflows(REPO_ROOT) == []


def test_bootstrap_docs_require_guarded_mode_before_first_ci_and_main_push() -> None:
    set_guarded = (
        "gh variable set WEATHER_GOVERNANCE_MODE "
        "--repo masondev1024/seoul-weather-platform --body guarded_private"
    )
    get_mode = (
        "gh variable get WEATHER_GOVERNANCE_MODE "
        "--repo masondev1024/seoul-weather-platform"
    )
    main_push = 'git push origin "${bootstrapSha}:refs/heads/main"'
    verify = "python -m tools.github_protection verify"
    set_protected = (
        "gh variable set WEATHER_GOVERNANCE_MODE "
        "--repo masondev1024/seoul-weather-platform --body protected"
    )

    for path in (GITHUB_BOOTSTRAP_DOC, CI_BOOTSTRAP_PLAN):
        document = path.read_text(encoding="utf-8")
        assert "최초 CI PR을 열거나 재실행하기 전에" in document
        assert document.count(get_mode) >= 2
        assert document.index(set_guarded) < document.index(get_mode)
        assert document.rindex(get_mode, 0, document.index(main_push)) < document.index(
            main_push
        )
        assert document.index(main_push) < document.index(verify)
        assert document.index(verify) < document.rindex(set_protected)

def test_repository_ci_trigger_permissions_and_checks_match_contract() -> None:
    workflow = _repository_ci()

    assert workflow["name"] == "CI"
    assert workflow["on"] == {
        "pull_request": {"branches": ["dev", "main"]},
        "push": {"branches": ["dev", "main"]},
    }
    permissions = workflow["permissions"]
    assert isinstance(permissions, dict)
    assert permissions == {
        "actions": "read",
        "contents": "read",
        "pull-requests": "read",
    }
    assert workflow["concurrency"] == {
        "group": "ci-${{ github.event.pull_request.number || github.sha }}",
        "cancel-in-progress": "true",
    }

    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    assert set(jobs) == {
        "repository-contract",
        "dbt-weather",
        "airflow-tests",
        "dagbag-policy",
        "dagbag-runtime",
        "promotion-source",
        "governance-mode",
        "required",
    }
    assert {
        job_id: job.get("name") if isinstance(job, dict) else None
        for job_id, job in jobs.items()
    } == {
        "repository-contract": "Repository Contract",
        "dbt-weather": "dbt-weather",
        "airflow-tests": "airflow-tests",
        "dagbag-policy": "dagbag-policy",
        "dagbag-runtime": "dagbag-runtime",
        "promotion-source": "Promotion Source / required",
        "governance-mode": "governance-mode",
        "required": "CI / required",
    }


def test_repository_ci_uses_only_pinned_actions_and_hosted_pr_jobs() -> None:
    workflow = _repository_ci()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    python_jobs = {
        "repository-contract",
        "dbt-weather",
        "airflow-tests",
        "dagbag-policy",
        "promotion-source",
        "required",
    }

    for job_id, job in jobs.items():
        assert isinstance(job, dict)
        if job_id != "dagbag-runtime":
            assert job["runs-on"] == "ubuntu-latest"
        steps = job.get("steps")
        assert isinstance(steps, list)
        uses_values = {
            step.get("uses")
            for step in steps
            if isinstance(step, dict) and step.get("uses") is not None
        }
        if job_id in python_jobs:
            assert uses_values == {
                f"actions/checkout@{PINNED_SHA}",
                f"actions/setup-python@{SETUP_PYTHON_SHA}",
            }
        for step in steps:
            assert isinstance(step, dict)
            uses = step.get("uses")
            if uses is None:
                continue
            assert uses in {
                f"actions/checkout@{PINNED_SHA}",
                f"actions/setup-python@{SETUP_PYTHON_SHA}",
            }
            options = step.get("with")
            assert isinstance(options, dict)
            if uses.startswith("actions/checkout@"):
                assert options["persist-credentials"] == "false"
            else:
                assert options["python-version"] == "3.11.15"


def test_repository_ci_runs_the_exact_secretless_verification_commands() -> None:
    workflow = _repository_ci()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)

    expected_commands = {
        "repository-contract": [
            DEV_DEPENDENCY_INSTALL_COMMAND,
            "pwsh -File tools/verify_repository.ps1",
        ],
        "dbt-weather": [
            "python -m pip install -r runtime/requirements-dbt.lock.txt",
            "mkdir -p /tmp/weather-dbt-runtime/dbt_packages /tmp/weather-dbt-runtime/logs /tmp/weather-dbt-runtime/target\nchmod -R a-w dbt/domains/traffic_weather dbt/packages",
            "dbt deps --project-dir dbt/domains/traffic_weather --profiles-dir dbt/domains/traffic_weather",
            "git diff --exit-code -- dbt/domains/traffic_weather dbt/packages",
            "dbt parse --project-dir dbt/domains/traffic_weather --profiles-dir dbt/domains/traffic_weather --target ci --no-partial-parse",
            "python dbt/serving_contract/validate_serving_contract.py --source dbt/domains/traffic_weather/models/weather/transform/gold/gold_weather_place_current_outlook.yml dbt/domains/traffic_weather/models/weather/transform/gold/gold_weather_place_precipitation_window.yml dbt/domains/traffic_weather/models/weather/transform/gold/gold_weather_place_risk_window.yml dbt/domains/traffic_weather/models/weather/transform/gold/gold_weather_place_forecast_change_daily.yml --manifest /tmp/weather-dbt-runtime/target/manifest.json --format text",
            "python -m pytest dbt/serving_contract/tests dbt/domains/traffic_weather/tests/weather tests/contracts -q",
        ],
        "airflow-tests": [
            "python -m pip install -r runtime/requirements-airflow.lock.txt",
            "python -m compileall dags tools release",
            "python -m pytest dags/common/serving/tests dags/domains/weather/tests tests/repository/test_airflow_boundary.py -q",
        ],
        "dagbag-policy": [
            DEV_DEPENDENCY_INSTALL_COMMAND,
            "python -m pytest tests/repository/test_dagbag_harness.py tests/repository/test_scaffold_contract.py -q",
        ],
    }
    for job_id, commands in expected_commands.items():
        actual = _run_commands(jobs[job_id])
        for command in commands:
            assert command in actual
        assert LEGACY_PACKAGE_INSTALL_COMMAND not in actual


def test_repository_ci_dbt_job_proves_read_only_source_and_external_artifacts() -> None:
    workflow = _repository_ci()
    job = workflow["jobs"]["dbt-weather"]

    assert job["env"] == {
        "DBT_LOG_PATH": "/tmp/weather-dbt-runtime/logs",
        "DBT_PACKAGES_INSTALL_PATH": "/tmp/weather-dbt-runtime/dbt_packages",
        "DBT_TARGET_PATH": "/tmp/weather-dbt-runtime/target",
    }
    commands = _run_commands(job)
    prepare_index = commands.index(
        "mkdir -p /tmp/weather-dbt-runtime/dbt_packages /tmp/weather-dbt-runtime/logs /tmp/weather-dbt-runtime/target\n"
        "chmod -R a-w dbt/domains/traffic_weather dbt/packages"
    )
    deps_index = commands.index(
        "dbt deps --project-dir dbt/domains/traffic_weather --profiles-dir dbt/domains/traffic_weather"
    )
    immutable_index = commands.index(
        "git diff --exit-code -- dbt/domains/traffic_weather dbt/packages"
    )
    parse_index = commands.index(
        "dbt parse --project-dir dbt/domains/traffic_weather --profiles-dir dbt/domains/traffic_weather --target ci --no-partial-parse"
    )

    assert prepare_index < deps_index < immutable_index < parse_index
    assert all("dbt deps --upgrade" not in command for command in commands)


def test_repository_ci_runtime_is_exactly_protected_branch_push_only() -> None:
    workflow = _repository_ci()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    runtime = jobs["dagbag-runtime"]
    assert isinstance(runtime, dict)

    assert runtime["runs-on"] == ["self-hosted", "windows", "weather-prod"]
    assert _normalized_expression(runtime["if"]) == (
        "vars.WEATHER_GOVERNANCE_MODE == 'protected' && "
        "github.event_name == 'push' && "
        "(github.ref == 'refs/heads/dev' || github.ref == 'refs/heads/main')"
    )
    assert "python -m tools.dagbag_check --repo-root ." in _run_commands(runtime)


def test_repository_ci_promotion_uses_sanitized_read_only_evidence() -> None:
    workflow = _repository_ci()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    promotion = jobs["promotion-source"]
    assert isinstance(promotion, dict)
    steps = promotion["steps"]
    assert isinstance(steps, list)
    commands = "\n".join(_run_commands(promotion))

    assert (
        "python -m tools.promotion_source pull-request "
        '--event-path "$GITHUB_EVENT_PATH" --repository "$GITHUB_REPOSITORY"'
    ) in commands
    assert (
        "python -m tools.promotion_source main-push "
        '--associated-prs-path "$associated_prs_path" '
        '--repository "$GITHUB_REPOSITORY" --sha "$GITHUB_SHA"'
    ) in commands
    assert "gh api --method GET" in commands
    assert "repos/${GITHUB_REPOSITORY}/commits/${GITHUB_SHA}/pulls" in commands
    assert 'associated_prs_path="${RUNNER_TEMP}/associated-prs.json"' in commands
    assert (
        "--jq '[.[] | {base: {ref: .base.ref, repo: {full_name: .base.repo.full_name}}, head: {ref: .head.ref, repo: {full_name: .head.repo.full_name}}, merged_at, merge_commit_sha}]'"
        in commands
    )
    assert '> "$associated_prs_path"' in commands

    main_push = next(
        step
        for step in steps
        if isinstance(step, dict)
        and step.get("name") == "Validate main push promotion source"
    )
    assert _normalized_expression(main_push["if"]) == (
        "github.event_name == 'push' && github.ref == 'refs/heads/main' && "
        "github.event.created != true"
    )

    runtime_evidence_steps = [
        step
        for step in steps
        if isinstance(step, dict)
        and step.get("name") == "Require protected dev DagBag evidence"
    ]
    assert len(runtime_evidence_steps) == 1
    runtime_evidence = runtime_evidence_steps[0]
    assert _normalized_expression(runtime_evidence["if"]) == (
        "github.event_name == 'pull_request' && github.base_ref == 'main'"
    )
    runtime_command = runtime_evidence["run"]
    assert isinstance(runtime_command, str)
    runtime_command = runtime_command.replace(r"\"", '"')
    assert "check-runs" not in runtime_command
    assert (
        'workflow_runs_path="${RUNNER_TEMP}/dagbag-runtime-workflow-runs.json"'
        in runtime_command
    )
    assert (
        'runtime_evidence_path="${RUNNER_TEMP}/dagbag-runtime-check.json"'
        in runtime_command
    )
    assert "repos/${GITHUB_REPOSITORY}/actions/workflows/ci.yml/runs" in runtime_command
    assert "branch=dev" in runtime_command
    assert "event=push" in runtime_command
    assert 'head_sha="${DEV_HEAD_SHA}"' in runtime_command
    assert "status=success" in runtime_command
    assert '.name == "CI"' in runtime_command
    assert '.path == ".github/workflows/ci.yml"' in runtime_command
    assert '.path == ".github/workflows/ci.yml@dev"' not in runtime_command
    assert '.event == "push"' in runtime_command
    assert '.head_branch == "dev"' in runtime_command
    assert '.conclusion == "success"' in runtime_command
    assert '.head_sha == "${DEV_HEAD_SHA}"' in runtime_command
    assert "repos/${GITHUB_REPOSITORY}/actions/runs/${run_id}/jobs" in runtime_command
    assert ".run_id == ${run_id}" in runtime_command
    assert '.name == "dagbag-runtime"' in runtime_command
    assert "--jq" in runtime_command
    assert '> "$workflow_runs_path"' in runtime_command
    assert '> "$runtime_evidence_path"' in runtime_command


def test_repository_ci_initial_main_bootstrap_uses_exact_read_only_evidence() -> None:
    workflow = _repository_ci()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    promotion = jobs["promotion-source"]
    assert isinstance(promotion, dict)
    steps = promotion["steps"]
    assert isinstance(steps, list)
    bootstrap_steps = [
        step
        for step in steps
        if isinstance(step, dict)
        and step.get("name") == "Validate initial main bootstrap source"
    ]
    assert len(bootstrap_steps) == 1
    bootstrap = bootstrap_steps[0]

    assert _normalized_expression(bootstrap["if"]) == (
        "github.event_name == 'push' && github.ref == 'refs/heads/main' && "
        "github.event.created == true"
    )
    assert bootstrap["env"] == {
        "GH_TOKEN": "${{ github.token }}",
        "GOVERNANCE_MODE": "${{ vars.WEATHER_GOVERNANCE_MODE }}",
    }
    command = bootstrap["run"]
    assert isinstance(command, str)
    assert (
        'repository_readback_path="${RUNNER_TEMP}/bootstrap-repository.json"' in command
    )
    assert 'dev_readback_path="${RUNNER_TEMP}/bootstrap-dev.json"' in command
    assert 'main_readback_path="${RUNNER_TEMP}/bootstrap-main.json"' in command
    assert command.count("gh api --method GET") == 3
    assert '"repos/${GITHUB_REPOSITORY}"' in command
    assert '"repos/${GITHUB_REPOSITORY}/branches/dev"' in command
    assert '"repos/${GITHUB_REPOSITORY}/branches/main"' in command
    assert "--jq '{full_name, default_branch}'" in command
    assert command.count("--jq '{name, sha: .commit.sha}'") == 2
    assert (
        "python -m tools.promotion_source initial-main-bootstrap "
        '--event-path "$GITHUB_EVENT_PATH" '
        '--repository-readback-path "$repository_readback_path" '
        '--dev-branch-readback-path "$dev_readback_path" '
        '--main-branch-readback-path "$main_readback_path" '
        '--repository "$GITHUB_REPOSITORY" --sha "$GITHUB_SHA" '
        '--governance-mode "$GOVERNANCE_MODE"'
    ) in command
    for forbidden in (
        "--method POST",
        "--method PUT",
        "--method PATCH",
        "--method DELETE",
    ):
        assert forbidden not in command


def test_repository_ci_runtime_evidence_rejects_wrong_workflow_run_path() -> None:
    workflow = _repository_ci()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    promotion = jobs["promotion-source"]
    assert isinstance(promotion, dict)
    steps = promotion["steps"]
    assert isinstance(steps, list)
    runtime_step = next(
        step
        for step in steps
        if isinstance(step, dict)
        and step.get("name") == "Require protected dev DagBag evidence"
    )
    runtime_command = runtime_step["run"]
    assert isinstance(runtime_command, str)
    jq_line = next(
        line.strip()
        for line in runtime_command.splitlines()
        if ".workflow_runs[]" in line
    )
    assert jq_line.startswith('--jq "') and jq_line.endswith('" \\')
    jq_filter = jq_line[len('--jq "') : -len('" \\')]
    head_sha = "0123456789abcdef0123456789abcdef01234567"
    jq_filter = jq_filter.replace(r"\"", '"').replace("${DEV_HEAD_SHA}", head_sha)
    common = {
        "name": "CI",
        "event": "push",
        "head_branch": "dev",
        "head_sha": head_sha,
        "status": "completed",
        "conclusion": "success",
    }
    fixture = {
        "workflow_runs": [
            common | {"id": 101, "path": ".github/workflows/ci.yml"},
            common | {"id": 102, "path": ".github/workflows/ci.yml@dev"},
            common | {"id": 103, "path": ".github/workflows/other.yml"},
        ]
    }

    result = subprocess.run(
        ["jq", "-c", jq_filter],
        input=json.dumps(fixture),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == '[{"id":101}]\n'


def test_repository_ci_governance_and_required_gate_fail_closed() -> None:
    workflow = _repository_ci()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    governance = jobs["governance-mode"]
    required = jobs["required"]
    assert isinstance(governance, dict)
    assert isinstance(required, dict)

    governance_commands = "\n".join(_run_commands(governance))
    assert '"protected"|"guarded_private"' in governance_commands
    assert "exit 1" in governance_commands

    expected_needs = {
        "repository-contract",
        "dbt-weather",
        "airflow-tests",
        "dagbag-policy",
        "dagbag-runtime",
        "promotion-source",
        "governance-mode",
    }
    assert set(required["needs"]) == expected_needs
    assert _normalized_expression(required["if"]) == "always()"
    required_commands = "\n".join(_run_commands(required))
    assert "python -m tools.ci_required_gate" in required_commands
    assert '--event-name "$EVENT_NAME"' in required_commands
    assert '--git-ref "$GIT_REF"' in required_commands
    assert '--governance-mode "$GOVERNANCE_MODE"' in required_commands
    result_environments = {
        result_name: result_name.replace("-", "_").upper()
        for result_name in expected_needs
    } | {"governance-mode": "GOVERNANCE_MODE_RESULT"}
    for result_name, environment_name in result_environments.items():
        assert f'--result "{result_name}=${{{environment_name}}}"' in required_commands


def test_base_loader_preserves_on_and_accepts_protected_ci_runtime(
    tmp_path: Path,
) -> None:
    repo_root = _write_repo(tmp_path, VALID_CI)

    assert audit_workflows(repo_root) == []


def test_exact_deploy_main_workflow_is_the_only_approved_auto_deploy_route(
    tmp_path: Path,
) -> None:
    """Rejecting the canonical route would prevent safe automatic deployment."""
    repo_root = _write_repo(
        tmp_path,
        VALID_DEPLOY_MAIN,
        filename="deploy-main.yml",
    )

    assert audit_workflows(repo_root) == []


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("branches: [main]", "branches: [dev]"),
        ("types: [completed]", "types: [requested]"),
        (TWO_MODE_GUARD, "vars.WEATHER_GOVERNANCE_MODE == 'protected'"),
        ("vars.WEATHER_DEPLOYMENT_ENABLED == 'enabled' &&", ""),
        ("github.event.action == 'completed'", "github.event.action == 'requested'"),
        ("workflow_run.name == 'CI'", "workflow_run.name == 'Other'"),
        (
            "workflow_run.path == '.github/workflows/ci.yml'",
            "workflow_run.path == '.github/workflows/ci.yml@main'",
        ),
        ("workflow_run.event == 'push'", "workflow_run.event == 'pull_request'"),
        ("head_branch == 'main'", "head_branch == 'dev'"),
        ("workflow_run.status == 'completed'", "workflow_run.status == 'in_progress'"),
        ("conclusion == 'success'", "conclusion == 'failure'"),
        (
            "github.workflow_sha == github.event.workflow_run.head_sha",
            "github.workflow_sha == github.sha",
        ),
    ],
)
def test_deploy_main_rejects_every_weakened_source_boundary(
    tmp_path: Path, old: str, new: str
) -> None:
    """Each source field prevents an unrelated workflow run from scheduling deploy."""
    repo_root = _write_repo(
        tmp_path,
        VALID_DEPLOY_MAIN.replace(old, new),
        filename="deploy-main.yml",
    )

    assert "deploy_main_contract" in _rules(repo_root)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("    needs: verify-main\n", ""),
        ("    timeout-minutes: 60\n", ""),
        (
            "    runs-on: [self-hosted, windows, weather-prod]",
            "    runs-on: [self-hosted, windows]",
        ),
        (
            "          ref: ${{ github.workflow_sha }}",
            "          ref: ${{ github.event.workflow_run.head_sha }}",
        ),
        ("        shell: pwsh", "        shell: powershell"),
        (
            "      - if: vars.WEATHER_GOVERNANCE_MODE == 'guarded_private'\n",
            f"      - run: {FORBIDDEN_MAIN_INSTALL_COMMAND}\n"
            "        shell: pwsh\n"
            "      - if: vars.WEATHER_GOVERNANCE_MODE == 'guarded_private'\n",
        ),
        (DEPLOY_MAIN_COMMAND, "python -m deployment.main_cli deploy-main"),
        (
            "          GH_TOKEN: ${{ secrets.WEATHER_GOVERNANCE_READ_TOKEN }}",
            "          GH_TOKEN: ${{ github.token }}",
        ),
    ],
)
def test_deploy_main_rejects_execution_surface_drift(
    tmp_path: Path, old: str, new: str
) -> None:
    """Execution drift could bypass hosted preflight or widen production authority."""
    repo_root = _write_repo(
        tmp_path,
        VALID_DEPLOY_MAIN.replace(old, new, 1),
        filename="deploy-main.yml",
    )

    assert "deploy_main_contract" in _rules(repo_root)


@pytest.mark.parametrize("occurrence", ["first", "last"])
def test_deploy_main_requires_both_exact_mode_clauses(
    tmp_path: Path, occurrence: str
) -> None:
    """Removing either job's guarded clause must not preserve deploy authority."""
    workflow = (
        VALID_DEPLOY_MAIN.replace(
            TWO_MODE_GUARD,
            "vars.WEATHER_GOVERNANCE_MODE == 'protected'",
            1,
        )
        if occurrence == "first"
        else _replace_last(
            VALID_DEPLOY_MAIN,
            TWO_MODE_GUARD,
            "vars.WEATHER_GOVERNANCE_MODE == 'protected'",
        )
    )
    repo_root = _write_repo(tmp_path, workflow, filename="deploy-main.yml")

    assert "deploy_main_contract" in _rules(repo_root)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (
            "GH_TOKEN: ${{ github.token }}",
            "GH_TOKEN: ${{ secrets.WEATHER_GOVERNANCE_READ_TOKEN }}",
        ),
        (
            "GH_TOKEN: ${{ secrets.WEATHER_GOVERNANCE_READ_TOKEN }}",
            "GH_TOKEN: ${{ github.token }}",
        ),
        (
            "GH_TOKEN: ${{ secrets.WEATHER_GOVERNANCE_READ_TOKEN }}",
            "GH_TOKEN: ${{ secrets.WEATHER_GOVERNANCE_READ_TOKEN || github.token }}",
        ),
        (
            "GH_TOKEN: ${{ github.token }}",
            "GH_TOKEN: ${{ github.token || secrets.WEATHER_GOVERNANCE_READ_TOKEN }}",
        ),
        (
            "if: vars.WEATHER_GOVERNANCE_MODE == 'guarded_private'",
            "if: vars.WEATHER_GOVERNANCE_MODE == 'protected'",
        ),
        (
            "if: vars.WEATHER_GOVERNANCE_MODE == 'protected'",
            "if: vars.WEATHER_GOVERNANCE_MODE == 'guarded_private'",
        ),
    ],
)
@pytest.mark.parametrize("occurrence", ["first", "last"])
def test_deploy_main_rejects_swapped_or_fallback_mode_authority(
    tmp_path: Path, old: str, new: str, occurrence: str
) -> None:
    """Each hosted and self-hosted mode step owns one non-fallback token source."""
    workflow = (
        VALID_DEPLOY_MAIN.replace(old, new, 1)
        if occurrence == "first"
        else _replace_last(VALID_DEPLOY_MAIN, old, new)
    )
    repo_root = _write_repo(tmp_path, workflow, filename="deploy-main.yml")

    assert "deploy_main_contract" in _rules(repo_root)


@pytest.mark.parametrize("event", ["pull_request", "workflow_dispatch", "release"])
def test_deploy_main_rejects_any_additional_trigger(
    tmp_path: Path, event: str
) -> None:
    workflow = VALID_DEPLOY_MAIN.replace(
        "on:\n  workflow_run:\n",
        f"on:\n  {event}:\n  workflow_run:\n",
        1,
    )
    repo_root = _write_repo(tmp_path, workflow, filename="deploy-main.yml")

    assert "deploy_main_contract" in _rules(repo_root)


def test_deploy_main_requires_pull_request_read_for_guarded_identity(
    tmp_path: Path,
) -> None:
    workflow = VALID_DEPLOY_MAIN.replace("  pull-requests: read\n", "", 1)
    repo_root = _write_repo(tmp_path, workflow, filename="deploy-main.yml")

    assert "deploy_main_contract" in _rules(repo_root)


@pytest.mark.parametrize(
    "workflow",
    [
        VALID_DEPLOY_MAIN.replace(SETUP_PYTHON_STEP, "", 1),
        VALID_DEPLOY_MAIN.replace(
            SETUP_PYTHON_STEP,
            SETUP_PYTHON_STEP.replace("'3.11.15'", "'3.12.0'"),
            1,
        ),
        VALID_DEPLOY_MAIN.replace(
            SETUP_PYTHON_STEP,
            SETUP_PYTHON_STEP.replace(
                "          python-version: '3.11.15'\n",
                "          python-version: '3.11.15'\n          cache: pip\n",
            ),
            1,
        ),
        VALID_DEPLOY_MAIN.replace(
            TRUSTED_CHECKOUT_STEP + SETUP_PYTHON_STEP,
            SETUP_PYTHON_STEP + TRUSTED_CHECKOUT_STEP,
            1,
        ),
    ],
)
def test_hosted_verify_requires_exact_pinned_setup_python_after_checkout(
    tmp_path: Path, workflow: str
) -> None:
    repo_root = _write_repo(tmp_path, workflow, filename="deploy-main.yml")

    assert "deploy_main_contract" in _rules(repo_root)


def test_deploy_main_contract_is_owned_only_by_canonical_workflow_path(
    tmp_path: Path,
) -> None:
    """A copied deployment workflow must not become a second runner entrypoint."""
    repo_root = _write_repo(tmp_path, VALID_DEPLOY_MAIN, filename="copied.yml")

    assert "deploy_main_contract" in _rules(repo_root)


@pytest.mark.parametrize(
    "workflow",
    [
        LEGACY_DRAFT_WORKFLOW_RUN,
        LEGACY_REPOSITORY_DISPATCH,
        LEGACY_PUBLISHED_RELEASE,
    ],
)
def test_superseded_draft_dispatch_and_release_routes_are_forbidden(
    tmp_path: Path, workflow: str
) -> None:
    """Legacy manual deployment routes must not coexist with main-CI authorization."""
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_event_boundary" in _rules(repo_root)


def test_multi_trigger_push_subset_does_not_require_release_checkout(
    tmp_path: Path,
) -> None:
    workflow = VALID_CI.replace(
        "  push:\n    branches: [dev, main]\n",
        "  push:\n    branches: [dev, main]\n  release:\n    types: [published]\n",
    )
    repo_root = _write_repo(tmp_path, workflow)

    assert audit_workflows(repo_root) == []


def test_multi_trigger_guard_rejects_an_extra_untrusted_route(
    tmp_path: Path,
) -> None:
    workflow = VALID_CI.replace(
        "  push:\n    branches: [dev, main]\n",
        "  push:\n    branches: [dev, main]\n  release:\n    types: [published]\n",
    ).replace(
        "(github.ref == 'refs/heads/dev' || github.ref == 'refs/heads/main')",
        "(github.ref == 'refs/heads/dev' || github.ref == 'refs/heads/main') ||\n"
        "      (vars.WEATHER_GOVERNANCE_MODE == 'protected' &&\n"
        "      github.event_name == 'pull_request')",
    )
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_event_boundary" in _rules(repo_root)


def test_pull_request_target_is_forbidden_even_without_self_hosted_jobs(
    tmp_path: Path,
) -> None:
    repo_root = _write_repo(
        tmp_path,
        _simple_workflow(event="pull_request_target"),
    )

    assert "pull_request_target" in _rules(repo_root)


@pytest.mark.parametrize(
    "reference",
    [
        "v4",
        "11d5960a326750d5838078e36cf38b85af67726",
        "11D5960A326750D5838078E36CF38B85AF677262",
    ],
)
def test_external_uses_requires_exact_lowercase_40_hex_sha(
    tmp_path: Path, reference: str
) -> None:
    workflow = _simple_workflow(body=f"uses: actions/checkout@{reference}")
    repo_root = _write_repo(tmp_path, workflow)

    assert "unpinned_external_use" in _rules(repo_root)


def test_local_action_is_forbidden_even_without_external_sha_pin(
    tmp_path: Path,
) -> None:
    repo_root = _write_repo(
        tmp_path,
        _simple_workflow(body="uses: ./.github/actions/check-policy"),
    )

    assert "local_action" in _rules(repo_root)


def test_other_pinned_action_remains_allowed_on_github_hosted_runner(
    tmp_path: Path,
) -> None:
    repo_root = _write_repo(
        tmp_path,
        _simple_workflow(body=f"uses: actions/cache@{PINNED_SHA}"),
    )

    assert audit_workflows(repo_root) == []


@pytest.mark.parametrize(
    "unapproved_use",
    [
        f"actions/cache@{PINNED_SHA}",
        f"actions/checkout@{SETUP_PYTHON_SHA}",
        f"docker/setup-buildx-action@{PINNED_SHA}",
        "./.github/actions/check-policy",
    ],
)
def test_self_hosted_job_rejects_every_action_outside_exact_allowlist(
    tmp_path: Path, unapproved_use: str
) -> None:
    repo_root = _write_repo(
        tmp_path,
        VALID_CI.replace(CHECKOUT_USE, unapproved_use, 1),
    )

    assert "self_hosted_action_allowlist" in _rules(repo_root)


@pytest.mark.parametrize(
    "workflow",
    [
        VALID_CI.replace(
            "name: CI\n",
            "name: CI\nenv:\n  PATH: C:/attacker\n",
            1,
        ),
        VALID_DEPLOY_MAIN.replace(
            "    runs-on: [self-hosted, windows, weather-prod]\n",
            "    runs-on: [self-hosted, windows, weather-prod]\n"
            "    env:\n"
            "      PIP_FIND_LINKS: C:/attacker\n",
            1,
        ),
        VALID_DEPLOY_MAIN.replace(
            "name: Deploy Main\n",
            "name: Deploy Main\nenv:\n  PIP_CONSTRAINT: C:/attacker.txt\n",
            1,
        ),
        VALID_CI.replace(
            "name: CI\n",
            "name: CI\nenv:\n  LD_AUDIT: C:/attacker.dll\n",
            1,
        ),
        VALID_CI.replace(
            "name: CI\n",
            "name: CI\nenv:\n  DYLD_LIBRARY_PATH: C:/attacker\n",
            1,
        ),
        VALID_CI.replace(
            "name: CI\n",
            "name: CI\nenv:\n  NODE_EXTRA_CA_CERTS: C:/attacker.pem\n",
            1,
        ),
        VALID_CI.replace(
            "    runs-on: [self-hosted, windows, weather-prod]\n",
            "    runs-on: [self-hosted, windows, weather-prod]\n"
            "    env:\n"
            "      PYTHONPATH: C:/attacker\n",
            1,
        ),
        VALID_CI.replace(
            "on:\n",
            "defaults:\n  run:\n    shell: bash\non:\n",
            1,
        ),
        VALID_CI.replace(
            "    runs-on: [self-hosted, windows, weather-prod]\n",
            "    defaults:\n"
            "      run:\n"
            "        working-directory: C:/attacker\n"
            "    runs-on: [self-hosted, windows, weather-prod]\n",
            1,
        ),
        VALID_CI.replace(
            f"      - run: {DAGBAG_COMMAND}\n",
            f"      - run: {DAGBAG_COMMAND}\n        shell: pwsh\n",
            1,
        ),
        VALID_CI.replace(
            f"      - run: {DAGBAG_COMMAND}\n",
            f"      - run: {DAGBAG_COMMAND}\n        working-directory: C:/attacker\n",
            1,
        ),
        VALID_CI.replace(
            "    runs-on: [self-hosted, windows, weather-prod]\n",
            "    runs-on: [self-hosted, windows, weather-prod]\n"
            "    container: python:3.11\n",
            1,
        ),
        VALID_CI.replace(
            "    runs-on: [self-hosted, windows, weather-prod]\n",
            "    runs-on: [self-hosted, windows, weather-prod]\n"
            "    services:\n"
            "      attacker:\n"
            "        image: python:3.11\n",
            1,
        ),
    ],
)
def test_self_hosted_job_rejects_execution_context_overrides(
    tmp_path: Path, workflow: str
) -> None:
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_execution_context" in _rules(repo_root)


@pytest.mark.parametrize(
    "workflow",
    [
        VALID_CI.replace(
            f"      - run: {DAGBAG_COMMAND}\n",
            f"      - run: {DAGBAG_COMMAND}\n"
            "        env:\n"
            "          SAFE_METADATA: present\n",
            1,
        ),
        _replace_last(VALID_DEPLOY_MAIN, MAIN_CLI_ENV, ""),
        _replace_last(
            VALID_DEPLOY_MAIN,
            MAIN_CLI_ENV,
            MAIN_CLI_ENV + "          PYTHONPATH: C:/attacker\n",
        ),
        _replace_last(
            VALID_DEPLOY_MAIN,
            "GH_TOKEN: ${{ secrets.WEATHER_GOVERNANCE_READ_TOKEN }}",
            "GH_TOKEN: ${{ github.token }}",
        ),
    ],
)
def test_self_hosted_run_step_env_must_match_exact_route_contract(
    tmp_path: Path, workflow: str
) -> None:
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_execution_context" in _rules(repo_root)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (
            "          GOVERNANCE_MODE: ${{ vars.WEATHER_GOVERNANCE_MODE }}\n",
            "",
        ),
        (
            "          GOVERNANCE_MODE: ${{ vars.WEATHER_GOVERNANCE_MODE }}",
            "          GOVERNANCE_MODE: protected",
        ),
        (
            "          DEPLOYMENT_ENABLED: ${{ vars.WEATHER_DEPLOYMENT_ENABLED }}\n",
            "",
        ),
        (
            "          DEPLOYMENT_ENABLED: ${{ vars.WEATHER_DEPLOYMENT_ENABLED }}",
            "          DEPLOYMENT_ENABLED: enabled",
        ),
        (
            "          DEPLOYMENT_ENABLED: ${{ vars.WEATHER_DEPLOYMENT_ENABLED }}\n",
            "          DEPLOYMENT_ENABLED: ${{ vars.WEATHER_DEPLOYMENT_ENABLED }}\n"
            "          EXTRA_GATE: enabled\n",
        ),
    ],
)
@pytest.mark.parametrize("occurrence", ["first", "last"])
def test_deploy_main_cli_env_rejects_missing_wrong_or_extra_gate_values(
    tmp_path: Path, old: str, new: str, occurrence: str
) -> None:
    """Both identity checks must receive exact gate values independently of job if."""
    workflow = (
        VALID_DEPLOY_MAIN.replace(old, new, 1)
        if occurrence == "first"
        else _replace_last(VALID_DEPLOY_MAIN, old, new)
    )
    repo_root = _write_repo(tmp_path, workflow, filename="deploy-main.yml")

    assert "deploy_main_contract" in _rules(repo_root)


@pytest.mark.parametrize(
    "workflow",
    [
        VALID_CI.replace(
            PROTECTED_CHECKOUT_STEP,
            PROTECTED_CHECKOUT_STEP.replace(
                "persist-credentials: false", "persist-credentials: true"
            ),
            1,
        ),
        VALID_CI.replace(
            PROTECTED_CHECKOUT_STEP,
            f"      - uses: {CHECKOUT_USE}\n",
            1,
        ),
        VALID_CI.replace(
            PROTECTED_CHECKOUT_STEP,
            PROTECTED_CHECKOUT_STEP.replace(
                "          persist-credentials: false\n",
                "          repository: attacker/repository\n"
                "          persist-credentials: false\n",
            ),
            1,
        ),
        VALID_CI.replace(
            PROTECTED_CHECKOUT_STEP,
            PROTECTED_CHECKOUT_STEP.replace(
                "          persist-credentials: false\n",
                "          ref: ${{ github.sha }}\n"
                "          persist-credentials: false\n",
            ),
            1,
        ),
        VALID_CI.replace(
            PROTECTED_CHECKOUT_STEP,
            PROTECTED_CHECKOUT_STEP.replace(
                "          persist-credentials: false\n",
                "          path: alternate\n          persist-credentials: false\n",
            ),
            1,
        ),
        _replace_last(
            VALID_DEPLOY_MAIN,
            TRUSTED_CHECKOUT_STEP,
            TRUSTED_CHECKOUT_STEP.replace(
                "          ref: ${{ github.workflow_sha }}\n", ""
            ),
        ),
        _replace_last(
            VALID_DEPLOY_MAIN,
            TRUSTED_CHECKOUT_STEP,
            TRUSTED_CHECKOUT_STEP.replace(
                "          ref: ${{ github.workflow_sha }}\n",
                "          ref: ${{ github.ref }}\n",
            ),
        ),
        _replace_last(
            VALID_DEPLOY_MAIN,
            TRUSTED_CHECKOUT_STEP,
            TRUSTED_CHECKOUT_STEP + SETUP_PYTHON_STEP,
        ),
    ],
)
def test_self_hosted_actions_require_route_specific_inputs_and_order(
    tmp_path: Path, workflow: str
) -> None:
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_action_contract" in _rules(repo_root)


def test_self_hosted_action_input_order_is_deterministic(tmp_path: Path) -> None:
    workflow = _replace_last(
        VALID_DEPLOY_MAIN,
        TRUSTED_CHECKOUT_STEP,
        TRUSTED_CHECKOUT_STEP.replace(
            "          ref: ${{ github.workflow_sha }}\n"
            "          persist-credentials: false\n",
            "          persist-credentials: false\n"
            "          ref: ${{ github.workflow_sha }}\n",
        ),
    )
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_action_contract" in _rules(repo_root)


@pytest.mark.parametrize(
    "workflow",
    [
        VALID_CI.replace(DAGBAG_COMMAND, f"{DAGBAG_COMMAND} --verbose"),
        VALID_CI.replace(DAGBAG_COMMAND, "python -m pytest tests/repository"),
        VALID_CI.replace(
            f"      - run: {DAGBAG_COMMAND}\n",
            f"      - run: {DAGBAG_COMMAND}\n      - run: echo extra\n",
        ),
        _replace_last(
            VALID_DEPLOY_MAIN,
            DEPLOY_MAIN_COMMAND,
            DEPLOY_MAIN_COMMAND.replace("deploy-main", "verify-main", 1),
        ),
        _replace_last(
            VALID_DEPLOY_MAIN,
            "      - if: vars.WEATHER_GOVERNANCE_MODE == 'protected'\n",
            f"      - run: {FORBIDDEN_MAIN_INSTALL_COMMAND}\n"
            "        shell: pwsh\n"
            "      - if: vars.WEATHER_GOVERNANCE_MODE == 'protected'\n",
        ),
        _replace_last(
            VALID_DEPLOY_MAIN,
            DEPLOY_MAIN_COMMAND,
            "python -m deployment.main_cli deploy-main",
        ),
        _replace_last(
            VALID_DEPLOY_MAIN,
            DEPLOY_MAIN_COMMAND,
            "docker compose run deploy main",
        ),
        _replace_last(
            VALID_DEPLOY_MAIN,
            DEPLOY_MAIN_COMMAND,
            "airflow dags trigger weather_collection",
        ),
    ],
)
def test_self_hosted_route_rejects_any_non_exact_command_sequence(
    tmp_path: Path, workflow: str
) -> None:
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_command_allowlist" in _rules(repo_root)


@pytest.mark.parametrize(
    ("workflow", "filename"),
    [(VALID_CI, "ci.yml"), (VALID_DEPLOY_MAIN, "deploy-main.yml")],
)
def test_self_hosted_route_accepts_only_planned_action_and_command_contract(
    tmp_path: Path, workflow: str, filename: str
) -> None:
    repo_root = _write_repo(tmp_path, workflow, filename=filename)

    assert audit_workflows(repo_root) == []


@pytest.mark.parametrize(
    "required_name",
    ["CI / required", "Promotion Source / required"],
)
def test_required_check_name_must_be_unique_across_workflow_files(
    tmp_path: Path, required_name: str
) -> None:
    repo_root = _write_repo(tmp_path, VALID_CI)
    duplicate = repo_root / ".github" / "workflows" / "duplicate.yml"
    duplicate.write_text(
        dedent(
            f"""\
            name: Duplicate
            on: push
            jobs:
              duplicate:
                name: {required_name}
                runs-on: ubuntu-latest
                steps:
                  - run: echo duplicate
            """
        ),
        encoding="utf-8",
    )

    assert "required_check_name_unique" in _rules(repo_root)


def test_required_check_names_are_owned_only_by_ci_yml(tmp_path: Path) -> None:
    repo_root = _write_repo(tmp_path, VALID_CI, filename="renamed-ci.yml")

    assert "required_check_name_owner" in _rules(repo_root)


def test_ci_workflow_requires_both_globally_unique_required_check_names(
    tmp_path: Path,
) -> None:
    workflow = VALID_CI.replace(
        "    name: Promotion Source / required",
        "    name: Promotion Source",
    )
    repo_root = _write_repo(tmp_path, workflow)

    assert "required_check_name_unique" in _rules(repo_root)


@pytest.mark.parametrize("event", ["pull_request", "workflow_dispatch"])
def test_untrusted_event_cannot_reach_self_hosted_job(
    tmp_path: Path, event: str
) -> None:
    workflow = f"""\
name: Unsafe Runner
on: {event}
jobs:
  unsafe:
    runs-on: self-hosted
    steps:
      - run: echo unsafe
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_event_boundary" in _rules(repo_root)


@pytest.mark.parametrize(
    ("event", "runner_config"),
    [
        ("pull_request", 'runs-on: "${{ matrix.runner }}"'),
        ("workflow_dispatch", 'runs-on: "${{ vars.RUNNER }}"'),
        (
            "pull_request",
            'runs-on:\n      - ubuntu-latest\n      - "${{ matrix.runner }}"',
        ),
        (
            "workflow_dispatch",
            'runs-on:\n      labels: "${{ vars.RUNNER_LABELS }}"',
        ),
        (
            "pull_request",
            'runs-on:\n      labels: [ubuntu-latest, "${{ matrix.label }}"]',
        ),
        (
            "workflow_dispatch",
            "runs-on:\n"
            '      group: "${{ vars.RUNNER_GROUP }}"\n'
            "      labels: ubuntu-latest",
        ),
    ],
)
def test_dynamic_runs_on_is_treated_as_potentially_self_hosted(
    tmp_path: Path, event: str, runner_config: str
) -> None:
    workflow = f"""\
name: Dynamic Runner
on: {event}
jobs:
  unsafe:
    {runner_config}
    steps:
      - run: echo unsafe
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_event_boundary" in _rules(repo_root)


@pytest.mark.parametrize("event", ["pull_request", "workflow_dispatch"])
@pytest.mark.parametrize(
    "runner_config",
    [
        "runs-on: weather-prod",
        "runs-on: [windows, weather-prod]",
        "runs-on: [ubuntu-latest, x64]",
        "runs-on: []",
        "runs-on:\n      labels: ubuntu-latest",
        "runs-on:\n      labels: [ubuntu-latest]",
    ],
)
def test_unproven_literal_runner_requires_self_hosted_guard(
    tmp_path: Path, event: str, runner_config: str
) -> None:
    workflow = f"""\
name: Unproven Runner
on: {event}
jobs:
  unsafe:
    {runner_config}
    steps:
      - run: echo unsafe
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_event_boundary" in _rules(repo_root)


@pytest.mark.parametrize(
    "runner_config",
    ["runs-on: ubuntu-latest", "runs-on: [ubuntu-latest]"],
)
def test_proven_github_hosted_runner_remains_allowed_on_pull_request(
    tmp_path: Path, runner_config: str
) -> None:
    workflow = f"""\
name: Hosted Runner
on: pull_request
jobs:
  safe:
    {runner_config}
    steps:
      - run: echo safe
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert audit_workflows(repo_root) == []


@pytest.mark.parametrize(
    ("event", "labels"),
    [
        ("pull_request", "[self-hosted, windows, weather-prod]"),
        ("workflow_dispatch", "SELF-HOSTED"),
    ],
)
def test_untrusted_event_cannot_reach_mapping_self_hosted_labels(
    tmp_path: Path, event: str, labels: str
) -> None:
    workflow = f"""\
name: Unsafe Runner Mapping
on: {event}
jobs:
  unsafe:
    runs-on:
      group: weather-prod
      labels: {labels}
    steps:
      - run: echo unsafe
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_event_boundary" in _rules(repo_root)


def test_untrusted_event_cannot_reach_unproven_runner_group(
    tmp_path: Path,
) -> None:
    workflow = """\
name: Unsafe Runner Group
on: pull_request
jobs:
  unsafe:
    runs-on:
      group: weather-prod
    steps:
      - run: echo unsafe
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_event_boundary" in _rules(repo_root)


@pytest.mark.parametrize(
    "old,new",
    [
        ("vars.WEATHER_GOVERNANCE_MODE == 'protected' &&", ""),
        ("github.event_name == 'push'", "github.event_name == 'pull_request'"),
        ("github.ref == 'refs/heads/dev'", "github.ref_name == 'dev'"),
    ],
)
def test_ci_self_hosted_guard_fails_closed_when_a_boundary_is_weakened(
    tmp_path: Path, old: str, new: str
) -> None:
    repo_root = _write_repo(tmp_path, VALID_CI.replace(old, new))

    assert "self_hosted_event_boundary" in _rules(repo_root)


def test_ci_self_hosted_guard_accepts_an_exact_single_branch_subset(
    tmp_path: Path,
) -> None:
    workflow = VALID_CI.replace(" || github.ref == 'refs/heads/main'", "")
    repo_root = _write_repo(tmp_path, workflow)

    assert audit_workflows(repo_root) == []


def test_ci_self_hosted_route_remains_protected_only(tmp_path: Path) -> None:
    workflow = VALID_CI.replace(
        "vars.WEATHER_GOVERNANCE_MODE == 'protected'",
        "vars.WEATHER_GOVERNANCE_MODE == 'guarded_private'",
    )
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_event_boundary" in _rules(repo_root)


def test_ci_self_hosted_dagbag_step_contract_is_not_widened(tmp_path: Path) -> None:
    workflow = VALID_CI.replace(
        f"      - run: {DAGBAG_COMMAND}\n",
        "      - if: always()\n"
        f"        run: {DAGBAG_COMMAND}\n",
        1,
    )
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_execution_context" in _rules(repo_root)


@pytest.mark.parametrize(
    "workflow",
    [
        VALID_CI.replace(
            "(github.ref == 'refs/heads/dev' || github.ref == 'refs/heads/main')",
            "github.ref != 'refs/heads/feature/weather'",
        ),
        VALID_DEPLOY_MAIN.replace(
            "head_branch == 'main'",
            "head_branch != 'dev'",
        ),
    ],
)
def test_self_hosted_guard_requires_positive_exact_allowlist(
    tmp_path: Path, workflow: str
) -> None:
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_event_boundary" in _rules(repo_root)


@pytest.mark.parametrize(
    "unsupported_expression",
    [
        "unknown_function(github.actor)",
        "github['actor']",
        "contains(github.actor, 'maintainer')",
        "github.run_number > 0",
    ],
)
def test_self_hosted_guard_rejects_unsupported_expression_nodes(
    tmp_path: Path, unsupported_expression: str
) -> None:
    workflow = VALID_CI.replace(
        "(github.ref == 'refs/heads/dev' || github.ref == 'refs/heads/main')",
        "(github.ref == 'refs/heads/dev' || "
        "github.ref == 'refs/heads/main') && "
        f"{unsupported_expression}",
    )
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_event_boundary" in _rules(repo_root)


@pytest.mark.parametrize(
    "command",
    [
        "docker compose up airflow-api-server",
        "docker build -t weather .",
        "docker compose up --force-recreate scheduler",
        "docker compose build weather",
        "docker compose -f compose.yml down",
        "docker compose --profile prod stop airflow",
        "docker --context weather compose restart scheduler",
        "docker compose recreate scheduler",
        "airflow dags pause weather_collection",
        "airflow dags unpause weather_collection",
        "airflow dags trigger weather_collection",
        "airflow dags backfill weather_collection",
        "airflow --config airflow.cfg dags clear weather_collection",
        "airflow dags --output json retry weather_collection",
        "airflow dags mark-success weather_collection",
        "dbt run --select weather",
        "dbt build --select weather",
        "wrangler d1 execute weather",
        "docker compose -f compose.yml up airflow-api-server",
        "dbt --profiles-dir profiles run --select weather",
        "airflow --config airflow.cfg dags trigger weather_collection",
        "wrangler --config wrangler.toml d1 execute weather",
        'pwsh -Command "docker compose down"',
        'bash -lc "airflow dags clear weather_collection"',
        "sh -c 'dbt build --select weather'",
        'powershell -Command "wrangler d1 execute weather"',
        'cmd /c "docker compose restart scheduler"',
    ],
)
def test_runtime_mutation_commands_are_forbidden(tmp_path: Path, command: str) -> None:
    workflow = f"""\
name: Static Check
on: push
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - run: |
          echo before
          {command}
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert "runtime_mutation" in _rules(repo_root)


@pytest.mark.parametrize(
    "command",
    [
        "docker compose exec airflow-apiserver airflow dags list",
        "docker compose run --rm dbt dbt parse --select weather",
        "docker-compose exec airflow-apiserver python -m tools.dagbag_check --repo-root .",
        "docker-compose run --rm dbt dbt parse --select weather",
    ],
)
def test_compose_container_execution_is_unconditionally_mutating(
    tmp_path: Path, command: str
) -> None:
    """Compose exec/run can hide mutation behind an apparently read-only argv."""
    repo_root = _write_repo(
        tmp_path,
        _simple_workflow(body=f"run: {command}"),
    )

    assert "runtime_mutation" in _rules(repo_root)


@pytest.mark.parametrize(
    "command",
    [
        "echo $(docker compose run --rm dbt dbt parse --select weather)",
        'echo "$(docker compose exec airflow-apiserver airflow dags list)"',
        "echo `docker-compose run --rm dbt dbt parse --select weather`",
        "Write-Output $(docker-compose exec airflow-apiserver airflow dags list)",
    ],
)
def test_shell_command_substitution_cannot_hide_compose_container_execution(
    tmp_path: Path, command: str
) -> None:
    repo_root = _write_repo(
        tmp_path,
        _simple_workflow(body=f"run: {command}"),
    )

    assert "runtime_mutation" in _rules(repo_root)


@pytest.mark.parametrize(
    "command",
    [
        "sudo -E docker compose down",
        "sudo -u runner airflow dags trigger weather_collection",
        "exec -a name dbt run --select weather",
        "command -p wrangler d1 execute weather",
    ],
)
def test_wrapper_option_indirection_fails_closed(tmp_path: Path, command: str) -> None:
    workflow = f"""\
name: Static Check
on: push
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - run: |
          {command}
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert "runtime_mutation" in _rules(repo_root)


@pytest.mark.parametrize(
    "command",
    [
        "echo ok & docker compose down",
        "docker compose config & dbt run --select weather",
        "airflow dags list & wrangler d1 execute weather",
        "echo ok&docker compose down",
        "& docker compose down",
    ],
)
def test_single_ampersand_indirection_fails_closed(
    tmp_path: Path, command: str
) -> None:
    workflow = f"""\
name: Static Check
on: push
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - run: |
          {command}
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert "runtime_mutation" in _rules(repo_root)


def test_quoted_ampersand_argument_is_not_shell_indirection(
    tmp_path: Path,
) -> None:
    workflow = _simple_workflow(body='run: echo "weather & traffic"')
    repo_root = _write_repo(tmp_path, workflow)

    assert "runtime_mutation" not in _rules(repo_root)


@pytest.mark.parametrize(
    "command",
    [
        "dbt parse --select run",
        "dbt test --select build",
        "docker compose config --services",
        "docker compose ps --all",
        "airflow dags list --dag-id trigger",
        "airflow dags list-runs --state pause",
        "airflow dags list",
        "airflow dags show weather_collection",
        "wrangler d1 info weather",
        "dbt parse 2>&1",
        "docker compose config &>compose.txt",
    ],
)
def test_read_only_command_arguments_are_not_mutating_subcommands(
    tmp_path: Path, command: str
) -> None:
    workflow = f"""\
name: Static Check
on: push
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - run: {command}
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert "runtime_mutation" not in _rules(repo_root)


@pytest.mark.parametrize(
    "command",
    [
        '/usr/bin/env bash -lc "docker compose down"',
        'env -i bash -lc "airflow dags clear weather_collection"',
        'env WEATHER_ENV=prod sh -c "dbt build --select weather"',
        "python -c \"print('docker compose down')\"",
        "python3 -",
        "python <<'PY'",
        "python3.12 -c \"print('docker compose down')\"",
        "/usr/bin/python3.12 -",
        'env python3.12 -c "print(1)"',
        'PYTHON3.12 -c "print(1)"',
        "node -e \"console.log('airflow dags trigger weather_collection')\"",
        "node --eval=\"console.log('dbt run')\"",
        'npx --yes zx -e "docker compose restart scheduler"',
        'npm exec -- bash -lc "airflow dags trigger weather_collection"',
        'pnpm dlx zx -e "wrangler d1 execute weather"',
    ],
)
def test_dynamic_interpreter_and_package_runner_modes_fail_closed(
    tmp_path: Path, command: str
) -> None:
    workflow = f"""\
name: Static Check
on: push
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - run: {command}
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert "runtime_mutation" in _rules(repo_root)


def test_python_heredoc_mode_fails_closed(tmp_path: Path) -> None:
    workflow = """\
name: Static Check
on: push
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - run: |
          python - <<'PY'
          print("docker compose down")
          PY
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert "runtime_mutation" in _rules(repo_root)


@pytest.mark.parametrize(
    "command",
    [
        "python -m pytest tests/repository",
        "python -m tools.repository_policy --repo-root .",
        "python tools/workflow_policy.py --repo-root .",
        "/usr/bin/env python -m pytest tests/repository",
        "python -X dev -m pytest tests/repository",
        "python -W error tools/workflow_policy.py --repo-root .",
        "python3.12 -m pytest tests/repository",
    ],
)
def test_static_python_entrypoints_are_not_dynamic_indirection(
    tmp_path: Path, command: str
) -> None:
    workflow = f"""\
name: Static Check
on: push
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - run: {command}
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert "runtime_mutation" not in _rules(repo_root)


def test_exact_repository_verifier_pwsh_entrypoint_is_static(
    tmp_path: Path,
) -> None:
    repo_root = _write_repo(
        tmp_path,
        _simple_workflow(body="run: pwsh -File tools/verify_repository.ps1"),
    )

    assert "runtime_mutation" not in _rules(repo_root)


@pytest.mark.parametrize(
    "command",
    [
        "powershell -File tools/verify_repository.ps1",
        "pwsh -Command tools/verify_repository.ps1",
        "pwsh -File tools/other.ps1",
        "pwsh -File /tmp/tools/verify_repository.ps1",
        "pwsh -File ../tools/verify_repository.ps1",
        "pwsh -File $VERIFIER_SCRIPT",
        "pwsh -File ${{ env.VERIFIER_SCRIPT }}",
        "pwsh -File tools/verify_repository.ps1 extra",
        'pwsh -File tools/verify_repository.ps1 "payload"',
    ],
)
def test_repository_verifier_pwsh_exception_rejects_other_argv(
    tmp_path: Path, command: str
) -> None:
    repo_root = _write_repo(
        tmp_path,
        _simple_workflow(body=f"run: {command}"),
    )

    assert "runtime_mutation" in _rules(repo_root)


@pytest.mark.parametrize(
    "required_job",
    [
        """\
  required:
    name: CI / required
    if: success()
    runs-on: ubuntu-latest
    steps:
      - run: echo aggregate
""",
        """\
  aggregate:
    name: Other Aggregate
    if: always()
    runs-on: ubuntu-latest
    steps:
      - run: echo aggregate
""",
    ],
)
def test_ci_required_job_must_exist_with_if_always(
    tmp_path: Path, required_job: str
) -> None:
    workflow = f"""\
name: CI
on: pull_request
jobs:
{required_job}
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert "required_ci_always" in _rules(repo_root)


@pytest.mark.parametrize(
    "missing_pattern",
    [
        ".github/workflows/**",
        "tools/**",
        "deployment/**",
        "runtime/**",
        "provenance/**",
        "docs/operations/**",
        "release/**",
    ],
)
def test_codeowners_requires_each_protected_path(
    tmp_path: Path, missing_pattern: str
) -> None:
    codeowners = "\n".join(
        line
        for line in REQUIRED_CODEOWNERS.splitlines()
        if not line.startswith(f"{missing_pattern} ")
    )
    repo_root = _write_repo(
        tmp_path,
        _simple_workflow(),
        codeowners=codeowners,
    )

    findings = audit_workflows(repo_root)

    assert any(
        finding.rule == "codeowners_coverage" and missing_pattern in finding.summary
        for finding in findings
    )


def test_missing_codeowners_fails_closed(tmp_path: Path) -> None:
    repo_root = _write_repo(
        tmp_path,
        _simple_workflow(),
        codeowners=None,
    )

    assert "codeowners_coverage" in _rules(repo_root)


def test_malformed_workflow_finding_does_not_expose_raw_contents(
    tmp_path: Path,
) -> None:
    repo_root = _write_repo(
        tmp_path,
        "name: [RAW_WORKFLOW_SECRET_MARKER\n",
    )

    findings = audit_workflows(repo_root)

    assert [finding.rule for finding in findings] == ["workflow_parse_error"]
    assert "RAW_WORKFLOW_SECRET_MARKER" not in repr(findings)


def test_cli_is_read_only_and_returns_zero_for_clean_repository(
    tmp_path: Path,
) -> None:
    repo_root = _write_repo(tmp_path, VALID_CI)
    before = {
        path.relative_to(repo_root).as_posix(): path.read_bytes()
        for path in repo_root.rglob("*")
        if path.is_file()
    }

    result = _run_cli("--repo-root", str(repo_root))

    after = {
        path.relative_to(repo_root).as_posix(): path.read_bytes()
        for path in repo_root.rglob("*")
        if path.is_file()
    }
    assert result.returncode == 0
    assert result.stdout == "Workflow policy verified.\n"
    assert result.stderr == ""
    assert after == before


def test_cli_fails_closed_without_printing_raw_workflow_contents(
    tmp_path: Path,
) -> None:
    repo_root = _write_repo(
        tmp_path,
        "name: [RAW_WORKFLOW_SECRET_MARKER\n",
    )

    result = _run_cli("--repo-root", str(repo_root))

    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert "workflow_parse_error" in result.stdout
    assert result.stderr == ""
    assert "RAW_WORKFLOW_SECRET_MARKER" not in output


def test_cli_unknown_argument_does_not_echo_raw_value() -> None:
    result = _run_cli("--unknown", "RAW_ARGUMENT_MARKER")

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "invalid-input\n"
    assert "RAW_ARGUMENT_MARKER" not in result.stderr
