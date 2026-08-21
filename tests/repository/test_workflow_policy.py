from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from textwrap import dedent

import pytest
import yaml

from tools.workflow_policy import audit_workflows


REPO_ROOT = Path(__file__).resolve().parents[2]
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
DEPLOY_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "deploy-main.yml"
CODEOWNERS = REPO_ROOT / ".github" / "CODEOWNERS"
PINNED_SHA = "11d5960a326750d5838078e36cf38b85af677262"
SETUP_PYTHON_SHA = "a26af69be951a213d495a4c3e4e4022e16d87065"
CHECKOUT_USE = f"actions/checkout@{PINNED_SHA}"
SETUP_PYTHON_USE = f"actions/setup-python@{SETUP_PYTHON_SHA}"
DEV_DEPENDENCY_INSTALL_COMMAND = (
    "python -m pip install jsonschema==4.26.0 PyYAML==6.0.2 pytest==9.0.3"
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


PUBLIC_CI = f"""\
name: CI
on:
  pull_request:
    branches: [dev, main]
  push:
    branches: [dev, main]
permissions:
  actions: read
  contents: read
  pull-requests: read
concurrency:
  group: ci-${{{{ github.event.pull_request.number || github.sha }}}}
  cancel-in-progress: true
jobs:
  repository-contract:
    name: Repository Contract
    runs-on: ubuntu-latest
    steps:
      - uses: {CHECKOUT_USE}
        with:
          persist-credentials: false
      - uses: {SETUP_PYTHON_USE}
        with:
          python-version: 3.11.15
      - run: {DEV_DEPENDENCY_INSTALL_COMMAND}
      - run: pwsh -File tools/verify_repository.ps1
  dbt-weather:
    name: dbt-weather
    runs-on: ubuntu-latest
    steps:
      - run: echo dbt-weather
  airflow-tests:
    name: airflow-tests
    runs-on: ubuntu-latest
    steps:
      - run: echo airflow-tests
  dagbag-policy:
    name: dagbag-policy
    runs-on: ubuntu-latest
    steps:
      - run: echo dagbag-policy
  promotion-source:
    name: Promotion Source / required
    runs-on: ubuntu-latest
    steps:
      - name: Validate main push promotion source
        run: |
          gh api "repos/${{GITHUB_REPOSITORY}}/commits/${{GITHUB_SHA}}/pulls" \
            --jq '[.[] | {{base: {{sha: .base.sha}}}}]' > "$associated_prs_path"
          python -m tools.promotion_source main-push --event-path "$GITHUB_EVENT_PATH" --associated-prs-path "$associated_prs_path" --repository "$GITHUB_REPOSITORY" --sha "$GITHUB_SHA"
  governance-mode:
    name: governance-mode
    runs-on: ubuntu-latest
    steps:
      - run: |
          case "$GOVERNANCE_MODE" in
            "public") ;;
            *) exit 1 ;;
          esac
        env:
          GOVERNANCE_MODE: ${{{{ vars.WEATHER_GOVERNANCE_MODE }}}}
  required:
    name: CI / required
    needs:
      - repository-contract
      - dbt-weather
      - airflow-tests
      - dagbag-policy
      - promotion-source
      - governance-mode
    if: always()
    runs-on: ubuntu-latest
    steps:
      - run: |
          python -m tools.ci_required_gate \
            --event-name "$EVENT_NAME" \
            --git-ref "$GIT_REF" \
            --governance-mode "$GOVERNANCE_MODE" \
            --result "repository-contract=${{REPOSITORY_CONTRACT}}" \
            --result "dbt-weather=${{DBT_WEATHER}}" \
            --result "airflow-tests=${{AIRFLOW_TESTS}}" \
            --result "dagbag-policy=${{DAGBAG_POLICY}}" \
            --result "promotion-source=${{PROMOTION_SOURCE}}" \
            --result "governance-mode=${{GOVERNANCE_MODE_RESULT}}"
        env:
          EVENT_NAME: ${{{{ github.event_name }}}}
          GIT_REF: ${{{{ github.ref }}}}
          GOVERNANCE_MODE: ${{{{ vars.WEATHER_GOVERNANCE_MODE }}}}
          REPOSITORY_CONTRACT: ${{{{ needs.repository-contract.result }}}}
          DBT_WEATHER: ${{{{ needs.dbt-weather.result }}}}
          AIRFLOW_TESTS: ${{{{ needs.airflow-tests.result }}}}
          DAGBAG_POLICY: ${{{{ needs.dagbag-policy.result }}}}
          PROMOTION_SOURCE: ${{{{ needs.promotion-source.result }}}}
          GOVERNANCE_MODE_RESULT: ${{{{ needs.governance-mode.result }}}}
"""


DISABLED_DEPLOY = """\
name: Deploy Main
on: workflow_dispatch
permissions:
  contents: read
jobs:
  disabled:
    name: disabled
    if: ${{ false }}
    runs-on: ubuntu-latest
    steps:
      - name: Deployment disabled
        run: echo disabled
"""


def _write_repo(
    tmp_path: Path,
    workflows: dict[str, str] | str,
    *,
    codeowners: str | None = REQUIRED_CODEOWNERS,
) -> Path:
    if isinstance(workflows, str):
        workflows = {"ci.yml": workflows}
    workflow_root = tmp_path / ".github" / "workflows"
    workflow_root.mkdir(parents=True)
    for filename, workflow in workflows.items():
        (workflow_root / filename).write_text(dedent(workflow), encoding="utf-8")
    if codeowners is not None:
        (tmp_path / ".github" / "CODEOWNERS").write_text(codeowners, encoding="utf-8")
    return tmp_path


def _rules(repo_root: Path) -> list[str]:
    return [finding.rule for finding in audit_workflows(repo_root)]


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


def test_repository_public_workflows_and_codeowners_satisfy_policy() -> None:
    assert CI_WORKFLOW.is_file()
    assert DEPLOY_WORKFLOW.is_file()
    assert CODEOWNERS.is_file()
    assert audit_workflows(REPO_ROOT) == []


def test_repository_ci_trigger_permissions_and_hosted_jobs_match_public_contract() -> None:
    workflow = _repository_ci()

    assert workflow["name"] == "CI"
    assert workflow["on"] == {
        "pull_request": {"branches": ["dev", "main"]},
        "push": {"branches": ["dev", "main"]},
    }
    assert workflow["permissions"] == {
        "actions": "read",
        "contents": "read",
        "pull-requests": "read",
    }
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    assert set(jobs) == {
        "repository-contract",
        "dbt-weather",
        "airflow-tests",
        "dagbag-policy",
        "promotion-source",
        "governance-mode",
        "required",
    }
    assert all(
        isinstance(job, dict) and job.get("runs-on") == "ubuntu-latest"
        for job in jobs.values()
    )
    assert "dagbag-runtime" not in jobs


def test_repository_ci_governance_and_required_gate_are_public_only() -> None:
    workflow = _repository_ci()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)

    governance = jobs["governance-mode"]
    required = jobs["required"]
    assert isinstance(governance, dict)
    assert isinstance(required, dict)
    governance_commands = "\n".join(_run_commands(governance))
    required_commands = "\n".join(_run_commands(required))

    assert '"public"' in governance_commands
    assert "protected" not in governance_commands
    assert "guarded_private" not in governance_commands
    assert set(required["needs"]) == {
        "repository-contract",
        "dbt-weather",
        "airflow-tests",
        "dagbag-policy",
        "promotion-source",
        "governance-mode",
    }
    assert "dagbag-runtime" not in required_commands
    assert "WEATHER_DEPLOYMENT_ENABLED" not in required_commands
    assert '--governance-mode "$GOVERNANCE_MODE"' in required_commands


@pytest.mark.parametrize(
    ("workflow", "filename"),
    [(PUBLIC_CI, "ci.yml"), (DISABLED_DEPLOY, "deploy-main.yml")],
)
def test_public_workflow_fixtures_are_accepted(
    tmp_path: Path, workflow: str, filename: str
) -> None:
    repo_root = _write_repo(tmp_path, {filename: workflow})

    assert audit_workflows(repo_root) == []


def test_policy_rejects_self_hosted_runner(tmp_path: Path) -> None:
    repo_root = _write_repo(
        tmp_path,
        PUBLIC_CI.replace("runs-on: ubuntu-latest", "runs-on: self-hosted", 1),
    )

    assert "self_hosted_runner" in _rules(repo_root)


@pytest.mark.parametrize(
    "runner_config",
    [
        'runs-on: "${{ matrix.runner }}"',
        "runs-on: [ubuntu-latest, weather-prod]",
        "runs-on:\n      group: weather-prod\n      labels: ubuntu-latest",
    ],
)
def test_policy_rejects_dynamic_or_unproven_runner_bypass(
    tmp_path: Path, runner_config: str
) -> None:
    workflow = f"""\
name: Runner Bypass
on: pull_request
jobs:
  unsafe:
    {runner_config}
    steps:
      - run: echo unsafe
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert "self_hosted_runner" in _rules(repo_root)


def test_pull_request_target_is_forbidden_even_on_hosted_runner(tmp_path: Path) -> None:
    workflow = """\
name: PR Target
on: pull_request_target
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - run: echo unsafe
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert "pull_request_target" in _rules(repo_root)


@pytest.mark.parametrize(
    "step",
    [
        f"uses: actions/checkout@{PINNED_SHA[:-1]}",
        f"uses: actions/checkout@{PINNED_SHA.upper()}",
        "uses: actions/checkout@v4",
        "uses: ./.github/actions/local",
    ],
)
def test_policy_rejects_unpinned_or_local_action_bypass(
    tmp_path: Path, step: str
) -> None:
    workflow = f"""\
name: Action Bypass
on: push
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - {step}
"""
    repo_root = _write_repo(tmp_path, workflow)

    assert set(_rules(repo_root)) & {"unpinned_external_use", "local_action"}


@pytest.mark.parametrize(
    "deploy_workflow",
    [
        DISABLED_DEPLOY.replace("on: workflow_dispatch", "on:\n  workflow_run:"),
        DISABLED_DEPLOY.replace("if: ${{ false }}", "if: success()"),
        DISABLED_DEPLOY.replace("name: Deploy Main", "name: Production Release"),
        DISABLED_DEPLOY.replace("run: echo disabled", "run: python -m deployment.main_cli deploy-main"),
        DISABLED_DEPLOY.replace(
            "run: echo disabled",
            "run: echo ${{ vars.WEATHER_DEPLOYMENT_ENABLED }}",
        ),
        DISABLED_DEPLOY.replace("permissions:\n  contents: read", "permissions:\n  contents: write"),
        DISABLED_DEPLOY.replace("run: echo disabled", f"uses: actions/checkout@{PINNED_SHA}"),
        DISABLED_DEPLOY.replace("run: echo disabled", "run: echo ${{ secrets.TOKEN }}"),
        DISABLED_DEPLOY.replace("runs-on: ubuntu-latest", "environment: production\n    runs-on: ubuntu-latest"),
        DISABLED_DEPLOY.replace("runs-on: ubuntu-latest", "env:\n      TOKEN: value\n    runs-on: ubuntu-latest"),
    ],
)
def test_policy_rejects_enabled_or_renamed_deploy_shapes(
    tmp_path: Path, deploy_workflow: str
) -> None:
    repo_root = _write_repo(tmp_path, {"deploy-main.yml": deploy_workflow})

    assert "deploy_main_contract" in _rules(repo_root)


def test_required_check_name_must_be_unique_across_workflow_files(tmp_path: Path) -> None:
    repo_root = _write_repo(
        tmp_path,
        {
            "ci.yml": PUBLIC_CI,
            "duplicate.yml": """\
            name: Duplicate
            on: push
            jobs:
              duplicate:
                name: CI / required
                runs-on: ubuntu-latest
                steps:
                  - run: echo duplicate
            """,
        },
    )

    assert "required_check_name_unique" in _rules(repo_root)


def test_required_check_names_are_owned_only_by_ci_yml(tmp_path: Path) -> None:
    repo_root = _write_repo(tmp_path, {"renamed-ci.yml": PUBLIC_CI})

    assert "required_check_name_owner" in _rules(repo_root)


@pytest.mark.parametrize(
    "command",
    [
        "docker compose up airflow-api-server",
        "airflow dags trigger weather_collection",
        "dbt run --select weather",
        "wrangler d1 execute weather",
        'bash -lc "airflow dags clear weather_collection"',
        "echo $(docker compose run --rm dbt dbt parse --select weather)",
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
        "python -m pytest tests/repository",
        "python -m tools.repository_policy --repo-root .",
        "python tools/workflow_policy.py --repo-root .",
        "pwsh -File tools/verify_repository.ps1",
        "docker compose config --services",
    ],
)
def test_read_only_commands_are_not_runtime_mutations(
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


def test_missing_codeowners_fails_closed(tmp_path: Path) -> None:
    repo_root = _write_repo(tmp_path, PUBLIC_CI, codeowners=None)

    assert "codeowners_coverage" in _rules(repo_root)


def test_malformed_workflow_finding_does_not_expose_raw_contents(tmp_path: Path) -> None:
    repo_root = _write_repo(tmp_path, "name: [RAW_WORKFLOW_SECRET_MARKER\n")

    findings = audit_workflows(repo_root)

    assert [finding.rule for finding in findings] == ["workflow_parse_error"]
    assert "RAW_WORKFLOW_SECRET_MARKER" not in repr(findings)


def test_cli_is_read_only_and_returns_zero_for_clean_public_repository(
    tmp_path: Path,
) -> None:
    repo_root = _write_repo(
        tmp_path,
        {"ci.yml": PUBLIC_CI, "deploy-main.yml": DISABLED_DEPLOY},
    )
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


def test_cli_fails_closed_without_printing_raw_workflow_contents(tmp_path: Path) -> None:
    repo_root = _write_repo(tmp_path, "name: [RAW_WORKFLOW_SECRET_MARKER\n")

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
