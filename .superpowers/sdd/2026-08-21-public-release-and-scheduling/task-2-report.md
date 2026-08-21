# Task 2 Report: Public-Safe GitHub Actions Boundary

## Result
- Replaced the protected/self-hosted CI runtime dependency with hosted-only public governance.
- Replaced `deploy-main.yml` with a `workflow_dispatch` strict-disabled no-op on `ubuntu-latest`.
- Updated workflow policy and contract tests to reject self-hosted runners, automatic deploy triggers, active/renamed deploy workflows, deployment commands, and `WEATHER_DEPLOYMENT_ENABLED` dependencies.
- Refreshed `provenance/source-files.jsonl`.

## Test Policy Rationale
- `tests/repository/test_workflow_policy.py` was reduced because most deleted cases asserted the old protected/private self-hosted runner and automatic deploy contract. Keeping those as accepted baselines would weaken the actual public workflow boundary.
- Public-policy tamper and bypass coverage was retained or replaced with focused tests for self-hosted, dynamic, and unproven runners; `pull_request_target`; unpinned and local actions; active, renamed, automatic, secret-reading, write-permission, environment, `uses`, deployment-command, and `WEATHER_DEPLOYMENT_ENABLED` deploy escapes; runtime mutation commands; required check ownership; malformed workflow sanitization; and CLI read-only behavior.
- The replacement tests validate behavior through `audit_workflows` and command execution instead of preserving obsolete source-shape assertions.

## RED Evidence
- `.venv/bin/python -m pytest tests/repository/test_ci_required_gate.py tests/repository/test_workflow_policy.py tests/deploy/test_deploy_main_workflow.py -q` failed before implementation with 14 failures.
- Expected RED failures included `unsupported_governance_mode` for public CI, checked-in `dagbag-runtime`, protected/guarded governance checks, and automatic `workflow_run` deploy behavior.

## GREEN Evidence
- `.venv/bin/python -m pytest tests/repository/test_ci_required_gate.py tests/repository/test_workflow_policy.py tests/deploy/test_deploy_main_workflow.py -q` -> `38 passed in 0.51s`.
- `.venv/bin/python -m pytest tests/repository/test_ci_required_gate.py tests/repository/test_workflow_policy.py tests/deploy/test_deploy_main_workflow.py tests/public_readiness/test_public_release_contract.py -q` -> `62 passed in 0.64s`.
- `.venv/bin/python -m tools.workflow_policy --repo-root .` -> `Workflow policy verified.`
- `.venv/bin/python -m tools.public_release_contract --repo-root .` -> exit 0 with no errors.
- `.venv/bin/python -m tools.refresh_provenance --repo-root . --check` -> `Provenance manifest is current.`
- `.venv/bin/python -m tools.verify_provenance --repo-root .` -> `Provenance manifest verified.`
- `git diff --check` -> exit 0.

## HEAD
- Pre-commit HEAD: `6374d0c542dc24632c129b66f101ee3b70a7297c`
- Task implementation commit before report HEAD note: `8ca946b`
