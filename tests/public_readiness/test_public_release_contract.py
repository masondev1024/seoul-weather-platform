from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
from pathlib import Path
from typing import Any

from tools.repository_policy import find_secret_candidates
from tools.public_release_contract import validate_public_release_contract


REPO_ROOT = Path(__file__).resolve().parents[2]
AUTHORIZED_LICENSE_STATUS = "public_republication_authorized"
APACHE_2_0_OFFICIAL_SHA256 = (
    "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30"
)
PLAN_TARGET = "docs/superpowers/plans/2026-08-21-public-release-and-scheduling.md"


def _validate_public_release_contract() -> list[str]:
    spec = importlib.util.find_spec("tools.public_release_contract")
    assert spec is not None, "tools.public_release_contract is missing"
    module = importlib.import_module("tools.public_release_contract")
    return module.validate_public_release_contract(REPO_ROOT)


def _manifest_records() -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in (REPO_ROOT / "provenance/source-files.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]


def _write_minimal_repo(
    root: Path,
    *,
    workflow_text: str,
    env_overrides: dict[str, str] | None = None,
) -> None:
    (root / ".github/workflows").mkdir(parents=True)
    (root / ".github/workflows/deploy.yml").write_text(workflow_text, encoding="utf-8")
    (root / "LICENSE").write_bytes((REPO_ROOT / "LICENSE").read_bytes())
    (root / "NOTICE").write_text((REPO_ROOT / "NOTICE").read_text(encoding="utf-8"))
    (root / "provenance").mkdir()
    (root / "sample.txt").write_text("sample\n", encoding="utf-8")
    digest = hashlib.sha256((root / "sample.txt").read_bytes()).hexdigest()
    record = {
        "license_status": AUTHORIZED_LICENSE_STATUS,
        "reason": "test fixture",
        "record_type": "local_authored",
        "scope": "repository_owned",
        "target_path": "sample.txt",
        "target_sha256": digest,
        "owner": "fixture",
    }
    (root / "provenance/source-files.jsonl").write_text(
        json.dumps(record, sort_keys=True) + "\n", encoding="utf-8"
    )
    values = {
        key: "<required>"
        for key in (
            "KMA_SERVICE_KEY",
            "R2_BUCKET_NAME",
            "R2_ENDPOINT",
            "R2_ACCESS_KEY_ID",
            "R2_SECRET_ACCESS_KEY",
            "R2_DATA_CATALOG_TOKEN",
            "R2_DATA_CATALOG_URI",
            "R2_DATA_CATALOG_WAREHOUSE",
            "SERVING_CLOUDFLARE_ACCOUNT_ID",
            "CLOUDFLARE_ACCOUNT_ID",
            "CLOUDFLARE_API_TOKEN",
            "SERVING_D1_DATABASE_ID",
            "SERVING_API_SMOKE_TOKEN",
            "AIRFLOW_ADMIN_PASSWORD",
            "AIRFLOW_FERNET_KEY",
            "AIRFLOW_SECRET_KEY",
            "POSTGRES_PASSWORD",
        )
    }
    values["SERVING_API_BASE_URL"] = "https://example.workers.dev/api/v1"
    if env_overrides:
        values.update(env_overrides)
    (root / ".env.example").write_text(
        "".join(f"{key}={value}\n" for key, value in values.items()),
        encoding="utf-8",
    )


def _workflow(job: dict[str, Any], *, on: str | dict[str, Any] = "push") -> str:
    return json.dumps({"name": "Test", "on": on, "jobs": {"job": job}})


def test_root_license_and_notice_record_public_terms_and_attribution() -> None:
    license_bytes = (REPO_ROOT / "LICENSE").read_bytes()
    license_text = license_bytes.decode("utf-8")
    notice_text = (REPO_ROOT / "NOTICE").read_text(encoding="utf-8")

    assert hashlib.sha256(license_bytes).hexdigest() == APACHE_2_0_OFFICIAL_SHA256
    assert license_text.startswith(
        "\n                                 Apache License\n"
        "                           Version 2.0, January 2004\n"
    )
    assert "TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION" in license_text
    assert "approved team-code republication authorization dated 2026-08-21" in notice_text
    assert "NomaDamas/k-skill" in notice_text
    assert "MIT License" in notice_text
    assert "sole author" not in notice_text.lower()


def test_provenance_manifest_has_no_private_publication_locks() -> None:
    records = _manifest_records()

    assert records
    assert {
        record["license_status"] for record in records
    }.isdisjoint({"internal_private_snapshot_only", "repository_owned_private"})
    assert all(record.get("license_status") == AUTHORIZED_LICENSE_STATUS for record in records)


def test_provenance_manifest_targets_exist_and_include_public_release_plan() -> None:
    records = _manifest_records()
    targets = {str(record["target_path"]) for record in records}

    assert PLAN_TARGET in targets
    assert all((REPO_ROOT / target).is_file() for target in targets)


def test_public_release_contract_keeps_only_legacy_workflow_blockers() -> None:
    errors = _validate_public_release_contract()

    assert errors == [
        "workflow.self_hosted_runner:.github/workflows/ci.yml:dagbag-runtime",
        "workflow.self_hosted_runner:.github/workflows/deploy-main.yml:deploy-main",
        "workflow.deploy_main_enabled:.github/workflows/deploy-main.yml:deploy-main",
    ]


def test_example_environment_remains_secretless_and_placeholder_only() -> None:
    example = REPO_ROOT / ".env.example"
    values = {
        line.partition("=")[0]: line.partition("=")[2]
        for line in example.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    }

    assert find_secret_candidates(REPO_ROOT, [".env.example"]) == []
    assert values["KMA_SERVICE_KEY"] == "<required>"
    assert values["R2_SECRET_ACCESS_KEY"] == "<required>"
    assert values["CLOUDFLARE_API_TOKEN"] == "<required>"
    assert values["AIRFLOW_SECRET_KEY"] == "<required>"


def test_contract_rejects_unknown_dynamic_mapping_and_case_variant_runners(
    tmp_path: Path,
) -> None:
    workflows = {
        "unknown": _workflow({"runs-on": "debian-latest", "steps": [{"run": "echo hi"}]}),
        "dynamic": _workflow(
            {"runs-on": "${{ matrix.os }}", "steps": [{"run": "echo hi"}]}
        ),
        "mapping": _workflow(
            {"runs-on": {"group": "ubuntu-runners"}, "steps": [{"run": "echo hi"}]}
        ),
        "case_variant": _workflow(
            {"runs-on": "Ubuntu-Latest", "steps": [{"run": "echo hi"}]}
        ),
    }

    for name, workflow_text in workflows.items():
        repo = tmp_path / name
        _write_minimal_repo(repo, workflow_text=workflow_text)

        assert validate_public_release_contract(repo) == [
            f"workflow.non_github_hosted_runner:.github/workflows/deploy.yml:job"
        ]


def test_contract_accepts_literal_github_hosted_runners(tmp_path: Path) -> None:
    for label in ("ubuntu-latest", "windows-latest", "macos-latest"):
        repo = tmp_path / label
        _write_minimal_repo(
            repo,
            workflow_text=_workflow({"runs-on": label, "steps": [{"run": "echo hi"}]}),
        )

        assert validate_public_release_contract(repo) == []


def test_disabled_deploy_manual_noop_shape_is_allowed(tmp_path: Path) -> None:
    repo = tmp_path / "noop"
    _write_minimal_repo(
        repo,
        workflow_text=json.dumps(
            {
                "name": "Deploy Main",
                "on": "workflow_dispatch",
                "permissions": {"contents": "read"},
                "jobs": {
                    "disabled": {
                        "if": "${{ false }}",
                        "runs-on": "ubuntu-latest",
                        "steps": [{"name": "Disabled", "run": "echo disabled"}],
                    }
                },
            }
        ),
    )

    assert validate_public_release_contract(repo) == []


def test_deploy_semantics_fail_for_renamed_active_and_workflow_run_cases(
    tmp_path: Path,
) -> None:
    cases = {
        "renamed": (
            json.dumps(
                {
                    "name": "Production Release",
                    "on": "workflow_dispatch",
                    "jobs": {
                        "noop": {
                            "if": "${{ false }}",
                            "runs-on": "ubuntu-latest",
                            "steps": [{"run": "echo deploy-main"}],
                        }
                    },
                }
            ),
            "workflow.deploy_main_enabled:.github/workflows/deploy.yml:noop",
        ),
        "active": (
            json.dumps(
                {
                    "name": "Deploy Main",
                    "on": "workflow_dispatch",
                    "jobs": {
                        "active": {
                            "runs-on": "ubuntu-latest",
                            "steps": [{"run": "echo disabled"}],
                        }
                    },
                }
            ),
            "workflow.deploy_main_enabled:.github/workflows/deploy.yml:active",
        ),
        "workflow_run": (
            json.dumps(
                {
                    "name": "Deploy Main",
                    "on": {"workflow_run": {"workflows": ["CI"]}},
                    "jobs": {
                        "disabled": {
                            "if": "${{ false }}",
                            "runs-on": "ubuntu-latest",
                            "steps": [{"run": "echo disabled"}],
                        }
                    },
                }
            ),
            "workflow.deploy_main_enabled:.github/workflows/deploy.yml:disabled",
        ),
    }

    for name, (workflow_text, expected) in cases.items():
        repo = tmp_path / name
        _write_minimal_repo(repo, workflow_text=workflow_text)

        assert expected in validate_public_release_contract(repo)


def test_disabled_deploy_noop_rejects_secrets_writes_actions_and_environment(
    tmp_path: Path,
) -> None:
    cases = {
        "secret": {"steps": [{"run": "echo ${{ secrets.TOKEN }}"}]},
        "write_permission": {"permissions": {"contents": "write"}},
        "action": {"steps": [{"uses": "actions/checkout@v4"}]},
        "environment": {"environment": "prod"},
    }

    for name, override in cases.items():
        job = {
            "if": "${{ false }}",
            "runs-on": "ubuntu-latest",
            "steps": [{"run": "echo disabled"}],
        }
        workflow = {
            "on": "workflow_dispatch",
            "permissions": {"contents": "read"},
            "jobs": {"disabled": {**job, **override}},
        }
        repo = tmp_path / name
        _write_minimal_repo(
            repo,
            workflow_text=json.dumps({"name": "Deploy Main", **workflow}),
        )

        assert any(
            error.startswith("workflow.deploy_main_enabled:")
            for error in validate_public_release_contract(repo)
        )


def test_example_environment_placeholder_schema_covers_all_private_runtime_fields(
    tmp_path: Path,
) -> None:
    required_private_keys = (
        "KMA_SERVICE_KEY",
        "R2_BUCKET_NAME",
        "R2_ENDPOINT",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_DATA_CATALOG_TOKEN",
        "R2_DATA_CATALOG_URI",
        "R2_DATA_CATALOG_WAREHOUSE",
        "SERVING_CLOUDFLARE_ACCOUNT_ID",
        "CLOUDFLARE_ACCOUNT_ID",
        "CLOUDFLARE_API_TOKEN",
        "SERVING_D1_DATABASE_ID",
        "SERVING_API_SMOKE_TOKEN",
        "AIRFLOW_ADMIN_PASSWORD",
        "AIRFLOW_FERNET_KEY",
        "AIRFLOW_SECRET_KEY",
        "POSTGRES_PASSWORD",
    )

    for key in required_private_keys:
        repo = tmp_path / key
        _write_minimal_repo(
            repo,
            workflow_text=_workflow(
                {"runs-on": "ubuntu-latest", "steps": [{"run": "echo hi"}]}
            ),
            env_overrides={key: "real-value"},
        )

        assert validate_public_release_contract(repo) == [
            f"example_env.non_placeholder_secret:{key}"
        ]
