from __future__ import annotations

import importlib
import importlib.util
import json
from pathlib import Path

from tools.repository_policy import find_secret_candidates


REPO_ROOT = Path(__file__).resolve().parents[2]
AUTHORIZED_LICENSE_STATUS = "public_republication_authorized"


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


def test_root_license_and_notice_record_public_terms_and_attribution() -> None:
    license_text = (REPO_ROOT / "LICENSE").read_text(encoding="utf-8")
    notice_text = (REPO_ROOT / "NOTICE").read_text(encoding="utf-8")

    assert license_text.lstrip().startswith(
        "Apache License\n                           Version 2.0, January 2004\n"
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
