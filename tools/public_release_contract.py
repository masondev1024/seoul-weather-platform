from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from tools.repository_policy import find_secret_candidates


AUTHORIZED_LICENSE_STATUS = "public_republication_authorized"
APACHE_LICENSE_MARKERS = (
    "Apache License",
    "Version 2.0, January 2004",
    "TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION",
    "END OF TERMS AND CONDITIONS",
)
NOTICE_MARKERS = (
    "approved team-code republication authorization dated 2026-08-21",
    "NomaDamas/k-skill",
    "MIT License",
)
PLACEHOLDER_SECRET_KEYS = frozenset(
    {
        "KMA_SERVICE_KEY",
        "R2_SECRET_ACCESS_KEY",
        "R2_DATA_CATALOG_TOKEN",
        "CLOUDFLARE_API_TOKEN",
        "SERVING_API_SMOKE_TOKEN",
        "AIRFLOW_ADMIN_PASSWORD",
        "AIRFLOW_FERNET_KEY",
        "AIRFLOW_SECRET_KEY",
        "POSTGRES_PASSWORD",
    }
)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not raw_line.strip():
            continue
        value = json.loads(raw_line)
        if not isinstance(value, dict):
            raise ValueError(f"{path}:{line_number}: provenance record must be an object")
        records.append(value)
    return records


def _env_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator:
            raise ValueError(f"{path}:{line_number}: environment line must use KEY=VALUE")
        values[key] = value
    return values


def _runs_on_self_hosted(runs_on: object) -> bool:
    if isinstance(runs_on, str):
        return runs_on == "self-hosted"
    if isinstance(runs_on, list):
        return "self-hosted" in runs_on
    return False


def _workflow_errors(repo_root: Path) -> list[str]:
    errors: list[str] = []
    workflows_dir = repo_root / ".github/workflows"
    if not workflows_dir.is_dir():
        return errors

    workflow_paths = sorted(workflows_dir.glob("*.yml")) + sorted(
        workflows_dir.glob("*.yaml")
    )
    for path in workflow_paths:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        jobs = data.get("jobs", {})
        if not isinstance(jobs, dict):
            continue
        relative = path.relative_to(repo_root).as_posix()
        for job_id, job in jobs.items():
            if not isinstance(job, dict):
                continue
            job_name = str(job.get("name") or job_id)
            if _runs_on_self_hosted(job.get("runs-on")):
                errors.append(f"workflow.self_hosted_runner:{relative}:{job_name}")
            if job_id == "deploy-main" or job_name == "deploy-main":
                errors.append(f"workflow.deploy_main_enabled:{relative}:{job_name}")
    return errors


def validate_public_release_contract(repo_root: Path) -> list[str]:
    root = repo_root.resolve()
    errors: list[str] = []

    license_path = root / "LICENSE"
    if not license_path.is_file():
        errors.append("license.missing:LICENSE")
    else:
        license_text = license_path.read_text(encoding="utf-8")
        for marker in APACHE_LICENSE_MARKERS:
            if marker not in license_text:
                errors.append(f"license.not_apache_2_0:{marker}")

    notice_path = root / "NOTICE"
    if not notice_path.is_file():
        errors.append("notice.missing:NOTICE")
    else:
        notice_text = notice_path.read_text(encoding="utf-8")
        for marker in NOTICE_MARKERS:
            if marker not in notice_text:
                errors.append(f"notice.missing_attribution:{marker}")
        if "sole author" in notice_text.lower():
            errors.append("notice.invalid_authorship_claim:sole_author")

    manifest_path = root / "provenance/source-files.jsonl"
    if not manifest_path.is_file():
        errors.append("provenance.missing:provenance/source-files.jsonl")
    else:
        for record in _read_jsonl(manifest_path):
            target = str(record.get("target_path", "<unknown>"))
            if record.get("license_status") != AUTHORIZED_LICENSE_STATUS:
                errors.append(f"provenance.unauthorized:{target}")

    env_path = root / ".env.example"
    if not env_path.is_file():
        errors.append("example_env.missing:.env.example")
    else:
        values = _env_values(env_path)
        for key in sorted(PLACEHOLDER_SECRET_KEYS):
            if values.get(key) != "<required>":
                errors.append(f"example_env.non_placeholder_secret:{key}")
        for finding in find_secret_candidates(root, [".env.example"]):
            errors.append(
                f"example_env.secret:{finding.path}:{finding.line}:{finding.rule}"
            )

    errors.extend(_workflow_errors(root))
    return errors
