from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import yaml

from tools.repository_policy import find_secret_candidates


AUTHORIZED_LICENSE_STATUS = "public_republication_authorized"
APACHE_2_0_SHA256 = "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30"
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
        "R2_BUCKET_NAME",
        "R2_ENDPOINT",
        "R2_SECRET_ACCESS_KEY",
        "R2_ACCESS_KEY_ID",
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
    }
)
GITHUB_HOSTED_RUNNERS = frozenset(
    {
        "ubuntu-latest",
        "windows-latest",
        "macos-latest",
    }
)
FALSE_CONDITIONS = frozenset({"false", "${{ false }}"})
ECHO_COMMAND = re.compile(r"^echo(?:\s+|$)")


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


def _workflow_on(data: dict[str, Any]) -> object:
    if "on" in data:
        return data["on"]
    return data.get(True)


def _is_workflow_dispatch_only(value: object) -> bool:
    if value == "workflow_dispatch":
        return True
    if isinstance(value, list):
        return value == ["workflow_dispatch"]
    if isinstance(value, dict):
        return set(value) == {"workflow_dispatch"}
    return False


def _is_github_hosted_runner(runs_on: object) -> bool:
    return isinstance(runs_on, str) and runs_on in GITHUB_HOSTED_RUNNERS


def _is_self_hosted_runner(runs_on: object) -> bool:
    if isinstance(runs_on, str):
        return runs_on == "self-hosted"
    if isinstance(runs_on, list):
        return "self-hosted" in runs_on
    return False


def _contains_dynamic_expression(value: object) -> bool:
    if isinstance(value, str):
        return "${{" in value
    if isinstance(value, list):
        return any(_contains_dynamic_expression(item) for item in value)
    if isinstance(value, dict):
        return any(
            _contains_dynamic_expression(key) or _contains_dynamic_expression(item)
            for key, item in value.items()
        )
    return False


def _contains_secret_reference(value: object) -> bool:
    if isinstance(value, str):
        return "secrets." in value
    if isinstance(value, list):
        return any(_contains_secret_reference(item) for item in value)
    if isinstance(value, dict):
        return any(_contains_secret_reference(item) for item in value.values())
    return False


def _permissions_are_read_only(value: object) -> bool:
    if value in (None, "read-all"):
        return True
    if value == "write-all":
        return False
    if isinstance(value, dict):
        return all(permission in {"read", "none"} for permission in value.values())
    return False


def _job_condition_is_false(job: dict[str, Any]) -> bool:
    condition = job.get("if")
    if isinstance(condition, bool):
        return condition is False
    if isinstance(condition, str):
        return condition.strip() in FALSE_CONDITIONS
    return False


def _steps_are_inert_message_only(job: dict[str, Any]) -> bool:
    steps = job.get("steps")
    if not isinstance(steps, list) or not steps:
        return False
    for step in steps:
        if not isinstance(step, dict):
            return False
        if "uses" in step:
            return False
        run = step.get("run")
        if not isinstance(run, str) or not ECHO_COMMAND.match(run.strip()):
            return False
    return True


def _has_deploy_semantics(relative: str, data: dict[str, Any]) -> bool:
    workflow_name = str(data.get("name", "")).lower()
    if relative.lower().endswith("/deploy-main.yml") or "deploy" in workflow_name:
        return True
    on_value = _workflow_on(data)
    if isinstance(on_value, dict) and "workflow_run" in on_value:
        return True
    return _value_contains_deploy_text(data)


def _value_contains_deploy_text(value: object) -> bool:
    if isinstance(value, str):
        return "deploy-main" in value.lower()
    if isinstance(value, list):
        return any(_value_contains_deploy_text(item) for item in value)
    if isinstance(value, dict):
        return any(
            _value_contains_deploy_text(key) or _value_contains_deploy_text(item)
            for key, item in value.items()
        )
    return False


def _deploy_job_names(jobs: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for job_id, job in jobs.items():
        if not isinstance(job, dict):
            continue
        job_name = str(job.get("name") or job_id)
        if (
            "deploy" in str(job_id).lower()
            or "deploy" in job_name.lower()
            or _is_self_hosted_runner(job.get("runs-on"))
            or _value_contains_deploy_text(job)
        ):
            names.append(job_name)
    if names:
        return names
    return [
        str(job.get("name") or job_id) if isinstance(job, dict) else str(job_id)
        for job_id, job in jobs.items()
    ]


def _is_strict_disabled_deploy_workflow(
    data: dict[str, Any], jobs: dict[str, Any]
) -> bool:
    if not _is_workflow_dispatch_only(_workflow_on(data)):
        return False
    if not _permissions_are_read_only(data.get("permissions")):
        return False
    if "environment" in data or "env" in data or "secrets" in data:
        return False
    if _contains_secret_reference(data):
        return False
    if not jobs:
        return False
    for job in jobs.values():
        if not isinstance(job, dict):
            return False
        if not _is_github_hosted_runner(job.get("runs-on")):
            return False
        if not _job_condition_is_false(job):
            return False
        if not _permissions_are_read_only(job.get("permissions")):
            return False
        if "environment" in job or "env" in job or "secrets" in job:
            return False
        if _value_contains_deploy_text(job):
            return False
        if not _steps_are_inert_message_only(job):
            return False
    return True


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
            runs_on = job.get("runs-on")
            if _is_self_hosted_runner(runs_on):
                errors.append(f"workflow.self_hosted_runner:{relative}:{job_name}")
            elif not _is_github_hosted_runner(runs_on) or _contains_dynamic_expression(
                runs_on
            ):
                errors.append(
                    f"workflow.non_github_hosted_runner:{relative}:{job_name}"
                )
        if _has_deploy_semantics(
            relative, data
        ) and not _is_strict_disabled_deploy_workflow(data, jobs):
            for job_name in _deploy_job_names(jobs):
                errors.append(f"workflow.deploy_main_enabled:{relative}:{job_name}")
    return errors


def validate_public_release_contract(repo_root: Path) -> list[str]:
    root = repo_root.resolve()
    errors: list[str] = []

    license_path = root / "LICENSE"
    if not license_path.is_file():
        errors.append("license.missing:LICENSE")
    else:
        import hashlib

        license_bytes = license_path.read_bytes()
        license_text = license_bytes.decode("utf-8")
        if hashlib.sha256(license_bytes).hexdigest() != APACHE_2_0_SHA256:
            errors.append("license.not_official_apache_2_0_sha256:LICENSE")
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
