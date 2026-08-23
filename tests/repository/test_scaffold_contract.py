from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import tomllib


REPO_ROOT = Path(__file__).resolve().parents[2]


EXPECTED_SOURCES = {
    "airflow_weather": "73ff5665ffd5526c59de8be2969cf65dffaf468b",
    "weather_dbt": "a64292d50bd8c2a19784388828de38d2b4a8c525",
    "weather_origin_contract": "efe393e7a925d5798867424993daf0dbe5d55902",
    "kskill_runtime": "43edf3c0f1037a4e510b21de61e26965212b6620",
}


def test_gitattributes_keeps_provenance_content_lf_stable() -> None:
    attributes = (REPO_ROOT / ".gitattributes").read_text(encoding="utf-8")

    assert "* text=auto eol=lf" in attributes.splitlines()


def test_gitignore_blocks_local_harness_secrets_and_generated_outputs() -> None:
    entries = {
        line.strip()
        for line in (REPO_ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    assert {
        ".env",
        ".env.*",
        ".omc/",
        ".omx/",
        ".pytest_cache/",
        "**/__pycache__/",
        "**/dbt_packages/",
        "**/logs/",
        "**/target/",
        "**/.user.yml",
        "*.key",
        "*.p12",
        "*.pem",
        ".wrangler/",
        "LessonRun.md",
        "engineering-decision-log.md",
    } <= entries


def test_dockerignore_excludes_repository_metadata_and_credentials() -> None:
    entries = {
        line.strip()
        for line in (REPO_ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    assert {".env*", ".git", ".omc", ".omx", "**/dbt_packages", "**/target"} <= entries


def test_source_lock_uses_the_reviewed_fixed_commits() -> None:
    payload = json.loads(
        (REPO_ROOT / "provenance" / "source-refs.lock.json").read_text(encoding="utf-8")
    )

    assert payload["schema_version"] == "source-refs/v1"
    assert {source["id"]: source["commit"] for source in payload["sources"]} == EXPECTED_SOURCES
    assert all(len(source["commit"]) == 40 for source in payload["sources"])


def test_source_inventory_matches_the_reviewed_extraction_counts() -> None:
    payload = json.loads(
        (REPO_ROOT / "provenance" / "source-inventory.json").read_text(
            encoding="utf-8"
        )
    )
    entries = payload["entries"]
    actual_counts = Counter(entry["source_id"] for entry in entries)

    assert payload["counts"] == {
        "airflow_weather": 141,
        "weather_dbt": 88,
        "weather_origin_contract": 3,
        "total": 232,
    }
    assert actual_counts == {
        "airflow_weather": 141,
        "weather_dbt": 88,
        "weather_origin_contract": 3,
    }
    assert len(entries) == payload["counts"]["total"]
    assert len({entry["target_path"] for entry in entries}) == len(entries)


def test_repository_rules_require_user_report_before_airflow_state_change() -> None:
    rules = (REPO_ROOT / "AGENTS.md").read_text(encoding="utf-8")

    assert "AIRFLOW_DEPLOYMENT_APPROVAL_REQUIRED" in rules
    assert "기존 로컬 파이프라인" in rules
    assert "DAG 활성화" in rules
    assert "수동 트리거" in rules


def test_toolchain_lock_is_secret_free_and_records_required_runtimes() -> None:
    payload = json.loads(
        (REPO_ROOT / "runtime" / "toolchain.lock.json").read_text(encoding="utf-8")
    )

    assert payload["schema_version"] == "weather-toolchain/v1"
    assert {"python", "airflow", "dbt_core", "dbt_adapter", "node"} <= set(
        payload["tools"]
    )
    assert all(payload["tools"][name]["version"] for name in payload["tools"])
    assert payload["tools"]["airflow"]["version"] == "3.2.2"
    assert payload["tools"]["airflow"]["image_repository"] == "elt-infra-airflow"
    assert payload["tools"]["dbt_core"]["version"] == "1.10.22"
    assert payload["tools"]["dbt_adapter"] == {
        "name": "dbt-trino",
        "version": "1.10.2",
        "evidence": "fixed DBT CI lock and running Airflow dbt virtualenv",
    }


def test_runtime_dependency_locks_and_all_test_roots_are_declared() -> None:
    airflow_lock = REPO_ROOT / "runtime" / "requirements-airflow.lock.txt"
    dbt_lock = REPO_ROOT / "runtime" / "requirements-dbt.lock.txt"
    airflow_dockerfile = (REPO_ROOT / "Dockerfile.airflow").read_text(
        encoding="utf-8"
    )
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert airflow_lock.is_file()
    assert dbt_lock.is_file()
    airflow_requirements = airflow_lock.read_text(encoding="utf-8")
    assert "apache-airflow==3.2.2" in airflow_requirements
    for dependency in (
        "boto3==1.43.0",
        "trino==0.338.0",
        "pyiceberg[pyarrow,s3fs]==0.11.1",
        "pyiceberg-core==0.8.0",
        "pyarrow==24.0.0",
        "s3fs==2026.6.0",
        "astronomer-cosmos[openlineage]==1.15.0",
        "apache-airflow-providers-trino==6.6.0",
    ):
        assert dependency in airflow_requirements
    assert "COPY --chown=airflow:root runtime/requirements-airflow.lock.txt" in (
        airflow_dockerfile
    )
    assert "pip install --no-cache-dir -r /tmp/requirements-airflow.lock.txt" in (
        airflow_dockerfile
    )
    assert "-r /tmp/requirements-dbt.lock.txt" in airflow_dockerfile
    assert "dbt-core==1.10.22" in dbt_lock.read_text(encoding="utf-8")
    assert "dbt-trino==1.10.2" in dbt_lock.read_text(encoding="utf-8")
    assert project["tool"]["pytest"]["ini_options"]["testpaths"] == [
        "tests",
        "release/weather/tests",
    ]
