"""Value objects and stable configuration for Weather dbt execution."""

from __future__ import annotations

import hashlib
import os
import re
import shlex
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DEFAULT_DBT_OL_BIN = "/home/airflow/dbt-venv/bin/dbt-ol"
DEFAULT_DBT_PROJECT_DIR = "/opt/airflow/dbt/domains/traffic_weather"
DBT_BIN_ENV = "ASK_SEOUL_DBT_BIN"
DBT_PROJECT_DIR_ENV = "ASK_SEOUL_DBT_PROJECT_DIR"
DBT_ARTIFACT_ROOT_ENV = "ASK_SEOUL_DBT_ARTIFACT_ROOT"
ARTIFACT_RETENTION_ENV = "ASK_SEOUL_DBT_ARTIFACT_RETENTION_RUNS"
DEFAULT_ARTIFACT_RETENTION_RUNS = 10
MATERIALIZATION_COMMANDS = frozenset({"seed", "run", "test", "build", "snapshot"})
MAX_PATH_SEGMENT_LENGTH = 96
ENCODED_PATH_SEGMENT_PREFIX = "encoded-"


@dataclass(frozen=True)
class DbtAttemptPaths:
    preflight_target_path: str
    preflight_log_path: str
    execution_target_path: str
    execution_log_path: str
    packages_path: str
    run_results_path: str | None
    sources_path: str | None
    manifest_path: str | None


@dataclass(frozen=True)
class DbtExecution:
    attempts: tuple[Any, ...]
    paths: DbtAttemptPaths
    selected_unique_ids: tuple[str, ...]
    actual_attempted: bool
    existing_run_results_path: str | None
    existing_sources_path: str | None
    existing_manifest_path: str | None
    missing_expected_artifacts: tuple[str, ...]

    @property
    def completed(self) -> Any:
        return self.attempts[-1]

    @property
    def primary_artifact_path(self) -> str | None:
        return self.existing_run_results_path or self.existing_sources_path


def dbt_bin() -> str:
    return (os.environ.get(DBT_BIN_ENV) or DEFAULT_DBT_BIN).strip()


def dbt_project_dir(environment: Mapping[str, str] = os.environ) -> str:
    return (environment.get(DBT_PROJECT_DIR_ENV) or DEFAULT_DBT_PROJECT_DIR).strip()


def _overlap(first: Path, second: Path) -> bool:
    return (
        first == second or first.is_relative_to(second) or second.is_relative_to(first)
    )


def dbt_artifact_root(
    project_dir: str,
    environment: Mapping[str, str] = os.environ,
) -> str:
    """Return a distinct generated-artifact root, or the legacy project root."""

    if DBT_ARTIFACT_ROOT_ENV not in environment:
        return project_dir
    configured = environment[DBT_ARTIFACT_ROOT_ENV]
    if (
        not isinstance(configured, str)
        or not configured
        or configured != configured.strip()
    ):
        raise RuntimeError(
            f"{DBT_ARTIFACT_ROOT_ENV} must be a clean absolute non-root path"
        )
    if {".", ".."} & set(re.split(r"[\\/]+", configured)):
        raise RuntimeError(
            f"{DBT_ARTIFACT_ROOT_ENV} must not contain lexical traversal"
        )

    artifact_path = Path(configured)
    if not artifact_path.is_absolute():
        raise RuntimeError(f"{DBT_ARTIFACT_ROOT_ENV} must be an absolute path")
    if artifact_path == Path(artifact_path.anchor):
        raise RuntimeError(f"{DBT_ARTIFACT_ROOT_ENV} must not be a filesystem root")

    lexical_artifact = Path(os.path.abspath(artifact_path))
    lexical_project = Path(os.path.abspath(project_dir))
    resolved_artifact = lexical_artifact.resolve(strict=False)
    resolved_project = lexical_project.resolve(strict=False)
    if _overlap(lexical_artifact, lexical_project) or _overlap(
        resolved_artifact, resolved_project
    ):
        raise RuntimeError(
            f"{DBT_ARTIFACT_ROOT_ENV} must be distinct from and outside the dbt project"
        )
    return str(artifact_path)


def safe_path_segment(value: str | None) -> str:
    raw = str(value) if value not in (None, "") else "unknown"
    if raw in {".", ".."}:
        raise ValueError(f"reserved dbt path segment: {raw}")
    normalized = "".join(
        char if char.isascii() and (char.isalnum() or char in "._=-") else "-"
        for char in raw
    )
    if (
        normalized == raw
        and len(raw) <= MAX_PATH_SEGMENT_LENGTH
        and not raw.startswith(ENCODED_PATH_SEGMENT_PREFIX)
    ):
        return raw

    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    fixed_length = len(ENCODED_PATH_SEGMENT_PREFIX) + len("--") + len(digest)
    readable_limit = MAX_PATH_SEGMENT_LENGTH - fixed_length
    readable = normalized.strip("._-=")[:readable_limit].rstrip("._-=")
    readable = readable or "segment"
    return f"{ENCODED_PATH_SEGMENT_PREFIX}{readable}--{digest}"


def command_name(dbt_command: str) -> str:
    command = shlex.split(dbt_command)
    if command[:2] == ["source", "freshness"]:
        return "source freshness"
    if not command:
        raise ValueError("dbt_command must not be empty")
    return command[0]
