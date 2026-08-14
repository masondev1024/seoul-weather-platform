"""Raw dbt and OpenLineage environment isolation for Weather."""

from __future__ import annotations

import os
from collections.abc import Mapping

from .contracts import DEFAULT_DBT_OL_BIN, safe_path_segment


DBT_VENV_BIN_DIR = "/home/airflow/dbt-venv/bin"
OPENLINEAGE_ENABLED_ENV = "ASK_SEOUL_DBT_OPENLINEAGE_ENABLED"
OPENLINEAGE_URL_ENV = "ASK_SEOUL_DBT_OPENLINEAGE_URL"
OPENLINEAGE_ENDPOINT_ENV = "ASK_SEOUL_DBT_OPENLINEAGE_ENDPOINT"
OPENLINEAGE_NAMESPACE_ENV = "ASK_SEOUL_DBT_OPENLINEAGE_NAMESPACE"
OPENLINEAGE_NAMESPACE_PREFIX_ENV = "ASK_SEOUL_DBT_OPENLINEAGE_NAMESPACE_PREFIX"
_STANDARD_OPENLINEAGE_CONFIG_KEYS = (
    "OPENLINEAGE_URL",
    "OPENLINEAGE_ENDPOINT",
    "OPENLINEAGE_NAMESPACE",
    "OPENLINEAGE_DBT_JOB_NAME",
    "OPENLINEAGE__FACETS__SOURCE_CODE_LOCATION__DISABLED",
)


def openlineage_enabled(env: Mapping[str, str]) -> bool:
    return str(env.get(OPENLINEAGE_ENABLED_ENV) or "").strip().lower() == "true"


def executable_available(path: str) -> bool:
    return os.path.isfile(path) and os.access(path, os.X_OK)


def raw_environment(
    *, project_dir: str, packages_path: str, environ: Mapping[str, str] | None
) -> dict[str, str]:
    env = dict(os.environ if environ is None else environ)
    for name in _STANDARD_OPENLINEAGE_CONFIG_KEYS:
        env.pop(name, None)
    env["DBT_PROJECT_DIR"] = project_dir
    env["DBT_PROFILES_DIR"] = project_dir
    env["DBT_PACKAGES_INSTALL_PATH"] = packages_path
    current_path = env.get("PATH", "")
    env["PATH"] = (
        DBT_VENV_BIN_DIR
        if not current_path
        else f"{DBT_VENV_BIN_DIR}{os.pathsep}{current_path}"
    )
    return env


def openlineage_environment(
    env: Mapping[str, str], *, pipeline: str, task_id: str | None
) -> dict[str, str]:
    url = str(env.get(OPENLINEAGE_URL_ENV) or "").strip()
    namespace_prefix = str(env.get(OPENLINEAGE_NAMESPACE_PREFIX_ENV) or "").strip()
    namespace = (
        f"{namespace_prefix}-weather"
        if namespace_prefix
        else str(env.get(OPENLINEAGE_NAMESPACE_ENV) or "").strip()
    )
    if not url:
        raise RuntimeError(f"{OPENLINEAGE_URL_ENV} is required when lineage is enabled")
    if not namespace:
        raise RuntimeError(
            f"{OPENLINEAGE_NAMESPACE_ENV} or {OPENLINEAGE_NAMESPACE_PREFIX_ENV} "
            "is required when lineage is enabled"
        )
    if not executable_available(DEFAULT_DBT_OL_BIN):
        raise RuntimeError(f"dbt-ol executable is unavailable: {DEFAULT_DBT_OL_BIN}")

    actual_env = dict(env)
    actual_env["OPENLINEAGE_URL"] = url
    actual_env["OPENLINEAGE_NAMESPACE"] = namespace
    endpoint = str(actual_env.get(OPENLINEAGE_ENDPOINT_ENV) or "").strip()
    if endpoint:
        actual_env["OPENLINEAGE_ENDPOINT"] = endpoint
    actual_env["OPENLINEAGE_DBT_JOB_NAME"] = (
        f"{safe_path_segment(pipeline)}.{safe_path_segment(task_id)}"
    )
    actual_env["OPENLINEAGE__FACETS__SOURCE_CODE_LOCATION__DISABLED"] = "true"
    return actual_env
