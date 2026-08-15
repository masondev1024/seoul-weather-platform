from __future__ import annotations

import json
import os
import pathlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath

from deployment.canonical_json import canonical_bytes, sha256_hex
from deployment.redaction import SensitiveArtifactError, reject_sensitive_artifact
from tools.dagbag_runtime_check import EXPECTED_DAG_IDS


SCHEMA_VERSION = "weather-local-deploy-target/v1"
_CREDENTIAL_SOURCE_KINDS = frozenset(
    {"windows_credential_store", "existing_local_env"}
)
_COMPOSE_SERVICE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


@dataclass(frozen=True)
class DeployTarget:
    raw: Mapping[str, object]
    canonical_target_bytes: bytes
    schema_version: str
    target_id: str
    credential_source_kind: str
    credential_reference: str
    project_name: str
    working_directory: PurePath
    compose_files: tuple[PurePath, ...]
    control_service: str
    airflow_code_services: frozenset[str]
    forbidden_data_services: frozenset[str]
    dags_host_path: PurePath
    dags_container_path: str
    dbt_host_path: PurePath
    dbt_container_path: str
    runtime_root: PurePath
    dag_allowlist: frozenset[str]
    writer_dag_allowlist: frozenset[str]
    never_trigger: bool
    drain_timeout_seconds: int
    poll_interval_seconds: int
    ledger_directory: PurePath
    lock_file: PurePath
    generated_overlay_file: PurePath


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return value


def _require_exact_keys(
    value: Mapping[str, object], field: str, expected: frozenset[str]
) -> None:
    unexpected = set(value) - expected
    missing = expected - set(value)
    if unexpected:
        raise ValueError(f"{field} has unexpected properties: {sorted(unexpected)}")
    if missing:
        raise ValueError(f"{field} is missing required properties: {sorted(missing)}")


def _string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _string_set(value: object, field: str, *, non_empty: bool = True) -> frozenset[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{field} must be an array")
    if any(not isinstance(item, str) or not item for item in value):
        raise ValueError(f"{field} must contain non-empty strings")
    items = frozenset(value)
    if len(items) != len(value):
        raise ValueError(f"{field} must not contain duplicates")
    if non_empty and not items:
        raise ValueError(f"{field} must not be empty")
    return items


def _service_name_set(value: object, field: str, *, non_empty: bool = True) -> frozenset[str]:
    items = _string_set(value, field, non_empty=non_empty)
    for item in items:
        if "airflow-init" in item.casefold():
            raise ValueError(f"{field} must exclude airflow-init")
        if not _COMPOSE_SERVICE_NAME.fullmatch(item):
            raise ValueError(f"{field} contains an invalid service name")
    return items


def _service_name(value: object, field: str) -> str:
    item = _string(value, field)
    if "airflow-init" in item.casefold():
        raise ValueError(f"{field} must exclude airflow-init")
    if not _COMPOSE_SERVICE_NAME.fullmatch(item):
        raise ValueError(f"{field} contains an invalid service name")
    return item


def _string_tuple(value: object, field: str, *, non_empty: bool = True) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{field} must be an array")
    if any(not isinstance(item, str) or not item for item in value):
        raise ValueError(f"{field} must contain non-empty strings")
    items = tuple(value)
    if len(set(items)) != len(items):
        raise ValueError(f"{field} must not contain duplicates")
    if non_empty and not items:
        raise ValueError(f"{field} must not be empty")
    return items


def _absolute_path(value: object, field: str) -> PurePath:
    raw_path = _string(value, field)
    if raw_path.startswith(("\\\\", "//")):
        raise ValueError(f"{field} must not use a UNC path")
    windows_path = PureWindowsPath(raw_path)
    if windows_path.is_absolute():
        if windows_path == PureWindowsPath(windows_path.anchor):
            raise ValueError(f"{field} must not be a filesystem root")
        if {".", ".."} & set(re.split(r"[\\/]+", raw_path)):
            raise ValueError(f"{field} must not contain lexical traversal")
        return windows_path
    posix_path = PurePosixPath(raw_path)
    if posix_path.is_absolute():
        if posix_path == PurePosixPath(posix_path.anchor):
            raise ValueError(f"{field} must not be a filesystem root")
        if {".", ".."} & set(re.split(r"[\\/]+", raw_path)):
            raise ValueError(f"{field} must not contain lexical traversal")
        return posix_path
    raise ValueError(f"{field} must be an absolute path")


def _path_identity(path: PurePath) -> tuple[str, str]:
    if isinstance(path, PureWindowsPath):
        normalized = str(PureWindowsPath(str(path))).replace("/", "\\")
        return ("windows", normalized.casefold())
    return ("posix", str(PurePosixPath(str(path))))


def _is_native_path(path: PurePath) -> bool:
    return (
        os.name == "nt"
        and isinstance(path, PureWindowsPath)
    ) or (os.name != "nt" and isinstance(path, PurePosixPath))


def _resolved_file_identity(path: PurePath | Path) -> tuple[str, str]:
    if isinstance(path, Path):
        return _path_identity(_absolute_path(str(path.resolve(strict=False)), "deploy target path"))
    if _is_native_path(path):
        resolved = pathlib.Path(str(path)).resolve(strict=False)
        return _path_identity(_absolute_path(str(resolved), "resolved file path"))
    return _path_identity(path)


def _file_identities(path: PurePath | Path) -> frozenset[tuple[str, str]]:
    return frozenset({_path_identity(path), _resolved_file_identity(path)})


def _integer_in_range(value: object, field: str, lower: int, upper: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer")
    if not lower <= value <= upper:
        raise ValueError(f"{field} must be between {lower} and {upper}")
    return value


def _require_outside_repository(path: PurePath, repo_root: Path, field: str) -> None:
    is_native_path = (
        os.name == "nt" and isinstance(path, PureWindowsPath)
    ) or (os.name != "nt" and isinstance(path, PurePosixPath))
    if is_native_path:
        candidate_path = pathlib.Path(str(path)).resolve(strict=False)
        repository_path = repo_root.resolve(strict=False)
    else:
        candidate_path = path
        repository_path = type(path)(str(repo_root))
    try:
        candidate_path.relative_to(repository_path)
    except ValueError:
        return
    raise ValueError(f"{field} must be outside the repository")


def load_deploy_target(path: Path, repo_root: Path) -> DeployTarget:
    """Load and fail closed on an invalid local deployment target."""
    if ".." in path.parts or ".." in repo_root.parts:
        raise ValueError("deploy target paths must not contain lexical traversal")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"unable to load deploy target: {path}") from error
    root = _mapping(raw, "deploy target")
    _require_exact_keys(
        root,
        "deploy target",
        frozenset(
            {
                "schema_version",
                "target_id",
                "credential_source_kind",
                "credential_reference",
                "compose",
                "mounts",
                "airflow",
                "timeouts",
                "local_state",
            }
        ),
    )

    schema_version = _string(root.get("schema_version"), "schema_version")
    if schema_version != SCHEMA_VERSION:
        raise ValueError("unsupported schema_version")
    credential_source_kind = _string(
        root.get("credential_source_kind"), "credential_source_kind"
    )
    if credential_source_kind not in _CREDENTIAL_SOURCE_KINDS:
        raise ValueError("unsupported credential_source_kind")

    compose = _mapping(root.get("compose"), "compose")
    mounts = _mapping(root.get("mounts"), "mounts")
    airflow = _mapping(root.get("airflow"), "airflow")
    timeouts = _mapping(root.get("timeouts"), "timeouts")
    local_state = _mapping(root.get("local_state"), "local_state")
    _require_exact_keys(
        compose,
        "compose",
        frozenset(
            {
                "project_name",
                "working_directory",
                "files",
                "control_service",
                "airflow_code_services",
                "forbidden_data_services",
            }
        ),
    )
    _require_exact_keys(
        mounts,
        "mounts",
        frozenset(
            {
                "dags_host_path",
                "dags_container_path",
                "dbt_host_path",
                "dbt_container_path",
                "runtime_root",
            }
        ),
    )
    _require_exact_keys(
        airflow,
        "airflow",
        frozenset({"dag_allowlist", "writer_dag_allowlist", "never_trigger"}),
    )
    _require_exact_keys(
        timeouts,
        "timeouts",
        frozenset({"drain_timeout_seconds", "poll_interval_seconds"}),
    )
    _require_exact_keys(
        local_state,
        "local_state",
        frozenset({"ledger_directory", "lock_file", "generated_overlay_file"}),
    )

    compose_files = tuple(
        _absolute_path(item, "compose.files")
        for item in _string_tuple(compose.get("files"), "compose.files")
    )
    if len({_path_identity(item) for item in compose_files}) != len(compose_files):
        raise ValueError("compose.files must not contain duplicate normalized paths")
    airflow_code_services = _service_name_set(
        compose.get("airflow_code_services"), "compose.airflow_code_services"
    )
    forbidden_data_services = _service_name_set(
        compose.get("forbidden_data_services"), "compose.forbidden_data_services"
    )
    if airflow_code_services & forbidden_data_services:
        raise ValueError("airflow code services must exclude forbidden data services")
    control_service = _service_name(compose.get("control_service"), "compose.control_service")
    if control_service not in airflow_code_services:
        raise ValueError("compose.control_service must be an airflow code service")
    if control_service in forbidden_data_services:
        raise ValueError("compose.control_service must not be a forbidden data service")

    dag_allowlist = _string_set(airflow.get("dag_allowlist"), "airflow.dag_allowlist")
    if dag_allowlist != EXPECTED_DAG_IDS:
        raise ValueError("airflow.dag_allowlist must exactly match weather DAG ids")
    writer_dag_allowlist = _string_set(
        airflow.get("writer_dag_allowlist"), "airflow.writer_dag_allowlist"
    )
    if not writer_dag_allowlist <= dag_allowlist:
        raise ValueError("airflow.writer_dag_allowlist must be a dag_allowlist subset")
    if airflow.get("never_trigger") is not True:
        raise ValueError("airflow.never_trigger must be true")

    runtime_root = _absolute_path(mounts.get("runtime_root"), "mounts.runtime_root")
    ledger_directory = _absolute_path(
        local_state.get("ledger_directory"), "local_state.ledger_directory"
    )
    lock_file = _absolute_path(local_state.get("lock_file"), "local_state.lock_file")
    generated_overlay_file = _absolute_path(
        local_state.get("generated_overlay_file"), "local_state.generated_overlay_file"
    )
    _require_outside_repository(runtime_root, repo_root, "mounts.runtime_root")
    _require_outside_repository(ledger_directory, repo_root, "local_state.ledger_directory")
    _require_outside_repository(lock_file, repo_root, "local_state.lock_file")
    _require_outside_repository(
        generated_overlay_file, repo_root, "local_state.generated_overlay_file"
    )
    local_state_identities = {
        _path_identity(ledger_directory),
        _path_identity(lock_file),
        _path_identity(generated_overlay_file),
    }
    if len(local_state_identities) != 3:
        raise ValueError("local_state paths must be normalized-distinct")
    if _path_identity(runtime_root) in local_state_identities:
        raise ValueError("mounts.runtime_root must be normalized-distinct from local_state paths")
    compose_file_identities = {
        identity
        for compose_file in compose_files
        for identity in _file_identities(compose_file)
    }
    target_file_identities = _file_identities(path)
    for field, local_file in (
        ("local_state.lock_file", lock_file),
        ("local_state.generated_overlay_file", generated_overlay_file),
    ):
        local_file_identities = _file_identities(local_file)
        if local_file_identities & compose_file_identities:
            raise ValueError(f"{field} must be distinct from compose.files")
        if local_file_identities & target_file_identities:
            raise ValueError(f"{field} must be distinct from deploy target path")
    if generated_overlay_file.suffix.lower() not in {".yml", ".yaml"}:
        raise ValueError("local_state.generated_overlay_file must be a .yml or .yaml file")
    dags_container_path = _string(
        mounts.get("dags_container_path"), "mounts.dags_container_path"
    )
    if dags_container_path != "/opt/airflow/dags":
        raise ValueError("mounts.dags_container_path must be /opt/airflow/dags")
    dbt_container_path = _string(
        mounts.get("dbt_container_path"), "mounts.dbt_container_path"
    )
    if dbt_container_path != "/opt/airflow/dbt":
        raise ValueError("mounts.dbt_container_path must be /opt/airflow/dbt")

    return DeployTarget(
        raw=root,
        canonical_target_bytes=canonical_bytes(root),
        schema_version=schema_version,
        target_id=_string(root.get("target_id"), "target_id"),
        credential_source_kind=credential_source_kind,
        credential_reference=_string(
            root.get("credential_reference"), "credential_reference"
        ),
        project_name=_string(compose.get("project_name"), "compose.project_name"),
        working_directory=_absolute_path(
            compose.get("working_directory"), "compose.working_directory"
        ),
        compose_files=compose_files,
        control_service=control_service,
        airflow_code_services=airflow_code_services,
        forbidden_data_services=forbidden_data_services,
        dags_host_path=_absolute_path(
            mounts.get("dags_host_path"), "mounts.dags_host_path"
        ),
        dags_container_path=dags_container_path,
        dbt_host_path=_absolute_path(mounts.get("dbt_host_path"), "mounts.dbt_host_path"),
        dbt_container_path=dbt_container_path,
        runtime_root=runtime_root,
        dag_allowlist=dag_allowlist,
        writer_dag_allowlist=writer_dag_allowlist,
        never_trigger=True,
        drain_timeout_seconds=_integer_in_range(
            timeouts.get("drain_timeout_seconds"),
            "timeouts.drain_timeout_seconds",
            60,
            3600,
        ),
        poll_interval_seconds=_integer_in_range(
            timeouts.get("poll_interval_seconds"),
            "timeouts.poll_interval_seconds",
            5,
            60,
        ),
        ledger_directory=ledger_directory,
        lock_file=lock_file,
        generated_overlay_file=generated_overlay_file,
    )


def target_fingerprint(target: DeployTarget) -> str:
    """Return the publishable digest of the full local target document."""
    return sha256_hex(target.canonical_target_bytes)


def _reject_credential_values(value: object, prohibited: frozenset[str]) -> None:
    if isinstance(value, Mapping):
        for item in value.values():
            _reject_credential_values(item, prohibited)
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            _reject_credential_values(item, prohibited)
        return
    if isinstance(value, str) and value in prohibited:
        raise SensitiveArtifactError("credential value in public target summary")


def public_target_summary(target: DeployTarget) -> dict[str, object]:
    """Return the non-sensitive target facts suitable for a release report."""
    summary: dict[str, object] = {
        "schema_version": target.schema_version,
        "target_id": target.target_id,
        "compose_project": target.project_name,
        "control_service": target.control_service,
        "airflow_code_services": sorted(target.airflow_code_services),
        "forbidden_data_services": sorted(target.forbidden_data_services),
        "dag_allowlist": sorted(target.dag_allowlist),
        "writer_dag_allowlist": sorted(target.writer_dag_allowlist),
        "never_trigger": target.never_trigger,
        "timeouts": {
            "drain_timeout_seconds": target.drain_timeout_seconds,
            "poll_interval_seconds": target.poll_interval_seconds,
        },
    }
    _reject_credential_values(
        summary,
        frozenset({target.credential_reference, target.credential_source_kind}),
    )
    reject_sensitive_artifact(summary)
    return summary
