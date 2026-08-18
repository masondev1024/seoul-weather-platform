import json
from pathlib import Path, PurePosixPath

import pytest
from jsonschema import Draft202012Validator

import deployment.target as target_module
from deployment.redaction import SensitiveArtifactError
from deployment.target import (
    load_deploy_target,
    public_target_summary,
    target_fingerprint,
)
from tools.dagbag_runtime_check import EXPECTED_DAG_IDS


def _valid_target() -> dict[str, object]:
    dag_ids = sorted(EXPECTED_DAG_IDS)
    return {
        "schema_version": "weather-local-deploy-target/v1",
        "target_id": "example-local-weather",
        "credential_source_kind": "windows_credential_store",
        "credential_reference": "weather-local-runtime",
        "compose": {
            "project_name": "example-weather",
            "working_directory": "C:/ProgramData/example-weather/runtime",
            "files": ["C:/ProgramData/example-weather/runtime/docker-compose.yml"],
            "control_service": "example-airflow-api",
            "airflow_code_services": [
                "example-airflow-api",
                "example-airflow-scheduler",
                "example-airflow-dag-processor",
                "example-airflow-triggerer",
            ],
            "forbidden_data_services": ["example-postgres", "example-trino"],
        },
        "mounts": {
            "dags_host_path": "C:/ProgramData/example-weather/runtime/dags",
            "dags_container_path": "/opt/airflow/dags",
            "dbt_host_path": "C:/ProgramData/example-weather/runtime/dbt",
            "dbt_container_path": "/opt/airflow/dbt",
            "runtime_root": "C:/ProgramData/example-weather/runtime",
        },
        "airflow": {
            "dag_allowlist": dag_ids,
            "writer_dag_allowlist": dag_ids,
            "never_trigger": True,
        },
        "timeouts": {"drain_timeout_seconds": 1800, "poll_interval_seconds": 15},
        "local_state": {
            "ledger_directory": "C:/ProgramData/example-weather/runtime/ledger",
            "lock_file": "C:/ProgramData/example-weather/runtime/deploy.lock",
            "generated_overlay_file": "C:/ProgramData/example-weather/runtime/generated/main-deploy.override.yml",
        },
    }


def _load(tmp_path: Path, payload: dict[str, object]):
    target_path = tmp_path / "deploy-target.json"
    target_path.write_text(json.dumps(payload), encoding="utf-8")
    return load_deploy_target(target_path, repo_root=tmp_path)


def _load_with_repo_root(
    target_directory: Path, payload: dict[str, object], repo_root: Path
):
    target_path = target_directory / "deploy-target.json"
    target_path.write_text(json.dumps(payload), encoding="utf-8")
    return load_deploy_target(target_path, repo_root=repo_root)


def _load_at_path(target_path: Path, payload: dict[str, object], repo_root: Path):
    target_path.write_text(json.dumps(payload), encoding="utf-8")
    return load_deploy_target(target_path, repo_root=repo_root)


def _set_filesystem_paths(payload: dict[str, object], root: str) -> None:
    payload["compose"]["working_directory"] = root
    payload["compose"]["files"] = [f"{root}/docker-compose.yml"]
    payload["mounts"]["dags_host_path"] = f"{root}/dags"
    payload["mounts"]["dbt_host_path"] = f"{root}/dbt"
    payload["mounts"]["runtime_root"] = root
    payload["local_state"]["ledger_directory"] = f"{root}/ledger"
    payload["local_state"]["lock_file"] = f"{root}/deploy.lock"
    payload["local_state"]["generated_overlay_file"] = f"{root}/generated/main-deploy.override.yml"


def _set_path_field(
    payload: dict[str, object], section: str, field: str, value: str
) -> None:
    payload[section][field] = [value] if field == "files" else value


def test_deploy_target_requires_exact_weather_dag_allowlist(tmp_path: Path):
    payload = _valid_target()
    payload["airflow"]["dag_allowlist"] = sorted(EXPECTED_DAG_IDS - {"weather_w2_canonical_transform"})

    with pytest.raises(ValueError, match="dag_allowlist"):
        _load(tmp_path, payload)


def test_deploy_target_rejects_data_services_in_airflow_code_allowlist(tmp_path: Path):
    payload = _valid_target()
    payload["compose"]["airflow_code_services"].append("example-postgres")

    with pytest.raises(ValueError, match="forbidden"):
        _load(tmp_path, payload)


def test_deploy_target_rejects_airflow_init_in_normal_code_service_set(tmp_path: Path):
    payload = _valid_target()
    payload["compose"]["airflow_code_services"].append("example-airflow-init")

    with pytest.raises(ValueError, match="airflow-init"):
        _load(tmp_path, payload)


def test_deploy_target_rejects_mixed_case_airflow_init_in_code_service_set(
    tmp_path: Path,
):
    payload = _valid_target()
    payload["compose"]["airflow_code_services"].append("Example-Airflow-Init")

    assert not _schema_validator().is_valid(payload)
    with pytest.raises(ValueError, match="airflow-init"):
        _load(tmp_path, payload)


@pytest.mark.parametrize("service", ["bad:service", "bad#service", "bad\nservice"])
def test_schema_and_loader_reject_yaml_unsafe_service_names(
    tmp_path: Path, service: str
):
    payload = _valid_target()
    payload["compose"]["airflow_code_services"].append(service)

    assert not _schema_validator().is_valid(payload)
    with pytest.raises(ValueError, match="service"):
        _load(tmp_path, payload)


def test_deploy_target_requires_writer_subset(tmp_path: Path):
    payload = _valid_target()
    payload["airflow"]["writer_dag_allowlist"] = ["not-a-weather-dag"]

    with pytest.raises(ValueError, match="writer_dag_allowlist"):
        _load(tmp_path, payload)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload["compose"].__setitem__(
            "control_service", "missing-from-code-services"
        ),
        lambda payload: payload["compose"].__setitem__(
            "control_service", "example-postgres"
        ),
    ],
)
def test_deploy_target_requires_control_service_in_code_services_and_not_forbidden(
    tmp_path: Path, mutate
):
    payload = _valid_target()
    mutate(payload)

    with pytest.raises(ValueError, match="control_service"):
        _load(tmp_path, payload)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload, repo_root: payload["compose"].__setitem__(
            "working_directory", "relative/runtime"
        ),
        lambda payload, repo_root: payload["mounts"].__setitem__(
            "runtime_root", "relative/runtime"
        ),
        lambda payload, repo_root: payload["local_state"].__setitem__(
            "ledger_directory", str(repo_root / "ledger")
        ),
    ],
)
def test_deploy_target_rejects_relative_and_repository_ledger_paths(
    tmp_path: Path, mutate
):
    payload = _valid_target()
    mutate(payload, tmp_path)

    with pytest.raises(ValueError, match="path|ledger"):
        _load(tmp_path, payload)


def test_deploy_target_rejects_ledger_inside_relative_repository_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    repository = tmp_path / "repository"
    repository.mkdir()
    payload = _valid_target()
    payload["local_state"]["ledger_directory"] = str(repository / "ledger")
    payload["local_state"]["lock_file"] = str(repository / "deploy.lock")
    monkeypatch.chdir(repository)

    with pytest.raises(ValueError, match="ledger"):
        _load_with_repo_root(repository, payload, Path("."))


def test_deploy_target_rejects_ledger_inside_symlinked_repository_root(tmp_path: Path):
    repository = tmp_path / "repository"
    repository.mkdir()
    repository_alias = tmp_path / "repository-alias"
    try:
        repository_alias.symlink_to(repository, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"current OS cannot create symlink: {error}")
    payload = _valid_target()
    payload["local_state"]["ledger_directory"] = str(repository / "ledger")
    payload["local_state"]["lock_file"] = str(repository / "deploy.lock")

    with pytest.raises(ValueError, match="ledger"):
        _load_with_repo_root(tmp_path, payload, repository_alias)


@pytest.mark.parametrize(
    "root_path",
    [
        "C:/",
        "C:\\",
        "/",
        "C:/./",
        "C:\\.\\",
        "/./",
        "C:////",
        "/.//./",
    ],
)
@pytest.mark.parametrize(
    "section, field",
    [
        ("compose", "working_directory"),
        ("compose", "files"),
        ("mounts", "dags_host_path"),
        ("mounts", "dbt_host_path"),
        ("mounts", "runtime_root"),
        ("local_state", "ledger_directory"),
        ("local_state", "lock_file"),
        ("local_state", "generated_overlay_file"),
    ],
)
def test_schema_and_loader_reject_filesystem_roots(
    tmp_path: Path, section: str, field: str, root_path: str
):
    payload = _valid_target()
    _set_path_field(payload, section, field, root_path)

    assert not _schema_validator().is_valid(payload)
    with pytest.raises(ValueError, match="root"):
        _load(tmp_path, payload)


@pytest.mark.parametrize(
    "path_value",
    [
        "C:/ProgramData/example-weather//runtime/../ledger",
        "C:\\ProgramData\\example-weather\\\\runtime\\..\\ledger",
        "/var/lib/example-weather//runtime/../ledger",
    ],
)
@pytest.mark.parametrize(
    "section, field",
    [
        ("compose", "working_directory"),
        ("compose", "files"),
        ("mounts", "dags_host_path"),
        ("mounts", "dbt_host_path"),
        ("mounts", "runtime_root"),
        ("local_state", "ledger_directory"),
        ("local_state", "lock_file"),
        ("local_state", "generated_overlay_file"),
    ],
)
def test_schema_and_loader_reject_lexical_traversal_aliases(
    tmp_path: Path, section: str, field: str, path_value: str
):
    payload = _valid_target()
    _set_path_field(payload, section, field, path_value)

    assert not _schema_validator().is_valid(payload)
    with pytest.raises(ValueError, match="traversal"):
        _load(tmp_path, payload)


@pytest.mark.parametrize(
    "path_value",
    [
        "C:/ProgramData/example-weather/./runtime",
        "C:\\ProgramData\\example-weather\\.\\runtime",
        "/var/lib/example-weather/./runtime",
    ],
)
@pytest.mark.parametrize(
    "section, field",
    [
        ("compose", "working_directory"),
        ("compose", "files"),
        ("mounts", "dags_host_path"),
        ("mounts", "dbt_host_path"),
        ("mounts", "runtime_root"),
        ("local_state", "ledger_directory"),
        ("local_state", "lock_file"),
        ("local_state", "generated_overlay_file"),
    ],
)
def test_schema_and_loader_reject_dot_segment_aliases(
    tmp_path: Path, section: str, field: str, path_value: str
):
    payload = _valid_target()
    _set_path_field(payload, section, field, path_value)

    assert not _schema_validator().is_valid(payload)
    with pytest.raises(ValueError, match="traversal"):
        _load(tmp_path, payload)


def test_deploy_target_accepts_windows_paths_when_host_path_parser_is_posix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(target_module, "Path", PurePosixPath)

    assert _load(tmp_path, _valid_target()).runtime_root.as_posix() == (
        "C:/ProgramData/example-weather/runtime"
    )


def test_deploy_target_accepts_posix_absolute_paths(tmp_path: Path):
    payload = _valid_target()
    _set_filesystem_paths(payload, "/var/lib/example-weather/runtime")

    assert _load(tmp_path, payload).runtime_root.as_posix() == (
        "/var/lib/example-weather/runtime"
    )


@pytest.mark.parametrize("path_root", ["C:runtime", "relative/runtime", r"\\server\share\runtime"])
def test_deploy_target_rejects_non_absolute_or_unc_filesystem_paths(
    tmp_path: Path, path_root: str
):
    payload = _valid_target()
    _set_filesystem_paths(payload, path_root)

    with pytest.raises(ValueError, match="absolute|UNC"):
        _load(tmp_path, payload)


@pytest.mark.parametrize(
    "timeouts",
    [
        {"drain_timeout_seconds": 59, "poll_interval_seconds": 15},
        {"drain_timeout_seconds": 1800, "poll_interval_seconds": 61},
    ],
)
def test_deploy_target_requires_timeout_bounds(tmp_path: Path, timeouts: dict[str, int]):
    payload = _valid_target()
    payload["timeouts"] = timeouts

    with pytest.raises(ValueError, match="timeout|poll"):
        _load(tmp_path, payload)


def test_public_summary_contains_no_absolute_path_or_credential_reference(tmp_path: Path):
    target = _load(tmp_path, _valid_target())
    summary = public_target_summary(target)

    rendered = json.dumps(summary)
    assert "credential" not in rendered.lower()
    assert "C:/" not in json.dumps(summary)
    assert target.credential_reference not in rendered
    assert target.credential_source_kind not in rendered


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.__setitem__(
            "target_id", payload["credential_reference"]
        ),
        lambda payload: (
            payload["compose"].__setitem__(
                "control_service", payload["credential_source_kind"]
            ),
            payload["compose"]["airflow_code_services"].append(
                payload["credential_source_kind"]
            ),
        ),
    ],
)
def test_public_summary_rejects_embedded_credential_reference_or_source(
    tmp_path: Path, mutate
):
    payload = _valid_target()
    mutate(payload)

    with pytest.raises(SensitiveArtifactError):
        public_target_summary(_load(tmp_path, payload))


def test_target_fingerprint_does_not_change_when_exposed_raw_mapping_is_mutated(
    tmp_path: Path,
):
    target = _load(tmp_path, _valid_target())
    original = target_fingerprint(target)
    target.raw["target_id"] = "mutated-after-load"

    assert target_fingerprint(target) == original


def test_deploy_target_rejects_unknown_properties(tmp_path: Path):
    payload = _valid_target()
    payload["unexpected"] = "unsafe-to-ignore"

    with pytest.raises(ValueError, match="unexpected"):
        _load(tmp_path, payload)


def test_deploy_target_preserves_compose_file_override_order(tmp_path: Path):
    payload = _valid_target()
    payload["compose"]["files"] = [
        "C:/ProgramData/example-weather/runtime/base.yml",
        "C:/ProgramData/example-weather/runtime/override.yml",
    ]

    target = _load(tmp_path, payload)

    assert tuple(str(path).replace("\\", "/") for path in target.compose_files) == (
        "C:/ProgramData/example-weather/runtime/base.yml",
        "C:/ProgramData/example-weather/runtime/override.yml",
    )


def test_deploy_target_rejects_duplicate_compose_files(tmp_path: Path):
    payload = _valid_target()
    payload["compose"]["files"] = [
        "C:/ProgramData/example-weather/runtime/docker-compose.yml",
        "C:/ProgramData/example-weather/runtime/docker-compose.yml",
    ]

    with pytest.raises(ValueError, match="compose.files.*duplicate"):
        _load(tmp_path, payload)


@pytest.mark.parametrize(
    "mutate, match",
    [
        (
            lambda payload: payload["local_state"].pop("generated_overlay_file"),
            "generated_overlay_file",
        ),
        (
            lambda payload: payload["local_state"].__setitem__(
                "generated_overlay_file",
                payload["local_state"]["ledger_directory"],
            ),
            "generated_overlay_file.*distinct|local_state.*distinct",
        ),
        (
            lambda payload: payload["local_state"].__setitem__(
                "generated_overlay_file",
                payload["local_state"]["lock_file"],
            ),
            "generated_overlay_file.*distinct|local_state.*distinct",
        ),
        (
            lambda payload: payload["mounts"].__setitem__(
                "runtime_root",
                payload["local_state"]["generated_overlay_file"],
            ),
            "runtime_root.*distinct|local_state.*distinct",
        ),
    ],
)
def test_deploy_target_requires_generated_overlay_file_and_distinct_local_paths(
    tmp_path: Path, mutate, match: str
):
    payload = _valid_target()
    mutate(payload)

    assert not _schema_validator().is_valid(payload)
    with pytest.raises(ValueError, match=match):
        _load(tmp_path, payload)


def test_deploy_target_requires_generated_overlay_file_yaml_extension(tmp_path: Path):
    payload = _valid_target()
    payload["local_state"]["generated_overlay_file"] = (
        "C:/ProgramData/example-weather/runtime/generated/main-deploy.override.txt"
    )

    assert not _schema_validator().is_valid(payload)
    with pytest.raises(ValueError, match="generated_overlay_file"):
        _load(tmp_path, payload)


def test_deploy_target_requires_runtime_root_and_generated_overlay_outside_repository(
    tmp_path: Path,
):
    payload = _valid_target()
    payload["mounts"]["runtime_root"] = str(tmp_path / "runtime")
    payload["local_state"]["generated_overlay_file"] = str(tmp_path / "overlay.yml")

    with pytest.raises(ValueError, match="runtime_root|generated_overlay_file"):
        _load(tmp_path, payload)


@pytest.mark.parametrize("field", ["generated_overlay_file", "lock_file"])
def test_deploy_target_rejects_local_state_file_collision_with_compose_files(
    tmp_path: Path, field: str
):
    payload = _valid_target()
    payload["compose"]["files"] = [
        "C:/ProgramData/example-weather/runtime/base.yml",
        "C:/ProgramData/example-weather/runtime/override.yml",
    ]
    if field == "generated_overlay_file":
        payload["local_state"][field] = (
            "c:\\programdata\\example-weather\\runtime\\BASE.yml"
        )
    else:
        payload["compose"]["files"] = [
            "C:/ProgramData/example-weather/runtime/base.lock",
            "C:/ProgramData/example-weather/runtime/override.yml",
        ]
        payload["local_state"][field] = (
            "c:\\programdata\\example-weather\\runtime\\BASE.lock"
        )

    assert _schema_validator().is_valid(payload)
    with pytest.raises(ValueError, match=f"local_state.{field}.*compose.files"):
        _load(tmp_path, payload)


@pytest.mark.parametrize("field", ["generated_overlay_file", "lock_file"])
def test_deploy_target_rejects_local_state_file_collision_with_target_json_path(
    tmp_path: Path, field: str
):
    target_path = tmp_path / "runtime" / (
        "deploy-target.yml" if field == "generated_overlay_file" else "deploy-target.lock"
    )
    target_path.parent.mkdir()
    payload = _valid_target()
    root = str(target_path.parent).replace("\\", "/")
    _set_filesystem_paths(payload, root)
    payload["local_state"]["ledger_directory"] = f"{root}/ledger"
    payload["local_state"]["generated_overlay_file"] = f"{root}/overlay.yml"
    payload["local_state"]["lock_file"] = f"{root}/deploy.lock"
    payload["local_state"][field] = str(target_path)

    with pytest.raises(ValueError, match=f"local_state.{field}.*deploy target"):
        _load_at_path(target_path, payload, repo_root=tmp_path / "repository")


def test_deploy_target_allows_ledger_directory_equal_to_compose_file_parent_only(
    tmp_path: Path,
):
    payload = _valid_target()
    payload["compose"]["files"] = [
        "C:/ProgramData/example-weather/runtime/config/docker-compose.yml"
    ]
    payload["mounts"]["runtime_root"] = "C:/ProgramData/example-weather/runtime"
    payload["local_state"]["ledger_directory"] = (
        "C:/ProgramData/example-weather/runtime/config"
    )

    assert _load(tmp_path, payload).ledger_directory.as_posix() == (
        "C:/ProgramData/example-weather/runtime/config"
    )


@pytest.mark.parametrize(
    "field, resolved_name",
    [
        ("generated_overlay_file", "resolved-overlay.yml"),
        ("lock_file", "resolved-lock.lock"),
    ],
)
def test_deploy_target_rejects_native_resolved_alias_file_collisions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    resolved_name: str,
):
    native_root = tmp_path / "runtime"
    native_root.mkdir()
    target_path = native_root / "deploy-target.json"
    payload = _valid_target()
    root = str(native_root).replace("\\", "/")
    _set_filesystem_paths(payload, root)
    payload["compose"]["files"] = [f"{root}/compose-alias.yml"]
    payload["local_state"]["generated_overlay_file"] = f"{root}/overlay.yml"
    payload["local_state"]["lock_file"] = f"{root}/deploy.lock"
    payload["local_state"][field] = f"{root}/{resolved_name}"
    resolved_collision = native_root / "same-file"
    original_resolve = target_module.pathlib.Path.resolve

    def fake_resolve(self, strict=False):
        normalized = str(self).replace("\\", "/")
        if normalized.endswith("/compose-alias.yml") or normalized.endswith(f"/{resolved_name}"):
            return resolved_collision
        return original_resolve(self, strict=strict)

    monkeypatch.setattr(target_module.pathlib.Path, "resolve", fake_resolve)

    with pytest.raises(ValueError, match=f"local_state.{field}.*compose.files"):
        _load_at_path(target_path, payload, repo_root=tmp_path / "repository")


def test_deploy_target_rejects_native_resolved_alias_to_target_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    native_root = tmp_path / "runtime"
    native_root.mkdir()
    target_path = native_root / "deploy-target.yml"
    payload = _valid_target()
    root = str(native_root).replace("\\", "/")
    _set_filesystem_paths(payload, root)
    payload["local_state"]["generated_overlay_file"] = f"{root}/overlay.yml"
    payload["local_state"]["lock_file"] = f"{root}/deploy.lock"
    resolved_collision = native_root / "same-target-file"
    original_resolve = target_module.pathlib.Path.resolve

    def fake_resolve(self, strict=False):
        normalized = str(self).replace("\\", "/")
        if normalized.endswith("/overlay.yml") or normalized.endswith("/deploy-target.yml"):
            return resolved_collision
        return original_resolve(self, strict=strict)

    monkeypatch.setattr(target_module.pathlib.Path, "resolve", fake_resolve)

    with pytest.raises(ValueError, match="local_state.generated_overlay_file.*deploy target"):
        _load_at_path(target_path, payload, repo_root=tmp_path / "repository")


@pytest.mark.parametrize(
    "files",
    [
        [
            "C:/ProgramData/Example-Weather/runtime/docker-compose.yml",
            "c:\\programdata\\example-weather\\runtime\\docker-compose.yml",
        ],
        [
            "/var/lib/example-weather//runtime/docker-compose.yml",
            "/var/lib/example-weather/runtime/docker-compose.yml",
        ],
    ],
)
def test_loader_rejects_normalized_compose_file_alias_duplicates(
    tmp_path: Path, files: list[str]
):
    payload = _valid_target()
    payload["compose"]["files"] = files

    assert _schema_validator().is_valid(payload)
    with pytest.raises(ValueError, match="compose.files.*duplicate"):
        _load(tmp_path, payload)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload["compose"].__setitem__(
            "working_directory", "C:/ProgramData/example-weather/../runtime"
        ),
        lambda payload: payload["compose"].__setitem__(
            "files", ["C:/ProgramData/example-weather/runtime/../docker-compose.yml"]
        ),
        lambda payload: payload["mounts"].__setitem__(
            "dags_host_path", "C:/ProgramData/example-weather/runtime/../dags"
        ),
        lambda payload: payload["mounts"].__setitem__(
            "dbt_host_path", "C:/ProgramData/example-weather/runtime/../dbt"
        ),
        lambda payload: payload["mounts"].__setitem__(
            "runtime_root", "C:/ProgramData/example-weather/../runtime"
        ),
        lambda payload: payload["local_state"].__setitem__(
            "ledger_directory", "C:/ProgramData/example-weather/runtime/../ledger"
        ),
        lambda payload: payload["local_state"].__setitem__(
            "lock_file", "C:/ProgramData/example-weather/runtime/../deploy.lock"
        ),
        lambda payload: payload["local_state"].__setitem__(
            "generated_overlay_file",
            "C:/ProgramData/example-weather/runtime/../generated/main-deploy.override.yml",
        ),
    ],
)
def test_deploy_target_rejects_lexical_parent_traversal_in_filesystem_paths(
    tmp_path: Path, mutate
):
    payload = _valid_target()
    mutate(payload)

    with pytest.raises(ValueError, match="traversal"):
        _load(tmp_path, payload)


@pytest.mark.parametrize(
    "field, value",
    [
        ("dags_container_path", "/unexpected/dags"),
        ("dbt_container_path", "/unexpected/dbt"),
    ],
)
def test_deploy_target_requires_fixed_container_seams(
    tmp_path: Path, field: str, value: str
):
    payload = _valid_target()
    payload["mounts"][field] = value

    with pytest.raises(ValueError, match=field):
        _load(tmp_path, payload)


def _schema_validator():
    schema_path = Path("runtime/deploy-target.schema.json")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload["airflow"].__setitem__(
            "dag_allowlist",
            [
                "arbitrary-dag-1",
                "arbitrary-dag-2",
                "arbitrary-dag-3",
                "arbitrary-dag-4",
                "arbitrary-dag-5",
                "arbitrary-dag-6",
                "arbitrary-dag-7",
                "arbitrary-dag-8",
                "arbitrary-dag-9",
                "arbitrary-dag-10",
            ],
        ),
        lambda payload: payload["airflow"].__setitem__(
            "writer_dag_allowlist", ["arbitrary-writer-dag"]
        ),
        lambda payload: payload["mounts"].__setitem__(
            "runtime_root", "relative/runtime"
        ),
        lambda payload: payload["mounts"].__setitem__(
            "dags_container_path", "/not/opt/airflow/dags"
        ),
    ],
)
def test_schema_rejects_invalid_dag_path_and_container_contracts(mutate):
    payload = _valid_target()
    mutate(payload)

    assert not _schema_validator().is_valid(payload)


def test_schema_accepts_non_empty_writer_subset_of_weather_dags():
    payload = _valid_target()
    payload["airflow"]["writer_dag_allowlist"] = ["weather_serving_export"]

    assert _schema_validator().is_valid(payload)


def test_schema_accepts_a_new_weather_dag_without_editing_the_schema():
    """DAG 를 추가할 때 schema 를 손대지 않아도 되도록, 권위 있는 DAG 집합은
    코드의 EXPECTED_DAG_IDS 하나로 일원화했다. schema 는 구조적 sanity 만 본다.
    실제 "그 DAG 여야 함"강제는 load_deploy_target 의 exact-match 가 담당한다
    (test_deploy_target_requires_exact_weather_dag_allowlist).
    """
    payload = _valid_target()
    payload["airflow"]["dag_allowlist"] = sorted(
        set(payload["airflow"]["dag_allowlist"]) | {"weather_future_new_dag"}
    )
    payload["airflow"]["writer_dag_allowlist"] = ["weather_future_new_dag"]

    assert _schema_validator().is_valid(payload)


@pytest.mark.parametrize(
    "bad_dag_id",
    ["not-a-weather-dag", "traffic_incident_bronze", "/opt/airflow/x", "Weather_Upper", ""],
)
def test_schema_still_rejects_structurally_invalid_dag_ids(bad_dag_id):
    payload = _valid_target()
    payload["airflow"]["dag_allowlist"] = [bad_dag_id]

    assert not _schema_validator().is_valid(payload)


def test_schema_accepts_posix_absolute_paths():
    payload = _valid_target()
    _set_filesystem_paths(payload, "/var/lib/example-weather/runtime")

    assert _schema_validator().is_valid(payload)
