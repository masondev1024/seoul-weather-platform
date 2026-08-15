from __future__ import annotations

import hashlib
import errno
from pathlib import Path, PurePosixPath

import pytest
import yaml

import deployment.overlay as overlay_module
from deployment.overlay import (
    AtomicOverlayStore,
    render_baseline_overlay,
    render_release_overlay,
    validate_overlay_content,
)
from tests.deploy.test_release_inventory import _target


def test_release_overlay_is_deterministic_read_only_with_candidate_artifact_root(
    tmp_path: Path,
):
    target = _target(tmp_path)
    sha = "a" * 40
    checkout = target.runtime_root / "releases" / sha

    artifact = render_release_overlay(target, checkout, sha)

    assert artifact.kind == "release"
    assert artifact.candidate_sha == sha
    assert artifact.sha256 == hashlib.sha256(artifact.content).hexdigest()
    assert artifact.content.endswith(b"\n")
    assert b"!override" not in artifact.content
    assert artifact.content == render_release_overlay(target, checkout, sha).content
    parsed = yaml.safe_load(artifact.content)
    assert set(parsed) == {"services"}
    assert list(parsed["services"]) == sorted(target.airflow_code_services)
    for service in sorted(target.airflow_code_services):
        assert parsed["services"][service] == {
            "volumes": [
                {
                    "type": "bind",
                    "source": f"{checkout}/dags".replace("\\", "/"),
                    "target": "/opt/airflow/dags",
                    "read_only": True,
                },
                {
                    "type": "bind",
                    "source": f"{checkout}/dbt".replace("\\", "/"),
                    "target": "/opt/airflow/dbt",
                    "read_only": True,
                },
            ],
            "environment": {
                "ASK_SEOUL_DBT_ARTIFACT_ROOT": (
                    f"/opt/airflow/logs/weather-dbt/releases/{sha}"
                )
            },
        }


def test_baseline_overlay_keeps_org_dbt_write_access_without_release_environment(
    tmp_path: Path,
):
    target = _target(tmp_path)

    artifact = render_baseline_overlay(target)

    assert artifact.kind == "baseline"
    assert artifact.candidate_sha is None
    assert artifact.sha256 == hashlib.sha256(artifact.content).hexdigest()
    assert artifact.content == render_baseline_overlay(target).content
    parsed = yaml.safe_load(artifact.content)
    assert list(parsed["services"]) == sorted(target.airflow_code_services)
    for service in sorted(target.airflow_code_services):
        assert parsed["services"][service] == {
            "volumes": [
                {
                    "type": "bind",
                    "source": str(target.dags_host_path).replace("\\", "/"),
                    "target": "/opt/airflow/dags",
                    "read_only": True,
                },
                {
                    "type": "bind",
                    "source": str(target.dbt_host_path).replace("\\", "/"),
                    "target": "/opt/airflow/dbt",
                    "read_only": False,
                },
            ]
        }

    validated = validate_overlay_content(target, artifact.content, artifact.sha256)
    assert validated.kind == "baseline"
    assert validated.candidate_sha is None


def test_release_overlay_requires_checkout_under_runtime_release_root(tmp_path: Path):
    target = _target(tmp_path)

    with pytest.raises(ValueError, match="checkout_root"):
        render_release_overlay(target, PurePosixPath("/elsewhere/release"), "a" * 40)


def test_validate_overlay_rejects_duplicate_keys_aliases_and_checksum_mismatch(tmp_path: Path):
    target = _target(tmp_path)
    content = b"services:\n  example-airflow-api: &svc\n    volumes: []\n  example-airflow-api: *svc\n"

    with pytest.raises(ValueError, match="duplicate|alias"):
        validate_overlay_content(target, content, hashlib.sha256(content).hexdigest())

    artifact = render_baseline_overlay(target)
    with pytest.raises(ValueError, match="checksum"):
        validate_overlay_content(target, artifact.content, "0" * 64)


def test_atomic_overlay_store_installs_restores_and_discards_only_staged_file(tmp_path: Path):
    target = _target(tmp_path)
    target = target.__class__(
        **{**target.__dict__, "generated_overlay_file": PurePosixPath(str(tmp_path / "overlay.yml"))}
    )
    store = AtomicOverlayStore(target)
    release = render_release_overlay(
        target,
        target.runtime_root / "releases" / ("b" * 40),
        "b" * 40,
    )
    baseline = render_baseline_overlay(target)

    staged = store.stage(release)
    assert staged.exists()
    store.install(staged, release)
    assert Path(str(target.generated_overlay_file)).read_bytes() == release.content
    store.restore(baseline.content, baseline.sha256)
    assert Path(str(target.generated_overlay_file)).read_bytes() == baseline.content
    staged = store.stage(release)
    store.discard(staged)
    assert not staged.exists()


def test_atomic_overlay_store_install_rejects_destination_readback_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    target = _target(tmp_path)
    target = target.__class__(
        **{**target.__dict__, "generated_overlay_file": PurePosixPath(str(tmp_path / "overlay.yml"))}
    )
    store = AtomicOverlayStore(target)
    artifact = render_baseline_overlay(target)
    staged = store.stage(artifact)

    def corrupting_replace(src, dst):
        Path(dst).write_bytes(b"corrupted overlay bytes\n")
        Path(src).unlink()

    monkeypatch.setattr(overlay_module.os, "replace", corrupting_replace)

    with pytest.raises(ValueError, match="installed overlay verification failed") as error:
        store.install(staged, artifact)

    rendered_error = str(error.value)
    assert str(target.generated_overlay_file) not in rendered_error
    assert "corrupted" not in rendered_error


def test_validate_overlay_rejects_forbidden_service(tmp_path: Path):
    target = _target(tmp_path)
    artifact = render_baseline_overlay(target)
    content = artifact.content.replace(b"services:\n", b"services:\n  example-postgres:\n    volumes: []\n", 1)

    with pytest.raises(ValueError, match="service"):
        validate_overlay_content(target, content, hashlib.sha256(content).hexdigest())


def test_validate_overlay_binds_sources_to_exact_baseline_or_release_pair(tmp_path: Path):
    target = _target(tmp_path)
    release = render_release_overlay(
        target,
        target.runtime_root / "releases" / ("c" * 40),
        "c" * 40,
    )

    validated = validate_overlay_content(target, release.content, release.sha256)

    assert validated.kind == "release"
    assert validated.candidate_sha == "c" * 40

    off_root = release.content.replace(
        b"C:/ProgramData/example-weather/runtime/releases/cccccccccccccccccccccccccccccccccccccccc/dags",
        b"C:/ProgramData/example-weather/other/dags",
    )
    with pytest.raises(ValueError, match="source"):
        validate_overlay_content(target, off_root, hashlib.sha256(off_root).hexdigest())

    mixture = release.content.replace(
        b"C:/ProgramData/example-weather/runtime/releases/cccccccccccccccccccccccccccccccccccccccc/dbt",
        b"C:/ProgramData/example-weather/runtime/dbt",
    )
    with pytest.raises(ValueError, match="source"):
        validate_overlay_content(target, mixture, hashlib.sha256(mixture).hexdigest())


@pytest.mark.parametrize(
    "kind,drift",
    [
        ("release", "missing_environment"),
        ("release", "environment_candidate_path"),
        ("release", "environment_extra_key"),
        ("release", "environment_credential"),
        ("release", "dbt_read_write"),
        ("baseline", "release_environment"),
        ("baseline", "dbt_read_only"),
        ("baseline", "dags_read_write"),
        ("baseline", "service_extra_key"),
        ("baseline", "volume_extra_key"),
        ("baseline", "volume_target"),
    ],
)
def test_validate_overlay_rejects_kind_specific_shape_drift(
    tmp_path: Path, kind: str, drift: str
):
    target = _target(tmp_path)
    sha = "d" * 40
    artifact = (
        render_release_overlay(target, target.runtime_root / "releases" / sha, sha)
        if kind == "release"
        else render_baseline_overlay(target)
    )
    parsed = yaml.safe_load(artifact.content)
    service = sorted(target.airflow_code_services)[0]
    service_body = parsed["services"][service]
    expected_release_environment = {
        "ASK_SEOUL_DBT_ARTIFACT_ROOT": (
            "/opt/airflow/logs/weather-dbt/releases/" + sha
        )
    }
    if kind == "release":
        service_body.setdefault("environment", expected_release_environment)

    if drift == "missing_environment":
        service_body.pop("environment", None)
    elif drift == "environment_candidate_path":
        service_body["environment"] = {
            "ASK_SEOUL_DBT_ARTIFACT_ROOT": (
                "/opt/airflow/logs/weather-dbt/releases/" + "e" * 40
            )
        }
    elif drift == "environment_extra_key":
        service_body["environment"]["SAFE_EXTRA"] = "unexpected"
    elif drift == "environment_credential":
        service_body["environment"]["PASSWORD"] = "test-only-sensitive-value"
    elif drift == "dbt_read_write":
        service_body["volumes"][1]["read_only"] = False
    elif drift == "release_environment":
        service_body["environment"] = expected_release_environment
    elif drift == "dbt_read_only":
        service_body["volumes"][1]["read_only"] = True
    elif drift == "dags_read_write":
        service_body["volumes"][0]["read_only"] = False
    elif drift == "service_extra_key":
        service_body["labels"] = {"safe": "but-not-allowed"}
    elif drift == "volume_extra_key":
        service_body["volumes"][0]["consistency"] = "cached"
    elif drift == "volume_target":
        service_body["volumes"][1]["target"] = "/opt/airflow/not-dbt"
    else:  # pragma: no cover - parametrization is exhaustive.
        raise AssertionError(f"unknown drift: {drift}")

    content = yaml.safe_dump(
        parsed,
        allow_unicode=False,
        default_flow_style=False,
        sort_keys=False,
        width=4096,
    ).encode("utf-8")

    with pytest.raises(ValueError) as error:
        validate_overlay_content(target, content, hashlib.sha256(content).hexdigest())

    assert "test-only-sensitive-value" not in str(error.value)


@pytest.mark.parametrize("location", ["service", "source"])
def test_validate_overlay_rejects_credential_markers(
    tmp_path: Path, location: str
):
    target = _target(tmp_path)
    artifact = render_baseline_overlay(target)
    parsed = yaml.safe_load(artifact.content)

    if location == "service":
        original_service = sorted(target.airflow_code_services)[0]
        credential_service = "example-airflow-token"
        parsed["services"] = {
            credential_service: parsed["services"][original_service]
        }
        target = target.__class__(
            **{
                **target.__dict__,
                "airflow_code_services": frozenset({credential_service}),
                "control_service": credential_service,
            }
        )
    else:
        service = sorted(target.airflow_code_services)[0]
        parsed["services"][service]["volumes"][0]["source"] = (
            "C:/ProgramData/example-weather/token/dags"
        )

    content = yaml.safe_dump(
        parsed,
        allow_unicode=False,
        default_flow_style=False,
        sort_keys=False,
        width=4096,
    ).encode("utf-8")

    with pytest.raises(ValueError, match="service|source"):
        validate_overlay_content(target, content, hashlib.sha256(content).hexdigest())


@pytest.mark.parametrize(
    "content",
    [
        b"\xff",
        b"? [services]\n: rejected\n",
    ],
)
def test_validate_overlay_sanitizes_malformed_yaml_failures(
    tmp_path: Path, content: bytes
):
    target = _target(tmp_path)

    with pytest.raises(ValueError, match="invalid overlay yaml"):
        validate_overlay_content(target, content, hashlib.sha256(content).hexdigest())


@pytest.mark.parametrize("service", ["bad:service", "bad#service", "bad\nservice"])
def test_overlay_render_rejects_yaml_injection_service_names(tmp_path: Path, service: str):
    target = _target(tmp_path)
    target = target.__class__(**{**target.__dict__, "airflow_code_services": frozenset({service})})

    with pytest.raises(ValueError, match="service"):
        render_baseline_overlay(target)


def test_overlay_store_rejects_unknown_staged_path_without_deleting(tmp_path: Path):
    target = _target(tmp_path)
    target = target.__class__(
        **{**target.__dict__, "generated_overlay_file": PurePosixPath(str(tmp_path / "overlay.yml"))}
    )
    store = AtomicOverlayStore(target)
    victim = tmp_path / "victim.tmp"
    victim.write_text("do-not-touch", encoding="utf-8")
    artifact = render_baseline_overlay(target)

    with pytest.raises(ValueError, match="staged"):
        store.install(victim, artifact)
    with pytest.raises(ValueError, match="staged"):
        store.discard(victim)

    assert victim.read_text(encoding="utf-8") == "do-not-touch"


def test_fsync_directory_ignores_documented_unsupported_fsync_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    calls: list[int] = []

    monkeypatch.setattr(overlay_module.os, "open", lambda path, flags: 123)

    def fake_fsync(fd: int) -> None:
        calls.append(fd)
        raise OSError(errno.EINVAL, "unsupported directory fsync")

    monkeypatch.setattr(overlay_module.os, "fsync", fake_fsync)
    monkeypatch.setattr(overlay_module.os, "close", lambda fd: None)

    overlay_module._fsync_directory(tmp_path)

    assert calls == [123]


@pytest.mark.parametrize("error_number", [errno.EACCES, errno.EPERM])
def test_fsync_directory_propagates_permission_errors_after_open(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, error_number: int
):
    monkeypatch.setattr(overlay_module.os, "open", lambda path, flags: 123)
    monkeypatch.setattr(
        overlay_module.os,
        "fsync",
        lambda fd: (_ for _ in ()).throw(OSError(error_number, "permission")),
    )
    monkeypatch.setattr(overlay_module.os, "close", lambda fd: None)

    with pytest.raises(OSError):
        overlay_module._fsync_directory(tmp_path)


@pytest.mark.parametrize("error_number", [errno.EACCES, errno.EPERM, errno.EINVAL])
def test_fsync_directory_still_ignores_documented_open_time_unsupported_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, error_number: int
):
    monkeypatch.setattr(
        overlay_module.os,
        "open",
        lambda path, flags: (_ for _ in ()).throw(OSError(error_number, "unsupported")),
    )

    overlay_module._fsync_directory(tmp_path)


def test_fsync_directory_fails_closed_for_unexpected_fsync_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    monkeypatch.setattr(overlay_module.os, "open", lambda path, flags: 123)
    monkeypatch.setattr(
        overlay_module.os,
        "fsync",
        lambda fd: (_ for _ in ()).throw(OSError(errno.EIO, "io")),
    )
    monkeypatch.setattr(overlay_module.os, "close", lambda fd: None)

    with pytest.raises(OSError):
        overlay_module._fsync_directory(tmp_path)
