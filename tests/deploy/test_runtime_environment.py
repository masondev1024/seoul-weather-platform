from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from deployment.runtime_environment import (
    RuntimeEnvironmentError,
    validate_runtime_environment,
)
from tests.deploy.test_release_inventory import _target


def _existing_env_target(tmp_path: Path):
    return replace(_target(tmp_path), credential_source_kind="existing_local_env")


def test_existing_local_env_requires_same_single_external_regular_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    env_file = tmp_path / "runtime" / "compose.env"
    env_file.parent.mkdir()
    env_file.write_text("not-read-by-validator", encoding="utf-8")
    target = _existing_env_target(tmp_path)
    monkeypatch.setenv("COMPOSE_ENV_FILES", str(env_file))
    monkeypatch.setenv("ASK_SEOUL_PROD_ENV_FILE", str(env_file))

    proof = validate_runtime_environment(target, repo_root)

    assert proof.compose_environment_ready is True
    assert not hasattr(proof, "path")


@pytest.mark.parametrize(
    "case",
    [
        "missing-compose",
        "missing-prod",
        "multiple",
        "mismatch",
        "relative",
        "missing-file",
        "directory",
        "inside-repository",
    ],
)
def test_existing_local_env_rejects_unusable_or_ambiguous_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    first = tmp_path / "runtime" / "first.env"
    second = tmp_path / "runtime" / "second.env"
    first.parent.mkdir()
    first.write_text("one", encoding="utf-8")
    second.write_text("two", encoding="utf-8")
    target = _existing_env_target(tmp_path)
    monkeypatch.setenv("COMPOSE_ENV_FILES", str(first))
    monkeypatch.setenv("ASK_SEOUL_PROD_ENV_FILE", str(first))
    if case == "missing-compose":
        monkeypatch.delenv("COMPOSE_ENV_FILES")
    elif case == "missing-prod":
        monkeypatch.delenv("ASK_SEOUL_PROD_ENV_FILE")
    elif case == "multiple":
        monkeypatch.setenv("COMPOSE_ENV_FILES", f"{first},{second}")
    elif case == "mismatch":
        monkeypatch.setenv("ASK_SEOUL_PROD_ENV_FILE", str(second))
    elif case == "relative":
        monkeypatch.setenv("COMPOSE_ENV_FILES", "relative.env")
        monkeypatch.setenv("ASK_SEOUL_PROD_ENV_FILE", "relative.env")
    elif case == "missing-file":
        missing = tmp_path / "runtime" / "missing.env"
        monkeypatch.setenv("COMPOSE_ENV_FILES", str(missing))
        monkeypatch.setenv("ASK_SEOUL_PROD_ENV_FILE", str(missing))
    elif case == "directory":
        monkeypatch.setenv("COMPOSE_ENV_FILES", str(first.parent))
        monkeypatch.setenv("ASK_SEOUL_PROD_ENV_FILE", str(first.parent))
    else:
        inside = repo_root / "private.env"
        inside.write_text("private", encoding="utf-8")
        monkeypatch.setenv("COMPOSE_ENV_FILES", str(inside))
        monkeypatch.setenv("ASK_SEOUL_PROD_ENV_FILE", str(inside))

    with pytest.raises(RuntimeEnvironmentError, match="^runtime_environment_invalid$"):
        validate_runtime_environment(target, repo_root)


def test_non_existing_local_env_does_not_read_process_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _target(tmp_path)
    monkeypatch.delenv("COMPOSE_ENV_FILES", raising=False)
    monkeypatch.delenv("ASK_SEOUL_PROD_ENV_FILE", raising=False)

    proof = validate_runtime_environment(target, tmp_path / "repo")

    assert proof.compose_environment_ready is True
