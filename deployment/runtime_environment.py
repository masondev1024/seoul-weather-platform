from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from deployment.target import DeployTarget


class RuntimeEnvironmentError(RuntimeError):
    """A redacted category for an unusable runner-local runtime reference."""


@dataclass(frozen=True)
class RuntimeEnvironmentProof:
    compose_environment_ready: bool


def _has_link_or_junction(path: Path) -> bool:
    current = path
    checked: set[Path] = set()
    while current not in checked:
        checked.add(current)
        if current.exists():
            if current.is_symlink():
                return True
            is_junction = getattr(current, "is_junction", None)
            if callable(is_junction) and is_junction():
                return True
        if current == current.parent:
            return False
        current = current.parent
    return True


def validate_runtime_environment(
    target: DeployTarget, repo_root: Path
) -> RuntimeEnvironmentProof:
    """Validate runner-local Compose credential references without reading them."""
    if target.credential_source_kind != "existing_local_env":
        return RuntimeEnvironmentProof(compose_environment_ready=True)
    try:
        compose_raw = os.environ.get("COMPOSE_ENV_FILES", "")
        prod_raw = os.environ.get("ASK_SEOUL_PROD_ENV_FILE", "")
        if (
            not compose_raw
            or not prod_raw
            or compose_raw != prod_raw
            or any(marker in compose_raw for marker in (",", "\r", "\n", "\x00"))
        ):
            raise ValueError
        reference = Path(compose_raw)
        if (
            not reference.is_absolute()
            or ".." in reference.parts
            or not reference.is_file()
            or _has_link_or_junction(reference)
        ):
            raise ValueError
        resolved = reference.resolve(strict=True)
        repository = repo_root.resolve(strict=False)
        try:
            resolved.relative_to(repository)
        except ValueError:
            pass
        else:
            raise ValueError
    except (OSError, RuntimeError, TypeError, ValueError):
        raise RuntimeEnvironmentError("runtime_environment_invalid") from None
    return RuntimeEnvironmentProof(compose_environment_ready=True)
