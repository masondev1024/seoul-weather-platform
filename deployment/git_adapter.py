from __future__ import annotations

import os
import re
import shutil
import tempfile
from collections.abc import Sequence
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath

from deployment.command import CommandRunner, CompletedCommand
from deployment.target import DeployTarget


class GitAdapterError(RuntimeError):
    """A fixed, redacted local Git checkout failure category."""


_SHA40 = re.compile(r"^[0-9a-f]{40}$")
_REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_UNSAFE = re.compile(r"[|;&`$<>\x00-\x1f\x7f]")


def _safe_atom(value: object) -> str:
    if type(value) is not str or not value or value.startswith("-") or _UNSAFE.search(value):
        raise GitAdapterError("git_adapter_input_rejected")
    return value


def _safe_path(value: PurePath | Path) -> Path:
    raw = _safe_atom(str(value))
    parts = re.split(r"[\\/]", raw)
    if "." in parts or ".." in parts:
        raise GitAdapterError("git_adapter_input_rejected")
    if not PureWindowsPath(raw).is_absolute() and not PurePosixPath(raw).is_absolute():
        raise GitAdapterError("git_adapter_input_rejected")
    return Path(raw)


class GitCommandAdapter:
    def __init__(
        self,
        target: DeployTarget,
        source_checkout_root: PurePath,
        runner: CommandRunner,
    ) -> None:
        self._target = target
        self._runner = runner
        self._runtime = _safe_path(target.runtime_root)
        self._source = _safe_path(source_checkout_root)
        if not self._source.is_dir() or not (self._source / ".git").exists():
            raise GitAdapterError("git_adapter_input_rejected")

    def _checked(self, argv: Sequence[str], cwd: Path) -> str:
        try:
            result: CompletedCommand = self._runner.run(argv, cwd)
        except Exception:
            raise GitAdapterError("git_adapter_command_failed") from None
        if result.returncode != 0 or result.stderr:
            raise GitAdapterError("git_adapter_command_failed")
        return result.stdout

    @staticmethod
    def _origin_repository(origin: str) -> str | None:
        value = origin.strip()
        patterns = (
            r"https://github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?)(?:\.git)?$",
            r"git@github\.com:([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?)(?:\.git)?$",
            r"ssh://git@github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?)(?:\.git)?$",
        )
        for pattern in patterns:
            match = re.fullmatch(pattern, value)
            if match:
                return match.group(1)
        return None

    def _verify_source(self, repository: str, candidate_sha: str) -> None:
        head = self._checked(("git", "rev-parse", "HEAD"), self._source).strip()
        origin = self._checked(
            ("git", "config", "--get", "remote.origin.url"), self._source
        )
        if head != candidate_sha or self._origin_repository(origin) != repository:
            raise GitAdapterError("git_adapter_source_rejected")

    def _verify_release(self, path: Path, candidate_sha: str) -> None:
        try:
            if path.is_symlink() or not path.is_dir() or not (path / ".git").is_dir():
                raise ValueError
            head = self._checked(("git", "rev-parse", "HEAD"), path).strip()
            origin = self._checked(
                ("git", "config", "--get", "remote.origin.url"), path
            ).strip()
            status = self._checked(("git", "status", "--porcelain"), path)
            if head != candidate_sha or status != "":
                raise ValueError
            if Path(origin).resolve(strict=False) != self._source.resolve(strict=False):
                raise ValueError
            if not (path / "dags").is_dir() or not (path / "dbt").is_dir():
                raise ValueError
        except GitAdapterError:
            raise
        except Exception:
            raise GitAdapterError("git_adapter_release_rejected") from None

    def _cleanup_temp(self, temp: Path, parent: Path, candidate_sha: str) -> None:
        try:
            if (
                temp.parent.resolve(strict=False) != parent.resolve(strict=False)
                or not temp.name.startswith(f".{candidate_sha}.")
                or not temp.name.endswith(".tmp")
            ):
                return
            if temp.is_symlink():
                temp.unlink(missing_ok=True)
            elif temp.exists():
                shutil.rmtree(temp)
        except OSError:
            return

    def detached_checkout(
        self, repository: str, candidate_sha: str, checkout_root: PurePath
    ) -> PurePath:
        if (
            type(repository) is not str
            or _REPOSITORY.fullmatch(repository) is None
            or type(candidate_sha) is not str
            or _SHA40.fullmatch(candidate_sha) is None
        ):
            raise GitAdapterError("git_adapter_input_rejected")
        destination = _safe_path(checkout_root)
        expected = self._runtime / "releases" / candidate_sha
        if destination.resolve(strict=False) != expected.resolve(strict=False):
            raise GitAdapterError("git_adapter_input_rejected")

        self._verify_source(repository, candidate_sha)
        if destination.exists():
            self._verify_release(destination, candidate_sha)
            return checkout_root

        parent = expected.parent
        parent.mkdir(parents=True, exist_ok=True)
        temp = Path(
            tempfile.mkdtemp(
                prefix=f".{candidate_sha}.", suffix=".tmp", dir=parent
            )
        )
        try:
            self._checked(
                (
                    "git",
                    "clone",
                    "--local",
                    "--no-hardlinks",
                    "--no-checkout",
                    str(self._source),
                    str(temp),
                ),
                parent,
            )
            self._checked(("git", "checkout", "--detach", candidate_sha), temp)
            self._verify_release(temp, candidate_sha)
            if destination.exists():
                raise GitAdapterError("git_adapter_release_rejected")
            os.replace(temp, destination)
            self._verify_release(destination, candidate_sha)
            return checkout_root
        except GitAdapterError:
            raise
        except Exception:
            raise GitAdapterError("git_adapter_release_rejected") from None
        finally:
            self._cleanup_temp(temp, parent, candidate_sha)
