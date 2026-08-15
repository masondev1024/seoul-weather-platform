from __future__ import annotations

import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CompletedCommand:
    stdout: str
    stderr: str
    returncode: int


class CommandExecutionError(RuntimeError):
    """A redacted category for a local command execution failure."""


class CommandRunner:
    """Execute one argv command without a shell for local deployment adapters."""

    def __init__(self, *, timeout_seconds: int = 30) -> None:
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 3600:
            raise ValueError("command_timeout_invalid")
        self.timeout_seconds = timeout_seconds

    def run(self, argv: Sequence[str], cwd: Path) -> CompletedCommand:
        try:
            completed = subprocess.run(
                list(argv),
                cwd=cwd,
                shell=False,
                capture_output=True,
                text=True,
                check=False,
                timeout=self.timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            raise CommandExecutionError("command_timeout") from None
        except (OSError, TypeError, ValueError, subprocess.SubprocessError):
            raise CommandExecutionError("command_execution_failed") from None
        return CompletedCommand(
            stdout=completed.stdout,
            stderr=completed.stderr,
            returncode=completed.returncode,
        )
