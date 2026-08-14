from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable


FORBIDDEN_PARTS = frozenset(
    {
        ".omc",
        ".omx",
        ".pytest_cache",
        "__pycache__",
        "dbt_packages",
        "logs",
        "target",
    }
)
FORBIDDEN_BASENAMES = frozenset({"LessonRun.md", "engineering-decision-log.md"})

SECRET_RULES = (
    ("marketplace_api_key", re.compile(r"\bask_[A-Fa-f0-9]{24,}\b")),
    (
        "private_key",
        re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    ),
    (
        "bearer_token",
        re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~-]{24,}\b"),
    ),
    (
        "assigned_credential",
        re.compile(
            r"(?i)\b(?:serviceKey|api[_-]?key|access[_-]?key|secret[_-]?key|token)"
            r"\s*[:=]\s*[\"']?(?!\{\{|\$\{|<)[A-Za-z0-9+/=_-]{24,}"
        ),
    ),
)


@dataclass(frozen=True)
class SecretFinding:
    path: str
    line: int
    rule: str
    summary: str


def _normalized(path: str) -> PurePosixPath:
    return PurePosixPath(path.replace("\\", "/"))


def _is_forbidden(path: str) -> bool:
    normalized = _normalized(path)
    if normalized.name in FORBIDDEN_BASENAMES:
        return True
    if normalized.name == ".env" or normalized.name.startswith(".env."):
        return True
    return any(part in FORBIDDEN_PARTS for part in normalized.parts)


def find_forbidden_paths(paths: Iterable[str]) -> list[str]:
    return [path for path in paths if _is_forbidden(path)]


def find_secret_candidates(repo_root: Path, paths: Iterable[str]) -> list[SecretFinding]:
    root = repo_root.resolve()
    findings: list[SecretFinding] = []
    for relative_path in paths:
        candidate = (root / Path(relative_path)).resolve()
        if not candidate.is_relative_to(root) or not candidate.is_file():
            continue
        normalized_path = _normalized(relative_path).as_posix()
        try:
            content = candidate.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            findings.append(
                SecretFinding(
                    path=normalized_path,
                    line=0,
                    rule="invalid_utf8",
                    summary="candidate file is not valid UTF-8",
                )
            )
            continue
        except OSError:
            findings.append(
                SecretFinding(
                    path=normalized_path,
                    line=0,
                    rule="file_read_error",
                    summary="candidate file could not be read",
                )
            )
            continue
        for line_number, line in enumerate(content.splitlines(), start=1):
            for rule, pattern in SECRET_RULES:
                if pattern.search(line):
                    findings.append(
                        SecretFinding(
                            path=normalized_path,
                            line=line_number,
                            rule=rule,
                            summary=f"credential-like value detected at line {line_number}",
                        )
                    )
    return findings


def repository_candidate_paths(repo_root: Path) -> list[str]:
    result = subprocess.run(
        [
            "git",
            "-C",
            str(repo_root),
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return [line for line in result.stdout.splitlines() if line]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check repository safety policy.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    paths = repository_candidate_paths(args.repo_root)
    forbidden = find_forbidden_paths(paths)
    findings = find_secret_candidates(args.repo_root, paths)
    for path in forbidden:
        print(f"ERROR: forbidden repository path: {path}")
    for finding in findings:
        print(f"ERROR: {finding.path}:{finding.line}: {finding.rule}: {finding.summary}")
    if forbidden or findings:
        return 1
    print(f"Repository policy verified for {len(paths)} candidate files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
