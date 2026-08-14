from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path
from typing import Any


DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
REPOSITORY_RE = re.compile(r"^[a-z0-9]+(?:(?:[._/-])[a-z0-9]+)*$")


class DagBagHarnessError(RuntimeError):
    """Raised when the isolated DagBag command cannot be built safely."""


def _toolchain_image_reference(repo_root: Path) -> str:
    lock_path = repo_root / "runtime" / "toolchain.lock.json"
    try:
        lock = json.loads(lock_path.read_text(encoding="utf-8"))
        airflow = lock["tools"]["airflow"]
        repository = airflow["image_repository"]
        digest = airflow["image_digest"]
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise DagBagHarnessError(f"cannot read Airflow image digest from {lock_path}") from exc
    if not isinstance(digest, str) or not DIGEST_RE.fullmatch(digest):
        raise DagBagHarnessError("Airflow image_digest must be a sha256 digest")
    if not isinstance(repository, str) or not REPOSITORY_RE.fullmatch(repository):
        raise DagBagHarnessError(
            "Airflow image_repository must be a tag-free lowercase repository name"
        )
    return f"{repository}@{digest}"


def _readonly_mount(source: Path, destination: str) -> str:
    return f"type=bind,src={source.resolve()},dst={destination},readonly"


def build_dagbag_command(repo_root: Path) -> list[str]:
    root = repo_root.resolve()
    dags = root / "dags"
    dbt = root / "dbt"
    verification = root / "tools"
    for directory in (dags, dbt, verification):
        if not directory.is_dir():
            raise DagBagHarnessError(f"required read-only mount is missing: {directory}")
    runtime_check = verification / "dagbag_runtime_check.py"
    if not runtime_check.is_file():
        raise DagBagHarnessError(f"DagBag runtime check is missing: {runtime_check}")

    image = _toolchain_image_reference(root)
    return [
        "docker",
        "run",
        "--rm",
        "--network",
        "none",
        "--read-only",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,noexec,size=64m",
        "--mount",
        _readonly_mount(dags, "/opt/airflow/dags"),
        "--mount",
        _readonly_mount(dbt, "/opt/airflow/dbt"),
        "--mount",
        _readonly_mount(verification, "/opt/verification"),
        "--env",
        "AIRFLOW_HOME=/tmp/airflow",
        "--env",
        "AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags",
        "--env",
        "AIRFLOW__CORE__LOAD_EXAMPLES=False",
        "--env",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow/airflow.db",
        "--env",
        "PYTHONDONTWRITEBYTECODE=1",
        image,
        "python",
        "/opt/verification/dagbag_runtime_check.py",
        "--dags-folder",
        "/opt/airflow/dags",
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run an isolated secretless Airflow DagBag check.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--print-command",
        action="store_true",
        help="print the one-off Docker command without executing it",
    )
    args = parser.parse_args(argv)
    try:
        command = build_dagbag_command(args.repo_root)
    except DagBagHarnessError as exc:
        print(f"ERROR: {exc}")
        return 1
    if args.print_command:
        print(json.dumps(command))
        return 0
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
