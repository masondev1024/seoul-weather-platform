"""Verified local handoff for one Weather raw landing-to-Bronze cycle."""

from __future__ import annotations

import hashlib
import os
import re
import time
import uuid
from pathlib import Path


RAW_SPOOL_DIR_ENV = "ASK_SEOUL_WEATHER_RAW_SPOOL_DIR"
DEFAULT_RAW_SPOOL_DIR = "/opt/airflow/logs/_weather_raw_spool"
DEFAULT_RAW_SPOOL_RETENTION_SECONDS = 24 * 60 * 60
_SPOOL_DIRECTORY = re.compile(r"^[0-9a-f]{2}$")
_SPOOL_FILE = re.compile(r"^[0-9a-f]{32}\.[0-9a-f]{16}\.raw$")


def _normalized_sha256(value: str) -> str:
    normalized = value.strip().lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError("raw payload checksum must be a SHA-256 hex digest")
    return normalized


class RawPayloadSpool:
    """Store verified payloads on the existing shared Airflow logs volume."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        if self._root == Path(self._root.anchor):
            raise ValueError("raw spool root must not be a filesystem root")

    def _path(self, raw_object_key: str, expected_hash: str) -> Path:
        checksum = _normalized_sha256(expected_hash)
        key_digest = hashlib.sha256(raw_object_key.encode("utf-8")).hexdigest()
        # Keep the deterministic identity well below the legacy Windows MAX_PATH
        # limit even when pytest/Airflow provides a long parent directory. The
        # 128-bit key prefix plus 64-bit payload prefix still gives a 192-bit
        # collision budget, while the full checksum is always revalidated on read.
        return self._root / checksum[:2] / f"{key_digest[:32]}.{checksum[:16]}.raw"

    def write_verified(
        self,
        raw_object_key: str,
        payload: bytes,
        expected_hash: str,
    ) -> None:
        checksum = _normalized_sha256(expected_hash)
        if hashlib.sha256(payload).hexdigest() != checksum:
            raise ValueError("refusing to spool a payload with a mismatched checksum")
        target = self._path(raw_object_key, checksum)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and self.read_verified(raw_object_key, checksum) is not None:
            return
        temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
        try:
            with temporary.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    def read_verified(
        self,
        raw_object_key: str,
        expected_hash: str,
    ) -> bytes | None:
        target = self._path(raw_object_key, expected_hash)
        try:
            payload = target.read_bytes()
        except FileNotFoundError:
            return None
        if hashlib.sha256(payload).hexdigest() == _normalized_sha256(expected_hash):
            return payload
        try:
            target.unlink(missing_ok=True)
        except OSError:
            pass
        return None

    def discard(self, raw_object_key: str, expected_hash: str) -> None:
        self._path(raw_object_key, expected_hash).unlink(missing_ok=True)

    def prune_expired(
        self,
        *,
        max_age_seconds: float = DEFAULT_RAW_SPOOL_RETENTION_SECONDS,
        now: float | None = None,
    ) -> int:
        if max_age_seconds <= 0:
            raise ValueError("raw spool retention must be positive")
        if not self._root.exists():
            return 0
        cutoff = (time.time() if now is None else now) - max_age_seconds
        removed = 0
        for directory in self._root.iterdir():
            if (
                directory.is_symlink()
                or not directory.is_dir()
                or _SPOOL_DIRECTORY.fullmatch(directory.name) is None
            ):
                continue
            for candidate in directory.iterdir():
                if _SPOOL_FILE.fullmatch(candidate.name) is None:
                    continue
                try:
                    if candidate.stat().st_mtime >= cutoff:
                        continue
                    candidate.unlink()
                    removed += 1
                except FileNotFoundError:
                    continue
        return removed


def configured_raw_payload_spool() -> RawPayloadSpool:
    return RawPayloadSpool(os.environ.get(RAW_SPOOL_DIR_ENV, DEFAULT_RAW_SPOOL_DIR))
