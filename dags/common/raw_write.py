"""Fail-closed immutable writes for raw source payloads."""

from __future__ import annotations

import hashlib
from typing import Protocol


class RawObjectStore(Protocol):
    def exists(self, key: str) -> bool: ...

    def read_bytes(self, key: str) -> bytes: ...

    def write_bytes(self, key: str, payload: bytes, content_type: str) -> None: ...

    def write_bytes_if_absent(
        self,
        key: str,
        payload: bytes,
        content_type: str,
    ) -> bool: ...


class RawObjectWriteConflictError(RuntimeError):
    """An immutable raw key already has different bytes or cannot be verified."""


def write_immutable_raw_object(
    store: RawObjectStore,
    key: str,
    payload: bytes,
    content_type: str,
) -> bool:
    """Write one raw payload once, returning whether this call created it."""

    expected_hash = hashlib.sha256(payload).hexdigest()
    created = store.write_bytes_if_absent(key, payload, content_type)
    stored = store.read_bytes(key)
    if hashlib.sha256(stored).hexdigest() != expected_hash:
        if not created:
            raise RawObjectWriteConflictError(
                f"raw object already exists with divergent payload: raw_object_key={key}"
            )
        raise RawObjectWriteConflictError(
            f"raw object write verification failed: raw_object_key={key}"
        )
    return created
