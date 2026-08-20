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


def _stored_metadata_hash(store: RawObjectStore, key: str) -> str | None:
    metadata_reader = getattr(store, "read_sha256", None)
    if callable(metadata_reader):
        metadata_hash = metadata_reader(key)
        if isinstance(metadata_hash, str):
            normalized = metadata_hash.strip().lower()
            if len(normalized) == 64 and all(
                character in "0123456789abcdef" for character in normalized
            ):
                return normalized
    return None


def write_immutable_raw_object(
    store: RawObjectStore,
    key: str,
    payload: bytes,
    content_type: str,
) -> bool:
    """Write one raw payload once, returning whether this call created it."""

    expected_hash = hashlib.sha256(payload).hexdigest()
    created = store.write_bytes_if_absent(key, payload, content_type)
    if created:
        # Only trust custom metadata for the object this call just created. The
        # R2 adapter couples that PUT with Content-MD5, so R2 validates the body
        # in transit before this cheap HEAD acknowledgement is accepted.
        metadata_hash = _stored_metadata_hash(store, key)
        if metadata_hash == expected_hash:
            return True
        if metadata_hash is not None:
            raise RawObjectWriteConflictError(
                f"raw object write verification failed: raw_object_key={key}"
            )

    stored_hash = hashlib.sha256(store.read_bytes(key)).hexdigest()
    if stored_hash != expected_hash:
        if not created:
            raise RawObjectWriteConflictError(
                f"raw object already exists with divergent payload: raw_object_key={key}"
            )
        raise RawObjectWriteConflictError(
            f"raw object write verification failed: raw_object_key={key}"
        )
    return created
