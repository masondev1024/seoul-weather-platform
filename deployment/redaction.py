from __future__ import annotations

import ipaddress
import re
from collections.abc import Mapping, Sequence
from urllib.parse import urlsplit


class SensitiveArtifactError(ValueError):
    """Raised when a value is unsafe to publish in a release artifact."""


_SENSITIVE_KEY = re.compile(r"(?:api[_-]?key|token|secret|password|credential)", re.I)
_WINDOWS_ABSOLUTE_PATH = re.compile(r"^[A-Za-z]:[\\/]")
_ENV_REFERENCE = re.compile(r"(?:^|[\\/])\.env(?:$|\.)", re.I)
_CREDENTIAL_VALUE = re.compile(
    r"(?:^ask_|^ghp_|^github_pat_|^sk-|^xox[a-z]*-|^bearer\s+|^eyJ[\w-]*\.|^-----BEGIN [A-Z ]+-----)",
    re.I,
)


def _private_address(value: str) -> bool:
    try:
        address = ipaddress.ip_address(value.strip("[]"))
    except ValueError:
        return False
    return (
        address.is_private
        or address.is_link_local
        or address.is_loopback
        or address.is_unspecified
        or address.is_reserved
    )


def _is_sensitive_string(value: str) -> bool:
    if _WINDOWS_ABSOLUTE_PATH.match(value) or value.startswith(("/", "\\\\")):
        return True
    if _ENV_REFERENCE.search(value):
        return True
    try:
        parsed = urlsplit(value)
    except ValueError:
        return "://" in value
    if parsed.scheme.lower() == "file":
        return True
    if _CREDENTIAL_VALUE.search(value):
        return True
    if parsed.scheme and parsed.netloc:
        if parsed.username is not None or parsed.password is not None:
            return True
        if parsed.hostname is None:
            return True
        if parsed.hostname.lower() == "localhost":
            return True
        return _private_address(parsed.hostname)
    if value.startswith("[") and value.endswith("]"):
        return _private_address(value)
    return _private_address(value)


def reject_sensitive_artifact(value: object) -> None:
    """Reject nested data containing paths, credential material, or private hosts."""
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise SensitiveArtifactError("release artifact keys must be strings")
            if _SENSITIVE_KEY.search(key) or _is_sensitive_string(key):
                raise SensitiveArtifactError("sensitive field name in release artifact")
            reject_sensitive_artifact(item)
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            reject_sensitive_artifact(item)
        return
    if isinstance(value, str) and _is_sensitive_string(value):
        raise SensitiveArtifactError("sensitive value in release artifact")
