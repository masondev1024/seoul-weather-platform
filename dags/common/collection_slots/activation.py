"""Activation gates for collection-slot rollout policies."""

from __future__ import annotations

from datetime import datetime, timezone


class CollectionSlotActivationError(ValueError):
    """Collection-slot activation or policy boundary configuration is invalid."""


def parse_activation_at(raw: datetime | str | None) -> datetime | None:
    """Parse an optional rollout activation timestamp as UTC."""
    if raw is None:
        return None
    if isinstance(raw, str) and not raw.strip():
        return None
    return _aware_utc(raw, field="activation_at")


def require_policy_boundary(raw: datetime | str | None, env_name: str) -> datetime:
    """Parse a required active policy boundary without inventing a fallback."""
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        raise CollectionSlotActivationError(f"{env_name} must be configured")
    return _aware_utc(raw, field=env_name)


def is_slot_active(slot_at: datetime | str, activation_at: datetime | None) -> bool:
    """Return true when a slot is at or after the configured activation instant."""
    if activation_at is None:
        return False
    return _aware_utc(slot_at, field="slot_at") >= _aware_utc(
        activation_at,
        field="activation_at",
    )


def _aware_utc(raw: datetime | str, *, field: str) -> datetime:
    if isinstance(raw, datetime):
        parsed = raw
    elif isinstance(raw, str):
        try:
            parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise CollectionSlotActivationError(f"{field} must be an ISO timestamp") from exc
    else:
        raise CollectionSlotActivationError(f"{field} must be a datetime or ISO timestamp")
    if parsed.tzinfo is None:
        raise CollectionSlotActivationError(f"{field} must include timezone")
    return parsed.astimezone(timezone.utc)


__all__ = [
    "CollectionSlotActivationError",
    "is_slot_active",
    "parse_activation_at",
    "require_policy_boundary",
]
