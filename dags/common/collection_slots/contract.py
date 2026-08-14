"""Immutable collection-slot identities and terminal evidence validation."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any


COLLECTION_STATES = frozenset(
    {
        "observed",
        "source_empty_valid",
        "collection_failed",
        "not_scheduled",
        "missing_unknown",
    }
)
RECOVERY_STATES = frozenset(
    {"not_required", "pending", "recovered", "unrecoverable"}
)
RECOVERY_CLASSES = frozenset(
    {
        "raw_replay",
        "historical_query",
        "rolling_window",
        "full_refresh",
        "next_snapshot_diff",
        "none",
    }
)

_TERMINAL_COLLECTION_STATES = frozenset(
    {"observed", "source_empty_valid", "not_scheduled"}
)
_GAP_REQUIRED_STATES = frozenset({"collection_failed", "missing_unknown"})
_SAFE_CODE_PATTERN = re.compile(r"^[a-z0-9_]+$")


def canonical_json(value: object) -> str:
    """Serialize JSON-compatible evidence deterministically for ids and comparisons."""
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
            default=_reject_non_json_value,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("collection slot evidence must be JSON serializable") from exc


def _reject_non_json_value(value: object) -> object:
    if isinstance(value, Mapping):
        return dict(value)
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def _timestamp(value: datetime | str, *, field: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO timestamp") from exc
    else:
        raise ValueError(f"{field} must be a datetime or ISO timestamp")
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include timezone")
    return parsed.astimezone(timezone.utc).isoformat()


def _text(value: object, *, field: str, required: bool = True) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _safe_code(value: object, *, field: str, required: bool = False) -> str | None:
    text = _text(value, field=field, required=required)
    if text is None:
        return None
    if not _SAFE_CODE_PATTERN.fullmatch(text):
        raise ValueError(f"{field} must contain only lowercase letters, digits, and underscores")
    return text


def _enum(value: object, *, field: str, allowed: frozenset[str]) -> str:
    text = _text(value, field=field)
    assert text is not None
    if text not in allowed:
        raise ValueError(f"unsupported {field}: {text}")
    return text


def _count(value: object, *, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _grain(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("grain must be a JSON object")
    try:
        normalized = json.loads(canonical_json(value))
    except ValueError as exc:
        raise ValueError("grain must be JSON serializable") from exc
    if not isinstance(normalized, dict):
        raise ValueError("grain must be a JSON object")
    return MappingProxyType(normalized)


def slot_id_for(
    contract_version: str,
    collection_contract_id: str,
    source_id: str,
    collection_slot_at: datetime | str,
    grain: Mapping[str, object],
) -> str:
    """Build the stable identifier shared by collector and reconciler retries."""
    payload = canonical_json(
        {
            "contract_version": _text(contract_version, field="contract_version"),
            "collection_contract_id": _text(
                collection_contract_id,
                field="collection_contract_id",
            ),
            "source_id": _text(source_id, field="source_id"),
            "collection_slot_at": _timestamp(
                collection_slot_at,
                field="collection_slot_at",
            ),
            "grain": _grain(grain),
        }
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ExpectedSlot:
    contract_version: str
    expected_slot_id: str
    domain: str
    collection_contract_id: str
    source_id: str
    collection_slot_at: str
    grain: Mapping[str, Any]
    schedule_version: str
    scheduled_at: str
    deadline_at: str
    is_scheduled: bool
    recovery_boundary_type: str
    recovery_boundary: str
    declared_at: str
    declared_by: str

    @classmethod
    def create(
        cls,
        *,
        contract_version: str,
        domain: str,
        collection_contract_id: str,
        source_id: str,
        collection_slot_at: datetime | str,
        scheduled_at: datetime | str,
        deadline_at: datetime | str,
        grain: Mapping[str, object],
        schedule_version: str,
        is_scheduled: bool,
        recovery_boundary_type: str,
        recovery_boundary: str,
        declared_at: datetime | str,
        declared_by: str,
    ) -> "ExpectedSlot":
        if not isinstance(is_scheduled, bool):
            raise ValueError("is_scheduled must be a bool")
        slot_at = _timestamp(collection_slot_at, field="collection_slot_at")
        scheduled = _timestamp(scheduled_at, field="scheduled_at")
        deadline = _timestamp(deadline_at, field="deadline_at")
        if deadline < slot_at or deadline < scheduled:
            raise ValueError("deadline_at must not precede collection_slot_at or scheduled_at")
        normalized_grain = _grain(grain)
        normalized_contract_version = _text(
            contract_version,
            field="contract_version",
        )
        normalized_contract_id = _text(
            collection_contract_id,
            field="collection_contract_id",
        )
        normalized_source_id = _text(source_id, field="source_id")
        assert normalized_contract_version is not None
        assert normalized_contract_id is not None
        assert normalized_source_id is not None
        return cls(
            contract_version=normalized_contract_version,
            expected_slot_id=slot_id_for(
                normalized_contract_version,
                normalized_contract_id,
                normalized_source_id,
                slot_at,
                normalized_grain,
            ),
            domain=_text(domain, field="domain") or "",
            collection_contract_id=normalized_contract_id,
            source_id=normalized_source_id,
            collection_slot_at=slot_at,
            grain=normalized_grain,
            schedule_version=_text(schedule_version, field="schedule_version") or "",
            scheduled_at=scheduled,
            deadline_at=deadline,
            is_scheduled=is_scheduled,
            recovery_boundary_type=_text(
                recovery_boundary_type,
                field="recovery_boundary_type",
            )
            or "",
            recovery_boundary=_text(
                recovery_boundary,
                field="recovery_boundary",
            )
            or "",
            declared_at=_timestamp(declared_at, field="declared_at"),
            declared_by=_text(declared_by, field="declared_by") or "",
        )

    @property
    def grain_key(self) -> str:
        return hashlib.sha256(canonical_json(self.grain).encode("utf-8")).hexdigest()

    def to_create_kwargs(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "domain": self.domain,
            "collection_contract_id": self.collection_contract_id,
            "source_id": self.source_id,
            "collection_slot_at": self.collection_slot_at,
            "scheduled_at": self.scheduled_at,
            "deadline_at": self.deadline_at,
            "grain": dict(self.grain),
            "schedule_version": self.schedule_version,
            "is_scheduled": self.is_scheduled,
            "recovery_boundary_type": self.recovery_boundary_type,
            "recovery_boundary": self.recovery_boundary,
            "declared_at": self.declared_at,
            "declared_by": self.declared_by,
        }

    def to_document(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "expected_slot_id": self.expected_slot_id,
            "domain": self.domain,
            "collection_contract_id": self.collection_contract_id,
            "source_id": self.source_id,
            "collection_slot_at": self.collection_slot_at,
            "grain_key": self.grain_key,
            "grain_json": canonical_json(self.grain),
            "schedule_version": self.schedule_version,
            "scheduled_at": self.scheduled_at,
            "deadline_at": self.deadline_at,
            "is_scheduled": self.is_scheduled,
            "recovery_boundary_type": self.recovery_boundary_type,
            "recovery_boundary": self.recovery_boundary,
            "declared_at": self.declared_at,
            "declared_by": self.declared_by,
        }


@dataclass(frozen=True)
class CollectionOutcome:
    event_id: str
    expected_slot_id: str
    event_type: str
    collection_state: str
    recovery_state: str
    recovery_class: str
    gap_reason_code: str | None
    dag_id: str
    dag_run_id: str
    task_id: str | None
    raw_manifest_key: str | None
    raw_object_count: int | None
    row_count: int | None
    source_result_code: str | None
    recovery_run_id: str | None
    recovered_at: str | None
    recovery_evidence_code: str | None
    event_at: str

    @classmethod
    def create(
        cls,
        *,
        expected_slot_id: str,
        collection_state: str,
        recovery_state: str,
        recovery_class: str,
        event_at: datetime | str,
        dag_id: str,
        dag_run_id: str,
        event_type: str = "terminal",
        gap_reason_code: str | None = None,
        task_id: str | None = None,
        raw_manifest_key: str | None = None,
        raw_object_count: int | None = None,
        row_count: int | None = None,
        source_result_code: str | None = None,
        recovery_run_id: str | None = None,
        recovered_at: datetime | str | None = None,
        recovery_evidence_code: str | None = None,
    ) -> "CollectionOutcome":
        normalized_collection_state = _enum(
            collection_state,
            field="collection_state",
            allowed=COLLECTION_STATES,
        )
        normalized_recovery_state = _enum(
            recovery_state,
            field="recovery_state",
            allowed=RECOVERY_STATES,
        )
        normalized_recovery_class = _enum(
            recovery_class,
            field="recovery_class",
            allowed=RECOVERY_CLASSES,
        )
        normalized_gap_reason = _text(
            gap_reason_code,
            field="gap_reason_code",
            required=False,
        )
        gap_required = (
            normalized_collection_state in _GAP_REQUIRED_STATES
            or normalized_recovery_state == "unrecoverable"
        )
        if gap_required and normalized_gap_reason is None:
            raise ValueError("gap_reason_code is required for failed or unrecoverable evidence")
        if normalized_collection_state in _TERMINAL_COLLECTION_STATES and (
            normalized_recovery_state != "not_required"
            or normalized_recovery_class != "none"
        ):
            raise ValueError(
                "observed, source_empty_valid, and not_scheduled require not_required/none recovery"
            )
        if normalized_recovery_state == "not_required" and normalized_recovery_class != "none":
            raise ValueError("not_required recovery must use recovery_class=none")
        if normalized_recovery_state == "recovered" and normalized_recovery_class == "none":
            raise ValueError("recovered evidence requires a recovery class")

        normalized_recovery_run_id = _text(
            recovery_run_id,
            field="recovery_run_id",
            required=False,
        )
        normalized_recovered_at = (
            _timestamp(recovered_at, field="recovered_at")
            if recovered_at is not None
            else None
        )
        if normalized_recovery_state == "recovered" and (
            normalized_recovery_run_id is None or normalized_recovered_at is None
        ):
            raise ValueError("recovered evidence requires recovery_run_id and recovered_at")
        if normalized_recovery_state != "recovered" and (
            normalized_recovery_run_id is not None or normalized_recovered_at is not None
        ):
            raise ValueError("recovery evidence is only valid for recovery_state=recovered")

        normalized_expected_slot_id = _text(
            expected_slot_id,
            field="expected_slot_id",
        )
        normalized_event_type = _text(event_type, field="event_type")
        normalized_dag_id = _text(dag_id, field="dag_id")
        normalized_dag_run_id = _text(dag_run_id, field="dag_run_id")
        assert normalized_expected_slot_id is not None
        assert normalized_event_type is not None
        assert normalized_dag_id is not None
        assert normalized_dag_run_id is not None
        normalized_task_id = _text(task_id, field="task_id", required=False)
        event_id = hashlib.sha256(
            canonical_json(
                {
                    "expected_slot_id": normalized_expected_slot_id,
                    "event_type": normalized_event_type,
                    "collection_state": normalized_collection_state,
                    "recovery_state": normalized_recovery_state,
                    "recovery_class": normalized_recovery_class,
                    "dag_id": normalized_dag_id,
                    "dag_run_id": normalized_dag_run_id,
                    "task_id": normalized_task_id,
                    "recovery_run_id": normalized_recovery_run_id,
                }
            ).encode("utf-8")
        ).hexdigest()
        return cls(
            event_id=event_id,
            expected_slot_id=normalized_expected_slot_id,
            event_type=normalized_event_type,
            collection_state=normalized_collection_state,
            recovery_state=normalized_recovery_state,
            recovery_class=normalized_recovery_class,
            gap_reason_code=normalized_gap_reason,
            dag_id=normalized_dag_id,
            dag_run_id=normalized_dag_run_id,
            task_id=normalized_task_id,
            raw_manifest_key=_text(
                raw_manifest_key,
                field="raw_manifest_key",
                required=False,
            ),
            raw_object_count=_count(raw_object_count, field="raw_object_count"),
            row_count=_count(row_count, field="row_count"),
            source_result_code=_text(
                source_result_code,
                field="source_result_code",
                required=False,
            ),
            recovery_run_id=normalized_recovery_run_id,
            recovered_at=normalized_recovered_at,
            recovery_evidence_code=_safe_code(
                recovery_evidence_code,
                field="recovery_evidence_code",
            ),
            event_at=_timestamp(event_at, field="event_at"),
        )

    def to_document(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "expected_slot_id": self.expected_slot_id,
            "event_type": self.event_type,
            "collection_state": self.collection_state,
            "recovery_state": self.recovery_state,
            "recovery_class": self.recovery_class,
            "gap_reason_code": self.gap_reason_code,
            "dag_id": self.dag_id,
            "dag_run_id": self.dag_run_id,
            "task_id": self.task_id,
            "raw_manifest_key": self.raw_manifest_key,
            "raw_object_count": self.raw_object_count,
            "row_count": self.row_count,
            "source_result_code": self.source_result_code,
            "recovery_run_id": self.recovery_run_id,
            "recovered_at": self.recovered_at,
            "recovery_evidence_code": self.recovery_evidence_code,
            "event_at": self.event_at,
        }


__all__ = [
    "COLLECTION_STATES",
    "RECOVERY_CLASSES",
    "RECOVERY_STATES",
    "CollectionOutcome",
    "ExpectedSlot",
    "canonical_json",
    "slot_id_for",
]
