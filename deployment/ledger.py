from __future__ import annotations

import base64
import binascii
import dataclasses
import json
import os
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from deployment.canonical_json import canonical_bytes, sha256_hex
from deployment.models import (
    BaselineRecord,
    DeploymentOutcome,
    DeploymentRecord,
    DeploymentTerminalCategory,
    deployment_id,
)


_DEPLOYMENT_SCHEMA_V1 = "weather-local-deployment-record/v1"
_DEPLOYMENT_SCHEMA_V2 = "weather-local-deployment-record/v2"
_DEPLOYMENT_SCHEMA_VERSIONS = frozenset(
    {_DEPLOYMENT_SCHEMA_V1, _DEPLOYMENT_SCHEMA_V2}
)
_BASELINE_SCHEMA_VERSION = "weather-local-baseline-record/v1"
_SHA40 = re.compile(r"^[0-9a-f]{40}$")
_SHA64 = re.compile(r"^[0-9a-f]{64}$")
_UTC_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_DEPLOYMENT_KEYS = frozenset(field.name for field in dataclasses.fields(DeploymentRecord))
_DEPLOYMENT_V1_KEYS = _DEPLOYMENT_KEYS - {"terminal_category"}
_BASELINE_KEYS = frozenset(field.name for field in dataclasses.fields(BaselineRecord))
_DEPLOYMENT_V1_JSON_KEYS = _DEPLOYMENT_V1_KEYS | {"record_sha256"}
_DEPLOYMENT_V2_JSON_KEYS = _DEPLOYMENT_KEYS | {"record_sha256"}
_BASELINE_JSON_KEYS = _BASELINE_KEYS | {"record_sha256"}


def _write_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(payload)
    payload["record_sha256"] = sha256_hex(canonical_bytes(payload))
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def _read_record(path: Path) -> dict[str, object] | None:
    if path.suffix == ".tmp":
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    checksum = payload.get("record_sha256")
    if not isinstance(checksum, str):
        return None
    if sha256_hex(canonical_bytes(payload, frozenset({"record_sha256"}))) != checksum:
        return None
    return payload


def _require_sha(value: object, pattern: re.Pattern[str], field: str) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ValueError(f"{field} must be lowercase hex")
    return value


def _require_timestamp(value: object, field: str) -> str:
    if not isinstance(value, str) or not _UTC_TIMESTAMP.fullmatch(value):
        raise ValueError(f"{field} must be canonical UTC")
    return value


def _require_overlay(content_b64: object, overlay_sha256: object) -> tuple[str, str]:
    if not isinstance(content_b64, str):
        raise ValueError("overlay_content_b64 must be base64")
    overlay_sha = _require_sha(overlay_sha256, _SHA64, "overlay_sha256")
    try:
        content = base64.b64decode(content_b64.encode("ascii"), validate=True)
    except (UnicodeEncodeError, binascii.Error):
        raise ValueError("overlay_content_b64 must be base64") from None
    if sha256_hex(content) != overlay_sha:
        raise ValueError("overlay checksum mismatch")
    return content_b64, overlay_sha


def _validate_deployment_record(record: DeploymentRecord) -> DeploymentRecord:
    if record.schema_version not in _DEPLOYMENT_SCHEMA_VERSIONS:
        raise ValueError("unsupported deployment record schema")
    if (
        record.schema_version == _DEPLOYMENT_SCHEMA_V1
        and record.terminal_category is not None
    ):
        raise ValueError("v1 deployment must not have terminal category")
    if (
        record.terminal_category is not None
        and type(record.terminal_category) is not DeploymentTerminalCategory
    ):
        raise ValueError("unsupported terminal category")
    if type(record.repository) is not str or not record.repository:
        raise ValueError("repository must be a non-empty string")
    _require_sha(record.candidate_sha, _SHA40, "candidate_sha")
    _require_sha(record.target_fingerprint, _SHA64, "target_fingerprint")
    if record.deployment_id != deployment_id(
        record.repository, record.candidate_sha, record.target_fingerprint
    ):
        raise ValueError("deployment_id mismatch")
    _require_timestamp(record.started_at, "started_at")
    if record.outcome is DeploymentOutcome.STARTED:
        if (
            record.completed_at is not None
            or record.health is not None
            or record.overlay_content_b64 is not None
            or record.overlay_sha256 is not None
            or record.terminal_category is not None
        ):
            raise ValueError("started deployment must not have completion fields")
        return record
    _require_timestamp(record.completed_at, "completed_at")
    if record.outcome is DeploymentOutcome.SUCCESS:
        if record.health != "passed":
            raise ValueError("successful deployment requires passed health")
        _require_overlay(record.overlay_content_b64, record.overlay_sha256)
        if record.terminal_category is not None:
            raise ValueError("successful deployment must not have terminal category")
        return record
    if record.outcome in {
        DeploymentOutcome.FAILED,
        DeploymentOutcome.ROLLED_BACK,
        DeploymentOutcome.ROLLBACK_FAILED,
    }:
        if record.health not in {None, "failed"}:
            raise ValueError("non-success deployment must not have passed health")
        if (record.overlay_content_b64 is None) != (record.overlay_sha256 is None):
            raise ValueError("overlay fields must be paired")
        if record.overlay_content_b64 is not None:
            _require_overlay(record.overlay_content_b64, record.overlay_sha256)
        if (
            record.terminal_category is not None
            and (
                record.outcome is not DeploymentOutcome.ROLLBACK_FAILED
                or record.terminal_category
                is not DeploymentTerminalCategory.PAUSE_STATE_UNVERIFIED
            )
        ):
            raise ValueError("terminal category requires rollback failure")
        return record
    raise ValueError("unsupported deployment outcome")


def _validate_baseline_record(record: BaselineRecord) -> BaselineRecord:
    if record.schema_version != _BASELINE_SCHEMA_VERSION:
        raise ValueError("unsupported baseline record schema")
    if record.baseline_id != "baseline://existing-local":
        raise ValueError("unsupported baseline id")
    _require_sha(record.target_fingerprint, _SHA64, "target_fingerprint")
    _require_timestamp(record.captured_at, "captured_at")
    if record.rehearsal != "passed":
        raise ValueError("baseline rehearsal must be passed")
    _require_overlay(record.overlay_content_b64, record.overlay_sha256)
    return record


def _record_payload(record: DeploymentRecord) -> dict[str, object]:
    payload = dataclasses.asdict(record)
    payload["outcome"] = record.outcome.value
    if record.schema_version == _DEPLOYMENT_SCHEMA_V1:
        payload.pop("terminal_category")
    elif record.terminal_category is not None:
        payload["terminal_category"] = record.terminal_category.value
    return payload


def _deployment_from_payload(payload: dict[str, object]) -> DeploymentRecord | None:
    schema_version = payload.get("schema_version")
    expected_keys = {
        _DEPLOYMENT_SCHEMA_V1: _DEPLOYMENT_V1_JSON_KEYS,
        _DEPLOYMENT_SCHEMA_V2: _DEPLOYMENT_V2_JSON_KEYS,
    }.get(schema_version)
    if expected_keys is None or set(payload) != expected_keys:
        return None
    try:
        terminal_category = (
            None
            if payload.get("terminal_category") is None
            else DeploymentTerminalCategory(str(payload["terminal_category"]))
        )
        record = DeploymentRecord(
            schema_version=payload["schema_version"],
            deployment_id=payload["deployment_id"],
            repository=payload["repository"],
            candidate_sha=payload["candidate_sha"],
            target_fingerprint=payload["target_fingerprint"],
            started_at=payload["started_at"],
            completed_at=payload["completed_at"] if isinstance(payload.get("completed_at"), str) else None,
            outcome=DeploymentOutcome(str(payload["outcome"])),
            health=payload["health"] if isinstance(payload.get("health"), str) else None,
            overlay_content_b64=payload["overlay_content_b64"] if isinstance(payload.get("overlay_content_b64"), str) else None,
            overlay_sha256=payload["overlay_sha256"] if isinstance(payload.get("overlay_sha256"), str) else None,
            terminal_category=terminal_category,
        )
    except Exception:
        return None
    try:
        return _validate_deployment_record(record)
    except ValueError:
        return None


def _baseline_from_payload(payload: dict[str, object]) -> BaselineRecord | None:
    if set(payload) != _BASELINE_JSON_KEYS:
        return None
    try:
        record = BaselineRecord(**{k: payload[k] for k in _BASELINE_KEYS})
        return _validate_baseline_record(record)
    except Exception:
        return None


def _eligible_success(record: DeploymentRecord, target_fingerprint: str) -> bool:
    return (
        record.target_fingerprint == target_fingerprint
        and record.outcome is DeploymentOutcome.SUCCESS
        and record.health == "passed"
        and record.completed_at is not None
        and record.overlay_content_b64 is not None
        and record.overlay_sha256 is not None
    )


class DeploymentLedger:
    def __init__(self, ledger_directory: Path, lock_file: Path):
        self.ledger_directory = Path(ledger_directory)
        self.lock_file = Path(lock_file)

    @contextmanager
    def acquire_lock(self, deployment_id: str) -> Iterator[None]:
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(deployment_id)
                handle.flush()
                os.fsync(handle.fileno())
            yield
        finally:
            self.lock_file.unlink(missing_ok=True)

    def _path(self, prefix: str, digest: str) -> Path:
        return self.ledger_directory / f"{prefix}-{sha256_hex(digest.encode('utf-8'))}.json"

    def begin(self, record: DeploymentRecord) -> None:
        _validate_deployment_record(record)
        _write_atomic(self._path("deployment", record.deployment_id), _record_payload(record))

    def complete(self, record: DeploymentRecord) -> None:
        _validate_deployment_record(record)
        _write_atomic(self._path("deployment", record.deployment_id), _record_payload(record))

    def record_baseline(self, record: BaselineRecord) -> None:
        _validate_baseline_record(record)
        _write_atomic(
            self._path("baseline", f"{record.baseline_id}|{record.captured_at}"),
            dataclasses.asdict(record),
        )

    def _deployment_records(self) -> list[DeploymentRecord]:
        records: list[DeploymentRecord] = []
        for path in sorted(self.ledger_directory.glob("deployment-*.json")):
            payload = _read_record(path)
            if payload is None:
                continue
            record = _deployment_from_payload(payload)
            if record is not None:
                records.append(record)
        return records

    def previous_success(self, target_fingerprint: str) -> DeploymentRecord | None:
        eligible = [record for record in self._deployment_records() if _eligible_success(record, target_fingerprint)]
        return max(eligible, key=lambda record: (record.completed_at, record.deployment_id)) if eligible else None

    def already_successful(self, deployment_id: str) -> bool:
        for record in self._deployment_records():
            if record.deployment_id == deployment_id and _eligible_success(record, record.target_fingerprint):
                return True
        return False

    def baseline(self, target_fingerprint: str) -> BaselineRecord | None:
        eligible: list[BaselineRecord] = []
        for path in sorted(self.ledger_directory.glob("baseline-*.json")):
            payload = _read_record(path)
            if payload is None:
                continue
            record = _baseline_from_payload(payload)
            if record is None:
                continue
            if (
                record.baseline_id == "baseline://existing-local"
                and record.rehearsal == "passed"
                and record.target_fingerprint == target_fingerprint
                and record.overlay_content_b64
                and record.overlay_sha256
            ):
                eligible.append(record)
        return max(eligible, key=lambda record: (record.captured_at, record.baseline_id)) if eligible else None

    def read_summary(self) -> dict[str, object]:
        success_records = [
            record
            for record in self._deployment_records()
            if _eligible_success(record, record.target_fingerprint)
        ]
        latest_success = (
            max(success_records, key=lambda record: (record.completed_at, record.deployment_id))
            if success_records
            else None
        )
        baselines = [
            record
            for path in sorted(self.ledger_directory.glob("baseline-*.json"))
            for payload in [_read_record(path)]
            if payload is not None
            for record in [_baseline_from_payload(payload)]
            if record is not None and record.baseline_id == "baseline://existing-local"
        ]
        latest_baseline = (
            max(baselines, key=lambda record: (record.captured_at, record.target_fingerprint))
            if baselines
            else None
        )
        return {
            "baseline": latest_baseline.baseline_id if latest_baseline else None,
            "previous_success": latest_success.deployment_id if latest_success else None,
        }
