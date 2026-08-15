from __future__ import annotations

import base64
import json
from dataclasses import asdict, replace

import pytest

from deployment.canonical_json import canonical_bytes, sha256_hex
from deployment.ledger import DeploymentLedger
from deployment.models import (
    BaselineRecord,
    DeploymentOutcome,
    DeploymentRecord,
    DeploymentTerminalCategory,
    deployment_id,
)


def _ledger(tmp_path):
    return DeploymentLedger(tmp_path / "ledger", tmp_path / "deploy.lock")


def _overlay_bytes() -> bytes:
    return b"overlay\n"


def _overlay_b64() -> str:
    return base64.b64encode(_overlay_bytes()).decode("ascii")


def _overlay_sha() -> str:
    return sha256_hex(_overlay_bytes())


def _record(**overrides) -> DeploymentRecord:
    repository = overrides.pop("repository", "seoul-weather-platform")
    candidate_sha = overrides.pop("candidate_sha", "a" * 40)
    target_fingerprint = overrides.pop("target_fingerprint", "b" * 64)
    base = DeploymentRecord(
        schema_version="weather-local-deployment-record/v1",
        deployment_id=deployment_id(repository, candidate_sha, target_fingerprint),
        repository=repository,
        candidate_sha=candidate_sha,
        target_fingerprint=target_fingerprint,
        started_at="2026-08-15T00:00:00Z",
        completed_at=None,
        outcome=DeploymentOutcome.STARTED,
        health=None,
        overlay_content_b64=None,
        overlay_sha256=None,
    )
    return replace(base, **overrides)


def _success(**overrides) -> DeploymentRecord:
    return _record(
        candidate_sha=overrides.pop("candidate_sha", "a" * 40),
        target_fingerprint=overrides.pop("target_fingerprint", "b" * 64),
        completed_at=overrides.pop("completed_at", "2026-08-15T00:01:00Z"),
        outcome=DeploymentOutcome.SUCCESS,
        health="passed",
        overlay_content_b64=_overlay_b64(),
        overlay_sha256=_overlay_sha(),
        **overrides,
    )


def _baseline(**overrides) -> BaselineRecord:
    return BaselineRecord(
        schema_version=overrides.pop("schema_version", "weather-local-baseline-record/v1"),
        baseline_id=overrides.pop("baseline_id", "baseline://existing-local"),
        target_fingerprint=overrides.pop("target_fingerprint", "b" * 64),
        captured_at=overrides.pop("captured_at", "2026-08-15T00:00:00Z"),
        rehearsal=overrides.pop("rehearsal", "passed"),
        overlay_content_b64=overrides.pop("overlay_content_b64", _overlay_b64()),
        overlay_sha256=overrides.pop("overlay_sha256", _overlay_sha()),
    )


def test_deployment_id_is_deterministic_and_rejects_bad_sha():
    assert deployment_id("repo", "a" * 40, "b" * 64) == deployment_id("repo", "a" * 40, "b" * 64)
    with pytest.raises(ValueError, match="candidate_sha"):
        deployment_id("repo", "not-a-sha", "b" * 64)


@pytest.mark.parametrize("repository", [123, {"repo": "x"}, ["repo"], True, ""])
def test_deployment_id_requires_non_empty_builtin_str_repository(repository):
    with pytest.raises(ValueError, match="repository"):
        deployment_id(repository, "a" * 40, "b" * 64)


def test_ledger_lock_is_exclusive_and_not_auto_deleted(tmp_path):
    ledger = _ledger(tmp_path)
    dep_id = deployment_id("repo", "a" * 40, "b" * 64)
    with ledger.acquire_lock(dep_id):
        with pytest.raises(FileExistsError):
            with ledger.acquire_lock(dep_id):
                pass
    assert not (tmp_path / "deploy.lock").exists()


def test_ledger_begin_complete_and_previous_success_round_trip(tmp_path):
    ledger = _ledger(tmp_path)
    started = _record()
    ledger.begin(started)
    assert not ledger.already_successful(started.deployment_id)

    completed = _success()
    ledger.complete(completed)

    assert ledger.already_successful(started.deployment_id)
    assert ledger.previous_success("b" * 64) == completed
    summary = ledger.read_summary()
    assert summary["previous_success"] == completed.deployment_id


def test_v2_pause_state_unverified_category_is_durable_and_v1_remains_readable(
    tmp_path,
):
    ledger = _ledger(tmp_path)
    legacy_success = _success(candidate_sha="c" * 40)
    ledger.complete(legacy_success)
    unverified = _record(
        schema_version="weather-local-deployment-record/v2",
        candidate_sha="d" * 40,
        completed_at="2026-08-15T00:02:00Z",
        outcome=DeploymentOutcome.ROLLBACK_FAILED,
        health="failed",
        terminal_category=DeploymentTerminalCategory.PAUSE_STATE_UNVERIFIED,
    )

    ledger.complete(unverified)

    payloads = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in ledger.ledger_directory.glob("deployment-*.json")
    ]
    persisted = next(
        payload for payload in payloads if payload["deployment_id"] == unverified.deployment_id
    )
    assert persisted["schema_version"] == "weather-local-deployment-record/v2"
    assert persisted["terminal_category"] == "pause_state_unverified"
    assert ledger.previous_success("b" * 64) == legacy_success


@pytest.mark.parametrize(
    "record",
    [
        _record(
            completed_at="2026-08-15T00:02:00Z",
            outcome=DeploymentOutcome.ROLLBACK_FAILED,
            health="failed",
            terminal_category=DeploymentTerminalCategory.PAUSE_STATE_UNVERIFIED,
        ),
        _success(
            schema_version="weather-local-deployment-record/v2",
            terminal_category=DeploymentTerminalCategory.PAUSE_STATE_UNVERIFIED,
        ),
    ],
)
def test_ledger_rejects_terminal_category_outside_v2_rollback_failure(
    tmp_path, record
):
    with pytest.raises(ValueError):
        _ledger(tmp_path).complete(record)


def test_ledger_ignores_partial_corrupt_failed_and_rollback_candidates(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.complete(
        replace(
            _success(candidate_sha="c" * 40),
            outcome=DeploymentOutcome.FAILED,
            health=None,
            overlay_content_b64=None,
            overlay_sha256=None,
        )
    )
    ledger.complete(
        replace(
            _success(candidate_sha="d" * 40),
            outcome=DeploymentOutcome.ROLLED_BACK,
            health=None,
            overlay_content_b64=None,
            overlay_sha256=None,
        )
    )
    (tmp_path / "ledger" / "partial.tmp").write_text("{", encoding="utf-8")
    assert ledger.previous_success("b" * 64) is None


@pytest.mark.parametrize("repository", [123, {"repo": "x"}, ["repo"], True])
def test_ledger_rejects_malformed_repository_writes(tmp_path, repository):
    record = replace(_record(), repository=repository)

    with pytest.raises(ValueError, match="repository"):
        _ledger(tmp_path).begin(record)


def test_baseline_requires_existing_local_id_and_rehearsal_passed(tmp_path):
    ledger = _ledger(tmp_path)
    baseline = _baseline()
    ledger.record_baseline(baseline)
    assert ledger.baseline("b" * 64) == baseline
    with pytest.raises(ValueError):
        ledger.record_baseline(replace(baseline, baseline_id="baseline-candidate://existing-local"))
    assert ledger.baseline("b" * 64) == baseline


@pytest.mark.parametrize(
    "record",
    [
        replace(_record(), candidate_sha="A" * 40),
        replace(_record(), deployment_id="bad"),
        replace(_record(), started_at="2026-08-15 00:00:00Z"),
        replace(_record(), completed_at="2026-08-15T00:00:01Z"),
        replace(_success(), health="failed"),
        replace(_success(), overlay_content_b64="not-base64"),
        replace(_success(), overlay_sha256="0" * 64),
    ],
)
def test_ledger_rejects_malformed_deployment_writes(tmp_path, record):
    ledger = _ledger(tmp_path)

    with pytest.raises(ValueError):
        if record.outcome is DeploymentOutcome.STARTED:
            ledger.begin(record)
        else:
            ledger.complete(record)


@pytest.mark.parametrize(
    "record",
    [
        replace(_success(), outcome=DeploymentOutcome.FAILED, health="passed"),
        replace(_success(), outcome=DeploymentOutcome.ROLLED_BACK, health="passed"),
        replace(_success(), outcome=DeploymentOutcome.ROLLBACK_FAILED, health="passed"),
    ],
)
def test_ledger_rejects_outcome_specific_invariant_violations(tmp_path, record):
    with pytest.raises(ValueError):
        _ledger(tmp_path).complete(record)


@pytest.mark.parametrize(
    "baseline",
    [
        _baseline(schema_version="wrong"),
        _baseline(target_fingerprint="B" * 64),
        _baseline(captured_at="2026-08-15 00:00:00Z"),
        _baseline(rehearsal="failed"),
        _baseline(overlay_content_b64="not-base64"),
        _baseline(overlay_sha256="0" * 64),
    ],
)
def test_ledger_rejects_malformed_baseline_writes(tmp_path, baseline):
    with pytest.raises(ValueError):
        _ledger(tmp_path).record_baseline(baseline)


def test_ledger_previous_success_and_baseline_select_latest_timestamp_not_filename(tmp_path):
    ledger = _ledger(tmp_path)
    newer = _success(candidate_sha="d" * 40, completed_at="2026-08-15T00:03:00Z")
    older = _success(candidate_sha="e" * 40, completed_at="2026-08-15T00:02:00Z")
    ledger.complete(newer)
    ledger.complete(older)

    assert ledger.previous_success("b" * 64) == newer

    old_baseline = _baseline(captured_at="2026-08-15T00:00:00Z")
    new_baseline = _baseline(captured_at="2026-08-15T00:05:00Z")
    ledger.record_baseline(new_baseline)
    ledger.record_baseline(old_baseline)
    assert ledger.baseline("b" * 64) == new_baseline


def test_ledger_read_summary_includes_baseline_without_deployments(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.record_baseline(_baseline())

    assert ledger.read_summary() == {"baseline": "baseline://existing-local", "previous_success": None}


def test_ledger_ignores_malformed_records_with_extra_keys_or_bad_overlay_checksum(tmp_path):
    ledger = _ledger(tmp_path)
    good = _success()
    ledger.ledger_directory.mkdir(parents=True)

    extra_key = asdict(good)
    extra_key["outcome"] = good.outcome.value
    extra_key["unexpected"] = "bad"
    extra_key["record_sha256"] = sha256_hex(canonical_bytes(extra_key))
    (ledger.ledger_directory / "deployment-extra.json").write_text(json.dumps(extra_key), encoding="utf-8")

    bad_overlay = asdict(good)
    bad_overlay["outcome"] = good.outcome.value
    bad_overlay["overlay_sha256"] = "0" * 64
    bad_overlay["record_sha256"] = sha256_hex(canonical_bytes(bad_overlay))
    (ledger.ledger_directory / "deployment-bad-overlay.json").write_text(json.dumps(bad_overlay), encoding="utf-8")

    assert ledger.previous_success("b" * 64) is None


@pytest.mark.parametrize("repository", [123, {"repo": "x"}, ["repo"], True])
def test_ledger_ignores_forged_json_records_with_non_string_repositories(tmp_path, repository):
    ledger = _ledger(tmp_path)
    good = _success()
    payload = asdict(good)
    payload["outcome"] = good.outcome.value
    payload["repository"] = repository
    payload["record_sha256"] = sha256_hex(canonical_bytes(payload))
    ledger.ledger_directory.mkdir(parents=True)
    (ledger.ledger_directory / f"deployment-forged-{type(repository).__name__}.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    assert ledger.previous_success("b" * 64) is None
