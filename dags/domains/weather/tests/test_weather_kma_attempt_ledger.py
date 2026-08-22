from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pytest


DOMAIN_ROOT = Path(__file__).resolve().parents[1]
DAGS_ROOT = DOMAIN_ROOT.parents[1]
REPOSITORY_ROOT = DAGS_ROOT.parent
sys.path.insert(0, str(DOMAIN_ROOT))
sys.path.insert(0, str(DAGS_ROOT))

from weather_ingest.kma_coordination import (  # noqa: E402
    AttemptReservation,
    KmaAttemptLedgerConfigurationError,
    KmaDailyAttemptLimitExceeded,
    PhysicalAttempt,
    PhysicalAttemptBudgetHook,
    SqliteAttemptLedger,
    initialize_attempt_ledger,
)


def _configure_ledger(monkeypatch, tmp_path: Path, *, limit: int = 3) -> Path:
    control_root = tmp_path / "control"
    ledger_path = control_root / "kma_api_budget.sqlite3"
    monkeypatch.setenv("ASK_SEOUL_KMA_CONTROL_ROOT", str(control_root))
    monkeypatch.setenv("ASK_SEOUL_KMA_ATTEMPT_LEDGER_PATH", str(ledger_path))
    monkeypatch.setenv("ASK_SEOUL_KMA_DAILY_ATTEMPT_LIMIT", str(limit))
    return ledger_path


def _attempt(
    reservation_id: str,
    *,
    ordinal: int = 1,
    reserved_at: datetime | None = None,
) -> AttemptReservation:
    return AttemptReservation(
        reservation_id=reservation_id,
        source_id="kma_ultra_srt_ncst",
        dag_run_id="scheduled__2026-08-22T00:45:00+00:00",
        observed_slot="2026-08-22T09:00:00+09:00",
        nx=60,
        ny=127,
        attempt_ordinal=ordinal,
        reserved_at=reserved_at or datetime(2026, 8, 22, tzinfo=timezone.utc),
    )


def test_initializer_creates_schema_v1_and_is_idempotent(monkeypatch, tmp_path):
    path = _configure_ledger(monkeypatch, tmp_path)

    initialize_attempt_ledger()
    initialize_attempt_ledger()

    with sqlite3.connect(path) as connection:
        version = connection.execute("SELECT version FROM schema_version").fetchone()
        columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(kma_api_attempt_reservations)"
            )
        }
    assert version == (1,)
    assert columns == {
        "reservation_id",
        "budget_date",
        "source_id",
        "dag_run_id",
        "observed_slot",
        "nx",
        "ny",
        "attempt_ordinal",
        "reserved_at",
    }


def test_runtime_never_auto_creates_a_missing_ledger(monkeypatch, tmp_path):
    path = _configure_ledger(monkeypatch, tmp_path)

    with pytest.raises(KmaAttemptLedgerConfigurationError, match="initialize"):
        SqliteAttemptLedger.from_environment()

    assert not path.exists()


@pytest.mark.parametrize("limit", ["0", "-1", "10000", "10001", "not-an-int"])
def test_runtime_rejects_unsafe_daily_limits(monkeypatch, tmp_path, limit):
    _configure_ledger(monkeypatch, tmp_path)
    monkeypatch.setenv("ASK_SEOUL_KMA_DAILY_ATTEMPT_LIMIT", limit)

    with pytest.raises(KmaAttemptLedgerConfigurationError, match="daily attempt"):
        SqliteAttemptLedger.from_environment()


def test_runtime_rejects_a_ledger_outside_control_root(monkeypatch, tmp_path):
    _configure_ledger(monkeypatch, tmp_path)
    monkeypatch.setenv(
        "ASK_SEOUL_KMA_ATTEMPT_LEDGER_PATH",
        str(tmp_path / "outside" / "budget.sqlite3"),
    )

    with pytest.raises(KmaAttemptLedgerConfigurationError, match="control root"):
        SqliteAttemptLedger.from_environment()


def test_runtime_rejects_an_incompatible_schema(monkeypatch, tmp_path):
    path = _configure_ledger(monkeypatch, tmp_path)
    initialize_attempt_ledger()
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE schema_version SET version = 2")

    with pytest.raises(KmaAttemptLedgerConfigurationError, match="schema version"):
        SqliteAttemptLedger.from_environment()


def test_reservation_commits_before_the_caller_can_issue_network_io(
    monkeypatch,
    tmp_path,
):
    path = _configure_ledger(monkeypatch, tmp_path)
    initialize_attempt_ledger()
    ledger = SqliteAttemptLedger.from_environment()

    result = ledger.reserve(_attempt("reservation-1"))

    with sqlite3.connect(path) as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM kma_api_attempt_reservations"
        ).fetchone()[0]
    assert result.created is True
    assert result.daily_count == 1
    assert count == 1


def test_same_reservation_id_is_idempotent(monkeypatch, tmp_path):
    _configure_ledger(monkeypatch, tmp_path)
    initialize_attempt_ledger()
    ledger = SqliteAttemptLedger.from_environment()

    first = ledger.reserve(_attempt("same"))
    second = ledger.reserve(_attempt("same"))

    assert first.created is True
    assert second.created is False
    assert second.daily_count == 1


def test_same_reservation_id_with_different_attempt_fails_closed(monkeypatch, tmp_path):
    _configure_ledger(monkeypatch, tmp_path)
    initialize_attempt_ledger()
    ledger = SqliteAttemptLedger.from_environment()
    ledger.reserve(_attempt("collision", ordinal=1))

    with pytest.raises(KmaAttemptLedgerConfigurationError, match="collision"):
        ledger.reserve(_attempt("collision", ordinal=2))


def test_distinct_physical_retries_consume_distinct_reservations(monkeypatch, tmp_path):
    _configure_ledger(monkeypatch, tmp_path)
    initialize_attempt_ledger()
    ledger = SqliteAttemptLedger.from_environment()

    assert ledger.reserve(_attempt("try-1", ordinal=1)).daily_count == 1
    assert ledger.reserve(_attempt("try-2", ordinal=2)).daily_count == 2


def test_daily_ceiling_fails_before_an_extra_reservation(monkeypatch, tmp_path):
    path = _configure_ledger(monkeypatch, tmp_path, limit=2)
    initialize_attempt_ledger()
    ledger = SqliteAttemptLedger.from_environment()
    ledger.reserve(_attempt("try-1", ordinal=1))
    ledger.reserve(_attempt("try-2", ordinal=2))

    with pytest.raises(KmaDailyAttemptLimitExceeded, match="daily attempt"):
        ledger.reserve(_attempt("try-3", ordinal=3))

    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM kma_api_attempt_reservations"
        ).fetchone()[0] == 2


def test_budget_date_uses_kst_midnight(monkeypatch, tmp_path):
    path = _configure_ledger(monkeypatch, tmp_path, limit=3)
    initialize_attempt_ledger()
    ledger = SqliteAttemptLedger.from_environment()
    ledger.reserve(
        _attempt(
            "before-kst-midnight",
            reserved_at=datetime(2026, 8, 21, 14, 59, tzinfo=timezone.utc),
        )
    )
    ledger.reserve(
        _attempt(
            "after-kst-midnight",
            reserved_at=datetime(2026, 8, 21, 15, 0, tzinfo=timezone.utc),
        )
    )

    with sqlite3.connect(path) as connection:
        dates = connection.execute(
            "SELECT budget_date FROM kma_api_attempt_reservations "
            "ORDER BY reserved_at"
        ).fetchall()
    assert dates == [("2026-08-21",), ("2026-08-22",)]


def test_concurrent_reservations_never_exceed_the_ceiling(monkeypatch, tmp_path):
    path = _configure_ledger(monkeypatch, tmp_path, limit=5)
    initialize_attempt_ledger()

    def reserve(index: int) -> bool:
        ledger = SqliteAttemptLedger.from_environment()
        try:
            ledger.reserve(_attempt(f"concurrent-{index}", ordinal=index + 1))
            return True
        except KmaDailyAttemptLimitExceeded:
            return False

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(reserve, range(10)))

    assert sum(results) == 5
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM kma_api_attempt_reservations"
        ).fetchone()[0] == 5


def test_physical_attempt_hook_reserves_each_attempt(monkeypatch, tmp_path):
    _configure_ledger(monkeypatch, tmp_path, limit=3)
    initialize_attempt_ledger()
    ledger = SqliteAttemptLedger.from_environment()
    hook = PhysicalAttemptBudgetHook(
        ledger,
        clock=lambda: datetime(2026, 8, 22, tzinfo=timezone.utc),
    )

    first = hook(
        PhysicalAttempt(
            reservation_id="hook-1",
            source_id="kma_ultra_srt_ncst",
            dag_run_id="scheduled__one",
            observed_slot="2026-08-22T09:00:00+09:00",
            nx=60,
            ny=127,
            attempt_ordinal=1,
        )
    )
    second = hook(
        PhysicalAttempt(
            reservation_id="hook-2",
            source_id="kma_ultra_srt_ncst",
            dag_run_id="scheduled__one",
            observed_slot="2026-08-22T09:00:00+09:00",
            nx=60,
            ny=127,
            attempt_ordinal=2,
        )
    )

    assert (first.daily_count, second.daily_count) == (1, 2)


def test_module_cli_initializes_the_configured_ledger_idempotently(
    monkeypatch,
    tmp_path,
):
    path = _configure_ledger(monkeypatch, tmp_path)
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join((str(DAGS_ROOT), str(DOMAIN_ROOT)))

    for _ in range(2):
        result = subprocess.run(
            [sys.executable, "-m", "weather_ingest.kma_coordination", "init-ledger"],
            cwd=REPOSITORY_ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        assert "schema_version=1" in result.stdout

    assert path.is_file()

