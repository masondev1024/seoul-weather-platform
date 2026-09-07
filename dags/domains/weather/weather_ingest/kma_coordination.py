"""Opt-in resource, attempt-budget, and deadline controls for KMA pipelines."""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Protocol, Sequence
from urllib.parse import quote
from zoneinfo import ZoneInfo

from common.pools import (
    KMA_API_POOL,
    TRINO_WEATHER_HEAVY_POOL,
    TRINO_WEATHER_HEAVY_POOL_SLOTS,
)
from weather_ingest.errors import WeatherBronzeConfigurationError


SHARED_GUARDS_ENV = "ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED"
CONTROL_ROOT_ENV = "ASK_SEOUL_KMA_CONTROL_ROOT"
ATTEMPT_LEDGER_PATH_ENV = "ASK_SEOUL_KMA_ATTEMPT_LEDGER_PATH"
DAILY_ATTEMPT_LIMIT_ENV = "ASK_SEOUL_KMA_DAILY_ATTEMPT_LIMIT"
DEFAULT_CONTROL_ROOT = Path("/opt/airflow/logs/_weather_control")
DEFAULT_ATTEMPT_LEDGER_PATH = DEFAULT_CONTROL_ROOT / "kma_api_budget.sqlite3"
DEFAULT_DAILY_ATTEMPT_LIMIT = 7_500
PROVIDER_DAILY_ATTEMPT_QUOTA = 10_000
ATTEMPT_LEDGER_SCHEMA_VERSION = 1
COLLECTION_DEADLINE = timedelta(minutes=40)
DEADLINE_CHECKPOINT_PREFIX = "ops/weather/_control/kma_cycle_deadlines"
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"", "0", "false", "no", "off"})
_KST = ZoneInfo("Asia/Seoul")
_RESERVATION_COLUMNS = (
    "reservation_id",
    "budget_date",
    "source_id",
    "dag_run_id",
    "observed_slot",
    "nx",
    "ny",
    "attempt_ordinal",
    "reserved_at",
)


class KmaCoordinationConfigurationError(WeatherBronzeConfigurationError):
    """Raised when an opt-in coordination setting is invalid."""


class KmaAttemptLedgerConfigurationError(KmaCoordinationConfigurationError):
    """Raised when the durable attempt ledger is absent or invalid."""


class KmaDailyAttemptLimitExceeded(RuntimeError):
    """Raised before HTTP when the configured shared daily budget is exhausted."""


class CycleDeadlineConfigurationError(KmaCoordinationConfigurationError):
    """Raised when a persisted collection deadline anchor is invalid."""


class CycleDeadlineExceeded(RuntimeError):
    """Raised before an action that cannot finish inside the original cycle."""


@dataclass(frozen=True)
class AttemptReservation:
    reservation_id: str
    source_id: str
    dag_run_id: str
    observed_slot: str
    nx: int
    ny: int
    attempt_ordinal: int
    reserved_at: datetime


@dataclass(frozen=True)
class AttemptReservationResult:
    reservation_id: str
    budget_date: str
    daily_count: int
    created: bool


@dataclass(frozen=True)
class PhysicalAttempt:
    reservation_id: str
    source_id: str
    dag_run_id: str
    observed_slot: str
    nx: int
    ny: int
    attempt_ordinal: int


@dataclass(frozen=True)
class CycleIdentity:
    dag_id: str
    dag_run_id: str
    source_id: str
    observed_slot: str


@dataclass(frozen=True)
class DeadlineAnchor:
    dag_id: str
    dag_run_id: str
    source_id: str
    observed_slot: str
    started_at: datetime
    deadline_at: datetime


class DeadlineCheckpointStore(Protocol):
    def read_bytes(self, key: str) -> bytes: ...

    def write_bytes_if_absent(
        self,
        key: str,
        payload: bytes,
        content_type: str,
    ) -> bool: ...


def shared_guards_enabled() -> bool:
    """Return whether shared KMA/Trino guards are explicitly enabled."""
    value = os.getenv(SHARED_GUARDS_ENV, "").strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    raise KmaCoordinationConfigurationError(
        f"{SHARED_GUARDS_ENV} must be a boolean value"
    )


def kma_api_pool_kwargs() -> dict[str, str | int]:
    """Return Airflow operator kwargs for shared KMA API serialization."""
    if not shared_guards_enabled():
        return {}
    return {"pool": KMA_API_POOL, "pool_slots": 1}


def weather_heavy_pool(legacy_pool: str) -> str:
    """Select the canonical Weather Trino pool only during opt-in rollout."""
    if shared_guards_enabled():
        return TRINO_WEATHER_HEAVY_POOL
    return legacy_pool


def weather_heavy_pool_kwargs(
    legacy_pool: str,
    *,
    pool_slots: int = 1,
) -> dict[str, str | int]:
    """Return a guarded Weather Trino pool assignment.

    The active personal runtime owns a two-slot canonical lane.  A normal
    transform branch consumes one slot; a writer that must not overlap another
    Weather writer (snapshot/reference refresh/ingest/maintenance) consumes
    both.  When the shared-guard rollout is deliberately disabled we preserve
    the historical one-slot fallback so a rollback cannot deadlock on a pool
    that was never resized.
    """

    if isinstance(pool_slots, bool) or not isinstance(pool_slots, int):
        raise KmaCoordinationConfigurationError(
            "Weather Trino pool_slots must be an integer"
        )
    if pool_slots < 1 or pool_slots > TRINO_WEATHER_HEAVY_POOL_SLOTS:
        raise KmaCoordinationConfigurationError(
            "Weather Trino pool_slots must be between 1 and "
            f"{TRINO_WEATHER_HEAVY_POOL_SLOTS}"
        )
    selected_pool = weather_heavy_pool(legacy_pool)
    if selected_pool != TRINO_WEATHER_HEAVY_POOL:
        # The legacy pool is registered with one slot.  Keep the fallback
        # schedulable even if a caller requested an exclusive active-lane lock.
        return {"pool": selected_pool, "pool_slots": 1}
    return {"pool": selected_pool, "pool_slots": pool_slots}


def _configured_daily_attempt_limit() -> int:
    raw_value = os.getenv(
        DAILY_ATTEMPT_LIMIT_ENV,
        str(DEFAULT_DAILY_ATTEMPT_LIMIT),
    )
    try:
        limit = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise KmaAttemptLedgerConfigurationError(
            f"{DAILY_ATTEMPT_LIMIT_ENV} daily attempt limit must be an integer"
        ) from exc
    if limit <= 0 or limit >= PROVIDER_DAILY_ATTEMPT_QUOTA:
        raise KmaAttemptLedgerConfigurationError(
            f"{DAILY_ATTEMPT_LIMIT_ENV} daily attempt limit must be greater than "
            f"zero and below {PROVIDER_DAILY_ATTEMPT_QUOTA}"
        )
    return limit


def _configured_ledger_path() -> tuple[Path, Path]:
    control_root = Path(os.getenv(CONTROL_ROOT_ENV, str(DEFAULT_CONTROL_ROOT)))
    ledger_path = Path(
        os.getenv(ATTEMPT_LEDGER_PATH_ENV, str(DEFAULT_ATTEMPT_LEDGER_PATH))
    )
    resolved_root = control_root.expanduser().resolve(strict=False)
    resolved_path = ledger_path.expanduser().resolve(strict=False)
    try:
        resolved_path.relative_to(resolved_root)
    except ValueError as exc:
        raise KmaAttemptLedgerConfigurationError(
            f"{ATTEMPT_LEDGER_PATH_ENV} must be beneath the configured control root"
        ) from exc
    if resolved_path == resolved_root:
        raise KmaAttemptLedgerConfigurationError(
            f"{ATTEMPT_LEDGER_PATH_ENV} must name a file beneath the control root"
        )
    return resolved_root, resolved_path


def initialize_attempt_ledger() -> Path:
    """Idempotently bootstrap schema v1; normal runtime never calls this."""
    control_root, ledger_path = _configured_ledger_path()
    control_root.mkdir(parents=True, exist_ok=True)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with sqlite3.connect(ledger_path) as connection:
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS schema_version ("
                "version INTEGER PRIMARY KEY NOT NULL)"
            )
            row = connection.execute("SELECT version FROM schema_version").fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO schema_version(version) VALUES (?)",
                    (ATTEMPT_LEDGER_SCHEMA_VERSION,),
                )
            elif row != (ATTEMPT_LEDGER_SCHEMA_VERSION,):
                raise KmaAttemptLedgerConfigurationError(
                    "KMA attempt ledger has an incompatible schema version"
                )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS kma_api_attempt_reservations ("
                "reservation_id TEXT PRIMARY KEY NOT NULL,"
                "budget_date TEXT NOT NULL,"
                "source_id TEXT NOT NULL,"
                "dag_run_id TEXT NOT NULL,"
                "observed_slot TEXT NOT NULL,"
                "nx INTEGER NOT NULL,"
                "ny INTEGER NOT NULL,"
                "attempt_ordinal INTEGER NOT NULL CHECK(attempt_ordinal > 0),"
                "reserved_at TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_kma_attempt_budget_date "
                "ON kma_api_attempt_reservations(budget_date)"
            )
    except sqlite3.Error as exc:
        raise KmaAttemptLedgerConfigurationError(
            "KMA attempt ledger initialization failed"
        ) from exc
    _validate_attempt_ledger_schema(ledger_path)
    return ledger_path


def _open_existing_ledger(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"file:{path.as_posix()}?mode=rw",
        uri=True,
        isolation_level=None,
        timeout=30,
    )
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def _validate_attempt_ledger_schema(path: Path) -> None:
    if not path.is_file():
        raise KmaAttemptLedgerConfigurationError(
            "KMA attempt ledger is missing; initialize it with init-ledger before "
            "enabling runtime"
        )
    try:
        with _open_existing_ledger(path) as connection:
            version_rows = connection.execute(
                "SELECT version FROM schema_version"
            ).fetchall()
            if version_rows != [(ATTEMPT_LEDGER_SCHEMA_VERSION,)]:
                raise KmaAttemptLedgerConfigurationError(
                    "KMA attempt ledger schema version is incompatible"
                )
            columns = tuple(
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(kma_api_attempt_reservations)"
                )
            )
            if columns != _RESERVATION_COLUMNS:
                raise KmaAttemptLedgerConfigurationError(
                    "KMA attempt ledger reservation schema is incompatible"
                )
    except KmaAttemptLedgerConfigurationError:
        raise
    except sqlite3.Error as exc:
        raise KmaAttemptLedgerConfigurationError(
            "KMA attempt ledger schema cannot be validated"
        ) from exc


def _require_reservation(reservation: AttemptReservation) -> tuple[str, str]:
    required_strings = (
        reservation.reservation_id,
        reservation.source_id,
        reservation.dag_run_id,
        reservation.observed_slot,
    )
    if any(not value or not value.strip() for value in required_strings):
        raise KmaAttemptLedgerConfigurationError(
            "KMA attempt reservation fields must be non-empty"
        )
    if reservation.attempt_ordinal <= 0:
        raise KmaAttemptLedgerConfigurationError(
            "KMA attempt ordinal must be positive"
        )
    if reservation.reserved_at.tzinfo is None:
        raise KmaAttemptLedgerConfigurationError(
            "KMA attempt reservation timestamp must be timezone-aware"
        )
    reserved_at = reservation.reserved_at.astimezone(timezone.utc).isoformat()
    budget_date = reservation.reserved_at.astimezone(_KST).date().isoformat()
    return budget_date, reserved_at


class SqliteAttemptLedger:
    """Shared, fail-closed daily physical-attempt reservation ledger."""

    def __init__(self, path: Path, daily_limit: int) -> None:
        self._path = path
        self._daily_limit = daily_limit

    @classmethod
    def from_environment(cls) -> "SqliteAttemptLedger":
        daily_limit = _configured_daily_attempt_limit()
        _, ledger_path = _configured_ledger_path()
        _validate_attempt_ledger_schema(ledger_path)
        return cls(ledger_path, daily_limit)

    def reserve(self, reservation: AttemptReservation) -> AttemptReservationResult:
        budget_date, reserved_at = _require_reservation(reservation)
        values = (
            reservation.reservation_id,
            budget_date,
            reservation.source_id,
            reservation.dag_run_id,
            reservation.observed_slot,
            reservation.nx,
            reservation.ny,
            reservation.attempt_ordinal,
            reserved_at,
        )
        connection = _open_existing_ledger(self._path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT " + ",".join(_RESERVATION_COLUMNS)
                + " FROM kma_api_attempt_reservations WHERE reservation_id = ?",
                (reservation.reservation_id,),
            ).fetchone()
            if existing is not None:
                if existing != values:
                    raise KmaAttemptLedgerConfigurationError(
                        "KMA attempt reservation id collision"
                    )
                daily_count = connection.execute(
                    "SELECT COUNT(*) FROM kma_api_attempt_reservations "
                    "WHERE budget_date = ?",
                    (budget_date,),
                ).fetchone()[0]
                connection.commit()
                return AttemptReservationResult(
                    reservation.reservation_id,
                    budget_date,
                    daily_count,
                    False,
                )
            daily_count = connection.execute(
                "SELECT COUNT(*) FROM kma_api_attempt_reservations "
                "WHERE budget_date = ?",
                (budget_date,),
            ).fetchone()[0]
            if daily_count >= self._daily_limit:
                raise KmaDailyAttemptLimitExceeded(
                    "KMA shared daily attempt limit is exhausted"
                )
            connection.execute(
                "INSERT INTO kma_api_attempt_reservations ("
                + ",".join(_RESERVATION_COLUMNS)
                + ") VALUES (?,?,?,?,?,?,?,?,?)",
                values,
            )
            connection.commit()
            return AttemptReservationResult(
                reservation.reservation_id,
                budget_date,
                daily_count + 1,
                True,
            )
        except (KmaAttemptLedgerConfigurationError, KmaDailyAttemptLimitExceeded):
            connection.rollback()
            raise
        except sqlite3.Error as exc:
            connection.rollback()
            raise KmaAttemptLedgerConfigurationError(
                "KMA attempt reservation failed closed"
            ) from exc
        finally:
            connection.close()


class PhysicalAttemptBudgetHook:
    """Callable to run immediately before each physical HTTP invocation."""

    def __init__(
        self,
        ledger: SqliteAttemptLedger,
        *,
        clock: Callable[[], datetime],
    ) -> None:
        self._ledger = ledger
        self._clock = clock

    def __call__(self, attempt: PhysicalAttempt) -> AttemptReservationResult:
        return self._ledger.reserve(
            AttemptReservation(
                reservation_id=attempt.reservation_id,
                source_id=attempt.source_id,
                dag_run_id=attempt.dag_run_id,
                observed_slot=attempt.observed_slot,
                nx=attempt.nx,
                ny=attempt.ny,
                attempt_ordinal=attempt.attempt_ordinal,
                reserved_at=self._clock(),
            )
        )


def deadline_checkpoint_key(identity: CycleIdentity) -> str:
    segments = (
        identity.source_id,
        identity.observed_slot,
        identity.dag_id,
        identity.dag_run_id,
    )
    if any(not value or not value.strip() for value in segments):
        raise CycleDeadlineConfigurationError(
            "KMA cycle deadline identity fields must be non-empty"
        )
    encoded = tuple(quote(value, safe="") for value in segments)
    return (
        f"{DEADLINE_CHECKPOINT_PREFIX}/source={encoded[0]}/slot={encoded[1]}/"
        f"dag_id={encoded[2]}/run_id={encoded[3]}.json"
    )


def _utc_datetime(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None:
        raise CycleDeadlineConfigurationError(
            f"KMA cycle deadline {field} must be timezone-aware"
        )
    return value.astimezone(timezone.utc)


def serialize_deadline_anchor(anchor: DeadlineAnchor) -> bytes:
    document = asdict(anchor)
    document["schema_version"] = 1
    document["duration_seconds"] = int(COLLECTION_DEADLINE.total_seconds())
    document["started_at"] = _utc_datetime(
        anchor.started_at,
        field="started_at",
    ).isoformat()
    document["deadline_at"] = _utc_datetime(
        anchor.deadline_at,
        field="deadline_at",
    ).isoformat()
    return json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _deserialize_deadline_anchor(payload: bytes) -> DeadlineAnchor:
    try:
        document = json.loads(payload.decode("utf-8"))
        if document.get("schema_version") != 1:
            raise ValueError("schema version")
        if document.get("duration_seconds") != int(
            COLLECTION_DEADLINE.total_seconds()
        ):
            raise ValueError("duration")
        return DeadlineAnchor(
            dag_id=str(document["dag_id"]),
            dag_run_id=str(document["dag_run_id"]),
            source_id=str(document["source_id"]),
            observed_slot=str(document["observed_slot"]),
            started_at=_utc_datetime(
                datetime.fromisoformat(document["started_at"]),
                field="started_at",
            ),
            deadline_at=_utc_datetime(
                datetime.fromisoformat(document["deadline_at"]),
                field="deadline_at",
            ),
        )
    except (KeyError, TypeError, ValueError, UnicodeDecodeError) as exc:
        raise CycleDeadlineConfigurationError(
            "KMA cycle deadline checkpoint is malformed"
        ) from exc


def _validate_deadline_anchor(
    anchor: DeadlineAnchor,
    identity: CycleIdentity,
) -> DeadlineAnchor:
    expected_identity = (
        identity.dag_id,
        identity.dag_run_id,
        identity.source_id,
        identity.observed_slot,
    )
    actual_identity = (
        anchor.dag_id,
        anchor.dag_run_id,
        anchor.source_id,
        anchor.observed_slot,
    )
    if actual_identity != expected_identity:
        raise CycleDeadlineConfigurationError(
            "KMA cycle deadline checkpoint identity does not match"
        )
    if anchor.deadline_at - anchor.started_at != COLLECTION_DEADLINE:
        raise CycleDeadlineConfigurationError(
            "KMA cycle deadline checkpoint must span exactly 40 minutes"
        )
    return anchor


def initialize_cycle_deadline(
    store: DeadlineCheckpointStore,
    identity: CycleIdentity,
    *,
    now: Callable[[], datetime],
) -> DeadlineAnchor:
    """Conditionally create or load the immutable per-run/per-slot anchor."""
    key = deadline_checkpoint_key(identity)
    started_at = _utc_datetime(now(), field="started_at")
    candidate = DeadlineAnchor(
        dag_id=identity.dag_id,
        dag_run_id=identity.dag_run_id,
        source_id=identity.source_id,
        observed_slot=identity.observed_slot,
        started_at=started_at,
        deadline_at=started_at + COLLECTION_DEADLINE,
    )
    created = store.write_bytes_if_absent(
        key,
        serialize_deadline_anchor(candidate),
        "application/json",
    )
    if created:
        return candidate
    try:
        persisted = _deserialize_deadline_anchor(store.read_bytes(key))
    except (KeyError, FileNotFoundError) as exc:
        raise CycleDeadlineConfigurationError(
            "KMA cycle deadline checkpoint disappeared after write conflict"
        ) from exc
    return _validate_deadline_anchor(persisted, identity)


def _required_seconds(value: float, *, field: str) -> float:
    try:
        seconds = float(value)
    except (TypeError, ValueError) as exc:
        raise CycleDeadlineConfigurationError(
            f"KMA cycle deadline {field} must be numeric"
        ) from exc
    if not math.isfinite(seconds) or seconds < 0:
        raise CycleDeadlineConfigurationError(
            f"KMA cycle deadline {field} must be finite and non-negative"
        )
    return seconds


class CycleDeadline:
    """Conservative deadline view combining durable UTC and local monotonic time."""

    def __init__(
        self,
        anchor: DeadlineAnchor,
        *,
        wall_clock: Callable[[], datetime],
        monotonic_clock: Callable[[], float],
    ) -> None:
        self._anchor = anchor
        self._wall_clock = wall_clock
        self._monotonic_clock = monotonic_clock
        self._monotonic_started = monotonic_clock()
        initial_wall = _utc_datetime(wall_clock(), field="wall clock")
        self._initial_remaining = max(
            0.0,
            (anchor.deadline_at - initial_wall).total_seconds(),
        )
        self._last_remaining = self._initial_remaining
        self._lock = threading.Lock()

    def remaining_seconds(self) -> float:
        wall_now = _utc_datetime(self._wall_clock(), field="wall clock")
        durable_remaining = max(
            0.0,
            (self._anchor.deadline_at - wall_now).total_seconds(),
        )
        monotonic_elapsed = max(
            0.0,
            self._monotonic_clock() - self._monotonic_started,
        )
        monotonic_remaining = max(
            0.0,
            self._initial_remaining - monotonic_elapsed,
        )
        with self._lock:
            self._last_remaining = min(
                self._last_remaining,
                durable_remaining,
                monotonic_remaining,
            )
            return self._last_remaining

    def _require(self, required_seconds: float, *, action: str) -> None:
        if self.remaining_seconds() < required_seconds:
            raise CycleDeadlineExceeded(
                f"KMA cycle deadline cannot admit {action}"
            )

    def require_request(
        self,
        *,
        request_timeout_seconds: float,
        headroom_seconds: float,
    ) -> None:
        timeout = _required_seconds(
            request_timeout_seconds,
            field="request timeout",
        )
        headroom = _required_seconds(headroom_seconds, field="request headroom")
        self._require(timeout + headroom, action="request")

    def require_retry_sleep(
        self,
        *,
        sleep_seconds: float,
        request_headroom_seconds: float,
    ) -> None:
        sleep = _required_seconds(sleep_seconds, field="retry sleep")
        headroom = _required_seconds(
            request_headroom_seconds,
            field="request headroom",
        )
        self._require(sleep + headroom, action="retry sleep")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KMA shared coordination controls")
    parser.add_argument("command", choices=("init-ledger",))
    args = parser.parse_args(argv)
    if args.command == "init-ledger":
        path = initialize_attempt_ledger()
        print(f"initialized={path} schema_version={ATTEMPT_LEDGER_SCHEMA_VERSION}")
        return 0
    return 2


__all__ = [
    "ATTEMPT_LEDGER_SCHEMA_VERSION",
    "AttemptReservation",
    "AttemptReservationResult",
    "COLLECTION_DEADLINE",
    "CycleDeadline",
    "CycleDeadlineConfigurationError",
    "CycleDeadlineExceeded",
    "CycleIdentity",
    "DeadlineAnchor",
    "KmaCoordinationConfigurationError",
    "KmaAttemptLedgerConfigurationError",
    "KmaDailyAttemptLimitExceeded",
    "PhysicalAttempt",
    "PhysicalAttemptBudgetHook",
    "SHARED_GUARDS_ENV",
    "SqliteAttemptLedger",
    "deadline_checkpoint_key",
    "initialize_attempt_ledger",
    "initialize_cycle_deadline",
    "kma_api_pool_kwargs",
    "main",
    "serialize_deadline_anchor",
    "shared_guards_enabled",
    "weather_heavy_pool",
    "weather_heavy_pool_kwargs",
]


if __name__ == "__main__":
    raise SystemExit(main())
