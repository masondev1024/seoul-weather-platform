"""Immutable, checkpoint-resumable Raw landing for KMA current observations."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Callable, Protocol, Sequence
from urllib.parse import quote

from weather_ingest.errors import (
    WeatherCompletenessError,
    WeatherRawIntegrityError,
    WeatherSourceSchemaError,
)
from weather_ingest.kma_coordination import (
    CycleDeadline,
    CycleIdentity,
    initialize_cycle_deadline,
)
from weather_ingest.kma_observation import (
    KST,
    REQUIRED_CATEGORIES,
    SOURCE_ID,
    normalize_observation_slot,
    observation_slot_utc,
    parse_and_normalize_kma_observation,
)


DEFAULT_RAW_PREFIX = "raw/weather_observation/kma_ultra_srt_ncst"
DEFAULT_CHECKPOINT_PREFIX = "ops/weather/_checkpoints/kma_ultra_srt_ncst"
DEFAULT_MANIFEST_PREFIX = "ops/weather/_manifests/kma_ultra_srt_ncst"


@dataclass(frozen=True, slots=True)
class ObservationRunIdentity:
    dag_id: str
    run_id: str


@dataclass(frozen=True, slots=True)
class ObservationGrid:
    nx: int
    ny: int

    def __post_init__(self) -> None:
        for field, value in (("nx", self.nx), ("ny", self.ny)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise WeatherCompletenessError(
                    f"KMA observation grid {field} must be a positive integer"
                )


@dataclass(frozen=True, slots=True)
class ObservationLandingRequest:
    base_date: str
    base_time: str
    grids: tuple[ObservationGrid, ...]


@dataclass(frozen=True, slots=True)
class ObservationCheckpoint:
    source_id: str
    dag_id: str
    run_id: str
    base_date: str
    base_time: str
    observed_slot: str
    nx: int
    ny: int
    request_id: str
    raw_object_key: str
    payload_sha256: str
    http_status: int
    collected_at: datetime
    category_count: int
    categories: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ObservationRawObject:
    request_id: str
    raw_object_key: str
    payload_sha256: str
    http_status: int
    collected_at: datetime
    nx: int
    ny: int
    category_count: int
    categories: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ObservationLandingBatch:
    source_id: str
    dag_id: str
    run_id: str
    base_date: str
    base_time: str
    observed_slot: str
    raw_objects: tuple[ObservationRawObject, ...]
    grid_count: int
    row_count: int
    api_request_count: int
    reused_grid_count: int
    manifest_key: str
    is_publishable: bool

    def to_xcom(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "base_date": self.base_date,
            "base_time": self.base_time,
            "observed_slot": self.observed_slot,
            "raw_objects": [
                {
                    "request_id": raw.request_id,
                    "raw_object_key": raw.raw_object_key,
                    "payload_sha256": raw.payload_sha256,
                    "http_status": raw.http_status,
                    "collected_at": raw.collected_at.isoformat(),
                    "nx": raw.nx,
                    "ny": raw.ny,
                    "category_count": raw.category_count,
                    "categories": list(raw.categories),
                }
                for raw in self.raw_objects
            ],
            "raw_object_keys": [raw.raw_object_key for raw in self.raw_objects],
            "grid_count": self.grid_count,
            "row_count": self.row_count,
            "api_request_count": self.api_request_count,
            "reused_grid_count": self.reused_grid_count,
            "manifest_key": self.manifest_key,
            "is_publishable": self.is_publishable,
        }

    @classmethod
    def from_xcom(cls, document: dict[str, object]) -> "ObservationLandingBatch":
        try:
            raw_documents = document["raw_objects"]
            if not isinstance(raw_documents, list):
                raise TypeError("raw_objects")
            raw_objects = tuple(
                ObservationRawObject(
                    request_id=str(raw["request_id"]),
                    raw_object_key=str(raw["raw_object_key"]),
                    payload_sha256=str(raw["payload_sha256"]),
                    http_status=int(raw["http_status"]),
                    collected_at=datetime.fromisoformat(str(raw["collected_at"])),
                    nx=int(raw["nx"]),
                    ny=int(raw["ny"]),
                    category_count=int(raw["category_count"]),
                    categories=tuple(str(value) for value in raw["categories"]),
                )
                for raw in raw_documents
                if isinstance(raw, dict)
            )
            if len(raw_objects) != len(raw_documents):
                raise TypeError("raw_objects")
            return cls(
                source_id=str(document["source_id"]),
                dag_id=str(document["dag_id"]),
                run_id=str(document["run_id"]),
                base_date=str(document["base_date"]),
                base_time=str(document["base_time"]),
                observed_slot=str(document["observed_slot"]),
                raw_objects=raw_objects,
                grid_count=int(document["grid_count"]),
                row_count=int(document["row_count"]),
                api_request_count=int(document["api_request_count"]),
                reused_grid_count=int(document["reused_grid_count"]),
                manifest_key=str(document["manifest_key"]),
                is_publishable=bool(document["is_publishable"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise WeatherRawIntegrityError(
                "KMA observation landing XCom contract is malformed"
            ) from exc


class ObservationSource(Protocol):
    def fetch(
        self,
        *,
        base_date: str,
        base_time: str,
        nx: int,
        ny: int,
        dag_run_id: str,
        observed_slot: str,
        request_id: str,
    ) -> tuple[int, bytes]: ...


class ObservationRawStore(Protocol):
    def exists(self, key: str) -> bool: ...

    def read_bytes(self, key: str) -> bytes: ...

    def write_bytes_if_absent(
        self,
        key: str,
        payload: bytes,
        content_type: str,
    ) -> bool: ...


def _safe(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WeatherRawIntegrityError(
            f"KMA observation {field} must be non-empty"
        )
    return quote(value.strip(), safe="")


def _observed_slot(base_date: str, base_time: str) -> str:
    return observation_slot_utc(base_date, base_time).astimezone(KST).isoformat()


def observation_raw_object_key(
    run: ObservationRunIdentity,
    base_date: str,
    base_time: str,
    grid: ObservationGrid,
    request_id: str,
    payload: bytes,
    *,
    raw_prefix: str = DEFAULT_RAW_PREFIX,
) -> str:
    date, hour = normalize_observation_slot(base_date, base_time)
    payload_hash = hashlib.sha256(payload).hexdigest()
    return (
        f"{raw_prefix.rstrip('/')}/observed_date={date[:4]}-{date[4:6]}-{date[6:]}/"
        f"observed_hour={hour[:2]}/run_id={_safe(run.run_id, field='run_id')}/"
        f"nx={grid.nx}/ny={grid.ny}/"
        f"{_safe(request_id, field='request_id')}_{payload_hash}.json"
    )


def observation_checkpoint_key(
    run: ObservationRunIdentity,
    base_date: str,
    base_time: str,
    grid: ObservationGrid,
    *,
    checkpoint_prefix: str = DEFAULT_CHECKPOINT_PREFIX,
) -> str:
    date, hour = normalize_observation_slot(base_date, base_time)
    return (
        f"{checkpoint_prefix.rstrip('/')}/observed_date={date[:4]}-{date[4:6]}-{date[6:]}/"
        f"observed_hour={hour[:2]}/dag_id={_safe(run.dag_id, field='dag_id')}/"
        f"run_id={_safe(run.run_id, field='run_id')}/nx={grid.nx}/ny={grid.ny}.json"
    )


def _manifest_key(
    run: ObservationRunIdentity,
    base_date: str,
    base_time: str,
    *,
    manifest_prefix: str,
) -> str:
    date, hour = normalize_observation_slot(base_date, base_time)
    return (
        f"{manifest_prefix.rstrip('/')}/observed_date={date[:4]}-{date[4:6]}-{date[6:]}/"
        f"observed_hour={hour[:2]}/dag_id={_safe(run.dag_id, field='dag_id')}/"
        f"run_id={_safe(run.run_id, field='run_id')}.json"
    )


def _checkpoint_bytes(checkpoint: ObservationCheckpoint) -> bytes:
    document = asdict(checkpoint)
    document["schema_version"] = 1
    document["collected_at"] = checkpoint.collected_at.isoformat()
    document["categories"] = list(checkpoint.categories)
    return json.dumps(
        document,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _checkpoint_from_bytes(payload: bytes) -> ObservationCheckpoint:
    try:
        document = json.loads(payload.decode("utf-8"))
        if document.get("schema_version") != 1:
            raise ValueError("schema version")
        return ObservationCheckpoint(
            source_id=str(document["source_id"]),
            dag_id=str(document["dag_id"]),
            run_id=str(document["run_id"]),
            base_date=str(document["base_date"]),
            base_time=str(document["base_time"]),
            observed_slot=str(document["observed_slot"]),
            nx=int(document["nx"]),
            ny=int(document["ny"]),
            request_id=str(document["request_id"]),
            raw_object_key=str(document["raw_object_key"]),
            payload_sha256=str(document["payload_sha256"]),
            http_status=int(document["http_status"]),
            collected_at=datetime.fromisoformat(document["collected_at"]),
            category_count=int(document["category_count"]),
            categories=tuple(str(value) for value in document["categories"]),
        )
    except (
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise WeatherRawIntegrityError(
            "KMA observation checkpoint is malformed"
        ) from exc


def _validate_grids(
    grids: Sequence[ObservationGrid],
    *,
    expected_grid_count: int,
) -> tuple[ObservationGrid, ...]:
    keys = [(grid.nx, grid.ny) for grid in grids]
    if len(set(keys)) != len(keys):
        raise WeatherCompletenessError(
            "KMA observation grid collection contains a duplicate grid"
        )
    if len(keys) != expected_grid_count:
        raise WeatherCompletenessError(
            "KMA observation grid count mismatch: "
            f"expected={expected_grid_count}, actual={len(keys)}"
        )
    return tuple(grids)


def _validate_checkpoint_identity(
    checkpoint: ObservationCheckpoint,
    *,
    run: ObservationRunIdentity,
    base_date: str,
    base_time: str,
    observed_slot: str,
    grid: ObservationGrid,
) -> None:
    if checkpoint.source_id != SOURCE_ID:
        raise WeatherRawIntegrityError(
            "KMA observation checkpoint source does not match"
        )
    if checkpoint.dag_id != run.dag_id or checkpoint.run_id != run.run_id:
        raise WeatherRawIntegrityError(
            "KMA observation checkpoint run identity does not match"
        )
    if (
        checkpoint.base_date != base_date
        or checkpoint.base_time != base_time
        or checkpoint.observed_slot != observed_slot
    ):
        raise WeatherRawIntegrityError(
            "KMA observation checkpoint slot does not match"
        )
    if checkpoint.nx != grid.nx or checkpoint.ny != grid.ny:
        raise WeatherRawIntegrityError(
            "KMA observation checkpoint grid does not match"
        )
    if checkpoint.category_count != len(REQUIRED_CATEGORIES) or (
        checkpoint.categories != REQUIRED_CATEGORIES
    ):
        raise WeatherRawIntegrityError(
            "KMA observation checkpoint category contract does not match"
        )


def _raw_from_checkpoint(
    checkpoint: ObservationCheckpoint,
    payload: bytes,
    *,
    run: ObservationRunIdentity,
    grid: ObservationGrid,
) -> ObservationRawObject:
    payload_hash = hashlib.sha256(payload).hexdigest()
    if payload_hash != checkpoint.payload_sha256:
        raise WeatherRawIntegrityError(
            "KMA observation checkpointed raw payload hash does not match"
        )
    expected_key = observation_raw_object_key(
        run,
        checkpoint.base_date,
        checkpoint.base_time,
        grid,
        checkpoint.request_id,
        payload,
    )
    if expected_key != checkpoint.raw_object_key:
        raise WeatherRawIntegrityError(
            "KMA observation checkpoint raw object key does not match hash context"
        )
    try:
        _, records = parse_and_normalize_kma_observation(
            payload,
            base_date=checkpoint.base_date,
            base_time=checkpoint.base_time,
            nx=grid.nx,
            ny=grid.ny,
            collected_at=checkpoint.collected_at,
        )
    except WeatherSourceSchemaError as exc:
        raise WeatherRawIntegrityError(
            "KMA observation checkpointed raw context is invalid"
        ) from exc
    categories = tuple(record.category for record in records)
    if categories != REQUIRED_CATEGORIES:
        raise WeatherRawIntegrityError(
            "KMA observation checkpointed raw categories are invalid"
        )
    return ObservationRawObject(
        request_id=checkpoint.request_id,
        raw_object_key=checkpoint.raw_object_key,
        payload_sha256=checkpoint.payload_sha256,
        http_status=checkpoint.http_status,
        collected_at=checkpoint.collected_at,
        nx=checkpoint.nx,
        ny=checkpoint.ny,
        category_count=checkpoint.category_count,
        categories=checkpoint.categories,
    )


def build_complete_observation_manifest(
    run: ObservationRunIdentity,
    *,
    base_date: str,
    base_time: str,
    checkpoints: Sequence[ObservationCheckpoint],
    expected_grid_count: int,
) -> dict[str, object]:
    date, hour = normalize_observation_slot(base_date, base_time)
    observed_slot = _observed_slot(date, hour)
    grid_keys = [(checkpoint.nx, checkpoint.ny) for checkpoint in checkpoints]
    if len(set(grid_keys)) != len(grid_keys):
        raise WeatherCompletenessError(
            "KMA observation complete manifest contains a duplicate grid"
        )
    if len(checkpoints) != expected_grid_count:
        raise WeatherCompletenessError(
            "KMA observation complete manifest grid count mismatch: "
            f"expected={expected_grid_count}, actual={len(checkpoints)}"
        )
    row_count = sum(checkpoint.category_count for checkpoint in checkpoints)
    expected_rows = expected_grid_count * len(REQUIRED_CATEGORIES)
    if row_count != expected_rows:
        raise WeatherCompletenessError(
            "KMA observation complete manifest row count mismatch: "
            f"expected={expected_rows}, row_count={row_count}"
        )
    for checkpoint in checkpoints:
        _validate_checkpoint_identity(
            checkpoint,
            run=run,
            base_date=date,
            base_time=hour,
            observed_slot=observed_slot,
            grid=ObservationGrid(checkpoint.nx, checkpoint.ny),
        )
    ordered = sorted(checkpoints, key=lambda item: (item.nx, item.ny))
    return {
        "schema_version": 1,
        "status": "complete",
        "source_id": SOURCE_ID,
        "dag_id": run.dag_id,
        "run_id": run.run_id,
        "base_date": date,
        "base_time": hour,
        "observed_slot": observed_slot,
        "grid_count": len(ordered),
        "row_count": row_count,
        "categories": list(REQUIRED_CATEGORIES),
        "raw_objects": [
            {
                "nx": item.nx,
                "ny": item.ny,
                "request_id": item.request_id,
                "raw_object_key": item.raw_object_key,
                "payload_sha256": item.payload_sha256,
                "category_count": item.category_count,
            }
            for item in ordered
        ],
    }


class KmaObservationLanding:
    def __init__(
        self,
        *,
        source_factory: Callable[[CycleDeadline], ObservationSource],
        raw_store: ObservationRawStore,
        clock: Callable[[], datetime],
        monotonic_clock: Callable[[], float],
        request_id: Callable[[], str],
        expected_grid_count: int = 80,
        raw_prefix: str = DEFAULT_RAW_PREFIX,
        checkpoint_prefix: str = DEFAULT_CHECKPOINT_PREFIX,
        manifest_prefix: str = DEFAULT_MANIFEST_PREFIX,
    ) -> None:
        if (
            isinstance(expected_grid_count, bool)
            or not isinstance(expected_grid_count, int)
            or expected_grid_count < 1
        ):
            raise WeatherCompletenessError(
                "KMA observation expected grid count must be positive"
            )
        self._source_factory = source_factory
        self._raw_store = raw_store
        self._clock = clock
        self._monotonic_clock = monotonic_clock
        self._request_id = request_id
        self._expected_grid_count = expected_grid_count
        self._raw_prefix = raw_prefix
        self._checkpoint_prefix = checkpoint_prefix
        self._manifest_prefix = manifest_prefix

    def collect(
        self,
        run: ObservationRunIdentity,
        request: ObservationLandingRequest,
    ) -> ObservationLandingBatch:
        base_date, base_time = normalize_observation_slot(
            request.base_date,
            request.base_time,
        )
        grids = _validate_grids(
            request.grids,
            expected_grid_count=self._expected_grid_count,
        )
        observed_slot = _observed_slot(base_date, base_time)
        anchor = initialize_cycle_deadline(
            self._raw_store,
            CycleIdentity(
                dag_id=run.dag_id,
                dag_run_id=run.run_id,
                source_id=SOURCE_ID,
                observed_slot=observed_slot,
            ),
            now=self._clock,
        )
        deadline = CycleDeadline(
            anchor,
            wall_clock=self._clock,
            monotonic_clock=self._monotonic_clock,
        )
        source = self._source_factory(deadline)
        checkpoints: list[ObservationCheckpoint] = []
        raw_objects: list[ObservationRawObject] = []
        api_request_count = 0
        reused_grid_count = 0

        for grid in grids:
            checkpoint_key = observation_checkpoint_key(
                run,
                base_date,
                base_time,
                grid,
                checkpoint_prefix=self._checkpoint_prefix,
            )
            if self._raw_store.exists(checkpoint_key):
                checkpoint = _checkpoint_from_bytes(
                    self._raw_store.read_bytes(checkpoint_key)
                )
                _validate_checkpoint_identity(
                    checkpoint,
                    run=run,
                    base_date=base_date,
                    base_time=base_time,
                    observed_slot=observed_slot,
                    grid=grid,
                )
                if not self._raw_store.exists(checkpoint.raw_object_key):
                    raise WeatherRawIntegrityError(
                        "KMA observation checkpointed raw object is missing"
                    )
                raw = _raw_from_checkpoint(
                    checkpoint,
                    self._raw_store.read_bytes(checkpoint.raw_object_key),
                    run=run,
                    grid=grid,
                )
                reused_grid_count += 1
            else:
                raw, checkpoint = self._collect_grid(
                    source,
                    run=run,
                    base_date=base_date,
                    base_time=base_time,
                    observed_slot=observed_slot,
                    grid=grid,
                    checkpoint_key=checkpoint_key,
                )
                api_request_count += 1
            checkpoints.append(checkpoint)
            raw_objects.append(raw)

        manifest = build_complete_observation_manifest(
            run,
            base_date=base_date,
            base_time=base_time,
            checkpoints=checkpoints,
            expected_grid_count=self._expected_grid_count,
        )
        manifest_key = _manifest_key(
            run,
            base_date,
            base_time,
            manifest_prefix=self._manifest_prefix,
        )
        manifest_payload = json.dumps(
            manifest,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        self._write_identical_or_absent(
            manifest_key,
            manifest_payload,
            conflict_kind="manifest",
        )
        return ObservationLandingBatch(
            source_id=SOURCE_ID,
            dag_id=run.dag_id,
            run_id=run.run_id,
            base_date=base_date,
            base_time=base_time,
            observed_slot=observed_slot,
            raw_objects=tuple(raw_objects),
            grid_count=len(raw_objects),
            row_count=sum(raw.category_count for raw in raw_objects),
            api_request_count=api_request_count,
            reused_grid_count=reused_grid_count,
            manifest_key=manifest_key,
            is_publishable=True,
        )

    def _collect_grid(
        self,
        source: ObservationSource,
        *,
        run: ObservationRunIdentity,
        base_date: str,
        base_time: str,
        observed_slot: str,
        grid: ObservationGrid,
        checkpoint_key: str,
    ) -> tuple[ObservationRawObject, ObservationCheckpoint]:
        request_id = self._request_id()
        status, payload = source.fetch(
            base_date=base_date,
            base_time=base_time,
            nx=grid.nx,
            ny=grid.ny,
            dag_run_id=run.run_id,
            observed_slot=observed_slot,
            request_id=request_id,
        )
        if not 200 <= status < 300:
            raise WeatherSourceSchemaError(
                "KMA observation source returned a non-success status"
            )
        collected_at = self._clock()
        metadata, records = parse_and_normalize_kma_observation(
            payload,
            base_date=base_date,
            base_time=base_time,
            nx=grid.nx,
            ny=grid.ny,
            collected_at=collected_at,
        )
        categories = tuple(record.category for record in records)
        if categories != REQUIRED_CATEGORIES:
            raise WeatherSourceSchemaError(
                "KMA observation normalized categories are incomplete"
            )
        raw_key = observation_raw_object_key(
            run,
            base_date,
            base_time,
            grid,
            request_id,
            payload,
            raw_prefix=self._raw_prefix,
        )
        self._write_identical_or_absent(
            raw_key,
            payload,
            conflict_kind="raw object",
        )
        checkpoint = ObservationCheckpoint(
            source_id=SOURCE_ID,
            dag_id=run.dag_id,
            run_id=run.run_id,
            base_date=base_date,
            base_time=base_time,
            observed_slot=observed_slot,
            nx=grid.nx,
            ny=grid.ny,
            request_id=request_id,
            raw_object_key=raw_key,
            payload_sha256=str(metadata["payload_sha256"]),
            http_status=status,
            collected_at=collected_at,
            category_count=len(records),
            categories=categories,
        )
        checkpoint_payload = _checkpoint_bytes(checkpoint)
        self._write_identical_or_absent(
            checkpoint_key,
            checkpoint_payload,
            conflict_kind="checkpoint",
        )
        return (
            ObservationRawObject(
                request_id=request_id,
                raw_object_key=raw_key,
                payload_sha256=checkpoint.payload_sha256,
                http_status=status,
                collected_at=collected_at,
                nx=grid.nx,
                ny=grid.ny,
                category_count=len(records),
                categories=categories,
            ),
            checkpoint,
        )

    def _write_identical_or_absent(
        self,
        key: str,
        payload: bytes,
        *,
        conflict_kind: str,
    ) -> None:
        created = self._raw_store.write_bytes_if_absent(
            key,
            payload,
            "application/json",
        )
        if created:
            return
        try:
            existing = self._raw_store.read_bytes(key)
        except (KeyError, FileNotFoundError) as exc:
            raise WeatherRawIntegrityError(
                f"KMA observation {conflict_kind} conflict disappeared"
            ) from exc
        if existing != payload:
            raise WeatherRawIntegrityError(
                f"KMA observation {conflict_kind} conflict is not byte-identical"
            )


__all__ = [
    "KmaObservationLanding",
    "ObservationCheckpoint",
    "ObservationGrid",
    "ObservationLandingBatch",
    "ObservationLandingRequest",
    "ObservationRawObject",
    "ObservationRunIdentity",
    "build_complete_observation_manifest",
    "observation_checkpoint_key",
    "observation_raw_object_key",
]
