"""Weather raw landing domain Module."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Callable, Protocol

from common.raw_manifest import RAW_MANIFEST_STATUS_COMPLETE, build_raw_manifest
from common.raw_path import build_raw_run_prefix
from common.raw_write import RawObjectWriteConflictError, write_immutable_raw_object
from weather_ingest.kma import KST, parse_kma_response, validate_kma_response_context
from weather_ingest.errors import (
    WeatherCompletenessError,
    WeatherRawIntegrityError,
    WeatherSourceBusinessError,
    WeatherSourceSchemaError,
)
from weather_ingest.raw_contract import normalize_kma_checkpoint_raw_object


_RAW_OBJECT_KEY = re.compile(
    r"/load_date=(?P<load_date>\d{4}-\d{2}-\d{2})/"
    r"(?:run_id=[^/]+/)?nx=(?P<nx>\d+)/ny=(?P<ny>\d+)/"
    r"(?P<collected>\d{8}T\d{6})KST_base-(?P<base_date>\d{8})"
    r"(?P<base_time>\d{4})_(?P<request_id>[^/]+)\.json$"
)


class KmaLandingIncompleteError(WeatherCompletenessError):
    """The landed KMA page set does not satisfy a grid's reported total."""


class RawObjectIntegrityError(WeatherRawIntegrityError):
    """A downloaded raw object no longer matches its recorded SHA-256."""


def verify_raw_payload_hash(
    payload: bytes, *, expected_hash: str, raw_object_key: str
) -> None:
    actual_hash = hashlib.sha256(payload).hexdigest()
    if actual_hash != expected_hash:
        raise RawObjectIntegrityError(
            f"KMA raw payload hash mismatch: raw_object_key={raw_object_key}"
        )


@dataclass(frozen=True)
class RunIdentity:
    dag_id: str
    run_id: str
    landing_load_date: str | None = None


@dataclass(frozen=True)
class KmaGrid:
    place_id: str
    nx: int
    ny: int


@dataclass(frozen=True)
class KmaLandingRequest:
    base_date: str
    base_time: str
    grids: tuple[KmaGrid, ...]
    num_of_rows: int


@dataclass(frozen=True)
class KmaRawObject:
    request_id: str
    raw_object_key: str
    payload_hash: str
    http_status: int
    collected_at: str
    place_id: str
    base_date: str
    base_time: str
    nx: int
    ny: int
    page_no: int
    num_of_rows: int
    total_count: int
    row_count: int
    page_count: int


@dataclass(frozen=True)
class KmaLandingBatch:
    raw_objects: tuple[KmaRawObject, ...]
    grid_count: int
    api_request_count: int
    reused_raw_object_count: int
    base_date: str
    base_time: str
    is_publishable: bool = True
    manifest_key: str | None = None
    landing_load_date: str | None = None

    def to_xcom(self) -> dict:
        raw_objects = [
            {
                "request_id": item.request_id,
                "raw_object_key": item.raw_object_key,
                "raw_hash": item.payload_hash,
                "http_status": item.http_status,
                "collected_at": item.collected_at,
                "place_id": item.place_id,
                "base_date": item.base_date,
                "base_time": item.base_time,
                "nx": item.nx,
                "ny": item.ny,
                "page_no": item.page_no,
                "num_of_rows": item.num_of_rows,
                "total_count": item.total_count,
                "row_count": item.row_count,
                "page_count": item.page_count,
            }
            for item in self.raw_objects
        ]
        return {
            "source_id": "kma_vilage_fcst",
            "raw_objects": raw_objects,
            "raw_object_keys": [item.raw_object_key for item in self.raw_objects],
            "grid_count": self.grid_count,
            "api_call_count": len(self.raw_objects),
            "api_request_count": self.api_request_count,
            "reused_raw_object_count": self.reused_raw_object_count,
            "raw_page_count": len(self.raw_objects),
            "expected_raw_object_count": len(self.raw_objects),
            "base_date": self.base_date,
            "base_time": self.base_time,
            "is_publishable": self.is_publishable,
            "manifest_key": self.manifest_key,
            "landing_load_date": self.landing_load_date,
        }

    @classmethod
    def from_xcom(cls, document: dict) -> "KmaLandingBatch":
        raw_objects = tuple(
            KmaRawObject(
                request_id=str(item["request_id"]),
                raw_object_key=str(item["raw_object_key"]),
                payload_hash=str(item.get("payload_hash") or item["raw_hash"]),
                http_status=int(item["http_status"]),
                collected_at=str(item["collected_at"]),
                place_id=str(item["place_id"]),
                base_date=str(item["base_date"]),
                base_time=str(item["base_time"]),
                nx=int(item["nx"]),
                ny=int(item["ny"]),
                page_no=int(item.get("page_no") or 1),
                num_of_rows=int(item.get("num_of_rows") or 1000),
                total_count=int(item.get("total_count") or 0),
                row_count=int(item.get("row_count") or 0),
                page_count=int(item.get("page_count") or 1),
            )
            for item in document.get("raw_objects") or []
        )
        return cls(
            raw_objects=raw_objects,
            grid_count=int(document.get("grid_count") or 0),
            api_request_count=int(document.get("api_request_count") or 0),
            reused_raw_object_count=int(document.get("reused_raw_object_count") or 0),
            base_date=str(document.get("base_date") or ""),
            base_time=str(document.get("base_time") or ""),
            is_publishable=bool(document.get("is_publishable", True)),
            manifest_key=(
                str(document["manifest_key"])
                if document.get("manifest_key")
                else None
            ),
            landing_load_date=(
                str(document["landing_load_date"])
                if document.get("landing_load_date")
                else None
            ),
        )


class KmaPageSource(Protocol):
    def fetch_page(
        self,
        *,
        base_date: str,
        base_time: str,
        nx: int,
        ny: int,
        page_no: int,
        num_of_rows: int,
    ) -> tuple[int, bytes]: ...


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


class KmaLanding:
    def __init__(
        self,
        *,
        source: KmaPageSource,
        raw_store: RawObjectStore,
        raw_prefix: str,
        clock: Callable[[], datetime],
        request_id: Callable[[], str],
        checkpoint_prefix: str | None = None,
    ) -> None:
        self._source = source
        self._raw_store = raw_store
        self._raw_prefix = raw_prefix.rstrip("/")
        # 미지정 = 구 위치(raw 안). 호출자가 ops 존을 주면 그쪽으로 간다(#60 약속②).
        self._checkpoint_prefix = (
            checkpoint_prefix or f"{self._raw_prefix}/_checkpoints"
        ).rstrip("/")
        self._clock = clock
        self._request_id = request_id

    @staticmethod
    def _safe_key_segment(value: str) -> str:
        return "".join(
            character if character.isalnum() or character in "._=-" else "_"
            for character in value
        )

    @staticmethod
    def _request_document(request: KmaLandingRequest) -> dict:
        return {
            "base_date": request.base_date,
            "base_time": request.base_time,
            "num_of_rows": request.num_of_rows,
            "grids": [asdict(grid) for grid in request.grids],
        }

    def _checkpoint_key(self, run: RunIdentity, request: KmaLandingRequest) -> str:
        return (
            f"{self._checkpoint_prefix}/kma_vilage_fcst/"
            f"dag_id={self._safe_key_segment(run.dag_id)}/"
            f"run_id={self._safe_key_segment(run.run_id)}/"
            f"base-{request.base_date}{request.base_time}.json"
        )

    @staticmethod
    def _validate_load_date(load_date: str) -> str:
        try:
            return datetime.strptime(load_date, "%Y-%m-%d").date().isoformat()
        except (TypeError, ValueError) as exc:
            raise WeatherSourceSchemaError(
                "KMA landing_load_date must be YYYY-MM-DD"
            ) from exc

    @staticmethod
    def _raw_object_load_dates(raw_objects: list[KmaRawObject]) -> set[str]:
        return {
            match.group("load_date")
            for item in raw_objects
            if (match := _RAW_OBJECT_KEY.search(item.raw_object_key)) is not None
        }

    def _resolve_landing_load_date(
        self,
        run: RunIdentity,
        checkpoint_load_date: str | None,
        raw_objects: list[KmaRawObject],
    ) -> str:
        candidates = {
            self._validate_load_date(value)
            for value in (
                run.landing_load_date,
                checkpoint_load_date,
                *self._raw_object_load_dates(raw_objects),
            )
            if value
        }
        if len(candidates) > 1:
            raise WeatherSourceSchemaError(
                "KMA landing run must use one landing_load_date"
            )
        if candidates:
            return candidates.pop()
        return self._clock().astimezone(KST).date().isoformat()

    def _manifest_key(self, run: RunIdentity, landing_load_date: str) -> str:
        return f"{self._raw_run_prefix(run.run_id, landing_load_date)}/_manifest.json"

    def _raw_run_prefix(self, run_id: str, landing_load_date: str) -> str:
        return build_raw_run_prefix(
            raw_prefix=self._raw_prefix,
            domain="weather",
            source_id="kma_vilage_fcst",
            load_date=landing_load_date,
            run_id=run_id,
        )

    def _write_manifest(
        self,
        run: RunIdentity,
        raw_objects: list[KmaRawObject],
        landing_load_date: str,
    ) -> str:
        key = self._manifest_key(run, landing_load_date)
        manifest = build_raw_manifest(
            run_id=run.run_id,
            dataset="kma_vilage_fcst",
            load_date=landing_load_date,
            object_keys=[item.raw_object_key for item in raw_objects],
            expected_count=len(raw_objects),
            actual_count=len(raw_objects),
            completed_at=self._clock().astimezone(KST).isoformat(),
            status=RAW_MANIFEST_STATUS_COMPLETE,
        )
        self._raw_store.write_bytes(
            key,
            json.dumps(manifest, ensure_ascii=True, sort_keys=True).encode("utf-8"),
            "application/json; charset=utf-8",
        )
        return key

    def _load_checkpoint(
        self,
        run: RunIdentity,
        request: KmaLandingRequest,
    ) -> tuple[str | None, list[KmaRawObject]]:
        checkpoint_key = self._checkpoint_key(run, request)
        if not self._raw_store.exists(checkpoint_key):
            return None, []
        checkpoint_payload = self._raw_store.read_bytes(checkpoint_key)
        try:
            document = json.loads(checkpoint_payload.decode("utf-8"))
            if not isinstance(document, dict):
                raise TypeError("checkpoint root must be an object")
            if document.get("request") != self._request_document(request):
                return None, []
            raw_objects_node = document["raw_objects"]
            if not isinstance(raw_objects_node, list):
                raise TypeError("checkpoint raw_objects must be a list")
            raw_objects = [
                KmaRawObject(**normalize_kma_checkpoint_raw_object(item))
                for item in raw_objects_node
            ]
            load_dates = self._raw_object_load_dates(raw_objects)
            checkpoint_load_date = document.get("landing_load_date")
            if checkpoint_load_date is not None and not isinstance(checkpoint_load_date, str):
                raise TypeError("checkpoint landing_load_date must be a string")
            if checkpoint_load_date:
                checkpoint_load_date = self._validate_load_date(checkpoint_load_date)
            if len(load_dates) > 1:
                raise WeatherSourceSchemaError(
                    "KMA checkpoint raw objects must share one load_date"
                )
            if load_dates and checkpoint_load_date and checkpoint_load_date not in load_dates:
                raise WeatherSourceSchemaError(
                    "KMA checkpoint landing_load_date disagrees with raw objects"
                )
            return checkpoint_load_date or next(iter(load_dates), None), raw_objects
        except (
            UnicodeError,
            json.JSONDecodeError,
            KeyError,
            TypeError,
            ValueError,
        ) as exc:
            raise WeatherSourceSchemaError(
                f"Malformed KMA landing checkpoint: {checkpoint_key}"
            ) from exc

    def _save_checkpoint(
        self,
        run: RunIdentity,
        request: KmaLandingRequest,
        raw_objects: list[KmaRawObject],
        *,
        landing_load_date: str,
    ) -> None:
        document = {
            "source_id": "kma_vilage_fcst",
            "dag_id": run.dag_id,
            "dag_run_id": run.run_id,
            "request": self._request_document(request),
            "landing_load_date": landing_load_date,
            "raw_objects": [asdict(item) for item in raw_objects],
        }
        self._raw_store.write_bytes(
            self._checkpoint_key(run, request),
            json.dumps(document, ensure_ascii=True, sort_keys=True).encode("utf-8"),
            "application/json; charset=utf-8",
        )

    def _raw_object_key(
        self,
        *,
        collected_at: datetime,
        request_id: str,
        base_date: str,
        base_time: str,
        nx: int,
        ny: int,
        landing_load_date: str,
        run_id: str,
    ) -> str:
        collected_kst = collected_at.astimezone(KST)
        return (
            f"{self._raw_run_prefix(run_id, landing_load_date)}/"
            f"nx={nx}/ny={ny}/"
            f"{collected_kst:%Y%m%dT%H%M%S}KST_"
            f"base-{base_date}{base_time}_{request_id}.json"
        )

    def _write_raw_payload(self, key: str, payload: bytes) -> None:
        try:
            write_immutable_raw_object(
                self._raw_store,
                key,
                payload,
                "application/json; charset=utf-8",
            )
        except RawObjectWriteConflictError as exc:
            raise RawObjectIntegrityError(str(exc)) from exc

    def collect(
        self,
        run: RunIdentity,
        request: KmaLandingRequest,
    ) -> KmaLandingBatch:
        checkpoint_load_date, saved_checkpoint_objects = self._load_checkpoint(run, request)
        landing_load_date = self._resolve_landing_load_date(
            run, checkpoint_load_date, saved_checkpoint_objects
        )
        checkpoint_objects = [
            item
            for item in saved_checkpoint_objects
            if self._checkpoint_object_is_trustworthy(item)
        ]
        checkpoint_pages = {
            (item.nx, item.ny, item.page_no): item for item in checkpoint_objects
        }
        raw_objects: list[KmaRawObject] = []
        self._save_checkpoint(
            run, request, raw_objects, landing_load_date=landing_load_date
        )
        api_request_count = 0
        reused_raw_object_count = 0
        for grid in request.grids:
            grid_objects: list[KmaRawObject] = []
            first_page = checkpoint_pages.get((grid.nx, grid.ny, 1))
            if first_page is None:
                first_page = self._fetch_page(
                    request,
                    grid,
                    page_no=1,
                    landing_load_date=landing_load_date,
                    run_id=run.run_id,
                )
                api_request_count += 1
            else:
                reused_raw_object_count += 1
            raw_objects.append(first_page)
            grid_objects.append(first_page)
            self._save_checkpoint(
                run, request, raw_objects, landing_load_date=landing_load_date
            )
            for page_no in range(2, first_page.page_count + 1):
                raw_object = checkpoint_pages.get((grid.nx, grid.ny, page_no))
                if raw_object is None:
                    raw_object = self._fetch_page(
                        request,
                        grid,
                        page_no=page_no,
                        landing_load_date=landing_load_date,
                        run_id=run.run_id,
                    )
                    api_request_count += 1
                else:
                    reused_raw_object_count += 1
                raw_objects.append(raw_object)
                grid_objects.append(raw_object)
                self._save_checkpoint(
                    run, request, raw_objects, landing_load_date=landing_load_date
                )
            total_count = max(item.total_count for item in grid_objects)
            parsed_rows = sum(item.row_count for item in grid_objects)
            if parsed_rows < total_count:
                raise KmaLandingIncompleteError(
                    "KMA landing incomplete: "
                    f"nx={grid.nx}, ny={grid.ny}, "
                    f"total_count={total_count}, parsed_rows={parsed_rows}"
                )
        manifest_key = self._write_manifest(run, raw_objects, landing_load_date)
        return KmaLandingBatch(
            raw_objects=tuple(raw_objects),
            grid_count=len(request.grids),
            api_request_count=api_request_count,
            reused_raw_object_count=reused_raw_object_count,
            base_date=request.base_date,
            base_time=request.base_time,
            is_publishable=True,
            manifest_key=manifest_key,
            landing_load_date=landing_load_date,
        )

    def _checkpoint_object_is_trustworthy(self, item: KmaRawObject) -> bool:
        if not self._raw_store.exists(item.raw_object_key):
            return False
        payload = self._raw_store.read_bytes(item.raw_object_key)
        return hashlib.sha256(payload).hexdigest() == item.payload_hash

    def _fetch_page(
        self,
        request: KmaLandingRequest,
        grid: KmaGrid,
        *,
        page_no: int,
        landing_load_date: str,
        run_id: str,
    ) -> KmaRawObject:
        collected_at = self._clock()
        request_id = self._request_id()
        http_status, payload = self._source.fetch_page(
            base_date=request.base_date,
            base_time=request.base_time,
            nx=grid.nx,
            ny=grid.ny,
            page_no=page_no,
            num_of_rows=request.num_of_rows,
        )
        raw_object_key = self._raw_object_key(
            collected_at=collected_at,
            request_id=request_id,
            base_date=request.base_date,
            base_time=request.base_time,
            nx=grid.nx,
            ny=grid.ny,
            landing_load_date=landing_load_date,
            run_id=run_id,
        )
        self._write_raw_payload(raw_object_key, payload)
        if not isinstance(http_status, int) or isinstance(http_status, bool):
            raise WeatherSourceSchemaError(
                "KMA response has invalid http_status: "
                f"{http_status!r}; raw_object_key={raw_object_key}"
            )
        if not 200 <= http_status < 300:
            raise WeatherSourceSchemaError(
                "KMA response is not HTTP-successful: "
                f"http_status={http_status}; raw_object_key={raw_object_key}"
            )
        try:
            metadata, rows = parse_kma_response(payload)
            validate_kma_response_context(
                rows,
                base_date=request.base_date,
                base_time=request.base_time,
                nx=grid.nx,
                ny=grid.ny,
            )
        except (WeatherSourceBusinessError, WeatherSourceSchemaError) as exc:
            raise type(exc)(f"{exc}; raw_object_key={raw_object_key}") from exc
        total_count = int(metadata["total_count"])
        page_count = max(1, math.ceil(total_count / request.num_of_rows))
        return KmaRawObject(
            request_id=request_id,
            raw_object_key=raw_object_key,
            payload_hash=hashlib.sha256(payload).hexdigest(),
            http_status=http_status,
            collected_at=collected_at.isoformat(),
            place_id=grid.place_id,
            base_date=request.base_date,
            base_time=request.base_time,
            nx=grid.nx,
            ny=grid.ny,
            page_no=page_no,
            num_of_rows=request.num_of_rows,
            total_count=total_count,
            row_count=len(rows),
            page_count=page_count,
        )

    def replay(
        self,
        raw_object_keys: list[str],
        *,
        grids: tuple[KmaGrid, ...],
        run: RunIdentity | None = None,
    ) -> KmaLandingBatch:
        grid_place_ids = {(grid.nx, grid.ny): grid.place_id for grid in grids}
        raw_objects: list[KmaRawObject] = []
        for raw_object_key in dict.fromkeys(raw_object_keys):
            match = _RAW_OBJECT_KEY.search(raw_object_key)
            if match is None:
                raise WeatherSourceSchemaError(
                    f"Unsupported KMA raw_object_key: {raw_object_key}"
                )
            payload = self._raw_store.read_bytes(raw_object_key)
            base_date = match.group("base_date")
            base_time = match.group("base_time")
            nx = int(match.group("nx"))
            ny = int(match.group("ny"))
            try:
                metadata, rows = parse_kma_response(payload)
                validate_kma_response_context(
                    rows,
                    base_date=base_date,
                    base_time=base_time,
                    nx=nx,
                    ny=ny,
                )
            except (WeatherSourceBusinessError, WeatherSourceSchemaError) as exc:
                raise type(exc)(f"{exc}; raw_object_key={raw_object_key}") from exc
            body = (json.loads(payload.decode("utf-8")).get("response") or {}).get(
                "body"
            ) or {}
            page_no = int(body.get("pageNo") or 1)
            num_of_rows = int(body.get("numOfRows") or 1000)
            total_count = int(metadata["total_count"])
            collected_at = datetime.strptime(
                match.group("collected"),
                "%Y%m%dT%H%M%S",
            ).replace(tzinfo=KST)
            raw_objects.append(
                KmaRawObject(
                    request_id=match.group("request_id"),
                    raw_object_key=raw_object_key,
                    payload_hash=hashlib.sha256(payload).hexdigest(),
                    http_status=200,
                    collected_at=collected_at.isoformat(),
                    place_id=grid_place_ids.get((nx, ny), f"kma_{nx}_{ny}"),
                    base_date=base_date,
                    base_time=base_time,
                    nx=nx,
                    ny=ny,
                    page_no=page_no,
                    num_of_rows=num_of_rows,
                    total_count=total_count,
                    row_count=len(rows),
                    page_count=max(1, math.ceil(total_count / num_of_rows)),
                )
            )
        if not raw_objects:
            raise WeatherSourceSchemaError(
                "KMA replay requires at least one raw_object_key"
            )
        base_datetimes = {(item.base_date, item.base_time) for item in raw_objects}
        if len(base_datetimes) != 1:
            raise WeatherCompletenessError(
                "KMA replay raw objects must share one base_date/base_time"
            )
        for grid_key in {(item.nx, item.ny) for item in raw_objects}:
            grid_objects = [
                item for item in raw_objects if (item.nx, item.ny) == grid_key
            ]
            total_count = max(item.total_count for item in grid_objects)
            parsed_rows = sum(item.row_count for item in grid_objects)
            expected_pages = set(
                range(1, max(item.page_count for item in grid_objects) + 1)
            )
            actual_pages = {item.page_no for item in grid_objects}
            if (
                parsed_rows < total_count
                or not expected_pages.issubset(actual_pages)
                or len(actual_pages) != len(grid_objects)
            ):
                nx, ny = grid_key
                raise KmaLandingIncompleteError(
                    "KMA replay incomplete: "
                    f"nx={nx}, ny={ny}, total_count={total_count}, "
                    f"parsed_rows={parsed_rows}, expected_pages={sorted(expected_pages)}, "
                    f"actual_pages={sorted(actual_pages)}"
                )
        base_date, base_time = next(iter(base_datetimes))
        actual_grid_keys = {(item.nx, item.ny) for item in raw_objects}
        configured_grid_keys = {(grid.nx, grid.ny) for grid in grids}
        replay_run = run or RunIdentity("weather_vilage_fcst_bronze", "replay")
        landing_load_date = self._resolve_landing_load_date(
            replay_run, None, raw_objects
        )
        manifest_key = self._write_manifest(replay_run, raw_objects, landing_load_date)
        return KmaLandingBatch(
            raw_objects=tuple(raw_objects),
            grid_count=len(actual_grid_keys),
            api_request_count=0,
            reused_raw_object_count=len(raw_objects),
            base_date=base_date,
            base_time=base_time,
            is_publishable=actual_grid_keys == configured_grid_keys,
            manifest_key=manifest_key,
            landing_load_date=landing_load_date,
        )
