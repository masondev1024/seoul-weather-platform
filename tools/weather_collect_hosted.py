"""Bounded, checkpointed KMA RAW collection for a dedicated GitHub runner.

This entry point has no Airflow dependency, writes only its hosted RAW runs and
control objects, and never promotes data into Bronze or downstream tables.
"""
from __future__ import annotations

import argparse
import functools
import hashlib
import json
import math
import os
import re
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo

REPOSITORY = "masondev1024/seoul-weather-platform"
KST = ZoneInfo("Asia/Seoul")
RAW_PREFIX = "raw/weather/kma_vilage_fcst"
CHECKPOINT_PREFIX = "ops/control/checkpoints/weather-hosted"
BUDGET_PREFIX = "ops/control/weather-hosted/api-budget"
DAG_ID = "weather_hosted_collection"
DAILY_ATTEMPT_LIMIT = 2400
RUN_ATTEMPT_LIMIT = 1280
RAW_MAX_BYTES = 512 * 1024
CHECKPOINT_MAX_BYTES = 256 * 1024
MANIFEST_MAX_BYTES = 128 * 1024
_BASE_HOURS = (2, 5, 8, 11, 14, 17, 20, 23)
_RAW_KEY = re.compile(
    r"^raw/weather/kma_vilage_fcst/load_date=(\d{4}-\d{2}-\d{2})/"
    r"run_id=(hosted-\d{8}T\d{4})/nx=(\d+)/ny=(\d+)/"
    r"(\d{8}T\d{6})KST_base-(\d{12})_([A-Za-z0-9_-]{1,80})\.json$"
)


def _domain_imports() -> None:
    root = Path(__file__).resolve().parents[1]
    for directory in (root / "dags", root / "dags/domains/weather"):
        value = str(directory)
        if value not in sys.path:
            sys.path.insert(0, value)


class HostedCollectionError(RuntimeError):
    """Only fixed codes and constructed, public-safe diagnostics are logged."""

    def __init__(self, code: str, *, diagnostics: dict | None = None) -> None:
        super().__init__(code)
        self.diagnostics = diagnostics or {}
        self.summary = None


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise HostedCollectionError(code)


def _aware(value: datetime) -> datetime:
    _require(isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None,
             "invalid_timestamp")
    return value.astimezone(KST)


def _integer(value: object, minimum: int, maximum: int, code: str) -> int:
    _require(type(value) is int and minimum <= value <= maximum, code)
    return value


def _json(payload: bytes, code: str) -> dict:
    try:
        document = json.loads(payload)
    except (UnicodeError, ValueError) as exc:
        raise HostedCollectionError(code) from exc
    _require(isinstance(document, dict), code)
    return document


def _missing(exc: Exception) -> bool:
    response = getattr(exc, "response", {}) or {}
    return (response.get("ResponseMetadata", {}) or {}).get("HTTPStatusCode") == 404


def _conflict(exc: Exception) -> bool:
    response = getattr(exc, "response", {}) or {}
    return (response.get("ResponseMetadata", {}) or {}).get("HTTPStatusCode") in (409, 412)


def eligible_issues(now: datetime) -> list[datetime]:
    now = _aware(now)
    issues = []
    for offset in (1, 0):
        day = (now - timedelta(days=offset)).date()
        for hour in _BASE_HOURS:
            issue = datetime.combine(day, datetime.min.time(), tzinfo=KST).replace(hour=hour)
            if now - timedelta(hours=24) < issue <= now - timedelta(minutes=40):
                issues.append(issue)
    return issues


def run_id_for(issue: datetime) -> str:
    return f"hosted-{_aware(issue):%Y%m%dT%H%M}"


class Deadline:
    def __init__(self, *, monotonic: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self._monotonic = monotonic
        self._sleep = sleep
        self._end = monotonic() + 20 * 60

    def check(self, headroom: float = 40) -> None:
        _require(self._monotonic() + headroom <= self._end, "run_deadline")

    def pause(self, seconds: float) -> None:
        _require(math.isfinite(seconds) and seconds >= 0, "invalid_delay")
        self.check(seconds + 40)
        self._sleep(seconds)


def _read_response(response: dict, maximum: int, code: str) -> bytes:
    body = response["Body"]
    try:
        size = response.get("ContentLength")
        if size is not None:
            _integer(size, 0, maximum, code)
        payload = body.read(maximum + 1)
        _require(isinstance(payload, bytes) and len(payload) <= maximum, code)
        return payload
    finally:
        body.close()


class AttemptBudget:
    """Reserve before each physical HTTP attempt, using an R2 compare-and-swap.

    Reservations are never refunded. An uncertain storage acknowledgement stops
    the source request; any committed reservation remains conservatively charged.
    """
    def __init__(self, client, bucket: str, *, clock: Callable[[], datetime], deadline: Deadline) -> None:
        self._client, self._bucket = client, bucket
        self._clock, self._deadline = clock, deadline
        self.attempts = 0

    def reserve(self, _attempt) -> None:
        self._deadline.check()
        _require(self.attempts < RUN_ATTEMPT_LIMIT, "run_budget_exhausted")
        for _ in range(5):
            self._deadline.check()
            now = _aware(self._clock())
            # Do not let a reservation for one quota day authorize a request
            # after midnight. The next hourly run safely resumes its checkpoint.
            midnight = datetime.combine(now.date() + timedelta(days=1), datetime.min.time(), tzinfo=KST)
            _require((midnight - now).total_seconds() >= 40, "budget_day_boundary")
            day = now.date().isoformat()
            key = f"{BUDGET_PREFIX}/{day}.json"
            try:
                response = self._client.get_object(Bucket=self._bucket, Key=key)
            except Exception as exc:
                if not _missing(exc):
                    raise
                count, etag = 0, None
            else:
                doc = _json(_read_response(response, 4096, "budget_invalid"), "budget_invalid")
                _require(set(doc) == {"schema_version", "budget_date", "attempts"} and
                         type(doc["schema_version"]) is int and doc["schema_version"] == 1 and
                         doc["budget_date"] == day, "budget_invalid")
                count = _integer(doc["attempts"], 0, DAILY_ATTEMPT_LIMIT, "budget_invalid")
                etag = response.get("ETag")
                _require(isinstance(etag, str) and bool(etag), "budget_invalid")
            _require(count < DAILY_ATTEMPT_LIMIT, "daily_budget_exhausted")
            document = {"schema_version": 1, "budget_date": day, "attempts": count + 1}
            options = {"IfMatch": etag} if etag is not None else {"IfNoneMatch": "*"}
            self._deadline.check()
            if _aware(self._clock()).date().isoformat() != day:
                continue
            try:
                self._client.put_object(Bucket=self._bucket, Key=key,
                                        Body=json.dumps(document, sort_keys=True).encode(),
                                        ContentType="application/json", **options)
            except Exception as exc:
                if _conflict(exc):
                    continue
                raise
            acknowledged_at = _aware(self._clock())
            next_midnight = datetime.combine(acknowledged_at.date() + timedelta(days=1), datetime.min.time(), tzinfo=KST)
            _require(acknowledged_at.date().isoformat() == day and
                     (next_midnight - acknowledged_at).total_seconds() >= 40, "budget_day_boundary")
            self.attempts += 1
            return
        raise HostedCollectionError("budget_conflict_exhausted")


def validate_page(payload: bytes, *, issue: datetime, nx: int, ny: int, page_no: int) -> tuple[dict, list]:
    _domain_imports()
    from weather_ingest.kma import parse_kma_response, validate_kma_response_context
    from weather_ingest.errors import WeatherSourceBusinessError, WeatherSourceSchemaError

    _require(isinstance(payload, bytes) and len(payload) <= RAW_MAX_BYTES, "source_size_limit")
    try:
        doc = _json(payload, "source_page_contract")
    except HostedCollectionError as exc:
        raise _page_failure('invalid_json', {}, issue, nx, ny, page_no) from exc
    try:
        metadata, rows = parse_kma_response(payload)
    except WeatherSourceBusinessError as exc:
        raise _page_failure('response_business', doc, issue, nx, ny, page_no) from exc
    except WeatherSourceSchemaError as exc:
        rule = 'response_schema'
        # Map only fixed validator messages to a closed field vocabulary. Never
        # log the exception itself, provider resultMsg, URLs or response rows.
        for field in ('baseDate', 'baseTime', 'category', 'fcstDate', 'fcstTime', 'fcstValue', 'nx', 'ny'):
            if str(exc).endswith(' is missing required field: ' + field):
                rule = 'required_' + field
                break
        raise _page_failure(rule, doc, issue, nx, ny, page_no) from exc
    try:
        validate_kma_response_context(rows, base_date=issue.strftime("%Y%m%d"),
                                      base_time=issue.strftime("%H%M"), nx=nx, ny=ny)
    except WeatherSourceSchemaError as exc:
        raise _page_failure('request_context', doc, issue, nx, ny, page_no) from exc
    body = doc["response"]["body"]
    total = metadata['total_count']
    if type(total) is not int or not 1 <= total <= 2000:
        raise _page_failure('total_count', doc, issue, nx, ny, page_no)
    pages = math.ceil(total / 1000)
    for valid, rule in (
        (type(body.get('pageNo')) is int and body['pageNo'] == page_no, 'page_number'),
        (type(body.get('numOfRows')) is int and body['numOfRows'] == 1000, 'page_size'),
        (1 <= page_no <= pages, 'page_range'),
        (len(rows) == min(1000, total - (page_no - 1) * 1000), 'row_count'),
    ):
        if not valid:
            raise _page_failure(rule, doc, issue, nx, ny, page_no)
    return {**metadata, "page_no": page_no, "page_count": pages}, rows


def _page_failure(rule: str, doc: dict, issue: datetime, nx: int, ny: int, page_no: int) -> HostedCollectionError:
    response = doc.get('response')
    response = response if isinstance(response, dict) else {}
    body, header = response.get('body'), response.get('header')
    body = body if isinstance(body, dict) else {}
    header = header if isinstance(header, dict) else {}
    diagnostic = {'validation_rule': rule, 'source_issue': _aware(issue).isoformat(),
                  'nx': nx, 'ny': ny, 'page_no': page_no}
    for source_field, field in (('pageNo', 'response_page_no'), ('numOfRows', 'response_page_size'),
                                ('totalCount', 'response_total_count')):
        value = body.get(source_field)
        diagnostic[field] = value if type(value) is int and 0 <= value <= 10_000_000 else None
    code = header.get('resultCode')
    diagnostic['source_result_code'] = code if isinstance(code, str) and re.fullmatch(r'[0-9]{2}', code) else None
    items = body.get('items')
    items = items.get('item') if isinstance(items, dict) else None
    diagnostic['response_row_count'] = len(items) if isinstance(items, list) else (1 if isinstance(items, dict) else None)
    return HostedCollectionError('source_page_contract', diagnostics=diagnostic)


class ValidatedSource:
    def __init__(self, source, issue: datetime) -> None:
        self._source, self._issue = source, issue

    def fetch_page(self, **options) -> tuple[int, bytes]:
        from common.http.errors import HttpProblemError

        _require(options["base_date"] == self._issue.strftime("%Y%m%d") and
                 options["base_time"] == self._issue.strftime("%H%M") and
                 options["num_of_rows"] == 1000 and options["page_no"] in (1, 2), "source_page_contract")
        try:
            status, payload = self._source.fetch_page(**options)
        except HttpProblemError as exc:
            kinds = {'urn:asac:error:api-timeout': 'timeout',
                     'urn:asac:error:connection-error': 'connection',
                     'urn:asac:error:http-error': 'http_status'}
            diagnostic = {'source_issue': _aware(self._issue).isoformat(),
                          'nx': options['nx'], 'ny': options['ny'], 'page_no': options['page_no'],
                          'source_http_kind': kinds.get(exc.problem.type, 'other'),
                          'source_http_status': exc.status if type(exc.status) is int and 100 <= exc.status <= 599 else None}
            raise HostedCollectionError('source_http_failure', diagnostics=diagnostic) from exc
        _require(type(status) is int and 200 <= status < 300, "source_http_failure")
        validate_page(payload, issue=self._issue, nx=options["nx"], ny=options["ny"], page_no=options["page_no"])
        return status, payload


class ScopedStore:
    """Compose the existing R2 adapter with strict key and byte boundaries."""
    def __init__(self, client, bucket: str, issue: datetime, grids, *, clock: Callable[[], datetime],
                 deadline: Deadline | None = None) -> None:
        _domain_imports()
        from weather_ingest.runtime import R2RawObjectStore

        self._client, self._bucket = client, bucket
        self._issue, self._clock = _aware(issue), clock
        self._deadline = deadline or Deadline()
        self.checkpoint_pages = 0
        self.checkpoint_write_acknowledged = False
        self._store = R2RawObjectStore(client, bucket=bucket, sleep=self._deadline.pause)
        self._configured_grids = [dict(item) for item in grids]
        self._grids = {(item["nx"], item["ny"]) for item in grids}
        _require(all(type(item["nx"]) is int and type(item["ny"]) is int for item in grids), "grid_contract")
        _require(len(grids) == len(self._grids) == 80, "grid_contract")
        self.run_id = run_id_for(issue)
        self.load_dates = tuple(dict.fromkeys((self._issue.date().isoformat(), _aware(clock()).date().isoformat())))
        self.checkpoint_key = (f"{CHECKPOINT_PREFIX}/kma_vilage_fcst/dag_id={DAG_ID}/"
                               f"run_id={self.run_id}/base-{issue:%Y%m%d%H%M}.json")

    def manifest_key(self, day: str) -> str:
        _require(day in self.load_dates, "storage_scope")
        return f"{RAW_PREFIX}/load_date={day}/run_id={self.run_id}/_manifest.json"

    def _kind(self, key: str) -> str:
        self._deadline.check()
        if key == self.checkpoint_key:
            return "checkpoint"
        if key in tuple(self.manifest_key(day) for day in self.load_dates):
            return "manifest"
        match = _RAW_KEY.fullmatch(key)
        _require(match is not None and match[1] in self.load_dates and match[2] == self.run_id and
                 (int(match[3]), int(match[4])) in self._grids and match[6] == self._issue.strftime("%Y%m%d%H%M"),
                 "storage_scope")
        try:
            collected = datetime.strptime(match[5], "%Y%m%dT%H%M%S").replace(tzinfo=KST)
        except ValueError as exc:
            raise HostedCollectionError("raw_timestamp_contract") from exc
        _require(self._issue <= collected <= _aware(self._clock()), "raw_timestamp_contract")
        return "raw"

    def exists(self, key: str) -> bool:
        self._kind(key)
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
        except Exception as exc:
            if _missing(exc):
                return False
            raise
        return True

    def read_bytes(self, key: str) -> bytes:
        kind = self._kind(key)
        limits = {"raw": RAW_MAX_BYTES, "checkpoint": CHECKPOINT_MAX_BYTES, "manifest": MANIFEST_MAX_BYTES}
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        payload = _read_response(response, limits[kind], "storage_size_limit")
        if kind == "checkpoint":
            document = _json(payload, 'checkpoint_invalid')
            self._validate_checkpoint(document)
            self.checkpoint_pages = len(document['raw_objects'])
        return payload

    def read_sha256(self, key: str) -> str | None:
        _require(self._kind(key) == "raw", "storage_scope")
        return self._store.read_sha256(key)

    def write_bytes_if_absent(self, key: str, payload: bytes, content_type: str) -> bool:
        _require(self._kind(key) == "raw" and len(payload) <= RAW_MAX_BYTES, "storage_scope")
        return self._store.write_bytes_if_absent(key, payload, content_type)

    def _validate_checkpoint(self, doc: dict) -> None:
        expected_request = {"base_date": self._issue.strftime("%Y%m%d"), "base_time": self._issue.strftime("%H%M"),
                            "num_of_rows": 1000, "grids": self._configured_grids}
        _require(doc.get("source_id") == "kma_vilage_fcst" and doc.get("dag_id") == DAG_ID and
                 doc.get("dag_run_id") == self.run_id and doc.get("request") == expected_request and
                 doc.get("landing_load_date") in self.load_dates and isinstance(doc.get("raw_objects"), list),
                 "checkpoint_invalid")
        _require(len(doc["raw_objects"]) <= 160, "checkpoint_invalid")
        seen = set()
        for item in doc["raw_objects"]:
            _require(isinstance(item, dict) and isinstance(item.get("raw_object_key"), str), "checkpoint_invalid")
            key = item["raw_object_key"]
            _require(self._kind(key) == "raw" and f"load_date={doc['landing_load_date']}/" in key and
                     item.get("base_date") == self._issue.strftime("%Y%m%d") and
                     item.get("base_time") == self._issue.strftime("%H%M") and
                     type(item.get("nx")) is int and type(item.get("ny")) is int and
                     (item.get("nx"), item.get("ny")) in self._grids and
                     type(item.get("num_of_rows")) is int and item["num_of_rows"] == 1000 and
                     type(item.get("http_status")) is int and item["http_status"] == 200 and
                     isinstance(item.get("payload_hash"), str) and
                     re.fullmatch(r"[0-9a-f]{64}", item["payload_hash"]) is not None,
                     "checkpoint_invalid")
            match = _RAW_KEY.fullmatch(key)
            try:
                collected = _aware(datetime.fromisoformat(item["collected_at"]))
            except (KeyError, TypeError, ValueError) as exc:
                raise HostedCollectionError("checkpoint_invalid") from exc
            _require(collected.strftime("%Y%m%dT%H%M%S") == match[5] and
                     self._issue <= collected <= _aware(self._clock()) and
                     (int(match[3]), int(match[4])) == (item["nx"], item["ny"]) and
                     item.get("request_id") == match[7] and
                     item.get("place_id") == next(grid["place_id"] for grid in self._configured_grids
                                                   if (grid["nx"], grid["ny"]) == (item["nx"], item["ny"])),
                     "checkpoint_invalid")
            total = _integer(item.get("total_count"), 1, 2000, "checkpoint_invalid")
            page_count = math.ceil(total / 1000)
            page = _integer(item.get("page_no"), 1, page_count, "checkpoint_invalid")
            _require(type(item.get("page_count")) is int and item["page_count"] == page_count and
                     type(item.get("row_count")) is int and item["row_count"] == min(1000, total - (page - 1) * 1000),
                     "checkpoint_invalid")
            identity = (item["nx"], item["ny"], page)
            _require(identity not in seen, "checkpoint_invalid")
            seen.add(identity)

    def write_bytes(self, key: str, payload: bytes, content_type: str) -> None:
        kind = self._kind(key)
        if kind == "checkpoint":
            _require(len(payload) <= CHECKPOINT_MAX_BYTES, "storage_size_limit")
            document = _json(payload, 'checkpoint_invalid')
            self._validate_checkpoint(document)
            self.checkpoint_write_acknowledged = False
            self._store.write_bytes(key, payload, content_type)
            self.checkpoint_pages = len(document['raw_objects'])
            self.checkpoint_write_acknowledged = True
            return
        _require(kind == "manifest" and len(payload) <= MANIFEST_MAX_BYTES, "storage_scope")
        doc = self.validate_manifest(key, payload)
        try:
            self._client.put_object(Bucket=self._bucket, Key=key, Body=payload,
                                    ContentType=content_type, IfNoneMatch="*")
        except Exception as exc:
            if not _conflict(exc):
                raise
            existing = self.validate_manifest(key, self.read_bytes(key))
            _require(existing["object_keys"] == doc["object_keys"], "manifest_conflict")
            # An already complete manifest retains its original completed_at.

    def validate_manifest(self, key: str, payload: bytes) -> dict:
        _require(self._kind(key) == "manifest", "storage_scope")
        doc = _json(payload, "manifest_invalid")
        day = key.split("load_date=", 1)[1].split("/", 1)[0]
        _require(set(doc) == {"run_id", "dataset", "load_date", "object_keys", "expected_count", "actual_count", "completed_at", "status"}
                 and doc["run_id"] == self.run_id and doc["dataset"] == "kma_vilage_fcst" and doc["load_date"] == day
                 and doc["status"] == "complete" and isinstance(doc["object_keys"], list), "manifest_invalid")
        keys = doc["object_keys"]
        count = _integer(doc["actual_count"], 80, 160, "manifest_invalid")
        _require(type(doc["expected_count"]) is int and doc["expected_count"] == count == len(keys) and
                 all(isinstance(item, str) for item in keys) and len(set(keys)) == count, "manifest_invalid")
        try:
            completed = _aware(datetime.fromisoformat(doc["completed_at"].replace("Z", "+00:00")))
        except (TypeError, ValueError, AttributeError) as exc:
            raise HostedCollectionError("manifest_invalid") from exc
        _require(self._issue <= completed <= _aware(self._clock()), "manifest_invalid")
        pages = defaultdict(dict)
        for raw_key in keys:
            _require(self._kind(raw_key) == "raw" and raw_key.startswith(key.rsplit("/", 1)[0] + "/"), "manifest_invalid")
            match = _RAW_KEY.fullmatch(raw_key)
            nx, ny = int(match[3]), int(match[4])
            collected = datetime.strptime(match[5], "%Y%m%dT%H%M%S").replace(tzinfo=KST)
            _require(collected <= completed, "manifest_invalid")
            raw_payload = self.read_bytes(raw_key)
            recorded_hash = self.read_sha256(raw_key)
            _require(isinstance(recorded_hash, str) and re.fullmatch(r"[0-9a-f]{64}", recorded_hash) is not None and
                     hashlib.sha256(raw_payload).hexdigest() == recorded_hash, "manifest_invalid")
            raw_doc = _json(raw_payload, "manifest_invalid")
            response = raw_doc.get("response")
            _require(isinstance(response, dict) and isinstance(response.get("body"), dict), "manifest_invalid")
            page_no = response["body"].get("pageNo")
            _require(type(page_no) is int and page_no not in pages[(nx, ny)], "manifest_invalid")
            try:
                metadata, rows = validate_page(raw_payload, issue=self._issue, nx=nx, ny=ny, page_no=page_no)
            except (ValueError, KeyError, TypeError) as exc:
                raise HostedCollectionError("manifest_invalid") from exc
            pages[(nx, ny)][page_no] = (metadata["total_count"], len(rows))
        _require(set(pages) == self._grids, "manifest_invalid")
        for group in pages.values():
            totals = {total for total, _rows in group.values()}
            _require(len(totals) == 1, "manifest_invalid")
            total = next(iter(totals))
            _require(set(group) == set(range(1, math.ceil(total / 1000) + 1)) and
                     sum(rows for _total, rows in group.values()) == total, "manifest_invalid")
        return doc


class BoundedTransport:
    """Stream one verified TLS response and refuse automatic redirect requests."""
    def __init__(self, deadline: Deadline) -> None:
        import requests
        self._session = requests.Session()
        self._deadline = deadline
        self.guard_failure = None

    def send(self, method, url, *, params, headers, timeout):
        try:
            return self._send(method, url, params=params, headers=headers, timeout=timeout)
        except HostedCollectionError as exc:
            self.guard_failure = exc
            raise

    def _send(self, method, url, *, params, headers, timeout):
        from common.http.contract import TransportResponse

        self._deadline.check()
        chunks, size = [], 0
        with self._session.request(method, url, params=params, headers=headers, timeout=timeout,
                                   stream=True, allow_redirects=False) as response:
            for chunk in response.iter_content(chunk_size=16 * 1024):
                self._deadline.check(headroom=0)
                size += len(chunk)
                _require(size <= RAW_MAX_BYTES, "source_size_limit")
                chunks.append(chunk)
            return TransportResponse(status=response.status_code, content=b"".join(chunks), headers=dict(response.headers))


def _hosted_source(budget: AttemptBudget, deadline: Deadline):
    _domain_imports()
    from common.http import HttpCore
    from common.http.errors import HttpProblemError
    from weather_ingest.common.runtime import fetch_url
    from weather_ingest.runtime import KmaHttpAdapter

    transport = BoundedTransport(deadline)

    class GuardedHttp(HttpCore):
        def get(self, *args, **kwargs):
            transport.guard_failure = None
            try:
                return super().get(*args, **kwargs)
            except HttpProblemError:
                # HttpCore catches Exception from transports. Restore our local
                # budget/size guard so it cannot turn into a retryable network fault.
                if transport.guard_failure is not None:
                    raise transport.guard_failure
                raise

    http = GuardedHttp(source="weather_kma", transport=transport, timeout=30,
                       max_attempts=1, rate_limit=None, sleep=deadline.pause)
    fetch = functools.partial(fetch_url, http_client=http, sleep=deadline.pause, retry_network_errors=True)
    return KmaHttpAdapter(fetch, delay_seconds=1, sleep=deadline.pause,
                          before_attempt=budget.reserve, dag_run_id=DAG_ID)


def collect_hosted(client, bucket: str, *, source=None, grids=None,
                   clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
                   only_latest: bool = False, deadline: Deadline | None = None) -> dict:
    _domain_imports()
    from weather_ingest.kma import DEFAULT_GRID_CSV, load_kma_grids
    from weather_ingest.landing import KmaGrid, KmaLanding, KmaLandingRequest, RunIdentity

    deadline = deadline or Deadline()
    deadline.check()
    grids = grids or load_kma_grids(path=str(DEFAULT_GRID_CSV), expected_grid_count=80)
    _require(len(grids) == 80, "grid_contract")
    budget = AttemptBudget(client, bucket, clock=clock, deadline=deadline)
    source = source or _hosted_source(budget, deadline)
    issues = eligible_issues(clock())
    _require(bool(issues), "issue_window_empty")
    if only_latest:
        issues = issues[-1:]
    pending, reused, reused_issues, blocked_issues = [], 0, [], []
    for issue in reversed(issues):
        store = ScopedStore(client, bucket, issue, grids, clock=clock, deadline=deadline)
        completed = False
        blocked = False
        for day in store.load_dates:
            key = store.manifest_key(day)
            if not store.exists(key):
                continue
            try:
                store.validate_manifest(key, store.read_bytes(key))
            except HostedCollectionError as exc:
                if str(exc) not in {"manifest_invalid", "source_page_contract", "source_size_limit",
                                    "storage_scope", "storage_size_limit", "raw_timestamp_contract", "invalid_timestamp"}:
                    raise
                blocked = True
                break
            except Exception as exc:
                if not _missing(exc):
                    raise
                # A complete manifest that references a missing raw object is
                # immutable bad evidence; preserve it for investigation.
                blocked = True
                break
            if completed:
                blocked = True
                break
            completed = True
        if blocked:
            blocked_issues.append(issue.isoformat())
            continue
        if completed:
            reused += 1
            reused_issues.append(issue.isoformat())
        else:
            pending.append((issue, store))
    selected = pending[:1]
    if len(pending) > 1:
        selected.append(pending[-1])
    result = {"schema_version": 1, "status": "degraded" if blocked_issues else "success",
              "cycles_completed": 0, "cycles_reused": reused, "cycles_blocked": len(blocked_issues),
              "cycles_failed": 0, "failures": [],
              "raw_pages": 0, "raw_objects_reused": 0, "forecast_rows": 0, "physical_attempts": 0,
              "source_issues_completed": [], "source_issues_reused": reused_issues,
              "source_issues_blocked": blocked_issues,
              "blocked_reason_code": "manifest_integrity_failure" if blocked_issues else None}
    for issue, store in selected:
        try:
            deadline.check()
            request = KmaLandingRequest(issue.strftime("%Y%m%d"), issue.strftime("%H%M"),
                                        tuple(KmaGrid(**grid) for grid in grids), 1000)
            landing = KmaLanding(source=ValidatedSource(source, issue), raw_store=store, raw_prefix="raw",
                                 checkpoint_prefix=CHECKPOINT_PREFIX, clock=clock,
                                 request_id=lambda: uuid.uuid4().hex, raw_spool=None)
            batch = landing.collect(RunIdentity(DAG_ID, store.run_id), request)
        except Exception as exc:
            failure = {'source_issue': issue.isoformat(),
                       'reason_code': str(exc) if isinstance(exc, HostedCollectionError) else 'collection_failed',
                       'checkpoint_pages': store.checkpoint_pages,
                       'checkpoint_write_acknowledged': store.checkpoint_write_acknowledged}
            if isinstance(exc, HostedCollectionError):
                failure.update(exc.diagnostics)
            result['failures'].append(failure)
            result['cycles_failed'] += 1
            result['physical_attempts'] = budget.attempts
            result['status'] = 'degraded'
            if isinstance(exc, HostedCollectionError) and str(exc) == 'source_page_contract':
                # One invalid source issue must not prevent another selected
                # issue from making durable progress. Keep the failed checkpoint.
                continue
            result['status'] = 'failed'
            if not isinstance(exc, HostedCollectionError):
                wrapped = HostedCollectionError('collection_failed')
                wrapped.summary = result
                raise wrapped from exc
            exc.summary = result
            raise
        result["cycles_completed"] += 1
        result["source_issues_completed"].append(issue.isoformat())
        result["raw_pages"] += len(batch.raw_objects)
        result["raw_objects_reused"] += batch.reused_raw_object_count
        result["forecast_rows"] += sum(item.row_count for item in batch.raw_objects)
    result["physical_attempts"] = budget.attempts
    return result


def _client_from_env():
    import boto3
    from botocore.config import Config

    values = {name: os.environ.get(name, "") for name in
              ("R2_ENDPOINT", "R2_BUCKET_NAME", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "KMA_SERVICE_KEY")}
    _require(all(values.values()), "credentials_missing")
    _require(re.fullmatch(r"https://[a-f0-9]{32}\.r2\.cloudflarestorage\.com", values["R2_ENDPOINT"]) is not None,
             "endpoint_invalid")
    _require(not os.environ.get("KMA_BASE_URL"), "source_endpoint_override")
    client = boto3.client("s3", endpoint_url=values["R2_ENDPOINT"], region_name="auto",
                          aws_access_key_id=values["R2_ACCESS_KEY_ID"],
                          aws_secret_access_key=values["R2_SECRET_ACCESS_KEY"],
                          config=Config(connect_timeout=5, read_timeout=15,
                                        retries={"mode": "standard", "total_max_attempts": 2},
                                        request_checksum_calculation="when_required",
                                        response_checksum_validation="when_required"))
    return client, values["R2_BUCKET_NAME"]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only-latest", action="store_true")
    parser.add_argument("--summary-file", type=Path)
    args = parser.parse_args(argv)
    try:
        _require(os.environ.get("GITHUB_REPOSITORY", REPOSITORY) == REPOSITORY, "repository_scope")
        _require(os.environ.get("GITHUB_REF", "refs/heads/main") == "refs/heads/main", "branch_scope")
        client, bucket = _client_from_env()
        result = collect_hosted(client, bucket, only_latest=args.only_latest)
        serialized = json.dumps(result, sort_keys=True)
        if args.summary_file:
            args.summary_file.parent.mkdir(parents=True, exist_ok=True)
            args.summary_file.write_text(serialized + "\n", encoding="utf-8")
        print(serialized)
        return 0 if result["status"] == "success" else 2
    except Exception as exc:
        code = str(exc) if isinstance(exc, HostedCollectionError) else "collection_failed"
        document = {"status": "failed", "reason_code": code, "error_type": type(exc).__name__}
        if isinstance(exc, HostedCollectionError):
            document.update(exc.diagnostics)
            if exc.summary is not None:
                document.update(exc.summary)
        failure = json.dumps(document, sort_keys=True)
        if args.summary_file:
            try:
                args.summary_file.parent.mkdir(parents=True, exist_ok=True)
                args.summary_file.write_text(failure + "\n", encoding="utf-8")
            except OSError:
                pass
        print(failure)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
