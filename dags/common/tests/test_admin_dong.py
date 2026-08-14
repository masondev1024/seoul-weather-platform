"""행정동 마스터 수집 순수 로직 단위 테스트 (#154).

가짜 Transport(공통 HttpCore 주입) + 가짜 Storage 로 실호출 없이 검증:
- load_service_key: PUBLIC_DATA_API_KEY 단일 이름 계약(#154, 폴백 폐지)
- latest_revision: max(개정일자) 판정
- iter_snapshot: currentCount < perPage 종료, 페이지 폭주 가드
- land_snapshot: R2 경로 규약 + manifest
- serviceKey 가 URL/로그/manifest 에 남지 않음(params 로만 전달)
"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common.http.contract import TransportResponse  # noqa: E402
from common.masters import admin_dong  # noqa: E402
from common.storage import Storage  # noqa: E402

_KEY = "PUBLICDATA-serviceKey-abcdef0123456789-longsecret"


class FakeTransport:
    """스크립트된 응답을 순서대로 돌려주는 가짜 Transport(계약: contract.Transport)."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = []

    def send(self, method, url, *, params, headers, timeout):
        self.calls.append({"method": method, "url": url,
                           "params": dict(params or {}), "headers": dict(headers or {})})
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class RepeatingTransport:
    """항상 같은 응답을 돌려준다 — 종료 조건 미충족(폭주 가드) 테스트용."""

    def __init__(self, response):
        self.response = response
        self.calls = []

    def send(self, method, url, *, params, headers, timeout):
        self.calls.append({"url": url, "params": dict(params or {})})
        return self.response


class FakeStorage(Storage):
    """put 을 메모리에 담는 가짜 Storage(write_json 은 base 구현 경유)."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def write_bytes(self, key, data):
        self.objects[key] = data

    def read_bytes(self, key):
        return self.objects[key]

    def exists(self, key):
        return key in self.objects

    def list_keys(self, prefix):
        return sorted(k for k in self.objects if k.startswith(prefix))

    def delete(self, key):
        self.objects.pop(key, None)


def _page(data, *, per_page, current=None):
    body = {
        "page": 1,
        "perPage": per_page,
        "totalCount": 1048575,
        "currentCount": len(data) if current is None else current,
        "matchCount": len(data),
        "data": data,
    }
    return TransportResponse(status=200,
                             content=json.dumps(body, ensure_ascii=False).encode("utf-8"))


def _row(rev, sido="서울특별시", dong="역삼동"):
    return {
        "시도명": sido, "시군구명": "강남구", "행정동명": dong, "법정동명": dong,
        "행정구역코드": 1168051000, "행정동코드": 1168064000, "법정동코드": 1168010100,
        "개정일자": rev, "연결번호": "0001",
    }


def _core(transport):
    return admin_dong.build_core(transport=transport)


# ── load_service_key 단일 이름 계약 (#154) ────────────────────────────────────────
def test_load_service_key_uses_public_data_api_key(monkeypatch):
    monkeypatch.setenv("PUBLIC_DATA_API_KEY", _KEY)
    assert admin_dong.load_service_key() == _KEY


def test_load_service_key_missing_raises(monkeypatch):
    monkeypatch.delenv("PUBLIC_DATA_API_KEY", raising=False)
    with pytest.raises(RuntimeError):
        admin_dong.load_service_key()


def test_load_service_key_ignores_old_bus_name(monkeypatch):
    # 폴백 폐지(#154): 구 PUBLIC_DATA_API_KEY_BUS 만 있으면 인식하지 않고 RuntimeError.
    monkeypatch.delenv("PUBLIC_DATA_API_KEY", raising=False)
    monkeypatch.setenv("PUBLIC_DATA_API_KEY_BUS", _KEY)
    with pytest.raises(RuntimeError):
        admin_dong.load_service_key()


# ── latest_revision ─────────────────────────────────────────────────────────────
def test_latest_revision_picks_max():
    data = [_row("2024-01-01"), _row("2025-07-01"), _row("2025-03-15")]
    core = _core(FakeTransport([_page(data, per_page=1000)]))
    assert admin_dong.latest_revision(core, _KEY) == "2025-07-01"


def test_latest_revision_empty_raises():
    core = _core(FakeTransport([_page([], per_page=1000)]))
    with pytest.raises(RuntimeError):
        admin_dong.latest_revision(core, _KEY)


# ── iter_snapshot 종료 조건 ───────────────────────────────────────────────────────
def test_iter_snapshot_terminates_on_short_page():
    # per_page=2: [2행, 2행, 1행] → 3페이지째 currentCount(1) < 2 에서 종료.
    p1 = _page([_row("2025-07-01"), _row("2025-07-01")], per_page=2)
    p2 = _page([_row("2025-07-01"), _row("2025-07-01")], per_page=2)
    p3 = _page([_row("2025-07-01")], per_page=2)
    core = _core(FakeTransport([p1, p2, p3]))
    pages = list(admin_dong.iter_snapshot(core, _KEY, "2025-07-01", per_page=2))
    assert [p[0] for p in pages] == [1, 2, 3]
    assert sum(len(rows) for _, _, rows in pages) == 5


def test_iter_snapshot_page_overflow_guard():
    # 항상 currentCount==per_page(=full) → 종료 안 됨 → 40페이지 초과 시 RuntimeError.
    full = _page([_row("2025-07-01"), _row("2025-07-01")], per_page=2, current=2)
    core = _core(RepeatingTransport(full))
    with pytest.raises(RuntimeError, match="폭주"):
        list(admin_dong.iter_snapshot(core, _KEY, "2025-07-01", per_page=2))


# ── land_snapshot 경로 규약 + manifest ───────────────────────────────────────────
def test_land_snapshot_path_convention_and_manifest():
    store = FakeStorage()
    pages = [
        (1, b'{"data": [1, 2]}', [{"a": 1}, {"a": 2}]),
        (2, b'{"data": [3]}', [{"a": 3}]),
    ]
    result = admin_dong.land_snapshot(
        iter(pages), revision="2025-07-01", run_id="run-xyz",
        storage=store, load_date="2026-07-06", ingest_ts="20260706T120000Z",
    )
    base = "raw/common/admin_dong/load_date=2026-07-06/ingest_ts=20260706T120000Z"
    assert result["object_keys"] == [f"{base}/page-0001.json", f"{base}/page-0002.json"]
    assert result["manifest_key"] == f"{base}/_manifest.json"
    assert result["rows"] == 3

    manifest = json.loads(store.read_bytes(f"{base}/_manifest.json").decode("utf-8"))
    assert manifest["dataset"] == "admin_dong"
    assert manifest["revision"] == "2025-07-01"
    assert manifest["pages"] == 2
    assert manifest["rows"] == 3
    assert manifest["request_params"] == {"cond[개정일자::EQ]": "2025-07-01"}
    assert manifest["run_id"] == "run-xyz"
    assert manifest["object_keys"] == result["object_keys"]


def test_default_load_date_is_kst_run_day(monkeypatch):
    """기본 load_date 는 KST 실행일(#78 P-1), ingest_ts 는 UTC 유지."""
    from datetime import datetime, timezone

    class _FrozenDatetime(datetime):
        _NOW = datetime(2026, 8, 5, 16, 30, 0, tzinfo=timezone.utc)  # = 08-06 01:30 KST

        @classmethod
        def now(cls, tz=None):
            return cls._NOW if tz is None else cls._NOW.astimezone(tz)

    monkeypatch.setattr(admin_dong, "datetime", _FrozenDatetime)
    result = admin_dong.land_snapshot(
        iter([(1, b'{"data": [1]}', [{"a": 1}])]), revision="2025-07-01",
        run_id="run-kst", storage=FakeStorage(),
    )
    assert "/load_date=2026-08-06/" in result["manifest_key"]        # 라벨은 KST
    assert "/ingest_ts=20260805T163000Z/" in result["manifest_key"]  # 묶음 키는 UTC


# 이 라벨은 commerce enrich_admin_dong_ref 가 max(load_date) → max(ingest_ts) 로 읽는다.
# 구·신 기준이 섞인 집합에서 최신본이 뽑히는지는 그 리더 쪽에서 검증한다 —
# domains/commerce/tests/test_silver_tasks.py::test_latest_admin_dong_pages_*.


# ── serviceKey 미노출 ─────────────────────────────────────────────────────────────
def test_service_key_goes_to_params_not_url():
    transport = FakeTransport([_page([_row("2025-07-01")], per_page=1000)])
    core = _core(transport)
    admin_dong.fetch_page(core, _KEY, page=1, revision="2025-07-01")
    call = transport.calls[0]
    # URL 에는 키가 없어야 하고(HttpCore 는 URL 만 로깅) params 로만 전달돼야 한다.
    assert _KEY not in call["url"]
    assert call["url"] == admin_dong.ODCLOUD_URL
    assert call["params"]["serviceKey"] == _KEY
    assert call["params"]["cond[개정일자::EQ]"] == "2025-07-01"


def test_service_key_not_in_manifest():
    store = FakeStorage()
    admin_dong.land_snapshot(
        iter([(1, b'{"data": []}', [])]), revision="2025-07-01",
        run_id="run-1", storage=store,
    )
    blob = b"".join(store.objects.values())
    assert _KEY.encode("utf-8") not in blob
