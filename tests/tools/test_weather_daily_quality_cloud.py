import io
import json
import sys
import types
from datetime import date, datetime, timezone

import pytest

from tools.weather_daily_quality_cloud import _client_from_env, build_report, main, render_markdown


DAY = date(2026, 10, 12)
AS_OF = datetime(2026, 10, 13, 0, 17, tzinfo=timezone.utc)
PREFIX = "raw/weather/kma_vilage_fcst/load_date="
GRIDS = ((60, 127), (61, 127))


def raw(day="2026-10-12", collected="20261012T230100", issue="202610122300", nx=60, run="r1"):
    return f"{PREFIX}{day}/run_id={run}/nx={nx}/ny=127/{collected}KST_base-{issue}_req.json"


def manifest(keys, day="2026-10-12", run="r1", **updates):
    doc = {"dataset": "kma_vilage_fcst", "run_id": run, "load_date": day,
           "status": "complete", "expected_count": len(set(keys)), "actual_count": len(set(keys)),
           "completed_at": "2026-10-12T23:02:00+09:00", "object_keys": keys}
    doc.update(updates)
    return f"{PREFIX}{day}/run_id={run}/_manifest.json", json.dumps(doc).encode()


class Store:
    def __init__(self, files=None, pages=None, error=None):
        self.files = files or {}
        self.pages = pages
        self.error = error
        self.list_calls = []
        self.get_calls = []

    def list_objects_v2(self, **kwargs):
        self.list_calls.append(kwargs)
        if self.error:
            raise RuntimeError(self.error)
        if self.pages is not None:
            return self.pages.pop(0)
        return {"Contents": [{"Key": key, "Size": len(payload)} for key, payload in self.files.items()
                             if key.startswith(kwargs["Prefix"])], "IsTruncated": False}

    def get_object(self, **kwargs):
        self.get_calls.append(kwargs)
        return {"Body": io.BytesIO(self.files[kwargs["Key"]])}


def report(store):
    return build_report(store, "private-bucket", DAY, AS_OF, grids=GRIDS)


def test_zero_listing_is_collection_failure_without_fabricated_quality():
    result = report(Store())
    assert result["status"] == "fail"
    assert result["checks"]["collection"]["reason_code"] == "no_raw_collection_objects"
    assert result["checks"]["missing"]["observed_slots"] == 0
    assert result["checks"]["missing"]["missing_slots"] is None
    assert result["checks"]["duplicates"]["duplicate_references"] is None


def test_only_two_exact_prefixes_are_listed_no_payload_get():
    key = raw()
    mkey, body = manifest([key])
    store = Store({key: b"private raw payload", mkey: body})
    result = report(store)
    assert [call["Prefix"] for call in store.list_calls] == [PREFIX + "2026-10-12/", PREFIX + "2026-10-13/"]
    assert [call["Key"] for call in store.get_calls] == [mkey]
    assert result["checks"]["missing"]["observed_slots"] == 1
    assert result["checks"]["missing"]["missing_slots"] == 15
    assert key not in json.dumps(result)
    assert "private-bucket" not in json.dumps(result)


def test_duplicate_references_fail_without_counting_cross_run_recollection():
    key = raw()
    mkey, body = manifest([key, key])
    result = report(Store({key: b"", mkey: body}))
    assert result["checks"]["duplicates"]["duplicate_references"] == 1
    assert result["checks"]["duplicates"]["status"] == "fail"
    assert result["checks"]["missing"]["status"] == "unmeasured"
    assert result["checks"]["missing"]["observed_slots"] == 0
    second = raw(run="r2")
    m2, b2 = manifest([second], run="r2")
    result = report(Store({key: b"", mkey: manifest([key])[1], second: b"", m2: b2}))
    assert result["checks"]["missing"]["observed_slots"] == 1
    assert result["checks"]["duplicates"]["duplicate_references"] == 0


@pytest.mark.parametrize("updates", [{"status": "PARTIAL"}, {"actual_count": 2}, {"run_id": "other"},
                                      {"completed_at": "2026-10-13T23:00:00+09:00"}])
def test_invalid_manifest_makes_coverage_unmeasured(updates):
    key = raw()
    mkey, body = manifest([key], **updates)
    result = report(Store({key: b"", mkey: body}))
    assert result["checks"]["missing"]["status"] == "unmeasured"
    assert result["checks"]["missing"]["missing_slots"] is None


def test_referenced_key_outside_two_prefixes_is_rejected_never_fetched():
    outside = raw(day="2026-10-01")
    mkey, body = manifest([outside])
    store = Store({mkey: body})
    result = report(store)
    assert result["checks"]["missing"]["status"] == "unmeasured"
    assert [call["Key"] for call in store.get_calls] == [mkey]


def test_recent_load_does_not_hide_stale_source_issue():
    key = raw(day="2026-10-13", collected="20261013T090100", issue="202610120200")
    mkey, body = manifest([key], day="2026-10-13", completed_at="2026-10-13T09:02:00+09:00")
    result = report(Store({key: b"", mkey: body}))
    assert result["checks"]["freshness"]["status"] == "fail"
    assert result["checks"]["freshness"]["stale_grids"] == 1
    assert result["checks"]["freshness"]["latest_collected_at"] != result["checks"]["freshness"]["latest_issue_at"]


def test_absent_grid_is_unmeasured_when_known_grid_is_fresh():
    key = raw(day="2026-10-13", collected="20261013T080100", issue="202610130800")
    mkey, body = manifest([key], day="2026-10-13", completed_at="2026-10-13T08:02:00+09:00")
    result = report(Store({key: b"", mkey: body}))
    assert result["checks"]["freshness"]["status"] == "unmeasured"
    assert result["checks"]["freshness"]["missing_grids"] == 1


def test_malformed_and_oversized_manifest_and_count_budget_fail_closed():
    mkey, body = manifest([raw()])
    for payload in (b"not json", b"x" * (256 * 1024 + 1)):
        result = report(Store({mkey: payload}))
        assert result["checks"]["missing"]["status"] == "unmeasured"
    files = {manifest([raw(run=f"r{i}")], run=f"r{i}")[0]: body for i in range(65)}
    store = Store(files)
    result = report(store)
    assert result["checks"]["missing"]["reason_code"] == "manifest_budget_exceeded"
    assert not store.get_calls


def test_paging_cap_is_bounded_and_does_not_claim_complete():
    entries = [{"Key": raw(run=f"r{i}"), "Size": 0} for i in range(1000)]
    pages = [{"Contents": entries, "IsTruncated": True, "NextContinuationToken": f"p{i}"} for i in range(5)]
    store = Store(pages=pages)
    result = report(store)
    assert len(store.list_calls) == 5
    assert sum(call["MaxKeys"] for call in store.list_calls) == 5000
    assert result["checks"]["missing"]["reason_code"] == "listing_budget_exceeded"


def test_transport_failure_is_sanitized_public_report():
    result = report(Store(error="token=secret endpoint=https://private.example private-bucket"))
    serialized = json.dumps(result)
    assert "secret" not in serialized and "private.example" not in serialized
    assert result["status"] == "unmeasured"
    assert result["checks"]["collection"]["reason_code"] == "r2_evidence_unavailable"


@pytest.mark.parametrize("as_of", [datetime(2026, 10, 13), datetime(2026, 10, 12, tzinfo=timezone.utc),
                                  datetime(2026, 10, 15, tzinfo=timezone.utc)])
def test_naive_future_or_nonlatest_window_rejected(as_of):
    with pytest.raises(ValueError):
        build_report(Store(), "private", DAY, as_of, grids=GRIDS)


def test_missing_credentials_still_produces_honest_report(tmp_path, monkeypatch):
    for name in ("R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME"):
        monkeypatch.delenv(name, raising=False)
    assert main(["--output-dir", str(tmp_path)]) == 0
    files = list(tmp_path.glob("*.json"))
    assert len(files) == 1
    assert json.loads(files[0].read_text())["checks"]["missing"]["reason_code"] == "credentials_missing"


def test_all_contract_slots_complete_and_all_grids_fresh_can_pass():
    files = {}
    for cycle in ("0200", "0500", "0800", "1100", "1400", "1700", "2000", "2300"):
        keys = [raw(issue=f"20261012{cycle}", nx=nx) for nx, _ in GRIDS]
        files.update({key: b"" for key in keys})
        mkey, body = manifest(keys, run=f"issue{cycle}")
        files[mkey] = body
    for nx, _ in GRIDS:
        current = raw(day="2026-10-13", issue="202610130800", collected="20261013T080100", nx=nx)
        files[current] = b""
        mkey, body = manifest([current], run=f"fresh{nx}", day="2026-10-13", completed_at="2026-10-13T08:02:00+09:00")
        files[mkey] = body
    result = report(Store(files))
    assert result["status"] == "pass"
    assert result["checks"]["missing"]["missing_slots"] == 0
    assert result["checks"]["freshness"]["stale_grids"] == 0


def test_zero_entry_pagination_has_explicit_request_bound():
    pages = [{"Contents": [], "IsTruncated": True, "NextContinuationToken": f"p{i}"} for i in range(15)]
    store = Store(pages=pages)
    result = report(store)
    assert len(store.list_calls) == 12
    assert result["checks"]["missing"]["reason_code"] == "listing_budget_exceeded"


def test_manifest_mixing_source_issue_cycles_cannot_prove_coverage():
    keys = [raw(issue="202610120200"), raw(issue="202610120500")]
    mkey, body = manifest(keys)
    result = report(Store({**dict.fromkeys(keys, b""), mkey: body}))
    assert result["checks"]["missing"]["status"] == "unmeasured"
    assert result["checks"]["missing"]["observed_slots"] == 0


def test_replay_manifest_can_reference_different_runs_and_two_allowed_load_days():
    keys = [raw(run="original"), raw(day="2026-10-13", run="retried", nx=61,
                                     issue="202610122300", collected="20261013T080100")]
    mkey, body = manifest(keys, run="replay", day="2026-10-13", completed_at="2026-10-13T08:02:00+09:00")
    result = report(Store({**dict.fromkeys(keys, b""), mkey: body}))
    assert result["checks"]["missing"]["observed_slots"] == 2
    assert result["evidence"]["accepted_manifests"] == 1


def test_markdown_includes_duplicate_freshness_counts_and_source_timestamp():
    key = raw()
    mkey, body = manifest([key])
    result = report(Store({key: b"", mkey: body}))
    rendered = render_markdown(result)
    assert "Manifest 참조 중복 수: 0" in rendered
    assert "오래된 발표 격자 수: 1" in rendered
    assert result["checks"]["freshness"]["latest_issue_at"] in rendered


@pytest.mark.parametrize("malformed", [False, True])
def test_actual_manifest_stream_bytes_are_bounded_even_if_listing_size_understates(monkeypatch, malformed):
    monkeypatch.setattr("tools.weather_daily_quality_cloud.MAX_TOTAL_BYTES", 500)
    key = raw()
    mkey, body = manifest([key])
    m2, b2 = manifest([key], run="r2")
    if malformed:
        body, b2 = b"x" * len(body), b"x" * len(b2)
    store = Store({key: b"", mkey: body, m2: b2}, pages=[
        {"Contents": [{"Key": key, "Size": 0}, {"Key": mkey, "Size": 1}, {"Key": m2, "Size": 1}], "IsTruncated": False},
        {"Contents": [], "IsTruncated": False}])
    result = report(store)
    assert result["checks"]["missing"]["reason_code"] == "manifest_budget_exceeded"


def test_invalid_manifest_cannot_hide_known_duplicate_failure():
    key = raw()
    mkey, body = manifest([key, key], status="PARTIAL")
    result = report(Store({key: b"", mkey: body}))
    assert result["checks"]["missing"]["status"] == "unmeasured"
    assert result["checks"]["duplicates"]["status"] == "fail"
    assert result["status"] == "fail"


def test_r2_client_uses_auto_region_and_bounded_transport_config(monkeypatch):
    calls = []
    client = object()
    def fake_client(service, **kwargs):
        calls.append((service, kwargs))
        return client
    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=fake_client))
    monkeypatch.setenv("AWS_DEFAULT_REGION", "invalid inherited region")
    for name, value in {"R2_ENDPOINT": "https://unit.example", "R2_ACCESS_KEY_ID": "unit-access",
                        "R2_SECRET_ACCESS_KEY": "unit-secret"}.items():
        monkeypatch.setenv(name, value)
    assert _client_from_env() is client
    service, options = calls[0]
    assert service == "s3"
    assert options["region_name"] == "auto"
    assert options["endpoint_url"] == "https://unit.example"
    assert options["aws_access_key_id"] == "unit-access"
    assert options["aws_secret_access_key"] == "unit-secret"
    config = options["config"]
    assert config.connect_timeout == 5
    assert config.read_timeout == 15
    assert config.retries == {"total_max_attempts": 2, "mode": "standard"}
