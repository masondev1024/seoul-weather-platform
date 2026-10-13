"""Hosted collection fault boundaries, without network access."""
import hashlib
import io
import json
from datetime import datetime, timedelta

import pytest
from botocore.exceptions import ClientError
from tools.weather_collect_hosted import (
    AttemptBudget, Deadline, HostedCollectionError, ScopedStore, collect_hosted,
    eligible_issues, main, run_id_for, validate_page,
)

NOW = datetime.fromisoformat("2026-10-13T09:45:00+09:00")
GRIDS = tuple({"place_id": str(nx), "nx": nx, "ny": 127} for nx in range(1, 81))


def error(code, status):
    return ClientError({"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}, "request")


class Client:
    def __init__(self):
        self.files = {}
        self.calls = []
        self.collisions = 0

    def get_object(self, **args):
        self.calls.append(("get", args["Key"]))
        key = args["Key"]
        if key not in self.files:
            raise error("NoSuchKey", 404)
        body = self.files[key]
        return {"Body": io.BytesIO(body), "ETag": hashlib.sha256(body).hexdigest(), "ContentLength": len(body)}

    def head_object(self, **args):
        self.calls.append(("head", args["Key"]))
        if args["Key"] not in self.files:
            raise error("NoSuchKey", 404)
        body = self.files[args["Key"]]
        return {"Metadata": {"sha256": hashlib.sha256(body).hexdigest()}, "ContentLength": len(body)}

    def put_object(self, **args):
        self.calls.append(("put", args["Key"]))
        key = args["Key"]
        old = self.files.get(key)
        if self.collisions:
            self.collisions -= 1
            raise error("PreconditionFailed", 412)
        if args.get("IfNoneMatch") == "*" and old is not None:
            raise error("PreconditionFailed", 412)
        if "IfMatch" in args and (old is None or hashlib.sha256(old).hexdigest() != args["IfMatch"]):
            raise error("PreconditionFailed", 412)
        self.files[key] = args["Body"]
        return {"ETag": hashlib.sha256(args["Body"]).hexdigest()}


def payload(issue, nx, page=1, total=1, rows=1):
    return json.dumps({"response": {"header": {"resultCode": "00"}, "body": {
        "pageNo": page, "numOfRows": 1000, "totalCount": total,
        "items": {"item": [{"baseDate": issue.strftime("%Y%m%d"), "baseTime": issue.strftime("%H%M"),
            "nx": nx, "ny": 127, "category": "TMP", "fcstDate": issue.strftime("%Y%m%d"),
            "fcstTime": "1200", "fcstValue": "1"} for _ in range(rows)]}}}}).encode()


class Source:
    def __init__(self, fail_nx=None):
        self.calls = []
        self.fail_nx = fail_nx

    def fetch_page(self, **args):
        self.calls.append(args)
        if args["nx"] == self.fail_nx:
            raise RuntimeError("token=secret https://private.example")
        issue = datetime.strptime(args["base_date"] + args["base_time"], "%Y%m%d%H%M").replace(tzinfo=NOW.tzinfo)
        return 200, payload(issue, args["nx"])


def run(client, source, now=NOW, only_latest=True):
    return collect_hosted(client, "private-bucket", source=source, grids=GRIDS,
                          clock=lambda: now, only_latest=only_latest)


def test_issue_window_delay_and_midnight():
    now = datetime.fromisoformat("2026-10-14T00:05:00+09:00")
    issues = eligible_issues(now)
    assert len(issues) == 8
    assert issues[-1].strftime("%Y%m%d%H%M") == "202610132300"
    assert eligible_issues(NOW.replace(hour=8, minute=39))[-1].hour == 5
    assert eligible_issues(NOW.replace(hour=8, minute=40))[-1].hour == 8


def test_complete_skip_has_no_source_calls_or_writes():
    client, source = Client(), Source()
    result = run(client, source)
    assert result["cycles_completed"] == 1 and len(source.calls) == 80
    before = sum(method == "put" for method, _ in client.calls)
    next_source = Source()
    assert run(client, next_source)["cycles_reused"] == 1
    assert not next_source.calls
    assert sum(method == "put" for method, _ in client.calls) == before


def test_partial_failure_resumes_only_missing_pages():
    client = Client()
    with pytest.raises(RuntimeError):
        run(client, Source(fail_nx=4))
    assert not any(key.endswith("/_manifest.json") for key in client.files)
    source = Source()
    result = run(client, source)
    assert result["raw_objects_reused"] == 3
    assert len(source.calls) == 77


def test_actual_landing_date_is_preserved_across_midnight_retry():
    client = Client()
    first = datetime.fromisoformat("2026-10-13T23:45:00+09:00")
    with pytest.raises(RuntimeError):
        run(client, Source(fail_nx=4), now=first)
    result = run(client, Source(), now=first + timedelta(minutes=30))
    assert result["cycles_completed"] == 1
    keys = [key for key in client.files if key.endswith("/_manifest.json")]
    assert len(keys) == 1 and "load_date=2026-10-13/" in keys[0]
    assert json.loads(client.files[keys[0]])["load_date"] == "2026-10-13"


def test_new_post_midnight_landing_uses_actual_date():
    client = Client()
    run(client, Source(), now=datetime.fromisoformat("2026-10-14T00:05:00+09:00"))
    key = next(key for key in client.files if key.endswith("/_manifest.json"))
    assert "load_date=2026-10-14/" in key and "hosted-20261013T2300" in key


def test_budget_cas_retries_and_daily_cap():
    client = Client()
    client.collisions = 2
    budget = AttemptBudget(client, "bucket", clock=lambda: NOW, deadline=Deadline())
    budget.reserve(None)
    doc = json.loads(next(iter(client.files.values())))
    assert doc["attempts"] == 1
    key = next(iter(client.files))
    doc["attempts"] = 2400
    client.files[key] = json.dumps(doc).encode()
    with pytest.raises(HostedCollectionError, match="daily_budget_exhausted"):
        budget.reserve(None)


@pytest.mark.parametrize("body", [b"null", b"{", b'{"schema_version":1,"budget_date":"2026-10-13","attempts":true}',
    b'{"schema_version":1,"budget_date":"2026-10-12","attempts":1}'])
def test_corrupt_budget_fails_closed(body):
    client = Client()
    client.files["ops/control/weather-hosted/api-budget/2026-10-13.json"] = body
    with pytest.raises(HostedCollectionError):
        AttemptBudget(client, "bucket", clock=lambda: NOW, deadline=Deadline()).reserve(None)
    assert not any(method == "put" for method, _ in client.calls)


def test_budget_read_forbidden_is_not_treated_as_missing():
    class Forbidden(Client):
        def get_object(self, **args):
            raise error("AccessDenied", 403)
    with pytest.raises(ClientError):
        AttemptBudget(Forbidden(), "bucket", clock=lambda: NOW, deadline=Deadline()).reserve(None)


def test_page_cap_rejects_before_more_fetches():
    with pytest.raises(HostedCollectionError, match="source_page_contract"):
        validate_page(payload(eligible_issues(NOW)[-1], 1, total=2001),
                      issue=eligible_issues(NOW)[-1], nx=1, ny=127, page_no=1)


def test_scope_rejects_unrelated_objects():
    store = ScopedStore(Client(), "bucket", eligible_issues(NOW)[-1], GRIDS, clock=lambda: NOW)
    for key in ("raw/other/a", "ops/control/checkpoints/weather/a", "raw/weather/kma_vilage_fcst/load_date=2026-10-13/run_id=airflow/x"):
        with pytest.raises(HostedCollectionError, match="storage_scope"):
            store.read_bytes(key)


def test_deadline_rejects_sleep_that_leaves_no_attempt_headroom():
    tick = [0.0]
    sleeps = []
    deadline = Deadline(monotonic=lambda: tick[0], sleep=sleeps.append)
    tick[0] = 1190
    with pytest.raises(HostedCollectionError, match="run_deadline"):
        deadline.pause(60)
    assert not sleeps


def test_cli_redacts_exceptions(monkeypatch, capsys):
    monkeypatch.setenv("GITHUB_REPOSITORY", "elsewhere/repo")
    assert main([]) == 2
    output = capsys.readouterr().out
    assert "repository_scope" in output and "elsewhere" not in output


def test_corrupt_old_complete_manifest_does_not_block_latest_collection():
    client = Client()
    old = NOW.replace(hour=5, minute=45)
    run(client, Source(), now=old)
    old_key = next(key for key in client.files if key.endswith("/_manifest.json"))
    doc = json.loads(client.files[old_key])
    doc["status"] = "PARTIAL"
    client.files[old_key] = json.dumps(doc).encode()
    original = client.files[old_key]
    result = run(client, Source(), only_latest=False)
    assert result["status"] == "degraded" and result["cycles_blocked"] == 1
    assert result["cycles_completed"] == 2
    assert result["source_issues_completed"][0] == "2026-10-13T08:00:00+09:00"
    assert client.files[old_key] == original


def test_completed_manifest_missing_raw_is_blocked_but_storage_forbidden_is_fatal():
    client = Client()
    run(client, Source(), now=NOW.replace(hour=5, minute=45))
    raw_key = next(key for key in client.files if "/nx=" in key)
    del client.files[raw_key]
    assert run(client, Source(), only_latest=False)["cycles_blocked"] == 1

    class Forbidden(Client):
        def get_object(self, **args):
            if args["Key"].endswith("/_manifest.json"):
                raise error("AccessDenied", 403)
            return super().get_object(**args)
    forbidden = Forbidden()
    forbidden.files = client.files
    source = Source()
    with pytest.raises(ClientError):
        run(forbidden, source, only_latest=False)
    assert not source.calls


def test_manifest_duplicate_page_and_future_timestamp_are_blocked():
    client = Client()
    run(client, Source())
    key = next(key for key in client.files if key.endswith("/_manifest.json"))
    original = json.loads(client.files[key])
    for updates in ({"completed_at": "2026-10-14T08:00:00+09:00"},
                    {"object_keys": original["object_keys"] + [original["object_keys"][0]],
                     "expected_count": 81, "actual_count": 81}):
        client.files[key] = json.dumps({**original, **updates}).encode()
        assert run(client, Source())["cycles_blocked"] == 1


def test_budget_conflict_limit_is_five_and_no_source_authorization():
    client = Client()
    client.collisions = 10
    with pytest.raises(HostedCollectionError, match="budget_conflict_exhausted"):
        AttemptBudget(client, "bucket", clock=lambda: NOW, deadline=Deadline()).reserve(None)
    assert sum(method == "put" for method, _ in client.calls) == 5


def test_budget_missing_requires_actual_404_and_attempt_cap_is_physical():
    class Misleading(Client):
        def get_object(self, **args):
            raise error("NoSuchKey", 500)
    with pytest.raises(ClientError):
        AttemptBudget(Misleading(), "bucket", clock=lambda: NOW, deadline=Deadline()).reserve(None)
    client = Client()
    budget = AttemptBudget(client, "bucket", clock=lambda: NOW, deadline=Deadline())
    budget.attempts = 1280
    with pytest.raises(HostedCollectionError, match="run_budget_exhausted"):
        budget.reserve(None)
    assert not client.calls


def test_budget_charges_uncertain_success_conservatively():
    class LostAcknowledgement(Client):
        def __init__(self):
            super().__init__()
            self.lost = False
        def put_object(self, **args):
            result = super().put_object(**args)
            if not self.lost:
                self.lost = True
                raise error("PreconditionFailed", 412)
            return result
    client = LostAcknowledgement()
    budget = AttemptBudget(client, "bucket", clock=lambda: NOW, deadline=Deadline())
    budget.reserve(None)
    assert budget.attempts == 1
    assert json.loads(next(iter(client.files.values())))["attempts"] == 2


def test_budget_refuses_request_near_daily_quota_boundary():
    late = NOW.replace(hour=23, minute=59, second=30)
    client = Client()
    with pytest.raises(HostedCollectionError, match="budget_day_boundary"):
        AttemptBudget(client, "bucket", clock=lambda: late, deadline=Deadline()).reserve(None)
    assert not client.calls


def test_oversized_response_is_stopped_during_stream(monkeypatch):
    from tools.weather_collect_hosted import BoundedTransport
    import requests
    class Response:
        status_code = 200
        headers = {}
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def iter_content(self, **args):
            yield b"x" * (512 * 1024)
            yield b"x"
            raise AssertionError("must stop reading before the rest of the response")
    class Session:
        def request(self, *args, **kwargs):
            assert kwargs["allow_redirects"] is False and kwargs["stream"] is True
            return Response()
    monkeypatch.setattr(requests, "Session", Session)
    with pytest.raises(HostedCollectionError, match="source_size_limit"):
        BoundedTransport(Deadline()).send("GET", "https://example.com", params={}, headers={}, timeout=30)


def test_cli_degraded_result_is_nonzero_and_summary_contains_real_counts(monkeypatch, tmp_path, capsys):
    import tools.weather_collect_hosted as module
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    monkeypatch.delenv("GITHUB_REF", raising=False)
    monkeypatch.setattr(module, "_client_from_env", lambda: (object(), "private"))
    result = {"status": "degraded", "cycles_completed": 1, "cycles_blocked": 1}
    monkeypatch.setattr(module, "collect_hosted", lambda *args, **kwargs: result)
    file = tmp_path / "summary.json"
    assert main(["--summary-file", str(file)]) == 2
    assert json.loads(file.read_text()) == result
    assert "private" not in capsys.readouterr().out


def test_cli_failure_writes_sanitized_summary(monkeypatch, tmp_path, capsys):
    import tools.weather_collect_hosted as module
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    monkeypatch.delenv("GITHUB_REF", raising=False)
    monkeypatch.setattr(module, "_client_from_env", lambda: (_ for _ in ()).throw(RuntimeError("secret-token")))
    file = tmp_path / "summary.json"
    assert main(["--summary-file", str(file)]) == 2
    doc = json.loads(file.read_text())
    assert doc == {"status": "failed", "reason_code": "collection_failed", "error_type": "RuntimeError"}
    assert "secret-token" not in capsys.readouterr().out


def test_valid_shaped_raw_tampering_with_old_hash_blocks_completion():
    class StoredMetadata(Client):
        def __init__(self):
            super().__init__()
            self.hashes = {}
        def put_object(self, **args):
            result = super().put_object(**args)
            if "Metadata" in args:
                self.hashes[args["Key"]] = args["Metadata"]["sha256"]
            return result
        def head_object(self, **args):
            result = super().head_object(**args)
            if args["Key"] in self.hashes:
                result["Metadata"]["sha256"] = self.hashes[args["Key"]]
            return result
    client = StoredMetadata()
    run(client, Source())
    key = next(key for key in client.files if "/nx=" in key)
    original_hash = client.hashes[key]
    doc = json.loads(client.files[key])
    doc["response"]["body"]["items"]["item"][0]["fcstValue"] = "99"
    client.files[key] = json.dumps(doc).encode()
    assert hashlib.sha256(client.files[key]).hexdigest() != original_hash
    before = sum(method == "put" for method, _ in client.calls)
    source = Source()
    result = run(client, source)
    assert result["status"] == "degraded" and result["cycles_blocked"] == 1
    assert not source.calls
    assert sum(method == "put" for method, _ in client.calls) == before


def test_every_retry_is_reserved_before_physical_http(monkeypatch):
    import tools.weather_collect_hosted as module
    from common.http.contract import TransportResponse
    responses = [429, 200]
    client = Client()
    deadline = Deadline(sleep=lambda _seconds: None)
    budget = AttemptBudget(client, "bucket", clock=lambda: NOW, deadline=deadline)
    class Transport:
        def __init__(self, _deadline):
            pass
        def send(self, *args, **kwargs):
            assert json.loads(next(iter(client.files.values())))["attempts"] == (3 - len(responses))
            status = responses.pop(0)
            return TransportResponse(status=status, content=payload(eligible_issues(NOW)[-1], 1))
    monkeypatch.setenv("KMA_SERVICE_KEY", "test-key")
    monkeypatch.setattr(module, "BoundedTransport", Transport)
    adapter = module._hosted_source(budget, deadline)
    assert adapter.fetch_page(base_date="20261013", base_time="0800", nx=1, ny=127,
                              page_no=1, num_of_rows=1000)[0] == 200
    assert budget.attempts == 2


def test_daily_reservation_cannot_authorize_http_after_midnight_acknowledgement():
    times = iter((NOW.replace(hour=23, minute=58), NOW.replace(hour=23, minute=58),
                  NOW.replace(hour=0, minute=0) + timedelta(days=1)))
    client = Client()
    with pytest.raises(HostedCollectionError, match="budget_day_boundary"):
        AttemptBudget(client, "bucket", clock=lambda: next(times), deadline=Deadline()).reserve(None)
    assert json.loads(next(iter(client.files.values())))["attempts"] == 1


def test_old_raw_source_schema_error_does_not_block_fresh_latest():
    client = Client()
    run(client, Source(), now=NOW.replace(hour=5, minute=45))
    raw_key = next(key for key in client.files if "/nx=" in key)
    doc = json.loads(client.files[raw_key])
    del doc["response"]["header"]
    client.files[raw_key] = json.dumps(doc).encode()
    result = run(client, Source(), only_latest=False)
    assert result["cycles_blocked"] == 1 and result["cycles_completed"] == 2
    assert result["source_issues_completed"][0] == "2026-10-13T08:00:00+09:00"


def test_invalid_old_raw_key_clock_is_isolated_from_latest_collection():
    client = Client()
    run(client, Source(), now=NOW.replace(hour=5, minute=45))
    key = next(key for key in client.files if key.endswith("/_manifest.json"))
    doc = json.loads(client.files[key])
    old_raw_key = doc["object_keys"][0]
    bad_raw_key = old_raw_key.replace("T054500KST", "T256199KST")
    assert bad_raw_key != old_raw_key
    doc["object_keys"][0] = bad_raw_key
    client.files[key] = json.dumps(doc).encode()
    result = run(client, Source(), only_latest=False)
    assert result["cycles_blocked"] == 1 and result["cycles_completed"] == 2
    assert result["source_issues_completed"][0] == "2026-10-13T08:00:00+09:00"
    assert json.loads(client.files[key])["object_keys"][0] == bad_raw_key
