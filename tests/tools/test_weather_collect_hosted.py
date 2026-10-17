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
    assert result["cycles_completed"] == 1
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
    module._domain_imports()
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
    assert result["cycles_blocked"] == 1 and result["cycles_completed"] == 1
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
    assert result["cycles_blocked"] == 1 and result["cycles_completed"] == 1
    assert result["source_issues_completed"][0] == "2026-10-13T08:00:00+09:00"
    assert json.loads(client.files[key])["object_keys"][0] == bad_raw_key


def test_page_failure_identifies_safe_rule_and_request_context():
    issue = eligible_issues(NOW)[-1]
    doc = json.loads(payload(issue, 1))
    doc['response']['body']['items']['item'][0]['fcstValue'] = ''
    doc['response']['header']['resultMsg'] = 'secret https://private.example'
    with pytest.raises(HostedCollectionError) as caught:
        validate_page(json.dumps(doc).encode(), issue=issue, nx=1, ny=127, page_no=1)
    diagnostic = caught.value.diagnostics
    assert diagnostic['validation_rule'] == 'required_fcstValue'
    assert diagnostic['source_issue'] == issue.isoformat()
    assert diagnostic['nx'] == 1 and diagnostic['ny'] == 127 and diagnostic['page_no'] == 1
    assert diagnostic['response_total_count'] == 1
    assert 'secret' not in json.dumps(diagnostic) and 'private.example' not in json.dumps(diagnostic)


def test_latest_source_contract_failure_keeps_progress_without_second_cycle():
    class InvalidLatest(Source):
        def fetch_page(self, **args):
            status, raw = super().fetch_page(**args)
            if args['base_time'] == '0800' and args['nx'] == 4:
                doc = json.loads(raw)
                doc['response']['body']['pageNo'] = 99
                return status, json.dumps(doc).encode()
            return status, raw
    client = Client()
    result = run(client, InvalidLatest(), only_latest=False)
    assert result['status'] == 'degraded' and result['cycles_failed'] == 1
    assert result['cycles_completed'] == 0
    assert result['source_issues_completed'] == []
    failure = result['failures'][0]
    assert failure['validation_rule'] == 'page_number'
    assert failure['checkpoint_pages'] == 3
    assert failure['source_issue'] == '2026-10-13T08:00:00+09:00'
    assert failure['checkpoint_write_acknowledged'] is True
    recovered = run(client, Source())
    assert recovered['raw_objects_reused'] == 3


def test_default_stops_after_one_cycle_instead_of_starting_second():
    client = Client()
    source = Source()
    result = collect_hosted(client, 'bucket', source=source, grids=GRIDS,
        clock=lambda: NOW, only_latest=False)
    assert result['cycles_completed'] == 1
    assert result['source_issues_completed'] == ['2026-10-13T08:00:00+09:00']
    assert result['status'] == 'success' and len(source.calls) == 80


def test_connection_timeout_retries_each_reserved_physical_attempt(monkeypatch):
    import tools.weather_collect_hosted as module
    module._domain_imports()
    from common.http.contract import TransportResponse
    from requests.exceptions import ConnectTimeout
    client = Client()
    pauses = []
    deadline = Deadline(sleep=pauses.append)
    budget = AttemptBudget(client, 'bucket', clock=lambda: NOW, deadline=deadline)
    attempts = []
    class Transport:
        def __init__(self, _deadline):
            pass
        def send(self, *args, **kwargs):
            attempts.append(budget.attempts)
            if len(attempts) < 3:
                raise ConnectTimeout('private token and URL')
            return TransportResponse(status=200, content=payload(eligible_issues(NOW)[-1], 1))
    monkeypatch.setenv('KMA_SERVICE_KEY', 'test-key')
    monkeypatch.setattr(module, 'BoundedTransport', Transport)
    source = module._hosted_source(budget, deadline)
    assert source.fetch_page(base_date='20261013',base_time='0800',nx=1,ny=127,
        page_no=1,num_of_rows=1000)[0] == 200
    assert attempts == [1,2,3] and pauses == [5,10]


def test_transport_guard_is_not_retried_as_a_connection_error(monkeypatch):
    import tools.weather_collect_hosted as module
    client = Client()
    deadline = Deadline(sleep=lambda _: pytest.fail('guard must not sleep'))
    budget = AttemptBudget(client, 'bucket', clock=lambda: NOW, deadline=deadline)
    class Transport:
        def __init__(self, _deadline):
            self.guard_failure = None
        def send(self, *args, **kwargs):
            self.guard_failure = HostedCollectionError('source_size_limit')
            raise self.guard_failure
    monkeypatch.setenv('KMA_SERVICE_KEY','test-key')
    monkeypatch.setattr(module,'BoundedTransport',Transport)
    with pytest.raises(HostedCollectionError,match='source_size_limit'):
        module._hosted_source(budget,deadline).fetch_page(base_date='20261013',base_time='0800',
            nx=1,ny=127,page_no=1,num_of_rows=1000)
    assert budget.attempts == 1


def test_network_retries_stop_at_three_and_report_safe_context(monkeypatch):
    import tools.weather_collect_hosted as module
    from requests.exceptions import ConnectTimeout
    module._domain_imports()
    client, pauses = Client(), []
    deadline = Deadline(sleep=pauses.append)
    budget = AttemptBudget(client,'bucket',clock=lambda: NOW,deadline=deadline)
    class Transport:
        def __init__(self,_deadline):
            pass
        def send(self,*args,**kwargs):
            raise ConnectTimeout('token=secret https://private.example')
    monkeypatch.setenv('KMA_SERVICE_KEY','test-key')
    monkeypatch.setattr(module,'BoundedTransport',Transport)
    source = module.ValidatedSource(module._hosted_source(budget,deadline),eligible_issues(NOW)[-1])
    with pytest.raises(HostedCollectionError,match='source_http_failure') as caught:
        source.fetch_page(base_date='20261013',base_time='0800',nx=1,ny=127,page_no=1,num_of_rows=1000)
    assert budget.attempts == 3 and pauses == [5,10]
    assert caught.value.diagnostics['source_http_kind'] == 'timeout'
    assert caught.value.diagnostics['page_no'] == 1
    assert 'secret' not in json.dumps(caught.value.diagnostics)


def test_cli_failure_retains_progress_without_provider_exception_text(monkeypatch,tmp_path,capsys):
    import tools.weather_collect_hosted as module
    exc = HostedCollectionError('run_deadline')
    exc.summary = {'status':'failed','cycles_completed':1,'physical_attempts':80,'failures':[]}
    monkeypatch.delenv('GITHUB_REPOSITORY',raising=False)
    monkeypatch.delenv('GITHUB_REF',raising=False)
    monkeypatch.setattr(module,'_client_from_env',lambda: (Client(),'bucket'))
    monkeypatch.setattr(module,'collect_hosted',lambda *a,**kw: (_ for _ in ()).throw(exc))
    file = tmp_path/'summary.json'
    assert main(['--summary-file',str(file)]) == 2
    doc = json.loads(file.read_text())
    assert doc['cycles_completed'] == 1 and doc['physical_attempts'] == 80
    assert doc['reason_code'] == 'run_deadline'
    assert json.loads(capsys.readouterr().out) == doc


def test_explicit_issue_resumes_only_the_eligible_selected_checkpoint():
    client, source = Client(), Source()
    issue = eligible_issues(NOW)[-2]
    result = collect_hosted(client,'bucket',source=source,grids=GRIDS,clock=lambda:NOW,source_issue=issue)
    assert result['source_issues_completed'] == [issue.isoformat()]
    assert len(source.calls) == 80 and {c['base_time'] for c in source.calls} == {'0500'}
    again = Source()
    before = sum(method=='put' for method,_ in client.calls)
    result = collect_hosted(client,'bucket',source=again,grids=GRIDS,clock=lambda:NOW,source_issue=issue)
    assert result['cycles_reused'] == 1 and result['physical_attempts'] == 0
    assert not again.calls and sum(method=='put' for method,_ in client.calls) == before


@pytest.mark.parametrize('issue',[NOW-timedelta(days=2),NOW.replace(hour=11,minute=0),NOW.replace(hour=8,minute=1)])
def test_explicit_issue_outside_release_window_is_rejected_without_requests_or_writes(issue):
    client,source = Client(),Source()
    with pytest.raises(HostedCollectionError,match='source_issue_outside_window'):
        collect_hosted(client,'bucket',source=source,grids=GRIDS,clock=lambda:NOW,source_issue=issue)
    assert not source.calls and not client.calls


def test_no_data_grid_does_not_block_other_grids_or_create_complete_manifest():
    class NoData(Source):
        def fetch_page(self,**args):
            if args['nx'] in (4,6):
                self.calls.append(args)
                return 200,json.dumps({'response':{'header':{'resultCode':'03','resultMsg':'NO_DATA'}}}).encode()
            return super().fetch_page(**args)
    client,source = Client(),NoData()
    result = run(client,source)
    assert result['status']=='degraded' and result['cycles_completed']==0
    assert len(source.calls)==80
    failure = result['failures'][0]
    assert failure['checkpoint_pages']==78
    assert {p['nx'] for p in failure['unavailable_source_pages']}=={4,6}
    assert not any(k.endswith('/_manifest.json') for k in client.files)
    recovered = Source()
    result = run(client,recovered)
    assert result['cycles_completed']==1 and result['raw_objects_reused']==78
    assert [c['nx'] for c in recovered.calls]==[4,6]


def test_repair_excludes_latest_and_failed_oldest_rotates_durably():
    class NoData(Source):
        def fetch_page(self, **args):
            self.calls.append(args)
            return 200, b'{"response":{"header":{"resultCode":"03"}}}'
    client, first = Client(), NoData()
    result = collect_hosted(client, 'bucket', source=first, grids=GRIDS, clock=lambda: NOW, repair_oldest=True)
    oldest = eligible_issues(NOW)[0]
    assert result['status'] == 'degraded' and result['cycles_failed'] == 1
    assert {call['base_date'] + call['base_time'] for call in first.calls} == {oldest.strftime('%Y%m%d%H%M')}
    next_source = Source()
    result = collect_hosted(client, 'bucket', source=next_source, grids=GRIDS, clock=lambda: NOW, repair_oldest=True)
    assert result['source_issues_completed'] == [eligible_issues(NOW)[1].isoformat()]
    assert result['source_issues_deferred'] == [oldest.isoformat()]
    assert eligible_issues(NOW)[-1].isoformat() not in result['source_issues_completed']


def test_repair_complete_repeat_has_no_storage_mutation_or_source_calls():
    client = Client()
    for issue in eligible_issues(NOW)[:-1]:
        collect_hosted(client, 'bucket', source=Source(), grids=GRIDS, clock=lambda: NOW, source_issue=issue)
    before = len([call for call in client.calls if call[0] == 'put'])
    source = Source()
    result = collect_hosted(client, 'bucket', source=source, grids=GRIDS, clock=lambda: NOW, repair_oldest=True)
    assert result['cycles_completed'] == 0 and result['cycles_reused'] == 7
    assert not source.calls and len([call for call in client.calls if call[0] == 'put']) == before


def test_mixed_no_data_timeout_success_share_three_physical_attempts(monkeypatch):
    import tools.weather_collect_hosted as module
    module._domain_imports()
    from common.http.contract import TransportResponse
    from requests.exceptions import ConnectTimeout
    client, pauses = Client(), []
    deadline = Deadline(sleep=pauses.append)
    budget = AttemptBudget(client, 'bucket', clock=lambda: NOW, deadline=deadline)
    attempts = []
    class Transport:
        def __init__(self, _deadline):
            pass
        def send(self, *args, **kwargs):
            attempts.append(budget.attempts)
            if len(attempts) == 1:
                return TransportResponse(status=200, content=b'{"response":{"header":{"resultCode":"03"}}}')
            if len(attempts) == 2:
                raise ConnectTimeout('private')
            return TransportResponse(status=200, content=payload(eligible_issues(NOW)[-1], 1))
    monkeypatch.setenv('KMA_SERVICE_KEY', 'test')
    monkeypatch.setattr(module, 'BoundedTransport', Transport)
    source = module.ValidatedSource(module._hosted_source(budget, deadline), eligible_issues(NOW)[-1])
    source.fetch_page(base_date='20261013', base_time='0800', nx=1, ny=127, page_no=1, num_of_rows=1000)
    assert attempts == [1, 2, 3] and pauses == [5, 10]


def test_manifest_verification_uses_at_most_four_read_workers():
    import threading
    import time
    class ConcurrentClient(Client):
        def __init__(self):
            super().__init__()
            self.lock = threading.Lock()
            self.active = self.maximum = 0
        def get_object(self, **args):
            if '/nx=' not in args['Key']:
                return super().get_object(**args)
            with self.lock:
                self.active += 1
                self.maximum = max(self.maximum, self.active)
            try:
                time.sleep(.002)
                return super().get_object(**args)
            finally:
                with self.lock:
                    self.active -= 1
    client = ConcurrentClient()
    run(client, Source())
    assert 1 < client.maximum <= 4
    client.maximum = 0
    assert run(client, Source())['cycles_reused'] == 1
    assert 1 < client.maximum <= 4


@pytest.mark.parametrize('status,body', [(200,b''), (200,b'not-json'), (200,b'null'),
    (200,b'{"response":{"header":{"resultCode":"30"}}}'), (401,b'')])
def test_hosted_retries_no_schema_empty_or_auth_failures(monkeypatch,status,body):
    import tools.weather_collect_hosted as module
    module._domain_imports()
    from common.http.contract import TransportResponse
    client = Client()
    deadline = Deadline(sleep=lambda _: pytest.fail('must not retry'))
    budget = AttemptBudget(client,'bucket',clock=lambda:NOW,deadline=deadline)
    class Transport:
        def __init__(self,_deadline):
            pass
        def send(self,*args,**kwargs):
            return TransportResponse(status=status,content=body)
    monkeypatch.setenv('KMA_SERVICE_KEY','test')
    monkeypatch.setattr(module,'BoundedTransport',Transport)
    with pytest.raises(HostedCollectionError):
        module.ValidatedSource(module._hosted_source(budget,deadline),eligible_issues(NOW)[-1]).fetch_page(
            base_date='20261013',base_time='0800',nx=1,ny=127,page_no=1,num_of_rows=1000)
    assert budget.attempts == 1


def test_retry_after_longer_than_deadline_stops_before_second_reservation(monkeypatch):
    import tools.weather_collect_hosted as module
    module._domain_imports()
    from common.http.contract import TransportResponse
    client = Client()
    deadline = Deadline(sleep=lambda _:pytest.fail('cannot sleep beyond deadline'))
    budget = AttemptBudget(client,'bucket',clock=lambda:NOW,deadline=deadline)
    class Transport:
        def __init__(self,_deadline):
            pass
        def send(self,*args,**kwargs):
            return TransportResponse(status=429,headers={'Retry-After':'3600'})
    monkeypatch.setenv('KMA_SERVICE_KEY','test')
    monkeypatch.setattr(module,'BoundedTransport',Transport)
    with pytest.raises(HostedCollectionError,match='run_deadline'):
        module._hosted_source(budget,deadline).fetch_page(base_date='20261013',base_time='0800',
            nx=1,ny=127,page_no=1,num_of_rows=1000)
    assert budget.attempts == 1


def test_persistent_no_data_uses_only_three_physical_attempts(monkeypatch):
    import tools.weather_collect_hosted as module
    module._domain_imports()
    from common.http.contract import TransportResponse
    client,pauses = Client(),[]
    deadline = Deadline(sleep=pauses.append)
    budget = AttemptBudget(client,'bucket',clock=lambda:NOW,deadline=deadline)
    class Transport:
        def __init__(self,_deadline):
            pass
        def send(self,*args,**kwargs):
            return TransportResponse(status=200,content=b'{"response":{"header":{"resultCode":"03"}}}')
    monkeypatch.setenv('KMA_SERVICE_KEY','test')
    monkeypatch.setattr(module,'BoundedTransport',Transport)
    with pytest.raises(HostedCollectionError,match='source_page_contract') as caught:
        module.ValidatedSource(module._hosted_source(budget,deadline),eligible_issues(NOW)[-1]).fetch_page(
            base_date='20261013',base_time='0800',nx=1,ny=127,page_no=1,num_of_rows=1000)
    assert budget.attempts==3 and pauses==[5,10]
    assert caught.value.diagnostics['source_result_code']=='03'


def test_repair_retry_corruption_and_forbidden_reads_fail_closed():
    from tools.weather_collect_hosted import REPAIR_RETRY_PREFIX
    issue = eligible_issues(NOW)[0]
    key = f'{REPAIR_RETRY_PREFIX}/{run_id_for(issue)}.json'
    client,source = Client(),Source()
    client.files[key]=b'{"schema_version":true}'
    with pytest.raises(HostedCollectionError,match='repair_retry_invalid'):
        collect_hosted(client,'bucket',source=source,grids=GRIDS,clock=lambda:NOW,repair_oldest=True)
    assert not source.calls and not any(method=='put' for method,_ in client.calls)
    class Forbidden(Client):
        def get_object(self,**options):
            raise error('AccessDenied',403)
    with pytest.raises(ClientError):
        collect_hosted(Forbidden(),'bucket',source=source,grids=GRIDS,clock=lambda:NOW,repair_oldest=True)
    assert not source.calls


def test_partial_two_page_hosted_grid_refetches_only_its_grid_after_no_data():
    class ChangedSource(Source):
        def __init__(self,second_attempt=False):
            super().__init__()
            self.second_attempt=second_attempt
        def fetch_page(self,**args):
            self.calls.append(args)
            issue = eligible_issues(NOW)[-1]
            if args['nx']==4 and not self.second_attempt:
                if args['page_no']==2:
                    return 200,b'{"response":{"header":{"resultCode":"03"}}}'
                return 200,payload(issue,4,total=1001,rows=1000)
            return 200,payload(issue,args['nx'])
    client = Client()
    failed = run(client,ChangedSource())
    assert failed['status']=='degraded' and failed['failures'][0]['checkpoint_pages']==80
    raw_before = {key:body for key,body in client.files.items() if '/nx=' in key}
    recovered = ChangedSource(second_attempt=True)
    result = run(client,recovered)
    assert result['cycles_completed']==1 and result['raw_objects_reused']==79
    assert [(call['nx'],call['page_no']) for call in recovered.calls]==[(4,1)]
    assert all(client.files[key]==body for key,body in raw_before.items())


def test_repair_cooldown_expires_and_refetches_same_oldest_issue():
    from tools.weather_collect_hosted import RepairRetry
    client = Client()
    issue = eligible_issues(NOW)[0]
    earlier = NOW - timedelta(hours=4)
    state = RepairRetry(client,'bucket',issue,clock=lambda:earlier,deadline=Deadline())
    assert state.deferred() is False
    state.defer('source_http_failure')
    source = Source()
    result = collect_hosted(client,'bucket',source=source,grids=GRIDS,clock=lambda:NOW,repair_oldest=True)
    assert result['source_issues_completed']==[issue.isoformat()]
    assert result['source_issues_deferred']==[] and len(source.calls)==80


def test_repair_retry_compare_and_swap_conflict_fails_closed():
    from tools.weather_collect_hosted import RepairRetry
    client = Client()
    state = RepairRetry(client,'bucket',eligible_issues(NOW)[0],clock=lambda:NOW,deadline=Deadline())
    assert not state.deferred()
    client.collisions=1
    with pytest.raises(HostedCollectionError,match='repair_retry_conflict'):
        state.defer('source_page_contract')
    assert not client.files


def test_repair_write_failure_keeps_original_source_failure():
    class NoData(Source):
        def fetch_page(self,**args):
            self.calls.append(args)
            return 200,b'{"response":{"header":{"resultCode":"03"}}}'
    class FailedWrite(Client):
        def put_object(self,**args):
            if '/repair-retry/' in args['Key']:
                raise error('AccessDenied',403)
            return super().put_object(**args)
    with pytest.raises(HostedCollectionError,match='source_page_contract') as caught:
        collect_hosted(FailedWrite(),'bucket',source=NoData(),grids=GRIDS,clock=lambda:NOW,repair_oldest=True)
    failure = caught.value.summary['failures'][0]
    assert failure['reason_code']=='source_page_contract'
    assert failure['source_result_code']=='03' and failure['repair_retry_persisted'] is False
    assert failure['repair_retry_write_reason']=='storage_failure'


def test_hosted_repeated_interruption_preserves_unvisited_grids_without_checkpoint_rewrites():
    class NoData(Source):
        def fetch_page(self,**args):
            if args['nx']==4:
                return 200,b'{"response":{"header":{"resultCode":"03"}}}'
            return super().fetch_page(**args)
    client = Client()
    assert run(client,NoData())['status']=='degraded'
    key = next(key for key in client.files if '/checkpoints/' in key)
    original = client.files[key]
    before = sum(method=='put' for method,_ in client.calls)
    with pytest.raises(HostedCollectionError,match='collection_failed'):
        run(client,Source(fail_nx=4))
    assert client.files[key]==original
    assert sum(method=='put' for method,_ in client.calls)==before
    source = Source()
    result = run(client,source)
    assert result['raw_objects_reused']==79 and [call['nx'] for call in source.calls]==[4]
    assert result['phase_seconds']['checkpoint_resume_validation'] >= 0
