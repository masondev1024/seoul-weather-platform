from datetime import date, datetime, timezone
import json
import sys
from types import ModuleType

import pytest

from tools import weather_daily_quality_evidence as evidence


AS_OF = datetime(2026, 10, 14, 1, 0, tzinfo=timezone.utc)
REPORT_DATE = date(2026, 10, 13)


class Cursor:
    def __init__(self, names=(), rows=(), error=None):
        self.description = [(name,) for name in names]
        self.rows = list(rows)
        self.error = error
        self.query_id = '20261014_010000_00001_test1'
        self.sql = None
        self.cancelled = self.closed = False

    def execute(self, sql):
        self.sql = sql
        if self.error:
            raise self.error

    def fetchmany(self, size):
        batch, self.rows = self.rows[:size], self.rows[size:]
        return batch

    def cancel(self):
        self.cancelled = True

    def close(self):
        self.closed = True


class Connection:
    def __init__(self, cursor):
        self.fake_cursor = cursor
        self.closed = False

    def cursor(self):
        return self.fake_cursor

    def close(self):
        self.closed = True


def collect(monkeypatch, cursors):
    connections = [Connection(cursor) for cursor in cursors]
    pending = iter(connections)
    monkeypatch.setattr(evidence, '_connect', lambda config: next(pending))
    result = evidence.collect_evidence(evidence.EvidenceConfig(), REPORT_DATE, AS_OF)
    assert all(connection.closed for connection in connections)
    assert all(cursor.cancelled and cursor.closed for cursor in cursors)
    return result


def test_queries_are_bounded_read_only_and_use_utc_cutoff(monkeypatch):
    cursors = [Cursor(), Cursor(), Cursor()]
    result = collect(monkeypatch, cursors)
    assert set(result) == {'coverage', 'duplicates', 'manifests'}
    assert all(phase['status'] == 'measured' for phase in result.values())
    sql = '\n'.join(cursor.sql for cursor in cursors)
    assert 'iceberg.ask_seoul.bronze_kma_vilage_fcst' in sql
    assert 'bronze_collection_run_manifest' in sql
    assert "load_date BETWEEN '2026-10-13' AND '2026-10-14'" in sql
    assert "collected_at <= TIMESTAMP '2026-10-14 01:00:00.000000'" in sql
    assert "source_id = 'kma_vilage_fcst'" in sql
    assert "result_code = '00'" in sql
    assert "base_date = '20261013'" in sql
    assert "event_at >= TIMESTAMP '2026-10-12 00:00:00.000000'" in sql
    assert 'LIMIT 30001' in sql and 'LIMIT 10001' in sql
    for verb in ('INSERT ', 'UPDATE ', 'DELETE ', 'CREATE ', 'MERGE ', 'DROP '):
        assert verb not in sql.upper()


def test_duplicate_grain_preserves_runs_and_places(monkeypatch):
    cursors = [Cursor(), Cursor(), Cursor()]
    collect(monkeypatch, cursors)
    duplicate_sql = next(cursor.sql for cursor in cursors if 'duplicate_excess_rows' in cursor.sql)
    assert 'GROUP BY dag_run_id, place_id, nx, ny, base_date, base_time, category, fcst_date, fcst_time' in duplicate_sql
    assert 'sum(row_count - 1)' in duplicate_sql.lower()
    assert 'invalid_grain_rows' in duplicate_sql


def test_query_failure_is_independent_and_redacted(monkeypatch):
    cursors = [Cursor(error=RuntimeError('serviceKey=secret SQL credential')), Cursor(), Cursor()]
    result = collect(monkeypatch, cursors)
    assert result['coverage']['status'] == 'unmeasured'
    assert result['coverage']['reason_code'] == 'query_failed'
    assert result['duplicates']['status'] == result['manifests']['status'] == 'measured'
    assert 'secret' not in json.dumps(result)


def test_timestamp_rows_are_json_serializable_and_empty_is_measured(monkeypatch):
    cursors = [Cursor(['last_collected_at_utc'], [(datetime(2026, 10, 13, 15),)]), Cursor(), Cursor()]
    result = collect(monkeypatch, cursors)
    assert result['coverage']['rows'][0]['last_collected_at_utc'] == '2026-10-13T15:00:00+00:00'
    assert result['manifests']['status'] == 'measured' and result['manifests']['rows'] == []
    json.dumps(result)


def test_row_limit_fails_closed(monkeypatch):
    cursors = [Cursor(['base_date'], [('20261013',)] * 30001), Cursor(), Cursor()]
    result = collect(monkeypatch, cursors)
    assert result['coverage']['reason_code'] == 'row_limit_exceeded'
    assert result['coverage']['rows'] == []


@pytest.mark.parametrize('kwargs', [
    {'host': 'example.com'}, {'host': 'http://127.0.0.1'},
    {'catalog': 'iceberg; DROP TABLE x'}, {'schema': 'foo.bar'},
    {'port': 0}, {'timeout_seconds': 0}, {'query_timeout_seconds': 601},
    {'user': 'token=secret'},
])
def test_configuration_rejects_remote_and_unbounded_values(kwargs):
    with pytest.raises(ValueError):
        evidence.EvidenceConfig(**kwargs)


@pytest.mark.parametrize('as_of', [
    datetime(2026, 10, 14),
    datetime(2026, 10, 13, 8, tzinfo=timezone.utc),
    datetime(2026, 10, 16, tzinfo=timezone.utc),
    datetime(2026, 10, 11, tzinfo=timezone.utc),
])
def test_time_window_rejected_before_connect(monkeypatch, as_of):
    monkeypatch.setattr(evidence, '_connect', lambda _: pytest.fail('must not connect'))
    with pytest.raises(ValueError):
        evidence.collect_evidence(evidence.EvidenceConfig(), REPORT_DATE, as_of)


def test_missing_dependency_is_unmeasured(monkeypatch):
    def missing(_):
        raise ImportError('trino unavailable')
    monkeypatch.setattr(evidence, '_connect', missing)
    result = evidence.collect_evidence(evidence.EvidenceConfig(), REPORT_DATE, AS_OF)
    assert all(phase['reason_code'] == 'trino_client_unavailable' for phase in result.values())


def test_connection_has_socket_and_server_time_limits_without_retries(monkeypatch):
    module = ModuleType('trino.dbapi')
    captured = {}
    module.connect = lambda **kwargs: captured.update(kwargs)
    monkeypatch.setitem(sys.modules, 'trino.dbapi', module)
    evidence._connect(evidence.EvidenceConfig())
    assert captured['request_timeout'] == 15
    assert captured['session_properties'] == {'query_max_run_time': '60s'}
    assert captured['max_attempts'] == 1
    assert captured['timezone'] == 'UTC'
    assert captured['host'] == '127.0.0.1'


def test_elapsed_query_timeout_cancels_and_continues_other_phases(monkeypatch):
    # Each phase measures elapsed time in finally in addition to deadline checks.
    ticks = iter([0, 61, 62, 63, 63, 63, 64, 65, 65, 65, 66])
    monkeypatch.setattr(evidence.time, 'monotonic', lambda: next(ticks))
    result = collect(monkeypatch, [Cursor(), Cursor(), Cursor()])
    assert result['coverage']['reason_code'] == 'query_timeout'
    assert result['coverage']['duration_seconds'] == 62
    assert result['duplicates']['status'] == result['manifests']['status'] == 'measured'


def test_invalid_query_id_cannot_expose_secrets(monkeypatch):
    cursors = [Cursor(), Cursor(), Cursor()]
    cursors[0].query_id = 'SQL serviceKey=secret'
    result = collect(monkeypatch, cursors)
    assert result['coverage']['query_ids'] == []
    assert 'secret' not in json.dumps(result)


def test_cleanup_errors_do_not_replace_evidence(monkeypatch):
    def broken_cleanup():
        raise RuntimeError('token=secret')
    cursors = [Cursor(), Cursor(), Cursor()]
    cursors[0].cancel = broken_cleanup
    connections = iter(Connection(cursor) for cursor in cursors)
    monkeypatch.setattr(evidence, '_connect', lambda _: next(connections))
    result = evidence.collect_evidence(evidence.EvidenceConfig(), REPORT_DATE, AS_OF)
    assert result['coverage']['status'] == 'measured'
    assert 'secret' not in json.dumps(result)


def test_safe_query_cost_stats_and_local_elapsed_measurement(monkeypatch):
    cursors = [Cursor(), Cursor(), Cursor()]
    cursors[0].stats = {'physicalInputBytes': 123, 'processedBytes': 456.0, 'unknown': 'serviceKey=secret'}
    result = collect(monkeypatch, cursors)
    assert result['coverage']['physical_input_bytes'] == 123
    assert result['coverage']['processed_bytes'] == 456.0
    assert result['coverage']['duration_seconds'] >= 0
    assert result['duplicates']['physical_input_bytes'] is None
    assert result['duplicates']['processed_bytes'] is None
    assert 'secret' not in json.dumps(result)


@pytest.mark.parametrize('value', [True, False, 'serviceKey=secret', -1, float('inf'), float('nan')])
def test_unsafe_cost_values_are_unknown(monkeypatch, value):
    cursors = [Cursor(), Cursor(), Cursor()]
    cursors[0].stats = {'physicalInputBytes': value, 'processedBytes': value}
    result = collect(monkeypatch, cursors)
    assert result['coverage']['physical_input_bytes'] is None
    assert result['coverage']['processed_bytes'] is None
    json.dumps(result, allow_nan=False)


def test_stats_property_errors_preserve_result(monkeypatch):
    class BadStatsCursor(Cursor):
        @property
        def stats(self):
            raise RuntimeError('serviceKey=secret')
    result = collect(monkeypatch, [BadStatsCursor(), Cursor(), Cursor()])
    assert result['coverage']['status'] == 'measured'
    assert result['coverage']['physical_input_bytes'] is None
    assert 'secret' not in json.dumps(result)


def test_loopback_connection_refused_is_unavailable_and_redacted(monkeypatch):
    def unavailable(_):
        raise ConnectionRefusedError('token=secret')
    monkeypatch.setattr(evidence, '_connect', unavailable)
    result = evidence.collect_evidence(evidence.EvidenceConfig(), REPORT_DATE, AS_OF)
    assert all(phase['reason_code'] == 'trino_unavailable' for phase in result.values())
    assert all(phase['physical_input_bytes'] is None for phase in result.values())
    assert 'secret' not in json.dumps(result)
