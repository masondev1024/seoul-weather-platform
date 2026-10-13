"""Bounded, read-only Trino evidence for Weather daily quality reports.

An unavailable query is never an empty successful dataset. Collection completeness
and acceptance of individual runs are evaluated by the report layer using manifests.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import ipaddress
import math
import re
import time
from zoneinfo import ZoneInfo


KST = ZoneInfo('Asia/Seoul')
IDENTIFIER = re.compile(r'[A-Za-z_][A-Za-z0-9_]*\Z')
QUERY_ID = re.compile(r'[A-Za-z0-9_]{1,128}\Z')
SOURCE_ID = 'kma_vilage_fcst'


@dataclass(frozen=True)
class EvidenceConfig:
    host: str = '127.0.0.1'
    port: int = 30586
    catalog: str = 'iceberg'
    schema: str = 'ask_seoul'
    user: str = 'weather_quality_report'
    timeout_seconds: int = 15
    query_timeout_seconds: int = 60

    def __post_init__(self):
        if not isinstance(self.host, str):
            raise ValueError('Trino evidence requires a loopback host')
        try:
            loopback = self.host == 'localhost' or ipaddress.ip_address(self.host).is_loopback
        except ValueError:
            loopback = False
        if not loopback:
            raise ValueError('Trino evidence requires a loopback host')
        for name in ('catalog', 'schema', 'user'):
            value = getattr(self, name)
            if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
                raise ValueError(f'invalid {name} identifier')
        for name, maximum in (('port', 65535), ('timeout_seconds', 60), ('query_timeout_seconds', 600)):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
                raise ValueError(f'invalid {name} bound')


def _connect(config: EvidenceConfig):
    # Lazy import lets reports record unavailable evidence without a Trino runtime.
    from trino.dbapi import connect
    return connect(
        host=config.host, port=config.port, user=config.user,
        catalog=config.catalog, schema=config.schema, http_scheme='http',
        timezone='UTC', request_timeout=config.timeout_seconds, max_attempts=1,
        session_properties={'query_max_run_time': f'{config.query_timeout_seconds}s'},
    )


class _EvidenceLimit(Exception):
    pass


def _json_value(value):
    if isinstance(value, datetime):
        # Bronze and manifest timestamp(6) values store naive UTC.
        return value.replace(tzinfo=timezone.utc).isoformat() if value.tzinfo is None else value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _query_ids(cursor) -> list[str]:
    try:
        query_id = getattr(cursor, 'query_id', None)
    except Exception:
        return []
    return [query_id] if isinstance(query_id, str) and QUERY_ID.fullmatch(query_id) else []


def _close(resource, method):
    if resource is not None:
        try:
            getattr(resource, method)()
        except Exception:
            # Cleanup cannot overwrite measured evidence or expose connection details.
            pass


def _safe_number(value):
    if type(value) not in (int, float) or value < 0:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _cost_stats(cursor):
    try:
        stats = getattr(cursor, 'stats', None)
    except Exception:
        stats = None
    if not isinstance(stats, dict):
        stats = {}
    return {
        'physical_input_bytes': _safe_number(stats.get('physicalInputBytes')),
        'processed_bytes': _safe_number(stats.get('processedBytes')),
    }


def _network_failure(exception):
    if isinstance(exception, ConnectionError):
        return True
    try:
        from requests.exceptions import ConnectionError as RequestConnectionError
    except ImportError:
        return False
    return isinstance(exception, RequestConnectionError)


def _measure(config, sql, row_limit):
    connection = cursor = None
    result = {
        'status': 'unmeasured', 'reason_code': 'query_failed', 'rows': [], 'query_ids': [],
        'duration_seconds': None, 'physical_input_bytes': None, 'processed_bytes': None,
    }
    started = time.monotonic()
    try:
        connection = _connect(config)
        cursor = connection.cursor()
        cursor.execute(sql)
        names = [column[0] for column in cursor.description]
        rows = []
        while True:
            if time.monotonic() - started >= config.query_timeout_seconds:
                raise TimeoutError
            batch = cursor.fetchmany(min(1000, row_limit + 1 - len(rows)))
            if time.monotonic() - started >= config.query_timeout_seconds:
                raise TimeoutError
            if not batch:
                break
            rows.extend(batch)
            if len(rows) > row_limit:
                raise _EvidenceLimit
        result.update(status='measured', reason_code='measured', rows=[
            {name: _json_value(value) for name, value in zip(names, row, strict=True)}
            for row in rows
        ])
    except ImportError:
        result['reason_code'] = 'trino_client_unavailable'
    except _EvidenceLimit:
        result['reason_code'] = 'row_limit_exceeded'
    except TimeoutError:
        result['reason_code'] = 'query_timeout'
    except Exception as exception:
        # SQL, exception text, endpoint responses and credentials never enter reports.
        if _network_failure(exception):
            result['reason_code'] = 'trino_unavailable'
    finally:
        result['duration_seconds'] = _safe_number(time.monotonic() - started)
        result['query_ids'] = _query_ids(cursor)
        result.update(_cost_stats(cursor))
        _close(cursor, 'cancel')
        _close(cursor, 'close')
        _close(connection, 'close')
    return result


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')


def collect_evidence(config: EvidenceConfig, report_date: date, evaluation_as_of: datetime) -> dict:
    """Measure three independent phases; never create tables or recover services.

    At most three KST load partitions are examined, allowing the report day plus
    two days of late arrivals. Older evaluations require a separately scoped audit.
    """
    if not isinstance(report_date, date) or isinstance(report_date, datetime):
        raise ValueError('report_date must be a date')
    if evaluation_as_of.tzinfo is None or evaluation_as_of.utcoffset() is None:
        raise ValueError('evaluation_as_of must be timezone aware')
    evaluation_date = evaluation_as_of.astimezone(KST).date()
    if not report_date + timedelta(days=1) <= evaluation_date <= report_date + timedelta(days=2):
        raise ValueError('report date must be a complete past KST day within two days of evaluation')
    table = f'{config.catalog}.{config.schema}.bronze_kma_vilage_fcst'
    manifest_table = f'{config.catalog}.{config.schema}.bronze_collection_run_manifest'
    partition_filter = f"load_date BETWEEN '{report_date.isoformat()}' AND '{evaluation_date.isoformat()}'"
    time_filter = f"collected_at <= TIMESTAMP '{_timestamp(evaluation_as_of)}'"
    common_filter = f"source_id = '{SOURCE_ID}' AND result_code = '00' AND {partition_filter} AND {time_filter}"
    invalid_grain = ' OR '.join(f'{column} IS NULL' for column in (
        'dag_run_id', 'place_id', 'nx', 'ny', 'base_date', 'base_time', 'category', 'fcst_date', 'fcst_time',
    ))
    coverage_sql = f"""
        SELECT base_date, base_time, place_id, nx, ny, dag_run_id,
               count(*) AS row_count, count(DISTINCT category) AS category_count,
               max(collected_at) AS last_collected_at_utc,
               count_if({invalid_grain}) AS invalid_rows
        FROM {table}
        WHERE {common_filter}
          AND base_date BETWEEN '{report_date:%Y%m%d}' AND '{evaluation_date:%Y%m%d}'
        GROUP BY base_date, base_time, place_id, nx, ny, dag_run_id
        ORDER BY base_date, base_time, place_id, nx, ny, dag_run_id
        LIMIT 30001
    """
    duplicates_sql = f"""
        WITH grain_counts AS (
            SELECT dag_run_id, place_id, nx, ny, base_date, base_time, category, fcst_date, fcst_time,
                   count(*) AS row_count,
                   count_if({invalid_grain}) AS invalid_rows
            FROM {table}
            WHERE {common_filter} AND base_date = '{report_date:%Y%m%d}'
            GROUP BY dag_run_id, place_id, nx, ny, base_date, base_time, category, fcst_date, fcst_time
        )
        SELECT count_if(row_count > 1) AS duplicate_groups,
               coalesce(sum(row_count - 1), 0) AS duplicate_excess_rows,
               coalesce(sum(row_count), 0) AS total_rows,
               coalesce(sum(invalid_rows), 0) AS invalid_grain_rows
        FROM grain_counts
    """
    manifest_start = datetime.combine(report_date, datetime.min.time(), tzinfo=timezone.utc) - timedelta(days=1)
    manifests_sql = f"""
        SELECT dag_run_id, status, is_publishable, event_at, expected_rows, actual_rows,
               expected_raw_objects, actual_raw_objects
        FROM {manifest_table}
        WHERE source_id = '{SOURCE_ID}'
          AND event_at >= TIMESTAMP '{_timestamp(manifest_start)}'
          AND event_at <= TIMESTAMP '{_timestamp(evaluation_as_of)}'
        ORDER BY dag_run_id, event_at, status
        LIMIT 10001
    """
    return {
        'coverage': _measure(config, coverage_sql, 30000),
        'duplicates': _measure(config, duplicates_sql, 1),
        'manifests': _measure(config, manifests_sql, 10000),
    }
