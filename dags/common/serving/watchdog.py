"""Read-only Serving Contract v1 freshness watchdog (#720).

The Publisher records a successful D1 publication.  This module deliberately
does not write, retry, or republish: it independently compares that runtime
evidence with the immutable manifest contract, so a dead Publisher or a stale
upstream observation cannot be mistaken for a healthy product.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from math import ceil
from typing import Any, Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from common.serving.contract import ServingContract


class WatchdogQueryClient(Protocol):
    """The read-only subset used by the watchdog."""

    def execute(self, sql: str) -> list[dict[str, Any]]: ...


class ServingWatchdogError(RuntimeError):
    """Raised by the DAG task after all configured products were evaluated."""


@dataclass(frozen=True)
class ProductWatchdogResult:
    product_id: str
    model_name: str
    publication_id: str | None
    freshness_age_minutes: int | None
    publication_age_minutes: int | None
    issues: tuple[str, ...]
    query_availability_row_count: int | None = None
    query_availability_freshness_age_minutes: int | None = None

    @property
    def healthy(self) -> bool:
        return not self.issues


@dataclass(frozen=True)
class ServingWatchdogReport:
    checked_at: datetime
    products: tuple[ProductWatchdogResult, ...]

    @property
    def healthy(self) -> bool:
        return all(product.healthy for product in self.products)

    def payload(self) -> dict[str, object]:
        return {
            "checked_at": self.checked_at.isoformat(),
            "healthy": self.healthy,
            "products": [
                {
                    "product_id": product.product_id,
                    "model_name": product.model_name,
                    "publication_id": product.publication_id,
                    "freshness_age_minutes": product.freshness_age_minutes,
                    "publication_age_minutes": product.publication_age_minutes,
                    "issues": list(product.issues),
                    **({"query_availability_row_count": product.query_availability_row_count}
                       if product.query_availability_row_count is not None else {}),
                    **({"query_availability_freshness_age_minutes": product.query_availability_freshness_age_minutes}
                       if product.query_availability_freshness_age_minutes is not None else {}),
                }
                for product in self.products
            ],
        }


_EVIDENCE_SQL = """
SELECT
    c.name AS model_name,
    c.product_id AS product_id,
    c.publication_id AS catalog_publication_id,
    c.exported_at AS published_at,
    c.freshness AS catalog_freshness,
    c.serving_status AS catalog_serving_status,
    q.publication_id AS quality_publication_id,
    q.freshness_as_of AS quality_freshness,
    q.freshness_slo_minutes AS quality_freshness_slo_minutes,
    q.serving_status AS quality_serving_status,
    q.measured_at AS quality_measured_at
FROM _catalog AS c
LEFT JOIN d1_product_quality AS q
    ON q.product_id = c.product_id
""".strip()

_QUERY_AVAILABILITY_EVIDENCE_SQL = """
SELECT product_id, publication_id,
       count(*) AS row_count,
       count(DISTINCT place_id) AS distinct_place_count,
       sum(CASE WHEN place_id IS NULL THEN 1 ELSE 0 END) AS null_place_count,
       sum(CASE WHEN availability_status != 'complete' THEN 1 ELSE 0 END) AS incomplete_count,
       sum(CASE WHEN available_from_at > available_to_at
                     OR forecast_collected_at_min > forecast_collected_at_max
                     OR expected_forecast_hour_count < 0
                     OR observed_forecast_hour_count < 0
                     OR expected_forecast_hour_count != observed_forecast_hour_count
                     OR source_population_revision IS NULL OR source_population_revision = ''
                THEN 1 ELSE 0 END) AS invalid_count,
       count(DISTINCT availability_fingerprint) AS fingerprint_count,
       min(forecast_collected_at_min) AS forecast_collected_at_min
FROM d1_product_query_availability
GROUP BY product_id, publication_id
""".strip()


def _as_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return value.astimezone(timezone.utc)


def _parse_timestamp(
    value: object,
    *,
    naive_timezone: str | None,
) -> tuple[datetime | None, str | None]:
    """Parse one D1 timestamp without silently guessing a source timezone."""

    if not isinstance(value, str) or not value.strip():
        return None, "timestamp_missing"
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None, "timestamp_invalid"
    if parsed.tzinfo is None:
        if naive_timezone is None:
            return None, "timestamp_timezone_unknown"
        try:
            parsed = parsed.replace(tzinfo=ZoneInfo(naive_timezone))
        except ZoneInfoNotFoundError:
            return None, "timestamp_timezone_configuration_invalid"
    return parsed.astimezone(timezone.utc), None


def _age_minutes(now: datetime, observed: datetime) -> tuple[int | None, str | None]:
    delta_seconds = (now - observed).total_seconds()
    if delta_seconds < 0:
        return None, "timestamp_in_future"
    return ceil(delta_seconds / 60), None


def _publication_interval_minutes(trigger: Mapping[str, object] | None, *, now: datetime) -> tuple[int | None, str | None]:
    if not isinstance(trigger, Mapping):
        return None, "publication_trigger_missing"
    max_interval = trigger.get("max_interval_minutes")
    if isinstance(max_interval, int) and not isinstance(max_interval, bool) and max_interval > 0:
        return max_interval, None
    schedule = trigger.get("schedule_cron")
    if not isinstance(schedule, str) or not schedule.strip():
        return None, "publication_trigger_invalid"
    try:
        from croniter import croniter

        previous = croniter(schedule, now).get_prev(datetime)
        before_previous = croniter(schedule, previous).get_prev(datetime)
        interval_minutes = ceil((previous - before_previous).total_seconds() / 60)
    except (ImportError, TypeError, ValueError):
        return None, "publication_trigger_invalid"
    if interval_minutes <= 0:
        return None, "publication_trigger_invalid"
    return interval_minutes, None


def _integer(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _results_by_product(rows: Sequence[Mapping[str, object]]) -> dict[str, list[Mapping[str, object]]]:
    result: dict[str, list[Mapping[str, object]]] = {}
    for row in rows:
        product_id = row.get("product_id")
        if isinstance(product_id, str) and product_id:
            result.setdefault(product_id, []).append(row)
    return result


def evaluate_watchdog(
    client: WatchdogQueryClient,
    contracts: Sequence[ServingContract],
    *,
    checked_at: datetime,
    naive_freshness_timezones: Mapping[str, str] | None = None,
    publication_grace_minutes: int = 0,
) -> ServingWatchdogReport:
    """Evaluate static contract versus current D1 runtime evidence.

    A missing, stale, malformed, or internally inconsistent record is an issue;
    no condition is converted into a pass merely because the Publisher's latest
    write succeeded.
    """

    now = _as_utc(checked_at, field="checked_at")
    if (
        not isinstance(publication_grace_minutes, int)
        or isinstance(publication_grace_minutes, bool)
        or publication_grace_minutes < 0
    ):
        raise ValueError("publication_grace_minutes must be a non-negative integer")
    rows_by_product = _results_by_product(client.execute(_EVIDENCE_SQL))
    opted_in = [contract for contract in contracts if contract.query_availability_relation is not None]
    availability_by_identity = {
        (str(row.get("product_id")), str(row.get("publication_id"))): row
        for row in (client.execute(_QUERY_AVAILABILITY_EVIDENCE_SQL) if opted_in else [])
        if isinstance(row.get("product_id"), str) and isinstance(row.get("publication_id"), str)
    }
    timezone_by_product = dict(naive_freshness_timezones or {})
    results: list[ProductWatchdogResult] = []

    for contract in contracts:
        rows = rows_by_product.get(contract.product_id, [])
        issues: list[str] = []
        freshness_age: int | None = None
        publication_age: int | None = None
        publication_id: str | None = None
        availability_count: int | None = None
        availability_freshness_age: int | None = None

        if len(rows) != 1:
            issues.append("catalog_missing" if not rows else "catalog_product_id_duplicated")
            results.append(
                ProductWatchdogResult(
                    product_id=contract.product_id,
                    model_name=contract.model_name,
                    publication_id=None,
                    freshness_age_minutes=None,
                    publication_age_minutes=None,
                    issues=tuple(issues),
                )
            )
            continue

        row = rows[0]
        if row.get("model_name") != contract.model_name:
            issues.append("catalog_model_name_mismatch")
        if row.get("catalog_serving_status") not in {"published", "degraded"}:
            issues.append("catalog_serving_status_not_active")

        raw_catalog_publication_id = row.get("catalog_publication_id")
        if isinstance(raw_catalog_publication_id, str) and raw_catalog_publication_id:
            publication_id = raw_catalog_publication_id
        else:
            issues.append("catalog_publication_id_missing")

        raw_quality_publication_id = row.get("quality_publication_id")
        if not isinstance(raw_quality_publication_id, str) or not raw_quality_publication_id:
            issues.append("quality_missing")
        else:
            if publication_id is not None and raw_quality_publication_id != publication_id:
                issues.append("publication_id_mismatch")
            if row.get("quality_serving_status") not in {"published", "degraded"}:
                issues.append("quality_serving_status_not_active")

        if contract.freshness_slo_minutes is not None:
            if _integer(row.get("quality_freshness_slo_minutes")) != contract.freshness_slo_minutes:
                issues.append("freshness_slo_mismatch")
            freshness, parse_issue = _parse_timestamp(
                row.get("quality_freshness"),
                naive_timezone=timezone_by_product.get(contract.product_id),
            )
            if parse_issue is not None:
                issues.append(f"freshness_{parse_issue}")
            elif freshness is not None:
                freshness_age, age_issue = _age_minutes(now, freshness)
                if age_issue is not None:
                    issues.append(f"freshness_{age_issue}")
                elif freshness_age is not None and freshness_age > contract.freshness_slo_minutes:
                    issues.append("freshness_slo_breached")

            catalog_freshness, catalog_parse_issue = _parse_timestamp(
                row.get("catalog_freshness"),
                naive_timezone=timezone_by_product.get(contract.product_id),
            )
            if catalog_parse_issue is not None:
                issues.append(f"catalog_freshness_{catalog_parse_issue}")
            elif freshness is not None and catalog_freshness != freshness:
                issues.append("catalog_quality_freshness_mismatch")

        interval_minutes, trigger_issue = _publication_interval_minutes(
            contract.publication_trigger,
            now=now,
        )
        if trigger_issue is not None:
            issues.append(trigger_issue)
        published_at, published_parse_issue = _parse_timestamp(
            row.get("published_at"),
            naive_timezone=None,
        )
        if published_parse_issue is not None:
            issues.append(f"published_at_{published_parse_issue}")
        elif published_at is not None:
            publication_age, publication_age_issue = _age_minutes(now, published_at)
            if publication_age_issue is not None:
                issues.append(f"published_at_{publication_age_issue}")
            elif (
                interval_minutes is not None
                and publication_age is not None
                and publication_age > interval_minutes + publication_grace_minutes
            ):
                issues.append("publication_interval_breached")

        if contract.query_availability_relation is not None:
            availability = availability_by_identity.get((contract.product_id, publication_id or ""))
            if availability is None:
                any_product_sidecar = any(key[0] == contract.product_id for key in availability_by_identity)
                issues.append("query_availability_publication_mismatch" if any_product_sidecar else "query_availability_missing")
            else:
                availability_count = _integer(availability.get("row_count"))
                if availability_count != 427 or _integer(availability.get("distinct_place_count")) != 427 or _integer(availability.get("null_place_count")) != 0:
                    issues.append("query_availability_row_count_mismatch")
                if _integer(availability.get("incomplete_count")) != 0 or _integer(availability.get("invalid_count")) != 0:
                    issues.append("query_availability_incomplete")
                if _integer(availability.get("fingerprint_count")) != 1:
                    issues.append("query_availability_fingerprint_mismatch")
                collected, parse_issue = _parse_timestamp(
                    availability.get("forecast_collected_at_min"),
                    naive_timezone=timezone_by_product.get(contract.product_id),
                )
                if parse_issue is not None:
                    issues.append(f"query_availability_freshness_{parse_issue}")
                elif collected is not None:
                    availability_freshness_age, age_issue = _age_minutes(now, collected)
                    if age_issue is not None:
                        issues.append(f"query_availability_freshness_{age_issue}")
                    elif contract.freshness_slo_minutes is not None and availability_freshness_age > contract.freshness_slo_minutes:
                        issues.append("query_availability_freshness_slo_breached")

        results.append(
            ProductWatchdogResult(
                product_id=contract.product_id,
                model_name=contract.model_name,
                publication_id=publication_id,
                freshness_age_minutes=freshness_age,
                publication_age_minutes=publication_age,
                issues=tuple(dict.fromkeys(issues)),
                query_availability_row_count=availability_count,
                query_availability_freshness_age_minutes=availability_freshness_age,
            )
        )

    return ServingWatchdogReport(checked_at=now, products=tuple(results))


def raise_for_watchdog_failures(report: ServingWatchdogReport) -> None:
    """Raise one actionable Airflow failure after emitting the full product report."""

    failed = [result for result in report.products if not result.healthy]
    if not failed:
        return
    summary = "; ".join(
        f"{result.product_id}:{','.join(result.issues)}" for result in failed
    )
    raise ServingWatchdogError(f"Serving freshness watchdog failed: {summary}")
