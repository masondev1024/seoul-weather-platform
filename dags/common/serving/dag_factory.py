"""Thin per-domain serving-export DAG factory.

A domain DAG declares only its ``domain`` and ``product_ids``; everything else —
contract load, gate, D1 write, verify, ``_catalog`` upsert, smoke — is the common
publisher. Example (domains/weather/weather_serving_export.py)::

    from common.serving.dag_factory import build_serving_export_dag

    dag = build_serving_export_dag(
        domain="weather",
        product_ids=["weather_place_current_outlook"],
        schedule="10 * * * *",
    )

Airflow is imported here only; the publisher/gate/contract modules stay import-clean
for unit tests.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone

from common.ops.product_observability import record_product_event
from common.pools import SERVING_D1_PUBLISH_POOL
from common.serving.publisher import ProductRecord, PublicationError

# dbt project that owns each domain's manifest (weather+traffic share the monoproject).
_DBT_PROJECT = {"weather": "traffic_weather", "traffic": "traffic_weather"}


def publication_record_payload(record: ProductRecord) -> dict[str, object]:
    """Keep the operator XCom aligned with the immutable publication ledger."""

    return {
        "product_id": record.product_id,
        "serving_status": record.serving_status,
        "source_row_count": record.source_row_count,
        "published_row_count": record.published_row_count,
        "d1_row_count": record.d1_row_count,
        "distinct_primary_key_count": record.distinct_primary_key_count,
        "null_primary_key_count": record.null_primary_key_count,
        "api_smoke_status": record.api_smoke_status,
        "api_smoke_detail": record.api_smoke_detail,
        "publication_id": record.publication_id,
        "stage": record.stage,
        "rollback_status": record.rollback_status,
        "projection_schema_hash": record.projection_schema_hash,
        "source_content_hash": record.source_content_hash,
        "d1_content_hash": record.d1_content_hash,
    }


def _publication_delay_quality(record: ProductRecord) -> dict[str, object]:
    """Measure source-freshness to D1 publication only when both instants are absolute."""
    delay_minutes: int | None = None
    try:
        published_at = datetime.fromisoformat(record.published_at.replace("Z", "+00:00"))
        freshness = datetime.fromisoformat(str(record.freshness).replace("Z", "+00:00"))
        if published_at.tzinfo is not None and freshness.tzinfo is not None:
            delay_minutes = max(
                0,
                int(
                    (published_at.astimezone(timezone.utc) - freshness.astimezone(timezone.utc)).total_seconds()
                    // 60
                ),
            )
    except (TypeError, ValueError):
        pass
    return {
        "value": delay_minutes,
        "unit": "minute",
        "quality_state": "observed" if delay_minutes is not None else "unknown",
        "null_meaning": None if delay_minutes is not None else "source_freshness_unavailable",
    }


def record_publication_events(
    context: dict[str, object],
    domain: str,
    records: Sequence[ProductRecord],
) -> None:
    """Emit the D1 transition for every product, including retained and failed records."""
    status_by_serving_status = {
        "published": "success",
        "degraded": "degraded",
        "skipped_retained": "skipped",
    }
    for record in records:
        has_publication_measurement = record.serving_status in status_by_serving_status
        row_count = (
            record.published_row_count if has_publication_measurement else None
        )
        record_product_event(
            context,
            domain=domain,
            layer="d1",
            product_ids=(record.product_id,),
            status=status_by_serving_status.get(record.serving_status, "failed"),
            row_count=row_count,
            rows_source=(
                "publication_ledger" if row_count is not None else "not_observed"
            ),
            publication_id=record.publication_id,
            quality={"publication_delay": _publication_delay_quality(record)},
        )


def _manifest_path(domain: str, dbt_project: str | None) -> str:
    project = dbt_project or _DBT_PROJECT.get(domain, domain)
    return f"/opt/airflow/dbt/domains/{project}/target/manifest.json"


def retire_domain_catalog_entries(
    d1,
    *,
    manifest_path: str,
    domain: str,
) -> tuple[str, ...]:
    """Retire only DBT-declared public catalog entries after a successful publish."""

    from common.serving.contract import load_domain_retirement_product_ids

    product_ids = load_domain_retirement_product_ids(manifest_path, domain)
    if product_ids:
        d1.delete_catalog_product_ids(product_ids)
    return product_ids


def publish_then_retire_catalog(
    *,
    publish_fn: Callable[..., object],
    contracts: Sequence[object],
    source: object,
    d1: object,
    smoke: object,
    source_run_id: str,
    verify_content_parity: bool,
    manifest_path: str,
    domain: str,
) -> tuple[object, tuple[str, ...]]:
    """Publish first, then retire only DBT-declared catalog entries.

    A publication failure exits before retirement.  A retirement write failure
    intentionally propagates so Airflow retries the task rather than claiming a
    partial catalog transition succeeded.
    """

    report = publish_fn(
        contracts,
        source,
        d1,
        smoke,
        source_run_id=source_run_id,
        verify_content_parity=verify_content_parity,
    )
    retired_catalog_product_ids = retire_domain_catalog_entries(
        d1,
        manifest_path=manifest_path,
        domain=domain,
    )
    return report, retired_catalog_product_ids


def resolve_publication_product_ids(
    context: Mapping[str, object],
    configured_product_ids: Sequence[str],
    *,
    metadata_key: str | None,
) -> tuple[str, ...]:
    """Resolve a validated subset from the latest triggering terminal Asset."""

    configured = tuple(configured_product_ids)
    if metadata_key is None:
        return configured

    triggering = context.get("triggering_asset_events")
    if not isinstance(triggering, Mapping) or not triggering:
        raise RuntimeError("serving publication scope requires a triggering Asset event")

    events: list[object] = []
    for values in triggering.values():
        if isinstance(values, Sequence) and not isinstance(
            values, (str, bytes, bytearray)
        ):
            events.extend(values)
        else:
            events.append(values)
    if not events:
        raise RuntimeError("serving publication scope requires a triggering Asset event")

    metadata = getattr(events[-1], "extra", None)
    if not isinstance(metadata, Mapping):
        raise RuntimeError("serving publication scope metadata is unavailable")
    raw_scope = metadata.get(metadata_key)
    if not isinstance(raw_scope, Sequence) or isinstance(
        raw_scope, (str, bytes, bytearray)
    ):
        raise RuntimeError("serving publication scope must be a product ID list")

    selected = tuple(raw_scope)
    if any(
        not isinstance(product_id, str) or not product_id.strip()
        for product_id in selected
    ):
        raise RuntimeError("serving publication scope contains an invalid product ID")
    if len(set(selected)) != len(selected):
        raise RuntimeError("serving publication scope contains duplicate product IDs")
    unexpected = sorted(set(selected) - set(configured))
    if unexpected:
        raise RuntimeError(
            "serving publication scope contains unknown product IDs: "
            + ",".join(unexpected)
        )
    return selected


def _load_export_contracts(
    manifest_path: str,
    domain: str,
    product_ids: Sequence[str],
    *,
    exact_domain_contracts: bool,
    require_public_projection: bool = False,
    partitioned_domain_scope: bool = False,
):
    from common.serving.contract import load_contracts, load_domain_contracts

    if exact_domain_contracts:
        return load_domain_contracts(
            manifest_path,
            domain,
            product_ids,
            require_public_projection=require_public_projection,
            allow_partitioned_scope=partitioned_domain_scope,
        )
    return load_contracts(
        manifest_path,
        product_ids,
        require_public_projection=require_public_projection,
    )


def validate_watchdog_target(
    target: str,
    *,
    env: Mapping[str, str] | None = None,
) -> str:
    """Fail before a D1 read if this watchdog's declared target is stale."""
    from common.runtime_guard import resolve_runtime_target

    declared_target = str(target).strip().lower()
    if declared_target not in {"dev", "prod"}:
        raise RuntimeError("serving watchdog target must be dev or prod")
    runtime_target = resolve_runtime_target(env)
    if declared_target != runtime_target:
        raise RuntimeError(
            "serving watchdog target disagrees with environment: "
            f"declared={declared_target!r} runtime={runtime_target!r}"
        )
    return runtime_target


def build_serving_freshness_watchdog_dag(
    domain: str,
    product_ids: Sequence[str],
    *,
    schedule: str,
    dag_id: str | None = None,
    dbt_project: str | None = None,
    target: str = "dev",
    exact_domain_contracts: bool = False,
    naive_freshness_timezones: Mapping[str, str] | None = None,
    publication_grace_minutes: int = 0,
    failure_callback: Callable[[dict[str, object]], None] | None = None,
):
    """Build an independent, read-only D1 freshness watchdog DAG.

    The watchdog intentionally has no Publisher, Trino, or write dependency.  It
    must still observe a stale D1 record when the export DAG itself is not running.
    """
    import pendulum
    from airflow import DAG
    from airflow.providers.standard.operators.python import PythonOperator

    from common.serving.runtime import build_d1_client_from_env
    from common.serving.watchdog import evaluate_watchdog, raise_for_watchdog_failures

    kst = pendulum.timezone("Asia/Seoul")
    expected_product_ids = tuple(product_ids)
    timezone_by_product = dict(naive_freshness_timezones or {})
    if (
        not isinstance(publication_grace_minutes, int)
        or isinstance(publication_grace_minutes, bool)
        or publication_grace_minutes < 0
    ):
        raise ValueError("publication_grace_minutes must be a non-negative integer")

    def _run(**context) -> None:
        runtime_target = validate_watchdog_target(target)
        contracts = _load_export_contracts(
            _manifest_path(domain, dbt_project),
            domain,
            expected_product_ids,
            exact_domain_contracts=exact_domain_contracts,
        )
        loaded_ids = {contract.product_id for contract in contracts}
        missing_ids = sorted(set(expected_product_ids) - loaded_ids)
        if missing_ids:
            raise RuntimeError(
                f"{domain}: freshness watchdog contract missing product_ids={','.join(missing_ids)}"
            )
        report = evaluate_watchdog(
            build_d1_client_from_env(),
            contracts,
            checked_at=datetime.now(timezone.utc),
            naive_freshness_timezones=timezone_by_product,
            publication_grace_minutes=publication_grace_minutes,
        )
        payload = {"domain": domain, "target": runtime_target, **report.payload()}
        print("[serving-watchdog] " + json.dumps(payload, ensure_ascii=False, sort_keys=True))
        context["ti"].xcom_push(key="serving_freshness_watchdog", value=payload)
        raise_for_watchdog_failures(report)

    with DAG(
        dag_id=dag_id or f"{domain}_serving_freshness_watchdog",
        description=f"{domain} D1 serving freshness를 독립 감시하는 Serving Contract v1 watchdog.",
        start_date=pendulum.datetime(2026, 1, 1, tz=kst),
        schedule=schedule,
        catchup=False,
        max_active_runs=1,
        default_args={
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "execution_timeout": timedelta(minutes=5),
        },
        params={"target": target},
        tags=["serving", domain, "d1", "watchdog"],
    ) as dag:
        PythonOperator(
            task_id="check_serving_freshness",
            python_callable=_run,
            on_failure_callback=failure_callback,
        )

    return dag


def build_serving_export_dag(
    domain: str,
    product_ids: Sequence[str],
    *,
    schedule: str | None = None,
    dag_id: str | None = None,
    dbt_project: str | None = None,
    target: str = "dev",
    schema: str | None = None,
    exact_domain_contracts: bool = False,
    require_public_projection: bool = False,
    verify_content_parity: bool = False,
    publication_scope_metadata_key: str | None = None,
    partitioned_domain_scope: bool = False,
):
    """Build a serving-export DAG for one domain. Returns an Airflow ``DAG``."""
    import os

    import pendulum
    from airflow import DAG
    from airflow.providers.standard.operators.python import PythonOperator

    from common.serving.publisher import publish
    from common.serving.runtime import (
        build_d1_client_from_env,
        build_smoke_tester_from_env,
        build_trino_source_reader,
    )

    kst = pendulum.timezone("Asia/Seoul")
    resolved_schema = schema or os.environ.get(f"SERVING_{domain.upper()}_SCHEMA", domain)

    def _run(**context) -> None:
        run_id = str(context.get("run_id") or context.get("ts") or "manual")
        manifest_path = _manifest_path(domain, dbt_project)
        contracts = _load_export_contracts(
            manifest_path,
            domain,
            product_ids,
            exact_domain_contracts=exact_domain_contracts,
            require_public_projection=require_public_projection,
            partitioned_domain_scope=partitioned_domain_scope,
        )
        if not contracts:
            raise RuntimeError(f"{domain}: product_ids {list(product_ids)} 에 해당하는 enabled 계약이 없다")
        publication_product_ids = resolve_publication_product_ids(
            context,
            product_ids,
            metadata_key=publication_scope_metadata_key,
        )
        if not publication_product_ids:
            print(f"[serving:{domain}] published=0 skipped=0 of 0 products (empty terminal scope)")
            return
        selected_ids = set(publication_product_ids)
        contracts = [
            contract for contract in contracts if contract.product_id in selected_ids
        ]
        if not contracts:
            raise RuntimeError(
                f"{domain}: 게시 신호의 product_ids {list(publication_product_ids)} 에 해당하는 계약이 없다"
            )
        source = build_trino_source_reader(context["params"].get("target", target), resolved_schema)
        d1 = build_d1_client_from_env()
        smoke = build_smoke_tester_from_env()

        try:
            report, retired_catalog_product_ids = publish_then_retire_catalog(
                publish_fn=publish,
                contracts=contracts,
                source=source,
                d1=d1,
                smoke=smoke,
                source_run_id=run_id,
                verify_content_parity=verify_content_parity,
                manifest_path=manifest_path,
                domain=domain,
            )
        except PublicationError as exc:
            record_publication_events(context, domain, exc.report.records)
            raise
        record_publication_events(context, domain, report.records)
        published = sum(1 for r in report.records if r.serving_status in {"published", "degraded"})
        skipped = sum(1 for r in report.records if r.serving_status == "skipped_retained")
        print(
            f"[serving:{domain}] published={published} skipped={skipped} "
            f"retired_catalog={len(retired_catalog_product_ids)} of {len(report.records)} products"
        )
        context["ti"].xcom_push(
            key="serving_publication",
            value={
                "domain": domain,
                "published": published,
                "skipped": skipped,
                "retired_catalog_product_ids": list(retired_catalog_product_ids),
                "records": [
                    publication_record_payload(r)
                    for r in report.records
                ],
            },
        )

    with DAG(
        dag_id=dag_id or f"{domain}_serving_export",
        description=f"{domain} Gold → Cloudflare D1 공통 Serving Contract v1 Publication.",
        start_date=pendulum.datetime(2026, 1, 1, tz=kst),
        schedule=schedule,
        catchup=False,
        max_active_runs=1,
        default_args={
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "execution_timeout": timedelta(minutes=30),
        },
        params={"target": target},
        tags=["serving", domain, "d1", "gold"],
    ) as dag:
        PythonOperator(
            task_id="publish_to_d1",
            python_callable=_run,
            pool=SERVING_D1_PUBLISH_POOL,
        )

    return dag
