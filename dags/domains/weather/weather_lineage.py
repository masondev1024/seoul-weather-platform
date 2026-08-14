"""Weather-only OpenLineage selective enable adapter."""

from __future__ import annotations

import importlib
import os
from typing import TypeVar


T = TypeVar("T")
SELECTIVE_ENABLE_ENV = "AIRFLOW__OPENLINEAGE__SELECTIVE_ENABLE"
SELECTIVE_ENABLE_MODULE = "airflow.providers.openlineage.utils.selective_enable"


def enable_lineage_if_configured(dag: T) -> T:
    """Opt this DAG in only when the runtime explicitly enables the policy."""
    if (os.environ.get(SELECTIVE_ENABLE_ENV) or "").strip().lower() != "true":
        return dag
    try:
        provider = importlib.import_module(SELECTIVE_ENABLE_MODULE)
        enable_lineage = provider.enable_lineage
    except (ImportError, AttributeError) as exc:
        raise RuntimeError(
            "OpenLineage selective enable is configured, but the Airflow "
            "OpenLineage provider/API is unavailable"
        ) from exc
    return enable_lineage(dag)
