"""Common D1 Publisher — config-driven Serving Contract v1 Publication.

Enforces ASAC-DAG docs/contracts/serving-contract-v1.md so that D1 write, ``_catalog``
registration, verification and API smoke are one Publication unit (the #477 fix).
Domain DAGs stay thin via ``dag_factory.build_serving_export_dag``.

Pure modules (contract/gate/publisher/d1_client) carry all the logic and are
unit-tested with in-memory fakes; ``runtime`` and ``dag_factory`` hold the
Trino/Cloudflare/Airflow wiring.
"""

from __future__ import annotations

from common.serving.contract import ServingContract, load_contracts
from common.serving.gate import GateDecision, GateResult, apply_reliability, evaluate_gate
from common.serving.publisher import (
    ProductRecord,
    PublicationError,
    PublicationReport,
    ReadPlan,
    publish,
)

__all__ = [
    "GateDecision",
    "GateResult",
    "ProductRecord",
    "PublicationError",
    "PublicationReport",
    "ReadPlan",
    "ServingContract",
    "apply_reliability",
    "evaluate_gate",
    "load_contracts",
    "publish",
]
