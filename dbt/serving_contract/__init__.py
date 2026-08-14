"""Common Serving Contract v1 validation harness.

Machine-readable schema (``schema.yml``) + validator + CLI facade that enforce
ASAC-DAG ``docs/contracts/serving-contract-v1.md`` across domains. Reuses the
``contracts/engine`` conventions: stable CLI facade, exit codes 0/1/2
(PASS/FAIL/ERROR), deterministic UTF-8 report bytes.
"""

from __future__ import annotations

from serving_contract.model import ServingModel, load_models_from_yaml
from serving_contract.validator import Finding, ValidationResult, validate

__all__ = [
    "Finding",
    "ServingModel",
    "ValidationResult",
    "load_models_from_yaml",
    "validate",
]
