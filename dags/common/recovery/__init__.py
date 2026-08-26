"""Recovery planning and fenced execution-boundary primitives.

The planner is side-effect free.  The lease registry is an explicit boundary
for a future executor and requires an atomic compare-and-swap backend; no
Airflow trigger or data write is performed by importing this package.
"""

from common.recovery.lease import (
    ACTIVE_STATES,
    LEASE_STATES,
    TERMINAL_STATES,
    InMemoryLeaseBackend,
    LeaseBackend,
    LeaseDecision,
    LeaseLostError,
    LeaseResult,
    RecoveryLease,
    RecoveryLeaseError,
    RecoveryLeaseRegistry,
)
from common.recovery.postgres import (
    CREATE_LEASE_TABLE_SQL,
    LEASE_TABLE,
    PostgresLeaseBackend,
    build_airflow_postgres_lease_backend,
)
from common.recovery.dispatch import (
    DispatchDecision,
    DispatchRequest,
    RawReplayEvidence,
    RecoveryDispatchError,
    compile_dispatch_requests,
)
from common.recovery.admission import (
    ACTIVE_RUN_STATES,
    AdmissionDecision,
    AdmissionPolicy,
    AdmissionResult,
    ActiveRunSnapshot,
    KMA_API_POOL,
    PoolSnapshot,
    RecoveryAdmissionError,
    TRINO_WEATHER_HEAVY_POOL,
    admit_dispatch_requests,
)
from common.recovery.airflow_snapshot import (
    AirflowSnapshotError,
    read_airflow_recovery_snapshot,
    snapshot_from_metadata,
)
from common.recovery.planner import (
    RecoveryAction,
    RecoveryCandidate,
    RecoveryJob,
    RecoveryPlan,
    RecoveryPolicy,
    plan_recovery,
)

__all__ = [
    "ACTIVE_STATES",
    "ACTIVE_RUN_STATES",
    "AirflowSnapshotError",
    "AdmissionDecision",
    "AdmissionPolicy",
    "AdmissionResult",
    "ActiveRunSnapshot",
    "DispatchDecision",
    "DispatchRequest",
    "InMemoryLeaseBackend",
    "KMA_API_POOL",
    "LEASE_STATES",
    "LeaseBackend",
    "LeaseDecision",
    "LeaseLostError",
    "LeaseResult",
    "RecoveryAction",
    "RecoveryCandidate",
    "RecoveryJob",
    "RecoveryPlan",
    "RecoveryPolicy",
    "RecoveryLease",
    "RecoveryLeaseError",
    "RecoveryLeaseRegistry",
    "RecoveryDispatchError",
    "RawReplayEvidence",
    "TERMINAL_STATES",
    "CREATE_LEASE_TABLE_SQL",
    "LEASE_TABLE",
    "PostgresLeaseBackend",
    "build_airflow_postgres_lease_backend",
    "PoolSnapshot",
    "RecoveryAdmissionError",
    "TRINO_WEATHER_HEAVY_POOL",
    "admit_dispatch_requests",
    "read_airflow_recovery_snapshot",
    "snapshot_from_metadata",
    "compile_dispatch_requests",
    "plan_recovery",
]
