from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from tools.repository_policy import repository_candidate_paths
from tools.verify_provenance import MANIFEST_SELF_PATH, sha256_file


OWNER = "masondev1024/seoul-weather-platform"
AUTHORIZED_LICENSE_STATUS = "public_republication_authorized"
PUBLIC_REPUBLICATION_REASON = (
    "Public republication authorized by approved team-code republication "
    "decision dated 2026-08-21; source lineage and validators retained."
)
HANDOFF_INVENTORY_PATH = "provenance/weather-mac-handoff.sha256"
AIRFLOW_HANDOFF_SOURCE_REPO = "ASAC-DE-bigkk/ASAC-DAG"
AIRFLOW_HANDOFF_SOURCE_COMMIT = "73ff5665ffd5526c59de8be2969cf65dffaf468b"
HANDOFF_PRIOR_AIRFLOW_PATHS = frozenset(
    {
        "dags/common/ops/run_sink.py",
        "dags/common/raw_write.py",
        "dags/common/runmetrics.py",
        "dags/domains/weather/tests/test_weather_landing_runtime.py",
        "dags/domains/weather/weather_ingest/bronze_batch.py",
        "dags/domains/weather/weather_ingest/landing.py",
        "dags/domains/weather/weather_ingest/runtime.py",
        "dags/domains/weather/weather_vilage_fcst_bronze.py",
    }
)
LOCAL_RUNTIME_VALIDATOR = (
    "python -m pytest tests/deploy/test_local_runtime_contract.py -q"
)
RETIRED_HANDOFF_PATHS = frozenset(
    {
        "README-MAC.md",
        "docker-compose.mac.yml",
        "docker-compose.prod.yml",
        "marquez.prod.yml",
    }
)
HANDOFF_OVERLAY_VALIDATORS = {
    ".dockerignore": "python -m tools.repository_policy --repo-root .",
    "Dockerfile.airflow": LOCAL_RUNTIME_VALIDATOR,
    "dags/common/ops/run_sink.py": "python -m pytest dags/common/tests -q",
    "dags/common/ops/telemetry_switch.py": "python -m pytest dags/common/tests -q",
    "dags/common/raw_write.py": "python -m pytest dags/common/tests -q",
    "dags/common/runmetrics.py": "python -m pytest dags/common/tests -q",
    "dags/common/tests/conftest.py": "python -m pytest dags/common/tests -q",
    "dags/common/tests/test_ops_telemetry_switch.py": "python -m pytest dags/common/tests -q",
    "dags/domains/weather/tests/test_weather_landing_runtime.py": "python -m pytest dags/domains/weather/tests -q",
    "dags/domains/weather/tests/test_weather_raw_transfer_budget.py": "python -m pytest dags/domains/weather/tests -q",
    "dags/domains/weather/weather_ingest/bronze_batch.py": "python -m pytest dags/domains/weather/tests -q",
    "dags/domains/weather/weather_ingest/landing.py": "python -m pytest dags/domains/weather/tests -q",
    "dags/domains/weather/weather_ingest/raw_spool.py": "python -m pytest dags/domains/weather/tests -q",
    "dags/domains/weather/weather_ingest/runtime.py": "python -m pytest dags/domains/weather/tests -q",
    "dags/domains/weather/weather_vilage_fcst_bronze.py": "python -m pytest dags/domains/weather/tests -q",
    "docker-compose.yml": LOCAL_RUNTIME_VALIDATOR,
    "scripts/safe-trigger-dag.sh": "bash -n scripts/safe-trigger-dag.sh",
    "scripts/safe_trigger_dag.py": "python -m compileall -q scripts/safe_trigger_dag.py",
    "trino/catalog-prod/iceberg.properties": LOCAL_RUNTIME_VALIDATOR,
    "trino/config.properties": LOCAL_RUNTIME_VALIDATOR,
    "trino/iceberg.properties": LOCAL_RUNTIME_VALIDATOR,
    "trino/jvm.config": LOCAL_RUNTIME_VALIDATOR,
    "trino/resource-groups.json": LOCAL_RUNTIME_VALIDATOR,
    "trino/resource-groups.properties": LOCAL_RUNTIME_VALIDATOR,
}
MAC_CUTOVER_ADAPTATION_VALIDATORS = {
    "dags/common/assets.py": (
        "PYTHONPATH=dags python -m pytest "
        "dags/domains/weather/tests/test_weather_kma_observation_dag.py -q"
    ),
    "dags/common/pools.py": (
        "PYTHONPATH=dags python -m pytest dags/common/tests/test_pools.py "
        "dags/domains/weather/tests/test_weather_kma_coordination.py -q"
    ),
    "dags/common/tests/test_pools.py": (
        "PYTHONPATH=dags python -m pytest dags/common/tests/test_pools.py -q"
    ),
    "dags/domains/weather/tests/test_weather_runtime_http.py": (
        "PYTHONPATH=dags python -m pytest "
        "dags/domains/weather/tests/test_weather_runtime_http.py "
        "dags/domains/weather/tests/test_weather_kma_attempt_ledger.py -q"
    ),
    "dags/domains/weather/weather_ingest/common/runtime.py": (
        "PYTHONPATH=dags python -m pytest "
        "dags/domains/weather/tests/test_weather_runtime_http.py "
        "dags/domains/weather/tests/test_weather_kma_attempt_ledger.py -q"
    ),
    "dags/domains/weather/tests/test_weather_serving_snapshot_refresh.py": (
        "PYTHONPATH=dags python -m pytest "
        "dags/domains/weather/tests/test_weather_serving_snapshot_refresh.py -q"
    ),
    "dags/domains/weather/tests/test_weather_transform_dag.py": (
        "PYTHONPATH=dags python -m pytest "
        "dags/domains/weather/tests/test_weather_transform_dag.py -q"
    ),
    "dags/domains/weather/tests/test_weather_transform_execution.py": (
        "PYTHONPATH=dags python -m pytest "
        "dags/domains/weather/tests/test_weather_transform_execution.py -q"
    ),
    "dags/domains/weather/weather_dbt_runtime.py": (
        "PYTHONPATH=dags python -m pytest "
        "dags/domains/weather/tests/test_weather_transform_dag.py -q"
    ),
    "dags/domains/weather/weather_serving_snapshot_refresh.py": (
        "PYTHONPATH=dags python -m pytest "
        "dags/domains/weather/tests/test_weather_serving_snapshot_refresh.py -q"
    ),
    "dags/domains/weather/weather_vilage_fcst_transform.py": (
        "PYTHONPATH=dags python -m pytest "
        "dags/domains/weather/tests/test_weather_transform_dag.py -q"
    ),
    "dbt/domains/traffic_weather/models/weather/transform/silver/silver_kma_vilage_fcst.sql": (
        "python -m pytest "
        "dbt/domains/traffic_weather/tests/weather/test_incremental_materialization_contract.py -q"
    ),
    "dbt/domains/traffic_weather/models/weather/transform/place_mart/"
    "silver_weather_forecast_by_admin_dong_serving.sql": (
        "python -m pytest "
        "dbt/domains/traffic_weather/tests/weather/"
        "test_weather_serving_working_set_contract.py -q"
    ),
    "dbt/domains/traffic_weather/tests/weather/test_incremental_materialization_contract.py": (
        "python -m pytest "
        "dbt/domains/traffic_weather/tests/weather/test_incremental_materialization_contract.py -q"
    ),
    "dbt/domains/traffic_weather/tests/weather/"
    "test_weather_serving_working_set_contract.py": (
        "python -m pytest "
        "dbt/domains/traffic_weather/tests/weather/"
        "test_weather_serving_working_set_contract.py -q"
    ),
    "dbt/domains/traffic_weather/models/weather/transform/gold/"
    "gold_weather_place_current_outlook.yml": (
        "python -m pytest "
        "tests/contracts/test_current_outlook_availability_contract.py -q"
    ),
}
MAC_MANUAL_SCHEDULE_ADAPTATIONS = frozenset(
    {
        "dags/domains/weather/tests/test_weather_serving_snapshot_refresh.py",
        "dags/domains/weather/weather_serving_snapshot_refresh.py",
    }
)
MAC_MEMORY_ADAPTATIONS = frozenset(
    {
        "dbt/domains/traffic_weather/models/weather/transform/place_mart/"
        "silver_weather_forecast_by_admin_dong_serving.sql",
        "dbt/domains/traffic_weather/tests/weather/"
        "test_weather_serving_working_set_contract.py",
    }
)
MAC_AVAILABILITY_ADAPTATIONS = frozenset(
    {
        "dbt/domains/traffic_weather/models/weather/transform/gold/"
        "gold_weather_place_current_outlook.yml",
    }
)
KMA_OBSERVATION_ADAPTATIONS = frozenset(
    {
        "dags/common/assets.py",
        "dags/common/pools.py",
        "dags/common/tests/test_pools.py",
        "dags/domains/weather/tests/test_weather_runtime_http.py",
        "dags/domains/weather/weather_ingest/common/runtime.py",
    }
)
HOST_TEST_PORTABILITY_ADAPTATIONS = frozenset(
    {"dags/domains/weather/tests/test_weather_transform_execution.py"}
)
LOCAL_DBT_SOURCES = frozenset(
    {
        "dbt/domains/traffic_weather/macros/weather/weather_quality_contract.sql",
        "dbt/domains/traffic_weather/dbt_project.yml",
        "dbt/domains/traffic_weather/profiles.yml",
        "dbt/domains/traffic_weather/selectors.yml",
        "dbt/domains/traffic_weather/models/groups.yml",
        "dbt/domains/traffic_weather/models/weather/sources.yml",
        "dbt/domains/traffic_weather/models/weather/quality/gold/_quality_gold.yml",
        "dbt/domains/traffic_weather/models/weather/quality/gold/gold_weather_forecast_quality_grid_score.sql",
        "dbt/domains/traffic_weather/models/weather/quality/gold/gold_weather_forecast_quality_grid_score_history.sql",
        "dbt/domains/traffic_weather/models/weather/quality/silver/_quality_silver.yml",
        "dbt/domains/traffic_weather/models/weather/quality/silver/silver_kma_observation_truth.sql",
        "dbt/domains/traffic_weather/models/weather/quality/silver/silver_weather_forecast_observation_match.sql",
        "dbt/domains/traffic_weather/models/weather/quality/silver/silver_weather_quality_forecast_vintage.sql",
        "dbt/domains/traffic_weather/seeds/weather/_weather_inputs.yml",
        "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_forecast_vintage_unique.sql",
        "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_grid_score_reconciles.sql",
        "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_match_unique.sql",
        "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_observation_truth_complete_hours.sql",
        "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_observation_truth_unique.sql",
        "dbt/domains/traffic_weather/tests/weather/test_weather_quality_model_contract.py",
        "dbt/domains/traffic_weather/models/weather/transform/place_mart/_place_mart.yml",
        "dbt/domains/traffic_weather/models/weather/transform/gold/_serving_gold.yml",
        "dbt/domains/traffic_weather/tests/weather/transform/place_mart/"
        "assert_silver_weather_admin_dong_serving_issue_window.sql",
    }
)
#: 이 저장소가 직접 작성한 Airflow 소스. `dags/` 는 기본적으로 고정 스냅샷 전제라
#: 자동 분류를 막지만, platform-boundaries.md 대로 Weather DAG 코드는 이 저장소가
#: 소유하므로 새로 쓴 파일이 생긴다. 상류에서 가져온 코드가 조용히 local_authored 로
#: 흘러들지 않도록 **파일 단위로 명시**해서만 허용한다(LOCAL_DBT_SOURCES 와 같은 방식).
#: 이 저장소에서 새로 작성한 정적 참조 refresh DAG. transform 에서 분리한 정적
#: seed/차원 phase 를 하루 1회 실행한다(상류에는 없던 신규 DAG). 병합된 dev(#48)가
#: exclusion 파일을 제거했으므로 그 항목은 넣지 않는다.
LOCAL_AIRFLOW_SOURCES = frozenset(
    {
        # This repository's internal-only forecast-quality runtime. Paths are
        # intentionally explicit: a new Airflow source cannot become public
        # provenance without a reviewed allowlist addition and test update.
        "dags/domains/weather/weather_quality_runtime.py",
        "dags/domains/weather/tests/test_weather_quality_runtime.py",
        # This repository's hourly observation implementation. Every new DAG,
        # runtime module, and focused contract test is explicit so inherited
        # Airflow code can never be silently reclassified as local authorship.
        "dags/domains/weather/weather_ingest/kma_coordination.py",
        "dags/domains/weather/weather_ingest/kma_observation.py",
        "dags/domains/weather/weather_ingest/kma_observation_bronze.py",
        "dags/domains/weather/weather_ingest/kma_observation_http.py",
        "dags/domains/weather/weather_ingest/kma_observation_landing.py",
        "dags/domains/weather/weather_ingest/kma_observation_runtime.py",
        "dags/domains/weather/weather_ultra_srt_ncst_bronze.py",
        "dags/domains/weather/tests/test_weather_kma_attempt_ledger.py",
        "dags/domains/weather/tests/test_weather_kma_coordination.py",
        "dags/domains/weather/tests/test_weather_kma_deadline.py",
        "dags/domains/weather/tests/test_weather_kma_observation.py",
        "dags/domains/weather/tests/test_weather_kma_observation_bronze.py",
        "dags/domains/weather/tests/test_weather_kma_observation_dag.py",
        "dags/domains/weather/tests/test_weather_kma_observation_landing.py",
        "dags/domains/weather/tests/test_weather_kma_retry_policy.py",
        "dags/domains/weather/weather_reference_data_refresh.py",
        "dags/domains/weather/tests/test_weather_reference_data_refresh_dag.py",
        # 이 fork 전용 Iceberg 유지보수 DAG. 상류 dev-단일-스키마 모듈을 그대로
        # 옮기지 않고 우리 2-스키마(weather/weather_traffic_bronze) 소유 테이블에
        # 맞춰 새로 작성했다.
        "dags/domains/weather/weather_iceberg_maintenance.py",
        "dags/domains/weather/weather_ingest/iceberg_maintenance.py",
        "dags/domains/weather/tests/test_weather_iceberg_maintenance.py",
        "dags/domains/weather/tests/test_weather_iceberg_maintenance_dag.py",
        # 소비자 없는 ops 관측 기록기를 잠그는 이 fork 전용 스위치와 그 경계 테스트.
        # 상류에는 없다 — 상류에는 ops/ 를 읽는 소비자(D1 적재 DAG)가 살아 있기 때문이다.
        # conftest 는 dags root 를 sys.path 에 넣는다(그전에는 수집 순서에 의존해
        # 우연히 import 가 성립했다).
        "dags/common/ops/telemetry_switch.py",
        "dags/common/tests/conftest.py",
        "dags/common/tests/test_ops_telemetry_switch.py",
        # 개인 노트북의 동일 Weather run 안에서 landing payload를 Bronze로 넘겨
        # 불필요한 R2 본문 재다운로드를 제거하는 repository-owned spool과 예산 테스트.
        "dags/domains/weather/weather_ingest/raw_spool.py",
        "dags/domains/weather/tests/test_weather_raw_transfer_budget.py",
    }
)


def _normalized(path: str) -> str:
    return Path(path).as_posix()


def _with_public_authorization(record: dict[str, Any]) -> dict[str, Any]:
    updated = dict(record)
    updated["license_status"] = AUTHORIZED_LICENSE_STATUS
    prior_reason = str(record.get("reason") or "No prior provenance reason recorded.")
    if prior_reason.startswith(PUBLIC_REPUBLICATION_REASON):
        updated["reason"] = prior_reason
        return updated
    updated["reason"] = f"{PUBLIC_REPUBLICATION_REASON} Prior provenance reason: {prior_reason}"
    return updated


def build_repository_record(target_path: str, checksum: str) -> dict[str, Any]:
    target = _normalized(target_path)
    if target.startswith("contracts/weather-risk/"):
        return _with_public_authorization(
            {
            "record_type": "derived",
            "target_path": target,
            "target_sha256": checksum,
            "scope": "weather_risk_contract_fixture",
            "reason": "Clean-room reduced Weather Risk origin or hosted-proxy contract evidence.",
            "license_status": "internal_private_snapshot_only",
            "derived_from": [
                "ASAC-DE-bigkk/ASK-Seoul-Serving@efe393e7a925d5798867424993daf0dbe5d55902",
                "NomaDamas/k-skill@43edf3c0f1037a4e510b21de61e26965212b6620",
            ],
            "derivation": "Route, response, error, cursor, and query-context semantics were independently reduced into contract-only JSON.",
            "validator": "tests/contracts/test_weather_risk_contract.py",
            }
        )
    if target.startswith("dags/") and target not in LOCAL_AIRFLOW_SOURCES:
        raise ValueError(f"Airflow source requires fixed snapshot provenance: {target}")
    if target.startswith("dbt/") and target not in LOCAL_DBT_SOURCES:
        raise ValueError(f"dbt source requires fixed snapshot provenance: {target}")
    return _with_public_authorization(
        {
        "record_type": "local_authored",
        "target_path": target,
        "target_sha256": checksum,
        "scope": "repository_owned",
        "reason": "Repository-owned implementation, test, or documentation.",
        "license_status": "repository_owned_private",
        "owner": OWNER,
        }
    )


def build_handoff_overlay_record(
    target_path: str,
    *,
    target_checksum: str,
    source_checksum: str,
) -> dict[str, Any]:
    target = _normalized(target_path)
    try:
        validator = HANDOFF_OVERLAY_VALIDATORS[target]
    except KeyError as error:
        raise ValueError(f"unreviewed handoff overlay path: {target}") from error
    handoff_source = {
        "inventory": HANDOFF_INVENTORY_PATH,
        "source_path": target,
        "source_sha256": source_checksum,
    }
    derived_from: dict[str, Any]
    if target in HANDOFF_PRIOR_AIRFLOW_PATHS:
        derived_from = {
            "source_repo": AIRFLOW_HANDOFF_SOURCE_REPO,
            "source_commit": AIRFLOW_HANDOFF_SOURCE_COMMIT,
            "source_path": target.removeprefix("dags/"),
            "handoff": handoff_source,
        }
    else:
        derived_from = handoff_source
    return _with_public_authorization(
        {
        "record_type": "derived",
        "target_path": target,
        "target_sha256": target_checksum,
        "scope": "weather_mac_handoff_overlay",
        "reason": "Reviewed operational handoff input for the personal Weather Mac runtime.",
        "license_status": "internal_private_snapshot_only",
        "derived_from": derived_from,
        "derivation": "Overlaid from the non-secret handoff inventory, then retained or adapted under executable repository validators.",
        "validator": validator,
        }
    )


def build_mac_cutover_adaptation_record(
    target_path: str,
    *,
    target_checksum: str,
    source_record: dict[str, Any],
) -> dict[str, Any]:
    target = _normalized(target_path)
    try:
        validator = MAC_CUTOVER_ADAPTATION_VALIDATORS[target]
    except KeyError as error:
        raise ValueError(f"unreviewed Mac cutover adaptation: {target}") from error
    if _normalized(str(source_record.get("target_path", ""))) != target:
        raise ValueError(f"Mac cutover source record mismatch: {target}")
    if source_record.get("record_type") not in {"snapshot_copy", "derived"}:
        raise ValueError(f"Mac cutover source record is not fixed or derived: {target}")
    previous_checksum = source_record.get("target_sha256")
    if not isinstance(previous_checksum, str) or len(previous_checksum) != 64:
        raise ValueError(f"Mac cutover source checksum is invalid: {target}")

    if target in KMA_OBSERVATION_ADAPTATIONS:
        scope = "kma_observation_coordination"
        reason = (
            "Shared KMA observation and forecast safety adaptation for the "
            "personal Weather runtime."
        )
        derivation = (
            "Add the observation asset and one-slot pools, and reserve every "
            "forecast physical retry in the same durable daily attempt ledger."
        )
    elif target in HOST_TEST_PORTABILITY_ADAPTATIONS:
        scope = "weather_host_test_portability"
        reason = "Weather dbt test adaptation for repository-host path isolation."
        derivation = (
            "Give the empty-selection regression an attempt-local temporary dbt "
            "project so host tests never write the container-only /opt path."
        )
    elif target in MAC_MANUAL_SCHEDULE_ADAPTATIONS:
        scope = "weather_mac_manual_schedule_control"
        reason = "Personal Mac Weather runtime adaptation for explicit serving refresh triggers."
        derivation = (
            "Allow the hourly serving refresh cron to be disabled through a "
            "Mac-only environment override while retaining the upstream default."
        )
    elif target in MAC_MEMORY_ADAPTATIONS:
        scope = "weather_mac_memory_optimization"
        reason = "Personal Mac Weather runtime adaptation for bounded Trino memory."
        derivation = (
            "Replace the target-hashing MERGE with atomic table rename and bound "
            "the issue horizon and ranks to the verified serving requirement."
        )
    elif target in MAC_AVAILABILITY_ADAPTATIONS:
        scope = "weather_serving_availability"
        reason = "Personal Weather serving adaptation for hour-boundary availability."
        derivation = (
            "Allow only the previous forecast hour during a bounded 30-minute "
            "refresh grace window while preserving fail-closed older-data behavior."
        )
    else:
        scope = "weather_mac_egress_optimization"
        reason = "Personal Mac Weather runtime adaptation for partition-pruned R2 reads."
        derivation = (
            "Propagate the verified Bronze load_date into dbt and enforce the "
            "Iceberg partition predicate without changing the snapshot identity."
        )

    if source_record.get("scope") == scope:
        previous = source_record.get("derived_from")
        if not isinstance(previous, dict):
            raise ValueError(f"Mac cutover derivation chain is invalid: {target}")
    else:
        source_fields = (
            "source_repo",
            "source_commit",
            "source_ref",
            "source_path",
            "source_blob_oid",
            "source_content_sha256",
        )
        upstream = source_record.get("derived_from")
        if upstream is None:
            upstream = {
                field: source_record[field]
                for field in source_fields
                if field in source_record
            }
        previous = {
            "record_type": source_record["record_type"],
            "target_path": target,
            "target_sha256": previous_checksum,
            "upstream": upstream,
        }
        if source_record.get("derivation"):
            previous["derivation"] = source_record["derivation"]

    return _with_public_authorization(
        {
        "record_type": "derived",
        "target_path": target,
        "target_sha256": target_checksum,
        "scope": scope,
        "reason": reason,
        "license_status": source_record.get(
            "license_status", "internal_private_snapshot_only"
        ),
        "derived_from": previous,
        "derivation": derivation,
        "validator": validator,
        }
    )


def _handoff_inventory_checksums(path: Path) -> dict[str, str]:
    checksums: dict[str, str] = {}
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        checksum, separator, target_path = raw_line.partition("  ")
        target = _normalized(target_path)
        if (
            not separator
            or len(checksum) != 64
            or any(character not in "0123456789abcdef" for character in checksum)
            or not target
            or target in checksums
        ):
            raise ValueError(f"invalid handoff inventory record at line {line_number}")
        checksums[target] = checksum
    return checksums


def _read_records(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def preserved_source_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        _with_public_authorization(record)
        for record in records
        if record.get("record_type") in {"snapshot_copy", "derived", "generated"}
    ]


def rendered_manifest(repo_root: Path, manifest_path: Path) -> bytes:
    root = repo_root.resolve()
    existing = _read_records(manifest_path)
    existing_by_target = {
        _normalized(str(record.get("target_path", ""))): record
        for record in existing
        if isinstance(record.get("target_path"), str)
    }
    records = [
        record
        for record in preserved_source_records(existing)
        if _normalized(str(record.get("target_path", "")))
        not in HANDOFF_OVERLAY_VALIDATORS
        and _normalized(str(record.get("target_path", "")))
        not in RETIRED_HANDOFF_PATHS
        and _normalized(str(record.get("target_path", "")))
        not in MAC_CUTOVER_ADAPTATION_VALIDATORS
    ]
    handoff_checksums = _handoff_inventory_checksums(
        root / HANDOFF_INVENTORY_PATH
    )
    recorded_targets = {
        _normalized(record["target_path"])
        for record in records
        if isinstance(record.get("target_path"), str)
    }
    # A checked-in manifest must be reproducible in a clean CI checkout. The
    # repository policy still scans untracked files for secrets, but personal
    # scratch files must never become dangling provenance records.
    for relative_path in sorted(
        repository_candidate_paths(root, include_untracked=False)
    ):
        target = _normalized(relative_path)
        if target == MANIFEST_SELF_PATH or target in recorded_targets:
            continue
        absolute = root / Path(target)
        target_checksum = sha256_file(absolute)
        if target in HANDOFF_OVERLAY_VALIDATORS:
            source_checksum = handoff_checksums.get(target)
            if source_checksum is None:
                raise ValueError(f"handoff inventory is missing reviewed path: {target}")
            records.append(
                build_handoff_overlay_record(
                    target,
                    target_checksum=target_checksum,
                    source_checksum=source_checksum,
                )
            )
            continue
        if target in MAC_CUTOVER_ADAPTATION_VALIDATORS:
            source_record = existing_by_target.get(target)
            if source_record is None:
                raise ValueError(
                    f"Mac cutover adaptation is missing source provenance: {target}"
                )
            records.append(
                build_mac_cutover_adaptation_record(
                    target,
                    target_checksum=target_checksum,
                    source_record=source_record,
                )
            )
            continue
        records.append(build_repository_record(target, target_checksum))
    records.sort(key=lambda record: _normalized(str(record["target_path"])))
    text = "".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n" for record in records
    )
    return text.encode("utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Refresh repository provenance checksums.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--manifest", type=Path, default=Path("provenance/source-files.jsonl")
    )
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    manifest = args.manifest
    if not manifest.is_absolute():
        manifest = args.repo_root / manifest
    payload = rendered_manifest(args.repo_root, manifest)
    if args.check:
        if manifest.read_bytes() != payload:
            print("ERROR: provenance manifest is not current")
            return 1
        print("Provenance manifest is current.")
        return 0
    manifest.write_bytes(payload)
    print(f"Refreshed provenance manifest with {len(payload.splitlines())} records.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
