from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from tools.repository_policy import repository_candidate_paths
from tools.verify_provenance import MANIFEST_SELF_PATH, sha256_file


OWNER = "masondev1024/seoul-weather-platform"
LOCAL_DBT_CONFIGS = frozenset(
    {
        "dbt/domains/traffic_weather/dbt_project.yml",
        "dbt/domains/traffic_weather/profiles.yml",
        "dbt/domains/traffic_weather/selectors.yml",
        "dbt/domains/traffic_weather/models/groups.yml",
        "dbt/domains/traffic_weather/models/weather/sources.yml",
        "dbt/domains/traffic_weather/seeds/weather/_weather_inputs.yml",
        "dbt/domains/traffic_weather/models/weather/transform/place_mart/_place_mart.yml",
        "dbt/domains/traffic_weather/models/weather/transform/gold/_serving_gold.yml",
    }
)
LOCAL_DBT_SOURCES = frozenset(
    {
        "dbt/domains/traffic_weather/macros/weather/weather_quality_contract.sql",
        "dbt/domains/traffic_weather/models/weather/quality/gold/_quality_gold.yml",
        "dbt/domains/traffic_weather/models/weather/quality/gold/gold_weather_forecast_quality_daily.sql",
        "dbt/domains/traffic_weather/models/weather/quality/gold/gold_weather_forecast_quality_daily_history.sql",
        "dbt/domains/traffic_weather/models/weather/quality/gold/gold_weather_forecast_quality_grid_score.sql",
        "dbt/domains/traffic_weather/models/weather/quality/gold/gold_weather_forecast_quality_grid_score_history.sql",
        "dbt/domains/traffic_weather/models/weather/quality/gold/gold_weather_forecast_quality_hourly.sql",
        "dbt/domains/traffic_weather/models/weather/quality/gold/gold_weather_forecast_quality_hourly_history.sql",
        "dbt/domains/traffic_weather/models/weather/quality/silver/_quality_silver.yml",
        "dbt/domains/traffic_weather/models/weather/quality/silver/silver_kma_observation_truth.sql",
        "dbt/domains/traffic_weather/models/weather/quality/silver/silver_weather_forecast_observation_match.sql",
        "dbt/domains/traffic_weather/models/weather/quality/silver/silver_weather_quality_forecast_vintage.sql",
        "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_daily_reconciles.sql",
        "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_forecast_vintage_unique.sql",
        "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_grid_score_reconciles.sql",
        "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_hourly_reconciles.sql",
        "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_match_unique.sql",
        "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_observation_truth_complete_hours.sql",
        "dbt/domains/traffic_weather/tests/weather/quality/assert_quality_observation_truth_unique.sql",
        "dbt/domains/traffic_weather/tests/weather/test_weather_quality_model_contract.py",
    }
)
#: 이 저장소가 직접 작성한 Airflow 소스. `dags/` 는 기본적으로 고정 스냅샷 전제라
#: 자동 분류를 막지만, platform-boundaries.md 대로 Weather DAG 코드는 이 저장소가
#: 소유하므로 새로 쓴 파일이 생긴다. 상류에서 가져온 코드가 조용히 local_authored 로
#: 흘러들지 않도록 **파일 단위로 명시**해서만 허용한다(LOCAL_DBT_CONFIGS 와 같은 방식).
#: 이 저장소에서 새로 작성한 정적 참조 refresh DAG. transform 에서 분리한 정적
#: seed/차원 phase 를 하루 1회 실행한다(상류에는 없던 신규 DAG). 병합된 dev(#48)가
#: exclusion 파일을 제거했으므로 그 항목은 넣지 않는다.
LOCAL_AIRFLOW_SOURCES = frozenset(
    {
        "dags/domains/weather/weather_reference_data_refresh.py",
        "dags/domains/weather/tests/test_weather_reference_data_refresh_dag.py",
        # 이 fork 전용 Iceberg 유지보수 DAG. 상류 dev-단일-스키마 모듈을 그대로
        # 옮기지 않고 우리 2-스키마(weather/weather_traffic_bronze) 소유 테이블에
        # 맞춰 새로 작성했다.
        "dags/domains/weather/weather_iceberg_maintenance.py",
        "dags/domains/weather/weather_ingest/iceberg_maintenance.py",
        "dags/domains/weather/tests/test_weather_iceberg_maintenance.py",
        "dags/domains/weather/tests/test_weather_iceberg_maintenance_dag.py",
        "dags/domains/weather/weather_forecast_quality_backfill.py",
        "dags/domains/weather/weather_forecast_quality_daily.py",
        "dags/domains/weather/weather_quality_publication.py",
        "dags/domains/weather/weather_quality_runtime.py",
        "dags/domains/weather/tests/test_weather_forecast_quality_dags.py",
        "dags/domains/weather/tests/test_weather_quality_publication.py",
        "dags/domains/weather/tests/test_weather_quality_runtime.py",
    }
)
DERIVED_AIRFLOW_OVERRIDES = frozenset(
    {
        "dags/common/assets.py",
        "dags/common/pools.py",
        "dags/common/tests/test_pools.py",
        "dags/domains/weather/tests/test_weather_transform_dag.py",
        "dags/domains/weather/weather_dbt_runtime.py",
    }
)
_SOURCE_LINEAGE_FIELDS = (
    "source_repo",
    "source_ref",
    "source_commit",
    "source_path",
    "source_blob_oid",
    "source_content_sha256",
)


def _normalized(path: str) -> str:
    return Path(path).as_posix()


def build_repository_record(target_path: str, checksum: str) -> dict[str, Any]:
    target = _normalized(target_path)
    if target.startswith("contracts/weather-risk/"):
        return {
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
    if target.startswith("dags/") and target not in LOCAL_AIRFLOW_SOURCES:
        raise ValueError(f"Airflow source requires fixed snapshot provenance: {target}")
    if target.startswith("dbt/") and target not in (
        LOCAL_DBT_CONFIGS | LOCAL_DBT_SOURCES
    ):
        raise ValueError(f"dbt source requires fixed snapshot provenance: {target}")
    return {
        "record_type": "local_authored",
        "target_path": target,
        "target_sha256": checksum,
        "scope": "repository_owned",
        "reason": "Repository-owned implementation, test, or documentation.",
        "license_status": "repository_owned_private",
        "owner": OWNER,
    }


def build_derived_override_record(
    target_path: str,
    checksum: str,
    *,
    source_record: dict[str, Any],
) -> dict[str, Any]:
    target = _normalized(target_path)
    if source_record.get("record_type") == "derived":
        derived_from = source_record.get("derived_from")
    else:
        derived_from = {
            field: source_record[field]
            for field in _SOURCE_LINEAGE_FIELDS
            if field in source_record
        }
    if not isinstance(derived_from, (dict, list)) or not derived_from:
        raise ValueError(f"missing source lineage for derived Airflow override: {target}")
    return {
        "record_type": "derived",
        "target_path": target,
        "target_sha256": checksum,
        "scope": "repository_owned",
        "reason": "Repository-owned Weather adaptation of a fixed upstream snapshot.",
        "license_status": "internal_private_snapshot_only",
        "derived_from": derived_from,
        "derivation": "Repository-owned Weather quality adaptation; see the reviewed git diff and focused tests.",
        "validator": "python -m pytest dags/common/tests dags/domains/weather/tests tests/forecast_quality -q",
    }


def _read_records(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def preserved_source_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        record
        for record in records
        if record.get("record_type") in {"snapshot_copy", "derived", "generated"}
    ]


def rendered_manifest(repo_root: Path, manifest_path: Path) -> bytes:
    root = repo_root.resolve()
    existing = _read_records(manifest_path)
    records: list[dict[str, Any]] = []
    for record in preserved_source_records(existing):
        target_path = record.get("target_path")
        if target_path not in DERIVED_AIRFLOW_OVERRIDES:
            records.append(record)
            continue
        if not isinstance(target_path, str):
            raise ValueError("derived Airflow override must have a target path")
        absolute = root / Path(target_path)
        records.append(
            build_derived_override_record(
                target_path,
                sha256_file(absolute),
                source_record=record,
            )
        )
    recorded_targets = {
        _normalized(record["target_path"])
        for record in records
        if isinstance(record.get("target_path"), str)
    }
    for relative_path in sorted(repository_candidate_paths(root)):
        target = _normalized(relative_path)
        if target == MANIFEST_SELF_PATH or target in recorded_targets:
            continue
        absolute = root / Path(target)
        records.append(build_repository_record(target, sha256_file(absolute)))
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
