from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import textwrap

import pytest
import yaml


PROJECT_DIR = Path(__file__).resolve().parents[2]
SOURCES_PATH = PROJECT_DIR / "models" / "weather" / "sources.yml"
SELECTORS_PATH = PROJECT_DIR / "selectors.yml"
PROFILES_PATH = PROJECT_DIR / "profiles.yml"
MACRO_PATH = PROJECT_DIR / "macros" / "weather" / "weather_quality_contract.sql"
FORECAST_VINTAGE = (
    PROJECT_DIR
    / "models"
    / "weather"
    / "quality"
    / "silver"
    / "silver_weather_quality_forecast_vintage.sql"
)
OBSERVATION_TRUTH = (
    PROJECT_DIR
    / "models"
    / "weather"
    / "quality"
    / "silver"
    / "silver_kma_observation_truth.sql"
)


def _resolve_dbt_executable(
    preferred: Path = Path("/Users/mason/Projects/seoul-weather-platform/.venv/bin/dbt"),
    which=shutil.which,
) -> Path | None:
    if preferred.exists():
        return preferred
    discovered = which("dbt")
    return Path(discovered) if discovered else None


DBT_EXECUTABLE = _resolve_dbt_executable()

VALID_QUALITY_VARS = {
    "weather_quality_run_id": "scheduled__quality",
    "weather_quality_evaluation_as_of": "2026-08-21T18:05:00+00:00",
    "weather_quality_window_start_date": "2026-08-15",
    "weather_quality_window_end_date": "2026-08-21",
    "weather_quality_forecast_load_start_date": "2026-08-11",
    "weather_quality_forecast_load_end_date": "2026-08-20",
    "weather_quality_truth_policy_version": "observation-truth-policy/v2-internal",
    "weather_quality_vintage_policy_version": "forecast-vintage-cutoff/v1",
    "weather_quality_evidence_policy_version": "metric-evidence-gate/v1",
    "weather_quality_pop_policy_version": "pop-threshold-0.5/v1",
}


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def source_table_names(sources_doc: dict) -> set[str]:
    return {
        table["name"]
        for source in sources_doc["sources"]
        for table in source.get("tables", [])
    }


def _source_by_name(sources_doc: dict, name: str) -> dict:
    matches = [source for source in sources_doc["sources"] if source["name"] == name]
    assert len(matches) == 1
    return matches[0]


def selector_block(selectors_doc: dict, name: str) -> dict:
    matches = [
        selector
        for selector in selectors_doc["selectors"]
        if selector["name"] == name
    ]
    assert len(matches) == 1
    return matches[0]["definition"]


def _selector_values(definition: dict) -> set[str]:
    if "union" in definition:
        return {
            value
            for child in definition["union"]
            for value in _selector_values(child)
        }
    value = definition.get("value")
    return {value} if value else set()


def _write_contract_project(project_dir: Path, probe_macro_sql: str) -> None:
    macros_dir = project_dir / "macros" / "weather"
    macros_dir.mkdir(parents=True)
    if MACRO_PATH.exists():
        shutil.copy2(MACRO_PATH, macros_dir / MACRO_PATH.name)
    (macros_dir / "contract_probe.sql").write_text(probe_macro_sql, encoding="utf-8")
    (project_dir / "dbt_project.yml").write_text(
        textwrap.dedent(
            """\
            name: weather_quality_contract_probe
            version: "1.0.0"
            config-version: 2
            profile: weather_quality_contract_probe
            macro-paths: ["macros"]
            """
        ),
        encoding="utf-8",
    )
    (project_dir / "profiles.yml").write_text(
        textwrap.dedent(
            """\
            weather_quality_contract_probe:
              target: ci
              outputs:
                ci:
                  type: trino
                  method: none
                  user: contract-test
                  host: 127.0.0.1
                  port: 8080
                  database: iceberg_dev
                  schema: weather_contract_test
                  http_scheme: http
            """
        ),
        encoding="utf-8",
    )


def _dbt_operation_probe(tmp_path: Path, probe_macro_sql: str, vars: dict[str, str]):
    if not DBT_EXECUTABLE:
        pytest.skip("dbt executable is not installed in this runtime")
    _write_contract_project(tmp_path, probe_macro_sql)
    env = os.environ.copy()
    env["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    result = subprocess.run(
        [
            str(DBT_EXECUTABLE),
            "run-operation",
            "probe_quality_contract",
            "--project-dir",
            str(tmp_path),
            "--profiles-dir",
            str(tmp_path),
            "--target-path",
            str(tmp_path / "target"),
            "--log-path",
            str(tmp_path / "logs"),
            "--no-partial-parse",
            "--no-use-colors",
            "--vars",
            json.dumps(vars),
        ],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    return result


def test_quality_sources_and_selectors_are_internal():
    sources = _load_yaml(SOURCES_PATH)
    selectors = _load_yaml(SELECTORS_PATH)

    names = source_table_names(sources)
    assert {
        "kma_vilage_fcst",
        "kma_ultra_srt_ncst",
        "quality_publication_manifest",
    } <= names

    quality_source = _source_by_name(sources, "weather_quality_control")
    assert quality_source["schema"] == "{{ env_var('WEATHER_SCHEMA', 'weather') }}"
    assert quality_source["tables"] == [
        {
            "name": "quality_publication_manifest",
            "identifier": "weather_forecast_quality_publication_manifest",
        }
    ]

    candidate = selector_block(selectors, "ask_seoul_weather_quality_candidate")
    assert "ask_seoul_weather_d1_public_products" not in str(candidate)
    assert "ask_seoul_weather_serving_snapshot_refresh" not in str(candidate)
    assert all(
        "quality" in value
        for value in _selector_values(candidate)
        if isinstance(value, str)
    )

    published = selector_block(selectors, "ask_seoul_weather_quality_published")
    assert "ask_seoul_weather_d1_public_products" not in str(published)
    assert "ask_seoul_weather_serving_snapshot_refresh" not in str(published)
    assert all(
        "quality" in value
        for value in _selector_values(published)
        if isinstance(value, str)
    )


def test_quality_profiles_apply_bounded_session_limit_to_every_target():
    profiles = _load_yaml(PROFILES_PATH)
    outputs = profiles["asac_seoul"]["outputs"]

    assert set(outputs) == {"prod", "dev", "ci"}
    for output in outputs.values():
        assert output["session_properties"] == {
            "query_max_run_time": "{{ env_var('TRINO_DBT_QUERY_MAX_RUN_TIME', '2h') }}"
        }


def test_quality_runtime_macros_parse_with_complete_valid_contract(tmp_path: Path):
    result = _dbt_operation_probe(
        tmp_path,
        textwrap.dedent(
            """\
            {% macro probe_quality_contract() %}
              {% do weather_quality_validate_runtime_contract() %}
              {% set fields = [
                weather_quality_run_id(),
                weather_quality_evaluation_as_of(),
                weather_quality_window_start_date(),
                weather_quality_window_end_date(),
                weather_quality_forecast_load_start_date(),
                weather_quality_forecast_load_end_date(),
                weather_quality_truth_policy_version(),
                weather_quality_vintage_policy_version(),
                weather_quality_evidence_policy_version(),
                weather_quality_pop_policy_version(),
                weather_quality_evidence_state('30', '30', 'false')
              ] %}
              {{ return(fields | join('|')) }}
            {% endmacro %}
            """,
        ),
        VALID_QUALITY_VARS,
    )

    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    ("var_name", "bad_value", "message"),
    [
        ("weather_quality_run_id", "../unsafe", "safe run ID"),
        ("weather_quality_evaluation_as_of", "2026-08-21 18:05:00", "ISO timestamp"),
        ("weather_quality_window_start_date", "2026-8-15", "ISO date"),
        (
            "weather_quality_truth_policy_version",
            "observation-truth-policy/v1",
            "must be observation-truth-policy/v2-internal",
        ),
    ],
)
def test_quality_runtime_macros_fail_closed_on_bad_vars(
    tmp_path: Path, var_name: str, bad_value: str, message: str
):
    vars = VALID_QUALITY_VARS | {var_name: bad_value}

    result = _dbt_operation_probe(
        tmp_path,
        textwrap.dedent(
            """\
            {% macro probe_quality_contract() %}
              {% do weather_quality_validate_runtime_contract() %}
              {{ return('ok') }}
            {% endmacro %}
            """
        ),
        vars,
    )

    assert result.returncode != 0
    assert message in result.stdout + result.stderr


def test_quality_runtime_macros_reject_inverted_windows(tmp_path: Path):
    vars = VALID_QUALITY_VARS | {
        "weather_quality_window_start_date": "2026-08-22",
        "weather_quality_window_end_date": "2026-08-21",
    }

    result = _dbt_operation_probe(
        tmp_path,
        textwrap.dedent(
            """\
            {% macro probe_quality_contract() %}
              {% do weather_quality_validate_runtime_contract() %}
              {{ return('ok') }}
            {% endmacro %}
            """
        ),
        vars,
    )

    assert result.returncode != 0
    assert "weather_quality_window_start_date must be <= weather_quality_window_end_date" in (
        result.stdout + result.stderr
    )


@pytest.mark.parametrize(
    ("window_end", "message"),
    [
        ("2026-08-16", "weather_quality_window_date span must be exactly 1 or 7 KST dates"),
        ("2026-08-20", "weather_quality_window_date span must be exactly 1 or 7 KST dates"),
        ("2026-08-22", "weather_quality_window_date span must be exactly 1 or 7 KST dates"),
    ],
)
def test_quality_runtime_macros_reject_unsupported_window_lengths(
    tmp_path: Path, window_end: str, message: str
):
    vars = VALID_QUALITY_VARS | {"weather_quality_window_end_date": window_end}

    result = _dbt_operation_probe(
        tmp_path,
        textwrap.dedent(
            """\
            {% macro probe_quality_contract() %}
              {% do weather_quality_validate_runtime_contract() %}
              {{ return('ok') }}
            {% endmacro %}
            """
        ),
        vars,
    )

    assert result.returncode != 0
    assert message in result.stdout + result.stderr


def test_quality_runtime_macros_accept_one_day_backfill_window(tmp_path: Path):
    vars = VALID_QUALITY_VARS | {
        "weather_quality_run_id": "manual__quality_backfill",
        "weather_quality_evaluation_as_of": "2026-08-21T00:00:00+00:00",
        "weather_quality_window_start_date": "2026-08-20",
        "weather_quality_window_end_date": "2026-08-20",
    }

    result = _dbt_operation_probe(
        tmp_path,
        textwrap.dedent(
            """\
            {% macro probe_quality_contract() %}
              {% do weather_quality_validate_runtime_contract() %}
              {{ return(weather_quality_window_start_date() ~ '|' ~ weather_quality_window_end_date()) }}
            {% endmacro %}
            """
        ),
        vars,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_quality_runtime_macros_reject_window_end_on_evaluation_kst_date(tmp_path: Path):
    vars = VALID_QUALITY_VARS | {
        "weather_quality_window_start_date": "2026-08-15",
        "weather_quality_window_end_date": "2026-08-21",
        "weather_quality_evaluation_as_of": "2026-08-20T15:00:00+00:00",
    }

    result = _dbt_operation_probe(
        tmp_path,
        textwrap.dedent(
            """\
            {% macro probe_quality_contract() %}
              {% do weather_quality_validate_runtime_contract() %}
              {{ return('ok') }}
            {% endmacro %}
            """
        ),
        vars,
    )

    assert result.returncode != 0
    assert "weather_quality_window_end_date must be before the evaluation KST date" in (
        result.stdout + result.stderr
    )


def test_quality_runtime_macros_require_explicit_window_dates(tmp_path: Path):
    vars = {
        key: value
        for key, value in VALID_QUALITY_VARS.items()
        if key != "weather_quality_window_start_date"
    }

    result = _dbt_operation_probe(
        tmp_path,
        textwrap.dedent(
            """\
            {% macro probe_quality_contract() %}
              {% do weather_quality_validate_runtime_contract() %}
              {{ return('ok') }}
            {% endmacro %}
            """
        ),
        vars,
    )

    assert result.returncode != 0
    assert "weather_quality_window_start_date must be an ISO date" in (
        result.stdout + result.stderr
    )


def test_missing_dbt_executable_is_represented_as_none():
    assert _resolve_dbt_executable(
        Path("/definitely/missing/dbt"),
        which=lambda _name: None,
    ) is None


def test_quality_forecast_reads_partitioned_bronze_not_serving_silver():
    sql = FORECAST_VINTAGE.read_text(encoding="utf-8")

    assert "source('weather_bronze', 'kma_vilage_fcst')" in sql
    assert "source('weather_bronze', 'collection_run_manifest')" in sql
    assert "ref('silver_kma_vilage_fcst')" not in sql
    assert "load_date >=" in sql and "load_date <=" in sql
    assert "ARRAY['day(valid_at)']" in sql


def test_quality_forecast_validates_full_runtime_contract_before_date_accessors():
    sql = FORECAST_VINTAGE.read_text(encoding="utf-8")

    validation_pos = sql.index("{% do weather_quality_validate_runtime_contract() %}")
    start_accessor_pos = sql.index("weather_quality_forecast_load_start_date()")
    end_accessor_pos = sql.index("weather_quality_forecast_load_end_date()")

    assert validation_pos < start_accessor_pos
    assert validation_pos < end_accessor_pos


def test_observation_truth_is_self_gated_and_bounded_without_manifest_join():
    sql = OBSERVATION_TRUTH.read_text(encoding="utf-8")

    assert "source('weather_bronze', 'kma_ultra_srt_ncst')" in sql
    assert "source('weather_bronze', 'collection_run_manifest')" not in sql
    assert "cast(bronze.observed_at as timestamp(6)) >=" in sql
    assert "cast(bronze.observed_at as timestamp(6)) <" in sql
    assert "in ('T1H', 'RN1', 'UUU', 'VVV', 'REH', 'PTY', 'VEC', 'WSD')" in sql
    assert "group by observed_at, dag_run_id, manifest_key" in sql
    assert "row_count = 640" in sql
    assert "canonical_grid_count = 80" in sql
    assert "category_count = 8" in sql
    assert "grid_category_count = 640" in sql
    assert "source_revision_count = 640" in sql
    assert "collected_at <= cast({{ evaluation_as_of }} as timestamp(6))" in sql
    assert "coalesce(rn1" not in sql.lower()
    assert "'provisional'" in sql
    assert "'invalid_truth'" in sql
    assert "ARRAY['day(observed_at)']" in sql


def test_observation_truth_counts_raw_scope_before_filtering_malformed_rows():
    sql = OBSERVATION_TRUTH.read_text(encoding="utf-8")

    raw_scope_pos = sql.index("raw_bronze_scope as")
    run_counts_pos = sql.index("run_counts as")
    eligible_pos = sql.index("eligible_required_rows as")

    assert raw_scope_pos < run_counts_pos < eligible_pos
    assert "from raw_bronze_scope" in sql
    assert "count(*) as row_count" in sql
    assert "count_if(source_id != 'kma_ultra_srt_ncst' or source_id is null) as wrong_source_row_count" in sql
    assert "count_if(grid_id is null) as noncanonical_grid_row_count" in sql
    assert "count_if(category not in ('T1H', 'RN1', 'UUU', 'VVV', 'REH', 'PTY', 'VEC', 'WSD') or category is null) as invalid_category_row_count" in sql
    assert "row_count = 640" in sql
    assert "wrong_source_row_count = 0" in sql
    assert "noncanonical_grid_row_count = 0" in sql
    assert "invalid_category_row_count = 0" in sql
    assert "duplicate_grid_category_row_count = 0" in sql
    assert "null_scope_identity_row_count = 0" in sql


def test_observation_completeness_reports_runtime_window_not_existing_truth_hours():
    test_sql = (
        PROJECT_DIR
        / "tests"
        / "weather"
        / "quality"
        / "assert_quality_observation_truth_complete_hours.sql"
    ).read_text(encoding="utf-8")

    assert "config(" in test_sql
    assert "severity='warn'" in test_sql
    assert "store_failures=true" in test_sql
    assert "sequence(0, 167)" in test_sql
    assert "weather_quality_window_start_date()" in test_sql
    assert "weather_quality_window_end_date()" in test_sql
    assert "weather_quality_evaluation_as_of()" in test_sql
    assert "select distinct observed_at" not in test_sql
    assert "current_timestamp" not in test_sql
    assert "current_date" not in test_sql


def test_observation_truth_validates_full_runtime_contract_before_bounds():
    sql = OBSERVATION_TRUTH.read_text(encoding="utf-8")

    validation_pos = sql.index("{% do weather_quality_validate_runtime_contract() %}")
    window_start_pos = sql.index("weather_quality_window_start_date()")
    window_end_pos = sql.index("weather_quality_window_end_date()")
    evaluation_pos = sql.index("weather_quality_evaluation_as_of()")

    assert validation_pos < window_start_pos
    assert validation_pos < window_end_pos
    assert validation_pos < evaluation_pos


def test_quality_model_config_matches_quality_model_directory():
    project = _load_yaml(PROFILES_PATH.parent / "dbt_project.yml")
    weather_models = project["models"]["asac_seoul"]["weather"]

    assert "quality" in weather_models
    assert "quality" not in weather_models["transform"]
    assert weather_models["quality"] == {
        "+tags": ["ask_seoul_weather_quality_candidate"]
    }
