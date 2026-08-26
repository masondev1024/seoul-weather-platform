from __future__ import annotations

from pathlib import Path

from tools.repository_policy import find_secret_candidates


REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_ENV = REPO_ROOT / ".env.example"

REQUIRED_WEATHER_KEYS = frozenset(
    {
        "KMA_SERVICE_KEY",
        "R2_BUCKET_NAME",
        "R2_ENDPOINT",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_DATA_CATALOG_TOKEN",
        "R2_DATA_CATALOG_URI",
        "R2_DATA_CATALOG_WAREHOUSE",
        "SERVING_CLOUDFLARE_ACCOUNT_ID",
        "SERVING_D1_DATABASE_ID",
        "CLOUDFLARE_API_TOKEN",
        "SERVING_API_BASE_URL",
        "AIRFLOW_FERNET_KEY",
        "AIRFLOW_SECRET_KEY",
        "POSTGRES_PASSWORD",
        "TRINO_MEMORY_LIMIT",
        "TRINO_QUERY_MAX_MEMORY_PER_NODE",
        "TRINO_MEMORY_HEAP_HEADROOM_PER_NODE",
        "TRINO_QUERY_MAX_MEMORY",
        "TRINO_QUERY_MAX_TOTAL_MEMORY",
        "ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED",
        "ASK_SEOUL_KMA_OBSERVATION_DAG_SCHEDULE",
        "ASK_SEOUL_KMA_CONTROL_ROOT",
        "ASK_SEOUL_KMA_ATTEMPT_LEDGER_PATH",
        "ASK_SEOUL_KMA_DAILY_ATTEMPT_LIMIT",
    }
)
FORBIDDEN_LEGACY_KEYS = frozenset(
    {
        "KOPIS_SERVICE_KEY",
        "TRAFFIC_DISCORD_WEBHOOK_URL",
        "SEOUL_API_KEY_COMM",
        "SEOUL_API_KEY_CULT",
        "SEOUL_API_KEY_PPLT",
        "SEOUL_API_KEY_TRAN",
        "SEOUL_API_KEY_TRIC",
        "MARQUEZ_POSTGRES_PASSWORD",
        "test_key",
    }
)


def _parse_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name, separator, value = line.partition("=")
        assert separator == "="
        assert name and name not in values
        values[name] = value
    return values


def test_public_environment_example_is_secretless_and_weather_only() -> None:
    values = _parse_env(EXAMPLE_ENV)

    assert REQUIRED_WEATHER_KEYS <= values.keys()
    assert FORBIDDEN_LEGACY_KEYS.isdisjoint(values)
    assert find_secret_candidates(REPO_ROOT, [".env.example"]) == []
    assert "/Users/" not in EXAMPLE_ENV.read_text(encoding="utf-8")
    assert "C:\\Users\\" not in EXAMPLE_ENV.read_text(encoding="utf-8")


def test_public_environment_example_preserves_the_mac_memory_envelope() -> None:
    values = _parse_env(EXAMPLE_ENV)

    assert values["TRINO_MEMORY_LIMIT"] == "5g"
    assert values["TRINO_QUERY_MAX_MEMORY_PER_NODE"] == "800MB"
    assert values["TRINO_MEMORY_HEAP_HEADROOM_PER_NODE"] == "1500MB"
    assert values["TRINO_QUERY_MAX_MEMORY"] == "800MB"
    assert values["TRINO_QUERY_MAX_TOTAL_MEMORY"] == "1200MB"


def test_public_environment_example_keeps_hourly_observation_inert() -> None:
    values = _parse_env(EXAMPLE_ENV)

    assert values["ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED"] == "false"
    assert values["ASK_SEOUL_KMA_OBSERVATION_DAG_SCHEDULE"] == ""
    assert values["ASK_SEOUL_KMA_CONTROL_ROOT"] == (
        "/opt/airflow/logs/_weather_control"
    )
    assert values["ASK_SEOUL_KMA_ATTEMPT_LEDGER_PATH"] == (
        "/opt/airflow/logs/_weather_control/kma_api_budget.sqlite3"
    )
    assert values["ASK_SEOUL_KMA_DAILY_ATTEMPT_LIMIT"] == "7500"
