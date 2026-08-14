"""Weather source freshness SLO resolved-manifest contract."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import textwrap
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCES_PATH = PROJECT_ROOT / "models" / "weather" / "sources.yml"
WARN_ENV = "ASK_SEOUL_REPORT_WEATHER_FRESHNESS_WARN_MINUTES"
ERROR_ENV = "ASK_SEOUL_REPORT_WEATHER_FRESHNESS_ERROR_MINUTES"
DBT_EXECUTABLE = os.environ.get("DBT_EXECUTABLE") or shutil.which("dbt")
EXPECTED_TABLES = {"kma_vilage_fcst", "collection_run_manifest"}


def test_weather_freshness_declares_airflow_watchdog_environment_contract():
    source_text = SOURCES_PATH.read_text(encoding="utf-8")

    assert f"env_var('{WARN_ENV}', '240') | int" in source_text
    assert f"env_var('{ERROR_ENV}', '360') | int" in source_text


def _write_contract_project(project_dir: Path) -> None:
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)
    shutil.copy2(SOURCES_PATH, models_dir / "sources.yml")
    (project_dir / "dbt_project.yml").write_text(
        textwrap.dedent(
            """\
            name: weather_freshness_contract
            version: "1.0.0"
            config-version: 2
            profile: weather_freshness_contract
            model-paths: ["models"]
            """
        ),
        encoding="utf-8",
    )
    (project_dir / "profiles.yml").write_text(
        textwrap.dedent(
            """\
            weather_freshness_contract:
              target: dev
              outputs:
                dev:
                  type: trino
                  method: none
                  user: contract-test
                  host: localhost
                  port: 8080
                  database: iceberg_dev
                  schema: weather_contract_test
                  http_scheme: http
            """
        ),
        encoding="utf-8",
    )


def _resolved_weather_freshness(overrides: dict[str, str]) -> dict[str, dict]:
    if DBT_EXECUTABLE is None:
        raise unittest.SkipTest("dbt executable is not installed in this runtime")

    with tempfile.TemporaryDirectory(prefix="weather-freshness-contract-") as temp_dir:
        project_dir = Path(temp_dir)
        _write_contract_project(project_dir)
        env = os.environ.copy()
        env.pop(WARN_ENV, None)
        env.pop(ERROR_ENV, None)
        env.update(overrides)
        env["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
        target_dir = project_dir / "target"
        result = subprocess.run(
            [
                DBT_EXECUTABLE,
                "parse",
                "--project-dir",
                str(project_dir),
                "--profiles-dir",
                str(project_dir),
                "--target-path",
                str(target_dir),
                "--log-path",
                str(project_dir / "logs"),
                "--no-partial-parse",
                "--no-use-colors",
            ],
            cwd=project_dir,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise AssertionError(
                "dbt parse failed:\n" + result.stdout + "\n" + result.stderr
            )
        manifest = json.loads(
            (target_dir / "manifest.json").read_text(encoding="utf-8")
        )
        return {
            node["name"]: node["freshness"]
            for node in manifest["sources"].values()
            if node["source_name"] == "weather_bronze"
        }


class WeatherFreshnessSloContractTest(unittest.TestCase):
    def assert_freshness(
        self,
        resolved: dict[str, dict],
        *,
        warn_minutes: int,
        error_minutes: int,
    ) -> None:
        self.assertEqual(set(resolved), EXPECTED_TABLES)
        self.assertLess(warn_minutes, error_minutes)
        for freshness in resolved.values():
            self.assertEqual(
                freshness["warn_after"],
                {"count": warn_minutes, "period": "minute"},
            )
            self.assertEqual(
                freshness["error_after"],
                {"count": error_minutes, "period": "minute"},
            )

    def test_defaults_resolve_to_240_and_360_minutes(self) -> None:
        self.assert_freshness(
            _resolved_weather_freshness({}),
            warn_minutes=240,
            error_minutes=360,
        )

    def test_dag_environment_overrides_resolve_in_manifest(self) -> None:
        self.assert_freshness(
            _resolved_weather_freshness({WARN_ENV: "241", ERROR_ENV: "361"}),
            warn_minutes=241,
            error_minutes=361,
        )


if __name__ == "__main__":
    unittest.main()
