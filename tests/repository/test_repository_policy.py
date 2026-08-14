from __future__ import annotations

from pathlib import Path

from tools.repository_policy import find_forbidden_paths, find_secret_candidates


def test_forbidden_paths_cover_local_harness_and_generated_outputs() -> None:
    paths = [
        ".env",
        ".omx/state.json",
        "dbt/domains/traffic_weather/target/manifest.json",
        "dbt/domains/traffic_weather/dbt_packages/asac_axes/dbt_project.yml",
        "LessonRun.md",
        "dags/domains/weather/weather_vilage_fcst_bronze.py",
    ]

    forbidden = find_forbidden_paths(paths)

    assert forbidden == paths[:-1]


def test_secret_scanner_redacts_but_does_not_return_secret_value(tmp_path: Path) -> None:
    secret_file = tmp_path / "config.txt"
    secret_file.write_text("MARKETPLACE_API_KEY=ask_" + "a" * 32 + "\n", encoding="utf-8")

    findings = find_secret_candidates(tmp_path, ["config.txt"])

    assert len(findings) == 1
    assert findings[0].path == "config.txt"
    assert findings[0].rule == "marketplace_api_key"
    assert "ask_" not in findings[0].summary


def test_secret_scanner_allows_environment_variable_placeholders(tmp_path: Path) -> None:
    profile = tmp_path / "profiles.example.yml"
    profile.write_text(
        "token: '{{ env_var(\"CLOUDFLARE_D1_TOKEN\") }}'\n",
        encoding="utf-8",
    )

    assert find_secret_candidates(tmp_path, ["profiles.example.yml"]) == []


def test_secret_scanner_reports_non_utf8_candidate(tmp_path: Path) -> None:
    candidate = tmp_path / "binary.dat"
    candidate.write_bytes(b"\xff\xfe")

    findings = find_secret_candidates(tmp_path, ["binary.dat"])

    assert len(findings) == 1
    assert findings[0].path == "binary.dat"
    assert findings[0].line == 0
    assert findings[0].rule == "invalid_utf8"
    assert findings[0].summary == "candidate file is not valid UTF-8"


def test_secret_scanner_reports_read_error_without_exception_details(
    tmp_path: Path,
    monkeypatch,
) -> None:
    candidate = tmp_path / "unreadable.txt"
    candidate.write_text("safe placeholder\n", encoding="utf-8")
    original_read_text = Path.read_text

    def raise_for_candidate(path: Path, *args, **kwargs) -> str:
        if path == candidate.resolve():
            raise OSError("secret-material-must-not-leak")
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", raise_for_candidate)

    findings = find_secret_candidates(tmp_path, ["unreadable.txt"])

    assert len(findings) == 1
    assert findings[0].path == "unreadable.txt"
    assert findings[0].line == 0
    assert findings[0].rule == "file_read_error"
    assert findings[0].summary == "candidate file could not be read"
    assert "secret-material" not in repr(findings[0])
