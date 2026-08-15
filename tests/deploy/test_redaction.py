import pytest

from deployment.redaction import SensitiveArtifactError, reject_sensitive_artifact


@pytest.mark.parametrize(
    "value",
    [
        {"path": "C:/Users/person/project"},
        {"host": "http://192.168.0.10:8080"},
        {"env": ".env.production"},
        {"api_key": "a" * 32},
        {1: "not-a-string-key"},
        {"C:/Users/person/project": "absolute-path-key"},
        {"host": "https://user:password@example.invalid"},
        {"host": "http://127.0.0.1:8080"},
        {"host": "http://[::1]:8080"},
        {"host": "http://169.254.1.1:8080"},
        {"host": "http://[fe80::1]:8080"},
    ],
)
def test_release_artifact_rejects_sensitive_values(value):
    with pytest.raises(SensitiveArtifactError):
        reject_sensitive_artifact(value)


def test_release_artifact_allows_relative_public_values():
    reject_sensitive_artifact({"schema_version": "weather-local-deploy-target/v1"})


@pytest.mark.parametrize(
    "value",
    [
        {"source": "file:///C:/private/value"},
        {"source": "file:///etc/private/value"},
        {"value": "ask_example"},
        {"value": "ghp_example"},
        {"value": "github_pat_example"},
        {"value": "sk-example"},
        {"value": "xoxb-example"},
        {"value": "Bearer example"},
        {"value": "eyJ.example.example"},
        {"value": "-----BEGIN " + "PRIVATE KEY-----"},
    ],
)
def test_release_artifact_rejects_file_uris_and_credential_shaped_values(value):
    with pytest.raises(SensitiveArtifactError):
        reject_sensitive_artifact(value)


def test_release_artifact_preserves_lowercase_commit_and_fingerprint_values():
    reject_sensitive_artifact({"commit": "a" * 40, "fingerprint": "b" * 64})
