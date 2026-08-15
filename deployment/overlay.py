from __future__ import annotations

import errno
import hashlib
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePath, PureWindowsPath
from typing import Any

import yaml

from deployment.target import DeployTarget


_SHA40 = re.compile(r"^[0-9a-f]{40}$")
_CREDENTIAL = re.compile(r"(token|secret|password|credential|authorization|bearer|api[_-]?key)", re.I)
_SERVICE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_DBT_ARTIFACT_ROOT_ENV = "ASK_SEOUL_DBT_ARTIFACT_ROOT"
_DBT_ARTIFACT_RELEASE_ROOT = "/opt/airflow/logs/weather-dbt/releases"


@dataclass(frozen=True)
class OverlayArtifact:
    kind: str
    candidate_sha: str | None
    content: bytes
    sha256: str


def _path_string(path: PurePath) -> str:
    return str(path).replace("\\", "/")


def _same_flavor_child(parent: PurePath, *parts: str) -> PurePath:
    return parent.joinpath(*parts)


def _validate_sha(candidate_sha: str) -> None:
    if not _SHA40.fullmatch(candidate_sha):
        raise ValueError("candidate_sha must be lowercase 40-hex")


def _validate_service_names(services: list[str]) -> None:
    for service in services:
        if "airflow-init" in service.casefold() or not _SERVICE_NAME.fullmatch(service):
            raise ValueError("overlay service name rejected")


def _artifact(kind: str, candidate_sha: str | None, services: list[str], dags: str, dbt: str) -> OverlayArtifact:
    _validate_service_names(services)
    payload = {"services": {}}
    for service in services:
        service_body = {
            "volumes": [
                {
                    "type": "bind",
                    "source": dags,
                    "target": "/opt/airflow/dags",
                    "read_only": True,
                },
                {
                    "type": "bind",
                    "source": dbt,
                    "target": "/opt/airflow/dbt",
                    "read_only": kind == "release",
                },
            ]
        }
        if kind == "release":
            service_body["environment"] = {
                _DBT_ARTIFACT_ROOT_ENV: f"{_DBT_ARTIFACT_RELEASE_ROOT}/{candidate_sha}"
            }
        payload["services"][service] = service_body
    content = yaml.safe_dump(
        payload,
        allow_unicode=False,
        default_flow_style=False,
        sort_keys=False,
        width=4096,
    ).encode("utf-8")
    return OverlayArtifact(kind, candidate_sha, content, hashlib.sha256(content).hexdigest())


def render_release_overlay(target: DeployTarget, checkout_root: PurePath, candidate_sha: str) -> OverlayArtifact:
    _validate_sha(candidate_sha)
    expected = _same_flavor_child(target.runtime_root, "releases", candidate_sha)
    if type(checkout_root) is not type(expected) or _path_string(checkout_root) != _path_string(expected):
        raise ValueError("checkout_root must be runtime_root/releases/<candidate_sha>")
    return _artifact(
        "release",
        candidate_sha,
        sorted(target.airflow_code_services),
        _path_string(checkout_root / "dags"),
        _path_string(checkout_root / "dbt"),
    )


def render_baseline_overlay(target: DeployTarget) -> OverlayArtifact:
    return _artifact(
        "baseline",
        None,
        sorted(target.airflow_code_services),
        _path_string(target.dags_host_path),
        _path_string(target.dbt_host_path),
    )


class _StrictLoader(yaml.SafeLoader):
    pass


def _construct_mapping(loader: _StrictLoader, node: yaml.MappingNode, deep: bool = False):
    seen = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in seen:
            raise ValueError("duplicate mapping key")
        seen.add(key)
    return yaml.SafeLoader.construct_mapping(loader, node, deep=deep)


_StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping)


def _walk_nodes(node: yaml.Node) -> None:
    if isinstance(node, yaml.AliasEvent):  # pragma: no cover - PyYAML compose returns nodes.
        raise ValueError("alias rejected")
    if node.tag not in {
        "tag:yaml.org,2002:map",
        "tag:yaml.org,2002:seq",
        "tag:yaml.org,2002:str",
        "tag:yaml.org,2002:bool",
    }:
        raise ValueError("non-standard yaml tag rejected")
    if isinstance(node, yaml.MappingNode):
        for key, value in node.value:
            if isinstance(value, yaml.nodes.Node) and getattr(value, "anchor", None):
                raise ValueError("alias rejected")
            _walk_nodes(key)
            _walk_nodes(value)
    elif isinstance(node, yaml.SequenceNode):
        for child in node.value:
            _walk_nodes(child)


def _parse_strict(content: bytes) -> Any:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("invalid overlay yaml") from None
    if "\r" in text or not text.endswith("\n") or "&" in text or "*" in text:
        raise ValueError("yaml must be utf-8 lf without alias")
    try:
        node = yaml.compose(text)
        if node is None:
            raise ValueError("empty overlay")
        _walk_nodes(node)
        return yaml.load(text, Loader=_StrictLoader)
    except (TypeError, yaml.YAMLError):
        raise ValueError("invalid overlay yaml") from None


def _absolute_clean(path: str) -> bool:
    if "\\" in path or re.match(r"^[A-Za-z]:/", path):
        normalized = PureWindowsPath(path)
        parts = re.split(r"[\\/]+", path)
        return normalized.is_absolute() and "." not in parts and ".." not in parts
    return path.startswith("/") and "/./" not in path and "/../" not in path and not path.endswith("/..")


def _overlay_pair_kind(target: DeployTarget, pair: tuple[str, str]) -> tuple[str, str | None] | None:
    baseline = (_path_string(target.dags_host_path), _path_string(target.dbt_host_path))
    if pair == baseline:
        return ("baseline", None)
    runtime_root = re.escape(_path_string(target.runtime_root))
    match = re.fullmatch(rf"{runtime_root}/releases/([0-9a-f]{{40}})/dags", pair[0])
    if match and pair[1] == f"{_path_string(target.runtime_root)}/releases/{match.group(1)}/dbt":
        return ("release", match.group(1))
    return None


def validate_overlay_content(target: DeployTarget, content: bytes, sha256: str) -> OverlayArtifact:
    if hashlib.sha256(content).hexdigest() != sha256:
        raise ValueError("overlay checksum mismatch")
    parsed = _parse_strict(content)
    if not isinstance(parsed, dict) or set(parsed) != {"services"} or not isinstance(parsed["services"], dict):
        raise ValueError("overlay must contain only services")
    services = parsed["services"]
    if set(services) != set(target.airflow_code_services):
        raise ValueError("overlay service allowlist mismatch")
    expected_pair: tuple[str, str] | None = None
    service_bodies: list[dict[str, Any]] = []
    for service in services:
        if (
            service in target.forbidden_data_services
            or _CREDENTIAL.search(service)
            or "airflow-init" in service.casefold()
            or not _SERVICE_NAME.fullmatch(service)
        ):
            raise ValueError("overlay service rejected")
        service_body = services[service]
        if (
            not isinstance(service_body, dict)
            or "volumes" not in service_body
            or set(service_body) - {"volumes", "environment"}
        ):
            raise ValueError("overlay service keys rejected")
        volumes = service_body["volumes"]
        if not isinstance(volumes, list) or len(volumes) != 2:
            raise ValueError("overlay volume count rejected")
        for volume, expected_target in zip(volumes, ("/opt/airflow/dags", "/opt/airflow/dbt")):
            if not isinstance(volume, dict) or set(volume) != {"type", "source", "target", "read_only"}:
                raise ValueError("overlay volume keys rejected")
            if volume["type"] != "bind" or volume["target"] != expected_target:
                raise ValueError("overlay volume contract rejected")
            source = volume["source"]
            if not isinstance(source, str) or not _absolute_clean(source) or _CREDENTIAL.search(source):
                raise ValueError("overlay source rejected")
        pair = (volumes[0]["source"], volumes[1]["source"])
        if expected_pair is None:
            expected_pair = pair
        elif pair != expected_pair:
            raise ValueError("overlay source pair must be shared by all services")
        service_bodies.append(service_body)
    if expected_pair is None:
        raise ValueError("overlay source pair missing")
    kind = _overlay_pair_kind(target, expected_pair)
    if kind is None:
        raise ValueError("overlay source pair rejected")
    expected_service_keys = (
        {"volumes", "environment"} if kind[0] == "release" else {"volumes"}
    )
    expected_read_only = (True, True) if kind[0] == "release" else (True, False)
    expected_environment = {
        _DBT_ARTIFACT_ROOT_ENV: f"{_DBT_ARTIFACT_RELEASE_ROOT}/{kind[1]}"
    }
    for service_body in service_bodies:
        if set(service_body) != expected_service_keys:
            raise ValueError("overlay service keys rejected")
        for volume, read_only in zip(service_body["volumes"], expected_read_only):
            if volume["read_only"] is not read_only:
                raise ValueError("overlay volume permission rejected")
        if kind[0] == "release":
            environment = service_body["environment"]
            if isinstance(environment, dict):
                for key, value in environment.items():
                    if (
                        isinstance(key, str)
                        and _CREDENTIAL.search(key)
                        or isinstance(value, str)
                        and _CREDENTIAL.search(value)
                    ):
                        raise ValueError("overlay environment credential rejected")
            if environment != expected_environment:
                raise ValueError("overlay environment rejected")
    return OverlayArtifact(kind[0], kind[1], content, sha256)


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError as error:
        if error.errno in {errno.EACCES, errno.EINVAL, errno.EPERM}:
            return
        raise
    try:
        os.fsync(fd)
    except OSError as error:
        if error.errno == errno.EINVAL:
            return
        raise
    finally:
        os.close(fd)


class AtomicOverlayStore:
    def __init__(self, target: DeployTarget):
        self.target = target
        self.path = Path(str(target.generated_overlay_file))
        self._staged: set[Path] = set()

    def _require_known_staged(self, staged: Path) -> Path:
        resolved = Path(staged)
        if resolved not in self._staged:
            raise ValueError("unknown staged overlay path")
        if resolved.parent != self.path.parent:
            raise ValueError("staged overlay path parent mismatch")
        if not resolved.name.startswith(f".{self.path.name}.") or not resolved.name.endswith(".tmp"):
            raise ValueError("staged overlay path shape mismatch")
        return resolved

    def stage(self, artifact: OverlayArtifact) -> Path:
        validate_overlay_content(self.target, artifact.content, artifact.sha256)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent)
        staged = Path(name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(artifact.content)
                handle.flush()
                os.fsync(handle.fileno())
        except Exception:
            staged.unlink(missing_ok=True)
            raise
        self._staged.add(staged)
        return staged

    def install(self, staged: Path, artifact: OverlayArtifact) -> None:
        staged = self._require_known_staged(staged)
        content = staged.read_bytes()
        if content != artifact.content:
            raise ValueError("staged overlay content mismatch")
        validate_overlay_content(self.target, content, artifact.sha256)
        os.replace(staged, self.path)
        self._staged.discard(staged)
        _fsync_directory(self.path.parent)
        self.verify_installed(artifact)

    def verify_installed(self, artifact: OverlayArtifact) -> None:
        try:
            content = self.path.read_bytes()
        except OSError:
            raise ValueError("installed overlay verification failed") from None
        if content != artifact.content:
            raise ValueError("installed overlay verification failed")
        try:
            validated = validate_overlay_content(self.target, content, artifact.sha256)
        except ValueError:
            raise ValueError("installed overlay verification failed") from None
        if validated.kind != artifact.kind or validated.candidate_sha != artifact.candidate_sha:
            raise ValueError("installed overlay verification failed")

    def restore(self, content: bytes, sha256: str) -> None:
        artifact = validate_overlay_content(self.target, content, sha256)
        staged = self.stage(artifact)
        self.install(staged, artifact)

    def discard(self, staged: Path) -> None:
        staged = self._require_known_staged(staged)
        staged.unlink(missing_ok=True)
        self._staged.discard(staged)
