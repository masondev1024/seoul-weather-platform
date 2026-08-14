"""Fail-fast checks for the shared traffic/weather runtime contract."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping


class RuntimeTargetError(RuntimeError):
    """Raised when a DAG would use an unsafe or inconsistent runtime target."""


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DOMAIN_SCHEMA_DEFAULTS = {"traffic": "traffic", "weather": "weather"}
_RESERVED_PROD_SCHEMAS = {"ops_smoke", "prod", "production"}
# 카탈로그 키 이름도 배포 환경을 담지 않는다 — canonical ``TRINO_ICEBERG_CATALOG`` 하나이고,
# 어느 카탈로그를 가리키는지는 그 값이 정한다(호스트 컴포즈가 이 값으로 카탈로그 파일 이름을
# 짓는다). 타깃별로 다른 것은 **기대하는 카탈로그 값**이지 키 이름이 아니다.
_CATALOG_ENV = "TRINO_ICEBERG_CATALOG"
_EXPECTED_CATALOG = {"dev": "iceberg_dev", "prod": "iceberg"}
TARGET_CHOICES = ("dev", "prod")
_TARGET_ALIASES = ("ASK_SEOUL_TARGET", "DBT_TARGET")
# 자격증명 키 이름은 배포 환경을 담지 않는다 — canonical ``R2_*`` 한 세트뿐이고, 어느 버킷을
# 가리키는지는 그 값이 정한다. 타깃별로 다른 것은 **기대하는 버킷 값**이지 키 이름이 아니다.
# (구조: 과거엔 dev 가 ``R2_DEV_*`` 를 요구했으나 호스트 ENV2 개편에서 그 키가 사라져,
#  키 이름으로 환경을 고르는 규칙만 코드에 남아 있었다.)
_R2_CREDENTIAL_KEYS = (
    "R2_BUCKET_NAME",
    "R2_ENDPOINT",
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
)
_R2_EXPECTED_BUCKET = {"dev": "seoul-dev", "prod": "seoul"}


def default_target(env: Mapping[str, str] | None = None) -> str:
    """DAG ``target`` Param 의 기본값 — 런타임 env 를 따라간다.

    bronze 는 env(``is_dev_target()``)로, transform 은 DAG param 으로 타깃을 정하던
    이원화(#236)를 없앤다. env 를 prod 로 넘기면 transform param 도 같이 따라오므로
    "bronze=prod / silver=dev" 엇갈림이 생기지 않는다.

    알 수 없는 값은 ``dev`` 로 clamp 한다 — Param ``enum`` 밖의 기본값은 DAG 파싱
    자체를 깨뜨리는데, 그러면 잘못된 값 하나가 도메인 전체를 스케줄에서 지운다.
    실제 거부는 태스크 시점의 :func:`validate_dev_runtime` 이 맡는다.
    """
    values = os.environ if env is None else env
    for name in _TARGET_ALIASES:
        candidate = str(values.get(name, "")).strip().lower()
        if candidate:
            return candidate if candidate in TARGET_CHOICES else "dev"
    return "dev"


def resolve_runtime_target(env: Mapping[str, str] | None = None) -> str:
    """Resolve the authoritative runtime target for execution-time side effects.

    DAG parsing keeps :func:`default_target` permissive so a malformed local
    environment does not hide every DAG.  A write path must instead have an
    explicit target: ``DBT_TARGET`` is authoritative and a legacy alias may
    only be present when it agrees.
    """
    values = os.environ if env is None else env
    target = str(values.get("DBT_TARGET", "")).strip().lower()
    if target not in TARGET_CHOICES:
        raise RuntimeTargetError("DBT_TARGET must be explicitly set to dev or prod")
    alias = str(values.get("ASK_SEOUL_TARGET", "")).strip().lower()
    if alias and alias != target:
        raise RuntimeTargetError("runtime target aliases disagree")
    return target


def validate_dev_runtime(
    domain: str,
    env: Mapping[str, str] | None = None,
    requested_target: str | None = None,
) -> None:
    """Validate the currently approved dev/prod catalog/schema contract.

    Both ``dev`` and ``prod`` are valid targets. What this still enforces is
    that ``DBT_TARGET``/``ASK_SEOUL_TARGET`` (bronze) and the DAG's
    ``requested_target`` (transform ``--target`` param) all agree, so bronze
    and transform can never silently run against different environments.
    Secret values are intentionally not inspected or included in errors.  The
    check runs as an Airflow task before API collection or dbt execution.
    """

    values = env if env is not None else os.environ
    if domain not in _DOMAIN_SCHEMA_DEFAULTS:
        raise RuntimeTargetError(f"unsupported runtime guard domain: {domain}")

    target_values = [
        (name, str(values[name]).strip().lower())
        for name in ("DBT_TARGET", "ASK_SEOUL_TARGET")
        if str(values.get(name, "")).strip()
    ]
    if not target_values:
        raise RuntimeTargetError("runtime target must be explicitly set to dev or prod")
    if len({value for _, value in target_values}) != 1:
        raise RuntimeTargetError("runtime target aliases disagree")
    target = target_values[0][1]
    if target not in _EXPECTED_CATALOG:
        raise RuntimeTargetError("runtime target must be dev or prod")
    if requested_target is not None:
        requested = str(requested_target).strip().lower()
        if requested not in _EXPECTED_CATALOG:
            raise RuntimeTargetError("requested runtime target must be dev or prod")
        if requested != target:
            raise RuntimeTargetError("requested runtime target disagrees with environment")

    expected_catalog = _EXPECTED_CATALOG[target]
    catalog = str(values.get(_CATALOG_ENV, "")).strip()
    if catalog != expected_catalog:
        raise RuntimeTargetError(f"{target} catalog must be {expected_catalog}")

    schema_env = f"{domain.upper()}_SCHEMA"
    schema_values = {
        "ASK_SEOUL_SCHEMA": str(values.get("ASK_SEOUL_SCHEMA", "ask_seoul")).strip(),
        schema_env: str(values.get(schema_env, _DOMAIN_SCHEMA_DEFAULTS[domain])).strip(),
    }
    for env_name, schema in schema_values.items():
        if not _IDENTIFIER.fullmatch(schema):
            raise RuntimeTargetError(f"schema identifier is invalid: {env_name}")
        if target == "dev" and schema.lower() in _RESERVED_PROD_SCHEMAS:
            raise RuntimeTargetError(
                f"schema is reserved for another environment: {env_name}"
            )

    expected_bucket = _R2_EXPECTED_BUCKET[target]
    for env_name in _R2_CREDENTIAL_KEYS:
        if not str(values.get(env_name, "")).strip():
            raise RuntimeTargetError(
                f"{target} R2 credential is missing: {env_name}"
            )
    bucket_name = str(values[_R2_CREDENTIAL_KEYS[0]]).strip()
    if bucket_name != expected_bucket:
        raise RuntimeTargetError(
            f"{target} R2 bucket must be {expected_bucket}"
        )
