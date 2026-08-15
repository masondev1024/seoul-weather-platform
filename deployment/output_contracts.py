from __future__ import annotations

import json
import re
from collections.abc import Mapping


class OutputContractError(ValueError):
    """A fixed category for an untrusted command-output shape mismatch."""


_AIRFLOW_PLUGINS = (
    "schemas",
    "tables",
    "types",
    "constraints",
    "defaults",
    "comments",
)
_AIRFLOW_PLUGIN_LINE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z "
    r"\[info\s+\] setup plugin alembic\.autogenerate\."
    r"(schemas|tables|types|constraints|defaults|comments) "
    r"\[alembic\.runtime\.plugins\] loc=plugins\.py:37$"
)


def _mapping_rows(value: object) -> list[Mapping[str, object]]:
    if type(value) is not list or len(value) > 100_000:
        raise OutputContractError("invalid_output")
    if any(not isinstance(row, Mapping) for row in value):
        raise OutputContractError("invalid_output")
    return value


def parse_airflow_json_rows(stdout: str) -> list[Mapping[str, object]]:
    """Parse Airflow 3.2.2 JSON, allowing only its known Alembic log preamble."""
    if type(stdout) is not str:
        raise OutputContractError("invalid_output")
    lines = [line.rstrip("\r") for line in stdout.splitlines() if line.strip()]
    if not lines:
        raise OutputContractError("invalid_output")
    preamble = lines[:-1]
    if preamble:
        if len(preamble) != len(_AIRFLOW_PLUGINS):
            raise OutputContractError("invalid_output")
        observed: list[str] = []
        for line in preamble:
            match = _AIRFLOW_PLUGIN_LINE.fullmatch(line)
            if match is None:
                raise OutputContractError("invalid_output")
            observed.append(match.group(1))
        if tuple(observed) != _AIRFLOW_PLUGINS:
            raise OutputContractError("invalid_output")
    try:
        payload = json.loads(lines[-1])
    except (TypeError, ValueError):
        raise OutputContractError("invalid_output") from None
    return _mapping_rows(payload)


def parse_compose_json_rows(stdout: str) -> list[Mapping[str, object]]:
    """Parse Compose JSON-list fixtures or the real CLI's NDJSON ps output."""
    if type(stdout) is not str:
        raise OutputContractError("invalid_output")
    try:
        payload = json.loads(stdout)
    except (TypeError, ValueError):
        payload = None
    if type(payload) is list:
        return _mapping_rows(payload)

    lines = [line for line in stdout.splitlines() if line.strip()]
    if not lines or len(lines) > 10_000:
        raise OutputContractError("invalid_output")
    rows: list[Mapping[str, object]] = []
    for line in lines:
        try:
            row = json.loads(line)
        except (TypeError, ValueError):
            raise OutputContractError("invalid_output") from None
        if not isinstance(row, Mapping):
            raise OutputContractError("invalid_output")
        rows.append(row)
    return rows


def parse_airflow_bool(value: object) -> bool:
    if type(value) is bool:
        return value
    if value in {"True", "true"}:
        return True
    if value in {"False", "false"}:
        return False
    raise OutputContractError("invalid_output")
