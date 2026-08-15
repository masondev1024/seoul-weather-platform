from __future__ import annotations

import argparse
import ast
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn

import yaml


EXTERNAL_USE = re.compile(r"^[^@]+@[0-9a-f]{40}$")
_COMMAND_SPLIT = re.compile(r"(?:\r?\n|&&|\|\||[;|])")
_COMMAND_TOKEN = re.compile(r""""[^"]*"|'[^']*'|[^\s]+""")
_SINGLE_AMPERSAND = re.compile(r"(?<![<>&])&(?![<>&])")
_EXECUTABLE_SUFFIXES = (".exe", ".cmd", ".bat")
_SHELL_WRAPPERS = frozenset({"&", "command", "exec", "sudo"})
_INDIRECT_SHELLS = frozenset(
    {"bash", "cmd", "dash", "fish", "ksh", "powershell", "pwsh", "sh", "wsl", "zsh"}
)
_SAFE_REPOSITORY_VERIFIER_PWSH_ARGV = (
    "pwsh",
    "-File",
    "tools/verify_repository.ps1",
)
_PYTHON_EXECUTABLE = re.compile(r"^(?:py|pythonw?(?:\d+(?:\.\d+)*)?)$")
_NODE_EXECUTABLES = frozenset({"node", "nodejs"})
_DYNAMIC_PACKAGE_LAUNCHERS = frozenset({"bunx", "corepack", "npx", "pnpx"})
_PACKAGE_RUNNER_COMMANDS = {
    "bun": frozenset({"run", "x"}),
    "npm": frozenset({"exec", "run", "run-script", "x"}),
    "pnpm": frozenset({"dlx", "exec", "run"}),
    "yarn": frozenset({"dlx", "exec", "run"}),
}
_GITHUB_HOSTED_RUNNERS = frozenset({"ubuntu-latest"})
_CHECKOUT_ACTION = "actions/checkout@11d5960a326750d5838078e36cf38b85af677262"
_SETUP_PYTHON_ACTION = "actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065"
_SELF_HOSTED_ACTIONS = frozenset(
    {
        _CHECKOUT_ACTION,
    }
)
_DAGBAG_COMMAND = "python -m tools.dagbag_check --repo-root ."
_VERIFY_MAIN_COMMAND = (
    "python -m deployment.main_cli verify-main "
    '--event-path "$env:GITHUB_EVENT_PATH" '
    '--workflow-ref "$env:GITHUB_WORKFLOW_REF" '
    '--workflow-sha "$env:GITHUB_WORKFLOW_SHA"'
)
_DEPLOY_MAIN_COMMAND = _VERIFY_MAIN_COMMAND.replace("verify-main", "deploy-main", 1)
_SELF_HOSTED_COMMANDS = {
    "protected_push": (_DAGBAG_COMMAND,),
    "deploy_main": (_DEPLOY_MAIN_COMMAND,),
}
_PROTECTED_CHECKOUT_INPUTS = (("persist-credentials", "false"),)
_TRUSTED_CHECKOUT_INPUTS = (
    ("ref", "${{ github.workflow_sha }}"),
    ("persist-credentials", "false"),
)
_SETUP_PYTHON_INPUTS = (("python-version", "3.11.15"),)
_SELF_HOSTED_ACTION_CONTRACTS = {
    "protected_push": ((0, _CHECKOUT_ACTION, _PROTECTED_CHECKOUT_INPUTS),),
    "deploy_main": (
        (0, _CHECKOUT_ACTION, _TRUSTED_CHECKOUT_INPUTS),
    ),
}
_MAIN_CLI_ENV = (
    ("GH_TOKEN", "${{ secrets.WEATHER_GOVERNANCE_READ_TOKEN }}"),
    ("GOVERNANCE_MODE", "${{ vars.WEATHER_GOVERNANCE_MODE }}"),
    ("DEPLOYMENT_ENABLED", "${{ vars.WEATHER_DEPLOYMENT_ENABLED }}"),
)
_SELF_HOSTED_RUN_CONTRACTS = {
    "protected_push": ((None, None),),
    "deploy_main": (("pwsh", _MAIN_CLI_ENV),),
}
_SELF_HOSTED_EXECUTION_ENV_NAMES = frozenset(
    {
        "comspec",
        "dyld_insert_libraries",
        "ld_library_path",
        "ld_preload",
        "node_options",
        "node_path",
        "path",
        "pathext",
        "pip_config_file",
        "pip_extra_index_url",
        "pip_index_url",
        "pip_trusted_host",
        "psmodulepath",
        "pythonexecutable",
        "pythonhome",
        "pythonpath",
        "pythonstartup",
        "pythonuserbase",
        "virtual_env",
        "weather_deploy_target_path",
    }
)
_SELF_HOSTED_EXECUTION_ENV_PREFIXES = ("dyld_", "ld_", "node_", "pip_")
_REQUIRED_CHECK_NAMES = ("CI / required", "Promotion Source / required")
_CI_WORKFLOW_PATH = ".github/workflows/ci.yml"
_DEPLOY_MAIN_WORKFLOW_PATH = ".github/workflows/deploy-main.yml"
_DEPLOY_MAIN_WORKFLOW_NAME = "Deploy Main"
_ENV_VALUE_OPTIONS = frozenset({"--chdir", "--unset", "-C", "-u"})
_ENV_FLAG_OPTIONS = frozenset({"--ignore-environment", "--null", "-0", "-i"})
_PYTHON_VALUE_OPTIONS = frozenset({"--check-hash-based-pycs", "-W", "-X"})
_PYTHON_FLAG_OPTIONS = frozenset(
    {
        "--help",
        "--version",
        "-b",
        "-B",
        "-d",
        "-E",
        "-h",
        "-I",
        "-O",
        "-OO",
        "-P",
        "-q",
        "-R",
        "-s",
        "-S",
        "-u",
        "-v",
        "-V",
        "-x",
    }
)

_DOCKER_GLOBAL_VALUE_OPTIONS = frozenset(
    {
        "--config",
        "--context",
        "--host",
        "-h",
        "--log-level",
        "-l",
        "--tlscacert",
        "--tlscert",
        "--tlskey",
    }
)
_DOCKER_GLOBAL_FLAG_OPTIONS = frozenset(
    {"--debug", "-d", "--tls", "--tlsverify", "--version", "-v"}
)
_COMPOSE_VALUE_OPTIONS = frozenset(
    {
        "--ansi",
        "--env-file",
        "--file",
        "-f",
        "--parallel",
        "--profile",
        "--progress",
        "--project-directory",
        "--project-name",
        "-p",
    }
)
_COMPOSE_FLAG_OPTIONS = frozenset({"--compatibility", "--dry-run"})
_COMPOSE_COMMANDS = frozenset(
    {
        "build",
        "config",
        "down",
        "exec",
        "logs",
        "ps",
        "pull",
        "recreate",
        "restart",
        "run",
        "stop",
        "up",
    }
)
_COMPOSE_MUTATION_COMMANDS = frozenset(
    {"build", "down", "exec", "recreate", "restart", "run", "stop", "up"}
)

_DBT_VALUE_OPTIONS = frozenset(
    {
        "--defer-state",
        "--indirect-selection",
        "--log-format",
        "--log-level",
        "--log-path",
        "--profile",
        "--profiles-dir",
        "--project-dir",
        "--selector",
        "--state",
        "--target",
        "--target-path",
        "--threads",
        "--vars",
        "--warn-error-options",
    }
)
_DBT_FLAG_OPTIONS = frozenset(
    {
        "--debug",
        "--fail-fast",
        "--no-use-colors",
        "--quiet",
        "--use-colors",
        "--version",
    }
)
_DBT_COMMANDS = frozenset(
    {
        "build",
        "clean",
        "clone",
        "compile",
        "debug",
        "deps",
        "docs",
        "list",
        "ls",
        "parse",
        "retry",
        "run",
        "run-operation",
        "seed",
        "show",
        "snapshot",
        "source",
        "test",
    }
)

_AIRFLOW_VALUE_OPTIONS = frozenset(
    {"--config", "--log-file", "--pid", "--stderr", "--stdout"}
)
_AIRFLOW_FLAG_OPTIONS = frozenset({"--daemon", "--help", "--version"})
_AIRFLOW_DAGS_COMMANDS = frozenset(
    {
        "backfill",
        "clear",
        "delete",
        "details",
        "list",
        "list-import-errors",
        "list-jobs",
        "list-runs",
        "next-execution",
        "pause",
        "report",
        "reserialize",
        "retry",
        "show",
        "show-dependencies",
        "state",
        "test",
        "trigger",
        "unpause",
        "mark-success",
    }
)
_AIRFLOW_DAGS_MUTATION_COMMANDS = frozenset(
    {"backfill", "clear", "mark-success", "pause", "retry", "trigger", "unpause"}
)

_WRANGLER_VALUE_OPTIONS = frozenset(
    {"--config", "-c", "--cwd", "--env", "-e", "--log-level"}
)
_WRANGLER_FLAG_OPTIONS = frozenset({"--help", "--version"})
_WRANGLER_D1_COMMANDS = frozenset(
    {"create", "delete", "execute", "export", "info", "list", "migrations"}
)
REQUIRED_CODEOWNER_PATTERNS = (
    ".github/workflows/**",
    "tools/**",
    "deployment/**",
    "runtime/**",
    "provenance/**",
    "docs/operations/**",
    "release/**",
)

_MODE = "vars.WEATHER_GOVERNANCE_MODE"
_DEPLOYMENT_ENABLED = "vars.WEATHER_DEPLOYMENT_ENABLED"
_EVENT = "github.event_name"
_REF = "github.ref"
_ACTION = "github.event.action"
_WORKFLOW_SHA = "github.workflow_sha"
_WORKFLOW_RUN_NAME = "github.event.workflow_run.name"
_WORKFLOW_RUN_PATH = "github.event.workflow_run.path"
_WORKFLOW_RUN_STATUS = "github.event.workflow_run.status"
_WORKFLOW_RUN_CONCLUSION = "github.event.workflow_run.conclusion"
_WORKFLOW_RUN_EVENT = "github.event.workflow_run.event"
_WORKFLOW_RUN_HEAD_BRANCH = "github.event.workflow_run.head_branch"
_WORKFLOW_RUN_HEAD_SHA = "github.event.workflow_run.head_sha"


@dataclass(frozen=True)
class WorkflowFinding:
    path: str
    rule: str
    summary: str


class _SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        self.exit(2, "invalid-input\n")


def _finding(path: str, rule: str, summary: str) -> WorkflowFinding:
    return WorkflowFinding(path=path, rule=rule, summary=summary)


def _workflow_paths(repo_root: Path) -> list[Path]:
    workflow_root = repo_root / ".github" / "workflows"
    if not workflow_root.is_dir():
        return []
    return sorted(
        path
        for path in workflow_root.iterdir()
        if path.is_file() and path.suffix.lower() in {".yml", ".yaml"}
    )


def _relative_path(repo_root: Path, path: Path) -> str:
    return path.relative_to(repo_root).as_posix()


def _load_workflow(path: Path) -> Mapping[str, object] | None:
    text = path.read_text(encoding="utf-8")
    loaded = yaml.load(text, Loader=yaml.BaseLoader)
    return loaded if isinstance(loaded, Mapping) else None


def _events(value: object) -> dict[str, object] | None:
    if isinstance(value, str):
        return {value: None}
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            return None
        return dict(value)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if not all(isinstance(item, str) for item in value):
            return None
        return {item: None for item in value}
    return None


def _walk_values(value: object, key: str):
    if isinstance(value, Mapping):
        for child_key, child_value in value.items():
            if child_key == key:
                yield child_value
            yield from _walk_values(child_value, key)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for child in value:
            yield from _walk_values(child, key)


def _external_use_findings(path: str, workflow: Mapping[str, object]):
    for value in _walk_values(workflow, "uses"):
        if isinstance(value, str) and value.startswith("./"):
            yield _finding(
                path,
                "local_action",
                "local actions are forbidden in repository workflows",
            )
            continue
        if not isinstance(value, str) or EXTERNAL_USE.fullmatch(value) is None:
            yield _finding(
                path,
                "unpinned_external_use",
                "external action or workflow must use a full 40-character lowercase commit SHA",
            )


def _token_text(token: str) -> str:
    if len(token) >= 2 and token[0] == token[-1] and token[0] in {"'", '"'}:
        return token[1:-1]
    return token


def _executable_name(token: str) -> str:
    name = _token_text(token).replace("\\", "/").rsplit("/", 1)[-1].casefold()
    for suffix in _EXECUTABLE_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def _option_key(token: str) -> str:
    return _token_text(token).casefold().split("=", 1)[0]


def _has_embedded_short_value(token: str, value_options: frozenset[str]) -> bool:
    return any(
        len(option) == 2
        and option.startswith("-")
        and not option.startswith("--")
        and token.startswith(option)
        and token != option
        for option in value_options
    )


def _next_positional(
    tokens: Sequence[str],
    start: int,
    *,
    value_options: frozenset[str],
    flag_options: frozenset[str],
    commands: frozenset[str],
) -> tuple[str | None, int]:
    index = start
    while index < len(tokens):
        token = _token_text(tokens[index]).casefold()
        if token == "--":
            index += 1
            if index >= len(tokens):
                return None, index
            return _token_text(tokens[index]).casefold(), index + 1
        if not token.startswith("-") or token == "-":
            return token, index + 1

        option = _option_key(token)
        if "=" in token or _has_embedded_short_value(token, value_options):
            index += 1
        elif option in value_options:
            index += 2
        elif option in flag_options:
            index += 1
        elif (
            index + 1 < len(tokens)
            and _token_text(tokens[index + 1]).casefold() in commands
        ):
            index += 1
        else:
            index += 2
    return None, index


def _env_command_start(tokens: Sequence[str], start: int) -> tuple[int | None, bool]:
    index = start
    while index < len(tokens):
        token = _token_text(tokens[index])
        lowered = token.casefold()
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", token):
            index += 1
            continue
        if lowered == "--":
            index += 1
            return (index if index < len(tokens) else None), False
        option = _option_key(lowered)
        if option in {"--split-string", "-S"}:
            return None, True
        if "=" in lowered or _has_embedded_short_value(lowered, _ENV_VALUE_OPTIONS):
            index += 1
            continue
        if option in _ENV_VALUE_OPTIONS:
            if index + 1 >= len(tokens):
                return None, True
            index += 2
            continue
        if option in _ENV_FLAG_OPTIONS:
            index += 1
            continue
        if lowered.startswith("-"):
            return None, True
        return index, False
    return None, False


def _python_invocation(
    tokens: Sequence[str], start: int
) -> tuple[str | None, int, bool]:
    index = start
    while index < len(tokens):
        token = _token_text(tokens[index])
        lowered = token.casefold()
        option = token.split("=", 1)[0]
        if lowered == "--":
            index += 1
            if index >= len(tokens):
                return None, index, True
            entrypoint = _token_text(tokens[index])
            return "python", index + 1, entrypoint == "-" or entrypoint.startswith("<")
        if option == "-c" or (lowered.startswith("-c") and len(lowered) > 2):
            return None, index, True
        if option == "-m":
            if index + 1 >= len(tokens):
                return None, index, True
            module = _token_text(tokens[index + 1]).casefold()
            if not module or module.startswith("-"):
                return None, index, True
            return module, index + 2, False
        if lowered == "-" or lowered.startswith("<"):
            return None, index, True
        if re.fullmatch(r"-(?:3(?:\.\d+)?|V:.+)", token):
            index += 1
            continue
        if "=" in token or _has_embedded_short_value(token, _PYTHON_VALUE_OPTIONS):
            index += 1
            continue
        if option in _PYTHON_VALUE_OPTIONS:
            if index + 1 >= len(tokens):
                return None, index, True
            index += 2
            continue
        if token in _PYTHON_FLAG_OPTIONS:
            index += 1
            continue
        if lowered.startswith("-"):
            return None, index, True
        if lowered in {"/dev/stdin", "con"}:
            return None, index, True
        return "python", index + 1, False
    return None, index, True


def _node_invocation_is_dynamic(tokens: Sequence[str], start: int) -> bool:
    if start >= len(tokens):
        return True
    has_script = False
    for raw_token in tokens[start:]:
        token = _token_text(raw_token).casefold()
        option = _option_key(token)
        if option in {"--eval", "--print", "-e", "-p"} or re.match(
            r"^-(?:e|p).+", token
        ):
            return True
        if token == "-" or token.startswith("<"):
            return True
        if not token.startswith("-"):
            has_script = True
    return not has_script


def _invocation(tokens: Sequence[str]) -> tuple[str | None, int, bool]:
    index = 0
    while index < len(tokens):
        token = _token_text(tokens[index])
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", token):
            index += 1
            continue
        executable = _executable_name(token)
        index += 1
        if executable in _SHELL_WRAPPERS:
            return None, index, True
        if executable == "env":
            command_start, dynamic = _env_command_start(tokens, index)
            if dynamic:
                return None, index, True
            if command_start is None:
                return executable, index, False
            index = command_start
            continue
        if _PYTHON_EXECUTABLE.fullmatch(executable):
            return _python_invocation(tokens, index)
        if executable in _NODE_EXECUTABLES:
            return executable, index, _node_invocation_is_dynamic(tokens, index)
        if executable in _DYNAMIC_PACKAGE_LAUNCHERS:
            return executable, index, True
        package_commands = _PACKAGE_RUNNER_COMMANDS.get(executable)
        if package_commands is not None:
            command, _index = _next_positional(
                tokens,
                index,
                value_options=frozenset(),
                flag_options=frozenset(),
                commands=package_commands,
            )
            return executable, index, command in package_commands
        return executable, index, False
    return None, index, False


def _segment_mutates(segment: str) -> bool:
    raw_tokens = _COMMAND_TOKEN.findall(segment)
    if any(
        token == _token_text(token) and _SINGLE_AMPERSAND.search(token)
        for token in raw_tokens
    ):
        return True
    tokens = [_token_text(token) for token in raw_tokens]
    if not tokens:
        return False
    if any(_option_key(token) == "--force-recreate" for token in tokens):
        return True
    if tuple(tokens) == _SAFE_REPOSITORY_VERIFIER_PWSH_ARGV:
        return False

    executable, index, dynamic = _invocation(tokens)
    if dynamic:
        return True
    if executable in _INDIRECT_SHELLS:
        return True
    if executable in {"docker", "docker-compose"}:
        if executable == "docker-compose":
            command, _index = _next_positional(
                tokens,
                index,
                value_options=_COMPOSE_VALUE_OPTIONS,
                flag_options=_COMPOSE_FLAG_OPTIONS,
                commands=_COMPOSE_COMMANDS,
            )
            return command in _COMPOSE_MUTATION_COMMANDS
        command, index = _next_positional(
            tokens,
            index,
            value_options=_DOCKER_GLOBAL_VALUE_OPTIONS,
            flag_options=_DOCKER_GLOBAL_FLAG_OPTIONS,
            commands=frozenset({"build", "compose"}),
        )
        if command == "build":
            return True
        if command != "compose":
            return False
        compose_command, _index = _next_positional(
            tokens,
            index,
            value_options=_COMPOSE_VALUE_OPTIONS,
            flag_options=_COMPOSE_FLAG_OPTIONS,
            commands=_COMPOSE_COMMANDS,
        )
        return compose_command in _COMPOSE_MUTATION_COMMANDS

    if executable == "dbt":
        command, _index = _next_positional(
            tokens,
            index,
            value_options=_DBT_VALUE_OPTIONS,
            flag_options=_DBT_FLAG_OPTIONS,
            commands=_DBT_COMMANDS,
        )
        return command in {"build", "run"}

    if executable == "airflow":
        group, index = _next_positional(
            tokens,
            index,
            value_options=_AIRFLOW_VALUE_OPTIONS,
            flag_options=_AIRFLOW_FLAG_OPTIONS,
            commands=frozenset({"dags"}),
        )
        if group != "dags":
            return False
        command, _index = _next_positional(
            tokens,
            index,
            value_options=_AIRFLOW_VALUE_OPTIONS,
            flag_options=_AIRFLOW_FLAG_OPTIONS,
            commands=_AIRFLOW_DAGS_COMMANDS,
        )
        return command in _AIRFLOW_DAGS_MUTATION_COMMANDS

    if executable == "wrangler":
        group, index = _next_positional(
            tokens,
            index,
            value_options=_WRANGLER_VALUE_OPTIONS,
            flag_options=_WRANGLER_FLAG_OPTIONS,
            commands=frozenset({"d1"}),
        )
        if group != "d1":
            return False
        command, _index = _next_positional(
            tokens,
            index,
            value_options=_WRANGLER_VALUE_OPTIONS,
            flag_options=_WRANGLER_FLAG_OPTIONS,
            commands=_WRANGLER_D1_COMMANDS,
        )
        return command == "execute"

    return False


def _backtick_substitution(value: str, start: int) -> tuple[str | None, int]:
    index = start + 1
    while index < len(value):
        if value[index] == "\\" and index + 1 < len(value):
            index += 2
            continue
        if value[index] == "`":
            return value[start + 1 : index], index + 1
        index += 1
    return None, len(value)


def _parenthesized_substitution(value: str, start: int) -> tuple[str | None, int]:
    body_start = start + 2
    index = body_start
    depth = 1
    quote: str | None = None
    while index < len(value):
        character = value[index]
        if quote == "'":
            if character == "'":
                quote = None
            index += 1
            continue
        if character == "\\" and index + 1 < len(value):
            index += 2
            continue
        if character == '"':
            quote = None if quote == '"' else '"'
            index += 1
            continue
        if character == "'" and quote is None:
            quote = "'"
            index += 1
            continue
        if character == "`":
            body, end = _backtick_substitution(value, index)
            if body is None:
                return None, len(value)
            index = end
            continue
        if value.startswith(("$(", "<(", ">("), index):
            body, end = _parenthesized_substitution(value, index)
            if body is None:
                return None, len(value)
            index = end
            continue
        if quote is None:
            if character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
                if depth == 0:
                    return value[body_start:index], index + 1
        index += 1
    return None, len(value)


def _shell_substitutions(value: str) -> tuple[list[str], bool]:
    bodies: list[str] = []
    index = 0
    quote: str | None = None
    while index < len(value):
        character = value[index]
        if quote == "'":
            if character == "'":
                quote = None
            index += 1
            continue
        if character == "\\" and index + 1 < len(value):
            index += 2
            continue
        if character == '"':
            quote = None if quote == '"' else '"'
            index += 1
            continue
        if character == "'" and quote is None:
            quote = "'"
            index += 1
            continue
        if character == "`":
            body, end = _backtick_substitution(value, index)
            if body is None:
                return bodies, True
            bodies.append(body)
            index = end
            continue
        if value.startswith(("$(", "<(", ">("), index):
            body, end = _parenthesized_substitution(value, index)
            if body is None:
                return bodies, True
            bodies.append(body)
            index = end
            continue
        index += 1
    return bodies, False


def _script_mutates(value: str) -> bool:
    normalized = re.sub(r"(?:\\|`)\r?\n[ \t]*", " ", value)
    substitutions, malformed = _shell_substitutions(normalized)
    if malformed or any(_script_mutates(body) for body in substitutions):
        return True
    return any(
        _segment_mutates(segment) for segment in _COMMAND_SPLIT.split(normalized)
    )


def _runtime_mutation_findings(path: str, workflow: Mapping[str, object]):
    for value in _walk_values(workflow, "run"):
        if not isinstance(value, str):
            continue
        if _script_mutates(value):
            yield _finding(
                path,
                "runtime_mutation",
                "workflow run command contains a forbidden runtime mutation",
            )


def _is_self_hosted(value: object) -> bool:
    if isinstance(value, str):
        return value not in _GITHUB_HOSTED_RUNNERS
    if isinstance(value, Mapping):
        return True
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return not value or any(
            not isinstance(item, str) or item not in _GITHUB_HOSTED_RUNNERS
            for item in value
        )
    return True


def _string_set(value: object) -> set[str] | None:
    if isinstance(value, str):
        return {value}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if all(isinstance(item, str) for item in value):
            return set(value)
    return None


def _event_config_is(
    events: Mapping[str, object], event: str, key: str, expected: set[str]
) -> bool:
    config = events.get(event)
    return isinstance(config, Mapping) and _string_set(config.get(key)) == expected


def _workflow_run_config_is_trusted(events: Mapping[str, object]) -> bool:
    return (
        _event_config_is(events, "workflow_run", "workflows", {"CI"})
        and _event_config_is(events, "workflow_run", "types", {"completed"})
        and _event_config_is(events, "workflow_run", "branches", {"main"})
    )


def _expression_tree(value: object) -> ast.expr | None:
    if not isinstance(value, str) or not value.strip():
        return None
    expression = value.strip()
    if expression.startswith("${{") and expression.endswith("}}"):
        expression = expression[3:-2].strip()
    expression = expression.replace("&&", " and ").replace("||", " or ")
    expression = re.sub(r"(?<![=!])!(?!=)", " not ", expression).strip()
    try:
        return ast.parse(expression, mode="eval").body
    except SyntaxError:
        return None


def _attribute_name(node: ast.AST) -> str | None:
    parts: list[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if not isinstance(current, ast.Name):
        return None
    parts.append(current.id)
    return ".".join(reversed(parts))


_GuardAtom = tuple[str, str, str | bool]
_GuardClause = frozenset[_GuardAtom]
_NO_LITERAL = object()
_MAX_GUARD_CLAUSES = 16


def _literal_value(node: ast.AST) -> object:
    if isinstance(node, ast.Constant) and isinstance(node.value, (str, bool)):
        return node.value
    if isinstance(node, ast.Name) and node.id in {"false", "true"}:
        return node.id == "true"
    return _NO_LITERAL


def _value_atom(field: str, value: str | bool) -> _GuardAtom:
    return ("value", field, value)


def _field_equality_atom(left: str, right: str) -> _GuardAtom:
    first, second = sorted((left, right))
    return ("field", first, second)


def _comparison_atom(node: ast.AST) -> _GuardAtom | None:
    if (
        not isinstance(node, ast.Compare)
        or len(node.ops) != 1
        or len(node.comparators) != 1
        or not isinstance(node.ops[0], ast.Eq)
    ):
        return None

    left_literal = _literal_value(node.left)
    right_literal = _literal_value(node.comparators[0])
    left_field = None if left_literal is not _NO_LITERAL else _attribute_name(node.left)
    right_field = (
        None
        if right_literal is not _NO_LITERAL
        else _attribute_name(node.comparators[0])
    )
    if left_field is not None and right_field is not None:
        return _field_equality_atom(left_field, right_field)
    if left_field is not None and isinstance(right_literal, (str, bool)):
        return _value_atom(left_field, right_literal)
    if right_field is not None and isinstance(left_literal, (str, bool)):
        return _value_atom(right_field, left_literal)
    return None


def _guard_clauses(node: ast.AST) -> set[_GuardClause] | None:
    if isinstance(node, ast.BoolOp):
        child_clauses = [_guard_clauses(child) for child in node.values]
        if any(clauses is None for clauses in child_clauses):
            return None
        known_children = [clauses for clauses in child_clauses if clauses is not None]
        if isinstance(node.op, ast.Or):
            result = set().union(*known_children)
            return result if len(result) <= _MAX_GUARD_CLAUSES else None
        if not isinstance(node.op, ast.And):
            return None
        result: set[_GuardClause] = {frozenset()}
        for clauses in known_children:
            result = {left | right for left in result for right in clauses}
            if len(result) > _MAX_GUARD_CLAUSES:
                return None
        return result

    atom = _comparison_atom(node)
    if atom is not None:
        return {frozenset({atom})}
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        field = _attribute_name(node.operand)
        if field is not None:
            return {frozenset({_value_atom(field, False)})}
    return None


def _required_guard_clauses(
    workflow_name: object, events: Mapping[str, object]
) -> set[_GuardClause] | None:
    required: set[_GuardClause] = set()
    protected = _value_atom(_MODE, "protected")

    if "push" in events and workflow_name == "CI":
        push = _value_atom(_EVENT, "push")
        required.update(
            {
                frozenset({protected, push, _value_atom(_REF, "refs/heads/dev")}),
                frozenset({protected, push, _value_atom(_REF, "refs/heads/main")}),
            }
        )

    if (
        workflow_name == _DEPLOY_MAIN_WORKFLOW_NAME
        and set(events) == {"workflow_run"}
        and _workflow_run_config_is_trusted(events)
    ):
        required.add(
            frozenset(
                {
                    protected,
                    _value_atom(_DEPLOYMENT_ENABLED, "enabled"),
                    _value_atom(_EVENT, "workflow_run"),
                    _value_atom(_ACTION, "completed"),
                    _value_atom(_WORKFLOW_RUN_NAME, "CI"),
                    _value_atom(
                        _WORKFLOW_RUN_PATH,
                        ".github/workflows/ci.yml",
                    ),
                    _value_atom(_WORKFLOW_RUN_STATUS, "completed"),
                    _value_atom(_WORKFLOW_RUN_CONCLUSION, "success"),
                    _value_atom(_WORKFLOW_RUN_EVENT, "push"),
                    _value_atom(_WORKFLOW_RUN_HEAD_BRANCH, "main"),
                    _field_equality_atom(_WORKFLOW_SHA, _WORKFLOW_RUN_HEAD_SHA),
                }
            )
        )

    return required or None


def _trusted_self_hosted_guard_clauses(
    workflow_name: object,
    events: Mapping[str, object],
    condition: object,
) -> set[_GuardClause] | None:
    tree = _expression_tree(condition)
    required = _required_guard_clauses(workflow_name, events)
    if tree is None or required is None:
        return None
    clauses = _guard_clauses(tree)
    if not clauses or not clauses <= required:
        return None
    return clauses


def _self_hosted_route(
    trusted_clauses: set[_GuardClause],
) -> str | None:
    event_values = {
        atom[2]
        for clause in trusted_clauses
        for atom in clause
        if atom[:2] == ("value", _EVENT)
    }
    if event_values == {"push"}:
        return "protected_push"
    if event_values == {"workflow_run"}:
        return "deploy_main"
    return None


def _self_hosted_steps(job: Mapping[str, object]) -> Sequence[object] | None:
    steps = job.get("steps")
    if not isinstance(steps, Sequence) or isinstance(steps, (str, bytes, bytearray)):
        return None
    return steps


def _self_hosted_actions_are_allowed(job: Mapping[str, object]) -> bool:
    steps = _self_hosted_steps(job)
    if steps is None:
        return False
    for step in steps:
        if not isinstance(step, Mapping):
            return False
        if "uses" in step and step.get("uses") not in _SELF_HOSTED_ACTIONS:
            return False
    return True


def _exact_string_mapping(value: object) -> tuple[tuple[str, str], ...] | None:
    if not isinstance(value, Mapping):
        return None
    if not all(
        isinstance(key, str) and isinstance(item, str) for key, item in value.items()
    ):
        return None
    return tuple(value.items())


def _self_hosted_action_contract_matches(route: str, job: Mapping[str, object]) -> bool:
    steps = _self_hosted_steps(job)
    expected = _SELF_HOSTED_ACTION_CONTRACTS.get(route)
    if steps is None or expected is None:
        return False
    actions: list[tuple[int, str, tuple[tuple[str, str], ...]]] = []
    for index, step in enumerate(steps):
        if not isinstance(step, Mapping) or "uses" not in step:
            continue
        if "name" in step and not isinstance(step.get("name"), str):
            return False
        if set(step) - {"name"} != {"uses", "with"}:
            return False
        uses = step.get("uses")
        inputs = _exact_string_mapping(step.get("with"))
        if not isinstance(uses, str) or inputs is None:
            return False
        actions.append((index, uses, inputs))
    return tuple(actions) == expected


def _has_run_defaults(scope: Mapping[str, object]) -> bool:
    if "defaults" not in scope:
        return False
    defaults = scope.get("defaults")
    return not isinstance(defaults, Mapping) or "run" in defaults


def _scope_execution_env_is_safe(scope: Mapping[str, object]) -> bool:
    if "env" not in scope:
        return True
    environment = scope.get("env")
    if not isinstance(environment, Mapping):
        return False
    for name in environment:
        if not isinstance(name, str):
            return False
        normalized = name.casefold()
        if normalized in _SELF_HOSTED_EXECUTION_ENV_NAMES or normalized.startswith(
            _SELF_HOSTED_EXECUTION_ENV_PREFIXES
        ):
            return False
    return True


def _self_hosted_execution_context_is_safe(
    workflow: Mapping[str, object],
    job: Mapping[str, object],
    route: str | None,
) -> bool:
    if (
        _has_run_defaults(workflow)
        or _has_run_defaults(job)
        or not _scope_execution_env_is_safe(workflow)
        or not _scope_execution_env_is_safe(job)
        or "container" in job
        or "services" in job
    ):
        return False
    steps = _self_hosted_steps(job)
    if steps is None:
        return False
    run_contracts: list[
        tuple[str | None, tuple[tuple[str, str], ...] | None]
    ] = []
    for step in steps:
        if not isinstance(step, Mapping):
            return False
        if "working-directory" in step:
            return False
        has_run = "run" in step
        has_uses = "uses" in step
        if has_run == has_uses:
            return False
        if not has_run:
            continue
        if "name" in step and not isinstance(step.get("name"), str):
            return False
        if set(step) - {"name"} not in (
            {"run"},
            {"run", "env"},
            {"run", "shell"},
            {"run", "shell", "env"},
        ):
            return False
        shell = step.get("shell")
        if shell is not None and not isinstance(shell, str):
            return False
        if "env" not in step:
            run_contracts.append((shell, None))
            continue
        environment = _exact_string_mapping(step.get("env"))
        if environment is None:
            return False
        run_contracts.append((shell, environment))
    expected = _SELF_HOSTED_RUN_CONTRACTS.get(route) if route is not None else None
    return expected is None or tuple(run_contracts) == expected


def _self_hosted_run_commands(job: Mapping[str, object]) -> tuple[str, ...] | None:
    steps = _self_hosted_steps(job)
    if steps is None:
        return None
    commands: list[str] = []
    for step in steps:
        if not isinstance(step, Mapping):
            return None
        if "run" not in step:
            continue
        command = step.get("run")
        if not isinstance(command, str):
            return None
        commands.append(command.strip())
    return tuple(commands)


def _self_hosted_findings(
    path: str,
    workflow: Mapping[str, object],
    events: Mapping[str, object],
):
    jobs = workflow.get("jobs")
    if not isinstance(jobs, Mapping):
        return
    for job in jobs.values():
        if not isinstance(job, Mapping) or not _is_self_hosted(job.get("runs-on")):
            continue
        trusted_clauses = _trusted_self_hosted_guard_clauses(
            workflow.get("name"), events, job.get("if")
        )
        if trusted_clauses is None:
            yield _finding(
                path,
                "self_hosted_event_boundary",
                "self-hosted job is not limited to an approved protected event boundary",
            )
        if not _self_hosted_actions_are_allowed(job):
            yield _finding(
                path,
                "self_hosted_action_allowlist",
                "self-hosted jobs may use only the pinned checkout action",
            )
        route = (
            _self_hosted_route(trusted_clauses) if trusted_clauses is not None else None
        )
        if not _self_hosted_execution_context_is_safe(workflow, job, route):
            yield _finding(
                path,
                "self_hosted_execution_context",
                "self-hosted job execution context does not match the approved route",
            )
        if route is not None and not _self_hosted_action_contract_matches(route, job):
            yield _finding(
                path,
                "self_hosted_action_contract",
                "self-hosted job actions do not match route inputs and order",
            )
        commands = _self_hosted_run_commands(job)
        if route is not None and commands != _SELF_HOSTED_COMMANDS[route]:
            yield _finding(
                path,
                "self_hosted_command_allowlist",
                "self-hosted job commands do not match the approved route entrypoints",
            )
def _has_required_ci_always(workflow: Mapping[str, object]) -> bool:
    jobs = workflow.get("jobs")
    if not isinstance(jobs, Mapping):
        return False
    for job in jobs.values():
        if not isinstance(job, Mapping) or job.get("name") != "CI / required":
            continue
        condition = job.get("if")
        if not isinstance(condition, str):
            continue
        expression = condition.strip()
        if expression.startswith("${{") and expression.endswith("}}"):
            expression = expression[3:-2].strip()
        if re.fullmatch(r"always\s*\(\s*\)", expression):
            return True
    return False


def _deploy_main_guard_is_exact(
    workflow_name: object,
    events: Mapping[str, object],
    condition: object,
) -> bool:
    tree = _expression_tree(condition)
    required = _required_guard_clauses(workflow_name, events)
    if tree is None or required is None or len(required) != 1:
        return False
    return _guard_clauses(tree) == required


def _contract_step(
    step: object, required_keys: set[str]
) -> Mapping[str, object] | None:
    if not isinstance(step, Mapping) or set(step) not in (
        required_keys,
        required_keys | {"name"},
    ):
        return None
    name = step.get("name")
    if name is not None and (not isinstance(name, str) or not name.strip()):
        return None
    return step


def _deploy_main_steps_match(
    steps: object, command: str, *, hosted: bool
) -> bool:
    expected_length = 3 if hosted else 2
    if (
        not isinstance(steps, Sequence)
        or isinstance(steps, (str, bytes, bytearray))
        or len(steps) != expected_length
    ):
        return False
    checkout = _contract_step(steps[0], {"uses", "with"})
    setup_python = (
        _contract_step(steps[1], {"uses", "with"}) if hosted else None
    )
    invoke_index = 2 if hosted else 1
    invoke = _contract_step(steps[invoke_index], {"run", "shell", "env"})
    required = (checkout, setup_python, invoke) if hosted else (checkout, invoke)
    if any(item is None for item in required):
        return False
    assert checkout is not None
    assert invoke is not None
    if (
        checkout.get("uses") == _CHECKOUT_ACTION
        and _exact_string_mapping(checkout.get("with")) == _TRUSTED_CHECKOUT_INPUTS
        and invoke.get("run") == command
        and invoke.get("shell") == "pwsh"
        and _exact_string_mapping(invoke.get("env")) == _MAIN_CLI_ENV
    ):
        if not hosted:
            return True
        assert setup_python is not None
        return (
            setup_python.get("uses") == _SETUP_PYTHON_ACTION
            and _exact_string_mapping(setup_python.get("with"))
            == _SETUP_PYTHON_INPUTS
        )
    return False


def _deploy_main_job_matches(
    job: object,
    *,
    workflow_name: object,
    events: Mapping[str, object],
    hosted: bool,
) -> bool:
    if not isinstance(job, Mapping):
        return False
    expected_keys = {"name", "if", "runs-on", "steps"}
    if not hosted:
        expected_keys.update({"needs", "timeout-minutes"})
    if set(job) != expected_keys:
        return False
    expected_name = "verify-main" if hosted else "deploy-main"
    expected_runner: object = (
        "ubuntu-latest" if hosted else ["self-hosted", "windows", "weather-prod"]
    )
    expected_command = _VERIFY_MAIN_COMMAND if hosted else _DEPLOY_MAIN_COMMAND
    if (
        job.get("name") != expected_name
        or job.get("runs-on") != expected_runner
        or not _deploy_main_guard_is_exact(workflow_name, events, job.get("if"))
        or not _deploy_main_steps_match(
            job.get("steps"), expected_command, hosted=hosted
        )
    ):
        return False
    if hosted:
        return True
    return job.get("needs") == "verify-main" and job.get("timeout-minutes") == "60"


def _deploy_main_contract_matches(
    relative_path: str,
    workflow: Mapping[str, object],
    events: Mapping[str, object],
) -> bool:
    if relative_path != _DEPLOY_MAIN_WORKFLOW_PATH:
        return False
    if set(workflow) != {"name", "on", "permissions", "concurrency", "jobs"}:
        return False
    if workflow.get("name") != _DEPLOY_MAIN_WORKFLOW_NAME:
        return False
    if events != {
        "workflow_run": {
            "workflows": ["CI"],
            "types": ["completed"],
            "branches": ["main"],
        }
    }:
        return False
    if workflow.get("permissions") != {
        "actions": "read",
        "checks": "read",
        "contents": "read",
    }:
        return False
    if workflow.get("concurrency") != {
        "group": "weather-main-deploy",
        "cancel-in-progress": "false",
    }:
        return False
    jobs = workflow.get("jobs")
    if not isinstance(jobs, Mapping) or set(jobs) != {"verify-main", "deploy-main"}:
        return False
    return _deploy_main_job_matches(
        jobs.get("verify-main"),
        workflow_name=workflow.get("name"),
        events=events,
        hosted=True,
    ) and _deploy_main_job_matches(
        jobs.get("deploy-main"),
        workflow_name=workflow.get("name"),
        events=events,
        hosted=False,
    )


def _workflow_findings(repo_root: Path, path: Path) -> list[WorkflowFinding]:
    relative_path = _relative_path(repo_root, path)
    try:
        workflow = _load_workflow(path)
    except (OSError, UnicodeError):
        return [
            _finding(
                relative_path,
                "workflow_read_error",
                "workflow file could not be read as UTF-8",
            )
        ]
    except yaml.YAMLError:
        return [
            _finding(
                relative_path,
                "workflow_parse_error",
                "workflow file is not valid YAML",
            )
        ]
    if workflow is None:
        return [
            _finding(
                relative_path,
                "workflow_schema",
                "workflow root must be a mapping",
            )
        ]

    events = _events(workflow.get("on"))
    jobs = workflow.get("jobs")
    if events is None or not isinstance(jobs, Mapping):
        return [
            _finding(
                relative_path,
                "workflow_schema",
                "workflow must define string-keyed triggers and jobs",
            )
        ]

    findings: list[WorkflowFinding] = []
    if "pull_request_target" in events:
        findings.append(
            _finding(
                relative_path,
                "pull_request_target",
                "pull_request_target trigger is forbidden",
            )
        )
    findings.extend(_external_use_findings(relative_path, workflow))
    findings.extend(_runtime_mutation_findings(relative_path, workflow))
    findings.extend(_self_hosted_findings(relative_path, workflow, events))
    if (
        relative_path == _DEPLOY_MAIN_WORKFLOW_PATH
        or workflow.get("name") == _DEPLOY_MAIN_WORKFLOW_NAME
    ) and not _deploy_main_contract_matches(relative_path, workflow, events):
        findings.append(
            _finding(
                relative_path,
                "deploy_main_contract",
                "Deploy Main workflow does not match the exact hosted-preflight and self-hosted deployment contract",
            )
        )
    if workflow.get("name") == "CI" and not _has_required_ci_always(workflow):
        findings.append(
            _finding(
                relative_path,
                "required_ci_always",
                "CI workflow must define CI / required with if: always()",
            )
        )
    return findings


def _codeowners_findings(repo_root: Path) -> list[WorkflowFinding]:
    relative_path = ".github/CODEOWNERS"
    path = repo_root / ".github" / "CODEOWNERS"
    patterns: set[str] = set()
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        content = ""
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        fields = stripped.split()
        if len(fields) >= 2:
            patterns.add(fields[0])
    return [
        _finding(
            relative_path,
            "codeowners_coverage",
            f"required path is not explicitly covered: {required}",
        )
        for required in REQUIRED_CODEOWNER_PATTERNS
        if required not in patterns
    ]


def _required_check_name_findings(repo_root: Path) -> list[WorkflowFinding]:
    workflows: list[tuple[str, Mapping[str, object]]] = []
    for path in _workflow_paths(repo_root):
        try:
            workflow = _load_workflow(path)
        except (OSError, UnicodeError, yaml.YAMLError):
            continue
        if workflow is not None:
            workflows.append((_relative_path(repo_root, path), workflow))
    if not any(workflow.get("name") == "CI" for _, workflow in workflows):
        return []

    occurrences: dict[str, list[str]] = {name: [] for name in _REQUIRED_CHECK_NAMES}
    for path, workflow in workflows:
        jobs = workflow.get("jobs")
        if not isinstance(jobs, Mapping):
            continue
        for job in jobs.values():
            if not isinstance(job, Mapping):
                continue
            name = job.get("name")
            if name in occurrences:
                occurrences[name].append(path)

    findings: list[WorkflowFinding] = []
    for name, paths in occurrences.items():
        if len(paths) != 1:
            findings.append(
                _finding(
                    _CI_WORKFLOW_PATH,
                    "required_check_name_unique",
                    f"required check name must occur exactly once: {name}",
                )
            )
        for path in paths:
            if path != _CI_WORKFLOW_PATH:
                findings.append(
                    _finding(
                        path,
                        "required_check_name_owner",
                        f"required check name must be owned by {_CI_WORKFLOW_PATH}: {name}",
                    )
                )
    return findings


def audit_workflows(repo_root: Path) -> list[WorkflowFinding]:
    root = repo_root.resolve()
    findings: list[WorkflowFinding] = []
    for path in _workflow_paths(root):
        findings.extend(_workflow_findings(root, path))
    findings.extend(_required_check_name_findings(root))
    findings.extend(_codeowners_findings(root))
    return findings


def build_parser() -> argparse.ArgumentParser:
    parser = _SanitizedArgumentParser(
        description="Audit GitHub workflows and CODEOWNERS without network access."
    )
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    findings = audit_workflows(args.repo_root)
    for finding in findings:
        print(f"ERROR: {finding.path}: {finding.rule}: {finding.summary}")
    if findings:
        return 1
    print("Workflow policy verified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
