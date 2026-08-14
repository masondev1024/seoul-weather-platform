"""Serving Contract v1 validation rules.

Pure functions over ``ServingModel`` records + an optional dbt manifest view.
Structural rules (required / optional / excluded / enum) are driven by
``schema.yml``; cross-model and semantic rules are coded here. Every finding is a
FAIL (CLI exit 1); invocation/IO problems are the CLI's ERROR (exit 2).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

from serving_contract.model import ManifestView, ServingModel

SCHEMA_PATH = Path(__file__).parent / "schema.yml"
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SEMVER_RE = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")
EMPTY_RESULT_CODE_RE = re.compile(r"^[a-z][a-z0-9_]*$")
VOCABULARY_ID_RE = re.compile(r"^[a-z][a-z0-9_]*:[a-z][a-z0-9_]*$")
INTERNAL_PUBLIC_FIELD_PARTS = (
    "raw_object_key",
    "payload_hash",
    "request_id",
    "dag_run_id",
    "source_run_id",
    "snapshot_dag_run_id",
    "representative_dag_run_id",
    "api_key",
    "service_key",
    "access_key",
    "secret",
    "token",
    "password",
    "credential",
    "email",
    "ip_address",
)


@dataclass(frozen=True)
class Finding:
    rule: str
    model: str
    message: str
    source: str = ""

    def as_dict(self) -> dict[str, str]:
        return {"rule": self.rule, "model": self.model, "message": self.message, "source": self.source}


@dataclass
class ValidationResult:
    findings: list[Finding]
    models_checked: int

    @property
    def ok(self) -> bool:
        return not self.findings


def load_schema(path: str | Path = SCHEMA_PATH) -> dict[str, Any]:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def _type_ok(value: Any, spec: dict[str, Any]) -> bool:
    kind = spec.get("type")
    if kind == "bool":
        return isinstance(value, bool)
    if kind == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if kind == "string":
        return isinstance(value, str)
    if kind == "list":
        return isinstance(value, list)
    if kind in {"object"}:
        return isinstance(value, dict)
    if kind == "enum":
        return True  # enum membership checked separately
    return True


def _validate_value(field: str, value: Any, spec: dict[str, Any], *, required: bool) -> list[tuple[str, str]]:
    """Return (rule_id, message) violations for one declared field value."""
    out: list[tuple[str, str]] = []
    base_rule = "required_field_invalid" if required else "optional_field_invalid"

    if spec.get("type") == "enum":
        allowed = spec.get("allowed", [])
        if value not in allowed:
            out.append(("invalid_enum_value", f"'{field}'={value!r} 은 허용값 {allowed} 이 아니다"))
        return out

    if not _type_ok(value, spec):
        out.append((base_rule, f"'{field}' 타입이 {spec.get('type')} 이어야 하는데 {type(value).__name__}"))
        return out

    if spec.get("non_empty") and isinstance(value, str) and not value.strip():
        out.append((base_rule, f"'{field}' 이 비어 있다"))
    if spec.get("pattern") and isinstance(value, str) and not re.fullmatch(spec["pattern"], value):
        out.append(("invalid_field_format", f"'{field}'={value!r} 이 형식 {spec['pattern']} 과 불일치"))
    if spec.get("min_items") and isinstance(value, list) and len(value) < spec["min_items"]:
        out.append((base_rule, f"'{field}' 은 최소 {spec['min_items']}개 항목이 필요하다"))
    if spec.get("min") is not None and isinstance(value, int) and value < spec["min"]:
        out.append((base_rule, f"'{field}'={value} 은 최소 {spec['min']} 이상이어야 한다"))
    return out


def _check_structural(model: ServingModel, schema: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    serving = model.serving

    def add(rule: str, message: str) -> None:
        findings.append(Finding(rule, model.name, message, model.source))

    # Required presence + value validity.
    for field, spec in schema["required"].items():
        if field not in serving:
            add("required_field_missing", f"필수 필드 '{field}' 누락")
            continue
        for rule, message in _validate_value(field, serving[field], spec, required=True):
            add(rule, message)

    # Optional value validity (only when present).
    for field, spec in schema.get("optional", {}).items():
        if field in serving:
            for rule, message in _validate_value(field, serving[field], spec, required=False):
                add(rule, message)

    # Excluded fields must not appear under meta.serving.
    for field in schema.get("excluded", []):
        if field in serving:
            add("excluded_field_present", f"'{field}' 은 계약에 선언할 수 없다 (실측/Worker 소유)")

    # Legacy meta keys alongside meta.serving == double declaration.
    for key in schema.get("legacy_meta_keys", []):
        if key in model.meta:
            add("legacy_double_declaration", f"구 메타 'meta.{key}' 와 신규 'meta.serving' 이중 선언")

    # Conditional requirement: declaring `if_present` obligates `then_required`.
    for rule in schema.get("conditional_required", []):
        trigger, needed = rule.get("if_present"), rule.get("then_required")
        if trigger and needed and trigger in serving and needed not in serving:
            add("conditional_required_missing", f"'{trigger}' 선언 제품은 '{needed}' 필수")

    # v1.3 (#600/#638): usage_patterns 항목 검증 — 스펙 밖 필드·requires 오타가 통과되지 않게.
    findings.extend(_check_usage_patterns(model, schema))
    findings.extend(_check_source_evidence(model, schema))
    findings.extend(_check_quality_coverage(model, schema))
    findings.extend(_check_display(model, schema))

    return findings


def _check_display(model: ServingModel, schema: dict[str, Any]) -> list[Finding]:
    """display(optional, v1.10 · #706) — 사람이 읽는 표시 메타.

    형(object)은 optional 루프가 보고 여기는 내용을 본다. 다른 중첩 스펙과 같은 규약이다:
    스펙 밖 필드는 오타로 보고 잡되, **미선언은 통과**시킨다(선언한 절반만 검증 대상).
    """
    findings: list[Finding] = []
    display = model.serving.get("display")
    spec = schema.get("display_fields") or {}
    if display is None:
        return findings

    def add(rule: str, message: str) -> None:
        findings.append(Finding(rule, model.name, message, model.source))

    if not isinstance(display, dict):
        add("display_invalid", f"display 는 매핑이어야 하는데 {type(display).__name__}")
        return findings

    required = list(spec.get("required") or ())
    optional = list(spec.get("optional") or ())
    for field in sorted(set(display) - set(required) - set(optional)):
        add("display_unknown_field", f"display — 스펙 밖 필드 {field!r} (오타 확인)")

    for field in required:
        value = display.get(field)
        if not isinstance(value, str) or not value.strip():
            add("display_invalid", f"display.{field} — 필수이며 비어 있지 않은 문자열이어야 한다")

    caveat = display.get("caveat")
    if caveat is not None and (not isinstance(caveat, str) or not caveat.strip()):
        add("display_invalid", "display.caveat — 선언했으면 비어 있지 않아야 한다")

    # title 은 표·카드 제목 자리라 길면 화면이 자른다. 잘린 제목은 뜻이 바뀌므로 계약에서 막는다.
    title = display.get("title")
    max_len = spec.get("title_max_len")
    if isinstance(title, str) and isinstance(max_len, int) and len(title) > max_len:
        add("display_invalid", f"display.title — {max_len}자 이하여야 한다 (현재 {len(title)}자)")

    use_cases = display.get("use_cases")
    if use_cases is not None:
        if not isinstance(use_cases, list) or not use_cases:
            add("display_invalid", "display.use_cases — 선언했으면 비어 있지 않은 리스트여야 한다")
        else:
            for index, entry in enumerate(use_cases):
                if not isinstance(entry, str) or not entry.strip():
                    add("display_invalid", f"display.use_cases[{index}] — 비어 있지 않은 문자열이어야 한다")
    return findings


def _check_usage_patterns(model: ServingModel, schema: dict[str, Any]) -> list[Finding]:
    """usage_patterns(optional, v1.3) 항목 규칙 — 형은 optional 루프가, 내용은 여기가 본다.

    전역 식별자는 (product_id, pattern_id) 쌍(#638 §1)이므로 pattern_id 는 모델 안 유일성만
    강제한다. requires 어휘는 schema.yml `usage_pattern_fields.requires_allowed` 11개 고정.
    """
    findings: list[Finding] = []
    patterns = model.serving.get("usage_patterns")
    spec = schema.get("usage_pattern_fields") or {}
    if not isinstance(patterns, list) or not spec:
        return findings  # 타입 위반은 optional 루프(optional_field_invalid)가 이미 보고한다

    def add(rule: str, message: str) -> None:
        findings.append(Finding(rule, model.name, message, model.source))

    required = list(spec.get("required", []))
    known = set(required) | set(spec.get("optional", []))
    requires_allowed = set(spec.get("requires_allowed", []))
    seen_ids: set[str] = set()

    for index, pattern in enumerate(patterns):
        label = f"usage_patterns[{index}]"
        if not isinstance(pattern, dict):
            add("usage_pattern_invalid", f"{label} 은 매핑이어야 하는데 {type(pattern).__name__}")
            continue
        pattern_id = pattern.get("pattern_id")
        if isinstance(pattern_id, str) and pattern_id.strip():
            label = f"usage_patterns[{index}] '{pattern_id}'"
            if pattern_id in seen_ids:
                add("usage_pattern_duplicate", f"{label} — pattern_id 가 모델 안에서 중복")
            seen_ids.add(pattern_id)
        for field in required:
            value = pattern.get(field)
            if not isinstance(value, str) or not value.strip():
                add("usage_pattern_required_missing", f"{label} — 필수 '{field}' 누락/비문자열")
        for field in sorted(set(pattern) - known):
            add("usage_pattern_unknown_field", f"{label} — 스펙 밖 필드 '{field}' (오타 확인)")
        requires = pattern.get("requires")
        if requires is not None:
            if not isinstance(requires, list):
                add("usage_pattern_invalid", f"{label} — 'requires' 는 리스트여야 한다")
            else:
                for entry in requires:
                    if entry not in requires_allowed:
                        add("usage_pattern_requires_unknown",
                            f"{label} — requires 값 {entry!r} 은 어휘 11개(#638 §1.1)에 없다")
        if "verified_rows" in pattern and (
            isinstance(pattern["verified_rows"], bool) or not isinstance(pattern["verified_rows"], int)
        ):
            add("usage_pattern_invalid", f"{label} — 'verified_rows' 는 정수여야 한다")
        if "allow_empty" in pattern and not isinstance(pattern["allow_empty"], bool):
            add("usage_pattern_invalid", f"{label} — 'allow_empty' 는 불리언이어야 한다")
        findings.extend(_check_pattern_param_meta(model, pattern, label, spec))
        findings.extend(_check_pattern_verifiability(model, pattern, label))

    return findings


def _scalar(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, (str, int, float))


# v1.12 (#217): 동적 기본값 — 날짜/기간 파라미터는 정적 상수면 낡으므로(어제의 :from 이 계속
# 나옴) `{rel: "-30d", as: date}` 상대 표현으로 선언한다. 게이트웨이가 실행 시점 KST '오늘'
# 기준으로 해석한다(run-pattern-ext.js resolveRelativeDefault 와 규격 잠금).
_REL_RE = re.compile(r"^[+-]?\d+(d|w|M|y)$")
_REL_AS = {"date", "datetime", "ym", "year"}


def _is_relative_default(value: Any) -> bool:
    return (isinstance(value, dict) and isinstance(value.get("rel"), str)
            and isinstance(value.get("as"), str))


def _relative_default_error(value: dict) -> str | None:
    extra = set(value) - {"rel", "as"}
    if extra:
        return f"허용 밖 키 {sorted(extra)} (rel·as 만)"
    if not _REL_RE.match(value["rel"]):
        return f"rel '{value['rel']}' 형식 오류 (예: -30d, -4w, -6M, -1y)"
    if value["as"] not in _REL_AS:
        return f"as '{value['as']}' 는 {sorted(_REL_AS)} 중 하나여야 한다"
    return None


def _check_pattern_param_meta(model: ServingModel, pattern: dict[str, Any],
                              label: str, spec: dict[str, Any]) -> list[Finding]:
    """v1.11 (#217 P1·P3) — param_defaults·param_enum·params 의 형과 SQL 정합.

    게이트웨이(convertPattern)는 선언 밖 메타 키를 **조용히 버린다**(게시 지연이 잘 돌던
    패턴을 죽이면 안 되므로). 그 관용은 런타임의 것이고, **저작 시점(CI)은 시끄럽게** 잡는다 —
    여기서 걸리는 키는 오타이거나 SQL 파라미터 개명 후 미갱신이다.
    """
    findings: list[Finding] = []

    def add(rule: str, message: str) -> None:
        findings.append(Finding(rule, model.name, message, model.source))

    sql = pattern.get("sql")
    declared: set[str] = set()
    if isinstance(sql, str):
        body = re.sub(r"--[^\n]*", "", sql)
        body = re.sub(r"/\*.*?\*/", "", body, flags=re.S)
        declared = {m.group(1) for m in re.finditer(r":([a-z_][a-z0-9_]*)", body, re.I)}

    def check_keys(field: str, mapping: Any) -> dict[str, Any]:
        if not isinstance(mapping, dict):
            add("usage_pattern_invalid", f"{label} — '{field}' 는 매핑이어야 한다")
            return {}
        for key in sorted(set(mapping) - declared):
            add("usage_pattern_param_meta_unknown",
                f"{label} — '{field}' 의 '{key}' 는 SQL 의 :파라미터에 없다(오타/개명 미반영)")
        return mapping

    if "param_defaults" in pattern:
        for key, value in check_keys("param_defaults", pattern["param_defaults"]).items():
            if _is_relative_default(value):           # v1.12 동적(상대 날짜) 기본값
                err = _relative_default_error(value)
                if err:
                    add("usage_pattern_invalid", f"{label} — param_defaults['{key}'] 상대 날짜 — {err}")
            elif not _scalar(value):
                add("usage_pattern_invalid", f"{label} — param_defaults['{key}'] 는 스칼라 또는 상대 날짜{{rel,as}}여야 한다")

    if "param_enum" in pattern:
        for key, value in check_keys("param_enum", pattern["param_enum"]).items():
            if not isinstance(value, list) or not value or not all(_scalar(v) for v in value):
                add("usage_pattern_invalid", f"{label} — param_enum['{key}'] 는 스칼라 리스트(1개 이상)여야 한다")

    if "params" in pattern:
        types = set(spec.get("param_spec_types", ["string", "number", "array"]))
        items = set(spec.get("param_spec_items", ["string", "number"]))
        cap = int(spec.get("param_spec_max_len_cap", 100))
        for key, value in check_keys("params", pattern["params"]).items():
            if not isinstance(value, dict):
                add("usage_pattern_invalid", f"{label} — params['{key}'] 는 매핑이어야 한다")
                continue
            for extra_key in sorted(set(value) - {"type", "item", "max_len"}):
                add("usage_pattern_invalid", f"{label} — params['{key}'] 의 '{extra_key}' 는 스펙 밖 키다")
            if value.get("type") not in types:
                add("usage_pattern_invalid", f"{label} — params['{key}'].type 은 {sorted(types)} 중 하나여야 한다")
            if "item" in value and value["item"] not in items:
                add("usage_pattern_invalid", f"{label} — params['{key}'].item 은 {sorted(items)} 중 하나여야 한다")
            if "max_len" in value and (
                isinstance(value["max_len"], bool) or not isinstance(value["max_len"], int)
                or not (1 <= value["max_len"] <= cap)
            ):
                add("usage_pattern_invalid", f"{label} — params['{key}'].max_len 은 1..{cap} 정수여야 한다")

    # 기본값이 허용값 밖이면 게이트웨이가 실행 시 400 을 낸다 — 저작 시점에 잡는다.
    defaults = pattern.get("param_defaults")
    enums = pattern.get("param_enum")
    if isinstance(defaults, dict) and isinstance(enums, dict):
        for key, value in defaults.items():
            if _is_relative_default(value):    # 상대 날짜는 enum 대상이 아니다(날짜 축엔 enum 없음)
                continue
            allow = enums.get(key)
            if isinstance(allow, list) and allow and not any(str(a) == str(value) for a in allow):
                add("usage_pattern_invalid",
                    f"{label} — param_defaults['{key}']={value!r} 가 param_enum 허용값 밖이다")

    return findings


# v1.13 (#217 후속): export 시점 자동검증 완결성 — 미검증 패턴(verified_at 없음)은 각 도메인
# gold→D1 export 가 SQL 을 실 D1 에 돌려 통과분에 verified_at 을 찍어야 runnable=true 가 된다
# (dags common/serving/pattern_verify.verify_and_stamp). 그 검증은 SQL 주석·힌트의 **예시값**으로
# 파라미터를 바인딩한다. 예시가 안 풀리는 :param 이 하나라도 있으면 export 가 그 패턴을 건너뛰어
# 영구 미검증 → 게이트웨이가 실행 시 409("카탈로그에서 가져올 수 없는 항목")로 막는다. 저작 시점에
# 잡는다. **해석 규약은 pattern_verify.resolve_params 와 잠금 — 한쪽을 고치면 다른 쪽도 같이 고친다.**
_PATTERN_PLACEHOLDER_RE = re.compile(r":([a-z][a-z0-9_]*)")


def _unresolved_example_params(sql: str, hint: str) -> list[str]:
    """resolve_params(pattern_verify) 와 동일 규약으로, 예시값이 안 풀리는 :param 이름들을 반환."""
    stripped = re.sub(r"/\*.*?\*/", " ", sql or "")
    executable = "\n".join(re.sub(r"--.*$", "", line) for line in stripped.splitlines())
    names = sorted(set(_PATTERN_PLACEHOLDER_RE.findall(executable)), key=len, reverse=True)
    unresolved: list[str] = []
    for name in names:
        found = False
        for source in (sql or "", hint or ""):
            for m in re.finditer(rf":{name}(?![a-z0-9_])", source):
                rest = source[m.end():]
                nl = rest.find("\n")
                tail = (rest if nl < 0 else rest[:nl])[:600]
                # 예시값은 `:이름=값` 꼴 — **`=` 앵커 필수**(ASAC-DAG#756/#763 pattern_verify
                # 와 규약 잠금). `=` 없는 관용 탐색을 남기면 게이트는 풀리는데 export 검증은
                # 못 푸는 드리프트(→ 영구 409)가 생긴다. 값 전체를 원자적으로 읽는다.
                # (해석 '가능 여부'만 보므로 숫자/문자 구분은 값 존재 판정에 영향 없음 —
                #  #763 의 숫자 경계 규칙은 실행측(verify_stamp)의 값 추출에 반영된다.)
                em = re.match(r"\s*=\s*", tail)
                if not em:
                    continue
                rv = tail[em.end():]
                if rv.startswith("'"):
                    found = bool(re.match(r"'(?:[^']|'')*'", rv))
                elif rv.startswith("["):
                    found = bool(re.match(r"\[[^\]\n]*\]", rv))
                else:
                    found = bool(rv.split(",")[0].strip())
                if found:
                    break
            if found:
                break
        if not found:
            unresolved.append(name)
    return unresolved


def _check_pattern_verifiability(model: ServingModel, pattern: dict[str, Any], label: str) -> list[Finding]:
    """미검증 패턴이 export 자동검증으로 runnable 이 될 수 있는지(예시값이 다 풀리는지) 검사."""
    if pattern.get("verified_at"):          # 이미 손 검증됨 — export 무관하게 runnable
        return []
    sql = pattern.get("sql")
    if not isinstance(sql, str):
        return []                            # 타입 위반은 required 검사가 이미 보고
    hint = " ".join(str(pattern.get(k) or "") for k in ("question_ko", "axes", "insight_sample_ko"))
    missing = _unresolved_example_params(sql, hint)
    if not missing:
        return []
    return [Finding(
        "usage_pattern_unverifiable_example", model.name,
        f"{label} — 미검증(verified_at 없음) 패턴인데 예시값이 없는 :파라미터 {missing} — export 자동검증이 "
        f"건너뛰어 영구 미검증→게이트웨이 409. SQL 주석에 예시(`-- :{missing[0]}=…`)를 넣거나 손 검증하라",
        model.source,
    )]


def _is_public_https_url(value: Any) -> bool:
    """Source/licence URLs must be public HTTPS references, never credentials in disguise."""
    if not isinstance(value, str) or not value.strip():
        return False
    parsed = urlparse(value)
    return parsed.scheme == "https" and bool(parsed.netloc) and not parsed.username and not parsed.password


def _check_source_evidence(model: ServingModel, schema: dict[str, Any]) -> list[Finding]:
    """Validate #678 source/right declarations before a Publisher can make them visible."""
    findings: list[Finding] = []
    sources = model.serving.get("source_evidence")
    spec = schema.get("source_evidence_fields") or {}
    if sources is None or not spec:
        return findings
    if not isinstance(sources, list):
        return findings  # optional type violation is already emitted by the structural loop

    def add(rule: str, message: str) -> None:
        findings.append(Finding(rule, model.name, message, model.source))

    required = tuple(spec.get("required") or ())
    known = set(required)
    allowed_redistribution = set(spec.get("redistribution_allowed") or ())
    seen_source_ids: set[str] = set()
    if not sources:
        add("source_evidence_invalid", "source_evidence 를 선언하면 최소 한 source record가 필요하다")
        return findings

    for index, source in enumerate(sources):
        label = f"source_evidence[{index}]"
        if not isinstance(source, dict):
            add("source_evidence_invalid", f"{label} 은 매핑이어야 하는데 {type(source).__name__}")
            continue
        for field in sorted(set(source) - known):
            add("source_evidence_unknown_field", f"{label} — 스펙 밖 필드 '{field}' (오타 확인)")
        missing = [field for field in required if field not in source]
        if missing:
            add("source_evidence_invalid", f"{label} — 필수 필드 누락: {missing}")

        source_id = source.get("source_id")
        if not isinstance(source_id, str) or not IDENTIFIER_RE.fullmatch(source_id):
            add("source_evidence_invalid", f"{label}.source_id 는 식별자여야 한다")
        elif source_id in seen_source_ids:
            add("source_evidence_duplicate", f"{label}.source_id '{source_id}' 가 모델 안에서 중복")
        else:
            seen_source_ids.add(source_id)

        for field in ("source_url", "license_url"):
            if not _is_public_https_url(source.get(field)):
                add("source_evidence_invalid", f"{label}.{field} 는 인증정보 없는 public HTTPS URL이어야 한다")
        for field in ("license", "attribution"):
            if not isinstance(source.get(field), str) or not source[field].strip():
                add("source_evidence_invalid", f"{label}.{field} 는 비어 있지 않은 문자열이어야 한다")
        if source.get("redistribution") not in allowed_redistribution:
            add(
                "source_evidence_invalid",
                f"{label}.redistribution={source.get('redistribution')!r} 은 허용값 {sorted(allowed_redistribution)} 이 아니다",
            )
        checked_at = source.get("rights_checked_at")
        try:
            if not isinstance(checked_at, str):
                raise ValueError("not a string")
            date.fromisoformat(checked_at)
        except ValueError:
            add("source_evidence_invalid", f"{label}.rights_checked_at 은 YYYY-MM-DD ISO 날짜여야 한다")

    return findings


def _check_quality_coverage(model: ServingModel, schema: dict[str, Any]) -> list[Finding]:
    """Validate measured coverage or an explicit reason that coverage has no stable denominator."""
    findings: list[Finding] = []
    coverage = model.serving.get("quality_coverage")
    spec = schema.get("quality_coverage_fields") or {}
    if coverage is None or not spec:
        return findings
    if not isinstance(coverage, dict):
        return findings  # optional type violation is already emitted by the structural loop

    def add(rule: str, message: str) -> None:
        findings.append(Finding(rule, model.name, message, model.source))

    not_applicable_required = set(spec.get("not_applicable_required") or ())
    if set(coverage) == not_applicable_required:
        reason = coverage.get("not_applicable_reason")
        if not isinstance(reason, str) or not reason.strip():
            add("quality_coverage_invalid", "quality_coverage.not_applicable_reason 은 비어 있지 않아야 한다")
        return findings

    required = set(spec.get("measured_required") or ())
    optional = set(spec.get("measured_optional") or ())
    allowed_fields = required | optional
    for field in sorted(set(coverage) - allowed_fields):
        add("quality_coverage_unknown_field", f"quality_coverage — 스펙 밖 필드 '{field}' (오타 확인)")
    missing = sorted(required - set(coverage))
    if missing:
        add("quality_coverage_invalid", f"quality_coverage — 필수 필드 누락: {missing}")

    field = coverage.get("field")
    if not isinstance(field, str) or not IDENTIFIER_RE.fullmatch(field):
        add("quality_coverage_invalid", "quality_coverage.field 는 물리 컬럼 식별자여야 한다")
    elif field not in model.columns:
        add("quality_coverage_invalid", f"quality_coverage.field '{field}' 이 YAML columns 계약에 없다")
    else:
        projection = model.serving.get("public_projection")
        scope = coverage.get("measurement_scope", "published_rows")
        if (
            scope == "published_rows"
            and isinstance(projection, dict)
            and field not in (projection.get("columns") or [])
        ):
            add("quality_coverage_invalid", f"quality_coverage.field '{field}' 은 public_projection에 포함돼야 한다")

    expected = coverage.get("expected_distinct_count")
    if isinstance(expected, bool) or not isinstance(expected, int) or expected < 1:
        add("quality_coverage_invalid", "quality_coverage.expected_distinct_count 는 1 이상 정수여야 한다")
    minimum_ratio = coverage.get("minimum_ratio")
    if (
        isinstance(minimum_ratio, bool)
        or not isinstance(minimum_ratio, (int, float))
        or not 0 < float(minimum_ratio) <= 1
    ):
        add("quality_coverage_invalid", "quality_coverage.minimum_ratio 는 0 초과 1 이하여야 한다")
    scope = coverage.get("measurement_scope", "published_rows")
    allowed_scopes = set(spec.get("measurement_scope_allowed") or ())
    if scope not in allowed_scopes:
        add(
            "quality_coverage_invalid",
            f"quality_coverage.measurement_scope={scope!r} 은 허용값 {sorted(allowed_scopes)} 이 아니다",
        )

    return findings


def _check_semantic(model: ServingModel, manifest: ManifestView) -> list[Finding]:
    findings: list[Finding] = []
    serving = model.serving

    def add(rule: str, message: str) -> None:
        findings.append(Finding(rule, model.name, message, model.source))

    # external=true 인데 enabled 이 true 가 아님.
    if serving.get("external") is True and serving.get("enabled") is not True:
        add("external_enabled_conflict", "external=true 인데 enabled 이 true 가 아니다 (게시 안 되는데 공개 노출)")

    # public catalog retirement is only safe for a contract that is already
    # disabled and non-external. The Publisher treats this as a catalog-only
    # cleanup signal; accepting a live product here could erase a public entry.
    if serving.get("retire_on_publish") is True and (
        serving.get("enabled") is not False or serving.get("external") is not False
    ):
        add(
            "retire_on_publish_invalid",
            "retire_on_publish requires enabled=false and external=false",
        )

    # publication_trigger 는 cron 또는 asset 정확히 하나.
    if "upsert_strategy" in serving and serving.get("publication_mode") != "upsert":
        add("upsert_strategy_invalid", "upsert_strategy requires publication_mode=upsert")

    trigger = serving.get("publication_trigger")
    if isinstance(trigger, dict):
        has_cron = "schedule_cron" in trigger
        has_asset = "trigger_type" in trigger or "max_interval_minutes" in trigger
        if has_cron and has_asset:
            add("publication_trigger_invalid", "publication_trigger 에 cron 과 asset 이 함께 선언됨")
        elif not has_cron and not has_asset:
            add("publication_trigger_invalid", "publication_trigger 에 schedule_cron 또는 (trigger_type+max_interval_minutes) 필요")
        elif has_asset:
            if trigger.get("trigger_type") != "asset":
                add("publication_trigger_invalid", "asset 트리거는 trigger_type: asset 이어야 한다")
            if "max_interval_minutes" not in trigger:
                add("publication_trigger_invalid", "asset 트리거는 max_interval_minutes 를 선언해야 한다")
    elif trigger is not None:
        add("publication_trigger_invalid", "publication_trigger 는 object 이어야 한다")

    # partial_policy.min_publish_ratio 는 0~1.
    partial = serving.get("partial_policy")
    if isinstance(partial, dict):
        ratio = partial.get("min_publish_ratio")
        if ratio is None or not isinstance(ratio, (int, float)) or isinstance(ratio, bool) or not (0 <= ratio <= 1):
            add("partial_policy_invalid", f"partial_policy.min_publish_ratio 는 0~1 이어야 한다 (현재 {ratio!r})")

    # reliability (rollup 전용) — 있으면 3개 키 + 정책 enum.
    reliability = serving.get("reliability")
    if isinstance(reliability, dict):
        for key in ("sample_count_field", "minimum_sample_count", "insufficient_sample_policy"):
            if key not in reliability:
                add("reliability_invalid", f"reliability.{key} 누락")
        policy = reliability.get("insufficient_sample_policy")
        allowed = {"suppress_row", "flag_degraded", "allow"}
        if policy is not None and policy not in allowed:
            add("reliability_invalid", f"insufficient_sample_policy={policy!r} 은 {sorted(allowed)} 이 아니다")

    # primary_key 컬럼 실존 + not_null·고유성 근거.
    _check_primary_key(model, manifest, add)
    _check_freshness_field(model, manifest, add)
    _check_empty_result_freshness(model, manifest, add)
    _check_query_availability(model, manifest, add)
    _check_valid_empty_contract(model, add)
    _check_public_projection(model, manifest, add)
    _check_column_vocabularies(model, add)

    # manifest 멤버십.
    if manifest.supplied and not manifest.has_model(model.name):
        add("model_not_in_manifest", "계약에 선언됐으나 dbt manifest 에 없는 모델")

    return findings


def _column_meta(contract: dict[str, Any]) -> dict[str, Any]:
    config = contract.get("config")
    if not isinstance(config, dict):
        return {}
    meta = config.get("meta")
    return meta if isinstance(meta, dict) else {}


def _check_column_vocabularies(model: ServingModel, add) -> None:
    """Validate optional glossary links and canonical term declarations."""
    for column, contract in model.column_contracts.items():
        if not isinstance(contract, dict):
            continue
        meta = _column_meta(contract)
        vocabulary_id = meta.get("vocabulary_id")
        valid_id = isinstance(vocabulary_id, str) and VOCABULARY_ID_RE.fullmatch(vocabulary_id)
        if vocabulary_id is not None and not valid_id:
            add(
                "column_vocabulary_id_invalid",
                f"column '{column}' vocabulary_id 는 namespace:name 형식이어야 한다",
            )

        terms = meta.get("vocabulary_terms")
        if terms is None:
            continue
        if not valid_id:
            add(
                "column_vocabulary_terms_without_id",
                f"column '{column}' vocabulary_terms 는 유효한 vocabulary_id 와 함께 선언해야 한다",
            )
        if not isinstance(terms, list):
            add("column_vocabulary_term_invalid", f"column '{column}' vocabulary_terms 는 리스트여야 한다")
            continue

        seen_codes: set[str] = set()
        for term in terms:
            if (
                not isinstance(term, dict)
                or set(term) != {"code", "label_ko"}
                or not isinstance(term.get("code"), str)
                or not term["code"].strip()
                or not isinstance(term.get("label_ko"), str)
                or not term["label_ko"].strip()
            ):
                add(
                    "column_vocabulary_term_invalid",
                    f"column '{column}' vocabulary_terms 항목은 비어 있지 않은 code·label_ko 여야 한다",
                )
                continue
            code = term["code"]
            if code in seen_codes:
                add(
                    "column_vocabulary_term_duplicate",
                    f"column '{column}' vocabulary_terms code '{code}' 중복",
                )
            seen_codes.add(code)


def _valid_vocabulary_term_declarations(model: ServingModel) -> list[tuple[str, tuple[tuple[str, str], ...]]]:
    """Return only well-formed term lists so global conflicts stay actionable."""
    declarations: list[tuple[str, tuple[tuple[str, str], ...]]] = []
    for contract in model.column_contracts.values():
        if not isinstance(contract, dict):
            continue
        meta = _column_meta(contract)
        vocabulary_id = meta.get("vocabulary_id")
        terms = meta.get("vocabulary_terms")
        if not (isinstance(vocabulary_id, str) and VOCABULARY_ID_RE.fullmatch(vocabulary_id) and isinstance(terms, list)):
            continue
        parsed: list[tuple[str, str]] = []
        for term in terms:
            if (
                not isinstance(term, dict)
                or set(term) != {"code", "label_ko"}
                or not isinstance(term.get("code"), str)
                or not term["code"].strip()
                or not isinstance(term.get("label_ko"), str)
                or not term["label_ko"].strip()
            ):
                parsed = []
                break
            parsed.append((term["code"], term["label_ko"]))
        if parsed and len({code for code, _ in parsed}) == len(parsed):
            declarations.append((vocabulary_id, tuple(parsed)))
    return declarations


def _check_freshness_field(model: ServingModel, manifest: ManifestView, add) -> None:
    """An explicit quality-time axis must be a real model column."""
    field = model.serving.get("freshness_field")
    if field is None:
        return
    if not isinstance(field, str) or not IDENTIFIER_RE.fullmatch(field):
        add("freshness_field_invalid", "freshness_field 는 물리 컬럼 식별자여야 한다")
        return
    if field not in model.columns:
        add("freshness_field_not_a_column", f"freshness_field '{field}' 이 YAML columns 계약에 없다")
        return
    if manifest.supplied and manifest.has_model(model.name) and field not in manifest.columns(model.name):
        add("freshness_field_not_a_column", f"freshness_field '{field}' 이 dbt manifest 컬럼에 없다")


def _check_empty_result_freshness(model: ServingModel, manifest: ManifestView, add) -> None:
    """Validate the upstream fallback used only when a sparse product has zero rows."""
    raw = model.serving.get("empty_result_freshness")
    if raw is None:
        return
    if not isinstance(raw, dict):
        return  # structural type validation reports this separately
    required = {"relation", "field"}
    if set(raw) != required:
        add(
            "empty_result_freshness_invalid",
            "empty_result_freshness 는 relation 과 field 만 선언해야 한다",
        )
        return
    relation = raw.get("relation")
    field = raw.get("field")
    if not isinstance(relation, str) or not IDENTIFIER_RE.fullmatch(relation):
        add("empty_result_freshness_invalid", "empty_result_freshness.relation 은 dbt model 식별자여야 한다")
        return
    if not isinstance(field, str) or not IDENTIFIER_RE.fullmatch(field):
        add("empty_result_freshness_invalid", "empty_result_freshness.field 는 physical column 식별자여야 한다")
        return
    if relation == model.name:
        add("empty_result_freshness_invalid", "empty_result_freshness.relation 은 현재 희소 상품 자신일 수 없다")
        return
    if manifest.supplied:
        if not manifest.has_model(relation):
            add("empty_result_freshness_invalid", f"empty_result_freshness.relation '{relation}' 이 manifest model에 없다")
        elif field not in manifest.columns(relation):
            add("empty_result_freshness_invalid", f"empty_result_freshness.field '{field}' 이 relation '{relation}' 컬럼에 없다")


def _check_query_availability(model: ServingModel, manifest: ManifestView, add) -> None:
    """Validate the private dbt companion used for filtered-query coverage."""
    raw = model.serving.get("query_availability")
    if raw is None:
        return
    if not isinstance(raw, dict):
        return
    if set(raw) != {"relation"}:
        add(
            "query_availability_invalid",
            "query_availability 는 relation 만 선언해야 한다",
        )
        return

    relation = raw.get("relation")
    if not isinstance(relation, str) or not IDENTIFIER_RE.fullmatch(relation):
        add("query_availability_invalid", "query_availability.relation 은 dbt model 식별자여야 한다")
        return
    if relation == model.name:
        add("query_availability_invalid", "query_availability.relation 은 현재 공개 상품 자신일 수 없다")
        return
    if manifest.supplied and not manifest.has_model(relation):
        add(
            "query_availability_invalid",
            f"query_availability.relation '{relation}' 이 manifest model에 없다",
        )


def _check_valid_empty_contract(model: ServingModel, add) -> None:
    """Public sparse products must publish a Worker-readable valid-empty state."""
    serving = model.serving
    if not (
        serving.get("enabled") is True
        and serving.get("external") is True
        and serving.get("zero_policy") == "allow"
    ):
        return

    missing: list[str] = []
    if not isinstance(serving.get("empty_result_freshness"), dict):
        missing.append("empty_result_freshness")

    projection = serving.get("mcp_projection")
    empty_result = projection.get("empty_result") if isinstance(projection, dict) else None
    if not isinstance(empty_result, dict):
        missing.append("mcp_projection.empty_result")
    elif (
        empty_result.get("state") != "valid_empty"
        or not isinstance(empty_result.get("code"), str)
        or not EMPTY_RESULT_CODE_RE.fullmatch(empty_result["code"])
        or not isinstance(empty_result.get("message_ko"), str)
        or not empty_result["message_ko"].strip()
    ):
        missing.append("mcp_projection.empty_result(valid_empty)")

    if missing:
        add(
            "valid_empty_contract_invalid",
            "enabled=true·external=true·zero_policy=allow 공개 상품은 "
            + ", ".join(missing)
            + " 선언이 필요하다",
        )


def _check_primary_key(model: ServingModel, manifest: ManifestView, add) -> None:
    pk = model.serving.get("primary_key")
    if not isinstance(pk, list) or not pk:
        return  # structural rule already flagged missing/typed primary_key

    available = manifest.columns(model.name) if (manifest.supplied and manifest.has_model(model.name)) else set(model.columns)
    check_columns = bool(available)

    for col in pk:
        if check_columns and col not in available:
            add("primary_key_not_a_column", f"primary_key '{col}' 이 모델 컬럼에 없다")
        if "not_null" not in model.columns.get(col, ()):  # not_null 근거는 yml 컬럼 테스트로 확인
            add("primary_key_evidence_missing", f"primary_key '{col}' 에 not_null 테스트 근거 없음")

    if len(pk) == 1:
        col = pk[0]
        if "unique" not in model.columns.get(col, ()):
            add("primary_key_evidence_missing", f"단일 primary_key '{col}' 에 unique 테스트 근거 없음")
    else:
        combined = any("unique_combination" in t for tests in model.columns.values() for t in tests)
        combined = combined or any("unique_combination" in t for t in model.model_tests)
        if not combined:
            add("primary_key_evidence_missing", f"복합 primary_key {pk} 에 조합 고유성(unique_combination_of_columns) 근거 없음")


def _check_public_projection(model: ServingModel, manifest: ManifestView, add) -> None:
    projection = model.serving.get("public_projection")
    if projection is None:
        if model.serving.get("public_primary_key") is not None:
            add("public_projection_invalid", "public_primary_key 는 public_projection 과 함께 선언해야 한다")
        return
    if not isinstance(projection, dict):
        add("public_projection_invalid", "public_projection 은 object 이어야 한다")
        return

    if set(projection) != {"schema_version", "columns"}:
        add("public_projection_invalid", "public_projection 은 schema_version 과 columns 만 선언해야 한다")

    schema_version = projection.get("schema_version")
    if not isinstance(schema_version, str) or not SEMVER_RE.fullmatch(schema_version):
        add("public_projection_invalid", "public_projection.schema_version 은 MAJOR.MINOR.PATCH 형식이어야 한다")

    columns = projection.get("columns")
    if not isinstance(columns, list) or not columns:
        add("public_projection_invalid", "public_projection.columns 는 비어 있지 않은 리스트여야 한다")
        return

    seen: set[str] = set()
    available = manifest.columns(model.name) if (manifest.supplied and manifest.has_model(model.name)) else set(model.columns)
    check_columns = bool(available)

    for column in columns:
        if not isinstance(column, str) or not IDENTIFIER_RE.fullmatch(column):
            add("public_projection_invalid", f"public_projection column {column!r} 은 물리 컬럼 식별자여야 한다")
            continue
        if column in seen:
            add("public_projection_invalid", f"public_projection column '{column}' 중복")
        seen.add(column)
        lowered = column.lower()
        if any(part in lowered for part in INTERNAL_PUBLIC_FIELD_PARTS):
            add("public_projection_internal_field", f"public_projection column '{column}' 은 내부/비밀 식별자로 공개할 수 없다")
        if check_columns and column not in available:
            add("public_projection_unknown_column", f"public_projection column '{column}' 이 모델 컬럼에 없다")
        if column not in model.columns:
            add("public_projection_unknown_column", f"public_projection column '{column}' 이 YAML columns 계약에 없다")
        else:
            _check_projected_column_metadata(model, column, add)

    public_primary_key = model.serving.get("public_primary_key")
    if public_primary_key is not None:
        if (
            not isinstance(public_primary_key, list)
            or not public_primary_key
            or any(not isinstance(column, str) or not IDENTIFIER_RE.fullmatch(column) for column in public_primary_key)
            or len(set(public_primary_key)) != len(public_primary_key)
        ):
            add("public_projection_invalid", "public_primary_key 는 중복 없는 물리 컬럼 식별자 리스트여야 한다")
            public_primary_key = []
    required_columns = list(public_primary_key or model.serving.get("primary_key") or [])
    if isinstance(model.serving.get("event_time"), str):
        required_columns.append(model.serving["event_time"])
    if isinstance(model.serving.get("freshness_field"), str):
        required_columns.append(model.serving["freshness_field"])
    reliability = model.serving.get("reliability")
    if isinstance(reliability, dict) and isinstance(reliability.get("sample_count_field"), str):
        required_columns.append(reliability["sample_count_field"])

    projected = set(c for c in columns if isinstance(c, str))
    for required_column in required_columns:
        if required_column not in projected:
            add("public_projection_required_field_missing", f"public_projection 에 필수 컬럼 '{required_column}' 누락")


def _check_projected_column_metadata(model: ServingModel, column: str, add) -> None:
    contract = model.column_contracts.get(column) or {}
    meta = ((contract.get("config") or {}).get("meta") or {}) if isinstance(contract, dict) else {}
    required_fields = {
        "description": contract.get("description"),
        "data_type": contract.get("data_type"),
        "semantic_role": meta.get("semantic_role"),
        "nullable": meta.get("nullable") if isinstance(meta.get("nullable"), bool) else None,
        "null_meaning": meta.get("null_meaning"),
        "unit": meta.get("unit"),
    }
    missing = [field for field, value in required_fields.items() if value in (None, "")]
    if missing:
        add("public_projection_column_metadata_missing", f"public_projection column '{column}' 메타데이터 누락: {missing}")
    if "not_null" in model.columns.get(column, ()) and meta.get("nullable") is True:
        add(
            "public_projection_nullability_conflict",
            f"public_projection column '{column}' 은 not_null 테스트와 nullable=true 를 함께 선언할 수 없다",
        )


def _check_global(models: list[ServingModel]) -> list[Finding]:
    findings: list[Finding] = []
    seen: dict[str, ServingModel] = {}
    vocabulary_terms: dict[str, tuple[tuple[str, str], ...]] = {}
    for model in models:
        pid = model.serving.get("product_id")
        if not isinstance(pid, str) or not pid:
            continue
        if pid in seen:
            findings.append(
                Finding(
                    "product_id_duplicate",
                    model.name,
                    f"product_id '{pid}' 가 '{seen[pid].name}' 와 중복 (전역 유일 위반)",
                    model.source,
                )
            )
        else:
            seen[pid] = model
        for vocabulary_id, terms in _valid_vocabulary_term_declarations(model):
            previous = vocabulary_terms.get(vocabulary_id)
            if previous is not None and previous != terms:
                findings.append(
                    Finding(
                        "column_vocabulary_terms_conflict",
                        model.name,
                        f"vocabulary_id '{vocabulary_id}' 의 vocabulary_terms 선언이 기존 선언과 다르다",
                        model.source,
                    )
                )
            else:
                vocabulary_terms[vocabulary_id] = terms
    return findings


def validate(models: list[ServingModel], manifest: ManifestView | None = None, schema: dict[str, Any] | None = None) -> ValidationResult:
    """Run all serving-contract rules and return a deterministic result."""
    manifest = manifest or ManifestView(supplied=False)
    schema = schema or load_schema()

    findings: list[Finding] = []
    for model in models:
        findings.extend(_check_structural(model, schema))
        findings.extend(_check_semantic(model, manifest))
    findings.extend(_check_global(models))

    findings.sort(key=lambda f: (f.source, f.model, f.rule, f.message))
    return ValidationResult(findings=findings, models_checked=len(models))
