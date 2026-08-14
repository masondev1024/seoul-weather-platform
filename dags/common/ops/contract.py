"""ops 기록 **단일 관문** — 전 도메인이 여기만 통과하면 저장소·운영기록 규약을 지킨다.

정본: ASK-Seoul#78 「저장소·운영 기록 적용 규약 v1」. 이 모듈은 그 규약 중 **기계가 강제할 수
있는 전부**를 코드로 옮긴 것이다. 규약 문서를 다시 읽지 않아도, 이 관문을 통과하면 경로·형식·
값 집합·보안이 동시에 만족된다.

관문이 강제하는 것
------------------
1. **경로는 만들 수 없고 요청만 할 수 있다** (`ops_key`). 문자열로 경로를 조립하는 길을 없애
   카테고리를 오타내거나 용도에 안 맞는 존에 쓰는 사고를 구조적으로 차단한다.
   - 관측 계열 → ``ops/<category>/<domain>/observed_date=<KST>/…``  (P-4·P-6·P-9)
   - control  → ``ops/control/{state,checkpoints,queues}/<domain>/…`` (하위유형이 도메인 앞)
   - 그 밖의 상태 계열 → ``ops/<category>/<domain>/…`` (날짜 칸 금지)  (P-5)
2. **선택지는 미리 좁혀져 있다.** category/layer/grain/status/rows_source/sink_type 은 전부
   닫힌 집합(enum)이라, 없는 값을 쓰면 그 자리에서 죽는다 (R-1·V-1·V-4·V-5·N-5).
3. **필수 항목이 빠지면 즉시 실패한다** (`build_ops_event`). 무엇이 어느 규칙 때문에 필요한지
   메시지에 적어 되돌려준다 — 나중에 조회 DB 에서 발견되는 대신 쓰는 순간 알게 한다.
4. **모른다 ≠ 0.** ``row_count=None`` 은 반드시 ``rows_source='not_observed'`` 와 짝이고,
   측정한 값에 ``not_observed`` 를 붙일 수 없다 (F-3·F-6·N-5).
5. **URL 을 기록에 담을 수 없다.** ``api_name`` 에 URL 형태가 오면 거부한다 (X-1) — 서울 열린
   데이터 API 는 인증키를 URL 경로에 싣기 때문에 이게 곧 시크릿 유출이다.
6. **날짜 축을 두 개 싣는다.** ``observed_date_kst``(기록 내용 기준·정본)와
   ``source_path_date``(그 기록이 실제 저장된 경로의 날짜 칸)를 함께 남긴다 (F-5·G-2).
7. **환경을 기록에 남긴다.** ``environment`` 는 인자로 받거나 런타임 타깃에서 뽑는다 (Z-7).

쓰는 법
-------
    from common.ops import OpsCategory, Grain, Layer, RunStatus, emit_ops_event

    emit_ops_event(
        OpsCategory.RUNS, domain="commerce", layer=Layer.RAW, grain=Grain.AIRFLOW_TASK,
        status=RunStatus.SUCCESS, dag_id="commerce_collect_raw", task_id="ingest",
        run_id=run_id, try_number=1, is_final_try=True,
        row_count=19377, rows_source=RowsSource.RAW_MANIFEST,
    )

기존 기록기(`run_sink`·`runmetrics`·`errors.sink`·`product_observability`)는 이 관문보다 먼저
있었고 각자 경로·형식이 다르다. 신규 배선은 전부 이 관문을 쓰고, 기존 것은 소유 도메인이
전환한다(#78 §13 G-1 — 기존 객체 이동 0건, 신규 쓰기부터).

stdlib only. Airflow 무의존(단위테스트 대상).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from enum import StrEnum
from typing import Any, Mapping, Sequence

LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = "ops-record/v1"
KST = timezone(timedelta(hours=9))

OPS_ROOT = "ops"


class OpsContractError(ValueError):
    """관문 위반 — 무엇이 왜 잘못됐는지 메시지에 담는다(쓰는 순간의 즉시 피드백)."""


# ── 닫힌 집합 (선택지를 미리 좁힌다) ────────────────────────────────────────────────

class OpsCategory(StrEnum):
    """ops 카테고리 10종 — 닫힌 집합(R-1). 늘리려면 규약 문서 표에 줄을 먼저 추가한다."""

    RUNS = "runs"
    METRICS = "metrics"
    ERRORS = "errors"
    LOGS = "logs"
    REPORTS = "reports"
    RECOVERY = "recovery"
    PRODUCT_EVENTS = "product-events"
    PRODUCT_HEALTH = "product-health"
    CONTROL = "control"
    RECEIPTS = "receipts"


#: 관측 계열 — 날짜 파티션 필수(P-4), 보관 기간 있음(R-1).
OBSERVATION_CATEGORIES: frozenset[OpsCategory] = frozenset({
    OpsCategory.RUNS, OpsCategory.METRICS, OpsCategory.ERRORS, OpsCategory.LOGS,
    OpsCategory.REPORTS, OpsCategory.RECOVERY, OpsCategory.PRODUCT_EVENTS,
    OpsCategory.PRODUCT_HEALTH,
})
#: 상태 계열 — 날짜 파티션 금지(P-5), 자동 삭제 금지(R-4).
STATE_CATEGORIES: frozenset[OpsCategory] = frozenset({
    OpsCategory.CONTROL, OpsCategory.RECEIPTS,
})


class ControlSubtype(StrEnum):
    """``ops/control/`` 하위유형 — 닫힌 집합(#78 §1 존 구조표).

    구조표가 ``control/{state,checkpoints,queues}/<domain>/`` 이라 하위유형이 **도메인보다
    앞**에 온다. 다른 카테고리(`ops/<category>/<domain>/`)와 모양이 다른 유일한 자리다.
    """

    STATE = "state"
    CHECKPOINTS = "checkpoints"
    QUEUES = "queues"
#: 보관 기간(일). None = 만료 금지(R-3·R-4). 자동 삭제 규칙은 ops 루트가 아니라
#: 반드시 이 카테고리 단위로만 건다(R-2).
RETENTION_DAYS: dict[OpsCategory, int | None] = {
    OpsCategory.RUNS: 400, OpsCategory.METRICS: 400, OpsCategory.ERRORS: 180,
    OpsCategory.LOGS: 30, OpsCategory.REPORTS: 180, OpsCategory.RECOVERY: 180,
    OpsCategory.PRODUCT_EVENTS: 400, OpsCategory.PRODUCT_HEALTH: 400,
    OpsCategory.CONTROL: None, OpsCategory.RECEIPTS: None,
}
#: 기록 1건이 아니라 **텍스트 압축본**을 담는 카테고리 — 조회 DB 행으로 만들지 않고,
#: 같은 (dag_id, run_id) 실행 기록에 포인터로 붙인다(ingest.attach_log_bundles).
BLOB_CATEGORIES: frozenset[OpsCategory] = frozenset({OpsCategory.LOGS})


class Layer(StrEnum):
    """단계(V-4) — silver 포함. 공통 모듈에 silver 가 없어 정제 기록이 튕기던 결함(§16 정정 2)."""

    RAW = "raw"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    D1 = "d1"


class Grain(StrEnum):
    """기록 단위(V-5) — 무엇 1건이 한 행인가. runs 와 metrics 가 같은 것을 두 번 세던
    중복(V-6)은 폴더가 아니라 이 값으로 가른다."""

    AIRFLOW_TASK = "airflow_task"
    DBT_NODE = "dbt_node"
    PRODUCT_TRANSITION = "product_transition"
    PRODUCT_HEALTH = "product_health"
    PUBLICATION = "publication"
    CONTROL_STATE = "control_state"


class RunStatus(StrEnum):
    """실행 기록의 status(V-1). R2 완결 확인서(V-2)·Iceberg lifecycle(V-3)과 **다른 집합**이다."""

    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    DEGRADED = "degraded"


class ManifestStatus(StrEnum):
    """R2 완결 확인서의 status(V-2). 미완결은 값이 아니라 **확인서 부재**로 표현한다(M-2)."""

    COMPLETE = "complete"
    COMPLETE_WITH_VIOLATIONS = "complete_with_violations"


class RowsSource(StrEnum):
    """행 수를 어디서 얻었는지(N-1~N-5). ``row_count`` 와 반드시 병기한다."""

    RAW_MANIFEST = "raw_manifest"
    BRONZE_RUN_MANIFEST = "bronze_run_manifest"
    ICEBERG_SNAPSHOT = "iceberg_snapshot"
    COUNT_QUERY = "count_query"
    PUBLICATION_LEDGER = "publication_ledger"
    NOT_OBSERVED = "not_observed"


class SinkType(StrEnum):
    """목적지 종류(F 표 목적지 묶음)."""

    FILE = "file"
    TABLE = "table"
    DB = "db"


class Environment(StrEnum):
    """개발/운영은 버킷으로 분리하고 기록 안에 환경 값을 남긴다(Z-7)."""

    DEV = "dev"
    PROD = "prod"


# ── 값 검증기 ─────────────────────────────────────────────────────────────────────

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_UNSAFE_CHARS = re.compile(r"[^A-Za-z0-9._=-]")
# X-1: 기록에 URL 을 담지 않는다. 스킴이 있거나 슬래시가 있으면 API '이름'이 아니다.
_URLISH = re.compile(r"(://|^/|\?|&|%[0-9A-Fa-f]{2})")

#: grain 별 추가 필수 항목 — 없으면 그 기록은 나중에 아무 축으로도 못 묶인다.
_REQUIRED_BY_GRAIN: dict[Grain, tuple[str, ...]] = {
    Grain.AIRFLOW_TASK: ("dag_id", "task_id", "run_id"),
    Grain.DBT_NODE: ("task_id", "run_id"),
    Grain.PRODUCT_TRANSITION: ("dag_id", "run_id"),
    Grain.PRODUCT_HEALTH: ("dag_id", "run_id"),
    Grain.PUBLICATION: ("publication_id",),
    Grain.CONTROL_STATE: ("dag_id",),
}

#: 기록 1건의 항목(F 표). **빼거나 이름을 바꾸지 않는다 — 추가만 한다**(F-1·D-3).
RECORD_FIELDS: tuple[str, ...] = (
    # 식별
    "schema_version", "event_id", "domain", "layer", "grain",
    "dag_id", "task_id", "run_id", "try_number", "is_final_try", "environment",
    # 시각
    "observed_at", "started_at", "ended_at", "duration_s", "duration_hms",
    "observed_date_kst", "source_path_date", "schedule_delay_s",
    # 양
    "row_count", "rows_source", "bytes",
    # 출처 (URL 은 싣지 않는다 — X-1)
    "api_name", "api_call_count", "retry_count", "failure_count",
    # 목적지
    "sink_type", "sink_target",
    # 결과
    "status", "error_ref", "quality", "publication_id", "product_id", "product_ids",
    # 대조 (저장소↔DB 를 event_id 로 맞추되, 어느 파일에서 왔는지도 남긴다 — C-4)
    "source_category", "source_key", "log_bundle_key",
)

#: `event_id` 를 만들 때 쓰는 식별 항목 — `product_observability` 와 **같은 규칙**(sha256 of
#: 정렬 JSON)이라, 이미 event_id 를 가진 기록은 그대로 쓰고 없는 것만 여기서 파생한다.
_IDENTITY_FIELDS: tuple[str, ...] = (
    "domain", "layer", "grain", "dag_id", "task_id", "run_id", "try_number",
    "product_id", "publication_id",
)


def _fail(message: str) -> None:
    raise OpsContractError(message)


def coerce_category(value: Any) -> OpsCategory:
    try:
        return OpsCategory(str(value))
    except ValueError:
        _fail(f"ops 카테고리가 닫힌 집합(R-1) 밖입니다: {value!r} — 허용: "
              + ", ".join(sorted(c.value for c in OpsCategory)))
    raise AssertionError  # pragma: no cover - _fail 이 항상 raise


def _coerce(enum_cls: type[StrEnum], value: Any, *, field: str, rule: str) -> Any:
    try:
        return enum_cls(str(value))
    except ValueError:
        _fail(f"{field} 가 닫힌 집합({rule}) 밖입니다: {value!r} — 허용: "
              + ", ".join(sorted(m.value for m in enum_cls)))
    raise AssertionError  # pragma: no cover


def assert_iso_date(value: Any, *, field: str) -> str:
    text = str(value)
    if not _ISO_DATE.match(text):
        _fail(f"{field} 는 YYYY-MM-DD 여야 합니다(P-1·P-4): {value!r}")
    return text


def assert_domain(value: Any) -> str:
    text = str(value or "").strip()
    if not text or not _SAFE_SEGMENT.match(text):
        _fail(f"domain 은 경로 세그먼트로 안전한 이름이어야 합니다: {value!r}")
    return text


def safe_segment(value: Any, *, default: str = "unknown") -> str:
    """오브젝트 키 세그먼트 안전화 — 기존 sink 들과 같은 규약(`[^A-Za-z0-9._=-]` → '-')."""
    text = str(value) if value is not None else ""
    return _UNSAFE_CHARS.sub("-", text)[:200] if text else default


def resolve_environment(env: Mapping[str, str] | None = None) -> Environment:
    """기록에 실을 환경 값(Z-7). ``DBT_TARGET``/``ASK_SEOUL_TARGET`` 을 따르고, 둘이
    엇갈리면 거부한다 — 조용히 한쪽을 고르면 개발 기록이 운영 지표에 섞인다."""
    values = os.environ if env is None else env
    seen = {
        str(values.get(name, "")).strip().lower()
        for name in ("DBT_TARGET", "ASK_SEOUL_TARGET")
        if str(values.get(name, "")).strip()
    }
    if not seen:
        _fail("환경을 정할 수 없습니다(Z-7) — DBT_TARGET 또는 ASK_SEOUL_TARGET 을 dev|prod 로 설정")
    if len(seen) > 1:
        _fail(f"환경 값이 엇갈립니다(Z-7): {sorted(seen)} — DBT_TARGET 과 ASK_SEOUL_TARGET 을 맞추세요")
    return _coerce(Environment, seen.pop(), field="environment", rule="Z-7")


# ── 경로: 만들 수 없고 요청만 할 수 있다 ────────────────────────────────────────────

def ops_key(category: OpsCategory | str, *, domain: str, filename: str,
            observed_date_kst: str | date | None = None,
            control: ControlSubtype | str | None = None,
            subpath: Sequence[str] = ()) -> str:
    """ops 오브젝트 키 — **이 함수 밖에서 ops 경로를 문자열로 조립하지 않는다.**

    관측 계열 → ``ops/<category>/<domain>/observed_date=<KST>/[subpath/]<filename>``
      (P-4 날짜 칸 이름 · P-6 카테고리 우선 · P-9 도메인은 인자)
    control  → ``ops/control/<subtype>/<domain>/[subpath/]<filename>``
      (#78 §1 — 하위유형이 도메인보다 앞. 날짜 칸은 금지 P-5)
    그 밖의 상태 계열 → ``ops/<category>/<domain>/[subpath/]<filename>``
      (P-5 — 최신본만 의미가 있어 날짜 파티션이 무의미하다)

    용도에 안 맞는 조합은 거부한다: 관측인데 날짜가 없거나, 상태인데 날짜를 주거나,
    control 인데 하위유형이 없는 경우.
    """
    cat = coerce_category(category)
    dom = assert_domain(domain)
    if not str(filename or "").strip():
        _fail("filename 이 비었습니다")
    if cat is OpsCategory.CONTROL:
        if control is None:
            _fail("control 은 하위유형이 필수입니다(#78 §1) — "
                  f"{', '.join(m.value for m in ControlSubtype)} 중 하나를 주세요.")
        sub = _coerce(ControlSubtype, control, field="control", rule="#78 §1")
        parts = [OPS_ROOT, cat.value, sub.value, dom]
    elif control is not None:
        _fail(f"control 하위유형은 '{OpsCategory.CONTROL.value}' 에만 씁니다: "
              f"category='{cat.value}'")
    else:
        parts = [OPS_ROOT, cat.value, dom]
    if cat in OBSERVATION_CATEGORIES:
        if observed_date_kst is None:
            _fail(f"관측 계열 '{cat.value}' 은 observed_date_kst 가 필수입니다(P-4). "
                  "기록 내용의 시각을 KST 로 접은 날짜를 주세요.")
        iso = observed_date_kst.isoformat() if isinstance(observed_date_kst, date) else str(observed_date_kst)
        parts.append(f"observed_date={assert_iso_date(iso, field='observed_date_kst')}")
    elif observed_date_kst is not None:
        _fail(f"상태 계열 '{cat.value}' 에는 날짜 경로를 두지 않습니다(P-5) — "
              "최신본만 의미가 있어 날짜 파티션이 관측 공백을 만듭니다.")
    parts.extend(safe_segment(segment) for segment in subpath if str(segment).strip())
    parts.append(safe_segment(filename))
    return "/".join(parts)


def legacy_observation_key(category: OpsCategory | str, *, domain: str, date: str,
                           subpath: Sequence[str] = (), filename: str) -> str:
    """전환 전 경로(``load_date=``) — **읽기 전용**. 이미 그 이름으로 올라간 것을 찾을 때만 쓴다.

    전환은 신규 쓰기부터이고(G-1) 읽는 쪽은 과도기 동안 신·구 양쪽을 본다(G-4). 이 함수가
    관문 안에 있는 이유는, 밖에 두면 경로 문자열을 손으로 조립하는 코드가 다시 생기기 때문이다
    (그걸 막는 검사가 `common/tests/test_ops_gate_enforcement.py` 다).

    **새로 쓸 때 쓰면 안 된다** — 쓰기는 :func:`ops_key` 만 쓴다.
    """
    cat = coerce_category(category)
    if cat not in OBSERVATION_CATEGORIES:
        _fail(f"'{cat.value}' 은 관측 계열이 아니라 날짜 칸 자체가 없습니다(P-5)")
    parts = [OPS_ROOT, cat.value, assert_domain(domain),
             f"load_date={assert_iso_date(date, field='date')}"]
    parts.extend(safe_segment(segment) for segment in subpath if str(segment).strip())
    parts.append(safe_segment(filename))
    return "/".join(parts)


def category_prefix(category: OpsCategory | str, *, domain: str | None = None,
                    control: ControlSubtype | str | None = None) -> str:
    """스캔용 접두 — 카테고리 전체 또는 한 도메인. 소비자(적재기·점검)가 쓴다(P-6).

    control 은 하위유형이 도메인보다 앞이라(#78 §1), 도메인만으로는 접두를 만들 수 없다 —
    ``ops/control/`` 전체를 훑거나 하위유형을 함께 지정한다.
    """
    cat = coerce_category(category)
    if cat is OpsCategory.CONTROL and control is not None:
        sub = _coerce(ControlSubtype, control, field="control", rule="#78 §1")
        base = f"{OPS_ROOT}/{cat.value}/{sub.value}/"
        return base if domain is None else f"{base}{assert_domain(domain)}/"
    if cat is OpsCategory.CONTROL and domain is not None:
        _fail("control 은 하위유형이 도메인보다 앞입니다(#78 §1) — "
              "domain 만으로는 접두를 만들 수 없습니다. control= 을 함께 주세요.")
    if control is not None:
        _fail(f"control 하위유형은 '{OpsCategory.CONTROL.value}' 에만 씁니다: "
              f"category='{cat.value}'")
    if domain is None:
        return f"{OPS_ROOT}/{cat.value}/"
    return f"{OPS_ROOT}/{cat.value}/{assert_domain(domain)}/"


# ── 기록 1건 ──────────────────────────────────────────────────────────────────────

def event_id_for(identity: Mapping[str, Any]) -> str:
    """식별 항목의 sha256 — `product_observability` 와 같은 규칙(정렬 JSON, ASCII)."""
    payload = {key: identity.get(key) for key in _IDENTITY_FIELDS}
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _as_utc(value: Any, *, field: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            _fail(f"{field} 를 ISO8601 시각으로 읽을 수 없습니다: {value!r}")
            raise AssertionError  # pragma: no cover
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _hms(seconds: float | None) -> str | None:
    if seconds is None:
        return None
    total = int(round(float(seconds)))
    sign = "-" if total < 0 else ""
    total = abs(total)
    return f"{sign}{total // 3600:02d}:{(total % 3600) // 60:02d}:{total % 60:02d}"


def _validate_rows(row_count: Any, rows_source: RowsSource) -> int | None:
    """모른다(NULL) 와 실제로 없다(0) 를 섞지 않는다(F-3·N-5)."""
    if row_count is None:
        if rows_source is not RowsSource.NOT_OBSERVED:
            _fail("row_count=None 은 rows_source='not_observed' 와만 짝입니다(F-3·N-5) — "
                  f"받은 값: {rows_source.value!r}. 측정했다면 실제 수치를, 못 했다면 "
                  "not_observed 를 주세요.")
        return None
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
        _fail(f"row_count 는 0 이상의 정수이거나 None 이어야 합니다: {row_count!r}")
    if rows_source is RowsSource.NOT_OBSERVED:
        _fail("측정한 row_count 에는 근거(rows_source)가 필요합니다(N-5) — "
              "not_observed 는 값을 못 얻었을 때만 씁니다.")
    return int(row_count)


def _validate_api_name(value: Any) -> str | None:
    """X-1 — URL 은 기록에 남기지 않는다. 정규화된 API 이름만(예: ``seoul.citydata``)."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if _URLISH.search(text):
        _fail("api_name 에 URL 을 담을 수 없습니다(X-1) — 서울 열린데이터 API 는 인증키를 "
              f"URL 경로에 싣기 때문에 그대로 저장하면 시크릿이 남습니다. 받은 값: {text[:40]!r}. "
              "'seoul.citydata' 같은 정규화된 이름을 주세요.")
    return text


def build_ops_event(
    category: OpsCategory | str,
    *,
    domain: str,
    layer: Layer | str,
    grain: Grain | str,
    status: RunStatus | str,
    environment: Environment | str | None = None,
    dag_id: str | None = None,
    task_id: str | None = None,
    run_id: str | None = None,
    try_number: int | None = None,
    is_final_try: bool | None = None,
    observed_at: datetime | str | None = None,
    started_at: datetime | str | None = None,
    ended_at: datetime | str | None = None,
    duration_s: float | None = None,
    schedule_delay_s: float | None = None,
    source_path_date: str | None = None,
    row_count: int | None = None,
    rows_source: RowsSource | str = RowsSource.NOT_OBSERVED,
    bytes_: int | None = None,
    api_name: str | None = None,
    api_call_count: int | None = None,
    retry_count: int | None = None,
    failure_count: int | None = None,
    sink_type: SinkType | str | None = None,
    sink_target: str | None = None,
    error_ref: str | None = None,
    quality: Mapping[str, Any] | None = None,
    publication_id: str | None = None,
    product_ids: Sequence[str] = (),
    event_id: str | None = None,
    source_key: str | None = None,
    log_bundle_key: str | None = None,
) -> dict[str, Any]:
    """검증된 기록 1건(F 표) — 필수 항목이 빠지면 그 자리에서 :class:`OpsContractError`.

    ``event_id`` 를 주면 그대로 쓴다(원천이 이미 발급한 식별자는 바꾸지 않는다). 없으면
    식별 항목에서 파생한다 — 같은 시도(try)면 같은 값이라 재적재가 자연스럽게 멱등해진다.
    """
    cat = coerce_category(category)
    if cat in STATE_CATEGORIES:
        _fail(f"'{cat.value}' 은 상태 계열이라 실행 기록으로 쓰지 않습니다(R-4) — "
              "다음 실행의 동작을 바꾸는 값이지 로그가 아닙니다.")
    dom = assert_domain(domain)
    lyr = _coerce(Layer, layer, field="layer", rule="V-4")
    grn = _coerce(Grain, grain, field="grain", rule="V-5")
    sts = _coerce(RunStatus, status, field="status", rule="V-1")
    rsrc = _coerce(RowsSource, rows_source, field="rows_source", rule="N-5")
    env = (_coerce(Environment, environment, field="environment", rule="Z-7")
           if environment is not None else resolve_environment())
    sink = _coerce(SinkType, sink_type, field="sink_type", rule="F 목적지") if sink_type is not None else None

    values = {"dag_id": dag_id, "task_id": task_id, "run_id": run_id,
              "publication_id": publication_id}
    missing = [name for name in _REQUIRED_BY_GRAIN[grn] if not str(values.get(name) or "").strip()]
    if missing:
        _fail(f"grain='{grn.value}' 기록에는 {', '.join(missing)} 가 필수입니다 — "
              "없으면 이 행은 조회 DB 에서 어떤 축으로도 묶이지 않습니다.")
    if grn is Grain.PRODUCT_TRANSITION and not product_ids:
        _fail("grain='product_transition' 에는 product_ids 가 필요합니다 — 어떤 제품이 "
              "어느 단계로 넘어갔는지가 이 기록의 내용입니다.")

    observed = _as_utc(observed_at, field="observed_at") or _as_utc(ended_at, field="ended_at") \
        or _as_utc(started_at, field="started_at") or datetime.now(timezone.utc)
    start = _as_utc(started_at, field="started_at")
    end = _as_utc(ended_at, field="ended_at")
    if duration_s is None and start is not None and end is not None:
        duration_s = (end - start).total_seconds()

    normalized_products = sorted({str(pid) for pid in product_ids})
    identity = {
        "domain": dom, "layer": lyr.value, "grain": grn.value, "dag_id": dag_id,
        "task_id": task_id, "run_id": run_id, "try_number": try_number,
        "product_id": (normalized_products[0] if len(normalized_products) == 1
                       else "|".join(normalized_products) or None),
        "publication_id": publication_id,
    }
    record: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "event_id": str(event_id) if event_id else event_id_for(identity),
        "domain": dom,
        "layer": lyr.value,
        "grain": grn.value,
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "try_number": try_number,
        "is_final_try": None if is_final_try is None else bool(is_final_try),
        "environment": env.value,
        "observed_at": observed.isoformat(),
        "started_at": start.isoformat() if start else None,
        "ended_at": end.isoformat() if end else None,
        "duration_s": round(float(duration_s), 3) if duration_s is not None else None,
        "duration_hms": _hms(duration_s),
        # F-5: 정본은 기록 내용 기준 KST 날짜. 경로 날짜는 저장소↔DB 대조 전용으로 따로 싣는다.
        "observed_date_kst": observed.astimezone(KST).date().isoformat(),
        "source_path_date": (assert_iso_date(source_path_date, field="source_path_date")
                             if source_path_date else None),
        "schedule_delay_s": (round(float(schedule_delay_s), 3)
                             if schedule_delay_s is not None else None),
        "row_count": _validate_rows(row_count, rsrc),
        "rows_source": rsrc.value,
        "bytes": int(bytes_) if bytes_ is not None else None,
        "api_name": _validate_api_name(api_name),
        "api_call_count": int(api_call_count) if api_call_count is not None else None,
        "retry_count": int(retry_count) if retry_count is not None else None,
        "failure_count": int(failure_count) if failure_count is not None else None,
        "sink_type": sink.value if sink else None,
        "sink_target": str(sink_target) if sink_target else None,
        "status": sts.value,
        "error_ref": str(error_ref) if error_ref else None,
        "quality": dict(quality or {}),
        "publication_id": publication_id,
        "product_id": identity["product_id"],
        "product_ids": normalized_products,
        "source_category": cat.value,
        "source_key": str(source_key) if source_key else None,
        "log_bundle_key": str(log_bundle_key) if log_bundle_key else None,
    }
    return {field: record.get(field) for field in RECORD_FIELDS}


def event_object_key(record: Mapping[str, Any]) -> str:
    """기록 1건의 저장 위치 — 관문 경로 규칙 그대로. 파일명은 event_id 라 재적재가 덮어쓴다."""
    return ops_key(
        record["source_category"],
        domain=record["domain"],
        observed_date_kst=record["observed_date_kst"],
        filename=f"event_id={record['event_id']}.json",
        subpath=(f"dag_id={safe_segment(record.get('dag_id'), default='none')}",),
    )


# ── 쓰기 (C-1 저장소가 원본, C-2 조회 DB 는 곁들여 · 실패 무시) ──────────────────────

def _redact(payload: dict[str, Any]) -> dict[str, Any]:
    """저장 직전 마스킹(X-2). 공용 redaction 이 없으면 원본을 그대로 둔다(fail-open)."""
    try:
        from common.security import redact, refresh_env_secrets

        refresh_env_secrets()
        return redact(payload)
    except Exception:  # noqa: BLE001 - 마스킹 모듈 부재가 기록을 막지 않는다
        LOGGER.warning("[ops.contract] redaction 사용 불가 — 원본 저장")
        return payload


def emit_ops_event(category: OpsCategory | str, *, d1_writer: Any | None = None,
                   **fields: Any) -> dict[str, Any]:
    """관문 통과 → 저장소에 기록(C-1) → 같은 자리에서 조회 DB 한 줄(C-2, 실패 무시).

    검증 실패는 **던진다**(관문의 존재 이유). 저장·적재 실패는 본 작업을 죽이지 않는다 —
    기록 저장 때문에 파이프라인이 멈추면 관측이 장애의 원인이 된다.
    """
    record = build_ops_event(category, **fields)
    object_key = event_object_key(record)
    payload = json.dumps(_redact(dict(record)), ensure_ascii=False, sort_keys=True).encode("utf-8")
    try:
        from common.ops.run_sink import _put_r2

        _put_r2(object_key, payload, target=record["environment"])
        LOGGER.info("[ops.%s] %s", record["source_category"], object_key)
    except Exception as exc:  # noqa: BLE001 - 관측 실패가 본 작업을 죽이지 않는다(C-2)
        LOGGER.warning("[ops.contract] 저장 실패(무시): %s", type(exc).__name__)
    if d1_writer is not None:
        try:
            d1_writer(record)
        except Exception as exc:  # noqa: BLE001 - C-2 "실패해도 무시한다"
            LOGGER.warning("[ops.contract] 조회 DB 적재 실패(무시): %s", type(exc).__name__)
    return record
