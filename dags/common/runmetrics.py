"""실행 메트릭 로그 (#188) — bronze(Airflow 태스크런)·silver(dbt 모델)의
소요시간·메모리·처리량을 **평평한 JSON 레코드**로 남긴다.

저장은 임시(R2 sink) — 나중에 DB 로 sink 교체 전제. 레코드 스키마는 DB 테이블로
그대로 이관 가능한 평평한 구조(이슈 #188 확정, `_RECORD_FIELDS` 가 유일 출처).

설계 요지:
- `track(layer, domain)` — PythonOperator python_callable 을 감싸는 데코레이터.
  Airflow context 에서 dag_id/task_id/run_id/try_number/data_interval_end 를 뽑고,
  wall clock + getrusage 로 시간·메모리·CPU 를, HttpCore 카운터로 api_calls·retries 를,
  콜러블 반환 dict 에서 rows/bytes/skipped 를 측정한다.
- HttpCore(#78) 카운터: 모듈 레벨 contextvar 컬렉터. track 이 활성화하는 동안에만
  HttpCore 가 요청/재시도를 집계한다(비활성 기본은 no-op — 기존 도메인 무영향).
- sink(기본 R2, #77 errors sink 동형): 오브젝트 키
  `ops/metrics/<domain>/observed_date=YYYY-MM-DD/<dag_id>__<task_id>__<run_id>__try<N>.json`
  (날짜 칸은 **KST** — ASK-Seoul#78 P-4. 레코드 안의 시각 필드는 UTC 그대로다).
  버킷·자격은 canonical `R2_*` 한 세트(#647/`0739845`) — **키 이름이 배포 환경을 담지
  않고 값이 정한다.** 배포 하나가 버킷 하나를 가리키므로 자격으로 환경을 고를 여지가 없다.
  ASAC_METRICS_DIR 설정 시 로컬 파일 sink(디버그) — 선택 로직은 `resolve_sink()`.
  sink 를 분리해 나중에 DB sink 로 교체 가능(write(record) 계약).
- 실패 안전: 메트릭 기록 실패는 **본 태스크를 절대 실패시키지 않는다**(경고 로그만).
  콜러블 예외는 status=failed 레코드를 남긴 뒤 **재던진다**(태스크 실패 의미 보존).
"""
from __future__ import annotations

import contextlib
import contextvars
import functools
import inspect
import json
import logging
import os
import re
import socket
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

# resource 는 POSIX 전용 — Windows 로컬 테스트에서는 부재. 안전 폴백(측정 필드 null).
try:  # pragma: no cover - 플랫폼 분기
    import resource as _resource
except ImportError:  # pragma: no cover
    _resource = None  # type: ignore[assignment]

LOGGER = logging.getLogger(__name__)

# ── 레코드 스키마 (이슈 #188 확정 — 변경 금지) ────────────────────────────────────
# 이 순서/키가 유일 출처. 값 없는 필드는 null. DB 이관 시 컬럼과 1:1.
_RECORD_FIELDS: tuple[str, ...] = (
    "layer", "domain", "dag_id", "task_id", "run_id", "try_number",
    "started_at", "finished_at", "duration_s", "schedule_delay_s",
    "peak_rss_mb", "cpu_time_s", "rows", "bytes", "api_calls", "retries",
    "status", "skipped", "target", "hostname", "error_id",
)

# 오브젝트/파일명 세그먼트에 남길 문자 — 이 밖은 전부 '-' 로 치환(errors/sink.py 규약과 동일).
_UNSAFE_SEGMENT_CHARS = re.compile(r"[^A-Za-z0-9._=-]")

# 관측 계열 경로의 날짜 칸 기준(P-4). 레코드 값은 UTC 유지 — 경로만 이 기준으로 접는다.
_KST = timezone(timedelta(hours=9))


def _blank_record() -> dict[str, Any]:
    """모든 스키마 키를 null 로 채운 레코드 — 어떤 경로든 키 누락이 없게 한다."""
    return {field: None for field in _RECORD_FIELDS}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None) -> str | None:
    """UTC ISO8601(KST 아님 — DB 이관용). tz 없는 값은 UTC 로 간주."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _resolve_target(explicit: str | None = None) -> str:
    """target(dev|prod) — 기존 도메인 관례 ASK_SEOUL_TARGET/DBT_TARGET 폴백."""
    if explicit:
        return explicit
    return os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod"))


def _safe_segment(value: str | None) -> str:
    if not value:
        return "unknown"
    return _UNSAFE_SEGMENT_CHARS.sub("-", str(value))


# ── HttpCore(#78) 카운터 — 모듈 레벨 contextvar 컬렉터 ────────────────────────────
# HttpCore 는 컬렉터가 활성일 때만 note_http_* 로 요청/재시도를 집계한다(비활성 기본 no-op).
class _HttpCounter:
    __slots__ = ("api_calls", "retries")

    def __init__(self) -> None:
        self.api_calls = 0
        self.retries = 0


_http_counter: contextvars.ContextVar[_HttpCounter | None] = contextvars.ContextVar(
    "asac_http_counter", default=None
)


def note_http_request() -> None:
    """HttpCore 요청 1회 — 컬렉터 활성 시에만 api_calls 증가(비활성이면 no-op)."""
    counter = _http_counter.get()
    if counter is not None:
        counter.api_calls += 1


def note_http_retry() -> None:
    """HttpCore 재시도 1회 — 컬렉터 활성 시에만 retries 증가(비활성이면 no-op)."""
    counter = _http_counter.get()
    if counter is not None:
        counter.retries += 1


@contextlib.contextmanager
def collect_http_metrics():
    """이 컨텍스트 동안 HttpCore 호출/재시도를 집계한다(track 이 내부에서 사용)."""
    counter = _HttpCounter()
    token = _http_counter.set(counter)
    try:
        yield counter
    finally:
        _http_counter.reset(token)


# ── 리소스 측정 (getrusage) ──────────────────────────────────────────────────────
def _read_rusage() -> Any | None:
    if _resource is None:
        return None
    try:
        return _resource.getrusage(_resource.RUSAGE_SELF)
    except Exception:  # noqa: BLE001 - 측정 실패는 무해(null 폴백)
        return None


def _maxrss_to_mb(ru_maxrss: int) -> float:
    """ru_maxrss → MB. Linux 는 KB, macOS 는 bytes 단위(플랫폼 주의)."""
    if sys.platform == "darwin":
        return ru_maxrss / (1024 * 1024)
    return ru_maxrss / 1024  # Linux/일반: KB


def _measure_resources(start: Any | None, end: Any | None) -> tuple[float | None, float | None]:
    """(peak_rss_mb, cpu_time_s). Airflow 태스크당 프로세스라 RUSAGE_SELF 가 태스크 근사.

    peak_rss_mb 는 프로세스 high-water mark(ru_maxrss)의 종료 시점 값,
    cpu_time_s 는 태스크 구간 delta(utime+stime).
    """
    if end is None:
        return None, None
    peak_rss_mb = _maxrss_to_mb(end.ru_maxrss)
    end_cpu = end.ru_utime + end.ru_stime
    start_cpu = (start.ru_utime + start.ru_stime) if start is not None else 0.0
    cpu_time_s = max(end_cpu - start_cpu, 0.0)
    return peak_rss_mb, cpu_time_s


# ── 콜러블 반환값 → rows/bytes/skipped 매핑 ──────────────────────────────────────
def _extract_payload(result: Any) -> tuple[int | None, int | None, bool]:
    """반환 dict 에서 rows/bytes/skipped 를 관례 키로 뽑는다.

    규칙(합리적 매핑):
      - rows   : `rows` 우선, 없으면 `inserted`(적재 행수)를 rows 로 사용.
      - bytes  : `bytes`.
      - skipped: `skipped`(bool). 멱등 skip 태스크(예: master load)의 플래그.
    dict 가 아니거나 키가 없으면 해당 필드는 null/False.
    """
    if not isinstance(result, dict):
        return None, None, False
    rows = result.get("rows")
    if rows is None:
        rows = result.get("inserted")
    bytes_ = result.get("bytes")
    skipped = bool(result.get("skipped", False))
    return (
        rows if isinstance(rows, int) else None,
        bytes_ if isinstance(bytes_, int) else None,
        skipped,
    )


# ── Airflow context 추출 ─────────────────────────────────────────────────────────
def _extract_context(context: dict[str, Any]) -> dict[str, Any]:
    """errors/airflow.py 와 같은 방식으로 dag_id/task_id/run_id/try_number 를 뽑는다."""
    ti = context.get("task_instance") or context.get("ti")
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    dag_id = (getattr(dag, "dag_id", None) or getattr(ti, "dag_id", None)
              or getattr(dag_run, "dag_id", None))
    run_id = context.get("run_id") or getattr(dag_run, "run_id", None)
    try_number = getattr(ti, "try_number", None)
    return {
        "dag_id": dag_id,
        "task_id": getattr(ti, "task_id", None),
        "run_id": run_id,
        "try_number": try_number if isinstance(try_number, int) else None,
        "data_interval_end": context.get("data_interval_end"),
    }


def _schedule_delay_s(started_at: datetime, data_interval_end: Any) -> float | None:
    """started_at - data_interval_end. 없거나 음수면 null(manual run 등)."""
    if data_interval_end is None:
        return None
    try:
        # pendulum.DateTime 도 datetime 서브클래스 — total_seconds 사용.
        end = data_interval_end
        if getattr(end, "tzinfo", None) is None:
            end = end.replace(tzinfo=timezone.utc)
        delay = (started_at - end).total_seconds()
    except Exception:  # noqa: BLE001 - 타입 이상 시 조용히 null
        return None
    return delay if delay >= 0 else None


# ── sink (기본 R2 → 추후 DB 교체 / 파일 sink 는 테스트·로컬 디버그) ───────────────
def _record_date(record: dict[str, Any]) -> str:
    """경로 날짜 칸(P-4) — 레코드 시각을 **KST 로 접은** 날짜.

    레코드 안의 started_at/finished_at 은 UTC ISO8601 그대로 둔다(#188 DB 이관 계약).
    조회 DB 의 정본 날짜도 기록 내용을 KST 로 접은 값이라(common.ops.ingest), 경로만
    KST 로 맞추면 저장소↔DB 대조에서 하루씩 어긋나던 것이 사라진다.
    시각을 못 읽으면 지금 시각으로 접는다 — 기록을 버리지 않는다.
    """
    started = record.get("started_at") or _iso(_utc_now())
    try:
        moment = datetime.fromisoformat(str(started))
    except ValueError:
        moment = _utc_now()
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(_KST).date().isoformat()


def _record_object_name(record: dict[str, Any]) -> str:
    """레코드 → 파일/오브젝트 이름. run_id 의 `:` `+` 등 예약문자는 '-' 치환.

    - bronze(dag 있음): <dag_id>__<task_id>__<run_id>__try<N>.json
    - silver(dag 없음): <task_id(모델명)>__<run_id(dbt invocation)>.json
    """
    task = _safe_segment(record.get("task_id"))
    run = _safe_segment(record.get("run_id"))
    if record.get("dag_id") is None:
        return f"{task}__{run}.json"
    try_n = record.get("try_number")
    try_part = f"try{try_n}" if try_n is not None else "try"
    return f"{_safe_segment(record.get('dag_id'))}__{task}__{run}__{try_part}.json"


class MetricsFileSink:
    """레코드를 로컬 `date=.../<...>.json` 파일로 저장 — 단위 테스트·로컬 디버그 용도.

    운영 기본은 MetricsR2Sink(아래). ASAC_METRICS_DIR 설정 시에만 이 sink 가 선택된다.
    """

    def __init__(self, root: Path | None = None) -> None:
        self._root = root

    def _root_dir(self) -> Path:
        if self._root is not None:
            return self._root
        # dags/common/runmetrics.py → parents[1] = dags 루트.
        return Path(__file__).resolve().parents[1] / "metrics"

    def write(self, record: dict[str, Any]) -> Path:
        target_dir = self._root_dir() / f"date={_record_date(record)}"
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / _record_object_name(record)
        payload = json.dumps(record, ensure_ascii=False, sort_keys=False, indent=2)
        # 원자적 쓰기(동시 태스크 충돌 없음 — 파일명이 태스크런 유일).
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(path)
        return path


class MetricsR2Sink:
    """레코드를 R2 에 per-record JSON 으로 적재 — #77 errors sink(R2ErrorSink)와 동형 패턴.

    - 버킷/자격은 errors sink 와 같은 env 규약: R2_ENDPOINT/R2_ACCESS_KEY_ID/
      R2_SECRET_ACCESS_KEY/R2_BUCKET_NAME — **canonical 한 세트뿐이다.**
      키 이름으로 환경을 고르는 분기(`R2_DEV_*` 우선)는 `0739845`(#647)에서 삭제됐다:
      호스트가 ENV2 개편에서 그 키를 없앴고, 분기를 남겨 두면 누가 `R2_DEV_*` 를 채우는
      순간 같은 날짜 기록이 두 버킷으로 갈리기 때문이다(ASK-Seoul#78 `Z-7`).
      **되살리지 말 것** — 환경을 바꾸려면 키 이름이 아니라 값을 바꾼다.
    - 오브젝트 키: ops/metrics/<domain>/observed_date=YYYY-MM-DD/<이름>.json (#60/#573)
      (이름 규약은 _record_object_name — bronze 는 dag/task/run/try, silver 는 모델/invocation).
    - put_object 주입은 테스트용(가짜 클라이언트) — 미지정 시 boto3 지연 임포트.
    - 나중에 DB sink 교체 시 write(record) 계약만 유지하면 된다.
    """

    def __init__(self, *, prefix: str | None = None,
                 put_object: Callable[[str, bytes], None] | None = None) -> None:
        # ops 존 관측 카테고리(ASK-Seoul#60/#573) — ops/metrics/<domain>/observed_date=…/…
        self.prefix = prefix if prefix is not None else os.environ.get(
            "ASAC_METRICS_PREFIX", "ops/metrics")
        self._put_object = put_object

    def object_key(self, record: dict[str, Any]) -> str:
        # 도메인이 카테고리 다음 bare 세그먼트, 날짜 키는 observed_date= 로 통일(#60).
        return (
            f"{self.prefix}/{_safe_segment(record.get('domain'))}"
            f"/observed_date={_record_date(record)}"
            f"/{_record_object_name(record)}"
        )

    def write(self, record: dict[str, Any]) -> str:
        object_key = self.object_key(record)
        payload = json.dumps(record, ensure_ascii=False, sort_keys=False).encode("utf-8")
        if self._put_object is not None:
            self._put_object(object_key, payload)
        else:
            self._put_r2_object(object_key, payload)
        return object_key

    @staticmethod
    def _put_r2_object(object_key: str, payload: bytes) -> None:
        # 이 fork 에는 ops/metrics 를 읽는 소비자가 없다(조회 DB 적재 DAG 미이관). 소비자
        # 없는 기록을 영구 적재하지 않도록 **PUT 만** 건너뛴다 — 레코드 파싱·반환은 그대로라
        # dump_dbt_run_results 의 반환값(태스크 rows)이 바뀌지 않는다. 주입 sink(put_object)
        # 와 ASAC_METRICS_DIR 파일 sink 는 명시적 선택이므로 잠그지 않는다.
        # 경계와 되살리는 법: common.ops.telemetry_switch.
        from common.ops.telemetry_switch import OPS_TELEMETRY_ENV, ops_telemetry_enabled

        if not ops_telemetry_enabled():
            LOGGER.info("[ops] telemetry off (%s 미설정) — 기록 생략: %s",
                        OPS_TELEMETRY_ENV, object_key)
            return

        import boto3

        # R2 자격 env 규약은 common.storage.r2_env 단일 출처(#230) — errors/admin_dong 과 공유.
        from common.storage import r2_env

        boto3.client(
            "s3",
            endpoint_url=r2_env("R2_ENDPOINT"),
            aws_access_key_id=r2_env("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=r2_env("R2_SECRET_ACCESS_KEY"),
            region_name="auto",
        ).put_object(
            Bucket=r2_env("R2_BUCKET_NAME"),
            Key=object_key,
            Body=payload,
            ContentType="application/json; charset=utf-8",
        )


def resolve_sink() -> MetricsFileSink | MetricsR2Sink:
    """기본 sink 선택(한 곳) — ASAC_METRICS_DIR 설정 시 파일(로컬 디버그), 기본은 R2."""
    override = os.environ.get("ASAC_METRICS_DIR")
    if override:
        return MetricsFileSink(root=Path(override))
    return MetricsR2Sink()


def write_record(record: dict[str, Any], *,
                 sink: MetricsFileSink | MetricsR2Sink | None = None) -> None:
    """레코드 저장 — 기록 실패(R2 자격 없음·PUT 실패 포함)는 절대 본 태스크를
    실패시키지 않는다(경고 로그만)."""
    try:
        (sink or resolve_sink()).write(record)
    except Exception as exc:  # noqa: BLE001
        # 예외 메시지는 찍지 않는다(경로/자격 값 노출 가능) — 타입명만.
        LOGGER.warning("실행 메트릭 기록 실패(무해 — 태스크 계속): %s", type(exc).__name__)


# ── track 데코레이터 ─────────────────────────────────────────────────────────────
def track(layer: str, domain: str, *, target: str | None = None,
          sink: MetricsFileSink | MetricsR2Sink | None = None) -> Callable[[Callable], Callable]:
    """PythonOperator python_callable 을 감싸 실행 메트릭을 기록한다.

    래퍼는 Airflow context(**kwargs)를 받도록 노출한다 — 그래야 dag_id/run_id 등을
    뽑을 수 있다. 원 콜러블에는 그 시그니처가 받는 인자만 골라 전달한다
    (예: `f(spec_key, **context)` 는 전부, `f()` 는 없음).

    ⚠️ functools.wraps 를 쓰지 않는다 — __wrapped__ 를 남기면 Airflow 의
    determine_kwargs(inspect.signature)가 원 시그니처를 보고 context 를 주입하지 않는다.
    """
    def decorator(fn: Callable) -> Callable:
        def wrapper(*args: Any, **context: Any) -> Any:
            return _run_tracked(fn, layer, domain, args, context,
                                target=target, sink=sink)

        # Airflow/로그 가독성용 이름만 복사(__wrapped__ 는 의도적으로 남기지 않음).
        wrapper.__name__ = getattr(fn, "__name__", "tracked")
        wrapper.__doc__ = getattr(fn, "__doc__", None)
        return wrapper

    return decorator


def _call_original(fn: Callable, args: tuple, context: dict[str, Any]) -> Any:
    """원 콜러블이 받는 인자만 골라 호출(가변 kwargs 있으면 전부 전달)."""
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return fn(*args, **context)
    if any(p.kind == p.VAR_KEYWORD for p in params.values()):
        return fn(*args, **context)
    accepted = {k: v for k, v in context.items() if k in params}
    return fn(*args, **accepted)


def _run_tracked(fn: Callable, layer: str, domain: str, args: tuple,
                 context: dict[str, Any], *, target: str | None,
                 sink: MetricsFileSink | MetricsR2Sink | None) -> Any:
    ctx = _extract_context(context)
    record = _blank_record()
    record.update(
        layer=layer,
        domain=domain,
        dag_id=ctx["dag_id"],
        task_id=ctx["task_id"],
        run_id=ctx["run_id"],
        try_number=ctx["try_number"],
        target=_resolve_target(target),
        hostname=socket.gethostname(),
        error_id=None,  # #77 연계는 2단계 — 현재 null 고정.
        api_calls=0,
        retries=0,
    )

    started_at = _utc_now()
    record["started_at"] = _iso(started_at)
    record["schedule_delay_s"] = _schedule_delay_s(started_at, ctx["data_interval_end"])
    rusage_start = _read_rusage()
    wall_start = time.monotonic()
    http_counter = None  # with __enter__ 전 예외 대비(그 땐 None 으로 남음)

    try:
        with collect_http_metrics() as http_counter:
            result = _call_original(fn, args, context)
    except BaseException as exc:  # noqa: BLE001 - 실패도 기록 후 재던짐
        # 실패 런도 재시도를 소진했으면 그 호출/재시도 카운트가 의미 있다 —
        # http_counter 를 전달한다(#230 A4). 과거엔 None 이라 api_calls/retries 가
        # 정작 중요한 실패 행에서 0 으로 공백이었다.
        _finalize(record, wall_start, rusage_start, http_counter=http_counter,
                  status="failed", skipped=False, result=None)
        write_record(record, sink=sink)
        raise exc

    rows, bytes_, skipped = _extract_payload(result)
    status = "skipped" if skipped else "success"
    record["rows"] = rows
    record["bytes"] = bytes_
    _finalize(record, wall_start, rusage_start, http_counter=http_counter,
              status=status, skipped=skipped, result=result)
    write_record(record, sink=sink)
    return result


def _finalize(record: dict[str, Any], wall_start: float, rusage_start: Any | None,
              *, http_counter: _HttpCounter | None, status: str, skipped: bool,
              result: Any) -> None:
    finished_at = _utc_now()
    record["finished_at"] = _iso(finished_at)
    record["duration_s"] = time.monotonic() - wall_start
    peak_rss_mb, cpu_time_s = _measure_resources(rusage_start, _read_rusage())
    record["peak_rss_mb"] = peak_rss_mb
    record["cpu_time_s"] = cpu_time_s
    record["status"] = status
    record["skipped"] = skipped
    if http_counter is not None:
        record["api_calls"] = http_counter.api_calls
        record["retries"] = http_counter.retries


# ── silver: dbt run_results.json 파서 ────────────────────────────────────────────
def _dbt_status(dbt_status: str | None) -> tuple[str, bool]:
    """dbt status → (status, skipped). success/pass → success, skipped → skipped, 그 외 failed."""
    s = (dbt_status or "").lower()
    if s in ("success", "pass"):
        return "success", False
    if s == "skipped":
        return "skipped", True
    # error / fail / runtime error / warn 등 — warn 은 실패 아님이나 안전상 success 로.
    if s == "warn":
        return "success", False
    return "failed", False


def _dbt_timing(timings: Iterable[dict[str, Any]] | None, name: str) -> tuple[str | None, str | None]:
    for t in timings or []:
        if t.get("name") == name:
            return t.get("started_at"), t.get("completed_at")
    return None, None


def parse_dbt_run_results(path: str | Path, domain: str, *,
                          target: str | None = None) -> list[dict[str, Any]]:
    """dbt `run_results.json` → 실행 메트릭 레코드 리스트(layer=silver, task_id=모델명).

    - task_id : unique_id 의 모델명(마지막 세그먼트).
    - run_id  : dbt invocation_id(메타).
    - rows    : adapter_response.rows_affected (음수는 N/A → null).
    - duration_s : execution_time. started/finished 는 execute 타이밍(없으면 compile).
    - 메모리·CPU·api·retries·schedule_delay·dag_id·try_number·error_id 는 silver 에서 null.
    """
    document = json.loads(Path(path).read_text(encoding="utf-8"))
    invocation_id = (document.get("metadata") or {}).get("invocation_id")
    resolved_target = _resolve_target(target)
    host = socket.gethostname()

    records: list[dict[str, Any]] = []
    for result in document.get("results", []):
        unique_id = result.get("unique_id") or ""
        model = unique_id.split(".")[-1] if unique_id else None
        status, skipped = _dbt_status(result.get("status"))

        started_at, completed_at = _dbt_timing(result.get("timing"), "execute")
        if started_at is None:
            started_at, completed_at = _dbt_timing(result.get("timing"), "compile")

        adapter = result.get("adapter_response") or {}
        rows_affected = adapter.get("rows_affected")
        rows = rows_affected if isinstance(rows_affected, int) and rows_affected >= 0 else None

        record = _blank_record()
        record.update(
            layer="silver",
            domain=domain,
            task_id=model,
            run_id=invocation_id,
            started_at=started_at,
            finished_at=completed_at,
            duration_s=result.get("execution_time"),
            rows=rows,
            status=status,
            skipped=skipped,
            target=resolved_target,
            hostname=host,
            error_id=None,
        )
        records.append(record)
    return records


def dump_dbt_run_results(path: str | Path, domain: str, *, target: str | None = None,
                         sink: MetricsFileSink | MetricsR2Sink | None = None) -> list[dict[str, Any]]:
    """parse_dbt_run_results 결과를 같은 파일 sink 로 저장하고 레코드를 돌려준다."""
    records = parse_dbt_run_results(path, domain, target=target)
    for record in records:
        write_record(record, sink=sink)
    return records
