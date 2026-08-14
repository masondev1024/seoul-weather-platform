"""Problem → R2 적재 (경로 규약 + 저장 직전 redaction) (#77).

경로 규약(ops 존 도메인-우선, per-error JSON — R2 는 append 불가·에러 발생량이
적어 rolling JSONL 불채택. #60/#573 에서 루트 errors/ 날짜-우선에서 전환):

    ops/errors/<domain>/observed_date=YYYY-MM-DD/dag_id=<dag_id>/
        <run_id>__<HHMMSSffffff>_<type-slug>.json

- observed_date 는 occurred_at 을 **KST 로 접은 날짜**(ASK-Seoul#78 P-4). 파일명의
  시각도 같은 KST 기준으로 맞춰 날짜 칸과 기준이 갈리지 않게 한다 — 섞이면 KST
  자정~09시 사건이 그 날 파티션 안에서 실제보다 늦은 시각으로 읽힌다.
  (파일명은 `<run_id>__<HHMMSSffffff>_…` 라 사전식 정렬의 첫 키는 run_id 다.
  시각은 한 run 안의 순서만 가른다 — 파티션 전체가 시간순으로 정렬되지는 않는다.)
  문서 본문의 occurred_at 은 UTC 원본 그대로다 — 경로만 KST 로 접는다.
- run_id 의 `:` `+` 등 예약문자는 `-` 로 정규화한다(경로 안전화 — 원본
  run_id 는 문서 본문에 보존되므로 손실 없음).
- 자격증명은 canonical `R2_*` 한 세트 — 키 이름이 아니라 값이 환경을 정한다(#647).
- DB 저장(DbSink)은 추후 결정 — 이 모듈에 sink 를 추가하는 자리만 남겨둔다.
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import timedelta, timezone
from typing import Any, Callable

from common.errors.problem import Problem
from common.security import redact, refresh_env_secrets

LOGGER = logging.getLogger(__name__)

# ops 존 관측 카테고리(ASK-Seoul#60) — ops/errors/<domain>/observed_date=…/…
# 구경로(루트 errors/)는 신규 쓰기 중단, R2 소비자(reader) 부재 실측으로 dual-read 없음(#573).
DEFAULT_PREFIX = os.environ.get("ASAC_ERRORS_PREFIX", "ops/errors")

# 관측 계열 경로의 날짜 칸 기준(P-4) — 형제 모듈(errors/airflow·ops/run_sink)과 같은 상수.
_KST = timezone(timedelta(hours=9))

# 오브젝트 키 세그먼트에 남길 문자 — 이 밖은 전부 '-' 로 치환.
_UNSAFE_SEGMENT_CHARS = re.compile(r"[^A-Za-z0-9._=-]")


def _safe_segment(value: str | None) -> str:
    if not value:
        return "unknown"
    return _UNSAFE_SEGMENT_CHARS.sub("-", value)


def _r2_env(name: str) -> str:
    """R2 자격증명 — 전 도메인 단일 규약(#230): common.storage.r2_env 로 위임.

    과거엔 is_dev_target(ASK_SEOUL_TARGET) 게이팅이라, R2_DEV_* 만 세팅하고 타깃을
    미설정(기본 prod)한 dev 박스에서 errors/metrics 가 미설정 prod R2_* 를 읽어
    RuntimeError → 콜백이 삼켜 조용히 유실됐다. admin_dong(raw 랜딩)과 동일한
    '존재 우선' 규약으로 통일해 같은 버킷으로 일관되게 간다.
    """
    from common.storage import r2_env

    return r2_env(name)


def build_object_key(problem: Problem, *, prefix: str = DEFAULT_PREFIX) -> str:
    # 도메인이 카테고리 바로 다음의 bare 세그먼트(#60 A 구조) — 날짜는 그 아래.
    # 날짜·시각 모두 KST(P-4) — 한 파티션 안에서 파일명 정렬이 곧 시간순이 된다.
    occurred_kst = problem.occurred_at.astimezone(_KST)
    observed_date = occurred_kst.date().isoformat()
    occurred = occurred_kst.strftime("%H%M%S%f")
    return (
        f"{prefix}/{_safe_segment(problem.domain)}"
        f"/observed_date={observed_date}"
        f"/dag_id={_safe_segment(problem.dag_id)}"
        f"/{_safe_segment(problem.run_id)}__{occurred}_{_safe_segment(problem.type_slug)}.json"
    )


class R2ErrorSink:
    """Problem 문서를 R2 에 per-error JSON 으로 적재한다.

    put_object 주입은 테스트용(가짜 클라이언트) — 미지정 시 boto3 를 지연 임포트한다.
    """

    def __init__(self, *, prefix: str = DEFAULT_PREFIX,
                 put_object: Callable[[str, bytes], None] | None = None) -> None:
        self.prefix = prefix
        self._put_object = put_object

    def serialize(self, problem: Problem) -> bytes:
        """저장 직전 공통 redaction 필수 — detail/request 에 키·토큰이 남지 않게."""
        refresh_env_secrets()
        document: dict[str, Any] = redact(problem.to_dict())
        return json.dumps(document, ensure_ascii=False, sort_keys=True).encode("utf-8")

    def write(self, problem: Problem) -> str:
        object_key = build_object_key(problem, prefix=self.prefix)
        payload = self.serialize(problem)
        if self._put_object is not None:
            self._put_object(object_key, payload)
        else:
            self._put_r2_object(object_key, payload)
        LOGGER.info("Stored problem document: %s", object_key)
        return object_key

    @staticmethod
    def _put_r2_object(object_key: str, payload: bytes) -> None:
        import boto3

        boto3.client(
            "s3",
            endpoint_url=_r2_env("R2_ENDPOINT"),
            aws_access_key_id=_r2_env("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=_r2_env("R2_SECRET_ACCESS_KEY"),
            region_name="auto",
        ).put_object(
            Bucket=_r2_env("R2_BUCKET_NAME"),
            Key=object_key,
            Body=payload,
            ContentType="application/problem+json; charset=utf-8",
        )


_default_sink = R2ErrorSink()


def write_problem(problem: Problem) -> str:
    return _default_sink.write(problem)
