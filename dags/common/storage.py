"""스토리지 추상화 — local(개발) ↔ R2(S3 호환). key 는 백엔드 무관 POSIX 경로 (#109).

commerce `include/common/storage.py`(현 `commerce_core`)에서 승격한 범용 부분.
도메인 결합(Settings)은 제거하고 `build_storage()` 팩토리로 파라미터화했다 —
도메인별 env 규약(예: commerce STORAGE_BACKEND/COMMERCE_STORAGE_PREFIX)은
각 도메인 어댑터(`commerce_core.storage.get_storage`)가 유지한다.
`write_parquet` 는 silver 전용(lazy pandas).
"""
from __future__ import annotations

import io
import json
import os
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


def r2_env(name: str) -> str:
    """R2 자격증명 env 해석 — 전 도메인 단일 규약(#230).

    **키 이름은 배포 환경을 담지 않는다.** ``R2_<X>`` 한 벌뿐이고, 어느 환경을 가리키는지는
    그 키의 **값**이 정한다(배포가 값을 채운다). 호스트 컴포즈도 같은 구조다 — Trino 카탈로그
    파일이 canonical ``R2_*`` 한 세트만 읽고, 타깃 전환은 파일 이름(카탈로그명)으로만 한다.

    과거에는 ``R2_DEV_<X>`` 를 먼저 보는 규칙이 있었으나(멘티 dev 버킷 분리), 호스트가 ENV2
    개편에서 ``R2_DEV_*`` 를 없앴고 어느 배포에도 설정돼 있지 않다. 그 분기를 남겨 두면
    누군가 ``R2_DEV_*`` 를 채우는 순간 같은 날짜 기록이 두 버킷으로 갈리므로(ASK-Seoul#78
    ``Z-7``) 규칙 자체를 제거했다.

    name 은 축약형(``ENDPOINT``)·전체형(``R2_ENDPOINT``) 모두 허용(선행 ``R2_`` 정규화).
    """
    base = name.removeprefix("R2_")
    value = os.environ.get("R2_" + base)
    if not value:
        raise RuntimeError(f"R2 자격증명 누락 — R2_{base}")
    return value


def r2_env_for(name: str, target: str) -> str:
    """``r2_env`` 와 동치. ``target`` 은 더 이상 분기하지 않는다(호출측 호환용으로만 남김).

    자격증명 키가 환경별로 갈리던 시절의 잔재다. 지금은 배포 하나가 버킷 하나를 가리키므로
    타깃으로 자격증명을 고를 여지가 없다. 새 코드는 ``r2_env`` 를 쓴다.
    """
    return r2_env(name)


class Storage(ABC):
    @abstractmethod
    def write_bytes(self, key: str, data: bytes) -> None: ...

    @abstractmethod
    def read_bytes(self, key: str) -> bytes: ...

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def list_keys(self, prefix: str) -> list[str]: ...

    @abstractmethod
    def delete(self, key: str) -> None: ...   # feat/59: 실패 파편 정리(한 파일 관리)

    # ── helpers ──
    def copy(self, src_key: str, dst_key: str) -> None:
        """src → dst 복사(기본은 read+write). R2 는 서버사이드 copy 로 오버라이드.

        diff-target 롤링의 '이동'(copy → 원본 delete)에 사용 — 데이터를 로컬로 내리지 않는다.
        """
        self.write_bytes(dst_key, self.read_bytes(src_key))

    def write_text(self, key: str, text: str) -> None:
        self.write_bytes(key, text.encode("utf-8"))

    def write_json(self, key: str, obj: Any) -> None:
        self.write_text(key, json.dumps(obj, ensure_ascii=False, indent=2))

    def write_bytes_if_absent(self, key: str, data: bytes) -> bool:
        """Write once when the backend can guarantee an atomic create.

        Control-plane receipts must never implement this with ``exists`` followed by
        ``write``: concurrent retries could otherwise overwrite conflicting evidence.
        Backends that do not provide an atomic create fail closed.
        """
        raise NotImplementedError(
            "Storage backend does not support atomic write-if-absent"
        )

    def write_json_if_absent(self, key: str, obj: Any) -> bool:
        return self.write_bytes_if_absent(
            key,
            json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8"),
        )

    def read_json(self, key: str) -> Any:
        return json.loads(self.read_bytes(key).decode("utf-8"))

    def write_parquet(self, key: str, records: list[dict]) -> None:
        import pandas as pd  # lazy: silver task 만 사용

        buf = io.BytesIO()
        pd.DataFrame(records).to_parquet(buf, engine="pyarrow", index=False)
        self.write_bytes(key, buf.getvalue())


class LocalStorage(Storage):
    def __init__(self, root: str) -> None:
        self.root = Path(root)

    def _path(self, key: str) -> Path:
        return self.root / key

    def write_bytes(self, key: str, data: bytes) -> None:
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_bytes(data)
        tmp.replace(p)

    def write_bytes_if_absent(self, key: str, data: bytes) -> bool:
        """Atomically link a completed temporary file only when ``key`` is absent."""
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary.write(data)
            temporary_path = Path(temporary.name)
        try:
            os.link(temporary_path, target)
        except FileExistsError:
            return False
        finally:
            temporary_path.unlink(missing_ok=True)
        return True

    def read_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def list_keys(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.root)).replace("\\", "/")
                      for p in base.rglob("*") if p.is_file())

    def delete(self, key: str) -> None:
        self._path(key).unlink(missing_ok=True)


class R2Storage(Storage):
    """Cloudflare R2(S3 호환) — **boto3**(호스트 이미지에 기본 포함). 객체키는 <key>(버킷 분리).

    s3fs 가 아니라 boto3 를 쓰는 이유: 현재 호스트 이미지에 boto3 만 있고 s3fs 는 없음.
    R2 는 path-style + region 'auto' + SigV4 로 접근한다.
    """

    def __init__(self, *, bucket: str, endpoint: str, key: str, secret: str,
                 region: str = "auto") -> None:
        import boto3  # lazy: local 백엔드는 boto3 임포트 안 함
        from botocore.config import Config

        if not (bucket and endpoint and key and secret):
            raise ValueError("R2 backend requires R2_BUCKET/R2_ENDPOINT/"
                             "R2_ACCESS_KEY_ID/R2_SECRET_ACCESS_KEY")
        self.bucket = bucket
        self._s3 = boto3.client(
            "s3", endpoint_url=endpoint, aws_access_key_id=key,
            aws_secret_access_key=secret, region_name=region,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    def write_bytes(self, key: str, data: bytes) -> None:
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=data)

    def write_bytes_if_absent(self, key: str, data: bytes) -> bool:
        """Conditionally create one R2 object without overwriting slot evidence."""
        from botocore.exceptions import ClientError

        for _ in range(3):
            try:
                self._s3.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=data,
                    IfNoneMatch="*",
                )
                return True
            except ClientError as exc:
                response = exc.response or {}
                error = response.get("Error", {})
                code = str(error.get("Code", ""))
                status = int((response.get("ResponseMetadata", {}) or {}).get(
                    "HTTPStatusCode", 0
                ))
                if code == "PreconditionFailed" or status == 412:
                    return False
                if code == "ConditionalRequestConflict" or status == 409:
                    continue
                raise
        raise RuntimeError("R2 conditional write conflicted repeatedly")

    def read_bytes(self, key: str) -> bytes:
        return self._s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError
        try:
            self._s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        for page in self._s3.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted(keys)

    def delete(self, key: str) -> None:
        self._s3.delete_object(Bucket=self.bucket, Key=key)

    def copy(self, src_key: str, dst_key: str) -> None:
        # 서버사이드 복사(로컬 전송 없음) — diff-target 롤링 '이동'용.
        self._s3.copy_object(Bucket=self.bucket, Key=dst_key,
                             CopySource={"Bucket": self.bucket, "Key": src_key})


def resolve_storage() -> "Storage":
    """**배포 환경이 정한 저장소를 그대로** 쓴다 — 도메인 무관, 공용 DAG 용.

    도메인 어댑터(예: `commerce_core.storage.get_storage`)는 그 도메인의 env 규약을 따르지만,
    공용 DAG 는 특정 도메인의 규약에 기댈 수 없다. 그래서 **배포 수준의 canonical 값**만 읽는다.

    백엔드 판정 순서:
      1. ``STORAGE_BACKEND`` 가 명시돼 있으면 그 값
      2. 없으면 **canonical R2 자격증명 유무로 추론** — 있으면 ``r2``, 없으면 ``local``

    2번이 있는 이유: 공용 DAG 하나를 위해 새 필수 설정을 만들지 않기 위해서다. R2 자격증명은
    Trino 카탈로그도 읽는 배포 필수값이라 운영 환경에는 반드시 있고, 없는 박스는 애초에 로컬이다.
    **환경별로 DAG 를 나누지 않는다** — 키는 하나이고 그 키의 **값**이 환경을 가리킨다(ASAC-DAG#654).
    """
    backend = os.environ.get("STORAGE_BACKEND", "").strip().lower()
    if not backend:
        has_r2 = all(os.environ.get(f"R2_{part}") for part in
                     ("BUCKET_NAME", "ENDPOINT", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY"))
        backend = "r2" if has_r2 else "local"
    if backend == "local":
        return build_storage("local",
                             local_root=os.environ.get("LOCAL_DATA_ROOT", "/opt/airflow/data"))
    return build_storage(
        "r2", bucket=r2_env("R2_BUCKET_NAME"), endpoint=r2_env("R2_ENDPOINT"),
        key=r2_env("R2_ACCESS_KEY_ID"), secret=r2_env("R2_SECRET_ACCESS_KEY"),
        region=os.environ.get("R2_REGION", "auto"))


def build_storage(backend: str, *, local_root: str = "",
                  bucket: str = "", endpoint: str = "", key: str = "",
                  secret: str = "", region: str = "auto") -> Storage:
    """백엔드 이름으로 Storage 조립 — env 규약은 호출측(도메인 어댑터)이 정한다."""
    if backend == "local":
        return LocalStorage(local_root)
    if backend == "r2":
        return R2Storage(bucket=bucket, endpoint=endpoint, key=key,
                         secret=secret, region=region)
    raise ValueError(f"unknown storage backend: {backend!r}")
