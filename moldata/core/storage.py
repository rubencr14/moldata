from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, BinaryIO, Protocol, runtime_checkable


@runtime_checkable
class Storage(Protocol):
    """Minimal storage interface.

    Keep it tiny on purpose:
      - store bytes or files
      - retrieve files
      - check if key exists (head)
      - list prefix (optional but useful)
    """

    def put_file(self, key: str, path: str) -> None: ...
    def get_file(self, key: str, path: str) -> None: ...
    def head(self, key: str) -> Optional[dict]: ...
    def list_prefix(self, prefix: str) -> Iterable[str]: ...


@dataclass(frozen=True)
class LocalStorage:
    """Local filesystem storage using a root directory."""

    root: Path

    def _resolve(self, key: str) -> Path:
        key = key.lstrip("/").replace("..", "")
        return self.root / key

    def put_file(self, key: str, path: str) -> None:
        dst = self._resolve(key)
        dst.parent.mkdir(parents=True, exist_ok=True)
        Path(path).replace(dst) if False else dst.write_bytes(Path(path).read_bytes())

    def get_file(self, key: str, path: str) -> None:
        src = self._resolve(key)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(src.read_bytes())

    def head(self, key: str) -> Optional[dict]:
        p = self._resolve(key)
        if not p.exists():
            return None
        st = p.stat()
        return {"ContentLength": st.st_size}

    def list_prefix(self, prefix: str) -> Iterable[str]:
        p = self._resolve(prefix)
        if not p.exists():
            return []
        for f in p.rglob("*"):
            if f.is_file():
                yield str(f.relative_to(self.root)).replace("\\", "/")


@dataclass
class S3Storage:
    """S3-compatible storage (MinIO, AWS S3) using boto3.

    This keeps things simple and relies on boto3's internal multipart handling.
    """

    bucket: str
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"
    max_concurrency: int = 16
    multipart_threshold_mb: int = 64

    def __post_init__(self) -> None:
        import boto3
        from boto3.s3.transfer import TransferConfig

        self._client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
        )
        self._transfer_config = TransferConfig(
            max_concurrency=self.max_concurrency,
            multipart_threshold=self.multipart_threshold_mb * 1024 * 1024,
            multipart_chunksize=64 * 1024 * 1024,
            use_threads=True,
        )

    def put_file(self, key: str, path: str) -> None:
        self._client.upload_file(
            Filename=path,
            Bucket=self.bucket,
            Key=key,
            Config=self._transfer_config,
        )

    def get_file(self, key: str, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._client.download_file(Bucket=self.bucket, Key=key, Filename=path)

    def head(self, key: str) -> Optional[dict]:
        try:
            return self._client.head_object(Bucket=self.bucket, Key=key)
        except Exception:
            return None

    def list_prefix(self, prefix: str) -> Iterable[str]:
        token: Optional[str] = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self._client.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                yield obj["Key"]
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
