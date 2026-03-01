from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

from dotenv import load_dotenv

_env_path = Path(__file__).resolve().parents[1] / ".env"
if _env_path.is_file():
    load_dotenv(_env_path, override=False)


@dataclass
class MoldataSettings:
    """Configuration loaded from MINIO_* and MOLDATA_* environment variables.

    MinIO/S3 connection:
      MINIO_ENDPOINT=localhost
      MINIO_PORT=9000
      MINIO_ACCESS_KEY=minioadmin
      MINIO_SECRET_KEY=minioadmin123
      MINIO_BUCKET=molfun-data

    Moldata behaviour:
      MOLDATA_STORAGE_BACKEND=s3
      MOLDATA_DATASETS_PREFIX=datasets/
      MOLDATA_UPLOAD_MAX_WORKERS=16
      MOLDATA_UPLOAD_BATCH_SIZE=500
    """

    storage_backend: Literal["local", "s3"] = "local"

    # Local
    local_root: str = "/data/moldata"

    # MinIO / S3 (from MINIO_* env vars)
    minio_endpoint: str = "localhost"
    minio_port: int = 9000
    minio_access_key: str = ""
    minio_secret_key: str = ""
    minio_bucket: str = "molfun-data"
    minio_region: str = "us-east-1"
    minio_secure: bool = False

    # Dataset prefix under bucket
    datasets_prefix: str = "datasets/"

    # Upload strategy
    upload_max_workers: int = 16
    upload_batch_size: int = 500
    checkpoint_dir: str = "/moldata/checkpoints"

    @property
    def s3_endpoint_url(self) -> str:
        scheme = "https" if self.minio_secure else "http"
        return f"{scheme}://{self.minio_endpoint}:{self.minio_port}"


def load_settings() -> MoldataSettings:
    """Load settings from environment variables."""
    backend = os.environ.get("MOLDATA_STORAGE_BACKEND", "local")
    has_minio = bool(os.environ.get("MINIO_ENDPOINT") or os.environ.get("MINIO_ACCESS_KEY"))
    if has_minio and backend == "local":
        backend = "s3"

    return MoldataSettings(
        storage_backend=backend,
        local_root=os.environ.get("MOLDATA_LOCAL_ROOT", "/data/moldata"),
        minio_endpoint=os.environ.get("MINIO_ENDPOINT", "localhost"),
        minio_port=int(os.environ.get("MINIO_PORT", "9000")),
        minio_access_key=os.environ.get("MINIO_ACCESS_KEY", ""),
        minio_secret_key=os.environ.get("MINIO_SECRET_KEY", ""),
        minio_bucket=os.environ.get("MINIO_BUCKET", "molfun-data"),
        minio_region=os.environ.get("MINIO_REGION", "us-east-1"),
        minio_secure=os.environ.get("MINIO_SECURE", "false").lower() in ("true", "1", "yes"),
        datasets_prefix=os.environ.get("MOLDATA_DATASETS_PREFIX", "datasets/"),
        upload_max_workers=int(os.environ.get("MOLDATA_UPLOAD_MAX_WORKERS", "16")),
        upload_batch_size=int(os.environ.get("MOLDATA_UPLOAD_BATCH_SIZE", "500")),
        checkpoint_dir=os.environ.get("MOLDATA_CHECKPOINT_DIR", "/moldata/checkpoints"),
    )
