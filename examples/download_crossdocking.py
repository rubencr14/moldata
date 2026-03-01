#!/usr/bin/env python3
"""Download CrossDocked2020 dataset and upload to MinIO.

Usage:
    # Download + upload:
    python examples/download_crossdocking.py --mode official

    # Upload pre-downloaded files:
    python examples/download_crossdocking.py --staging /path/to/crossdocking --mode local
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from moldata.config import load_settings
from moldata.core.storage import S3Storage, LocalStorage
from moldata.core.upload_utils import UploadOptions
from moldata.datasets.crossdocking import CrossDockingDataset

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")


def main() -> None:
    p = argparse.ArgumentParser(description="Download CrossDocked2020 and upload to MinIO/S3")
    p.add_argument("--staging", default="/moldata/crossdocking", help="Local staging directory")
    p.add_argument("--manifest", default="manifests/crossdocking.parquet", help="Output manifest")
    p.add_argument("--mode", default="official", choices=["local", "official"], help="Download mode")
    p.add_argument("--version", default="v1.3", choices=["v1.0", "v1.3"], help="Dataset version")
    p.add_argument("--workers", type=int, default=16, help="Parallel upload workers")
    p.add_argument("--batch-size", type=int, default=500, help="Upload batch size")
    p.add_argument("--keep-local", action="store_true", help="Keep local staging files after upload (default: remove)")
    args = p.parse_args()

    settings = load_settings()
    if settings.storage_backend == "s3":
        storage = S3Storage(
            bucket=settings.minio_bucket,
            endpoint_url=settings.s3_endpoint_url,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            region=settings.minio_region,
        )
        bucket = settings.minio_bucket
    else:
        storage = LocalStorage(root=Path(settings.local_root))
        bucket = None

    prefix = f"{settings.datasets_prefix}crossdocking/"
    ds = CrossDockingDataset(
        storage=storage, bucket=bucket, s3_prefix=prefix,
        _staging_dir=Path(args.staging), mode=args.mode, version=args.version,
    )
    opts = UploadOptions(max_workers=args.workers, batch_size=args.batch_size, checkpoint_dir=settings.checkpoint_dir)

    print(f"CrossDocking mode={args.mode} version={args.version}")
    stats = ds.prepare(Path(args.manifest), upload_options=opts, remove_local_on_end=not args.keep_local)

    # Upload manifest to storage alongside the dataset
    manifest_path = Path(args.manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_key = f"{settings.datasets_prefix}crossdocking/manifests/{manifest_path.name}"
    storage.put_file(manifest_key, str(manifest_path))
    print(f"Manifest uploaded to storage: {manifest_key}")

    print(f"Done! {stats.count} files, {stats.size_bytes} bytes")


if __name__ == "__main__":
    main()
