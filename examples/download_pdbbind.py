#!/usr/bin/env python3
"""Upload pre-downloaded PDBBind files to MinIO.

PDBBind requires registration. Download the archives manually, then:
    python examples/download_pdbbind.py --staging /path/to/pdbbind/files
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from moldata.config import load_settings
from moldata.core.storage import S3Storage, LocalStorage
from moldata.core.upload_utils import UploadOptions
from moldata.datasets.pdbbind import PDBBindDataset

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")


def main() -> None:
    p = argparse.ArgumentParser(description="Upload PDBBind to MinIO/S3")
    p.add_argument("--staging", required=True, help="Directory with PDBBind files")
    p.add_argument("--manifest", default="manifests/pdbbind.parquet", help="Output manifest")
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

    prefix = f"{settings.datasets_prefix}pdbbind/"
    ds = PDBBindDataset(
        storage=storage, bucket=bucket, s3_prefix=prefix,
        _staging_dir=Path(args.staging), mode="local",
    )
    opts = UploadOptions(max_workers=args.workers, batch_size=args.batch_size, checkpoint_dir=settings.checkpoint_dir)

    print(f"Uploading PDBBind from {args.staging}...")
    ds.upload(upload_options=opts)

    manifest_path = Path(args.manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = ds.build_manifest()
    manifest.save_parquet(manifest_path)

    # Upload manifest to storage alongside the dataset
    manifest_key = f"{settings.datasets_prefix}pdbbind/manifests/{manifest_path.name}"
    storage.put_file(manifest_key, str(manifest_path))
    print(f"Manifest uploaded to storage: {manifest_key}")

    print(f"Done! Manifest: {manifest_path} ({manifest.count()} entries)")

    if not args.keep_local:
        ds.cleanup_staging()
        print(f"Cleaned up local staging: {args.staging}")


if __name__ == "__main__":
    main()
