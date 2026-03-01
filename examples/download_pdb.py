#!/usr/bin/env python3
"""Download the full PDB archive (mmCIF) and upload to MinIO.

Usage:
    # With env vars set (see .env.example):
    python examples/download_pdb.py

    # Upload only (staging already populated):
    python examples/download_pdb.py --upload-only

    # Upload manifest only (PDBs already in MinIO, manifest exists locally):
    python examples/download_pdb.py --upload-manifest-only

    # Or override via args:
    python examples/download_pdb.py --source rcsb --format mmcif --method rsync
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from moldata.config import load_settings
from moldata.core.storage import S3Storage, LocalStorage
from moldata.core.upload_utils import UploadOptions
from moldata.datasets.pdb import PDBDataset

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")


def main() -> None:
    p = argparse.ArgumentParser(description="Download PDB and upload to MinIO/S3")
    p.add_argument("--staging", default="/moldata/pdb/mmCIF", help="Local staging directory")
    p.add_argument("--manifest", default="manifests/pdb.parquet", help="Output manifest path")
    p.add_argument("--source", default="rcsb", choices=["rcsb", "ebi"], help="rsync source")
    p.add_argument("--format", default="mmcif", choices=["mmcif", "pdb"], help="File format")
    p.add_argument("--method", default="rsync", choices=["rsync", "https"], help="Download method")
    p.add_argument("--snapshot-year", type=int, default=None, help="Yearly snapshot for reproducibility")
    p.add_argument("--upload-format", default="raw", choices=["raw", "tar_shards"], help="Upload format")
    p.add_argument("--tar-shard-size", type=int, default=1000, help="Files per tar shard")
    p.add_argument("--workers", type=int, default=16, help="Parallel upload workers")
    p.add_argument("--batch-size", type=int, default=500, help="Upload batch size")
    p.add_argument("--enriched", action="store_true", help="Build enriched manifest with mmCIF metadata")
    p.add_argument("--download-only", action="store_true", help="Only download, skip upload")
    p.add_argument("--upload-only", action="store_true", help="Only upload from existing staging, skip download")
    p.add_argument("--upload-manifest-only", action="store_true", help="Only upload the manifest to MinIO (PDBs already uploaded, manifest exists locally)")
    p.add_argument("--remove-local", action="store_true", default=False, help="Remove local staging files after upload (default: False, keep PDBs)")
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

    subpath = "mmCIF" if args.format == "mmcif" else "pdb"
    prefix = f"{settings.datasets_prefix}pdb/{subpath}/"

    # Upload manifest only: PDBs already in MinIO, just upload the local manifest
    if args.upload_manifest_only:
        manifest_path = Path(args.manifest)
        if not manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest_path}. Build it first or point to the correct path.")
        manifest_key = f"{settings.datasets_prefix}pdb/manifests/{manifest_path.name}"
        storage.put_file(manifest_key, str(manifest_path))
        print(f"Manifest uploaded to {manifest_key}")
        return

    ds = PDBDataset(
        storage=storage,
        bucket=bucket,
        s3_prefix=prefix,
        _staging_dir=Path(args.staging),
        source=args.source,
        pdb_format=args.format,
        download_method=args.method,
        snapshot_year=args.snapshot_year,
        upload_format=args.upload_format,
        tar_shard_size=args.tar_shard_size,
    )

    # Download (skip if --upload-only)
    if not args.upload_only:
        print(f"Downloading PDB ({args.source}/{args.format}/{args.method})...")
        ds.download()
    else:
        print("Upload-only mode: skipping download, using existing files in staging.")

    if args.download_only:
        print(f"Download complete. Files in {args.staging}")
        return

    # Upload
    opts = UploadOptions(
        max_workers=args.workers,
        batch_size=args.batch_size,
        checkpoint_dir=settings.checkpoint_dir,
    )
    print(f"Uploading to {settings.minio_bucket}/{prefix}...")
    ds.upload(upload_options=opts)

    # Manifest
    manifest_path = Path(args.manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    if args.enriched and args.format == "mmcif":
        print("Building enriched manifest (parsing mmCIF metadata)...")
        manifest = ds.build_enriched_manifest()
    else:
        manifest = ds.build_manifest()
    manifest.save_parquet(manifest_path)

    # Upload manifest to storage (S3/MinIO or local) alongside the dataset
    manifest_key = f"{settings.datasets_prefix}pdb/manifests/{manifest_path.name}"
    storage.put_file(manifest_key, str(manifest_path))
    print(f"Manifest uploaded to storage: {manifest_key}")

    print(f"Done! Manifest: {manifest_path} ({manifest.count()} entries, {manifest.size_bytes()} bytes)")

    if args.remove_local:
        ds.cleanup_staging()
        print(f"Cleaned up local staging: {args.staging}")
    else:
        print(f"Local PDBs kept in {args.staging}")


if __name__ == "__main__":
    main()
