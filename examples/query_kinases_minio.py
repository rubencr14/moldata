#!/usr/bin/env python3
"""Query kinases from MinIO and list structures available.

Requires:
  1. PDB dataset uploaded to MinIO (download_pdb.py)
  2. Manifest at manifests/pdb.parquet or s3://bucket/datasets/pdb/manifests/pdb.parquet
  3. MINIO_* env vars in .env

Usage:
    # List kinases available in MinIO (no download)
    python examples/query_kinases_minio.py

    # Human kinases only
    python examples/query_kinases_minio.py --collection kinases_human

    # Download kinases to cache
    python examples/query_kinases_minio.py --download --max 50

    # Use S3 manifest URI
    python examples/query_kinases_minio.py --manifest s3://molfun-data/datasets/pdb/manifests/pdb.parquet
"""

from __future__ import annotations

import argparse
import logging

from moldata.query import MinIOQuery, get_collection, search_ids

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")


def main() -> None:
    p = argparse.ArgumentParser(description="Query kinases from MinIO")
    p.add_argument("--manifest", default="manifests/pdb.parquet", help="Manifest path or s3:// URI")
    p.add_argument("--collection", default="kinases_human", help="Collection: kinases, kinases_human, tyrosine_kinases")
    p.add_argument("--resolution", type=float, default=2.5, help="Max resolution (Å)")
    p.add_argument("--max", type=int, default=200, help="Max IDs to consider")
    p.add_argument("--download", action="store_true", help="Download structures to cache")
    p.add_argument("--cache-dir", default="/tmp/moldata/query_cache", help="Cache directory")
    args = p.parse_args()

    q = MinIOQuery(manifest_path=args.manifest, cache_dir=args.cache_dir)
    spec = get_collection(args.collection)

    # Kinase PDB IDs from RCSB (uses collection filters: Pfam, EC, taxonomy)
    kwargs = {"max_results": args.max * 2, "resolution_max": args.resolution}
    if spec.pfam_id:
        kwargs["pfam_id"] = spec.pfam_id
    if spec.ec_number:
        kwargs["ec_number"] = spec.ec_number
    if spec.taxonomy_id is not None:
        kwargs["taxonomy_id"] = spec.taxonomy_id
    pdb_ids = search_ids(**kwargs)

    # Intersect with manifest: kinases that are actually in MinIO
    df = q._filter_by_pdb_ids(pdb_ids, max_structures=args.max)
    kinases_in_minio = df["sample_id"].tolist()

    print(f"\n{'='*60}")
    print(f"Kinases in MinIO ({args.collection}, ≤{args.resolution} Å)")
    print(f"{'='*60}")
    print(f"Manifest total: {q.count()} structures")
    print(f"RCSB kinases matching criteria: {len(pdb_ids)}")
    print(f"Kinases available in MinIO: {len(kinases_in_minio)}")
    print(f"{'='*60}\n")

    if not kinases_in_minio:
        print("No kinases found. Ensure PDB dataset is uploaded and manifest is correct.")
        return

    # Show list (grouped in rows of 10)
    n = 10
    for i in range(0, len(kinases_in_minio), n):
        chunk = kinases_in_minio[i : i + n]
        print("  " + "  ".join(f"{pid:6}" for pid in chunk))

    if args.download:
        print(f"\nDownloading {len(df)} structures to {args.cache_dir}...")
        paths = q._download_keys(df)
        print(f"Downloaded {len(paths)} structures")
        if paths:
            print(f"  First: {paths[0]}")
            print(f"  Last:  {paths[-1]}")


if __name__ == "__main__":
    main()
