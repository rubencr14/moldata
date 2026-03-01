"""Query structures from MinIO using manifest + RCSB Search API.

MinIOQuery is the main entry point for selecting and downloading subsets
of structures that have been previously uploaded to MinIO via moldata.

Usage::

    from moldata.query import MinIOQuery

    q = MinIOQuery("manifests/pdb.parquet")
    paths = q.fetch_by_family("PF00069", resolution_max=2.5, max_structures=200)
    paths = q.fetch_by_ec("2.7.10", max_structures=100)
    paths = q.fetch_collection("kinases_human", max_structures=150)
"""

from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional

import pandas as pd

from moldata.config import load_settings
from moldata.core.download_utils import parallel_s3_download
from moldata.core.manifest import Manifest
from moldata.core.storage import S3Storage, Storage
from moldata.query.collections import CollectionSpec, get_collection
from moldata.query.rcsb_search import search_ids as _rcsb_search_ids

logger = logging.getLogger(__name__)


@dataclass
class MinIOQuery:
    """Query and download structures from MinIO.

    Combines a local manifest (parquet) with RCSB Search API to identify
    PDB IDs matching biological criteria, then downloads only the matching
    files from MinIO to a local cache directory.

    Args:
        manifest_path: Path to a parquet manifest (local file or s3://bucket/key).
        cache_dir: Local directory where downloaded files are cached.
        storage: Optional pre-configured Storage instance.
            If None, one is built from MINIO_* env vars.
        workers: Parallel download workers for fetching from MinIO.
    """

    manifest_path: Optional[str] = None
    cache_dir: str = "/moldata/query_cache"
    storage: Optional[Storage] = None
    workers: int = 8
    _manifest: Optional[Manifest] = field(default=None, init=False, repr=False)
    _storage: Optional[Storage] = field(default=None, init=False, repr=False)

    @property
    def manifest(self) -> Manifest:
        if self._manifest is None:
            if self.manifest_path is None:
                raise ValueError("manifest_path is required")
            path = self.manifest_path
            if path.startswith("s3://"):
                self._manifest = self._load_manifest_from_s3(path)
            else:
                self._manifest = Manifest.load_parquet(Path(path))
            logger.info("Loaded manifest: %d entries from %s", self._manifest.count(), path)
        return self._manifest

    def _load_manifest_from_s3(self, s3_uri: str) -> Manifest:
        """Download manifest from S3 and load. Expects s3://bucket/key format."""
        parsed = urlparse(s3_uri)
        if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
            raise ValueError(f"Invalid S3 URI: {s3_uri}")
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        settings = load_settings()
        if bucket != settings.minio_bucket:
            raise ValueError(
                f"Manifest bucket '{bucket}' does not match configured bucket '{settings.minio_bucket}'"
            )
        storage = self._get_storage()
        fd, tmp = tempfile.mkstemp(suffix=".parquet")
        try:
            os.close(fd)
            storage.get_file(key, tmp)
            return Manifest.load_parquet(Path(tmp))
        finally:
            Path(tmp).unlink(missing_ok=True)

    def _get_storage(self) -> Storage:
        if self._storage is not None:
            return self._storage
        if self.storage is not None:
            self._storage = self.storage
            return self._storage
        settings = load_settings()
        if settings.storage_backend != "s3":
            raise ValueError(
                "MinIOQuery requires S3 storage. Set MINIO_ENDPOINT and MINIO_ACCESS_KEY."
            )
        self._storage = S3Storage(
            bucket=settings.minio_bucket,
            endpoint_url=settings.s3_endpoint_url,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            region=settings.minio_region,
        )
        return self._storage

    # -- Core: filter manifest and download from MinIO ----------------------

    def _filter_by_pdb_ids(self, pdb_ids: list[str], max_structures: Optional[int] = None) -> pd.DataFrame:
        """Filter manifest rows to only those whose sample_id is in pdb_ids."""
        df = self.manifest.df
        ids_lower = {pid.lower() for pid in pdb_ids}
        mask = df["sample_id"].str.lower().isin(ids_lower)
        filtered = df[mask]
        if max_structures and len(filtered) > max_structures:
            filtered = filtered.head(max_structures)
        return filtered

    def _filter_by_column(
        self,
        column: str,
        value,
        op: str = "eq",
        max_structures: Optional[int] = None,
    ) -> pd.DataFrame:
        df = self.manifest.df
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not in manifest. Available: {list(df.columns)}")
        if op == "eq":
            mask = df[column] == value
        elif op == "le":
            mask = df[column] <= value
        elif op == "ge":
            mask = df[column] >= value
        elif op == "contains":
            mask = df[column].astype(str).str.contains(str(value), case=False, na=False)
        else:
            raise ValueError(f"Unsupported op: {op}")
        filtered = df[mask]
        if max_structures and len(filtered) > max_structures:
            filtered = filtered.head(max_structures)
        return filtered

    def _download_keys(self, df: pd.DataFrame) -> list[Path]:
        """Download files from MinIO for the given manifest rows, return local paths."""
        if df.empty:
            logger.warning("No matching structures found")
            return []

        keys = df["key"].tolist()
        dest = Path(self.cache_dir)
        dest.mkdir(parents=True, exist_ok=True)

        storage = self._get_storage()
        downloaded, skipped = parallel_s3_download(
            storage, keys, dest, max_workers=self.workers, prefix_label="query",
        )
        logger.info("Downloaded %d, skipped %d (already cached)", downloaded, skipped)

        paths = []
        for key in keys:
            local = dest / key
            if local.exists():
                paths.append(local)
        return paths

    # -- Public fetch methods (same API as molfun's PDBFetcher) -------------

    def fetch(self, pdb_ids: list[str], max_structures: Optional[int] = None) -> list[Path]:
        """Fetch structures by PDB ID from MinIO.

        Args:
            pdb_ids: List of PDB IDs (e.g. ["1abc", "2xyz"]).
            max_structures: Limit number of structures.

        Returns:
            List of local file paths to the downloaded structures.
        """
        df = self._filter_by_pdb_ids(pdb_ids, max_structures)
        logger.info("fetch: %d/%d PDB IDs found in manifest", len(df), len(pdb_ids))
        return self._download_keys(df)

    def fetch_by_family(
        self,
        pfam_id: str,
        max_structures: int = 500,
        resolution_max: float = 3.0,
    ) -> list[Path]:
        """Fetch structures by Pfam family ID.

        Queries RCSB Search API to find PDB IDs in the family, then
        intersects with the manifest and downloads from MinIO.
        """
        pdb_ids = _rcsb_search_ids(pfam_id=pfam_id, max_results=max_structures * 2, resolution_max=resolution_max)
        logger.info("fetch_by_family(%s): %d IDs from RCSB", pfam_id, len(pdb_ids))
        df = self._filter_by_pdb_ids(pdb_ids, max_structures)
        return self._download_keys(df)

    def fetch_by_ec(
        self,
        ec_number: str,
        max_structures: int = 500,
        resolution_max: float = 3.0,
    ) -> list[Path]:
        """Fetch structures by EC number (e.g. '2.7.10' or '2.7.*')."""
        pdb_ids = _rcsb_search_ids(ec_number=ec_number, max_results=max_structures * 2, resolution_max=resolution_max)
        logger.info("fetch_by_ec(%s): %d IDs from RCSB", ec_number, len(pdb_ids))
        df = self._filter_by_pdb_ids(pdb_ids, max_structures)
        return self._download_keys(df)

    def fetch_by_go(
        self,
        go_id: str,
        max_structures: int = 500,
        resolution_max: float = 3.0,
    ) -> list[Path]:
        """Fetch structures by Gene Ontology term."""
        pdb_ids = _rcsb_search_ids(go_id=go_id, max_results=max_structures * 2, resolution_max=resolution_max)
        logger.info("fetch_by_go(%s): %d IDs from RCSB", go_id, len(pdb_ids))
        df = self._filter_by_pdb_ids(pdb_ids, max_structures)
        return self._download_keys(df)

    def fetch_by_taxonomy(
        self,
        taxonomy_id: int,
        max_structures: int = 500,
        resolution_max: float = 3.0,
    ) -> list[Path]:
        """Fetch structures by NCBI taxonomy ID (e.g. 9606 for human)."""
        pdb_ids = _rcsb_search_ids(taxonomy_id=taxonomy_id, max_results=max_structures * 2, resolution_max=resolution_max)
        logger.info("fetch_by_taxonomy(%d): %d IDs from RCSB", taxonomy_id, len(pdb_ids))
        df = self._filter_by_pdb_ids(pdb_ids, max_structures)
        return self._download_keys(df)

    def fetch_by_keyword(
        self,
        keyword: str,
        max_structures: int = 500,
        resolution_max: float = 3.0,
    ) -> list[Path]:
        """Fetch structures by free-text keyword search."""
        pdb_ids = _rcsb_search_ids(keyword=keyword, max_results=max_structures * 2, resolution_max=resolution_max)
        logger.info("fetch_by_keyword(%s): %d IDs from RCSB", keyword, len(pdb_ids))
        df = self._filter_by_pdb_ids(pdb_ids, max_structures)
        return self._download_keys(df)

    def fetch_by_scop(
        self,
        scop_id: str,
        max_structures: int = 500,
        resolution_max: float = 3.0,
    ) -> list[Path]:
        """Fetch structures by SCOPe classification."""
        pdb_ids = _rcsb_search_ids(scop_id=scop_id, max_results=max_structures * 2, resolution_max=resolution_max)
        logger.info("fetch_by_scop(%s): %d IDs from RCSB", scop_id, len(pdb_ids))
        df = self._filter_by_pdb_ids(pdb_ids, max_structures)
        return self._download_keys(df)

    def fetch_combined(
        self,
        *,
        pfam_id: Optional[str] = None,
        ec_number: Optional[str] = None,
        go_id: Optional[str] = None,
        taxonomy_id: Optional[int] = None,
        keyword: Optional[str] = None,
        uniprot_id: Optional[str] = None,
        scop_id: Optional[str] = None,
        max_structures: int = 500,
        resolution_max: float = 3.0,
    ) -> list[Path]:
        """Fetch with multiple RCSB filters combined (AND logic)."""
        pdb_ids = _rcsb_search_ids(
            pfam_id=pfam_id, ec_number=ec_number, go_id=go_id,
            taxonomy_id=taxonomy_id, keyword=keyword,
            uniprot_id=uniprot_id, scop_id=scop_id,
            max_results=max_structures * 2, resolution_max=resolution_max,
        )
        logger.info("fetch_combined: %d IDs from RCSB", len(pdb_ids))
        df = self._filter_by_pdb_ids(pdb_ids, max_structures)
        return self._download_keys(df)

    def fetch_collection(
        self,
        name: str,
        max_structures: Optional[int] = None,
        resolution_max: Optional[float] = None,
    ) -> list[Path]:
        """Fetch a pre-defined protein collection (e.g. 'kinases_human').

        Uses the same collection definitions as molfun.
        """
        spec: CollectionSpec = get_collection(name)
        max_s = max_structures or spec.default_max
        res = resolution_max or spec.default_resolution

        kwargs: dict = {}
        if spec.pfam_id:
            kwargs["pfam_id"] = spec.pfam_id
        if spec.ec_number:
            kwargs["ec_number"] = spec.ec_number
        if spec.go_id:
            kwargs["go_id"] = spec.go_id
        if spec.taxonomy_id:
            kwargs["taxonomy_id"] = spec.taxonomy_id
        if spec.keyword:
            kwargs["keyword"] = spec.keyword

        return self.fetch_combined(**kwargs, max_structures=max_s, resolution_max=res)

    # -- Manifest-only queries (no RCSB, no download) -----------------------

    def filter_manifest(
        self,
        *,
        method: Optional[str] = None,
        resolution_max: Optional[float] = None,
        resolution_min: Optional[float] = None,
        max_structures: Optional[int] = None,
    ) -> pd.DataFrame:
        """Filter the manifest by local metadata columns (no RCSB call).

        Useful with enriched manifests that contain resolution, method, etc.
        """
        df = self.manifest.df.copy()
        if method and "method" in df.columns:
            df = df[df["method"].str.contains(method, case=False, na=False)]
        if resolution_max is not None and "resolution" in df.columns:
            df = df[df["resolution"] <= resolution_max]
        if resolution_min is not None and "resolution" in df.columns:
            df = df[df["resolution"] >= resolution_min]
        if max_structures and len(df) > max_structures:
            df = df.head(max_structures)
        return df

    def fetch_filtered(
        self,
        *,
        method: Optional[str] = None,
        resolution_max: Optional[float] = None,
        resolution_min: Optional[float] = None,
        max_structures: Optional[int] = None,
    ) -> list[Path]:
        """Filter manifest by metadata and download matching structures."""
        df = self.filter_manifest(
            method=method, resolution_max=resolution_max,
            resolution_min=resolution_min, max_structures=max_structures,
        )
        return self._download_keys(df)

    # -- Info ---------------------------------------------------------------

    def available_pdb_ids(self) -> list[str]:
        """Return all PDB IDs available in the manifest."""
        return self.manifest.df["sample_id"].tolist()

    def count(self) -> int:
        """Total number of structures in the manifest."""
        return self.manifest.count()

    def summary(self) -> dict:
        """Summary statistics of the manifest."""
        df = self.manifest.df
        info: dict = {"total": len(df)}
        if "resolution" in df.columns:
            info["resolution_mean"] = float(df["resolution"].mean())
            info["resolution_median"] = float(df["resolution"].median())
        if "method" in df.columns:
            info["methods"] = df["method"].value_counts().to_dict()
        if "size_bytes" in df.columns:
            info["total_size_bytes"] = int(df["size_bytes"].sum())
        return info
