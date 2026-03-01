"""PDB dataset: mmCIF/PDB formats, multiple sources and download methods."""

from __future__ import annotations

import re
import subprocess
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional
from urllib.request import Request, urlopen

import pandas as pd

from moldata.config import load_settings
from moldata.core.logging_utils import get_logger
from moldata.core.manifest import Manifest
from moldata.core.storage import Storage
from moldata.core.upload_utils import UploadItem, UploadOptions, parallel_upload
from moldata.core.download_utils import DownloadItem, DownloadOptions, parallel_download
from moldata.datasets.base import BaseDataset

logger = get_logger(__name__)

# --- RSYNC sources -----------------------------------------------------------
RSYNC_EBI_MMCIF = "rsync.ebi.ac.uk::pub/databases/pdb/data/structures/divided/mmCIF/"
RSYNC_EBI_PDB = "rsync.ebi.ac.uk::pub/databases/pdb/data/structures/divided/pdb/"

RSYNC_RCSB_MMCIF = "rsync.rcsb.org::ftp_data/structures/divided/mmCIF/"
RSYNC_RCSB_PDB = "rsync.rcsb.org::ftp_data/structures/divided/pdb/"
RSYNC_RCSB_PORT = 33444

# --- HTTPS (RCSB) -----------------------------------------------------------
HTTPS_RCSB_BASE = "https://files.rcsb.org/pub/pdb/data/structures/divided"
HTTPS_MMCIF = f"{HTTPS_RCSB_BASE}/mmCIF"
HTTPS_PDB = f"{HTTPS_RCSB_BASE}/pdb"

# --- Snapshots (RCSB rsync) -------------------------------------------------
RSYNC_SNAPSHOT_BASE = "rsync.rcsb.org::ftp_snapshots"

# PDB divided subdirectories (middle 2 chars of ID)
_DIVIDED_SUBDIRS: list[str] = []


def _get_divided_subdirs() -> list[str]:
    """Lazily generate the list of divided PDB subdirectories."""
    global _DIVIDED_SUBDIRS
    if not _DIVIDED_SUBDIRS:
        chars = "0123456789abcdefghijklmnopqrstuvwxyz"
        _DIVIDED_SUBDIRS = [f"{a}{b}" for a in chars for b in chars]
    return _DIVIDED_SUBDIRS


@dataclass
class PDBDataset(BaseDataset):
    """PDB divided archive: mmCIF or PDB legacy, multiple sources and download methods.

    Sources: ebi (rsync default port), rcsb (rsync port 33444)
    Formats: mmcif (.cif.gz), pdb (.ent.gz)
    Download: rsync (bulk), https (parallel fallback)
    Snapshots: snapshot_year uses rsync from ftp_snapshots
    Upload: raw (files) or tar_shards (fewer S3 objects)
    """

    storage: Storage
    bucket: Optional[str] = None
    s3_prefix: str = "datasets/pdb/mmCIF/"
    _staging_dir: Path = Path("/moldata/pdb/mmCIF")
    source: Literal["ebi", "rcsb"] = "rcsb"
    pdb_format: Literal["mmcif", "pdb"] = "mmcif"
    download_method: Literal["rsync", "https"] = "rsync"
    snapshot_year: Optional[int] = None
    upload_format: Literal["raw", "tar_shards"] = "raw"
    tar_shard_size: int = 1000
    https_download_workers: int = 8
    remove_local_on_end: bool = True

    @property
    def name(self) -> str:
        return "pdb"

    @property
    def staging_dir(self) -> Path:
        return self._staging_dir

    def describe(self) -> str:
        return "Protein Data Bank (divided mmCIF/PDB archive)."

    @property
    def _ext(self) -> str:
        return ".cif.gz" if self.pdb_format == "mmcif" else ".ent.gz"

    # --- Download ------------------------------------------------------------

    def download(self) -> None:
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        if self.snapshot_year:
            self._download_snapshot_rsync()
        elif self.download_method == "rsync":
            self._download_rsync()
        else:
            self._download_https_parallel()

    def _download_rsync(self) -> None:
        if self.source == "ebi" and self.pdb_format == "mmcif":
            url = RSYNC_EBI_MMCIF
            port_args: list[str] = []
        elif self.source == "ebi" and self.pdb_format == "pdb":
            url = RSYNC_EBI_PDB
            port_args = []
        elif self.source == "rcsb" and self.pdb_format == "mmcif":
            url = RSYNC_RCSB_MMCIF
            port_args = ["--port", str(RSYNC_RCSB_PORT)]
        else:
            url = RSYNC_RCSB_PDB
            port_args = ["--port", str(RSYNC_RCSB_PORT)]

        cmd = [
            "rsync", "-rlpt", "-v", "-z", "--delete",
            *port_args, url, str(self.staging_dir) + "/",
        ]
        logger.info("rsync PDB (source=%s format=%s) -> %s", self.source, self.pdb_format, self.staging_dir)
        subprocess.check_call(cmd)

    def _download_snapshot_rsync(self) -> None:
        """Download yearly snapshot via rsync from ftp_snapshots."""
        year = self.snapshot_year
        fmt = "mmCIF" if self.pdb_format == "mmcif" else "pdb"
        url = f"{RSYNC_SNAPSHOT_BASE}/{year}/pub/pdb/data/structures/divided/{fmt}/"
        cmd = [
            "rsync", "-rlpt", "-v", "-z",
            "--port", str(RSYNC_RCSB_PORT),
            url, str(self.staging_dir) + "/",
        ]
        logger.info("rsync PDB snapshot year=%d -> %s", year, self.staging_dir)
        subprocess.check_call(cmd)

    def _download_https_parallel(self) -> None:
        """HTTPS download with parallel workers per subdirectory."""
        base = HTTPS_MMCIF if self.pdb_format == "mmcif" else HTTPS_PDB
        ext = self._ext
        all_items: list[DownloadItem] = []

        logger.info("Scanning PDB HTTPS subdirectories for files...")
        try:
            from tqdm import tqdm
            subdirs_iter = tqdm(_get_divided_subdirs(), desc="Scanning subdirs")
        except ImportError:
            subdirs_iter = _get_divided_subdirs()

        for sub in subdirs_iter:
            sub_url = f"{base}/{sub}/"
            sub_path = self.staging_dir / sub
            sub_path.mkdir(parents=True, exist_ok=True)
            items = self._list_https_dir(sub_url, sub_path, ext)
            all_items.extend(items)

        if not all_items:
            logger.warning("No files found to download via HTTPS")
            return

        logger.info("Downloading %d PDB files via HTTPS (workers=%d)", len(all_items), self.https_download_workers)
        settings = load_settings()
        opts = DownloadOptions(
            max_workers=self.https_download_workers,
            batch_size=200,
            timeout=60,
            max_retries=3,
            skip_existing=True,
            use_checkpoint=True,
            checkpoint_dir=settings.checkpoint_dir,
        )
        downloaded, skipped = parallel_download(all_items, opts, prefix_label="pdb_https")
        logger.info("PDB HTTPS download done: downloaded=%d skipped=%d", downloaded, skipped)

    def _list_https_dir(self, url: str, dest: Path, ext: str) -> list[DownloadItem]:
        """Parse directory listing and return DownloadItems."""
        try:
            req = Request(url, headers={"User-Agent": "moldata/1.0"})
            with urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="ignore")
        except Exception:
            return []
        items: list[DownloadItem] = []
        for m in re.finditer(r'href="([^"]+)"', html, re.I):
            fname = m.group(1).strip()
            if not fname.endswith(ext):
                continue
            file_url = url.rstrip("/") + "/" + fname
            items.append(DownloadItem(url=file_url, dest=str(dest / fname)))
        return items

    # --- Upload --------------------------------------------------------------

    def upload(self, upload_options: Optional[UploadOptions] = None) -> None:
        opts = upload_options or self._default_upload_options()
        if self.upload_format == "tar_shards":
            self._upload_tar_shards(opts)
        else:
            self._upload_raw(opts)

    def _upload_raw(self, opts: UploadOptions) -> None:
        ext = self._ext
        logger.info("Uploading PDB raw to prefix=%s (workers=%d)", self.s3_prefix, opts.max_workers)
        items = [
            UploadItem(
                key=f"{self.s3_prefix}{p.relative_to(self.staging_dir).as_posix()}",
                path=str(p),
                size_bytes=p.stat().st_size,
            )
            for p in self.staging_dir.rglob(f"*{ext}")
        ]
        uploaded, skipped = parallel_upload(self.storage, items, opts, prefix_label="pdb")
        logger.info("PDB upload done: uploaded=%d skipped=%d", uploaded, skipped)

    def _upload_tar_shards(self, opts: UploadOptions) -> None:
        ext = self._ext
        files = sorted(self.staging_dir.rglob(f"*{ext}"))
        shards_dir = self.staging_dir.parent / "_shards"
        shards_dir.mkdir(parents=True, exist_ok=True)

        shard_items: list[UploadItem] = []
        for i in range(0, len(files), self.tar_shard_size):
            batch = files[i : i + self.tar_shard_size]
            shard_name = f"pdb_shard_{i:06d}_{i+len(batch):06d}.tar"
            shard_path = shards_dir / shard_name
            if not shard_path.exists():
                with tarfile.open(shard_path, "w") as tf:
                    for p in batch:
                        tf.add(str(p), arcname=p.relative_to(self.staging_dir).as_posix())
            key = f"{self.s3_prefix}shards/{shard_name}"
            shard_items.append(UploadItem(key=key, path=str(shard_path), size_bytes=shard_path.stat().st_size))

        logger.info("Uploading %d PDB tar shards to prefix=%s", len(shard_items), self.s3_prefix)
        uploaded, skipped = parallel_upload(self.storage, shard_items, opts, prefix_label="pdb_shards")
        logger.info("PDB shards upload done: uploaded=%d skipped=%d", uploaded, skipped)

    def _default_upload_options(self) -> UploadOptions:
        s = load_settings()
        return UploadOptions(
            max_workers=s.upload_max_workers,
            batch_size=s.upload_batch_size,
            checkpoint_dir=s.checkpoint_dir,
        )

    # --- Manifest ------------------------------------------------------------

    def build_manifest(self) -> Manifest:
        ext = self._ext
        fmt_tag = "cif" if self.pdb_format == "mmcif" else "ent"
        # Match both pdb1abc.cif.gz and 1abc.cif.gz (RCSB/EBI naming variants)
        pat = re.compile(rf"(?:pdb)?([0-9a-z]{{4}})\.{fmt_tag}\.gz$", re.I)
        rows = []
        for path in self.staging_dir.rglob(f"*{ext}"):
            m = pat.search(path.name)
            if not m:
                continue
            pdb_id = m.group(1).lower()
            rel = path.relative_to(self.staging_dir).as_posix()
            key = f"{self.s3_prefix}{rel}"
            rows.append({
                "sample_id": pdb_id,
                "dataset": "pdb",
                "subset": "mmCIF" if self.pdb_format == "mmcif" else "pdb",
                "uri": self._format_uri(key),
                "key": key,
                "size_bytes": int(path.stat().st_size),
            })
        if not rows:
            return Manifest(pd.DataFrame(columns=["sample_id", "dataset", "subset", "uri", "key", "size_bytes"]))
        df = pd.DataFrame(rows).sort_values("sample_id")
        return Manifest(df)

    def build_enriched_manifest(self) -> Manifest:
        """Build manifest enriched with mmCIF metadata (resolution, method, etc.)."""
        from moldata.parsers.mmcif import parse_mmcif

        ext = self._ext
        fmt_tag = "cif" if self.pdb_format == "mmcif" else "ent"
        # Match both pdb1abc.cif.gz and 1abc.cif.gz (RCSB/EBI naming variants)
        pat = re.compile(rf"(?:pdb)?([0-9a-z]{{4}})\.{fmt_tag}\.gz$", re.I)
        rows = []

        files = list(self.staging_dir.rglob(f"*{ext}"))
        try:
            from tqdm import tqdm
            files_iter = tqdm(files, desc="Parsing mmCIF", unit="file")
        except ImportError:
            files_iter = files

        for path in files_iter:
            m = pat.search(path.name)
            if not m:
                continue
            pdb_id = m.group(1).lower()
            rel = path.relative_to(self.staging_dir).as_posix()
            key = f"{self.s3_prefix}{rel}"
            row = {
                "sample_id": pdb_id,
                "dataset": "pdb",
                "subset": "mmCIF" if self.pdb_format == "mmcif" else "pdb",
                "uri": self._format_uri(key),
                "key": key,
                "size_bytes": int(path.stat().st_size),
            }
            info = parse_mmcif(path)
            if info:
                row["method"] = info.method
                row["resolution"] = info.resolution
                row["title"] = info.title
                row["space_group"] = info.space_group
                row["entity_count"] = info.entity_count
                row["polymer_entity_count"] = info.polymer_entity_count
                row["nonpolymer_entity_count"] = info.nonpolymer_entity_count
            rows.append(row)

        if not rows:
            return Manifest(pd.DataFrame(columns=[
                "sample_id", "dataset", "subset", "uri", "key", "size_bytes",
                "method", "resolution", "title", "space_group",
                "entity_count", "polymer_entity_count", "nonpolymer_entity_count",
            ]))
        df = pd.DataFrame(rows).sort_values("sample_id")
        return Manifest(df)

    def _format_uri(self, key: str) -> str:
        if self.bucket:
            return f"s3://{self.bucket}/{key}"
        return f"key://{key}"
