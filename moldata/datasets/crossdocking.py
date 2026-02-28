"""CrossDocking dataset (CrossDocked2020)."""

from __future__ import annotations

import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from moldata.config import load_settings
from moldata.core.logging_utils import get_logger
from moldata.core.manifest import Manifest
from moldata.core.storage import Storage
from moldata.core.upload_utils import UploadItem, UploadOptions, parallel_upload
from moldata.core.download_utils import DownloadItem, DownloadOptions, parallel_download
from moldata.datasets.base import BaseDataset

logger = get_logger(__name__)

CROSSDOCK_BASE_URL = "https://bits.csb.pitt.edu/files/crossdock2020/"
CROSSDOCK_ARCHIVES = {
    "v1.3": [
        "CrossDocked2020_v1.3.tgz",
    ],
    "v1.0": [
        "v1.0/CrossDocked2020.tgz",
        "v1.0/CrossDocked2020_receptors.tgz",
    ],
}
DEFAULT_VERSION = "v1.3"


@dataclass
class CrossDockingDataset(BaseDataset):
    """CrossDocked2020 dataset for structure-based drug design.

    Modes:
      - local: expect pre-downloaded + extracted files in staging_dir
      - official: download archives from Pitt server and auto-extract
    """

    storage: Storage
    bucket: Optional[str] = None
    s3_prefix: str = "datasets/crossdocking/"
    _staging_dir: Path = Path("/tmp/moldata/crossdocking")
    mode: str = "local"
    version: str = DEFAULT_VERSION
    remove_local_on_end: bool = True

    @property
    def name(self) -> str:
        return "crossdocking"

    @property
    def staging_dir(self) -> Path:
        return self._staging_dir

    def describe(self) -> str:
        return f"CrossDocked2020 {self.version} (protein-ligand docking benchmark)."

    def download(self) -> None:
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        if self.mode == "local":
            logger.info("CrossDocking mode=local; expecting files in %s", self.staging_dir)
            return
        if self.mode == "official":
            self._download_official()
            return
        raise ValueError(f"Unknown CrossDocking mode: {self.mode}")

    def _download_official(self) -> None:
        archives = CROSSDOCK_ARCHIVES.get(self.version, CROSSDOCK_ARCHIVES[DEFAULT_VERSION])
        settings = load_settings()
        items = []
        for arc in archives:
            url = f"{CROSSDOCK_BASE_URL}{arc}"
            dst = self.staging_dir / Path(arc).name
            items.append(DownloadItem(url=url, dest=str(dst)))

        opts = DownloadOptions(
            max_workers=2,
            batch_size=10,
            timeout=600,
            max_retries=3,
            skip_existing=True,
            use_checkpoint=True,
            checkpoint_dir=settings.checkpoint_dir,
        )
        downloaded, skipped = parallel_download(items, opts, prefix_label="crossdocking_dl")
        logger.info("CrossDocking download done: downloaded=%d skipped=%d", downloaded, skipped)

        for arc in archives:
            arc_path = self.staging_dir / Path(arc).name
            if arc_path.exists() and arc_path.suffix in (".tgz", ".gz"):
                self._extract_archive(arc_path)

    def _extract_archive(self, path: Path) -> None:
        """Extract .tgz / .tar.gz into staging dir."""
        marker = path.with_suffix(".extracted")
        if marker.exists():
            logger.info("Already extracted: %s", path.name)
            return
        logger.info("Extracting %s ...", path.name)
        try:
            with tarfile.open(path, "r:gz") as tf:
                tf.extractall(self.staging_dir)
            marker.touch()
        except Exception as e:
            logger.error("Failed to extract %s: %s", path, e)

    def upload(self, upload_options: Optional[UploadOptions] = None) -> None:
        opts = upload_options or self._default_upload_options()
        logger.info("Uploading CrossDocking to prefix=%s (workers=%d)", self.s3_prefix, opts.max_workers)
        items = [
            UploadItem(
                key=f"{self.s3_prefix}{p.relative_to(self.staging_dir).as_posix()}",
                path=str(p),
                size_bytes=p.stat().st_size,
            )
            for p in self.staging_dir.rglob("*")
            if p.is_file() and not p.name.endswith(".extracted")
        ]
        uploaded, skipped = parallel_upload(self.storage, items, opts, prefix_label="crossdocking")
        logger.info("CrossDocking upload done: uploaded=%d skipped=%d", uploaded, skipped)

    def _default_upload_options(self) -> UploadOptions:
        s = load_settings()
        return UploadOptions(
            max_workers=s.upload_max_workers,
            batch_size=s.upload_batch_size,
            checkpoint_dir=s.checkpoint_dir,
        )

    def build_manifest(self) -> Manifest:
        rows = []
        for path in self.staging_dir.rglob("*"):
            if not path.is_file() or path.name.endswith(".extracted"):
                continue
            rel = path.relative_to(self.staging_dir).as_posix()
            key = f"{self.s3_prefix}{rel}"
            rows.append({
                "sample_id": rel,
                "dataset": "crossdocking",
                "subset": self.version,
                "uri": self._format_uri(key),
                "key": key,
                "size_bytes": int(path.stat().st_size),
            })
        df = pd.DataFrame(rows).sort_values("sample_id")
        return Manifest(df)

    def _format_uri(self, key: str) -> str:
        if self.bucket:
            return f"s3://{self.bucket}/{key}"
        return f"key://{key}"
