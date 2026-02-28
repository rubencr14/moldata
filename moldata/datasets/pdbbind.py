from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from moldata.config import load_settings
from moldata.core.logging_utils import get_logger
from moldata.core.storage import Storage
from moldata.core.manifest import Manifest
from moldata.core.upload_utils import UploadItem, UploadOptions, parallel_upload
from moldata.datasets.base import BaseDataset

logger = get_logger(__name__)


@dataclass
class PDBBindDataset(BaseDataset):
    """PDBBind dataset manager.

    PDBBind bulk downloads usually require registration and license acceptance.
    This implementation supports a simple workflow:

      - mode="local": you provide the already-downloaded archive(s) into staging_dir
        (or you download them yourself outside of this tool).

    Extend `download()` with your preferred authenticated method if you have one.
    """

    storage: Storage
    bucket: Optional[str] = None
    s3_prefix: str = "datasets/pdbbind/"
    _staging_dir: Path = Path("/tmp/moldata/pdbbind")
    mode: str = "local"  # "local" | "official" (optional extension)
    remove_local_on_end: bool = True

    @property
    def name(self) -> str:
        return "pdbbind"

    @property
    def staging_dir(self) -> Path:
        return self._staging_dir

    def describe(self) -> str:
        return "PDBBind (structures + affinities; requires licensing for official bulk download)."

    def download(self) -> None:
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        if self.mode == "local":
            # Expect user-provided files already present. No-op by design.
            logger.info("PDBBind download mode=local; expecting archives already in %s", self.staging_dir)
            return
        raise NotImplementedError("Implement your authenticated download method for PDBBind if needed.")

    def upload(self, upload_options: Optional[UploadOptions] = None) -> None:
        opts = upload_options or self._default_upload_options()
        logger.info("Uploading staged PDBBind to storage prefix=%s (workers=%d)", self.s3_prefix, opts.max_workers)
        items = [
            UploadItem(
                key=f"{self.s3_prefix}{path.relative_to(self.staging_dir).as_posix()}",
                path=str(path),
                size_bytes=path.stat().st_size,
            )
            for path in self.staging_dir.rglob("*")
            if path.is_file()
        ]
        uploaded, skipped = parallel_upload(self.storage, items, opts, prefix_label="pdbbind")
        logger.info("PDBBind upload done: uploaded=%d skipped=%d", uploaded, skipped)

    def _default_upload_options(self) -> UploadOptions:
        s = load_settings()
        return UploadOptions(
            max_workers=s.upload_max_workers,
            batch_size=s.upload_batch_size,
            checkpoint_dir=s.checkpoint_dir,
        )

    def build_manifest(self) -> Manifest:
        # Minimal manifest: one row per file. You can replace this with a
        # "one row per complex" manifest once you parse the dataset structure.
        rows = []
        for path in self.staging_dir.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(self.staging_dir).as_posix()
            key = f"{self.s3_prefix}{rel}"
            rows.append(
                {
                    "sample_id": rel,  # placeholder
                    "dataset": "pdbbind",
                    "subset": "raw",
                    "uri": self._format_uri(key),
                    "key": key,
                    "size_bytes": int(path.stat().st_size),
                }
            )
        df = pd.DataFrame(rows).sort_values("sample_id")
        return Manifest(df)

    def _format_uri(self, key: str) -> str:
        if self.bucket:
            return f"s3://{self.bucket}/{key}"
        return f"key://{key}"
