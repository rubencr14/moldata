from __future__ import annotations

import logging
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from moldata.core.manifest import Manifest

if TYPE_CHECKING:
    from moldata.core.upload_utils import UploadOptions

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetStats:
    count: int
    size_bytes: Optional[int] = None


class BaseDataset(ABC):
    """A tiny dataset interface.

    Keep the interface small to avoid over-engineering.
    Each dataset decides the best strategy to download and index its data.
    """

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def staging_dir(self) -> Path: ...

    @abstractmethod
    def describe(self) -> str: ...

    @abstractmethod
    def download(self) -> None:
        """Download/sync raw data into staging_dir."""

    @abstractmethod
    def upload(self, upload_options: Optional["UploadOptions"] = None) -> None:
        """Upload staged raw data into configured storage."""

    @abstractmethod
    def build_manifest(self) -> Manifest:
        """Create a manifest describing the dataset content."""

    def stats(self, manifest: Manifest) -> DatasetStats:
        return DatasetStats(count=manifest.count(), size_bytes=manifest.size_bytes())

    def cleanup_staging(self) -> None:
        """Remove the local staging directory to free disk space."""
        d = self.staging_dir
        if d.exists():
            logger.info("Removing local staging directory: %s", d)
            shutil.rmtree(d)

    def prepare(
        self,
        manifest_path: Path,
        upload_options: Optional["UploadOptions"] = None,
        remove_local_on_end: bool = True,
    ) -> DatasetStats:
        """End-to-end pipeline: download -> upload -> manifest -> optional cleanup."""
        self.download()
        self.upload(upload_options=upload_options)
        manifest = self.build_manifest()
        manifest.save_parquet(manifest_path)
        st = self.stats(manifest)
        if remove_local_on_end:
            self.cleanup_staging()
        return st
