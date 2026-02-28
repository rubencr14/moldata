from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class Manifest:
    """A dataset manifest.

    Convention:
      - one row per sample (or per file, depending on dataset)
      - `uri` is storage-agnostic: s3://bucket/key or file://...
    """

    df: pd.DataFrame

    def save_parquet(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_parquet(path, index=False)

    @staticmethod
    def load_parquet(path: Path) -> "Manifest":
        return Manifest(pd.read_parquet(path))

    def count(self) -> int:
        return int(len(self.df))

    def size_bytes(self) -> Optional[int]:
        if "size_bytes" not in self.df.columns:
            return None
        return int(self.df["size_bytes"].fillna(0).sum())
