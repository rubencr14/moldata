from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from moldata.core.manifest import Manifest


def random_split(
    manifest: Manifest,
    seed: int,
    ratios: Tuple[float, float, float] = (0.8, 0.1, 0.1),
    group_col: Optional[str] = None,
) -> Manifest:
    """Create reproducible random splits.

    If `group_col` is provided, splitting is done at group-level (no leakage).
    """
    df = manifest.df.copy()
    rng = np.random.default_rng(seed)

    if group_col:
        groups = df[group_col].dropna().unique()
        rng.shuffle(groups)
        n = len(groups)
        n_train = int(ratios[0] * n)
        n_val = int(ratios[1] * n)
        train_g = set(groups[:n_train])
        val_g = set(groups[n_train : n_train + n_val])

        def tag(g: object) -> str:
            if g in train_g:
                return "train"
            if g in val_g:
                return "val"
            return "test"

        df["split"] = df[group_col].map(tag)
    else:
        idx = np.arange(len(df))
        rng.shuffle(idx)
        n = len(df)
        n_train = int(ratios[0] * n)
        n_val = int(ratios[1] * n)
        split = np.array(["test"] * n, dtype=object)
        split[idx[:n_train]] = "train"
        split[idx[n_train : n_train + n_val]] = "val"
        df["split"] = split

    return Manifest(df)
