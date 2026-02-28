#!/usr/bin/env python3
"""Enrich a PDB manifest with RCSB API metadata.

Usage:
    python examples/enrich_manifest.py --manifest manifests/pdb.parquet --output manifests/pdb_enriched.parquet
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from moldata.core.manifest import Manifest
from moldata.rcsb.client import RCSBClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    p = argparse.ArgumentParser(description="Enrich PDB manifest with RCSB API metadata")
    p.add_argument("--manifest", required=True, help="Input manifest parquet")
    p.add_argument("--output", required=True, help="Output enriched manifest parquet")
    p.add_argument("--batch-size", type=int, default=50, help="API batch size")
    args = p.parse_args()

    m = Manifest.load_parquet(Path(args.manifest))
    df = m.df.copy()
    client = RCSBClient()

    ids = df["sample_id"].unique().tolist()
    logger.info("Enriching %d entries from RCSB API...", len(ids))

    api_rows: dict[str, dict] = {}

    try:
        from tqdm import tqdm
        ids_iter = tqdm(ids, desc="RCSB API", unit="entry")
    except ImportError:
        ids_iter = ids

    for pdb_id in ids_iter:
        data = client.get_entry(pdb_id)
        if not data:
            continue
        info = data.get("rcsb_entry_info", {})
        api_rows[pdb_id] = {
            "api_resolution": (info.get("resolution_combined") or [None])[0],
            "api_method": data.get("exptl", [{}])[0].get("method") if data.get("exptl") else None,
            "api_polymer_entity_count": info.get("polymer_entity_count"),
            "api_nonpolymer_entity_count": info.get("nonpolymer_entity_count"),
            "api_molecular_weight": info.get("molecular_weight"),
            "api_deposit_date": data.get("rcsb_accession_info", {}).get("deposit_date"),
            "api_release_date": data.get("rcsb_accession_info", {}).get("initial_release_date"),
        }

    if api_rows:
        api_df = pd.DataFrame.from_dict(api_rows, orient="index")
        api_df.index.name = "sample_id"
        df = df.merge(api_df, on="sample_id", how="left")

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    Manifest(df).save_parquet(out)
    logger.info("Enriched manifest saved to %s (%d rows)", out, len(df))


if __name__ == "__main__":
    main()
