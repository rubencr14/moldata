"""Enrich mmCIF metadata with RCSB API data."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from moldata.parsers.mmcif import MMCIFInfo, parse_mmcif
from moldata.rcsb.client import RCSBClient


def enrich_mmcif_with_api(
    path: Path | str,
    client: Optional[RCSBClient] = None,
) -> Optional[dict]:
    """Parse mmCIF and enrich with RCSB API data.

    Returns merged dict with:
      - mmcif: MMCIFInfo as dict
      - api: full RCSB entry response (or None if API fails)
    """
    info = parse_mmcif(path)
    if not info:
        return None
    cl = client or RCSBClient()
    api_data = cl.get_entry(info.entry_id.upper())
    return {
        "mmcif": {
            "entry_id": info.entry_id,
            "method": info.method,
            "resolution": info.resolution,
            "release_date": info.release_date,
            "deposit_date": info.deposit_date,
            "title": info.title,
            "space_group": info.space_group,
            "entity_count": info.entity_count,
            "polymer_entity_count": info.polymer_entity_count,
            "nonpolymer_entity_count": info.nonpolymer_entity_count,
        },
        "api": api_data,
    }
