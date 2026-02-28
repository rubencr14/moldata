"""Convenience functions for RCSB Data API."""

from __future__ import annotations

from typing import Optional

from moldata.rcsb.client import RCSBClient

_default_client: Optional[RCSBClient] = None


def _client() -> RCSBClient:
    global _default_client
    if _default_client is None:
        _default_client = RCSBClient()
    return _default_client


def get_entry(entry_id: str) -> Optional[dict]:
    """Fetch entry metadata from RCSB Data API.

    Returns dict with keys like: entry, exptl, refine, rcsb_entry_info,
    rcsb_primary_citation, rcsb_accession_info, etc.
    """
    return _client().get_entry(entry_id)


def get_polymer_entity(entry_id: str, entity_id: str) -> Optional[dict]:
    """Fetch polymer entity metadata."""
    return _client().get_polymer_entity(entry_id, entity_id)


def get_assembly(entry_id: str, assembly_id: str) -> Optional[dict]:
    """Fetch assembly metadata."""
    return _client().get_assembly(entry_id, assembly_id)


def enrich_from_api(entry_id: str) -> Optional[dict]:
    """Enrich entry_id with full RCSB entry data. Combines mmCIF parse + API."""
    return get_entry(entry_id)
