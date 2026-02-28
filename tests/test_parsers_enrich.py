"""Tests for mmCIF + RCSB enrichment."""

from pathlib import Path

import pytest

from moldata.parsers.enrich import enrich_mmcif_with_api

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_enrich_mmcif_with_api_local() -> None:
    """Enrich parses mmCIF and optionally fetches API (API may fail offline)."""
    path = FIXTURES / "sample.cif"
    result = enrich_mmcif_with_api(path)
    assert result is not None
    assert "mmcif" in result
    assert result["mmcif"]["entry_id"] == "4HHB"
    assert result["mmcif"]["method"] == "X-RAY DIFFRACTION"
    assert result["mmcif"]["resolution"] == 1.74
    # api may be None if offline
    if result.get("api"):
        assert result["api"].get("entry", {}).get("id") == "4HHB"
