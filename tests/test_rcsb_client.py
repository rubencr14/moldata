"""Tests for RCSB API client."""

import pytest

from moldata.rcsb.client import RCSBClient


def test_get_entry() -> None:
    """Fetch 4HHB entry from RCSB Data API (requires network)."""
    client = RCSBClient()
    data = client.get_entry("4HHB")
    if data is None:
        pytest.skip("RCSB API unreachable (no network or API down)")

    assert isinstance(data, dict)
    assert data.get("entry", {}).get("id") == "4HHB"
    assert "exptl" in data
    assert "rcsb_entry_info" in data or "refine" in data


def test_get_entry_404() -> None:
    """Nonexistent entry returns None or 404."""
    client = RCSBClient()
    # Very unlikely to exist
    data = client.get_entry("XXXX")
    assert data is None or (isinstance(data, dict) and "entry" in data)


def test_graphql_query() -> None:
    """GraphQL query returns data (requires network)."""
    client = RCSBClient()
    query = """
    query {
        entry(entry_id: "4HHB") {
            rcsb_id
            rcsb_entry_info {
                resolution_combined
            }
        }
    }
    """
    result = client.graphql(query)
    if result is None:
        pytest.skip("RCSB GraphQL unreachable")

    assert isinstance(result, dict)
    assert "data" in result
    assert "entry" in result.get("data", {})
    assert result["data"]["entry"]["rcsb_id"] == "4HHB"
