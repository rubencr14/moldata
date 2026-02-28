"""RCSB Search API v2 query builders and executor.

Provides composable query node builders and a search function that returns
PDB IDs matching the query. Compatible with the RCSB Search API v2 spec.
"""

from __future__ import annotations

import json
import logging
from typing import Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)

SEARCH_URL = "https://search.rcsb.org/rcsbsearch/v2/query"


def _wrap_terminal(terminal: dict) -> dict:
    return {
        "type": "group",
        "logical_operator": "and",
        "nodes": [terminal],
        "label": terminal.get("service", "text"),
    }


def resolution_node(resolution_max: float) -> dict:
    return _wrap_terminal({
        "type": "terminal",
        "service": "text",
        "parameters": {
            "attribute": "rcsb_entry_info.resolution_combined",
            "operator": "less_or_equal",
            "value": resolution_max,
        },
    })


def pfam_node(pfam_id: str) -> dict:
    return _wrap_terminal({
        "type": "terminal",
        "service": "text",
        "parameters": {
            "attribute": "rcsb_polymer_entity_annotation.annotation_id",
            "operator": "exact_match",
            "value": pfam_id,
        },
    })


def uniprot_node(uniprot_id: str) -> dict:
    return _wrap_terminal({
        "type": "terminal",
        "service": "text",
        "parameters": {
            "attribute": (
                "rcsb_polymer_entity_container_identifiers"
                ".reference_sequence_identifiers.database_accession"
            ),
            "operator": "exact_match",
            "value": uniprot_id,
        },
    })


def ec_node(ec_number: str) -> dict:
    ec_clean = ec_number.rstrip(".*")
    return _wrap_terminal({
        "type": "terminal",
        "service": "text",
        "parameters": {
            "attribute": "rcsb_polymer_entity.rcsb_ec_lineage.id",
            "operator": "exact_match",
            "value": ec_clean,
        },
    })


def go_node(go_id: str) -> dict:
    return _wrap_terminal({
        "type": "terminal",
        "service": "text",
        "parameters": {
            "attribute": "rcsb_polymer_entity_annotation.annotation_lineage.id",
            "operator": "exact_match",
            "value": go_id,
        },
    })


def taxonomy_node(taxonomy_id: int) -> dict:
    return _wrap_terminal({
        "type": "terminal",
        "service": "text",
        "parameters": {
            "attribute": "rcsb_entity_source_organism.taxonomy_lineage.id",
            "operator": "exact_match",
            "value": str(taxonomy_id),
        },
    })


def keyword_node(keyword: str) -> dict:
    return _wrap_terminal({
        "type": "terminal",
        "service": "full_text",
        "parameters": {"value": keyword},
    })


def scop_node(scop_id: str) -> dict:
    return _wrap_terminal({
        "type": "terminal",
        "service": "text",
        "parameters": {
            "attribute": "rcsb_polymer_entity_annotation.annotation_lineage.id",
            "operator": "exact_match",
            "value": scop_id,
        },
    })


def and_query(*nodes: dict) -> dict:
    return {"query": {"type": "group", "logical_operator": "and", "nodes": list(nodes)}}


def search_rcsb(
    query: dict,
    max_results: int = 500,
    search_url: str = SEARCH_URL,
    timeout: float = 30,
) -> list[str]:
    """Execute an RCSB Search API v2 query and return PDB IDs."""
    payload = dict(query)
    payload["return_type"] = "entry"
    payload["request_options"] = {
        "paginate": {"start": 0, "rows": max_results},
        "results_content_type": ["experimental"],
    }

    body = json.dumps(payload).encode()
    req = Request(search_url, data=body, headers={
        "Content-Type": "application/json",
        "User-Agent": "moldata/1.0",
    })
    try:
        with urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
    except (HTTPError, URLError) as e:
        logger.error("RCSB search failed: %s", e)
        return []

    result_set = data.get("result_set") or []
    return [r["identifier"] for r in result_set if "identifier" in r]


def search_ids(
    *,
    pfam_id: Optional[str] = None,
    ec_number: Optional[str] = None,
    go_id: Optional[str] = None,
    taxonomy_id: Optional[int] = None,
    keyword: Optional[str] = None,
    uniprot_id: Optional[str] = None,
    scop_id: Optional[str] = None,
    max_results: int = 500,
    resolution_max: float = 3.0,
) -> list[str]:
    """Build a combined RCSB query from filters and return matching PDB IDs."""
    nodes = []
    if pfam_id:
        nodes.append(pfam_node(pfam_id))
    if ec_number:
        nodes.append(ec_node(ec_number))
    if go_id:
        nodes.append(go_node(go_id))
    if taxonomy_id is not None:
        nodes.append(taxonomy_node(taxonomy_id))
    if keyword:
        nodes.append(keyword_node(keyword))
    if uniprot_id:
        nodes.append(uniprot_node(uniprot_id))
    if scop_id:
        nodes.append(scop_node(scop_id))
    nodes.append(resolution_node(resolution_max))

    if len(nodes) < 2:
        raise ValueError("At least one filter (pfam_id, ec_number, etc.) is required.")

    query = and_query(*nodes)
    return search_rcsb(query, max_results=max_results)


def count_rcsb(
    query: dict,
    search_url: str = SEARCH_URL,
    timeout: float = 30,
) -> int:
    """Count matching entries without fetching them all."""
    payload = dict(query)
    payload["return_type"] = "entry"
    payload["request_options"] = {
        "paginate": {"start": 0, "rows": 0},
        "results_content_type": ["experimental"],
    }

    body = json.dumps(payload).encode()
    req = Request(search_url, data=body, headers={
        "Content-Type": "application/json",
        "User-Agent": "moldata/1.0",
    })
    try:
        with urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
    except (HTTPError, URLError):
        return 0

    return data.get("total_count", 0)
