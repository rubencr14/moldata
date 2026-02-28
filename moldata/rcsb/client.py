"""RCSB API client: Data API REST, GraphQL, Search."""

from __future__ import annotations

import json
from typing import Any, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

DATA_API_BASE = "https://data.rcsb.org/rest/v1/core"
GRAPHQL_URL = "https://data.rcsb.org/graphql"
SEARCH_URL = "https://search.rcsb.org/rcsbsearch/v2/query"


def _request(
    url: str,
    method: str = "GET",
    data: Optional[dict | str] = None,
    timeout: float = 30,
) -> Optional[dict | list]:
    """Execute HTTP request and return JSON or None."""
    headers = {"User-Agent": "moldata/1.0", "Content-Type": "application/json"}
    req = Request(url, method=method, headers=headers)
    if data is not None:
        body = json.dumps(data).encode("utf-8") if isinstance(data, dict) else data.encode("utf-8")
        req.data = body
    try:
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (HTTPError, URLError, json.JSONDecodeError) as e:
        return None


class RCSBClient:
    """Client for RCSB Data API, GraphQL, and Search API."""

    def __init__(
        self,
        data_base: str = DATA_API_BASE,
        graphql_url: str = GRAPHQL_URL,
        search_url: str = SEARCH_URL,
        timeout: float = 30,
    ) -> None:
        self.data_base = data_base.rstrip("/")
        self.graphql_url = graphql_url
        self.search_url = search_url
        self.timeout = timeout

    # --- Data API REST -------------------------------------------------------

    def get_entry(self, entry_id: str) -> Optional[dict]:
        """GET /rest/v1/core/entry/{entry_id}"""
        url = f"{self.data_base}/entry/{entry_id.upper()}"
        return _request(url, timeout=self.timeout)

    def get_polymer_entity(self, entry_id: str, entity_id: str) -> Optional[dict]:
        """GET /rest/v1/core/polymer_entity/{entry_id}/{entity_id}"""
        url = f"{self.data_base}/polymer_entity/{entry_id.upper()}/{entity_id}"
        return _request(url, timeout=self.timeout)

    def get_assembly(self, entry_id: str, assembly_id: str) -> Optional[dict]:
        """GET /rest/v1/core/assembly/{entry_id}/{assembly_id}"""
        url = f"{self.data_base}/assembly/{entry_id.upper()}/{assembly_id}"
        return _request(url, timeout=self.timeout)

    def get_nonpolymer_entity(self, entry_id: str, entity_id: str) -> Optional[dict]:
        """GET /rest/v1/core/nonpolymer_entity/{entry_id}/{entity_id}"""
        url = f"{self.data_base}/nonpolymer_entity/{entry_id.upper()}/{entity_id}"
        return _request(url, timeout=self.timeout)

    def get_chem_comp(self, comp_id: str) -> Optional[dict]:
        """GET /rest/v1/core/chem_comp/{comp_id}"""
        url = f"{self.data_base}/chem_comp/{comp_id.upper()}"
        return _request(url, timeout=self.timeout)

    # --- GraphQL -------------------------------------------------------------

    def graphql(self, query: str, variables: Optional[dict] = None) -> Optional[dict]:
        """POST to GraphQL endpoint."""
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables
        return _request(self.graphql_url, method="POST", data=payload, timeout=self.timeout)

    # --- Search API ----------------------------------------------------------

    def search(self, query: dict) -> Optional[dict]:
        """POST query to Search API. Returns result with 'result_set'."""
        return _request(self.search_url, method="POST", data=query, timeout=self.timeout)
