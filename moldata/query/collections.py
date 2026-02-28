"""Pre-defined protein family collections.

Mirror of molfun's collections, usable standalone from moldata.
Each collection maps to RCSB Search API filters (Pfam, EC, GO, taxonomy, keyword).

Usage::

    from moldata.query.collections import COLLECTIONS, list_collections

    for c in list_collections(tag="kinase"):
        print(c.name, c.description)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class CollectionSpec:
    """Defines a reusable protein collection query."""

    name: str
    description: str
    pfam_id: Optional[str] = None
    ec_number: Optional[str] = None
    go_id: Optional[str] = None
    taxonomy_id: Optional[int] = None
    keyword: Optional[str] = None
    uniprot_ids: Optional[list[str]] = None
    default_resolution: float = 3.0
    default_max: int = 500
    tags: list[str] = field(default_factory=list)


COLLECTIONS: dict[str, CollectionSpec] = {}


def _reg(spec: CollectionSpec) -> None:
    COLLECTIONS[spec.name] = spec


# -- Kinases ----------------------------------------------------------------

_reg(CollectionSpec(
    name="kinases", description="Protein kinases (Pfam PF00069)",
    pfam_id="PF00069", tags=["enzyme", "kinase", "signaling"],
))
_reg(CollectionSpec(
    name="kinases_human", description="Human protein kinases",
    pfam_id="PF00069", taxonomy_id=9606, default_resolution=2.5,
    tags=["enzyme", "kinase", "signaling", "human"],
))
_reg(CollectionSpec(
    name="tyrosine_kinases", description="Tyrosine kinases (EC 2.7.10)",
    ec_number="2.7.10", tags=["enzyme", "kinase", "tyrosine_kinase"],
))
_reg(CollectionSpec(
    name="serine_threonine_kinases", description="Serine/threonine kinases (EC 2.7.11)",
    ec_number="2.7.11", tags=["enzyme", "kinase", "serine_threonine_kinase"],
))

# -- Proteases --------------------------------------------------------------

_reg(CollectionSpec(
    name="serine_proteases", description="Serine proteases (Pfam PF00089)",
    pfam_id="PF00089", default_resolution=2.5, tags=["enzyme", "protease", "serine"],
))
_reg(CollectionSpec(
    name="metalloproteases", description="Zinc metalloproteases (Pfam PF00557)",
    pfam_id="PF00557", tags=["enzyme", "protease", "metalloprotease"],
))
_reg(CollectionSpec(
    name="cysteine_proteases", description="Cysteine proteases — Papain (Pfam PF00112)",
    pfam_id="PF00112", tags=["enzyme", "protease", "cysteine"],
))

# -- GPCRs ------------------------------------------------------------------

_reg(CollectionSpec(
    name="gpcr", description="G-protein coupled receptors (Pfam PF00001)",
    pfam_id="PF00001", default_resolution=3.5, default_max=300,
    tags=["receptor", "membrane", "gpcr"],
))
_reg(CollectionSpec(
    name="gpcr_human", description="Human GPCRs",
    pfam_id="PF00001", taxonomy_id=9606, default_resolution=3.5,
    tags=["receptor", "membrane", "gpcr", "human"],
))

# -- Ion channels -----------------------------------------------------------

_reg(CollectionSpec(
    name="ion_channels", description="Voltage-gated K+ channels (Pfam PF00520)",
    pfam_id="PF00520", default_resolution=3.5,
    tags=["channel", "membrane", "ion_channel"],
))

# -- Immunoglobulins --------------------------------------------------------

_reg(CollectionSpec(
    name="immunoglobulins", description="Immunoglobulin domains (Pfam PF07654)",
    pfam_id="PF07654", tags=["immune", "antibody", "immunoglobulin"],
))
_reg(CollectionSpec(
    name="nanobodies", description="Nanobody / VHH domains (Pfam PF07686)",
    pfam_id="PF07686", keyword="nanobody",
    tags=["immune", "nanobody", "therapeutic"],
))

# -- Globins ----------------------------------------------------------------

_reg(CollectionSpec(
    name="globins", description="Globin family (Pfam PF00042)",
    pfam_id="PF00042", default_resolution=2.5, tags=["oxygen_transport", "globin"],
))

# -- SH2 / SH3 -------------------------------------------------------------

_reg(CollectionSpec(
    name="sh2_domains", description="SH2 phosphotyrosine-binding (Pfam PF00017)",
    pfam_id="PF00017", default_resolution=2.5, tags=["signaling", "sh2", "domain"],
))
_reg(CollectionSpec(
    name="sh3_domains", description="SH3 domains (Pfam PF00018)",
    pfam_id="PF00018", default_resolution=2.5, tags=["signaling", "sh3", "domain"],
))

# -- Nucleic-acid binding ---------------------------------------------------

_reg(CollectionSpec(
    name="zinc_fingers", description="Zinc finger C2H2 (Pfam PF00096)",
    pfam_id="PF00096", tags=["transcription_factor", "zinc_finger", "dna_binding"],
))
_reg(CollectionSpec(
    name="helicase", description="DEAD-box RNA helicases (Pfam PF00270)",
    pfam_id="PF00270", tags=["enzyme", "helicase", "rna"],
))

# -- Oxidoreductases --------------------------------------------------------

_reg(CollectionSpec(
    name="p450", description="Cytochrome P450 (Pfam PF00067)",
    pfam_id="PF00067", tags=["enzyme", "oxidoreductase", "p450", "drug_metabolism"],
))

# -- Organism-specific ------------------------------------------------------

_reg(CollectionSpec(
    name="human_all", description="All human protein structures",
    taxonomy_id=9606, default_max=1000, tags=["human", "all"],
))
_reg(CollectionSpec(
    name="sars_cov2", description="SARS-CoV-2 protein structures",
    taxonomy_id=2697049, default_resolution=3.5, tags=["virus", "covid", "sars"],
))


# -- Public API -------------------------------------------------------------

def list_collections(tag: Optional[str] = None) -> list[CollectionSpec]:
    """List available collections, optionally filtered by tag."""
    if tag is None:
        return list(COLLECTIONS.values())
    return [c for c in COLLECTIONS.values() if tag in c.tags]


def get_collection(name: str) -> CollectionSpec:
    """Get a collection by name. Raises ValueError if not found."""
    if name not in COLLECTIONS:
        available = ", ".join(sorted(COLLECTIONS.keys()))
        raise ValueError(f"Unknown collection '{name}'. Available: {available}")
    return COLLECTIONS[name]
