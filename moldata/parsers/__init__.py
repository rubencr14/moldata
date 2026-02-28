"""moldata.parsers — SOLID molecular structure parsers.

Architecture:
    - base.py: Abstract interfaces (Structure, Atom, Chain, Residue, Entity)
    - mmcif.py: CIFStructure + CIFParser (mmCIF format)
    - pdb_format.py: PDBStructure + PDBFormatParser (PDB format)
    - dataset.py: StructureDataset (loads files, returns Structure objects)
    - enrich.py: RCSB API enrichment

Usage::

    from moldata.parsers import StructureDataset, CIFParser

    # Load from directory
    ds = StructureDataset.from_directory("/data/pdb/mmCIF", pattern="*.cif.gz")
    for structure in ds:
        print(structure.entry_id, structure.resolution, structure.num_chains)

    # Single file
    s = CIFParser().parse("1abc.cif.gz")
    for chain in s.chains:
        print(chain.chain_id, chain.sequence)

    # Auto-detect format
    from moldata.parsers import auto_parser
    parser = auto_parser("1abc.pdb")
    s = parser.parse("1abc.pdb")

    # Backward compatible
    from moldata.parsers import parse_mmcif
    info = parse_mmcif("1abc.cif.gz")  # returns MMCIFInfo
"""

from moldata.parsers.base import (
    Atom,
    Chain,
    Entity,
    Residue,
    Structure,
    StructureMetadata,
    StructureParser,
)
from moldata.parsers.mmcif import CIFParser, CIFStructure, MMCIFInfo, parse_mmcif
from moldata.parsers.pdb_format import PDBFormatParser, PDBStructure
from moldata.parsers.dataset import StructureDataset, auto_parser, register_parser
from moldata.parsers.enrich import enrich_mmcif_with_api

__all__ = [
    # Abstract interfaces
    "Structure",
    "StructureMetadata",
    "StructureParser",
    "Atom",
    "Chain",
    "Entity",
    "Residue",
    # Concrete parsers
    "CIFParser",
    "CIFStructure",
    "PDBFormatParser",
    "PDBStructure",
    # Dataset
    "StructureDataset",
    "auto_parser",
    "register_parser",
    # Backward compatible
    "MMCIFInfo",
    "parse_mmcif",
    "enrich_mmcif_with_api",
]
