"""Abstract interfaces for molecular structure parsing (SOLID).

Interface Segregation: small, focused protocols.
Dependency Inversion: consumers depend on these abstractions, not concrete parsers.
Open/Closed: new formats extend without modifying existing code.

Hierarchy:
    Structure (top-level)
    ├── metadata: StructureMetadata
    ├── entities: list[Entity]
    │   └── chains: list[Chain]
    │       └── residues: list[Residue]
    │           └── atoms: list[Atom]
    └── atoms (flat view)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Optional


# ======================================================================
# Value objects
# ======================================================================

@dataclass(frozen=True)
class Atom:
    """Single atom with coordinates and identity."""

    serial: int
    name: str
    element: str
    x: float
    y: float
    z: float
    occupancy: float = 1.0
    b_factor: float = 0.0
    alt_id: str = ""
    charge: float = 0.0

    @property
    def coords(self) -> tuple[float, float, float]:
        return (self.x, self.y, self.z)


@dataclass(frozen=True)
class Residue:
    """Single residue (amino acid, nucleotide, or ligand)."""

    name: str
    seq_id: int
    atoms: tuple[Atom, ...] = ()
    one_letter: str = "X"
    ins_code: str = ""
    is_standard: bool = True

    @property
    def ca(self) -> Optional[Atom]:
        """Alpha-carbon atom, or None."""
        for a in self.atoms:
            if a.name.strip() == "CA":
                return a
        return None

    @property
    def num_atoms(self) -> int:
        return len(self.atoms)


@dataclass(frozen=True)
class Chain:
    """Single chain of residues."""

    chain_id: str
    residues: tuple[Residue, ...] = ()
    entity_id: str = ""

    @property
    def sequence(self) -> str:
        return "".join(r.one_letter for r in self.residues if r.is_standard)

    @property
    def num_residues(self) -> int:
        return len(self.residues)

    def __len__(self) -> int:
        return len(self.residues)

    def __iter__(self) -> Iterator[Residue]:
        return iter(self.residues)


@dataclass(frozen=True)
class Entity:
    """Biological entity (polymer, non-polymer, water, etc.)."""

    entity_id: str
    entity_type: str  # "polymer", "non-polymer", "water", "branched"
    description: str = ""
    chains: tuple[Chain, ...] = ()

    @property
    def is_polymer(self) -> bool:
        return self.entity_type == "polymer"

    @property
    def is_nonpolymer(self) -> bool:
        return self.entity_type == "non-polymer"

    @property
    def is_water(self) -> bool:
        return self.entity_type == "water"


@dataclass
class StructureMetadata:
    """Extracted metadata from a structure file."""

    entry_id: str
    format: str  # "mmcif" or "pdb"
    method: Optional[str] = None
    resolution: Optional[float] = None
    release_date: Optional[str] = None
    deposit_date: Optional[str] = None
    title: Optional[str] = None
    keywords: Optional[str] = None
    space_group: Optional[str] = None
    cell_a: Optional[float] = None
    cell_b: Optional[float] = None
    cell_c: Optional[float] = None
    cell_alpha: Optional[float] = None
    cell_beta: Optional[float] = None
    cell_gamma: Optional[float] = None
    raw: dict = field(default_factory=dict)


# ======================================================================
# Abstract Structure (Liskov: any Structure subclass is substitutable)
# ======================================================================

class Structure(ABC):
    """Abstract protein/macromolecular structure.

    Concrete implementations: CIFStructure, PDBStructure.
    Each is responsible for parsing one format (Single Responsibility).
    """

    @property
    @abstractmethod
    def metadata(self) -> StructureMetadata: ...

    @property
    @abstractmethod
    def entities(self) -> list[Entity]: ...

    @property
    @abstractmethod
    def chains(self) -> list[Chain]: ...

    @property
    @abstractmethod
    def atoms(self) -> list[Atom]: ...

    @property
    def entry_id(self) -> str:
        return self.metadata.entry_id

    @property
    def resolution(self) -> Optional[float]:
        return self.metadata.resolution

    @property
    def method(self) -> Optional[str]:
        return self.metadata.method

    @property
    def title(self) -> Optional[str]:
        return self.metadata.title

    @property
    def num_chains(self) -> int:
        return len(self.chains)

    @property
    def num_atoms(self) -> int:
        return len(self.atoms)

    @property
    def num_entities(self) -> int:
        return len(self.entities)

    @property
    def polymer_entity_count(self) -> int:
        return sum(1 for e in self.entities if e.is_polymer)

    @property
    def nonpolymer_entity_count(self) -> int:
        return sum(1 for e in self.entities if e.is_nonpolymer)

    @property
    def sequences(self) -> dict[str, str]:
        """Chain ID -> amino acid sequence."""
        return {c.chain_id: c.sequence for c in self.chains}

    @property
    def chain_ids(self) -> list[str]:
        return [c.chain_id for c in self.chains]

    def get_chain(self, chain_id: str) -> Optional[Chain]:
        for c in self.chains:
            if c.chain_id == chain_id:
                return c
        return None

    def to_dict(self) -> dict:
        """Flat dict for manifest / DataFrame usage."""
        m = self.metadata
        return {
            "entry_id": m.entry_id,
            "format": m.format,
            "method": m.method,
            "resolution": m.resolution,
            "release_date": m.release_date,
            "deposit_date": m.deposit_date,
            "title": m.title,
            "space_group": m.space_group,
            "entity_count": self.num_entities,
            "polymer_entity_count": self.polymer_entity_count,
            "nonpolymer_entity_count": self.nonpolymer_entity_count,
            "chain_count": self.num_chains,
            "atom_count": self.num_atoms,
        }

    def __repr__(self) -> str:
        return (
            f"<{type(self).__name__} {self.entry_id} "
            f"chains={self.num_chains} entities={self.num_entities} "
            f"atoms={self.num_atoms}>"
        )


# ======================================================================
# Parser protocol (factory)
# ======================================================================

class StructureParser(ABC):
    """Parse a file into a Structure object.

    Single Responsibility: one parser per format.
    """

    @abstractmethod
    def parse(self, path: Path) -> Structure:
        """Parse a file and return a Structure."""
        ...

    @staticmethod
    @abstractmethod
    def extensions() -> list[str]:
        """File extensions this parser handles (e.g. ['.cif', '.cif.gz'])."""
        ...
