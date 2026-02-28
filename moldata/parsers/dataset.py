"""StructureDataset: load a list of structure files into Structure objects.

Provides an iterable collection of parsed structures, usable as input to
molfun's training pipelines or for standalone analysis.

Depends only on the Structure abstraction (Dependency Inversion), not on
specific parsers. New formats can be added by registering new parsers
without modifying this class (Open/Closed).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator, Optional, overload

from moldata.parsers.base import Structure, StructureParser

logger = logging.getLogger(__name__)

# ======================================================================
# Parser registry (Open/Closed: register new formats without changes)
# ======================================================================

_REGISTRY: dict[str, type[StructureParser]] = {}


def register_parser(parser_cls: type[StructureParser]) -> None:
    """Register a parser class for its declared extensions."""
    for ext in parser_cls.extensions():
        _REGISTRY[ext.lower()] = parser_cls


def _ensure_registry() -> None:
    if _REGISTRY:
        return
    from moldata.parsers.mmcif import CIFParser
    from moldata.parsers.pdb_format import PDBFormatParser
    register_parser(CIFParser)
    register_parser(PDBFormatParser)


def auto_parser(path: str | Path) -> StructureParser:
    """Return the appropriate parser for a file path based on extension."""
    _ensure_registry()
    name = str(path).lower()
    for ext in sorted(_REGISTRY, key=len, reverse=True):
        if name.endswith(ext):
            return _REGISTRY[ext]()
    available = sorted(set(_REGISTRY.keys()))
    raise ValueError(f"No parser for '{path}'. Supported: {available}")


# ======================================================================
# StructureDataset
# ======================================================================

class StructureDataset:
    """A dataset of parsed molecular structures.

    Loads structure files lazily (on access) and returns Structure objects.
    Compatible with molfun's StructureDataset interface for the path list.

    Usage::

        from moldata.parsers import StructureDataset

        ds = StructureDataset.from_paths(["1abc.cif.gz", "2xyz.pdb"])
        for structure in ds:
            print(structure.entry_id, structure.resolution)
            for chain in structure.chains:
                print(f"  Chain {chain.chain_id}: {chain.sequence[:50]}")

        # Index access
        s = ds[0]
        print(s.metadata.method)

        # Filter
        xray = ds.filter(lambda s: s.method and "X-RAY" in s.method)
    """

    def __init__(self, paths: list[Path], parser: Optional[StructureParser] = None):
        self._paths = paths
        self._parser = parser
        self._cache: dict[int, Structure] = {}

    @classmethod
    def from_paths(cls, paths: list[str | Path], parser: Optional[StructureParser] = None) -> "StructureDataset":
        """Create from a list of file paths (strings or Path objects)."""
        return cls([Path(p) for p in paths], parser=parser)

    @classmethod
    def from_directory(
        cls,
        directory: str | Path,
        pattern: str = "*.cif.gz",
        parser: Optional[StructureParser] = None,
    ) -> "StructureDataset":
        """Create from all matching files in a directory."""
        d = Path(directory)
        paths = sorted(d.rglob(pattern))
        logger.info("StructureDataset: found %d files matching '%s' in %s", len(paths), pattern, d)
        return cls(paths, parser=parser)

    def __len__(self) -> int:
        return len(self._paths)

    @overload
    def __getitem__(self, idx: int) -> Structure: ...
    @overload
    def __getitem__(self, idx: slice) -> list[Structure]: ...

    def __getitem__(self, idx):
        if isinstance(idx, slice):
            return [self._load(i) for i in range(*idx.indices(len(self)))]
        if idx < 0:
            idx = len(self) + idx
        return self._load(idx)

    def __iter__(self) -> Iterator[Structure]:
        for i in range(len(self)):
            yield self._load(i)

    def _load(self, idx: int) -> Structure:
        if idx in self._cache:
            return self._cache[idx]
        path = self._paths[idx]
        parser = self._parser or auto_parser(path)
        try:
            structure = parser.parse(path)
        except Exception as e:
            logger.error("Failed to parse %s: %s", path, e)
            raise
        self._cache[idx] = structure
        return structure

    @property
    def paths(self) -> list[Path]:
        return list(self._paths)

    @property
    def pdb_ids(self) -> list[str]:
        """Extract PDB IDs from all structures (parses lazily)."""
        return [s.entry_id for s in self]

    def filter(self, predicate) -> "StructureDataset":
        """Return a new dataset with only structures matching the predicate.

        The predicate receives a Structure and returns bool.
        Note: this triggers parsing of all structures.
        """
        indices = [i for i in range(len(self)) if predicate(self._load(i))]
        paths = [self._paths[i] for i in indices]
        ds = StructureDataset(paths, parser=self._parser)
        for new_idx, old_idx in enumerate(indices):
            if old_idx in self._cache:
                ds._cache[new_idx] = self._cache[old_idx]
        return ds

    def to_list(self) -> list[Structure]:
        """Parse all structures and return as a list."""
        return [self._load(i) for i in range(len(self))]

    def summary(self) -> dict:
        """Parse all and return summary statistics."""
        structures = self.to_list()
        resolutions = [s.resolution for s in structures if s.resolution is not None]
        methods = {}
        for s in structures:
            m = s.method or "unknown"
            methods[m] = methods.get(m, 0) + 1
        return {
            "total": len(structures),
            "resolution_mean": sum(resolutions) / len(resolutions) if resolutions else None,
            "resolution_min": min(resolutions) if resolutions else None,
            "resolution_max": max(resolutions) if resolutions else None,
            "methods": methods,
            "total_atoms": sum(s.num_atoms for s in structures),
            "total_chains": sum(s.num_chains for s in structures),
        }

    def __repr__(self) -> str:
        return f"<StructureDataset n={len(self)} paths={self._paths[:3]}...>"
