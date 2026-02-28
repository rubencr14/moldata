"""Legacy PDB format parser — pure Python, no external dependencies.

Parses .pdb and .ent.gz files into a PDBStructure object that
implements the Structure interface.

Single Responsibility: only handles PDB format.
"""

from __future__ import annotations

import gzip
import re
from pathlib import Path
from typing import Optional

from moldata.core.logging_utils import get_logger
from moldata.parsers.base import (
    Atom,
    Chain,
    Entity,
    Residue,
    Structure,
    StructureMetadata,
    StructureParser,
)
from moldata.parsers.mmcif import THREE_TO_ONE

logger = get_logger(__name__)


class PDBStructure(Structure):
    """Parsed PDB-format structure with full SOLID interface."""

    def __init__(self, lines: list[str], source_path: Optional[Path] = None):
        self._lines = lines
        self._source_path = source_path
        self._metadata: Optional[StructureMetadata] = None
        self._entities: Optional[list[Entity]] = None
        self._chains: Optional[list[Chain]] = None
        self._atoms: Optional[list[Atom]] = None

    @property
    def metadata(self) -> StructureMetadata:
        if self._metadata is None:
            self._metadata = self._build_metadata()
        return self._metadata

    @property
    def entities(self) -> list[Entity]:
        if self._entities is None:
            self._entities = self._build_entities()
        return self._entities

    @property
    def chains(self) -> list[Chain]:
        if self._chains is None:
            self._chains = self._build_chains()
        return self._chains

    @property
    def atoms(self) -> list[Atom]:
        if self._atoms is None:
            self._atoms = self._build_atoms()
        return self._atoms

    def _build_metadata(self) -> StructureMetadata:
        entry_id = ""
        title = ""
        method = None
        resolution = None
        space_group = None
        cell_a = cell_b = cell_c = None
        cell_alpha = cell_beta = cell_gamma = None
        deposit_date = None
        raw: dict = {}

        for line in self._lines:
            rec = line[:6].strip()

            if rec == "HEADER":
                entry_id = line[62:66].strip()
                date_str = line[50:59].strip()
                if date_str:
                    deposit_date = date_str

            elif rec == "TITLE":
                title += line[10:80].strip() + " "

            elif rec == "EXPDTA":
                method = line[10:79].strip()

            elif rec == "REMARK":
                remark_num = line[7:10].strip()
                if remark_num == "2" and "RESOLUTION" in line.upper():
                    m = re.search(r"(\d+\.\d+)\s*ANGSTROM", line, re.I)
                    if m:
                        resolution = float(m.group(1))

            elif rec == "CRYST1":
                try:
                    cell_a = float(line[6:15])
                    cell_b = float(line[15:24])
                    cell_c = float(line[24:33])
                    cell_alpha = float(line[33:40])
                    cell_beta = float(line[40:47])
                    cell_gamma = float(line[47:54])
                    space_group = line[55:66].strip()
                except (ValueError, IndexError):
                    pass

        if not entry_id and self._source_path:
            m = re.search(r"(?:pdb)?([0-9][a-z0-9]{3})", self._source_path.stem, re.I)
            if m:
                entry_id = m.group(1).upper()

        return StructureMetadata(
            entry_id=entry_id,
            format="pdb",
            method=method,
            resolution=resolution,
            deposit_date=deposit_date,
            title=title.strip() or None,
            space_group=space_group,
            cell_a=cell_a,
            cell_b=cell_b,
            cell_c=cell_c,
            cell_alpha=cell_alpha,
            cell_beta=cell_beta,
            cell_gamma=cell_gamma,
            raw=raw,
        )

    def _build_atoms(self) -> list[Atom]:
        atoms = []
        for line in self._lines:
            rec = line[:6].strip()
            if rec not in ("ATOM", "HETATM"):
                continue
            try:
                atoms.append(Atom(
                    serial=int(line[6:11]),
                    name=line[12:16].strip(),
                    element=line[76:78].strip() if len(line) > 77 else "",
                    x=float(line[30:38]),
                    y=float(line[38:46]),
                    z=float(line[46:54]),
                    occupancy=float(line[54:60]) if len(line) > 59 else 1.0,
                    b_factor=float(line[60:66]) if len(line) > 65 else 0.0,
                ))
            except (ValueError, IndexError):
                continue
        return atoms

    def _build_chains(self) -> list[Chain]:
        chain_residues: dict[str, dict[int, list[tuple[str, Atom]]]] = {}

        for line in self._lines:
            rec = line[:6].strip()
            if rec not in ("ATOM",):
                continue
            try:
                cid = line[21]
                comp = line[17:20].strip()
                seq_id = int(line[22:26])
                atom = Atom(
                    serial=int(line[6:11]),
                    name=line[12:16].strip(),
                    element=line[76:78].strip() if len(line) > 77 else "",
                    x=float(line[30:38]),
                    y=float(line[38:46]),
                    z=float(line[46:54]),
                )
            except (ValueError, IndexError):
                continue

            if cid not in chain_residues:
                chain_residues[cid] = {}
            if seq_id not in chain_residues[cid]:
                chain_residues[cid][seq_id] = []
            chain_residues[cid][seq_id].append((comp, atom))

        chains = []
        for cid, res_map in chain_residues.items():
            residues = []
            for seq_id in sorted(res_map):
                entries = res_map[seq_id]
                comp_name = entries[0][0]
                atoms_tuple = tuple(e[1] for e in entries)
                one = THREE_TO_ONE.get(comp_name.upper(), "X")
                is_std = comp_name.upper() in THREE_TO_ONE
                residues.append(Residue(
                    name=comp_name, seq_id=seq_id, atoms=atoms_tuple,
                    one_letter=one, is_standard=is_std,
                ))
            chains.append(Chain(chain_id=cid, residues=tuple(residues)))
        return chains

    def _build_entities(self) -> list[Entity]:
        compnd_polymer_ids: set[str] = set()
        het_ids: set[str] = set()
        has_water = False

        for line in self._lines:
            rec = line[:6].strip()
            if rec == "ATOM":
                cid = line[21]
                compnd_polymer_ids.add(cid)
            elif rec == "HETATM":
                comp = line[17:20].strip()
                if comp in ("HOH", "WAT", "DOD"):
                    has_water = True
                else:
                    het_ids.add(comp)

        entities: list[Entity] = []
        eid = 1
        for cid in sorted(compnd_polymer_ids):
            entities.append(Entity(entity_id=str(eid), entity_type="polymer"))
            eid += 1
        for hid in sorted(het_ids):
            entities.append(Entity(entity_id=str(eid), entity_type="non-polymer", description=hid))
            eid += 1
        if has_water:
            entities.append(Entity(entity_id=str(eid), entity_type="water"))
        return entities


class PDBFormatParser(StructureParser):
    """Parse PDB-format files (.pdb, .ent, .ent.gz) into PDBStructure."""

    def parse(self, path: Path) -> PDBStructure:
        path = Path(path)
        lines = self._read_lines(path)
        return PDBStructure(lines, source_path=path)

    @staticmethod
    def _read_lines(path: Path) -> list[str]:
        opener = gzip.open if path.suffix == ".gz" else open
        mode = "rt" if path.suffix == ".gz" else "r"
        with opener(path, mode, encoding="utf-8", errors="ignore") as f:
            return f.readlines()

    @staticmethod
    def extensions() -> list[str]:
        return [".pdb", ".ent", ".ent.gz"]
