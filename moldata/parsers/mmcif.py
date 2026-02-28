"""mmCIF parser — pure Python, no external dependencies.

Parses .cif and .cif.gz files into a CIFStructure object that implements
the Structure interface. Also provides backward-compatible MMCIFInfo and
parse_mmcif() for existing callers.

Single Responsibility: only handles mmCIF format.
"""

from __future__ import annotations

import gzip
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

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

logger = get_logger(__name__)

THREE_TO_ONE = {
    "ALA": "A", "ARG": "R", "ASN": "N", "ASP": "D", "CYS": "C",
    "GLN": "Q", "GLU": "E", "GLY": "G", "HIS": "H", "ILE": "I",
    "LEU": "L", "LYS": "K", "MET": "M", "PHE": "F", "PRO": "P",
    "SER": "S", "THR": "T", "TRP": "W", "TYR": "Y", "VAL": "V",
    "SEC": "U", "PYL": "O",
    "ASX": "B", "GLX": "Z", "XLE": "J", "UNK": "X",
}


# ======================================================================
# Low-level mmCIF tokenizer
# ======================================================================

def _unwrap_value(s: str) -> str:
    s = s.strip()
    if s.startswith("'") and s.endswith("'"):
        return s[1:-1].replace("''", "'")
    if s.startswith('"') and s.endswith('"'):
        return s[1:-1].replace('""', '"')
    if s in (".", "?"):
        return ""
    return s


def _tokenize_mmcif(path: Path) -> list[tuple[str, str]]:
    """Read mmCIF as (keyword, value) pairs from first data block."""
    pairs: list[tuple[str, str]] = []
    opener = gzip.open if path.suffix == ".gz" else open
    mode = "rt" if path.suffix == ".gz" else "r"

    with opener(path, mode, encoding="utf-8", errors="ignore") as f:
        in_loop = False
        loop_cols: list[str] = []
        loop_rows: list[list[str]] = []

        for line in f:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            if line.startswith("data_"):
                if pairs or loop_rows:
                    break
                continue
            if line.startswith("loop_"):
                if loop_cols and loop_rows:
                    _flush_loop(pairs, loop_cols, loop_rows)
                in_loop = True
                loop_cols = []
                loop_rows = []
                continue
            if in_loop:
                if line.startswith("_"):
                    loop_cols.append(line.strip().lstrip("_"))
                    continue
                vals = re.findall(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|[^\s]+", line)
                if vals and loop_cols:
                    loop_rows.append([_unwrap_value(v) for v in vals])
                continue
            if line.startswith("_") and (" " in line or "\t" in line):
                m = re.match(r"(_[^\s]+)\s+(.*)", line)
                if m:
                    pairs.append((m.group(1), m.group(2).strip()))

    if loop_cols and loop_rows:
        _flush_loop(pairs, loop_cols, loop_rows)
    return pairs


def _flush_loop(
    pairs: list[tuple[str, str]],
    cols: list[str],
    rows: list[list[str]],
) -> None:
    for i, col in enumerate(cols):
        for row in rows:
            if i < len(row):
                pairs.append((f"_{col}", row[i]))


def _get_single(pairs: list[tuple[str, str]], cat_attr: str) -> Optional[str]:
    key = f"_{cat_attr}"
    for k, v in pairs:
        if k and v and k.lower() == key.lower():
            u = _unwrap_value(v)
            return u if u else None
    return None


def _get_loop_values(pairs: list[tuple[str, str]], cat_attr: str) -> list[str]:
    col = f"_{cat_attr}".lower()
    return [_unwrap_value(v) for k, v in pairs if k and k.lower() == col and v]


def _opt_float(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ======================================================================
# CIFStructure: concrete Structure for mmCIF
# ======================================================================

class CIFStructure(Structure):
    """Parsed mmCIF structure with full SOLID interface.

    Lazily extracts entities, chains, residues, and atoms from the
    raw tokenized pairs on first access.
    """

    def __init__(self, pairs: list[tuple[str, str]], source_path: Optional[Path] = None):
        self._pairs = pairs
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
        g = lambda k, d=None: _get_single(self._pairs, k) or d

        entry_id = g("entry.id") or g("struct_keywords.entry_id") or ""
        if not entry_id and self._source_path:
            m = re.search(r"(?:pdb)?([0-9][a-z0-9]{3})\.cif", self._source_path.name, re.I)
            if m:
                entry_id = m.group(1).upper()

        method = g("exptl.method")
        if not method:
            for k, v in self._pairs:
                if "exptl" in k.lower() and "method" in k.lower():
                    method = _unwrap_value(v)
                    break

        res_raw = g("refine.ls_dres_high") or g("reflns.d_resolution_high") or g("refine_hist.d_res_high")

        return StructureMetadata(
            entry_id=entry_id,
            format="mmcif",
            method=method,
            resolution=_opt_float(res_raw),
            release_date=g("pdbx_database_status.recvd_initial_deposition_date") or g("rcsb_accession_info.initial_release_date"),
            deposit_date=g("pdbx_database_status.recvd_deposition_form") or g("rcsb_accession_info.deposit_date"),
            title=g("struct.title") or g("struct_keywords.text"),
            keywords=g("struct_keywords.text"),
            space_group=g("symmetry.space_group_name_H-M") or g("symmetry.space_group_name_hm"),
            cell_a=_opt_float(g("cell.length_a")),
            cell_b=_opt_float(g("cell.length_b")),
            cell_c=_opt_float(g("cell.length_c")),
            cell_alpha=_opt_float(g("cell.angle_alpha")),
            cell_beta=_opt_float(g("cell.angle_beta")),
            cell_gamma=_opt_float(g("cell.angle_gamma")),
            raw={k: v for k, v in self._pairs if k.startswith("_") and v},
        )

    def _build_entities(self) -> list[Entity]:
        ids = _get_loop_values(self._pairs, "entity.id")
        types = _get_loop_values(self._pairs, "entity.type")
        descs = _get_loop_values(self._pairs, "entity.pdbx_description")
        if not descs:
            descs = [""] * len(ids)

        entities = []
        for i, eid in enumerate(ids):
            etype = types[i] if i < len(types) else "unknown"
            desc = descs[i] if i < len(descs) else ""
            entities.append(Entity(
                entity_id=eid,
                entity_type=etype.lower(),
                description=_unwrap_value(desc),
            ))
        return entities

    def _build_atoms(self) -> list[Atom]:
        serials = _get_loop_values(self._pairs, "atom_site.id")
        names = _get_loop_values(self._pairs, "atom_site.label_atom_id")
        comp_ids = _get_loop_values(self._pairs, "atom_site.label_comp_id")
        xs = _get_loop_values(self._pairs, "atom_site.Cartn_x")
        ys = _get_loop_values(self._pairs, "atom_site.Cartn_y")
        zs = _get_loop_values(self._pairs, "atom_site.Cartn_z")
        elements = _get_loop_values(self._pairs, "atom_site.type_symbol")
        occupancies = _get_loop_values(self._pairs, "atom_site.occupancy")
        b_factors = _get_loop_values(self._pairs, "atom_site.B_iso_or_equiv")
        groups = _get_loop_values(self._pairs, "atom_site.group_PDB")

        atoms = []
        for i in range(len(serials)):
            try:
                atoms.append(Atom(
                    serial=int(serials[i]) if serials[i] else i + 1,
                    name=names[i] if i < len(names) else "",
                    element=elements[i] if i < len(elements) else "",
                    x=float(xs[i]) if i < len(xs) else 0.0,
                    y=float(ys[i]) if i < len(ys) else 0.0,
                    z=float(zs[i]) if i < len(zs) else 0.0,
                    occupancy=float(occupancies[i]) if i < len(occupancies) and occupancies[i] else 1.0,
                    b_factor=float(b_factors[i]) if i < len(b_factors) and b_factors[i] else 0.0,
                ))
            except (ValueError, IndexError):
                continue
        return atoms

    def _build_chains(self) -> list[Chain]:
        chain_ids_raw = _get_loop_values(self._pairs, "atom_site.auth_asym_id")
        if not chain_ids_raw:
            chain_ids_raw = _get_loop_values(self._pairs, "atom_site.label_asym_id")
        comp_ids = _get_loop_values(self._pairs, "atom_site.label_comp_id")
        seq_ids_raw = _get_loop_values(self._pairs, "atom_site.label_seq_id")
        atom_names = _get_loop_values(self._pairs, "atom_site.label_atom_id")
        xs = _get_loop_values(self._pairs, "atom_site.Cartn_x")
        ys = _get_loop_values(self._pairs, "atom_site.Cartn_y")
        zs = _get_loop_values(self._pairs, "atom_site.Cartn_z")
        serials = _get_loop_values(self._pairs, "atom_site.id")
        elements = _get_loop_values(self._pairs, "atom_site.type_symbol")
        groups = _get_loop_values(self._pairs, "atom_site.group_PDB")

        n = len(chain_ids_raw)
        if n == 0:
            return []

        chain_residues: dict[str, dict[int, list[tuple[str, Atom]]]] = {}

        for i in range(n):
            cid = chain_ids_raw[i] if i < len(chain_ids_raw) else "A"
            comp = comp_ids[i] if i < len(comp_ids) else "UNK"
            seq_str = seq_ids_raw[i] if i < len(seq_ids_raw) else ""
            if not seq_str or seq_str == ".":
                continue
            try:
                seq_id = int(seq_str)
            except ValueError:
                continue
            try:
                atom = Atom(
                    serial=int(serials[i]) if i < len(serials) and serials[i] else i + 1,
                    name=atom_names[i] if i < len(atom_names) else "",
                    element=elements[i] if i < len(elements) else "",
                    x=float(xs[i]) if i < len(xs) else 0.0,
                    y=float(ys[i]) if i < len(ys) else 0.0,
                    z=float(zs[i]) if i < len(zs) else 0.0,
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
                    name=comp_name,
                    seq_id=seq_id,
                    atoms=atoms_tuple,
                    one_letter=one,
                    is_standard=is_std,
                ))
            chains.append(Chain(chain_id=cid, residues=tuple(residues)))
        return chains


# ======================================================================
# CIFParser: StructureParser for mmCIF
# ======================================================================

class CIFParser(StructureParser):
    """Parse mmCIF files (.cif, .cif.gz) into CIFStructure."""

    def parse(self, path: Path) -> CIFStructure:
        path = Path(path)
        pairs = _tokenize_mmcif(path)
        return CIFStructure(pairs, source_path=path)

    @staticmethod
    def extensions() -> list[str]:
        return [".cif", ".cif.gz", ".mmcif"]


# ======================================================================
# Backward-compatible API (MMCIFInfo + parse_mmcif)
# ======================================================================

@dataclass
class MMCIFInfo:
    """Legacy metadata container. Prefer CIFStructure.metadata for new code."""

    entry_id: str
    method: Optional[str] = None
    resolution: Optional[float] = None
    resolution_high: Optional[float] = None
    release_date: Optional[str] = None
    deposit_date: Optional[str] = None
    title: Optional[str] = None
    keywords: Optional[str] = None
    space_group: Optional[str] = None
    cell_a: Optional[float] = None
    cell_b: Optional[float] = None
    cell_c: Optional[float] = None
    entity_count: Optional[int] = None
    polymer_entity_count: Optional[int] = None
    nonpolymer_entity_count: Optional[int] = None
    atom_count: Optional[int] = None
    raw: dict[str, Any] = field(default_factory=dict)


def parse_mmcif(path: Path | str) -> Optional[MMCIFInfo]:
    """Parse mmCIF and return legacy MMCIFInfo.

    Backward-compatible wrapper around CIFParser. For new code, use::

        structure = CIFParser().parse(path)
        structure.metadata  # StructureMetadata
        structure.entities  # list[Entity]
        structure.chains    # list[Chain]
    """
    path = Path(path)
    if not path.exists():
        logger.warning("mmCIF file not found: %s", path)
        return None
    try:
        s = CIFParser().parse(path)
    except Exception as e:
        logger.warning("Failed to parse mmCIF %s: %s", path, e)
        return None

    m = s.metadata
    if not m.entry_id:
        return None

    return MMCIFInfo(
        entry_id=m.entry_id,
        method=m.method,
        resolution=m.resolution,
        resolution_high=m.resolution,
        release_date=m.release_date,
        deposit_date=m.deposit_date,
        title=m.title,
        keywords=m.keywords,
        space_group=m.space_group,
        cell_a=m.cell_a,
        cell_b=m.cell_b,
        cell_c=m.cell_c,
        entity_count=s.num_entities,
        polymer_entity_count=s.polymer_entity_count,
        nonpolymer_entity_count=s.nonpolymer_entity_count,
        atom_count=s.num_atoms,
        raw=m.raw,
    )
