"""Tests for SOLID parser classes: Structure, CIFStructure, PDBStructure, StructureDataset."""

from pathlib import Path

import pytest

from moldata.parsers.base import Atom, Chain, Entity, Residue, Structure, StructureMetadata
from moldata.parsers.mmcif import CIFParser, CIFStructure
from moldata.parsers.pdb_format import PDBFormatParser, PDBStructure
from moldata.parsers.dataset import StructureDataset, auto_parser, register_parser

FIXTURES = Path(__file__).resolve().parent / "fixtures"


# -- CIFParser tests ---------------------------------------------------------


class TestCIFParser:
    def test_parse_sample_cif(self):
        path = FIXTURES / "sample.cif"
        s = CIFParser().parse(path)
        assert isinstance(s, CIFStructure)
        assert isinstance(s, Structure)

    def test_metadata(self):
        s = CIFParser().parse(FIXTURES / "sample.cif")
        m = s.metadata
        assert isinstance(m, StructureMetadata)
        assert m.entry_id == "4HHB"
        assert m.format == "mmcif"
        assert m.method == "X-RAY DIFFRACTION"
        assert m.resolution == 1.74
        assert m.release_date == "1984-03-07"
        assert m.space_group == "P 1 21 1"
        assert m.cell_a == 63.15
        assert m.cell_b == 83.59
        assert m.cell_c == 53.80

    def test_entities(self):
        s = CIFParser().parse(FIXTURES / "sample.cif")
        assert s.num_entities == 5
        assert s.polymer_entity_count == 2
        assert s.nonpolymer_entity_count == 2
        water = [e for e in s.entities if e.is_water]
        assert len(water) == 1

    def test_entity_interface(self):
        s = CIFParser().parse(FIXTURES / "sample.cif")
        for e in s.entities:
            assert isinstance(e, Entity)
            assert hasattr(e, "entity_id")
            assert hasattr(e, "entity_type")
            assert hasattr(e, "is_polymer")
            assert hasattr(e, "is_nonpolymer")
            assert hasattr(e, "is_water")

    def test_atoms(self):
        s = CIFParser().parse(FIXTURES / "sample.cif")
        assert s.num_atoms == 2
        for a in s.atoms:
            assert isinstance(a, Atom)
            assert isinstance(a.coords, tuple)
            assert len(a.coords) == 3

    def test_to_dict(self):
        s = CIFParser().parse(FIXTURES / "sample.cif")
        d = s.to_dict()
        assert d["entry_id"] == "4HHB"
        assert d["format"] == "mmcif"
        assert d["entity_count"] == 5
        assert d["polymer_entity_count"] == 2
        assert "chain_count" in d
        assert "atom_count" in d

    def test_repr(self):
        s = CIFParser().parse(FIXTURES / "sample.cif")
        r = repr(s)
        assert "CIFStructure" in r
        assert "4HHB" in r

    def test_convenience_properties(self):
        s = CIFParser().parse(FIXTURES / "sample.cif")
        assert s.entry_id == "4HHB"
        assert s.resolution == 1.74
        assert s.method == "X-RAY DIFFRACTION"
        assert "DEOXYHAEMOGLOBIN" in (s.title or "")

    def test_extensions(self):
        exts = CIFParser.extensions()
        assert ".cif" in exts
        assert ".cif.gz" in exts


# -- Residue and Chain tests -------------------------------------------------


class TestResidueAndChain:
    def test_residue_frozen(self):
        r = Residue(name="GLY", seq_id=1, one_letter="G")
        with pytest.raises(AttributeError):
            r.name = "ALA"

    def test_atom_frozen(self):
        a = Atom(serial=1, name="CA", element="C", x=1.0, y=2.0, z=3.0)
        assert a.coords == (1.0, 2.0, 3.0)
        with pytest.raises(AttributeError):
            a.x = 5.0

    def test_chain_sequence(self):
        residues = (
            Residue(name="GLY", seq_id=1, one_letter="G"),
            Residue(name="ALA", seq_id=2, one_letter="A"),
            Residue(name="VAL", seq_id=3, one_letter="V"),
        )
        c = Chain(chain_id="A", residues=residues)
        assert c.sequence == "GAV"
        assert c.num_residues == 3
        assert len(c) == 3

    def test_chain_iteration(self):
        residues = (
            Residue(name="GLY", seq_id=1, one_letter="G"),
            Residue(name="ALA", seq_id=2, one_letter="A"),
        )
        c = Chain(chain_id="A", residues=residues)
        names = [r.name for r in c]
        assert names == ["GLY", "ALA"]

    def test_residue_ca_found(self):
        ca = Atom(serial=2, name="CA", element="C", x=11.0, y=21.0, z=31.0)
        n = Atom(serial=1, name="N", element="N", x=10.0, y=20.0, z=30.0)
        r = Residue(name="GLY", seq_id=1, atoms=(n, ca), one_letter="G")
        assert r.ca is not None
        assert r.ca.name == "CA"

    def test_residue_ca_missing(self):
        n = Atom(serial=1, name="N", element="N", x=10.0, y=20.0, z=30.0)
        r = Residue(name="GLY", seq_id=1, atoms=(n,), one_letter="G")
        assert r.ca is None


# -- PDBFormatParser tests ---------------------------------------------------


@pytest.fixture
def sample_pdb(tmp_path: Path) -> Path:
    content = """\
HEADER    OXYGEN TRANSPORT                        07-MAR-84   4HHB
TITLE     THE CRYSTAL STRUCTURE OF HUMAN DEOXYHAEMOGLOBIN
EXPDTA    X-RAY DIFFRACTION
REMARK   2 RESOLUTION.    1.74 ANGSTROMS.
CRYST1   63.150   83.590   53.800  90.00  99.34  90.00 P 1 21 1      8
ATOM      1  N   VAL A   1       6.204  16.869   4.854  1.00 49.05           N
ATOM      2  CA  VAL A   1       6.913  17.759   4.607  1.00 43.14           C
ATOM      3  N   LEU A   2       8.479  19.073   3.577  1.00 24.80           N
ATOM      4  CA  LEU A   2       9.658  19.344   3.806  1.00 28.68           C
ATOM      5  N   SER B   1      12.248  21.146   2.889  1.00 30.34           N
ATOM      6  CA  SER B   1      13.381  20.553   2.456  1.00 28.81           C
HETATM    7  FE  HEM C   1      15.000  16.000  12.000  1.00 20.00          FE
END
"""
    p = tmp_path / "pdb4hhb.pdb"
    p.write_text(content)
    return p


class TestPDBFormatParser:
    def test_parse_pdb(self, sample_pdb: Path):
        s = PDBFormatParser().parse(sample_pdb)
        assert isinstance(s, PDBStructure)
        assert isinstance(s, Structure)

    def test_metadata(self, sample_pdb: Path):
        s = PDBFormatParser().parse(sample_pdb)
        assert s.entry_id == "4HHB"
        assert s.metadata.format == "pdb"
        assert s.method == "X-RAY DIFFRACTION"
        assert s.resolution == 1.74
        assert s.metadata.space_group == "P 1 21 1"
        assert s.metadata.cell_a == 63.15

    def test_chains(self, sample_pdb: Path):
        s = PDBFormatParser().parse(sample_pdb)
        assert s.num_chains == 2
        ids = s.chain_ids
        assert "A" in ids
        assert "B" in ids

    def test_chain_sequence(self, sample_pdb: Path):
        s = PDBFormatParser().parse(sample_pdb)
        chain_a = s.get_chain("A")
        assert chain_a is not None
        assert chain_a.sequence == "VL"

    def test_atoms(self, sample_pdb: Path):
        s = PDBFormatParser().parse(sample_pdb)
        assert s.num_atoms == 7  # 6 ATOM + 1 HETATM

    def test_entities(self, sample_pdb: Path):
        s = PDBFormatParser().parse(sample_pdb)
        assert s.polymer_entity_count == 2
        assert s.nonpolymer_entity_count == 1

    def test_to_dict(self, sample_pdb: Path):
        s = PDBFormatParser().parse(sample_pdb)
        d = s.to_dict()
        assert d["format"] == "pdb"
        assert d["entry_id"] == "4HHB"

    def test_extensions(self):
        exts = PDBFormatParser.extensions()
        assert ".pdb" in exts
        assert ".ent" in exts


# -- auto_parser tests -------------------------------------------------------


class TestAutoParser:
    def test_cif_extension(self):
        p = auto_parser("1abc.cif")
        assert isinstance(p, CIFParser)

    def test_cif_gz_extension(self):
        p = auto_parser("1abc.cif.gz")
        assert isinstance(p, CIFParser)

    def test_pdb_extension(self):
        p = auto_parser("1abc.pdb")
        assert isinstance(p, PDBFormatParser)

    def test_unknown_extension(self):
        with pytest.raises(ValueError, match="No parser"):
            auto_parser("data.xyz")


# -- StructureDataset tests --------------------------------------------------


class TestStructureDataset:
    def test_from_paths(self):
        ds = StructureDataset.from_paths([FIXTURES / "sample.cif"])
        assert len(ds) == 1

    def test_getitem(self):
        ds = StructureDataset.from_paths([FIXTURES / "sample.cif"])
        s = ds[0]
        assert isinstance(s, Structure)
        assert s.entry_id == "4HHB"

    def test_negative_index(self):
        ds = StructureDataset.from_paths([FIXTURES / "sample.cif"])
        s = ds[-1]
        assert s.entry_id == "4HHB"

    def test_iteration(self):
        ds = StructureDataset.from_paths([FIXTURES / "sample.cif"])
        structures = list(ds)
        assert len(structures) == 1
        assert structures[0].entry_id == "4HHB"

    def test_slice(self):
        ds = StructureDataset.from_paths([FIXTURES / "sample.cif"])
        sliced = ds[0:1]
        assert isinstance(sliced, list)
        assert len(sliced) == 1

    def test_caching(self):
        ds = StructureDataset.from_paths([FIXTURES / "sample.cif"])
        s1 = ds[0]
        s2 = ds[0]
        assert s1 is s2

    def test_pdb_ids(self):
        ds = StructureDataset.from_paths([FIXTURES / "sample.cif"])
        assert ds.pdb_ids == ["4HHB"]

    def test_filter(self):
        ds = StructureDataset.from_paths([FIXTURES / "sample.cif"])
        filtered = ds.filter(lambda s: s.resolution is not None and s.resolution < 2.0)
        assert len(filtered) == 1

    def test_filter_empty(self):
        ds = StructureDataset.from_paths([FIXTURES / "sample.cif"])
        filtered = ds.filter(lambda s: s.resolution is not None and s.resolution < 0.5)
        assert len(filtered) == 0

    def test_to_list(self):
        ds = StructureDataset.from_paths([FIXTURES / "sample.cif"])
        lst = ds.to_list()
        assert isinstance(lst, list)
        assert len(lst) == 1

    def test_summary(self):
        ds = StructureDataset.from_paths([FIXTURES / "sample.cif"])
        s = ds.summary()
        assert s["total"] == 1
        assert s["resolution_mean"] == 1.74
        assert "X-RAY DIFFRACTION" in s["methods"]

    def test_from_directory(self):
        ds = StructureDataset.from_directory(FIXTURES, pattern="*.cif")
        assert len(ds) >= 1

    def test_mixed_formats(self, sample_pdb: Path):
        ds = StructureDataset.from_paths([FIXTURES / "sample.cif", sample_pdb])
        assert len(ds) == 2
        cif_s = ds[0]
        pdb_s = ds[1]
        assert isinstance(cif_s, CIFStructure)
        assert isinstance(pdb_s, PDBStructure)
        assert cif_s.metadata.format == "mmcif"
        assert pdb_s.metadata.format == "pdb"

    def test_repr(self):
        ds = StructureDataset.from_paths([FIXTURES / "sample.cif"])
        r = repr(ds)
        assert "StructureDataset" in r
        assert "n=1" in r
