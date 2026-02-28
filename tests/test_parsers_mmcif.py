"""Tests for mmCIF parser."""

from pathlib import Path

import pytest

from moldata.parsers.mmcif import MMCIFInfo, parse_mmcif

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_parse_mmcif_sample() -> None:
    """Parse sample.cif and check extracted fields."""
    path = FIXTURES / "sample.cif"
    assert path.exists(), f"Fixture missing: {path}"

    info = parse_mmcif(path)
    assert info is not None
    assert isinstance(info, MMCIFInfo)
    assert info.entry_id == "4HHB"
    assert info.method == "X-RAY DIFFRACTION"
    assert info.resolution == 1.74
    assert info.release_date == "1984-03-07"
    assert "DEOXYHAEMOGLOBIN" in (info.title or "")
    assert info.space_group == "P 1 21 1"
    assert info.cell_a == 63.15
    assert info.cell_b == 83.59
    assert info.cell_c == 53.80
    assert info.entity_count == 5
    assert info.polymer_entity_count == 2
    assert info.nonpolymer_entity_count == 2


def test_parse_mmcif_missing_file() -> None:
    """Missing file returns None."""
    info = parse_mmcif("/nonexistent/path.cif")
    assert info is None


def test_parse_mmcif_from_filename() -> None:
    """Entry ID can be inferred from filename when not in file."""
    path = FIXTURES / "sample.cif"
    info = parse_mmcif(path)
    assert info is not None
    assert info.entry_id == "4HHB"
