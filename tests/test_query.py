"""Tests for moldata.query module."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from moldata.core.manifest import Manifest
from moldata.core.storage import LocalStorage
from moldata.query.collections import COLLECTIONS, CollectionSpec, get_collection, list_collections
from moldata.query.minio_query import MinIOQuery
from moldata.query.rcsb_search import (
    and_query,
    ec_node,
    go_node,
    keyword_node,
    pfam_node,
    resolution_node,
    taxonomy_node,
)


# -- Collections tests -------------------------------------------------------


def test_collections_registry_not_empty():
    assert len(COLLECTIONS) > 0


def test_list_collections_all():
    all_c = list_collections()
    assert len(all_c) == len(COLLECTIONS)


def test_list_collections_by_tag():
    kinases = list_collections(tag="kinase")
    assert len(kinases) >= 2
    for c in kinases:
        assert "kinase" in c.tags


def test_get_collection_known():
    spec = get_collection("kinases_human")
    assert spec.pfam_id == "PF00069"
    assert spec.taxonomy_id == 9606


def test_get_collection_unknown():
    with pytest.raises(ValueError, match="Unknown collection"):
        get_collection("nonexistent_collection_xyz")


def test_collection_spec_fields():
    spec = get_collection("gpcr")
    assert spec.pfam_id == "PF00001"
    assert spec.default_resolution == 3.5
    assert "membrane" in spec.tags


# -- RCSB Search query builder tests ----------------------------------------


def test_pfam_node_structure():
    node = pfam_node("PF00069")
    assert node["type"] == "group"
    inner = node["nodes"][0]
    assert inner["parameters"]["attribute"] == "rcsb_polymer_entity_annotation.annotation_id"
    assert inner["parameters"]["value"] == "PF00069"


def test_ec_node_strips_wildcard():
    node = ec_node("2.7.10.*")
    inner = node["nodes"][0]
    assert inner["parameters"]["value"] == "2.7.10"


def test_resolution_node():
    node = resolution_node(2.5)
    inner = node["nodes"][0]
    assert inner["parameters"]["operator"] == "less_or_equal"
    assert inner["parameters"]["value"] == 2.5


def test_taxonomy_node_converts_to_string():
    node = taxonomy_node(9606)
    inner = node["nodes"][0]
    assert inner["parameters"]["value"] == "9606"


def test_keyword_node_uses_full_text():
    node = keyword_node("kinase")
    inner = node["nodes"][0]
    assert inner["service"] == "full_text"


def test_go_node():
    node = go_node("GO:0004672")
    inner = node["nodes"][0]
    assert inner["parameters"]["value"] == "GO:0004672"


def test_and_query_combines_nodes():
    q = and_query(pfam_node("PF00069"), resolution_node(2.5))
    assert q["query"]["type"] == "group"
    assert q["query"]["logical_operator"] == "and"
    assert len(q["query"]["nodes"]) == 2


# -- MinIOQuery tests (offline, using LocalStorage) --------------------------


@pytest.fixture
def sample_manifest(tmp_path: Path) -> Path:
    df = pd.DataFrame([
        {"sample_id": "1abc", "dataset": "pdb", "subset": "mmCIF",
         "uri": "s3://molfun-data/datasets/pdb/mmCIF/ab/1abc.cif.gz",
         "key": "datasets/pdb/mmCIF/ab/1abc.cif.gz", "size_bytes": 1000,
         "resolution": 1.8, "method": "X-RAY DIFFRACTION"},
        {"sample_id": "2def", "dataset": "pdb", "subset": "mmCIF",
         "uri": "s3://molfun-data/datasets/pdb/mmCIF/de/2def.cif.gz",
         "key": "datasets/pdb/mmCIF/de/2def.cif.gz", "size_bytes": 2000,
         "resolution": 2.5, "method": "X-RAY DIFFRACTION"},
        {"sample_id": "3ghi", "dataset": "pdb", "subset": "mmCIF",
         "uri": "s3://molfun-data/datasets/pdb/mmCIF/gh/3ghi.cif.gz",
         "key": "datasets/pdb/mmCIF/gh/3ghi.cif.gz", "size_bytes": 3000,
         "resolution": 3.2, "method": "ELECTRON MICROSCOPY"},
    ])
    path = tmp_path / "test_manifest.parquet"
    df.to_parquet(path, index=False)
    return path


@pytest.fixture
def local_storage_with_files(tmp_path: Path) -> tuple[LocalStorage, Path]:
    """Create a LocalStorage with fake structure files."""
    root = tmp_path / "storage"
    for key in [
        "datasets/pdb/mmCIF/ab/1abc.cif.gz",
        "datasets/pdb/mmCIF/de/2def.cif.gz",
        "datasets/pdb/mmCIF/gh/3ghi.cif.gz",
    ]:
        p = root / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"fake structure data")
    return LocalStorage(root=root), root


def test_minio_query_load_manifest(sample_manifest: Path):
    q = MinIOQuery(manifest_path=str(sample_manifest))
    assert q.count() == 3


def test_minio_query_available_ids(sample_manifest: Path):
    q = MinIOQuery(manifest_path=str(sample_manifest))
    ids = q.available_pdb_ids()
    assert set(ids) == {"1abc", "2def", "3ghi"}


def test_minio_query_summary(sample_manifest: Path):
    q = MinIOQuery(manifest_path=str(sample_manifest))
    s = q.summary()
    assert s["total"] == 3
    assert "resolution_mean" in s
    assert "methods" in s


def test_minio_query_filter_manifest_resolution(sample_manifest: Path):
    q = MinIOQuery(manifest_path=str(sample_manifest))
    df = q.filter_manifest(resolution_max=2.5)
    assert len(df) == 2
    assert set(df["sample_id"]) == {"1abc", "2def"}


def test_minio_query_filter_manifest_method(sample_manifest: Path):
    q = MinIOQuery(manifest_path=str(sample_manifest))
    df = q.filter_manifest(method="electron")
    assert len(df) == 1
    assert df.iloc[0]["sample_id"] == "3ghi"


def test_minio_query_filter_manifest_max(sample_manifest: Path):
    q = MinIOQuery(manifest_path=str(sample_manifest))
    df = q.filter_manifest(max_structures=1)
    assert len(df) == 1


def test_minio_query_fetch_by_ids(
    sample_manifest: Path,
    local_storage_with_files: tuple[LocalStorage, Path],
    tmp_path: Path,
):
    storage, _ = local_storage_with_files
    cache = tmp_path / "cache"
    q = MinIOQuery(
        manifest_path=str(sample_manifest),
        cache_dir=str(cache),
        storage=storage,
    )
    paths = q.fetch(["1abc", "2def"])
    assert len(paths) == 2
    assert all(p.exists() for p in paths)


def test_minio_query_fetch_unknown_ids(
    sample_manifest: Path,
    local_storage_with_files: tuple[LocalStorage, Path],
    tmp_path: Path,
):
    storage, _ = local_storage_with_files
    cache = tmp_path / "cache"
    q = MinIOQuery(
        manifest_path=str(sample_manifest),
        cache_dir=str(cache),
        storage=storage,
    )
    paths = q.fetch(["9zzz", "8yyy"])
    assert len(paths) == 0


def test_minio_query_fetch_filtered(
    sample_manifest: Path,
    local_storage_with_files: tuple[LocalStorage, Path],
    tmp_path: Path,
):
    storage, _ = local_storage_with_files
    cache = tmp_path / "cache"
    q = MinIOQuery(
        manifest_path=str(sample_manifest),
        cache_dir=str(cache),
        storage=storage,
    )
    paths = q.fetch_filtered(resolution_max=2.0)
    assert len(paths) == 1
    assert "1abc" in str(paths[0])


def test_minio_query_fetch_with_max_structures(
    sample_manifest: Path,
    local_storage_with_files: tuple[LocalStorage, Path],
    tmp_path: Path,
):
    storage, _ = local_storage_with_files
    cache = tmp_path / "cache"
    q = MinIOQuery(
        manifest_path=str(sample_manifest),
        cache_dir=str(cache),
        storage=storage,
    )
    paths = q.fetch(["1abc", "2def", "3ghi"], max_structures=1)
    assert len(paths) == 1
