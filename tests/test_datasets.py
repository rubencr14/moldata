"""Tests for dataset classes with LocalStorage (no network)."""

from pathlib import Path

import pytest

from moldata.core.storage import LocalStorage
from moldata.core.upload_utils import UploadOptions
from moldata.datasets.pdb import PDBDataset
from moldata.datasets.pdbbind import PDBBindDataset
from moldata.datasets.crossdocking import CrossDockingDataset


@pytest.fixture
def storage(tmp_path: Path) -> LocalStorage:
    return LocalStorage(root=tmp_path / "storage")


@pytest.fixture
def pdb_staging(tmp_path: Path) -> Path:
    """Create a fake PDB staging directory with mmCIF files."""
    staging = tmp_path / "pdb_staging"
    sub = staging / "ab"
    sub.mkdir(parents=True)
    (sub / "pdb1abc.cif.gz").write_bytes(b"fake mmcif data 1")
    (sub / "pdb2abd.cif.gz").write_bytes(b"fake mmcif data 2")
    return staging


@pytest.fixture
def pdbbind_staging(tmp_path: Path) -> Path:
    staging = tmp_path / "pdbbind_staging"
    staging.mkdir()
    (staging / "1abc" / "1abc_protein.pdb").parent.mkdir(parents=True)
    (staging / "1abc" / "1abc_protein.pdb").write_text("ATOM ...")
    (staging / "1abc" / "1abc_ligand.sdf").write_text("ligand data")
    return staging


@pytest.fixture
def crossdocking_staging(tmp_path: Path) -> Path:
    staging = tmp_path / "crossdock_staging"
    staging.mkdir()
    (staging / "complex_1.pdb").write_text("ATOM ...")
    (staging / "complex_2.pdb").write_text("ATOM ...")
    return staging


def _opts(tmp_path: Path) -> UploadOptions:
    return UploadOptions(
        max_workers=2, batch_size=10,
        skip_existing=False, use_checkpoint=False,
    )


class TestPDBDataset:
    def test_upload_and_manifest(self, storage: LocalStorage, pdb_staging: Path, tmp_path: Path) -> None:
        ds = PDBDataset(
            storage=storage, bucket=None,
            s3_prefix="datasets/pdb/mmCIF/",
            _staging_dir=pdb_staging,
        )
        ds.upload(upload_options=_opts(tmp_path))
        manifest = ds.build_manifest()
        assert manifest.count() == 2
        assert "1abc" in manifest.df["sample_id"].values
        assert "2abd" in manifest.df["sample_id"].values

    def test_upload_tar_shards(self, storage: LocalStorage, pdb_staging: Path, tmp_path: Path) -> None:
        ds = PDBDataset(
            storage=storage, bucket=None,
            s3_prefix="datasets/pdb/mmCIF/",
            _staging_dir=pdb_staging,
            upload_format="tar_shards",
            tar_shard_size=5,
        )
        ds.upload(upload_options=_opts(tmp_path))
        keys = list(storage.list_prefix("datasets/pdb/mmCIF/shards/"))
        assert len(keys) >= 1

    def test_manifest_with_bucket(self, storage: LocalStorage, pdb_staging: Path) -> None:
        ds = PDBDataset(
            storage=storage, bucket="molfun-data",
            s3_prefix="datasets/pdb/mmCIF/",
            _staging_dir=pdb_staging,
        )
        manifest = ds.build_manifest()
        uris = manifest.df["uri"].tolist()
        assert all(u.startswith("s3://molfun-data/") for u in uris)


class TestPDBBindDataset:
    def test_upload_and_manifest(self, storage: LocalStorage, pdbbind_staging: Path, tmp_path: Path) -> None:
        ds = PDBBindDataset(
            storage=storage, bucket=None,
            s3_prefix="datasets/pdbbind/",
            _staging_dir=pdbbind_staging,
            mode="local",
        )
        ds.download()
        ds.upload(upload_options=_opts(tmp_path))
        manifest = ds.build_manifest()
        assert manifest.count() == 2


class TestCrossDockingDataset:
    def test_upload_and_manifest(self, storage: LocalStorage, crossdocking_staging: Path, tmp_path: Path) -> None:
        ds = CrossDockingDataset(
            storage=storage, bucket=None,
            s3_prefix="datasets/crossdocking/",
            _staging_dir=crossdocking_staging,
            mode="local",
        )
        ds.download()
        ds.upload(upload_options=_opts(tmp_path))
        manifest = ds.build_manifest()
        assert manifest.count() == 2
        assert all(r["dataset"] == "crossdocking" for _, r in manifest.df.iterrows())
