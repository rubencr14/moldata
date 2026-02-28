from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from moldata.config import load_settings
from moldata.core.logging_utils import get_logger
from moldata.core.storage import LocalStorage, S3Storage
from moldata.core.manifest import Manifest
from moldata.core.splits import random_split
from moldata.core.upload_utils import UploadOptions
from moldata.datasets.pdb import PDBDataset
from moldata.datasets.pdbbind import PDBBindDataset
from moldata.datasets.crossdocking import CrossDockingDataset

logger = get_logger(__name__)
app = typer.Typer(no_args_is_help=True)

pdb_app = typer.Typer(no_args_is_help=True)
pdbbind_app = typer.Typer(no_args_is_help=True)
crossdocking_app = typer.Typer(no_args_is_help=True)
splits_app = typer.Typer(no_args_is_help=True)

app.add_typer(pdb_app, name="pdb")
app.add_typer(pdbbind_app, name="pdbbind")
app.add_typer(crossdocking_app, name="crossdocking")
app.add_typer(splits_app, name="splits")


def _make_storage():
    settings = load_settings()
    if settings.storage_backend == "local":
        return LocalStorage(root=Path(settings.local_root)), None
    if settings.storage_backend == "s3":
        if not (settings.minio_access_key and settings.minio_secret_key):
            raise typer.BadParameter("S3 backend requires MINIO_ACCESS_KEY and MINIO_SECRET_KEY.")
        storage = S3Storage(
            bucket=settings.minio_bucket,
            endpoint_url=settings.s3_endpoint_url,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            region=settings.minio_region,
        )
        return storage, settings.minio_bucket
    raise typer.BadParameter(f"Unknown storage_backend: {settings.storage_backend}")


def _upload_opts(
    max_workers: int,
    batch_size: int,
    no_checkpoint: bool,
) -> UploadOptions:
    s = load_settings()
    return UploadOptions(
        max_workers=max_workers or s.upload_max_workers,
        batch_size=batch_size or s.upload_batch_size,
        use_checkpoint=not no_checkpoint,
        checkpoint_dir=s.checkpoint_dir,
    )


@pdb_app.command("prepare")
def pdb_prepare(
    staging: Path = typer.Option(..., help="Local staging directory."),
    manifest: Path = typer.Option(..., help="Output manifest path (parquet)."),
    s3_prefix: str = typer.Option(None, help="Destination prefix."),
    source: str = typer.Option("rcsb", help="Source: ebi or rcsb (RCSB uses port 33444)."),
    pdb_format: str = typer.Option("mmcif", help="Format: mmcif or pdb (legacy .ent.gz)."),
    method: str = typer.Option("rsync", help="Download method: rsync or https."),
    snapshot_year: Optional[int] = typer.Option(None, help="Year for RCSB snapshot (reproducible)."),
    upload_format: str = typer.Option("raw", help="Upload: raw (files) or tar_shards (fewer S3 objects)."),
    tar_shard_size: int = typer.Option(1000, help="Files per tar when upload_format=tar_shards."),
    max_workers: int = typer.Option(16, help="Parallel upload workers."),
    batch_size: int = typer.Option(500, help="Upload batch size."),
    no_checkpoint: bool = typer.Option(False, help="Disable resume checkpoint."),
    keep_local: bool = typer.Option(False, help="Keep local staging files after upload (default: remove)."),
):
    settings = load_settings()
    subpath = "mmCIF" if pdb_format == "mmcif" else "pdb"
    prefix = s3_prefix or f"{settings.datasets_prefix}pdb/{subpath}/"
    storage, bucket = _make_storage()
    ds = PDBDataset(
        storage=storage,
        bucket=bucket,
        s3_prefix=prefix,
        _staging_dir=staging,
        source=source,
        pdb_format=pdb_format,
        download_method=method,
        snapshot_year=snapshot_year,
        upload_format=upload_format,
        tar_shard_size=tar_shard_size,
        remove_local_on_end=not keep_local,
    )
    opts = _upload_opts(max_workers=max_workers, batch_size=batch_size, no_checkpoint=no_checkpoint)
    stats = ds.prepare(manifest, upload_options=opts, remove_local_on_end=not keep_local)
    logger.info("Prepared dataset=%s count=%d size_bytes=%s", ds.name, stats.count, stats.size_bytes)


@pdbbind_app.command("prepare")
def pdbbind_prepare(
    staging: Path = typer.Option(..., help="Local staging directory containing PDBBind archives/files."),
    manifest: Path = typer.Option(..., help="Output manifest path (parquet)."),
    s3_prefix: str = typer.Option(None, help="Destination prefix (default: datasets/pdbbind/)."),
    mode: str = typer.Option("local", help="Download mode: local (default) or official."),
    max_workers: int = typer.Option(16, help="Parallel upload workers."),
    batch_size: int = typer.Option(500, help="Upload batch size."),
    no_checkpoint: bool = typer.Option(False, help="Disable resume checkpoint."),
    keep_local: bool = typer.Option(False, help="Keep local staging files after upload (default: remove)."),
):
    settings = load_settings()
    prefix = s3_prefix or f"{settings.datasets_prefix}pdbbind/"
    storage, bucket = _make_storage()
    ds = PDBBindDataset(storage=storage, bucket=bucket, s3_prefix=prefix, _staging_dir=staging, mode=mode, remove_local_on_end=not keep_local)
    opts = _upload_opts(max_workers=max_workers, batch_size=batch_size, no_checkpoint=no_checkpoint)
    stats = ds.prepare(manifest, upload_options=opts, remove_local_on_end=not keep_local)
    logger.info("Prepared dataset=%s count=%d size_bytes=%s", ds.name, stats.count, stats.size_bytes)


@crossdocking_app.command("prepare")
def crossdocking_prepare(
    staging: Path = typer.Option(..., help="Local staging directory for CrossDocking files."),
    manifest: Path = typer.Option(..., help="Output manifest path (parquet)."),
    s3_prefix: str = typer.Option(None, help="Destination prefix (default: datasets/crossdocking/)."),
    mode: str = typer.Option("local", help="Download mode: local (default) or official."),
    max_workers: int = typer.Option(16, help="Parallel upload workers."),
    batch_size: int = typer.Option(500, help="Upload batch size."),
    no_checkpoint: bool = typer.Option(False, help="Disable resume checkpoint."),
    keep_local: bool = typer.Option(False, help="Keep local staging files after upload (default: remove)."),
):
    settings = load_settings()
    prefix = s3_prefix or f"{settings.datasets_prefix}crossdocking/"
    storage, bucket = _make_storage()
    ds = CrossDockingDataset(storage=storage, bucket=bucket, s3_prefix=prefix, _staging_dir=staging, mode=mode, remove_local_on_end=not keep_local)
    opts = _upload_opts(max_workers=max_workers, batch_size=batch_size, no_checkpoint=no_checkpoint)
    stats = ds.prepare(manifest, upload_options=opts, remove_local_on_end=not keep_local)
    logger.info("Prepared dataset=%s count=%d size_bytes=%s", ds.name, stats.count, stats.size_bytes)


@splits_app.command("random")
def splits_random(
    manifest: Path = typer.Option(..., help="Input manifest parquet."),
    out: Path = typer.Option(..., help="Output splits parquet."),
    seed: int = typer.Option(42, help="Random seed."),
    train: float = typer.Option(0.8, help="Train ratio."),
    val: float = typer.Option(0.1, help="Val ratio."),
    test: float = typer.Option(0.1, help="Test ratio."),
    group_col: Optional[str] = typer.Option(None, help="Optional column to group by (avoid leakage)."),
):
    m = Manifest.load_parquet(manifest)
    split_manifest = random_split(m, seed=seed, ratios=(train, val, test), group_col=group_col)
    split_manifest.save_parquet(out)
    logger.info("Wrote splits to %s (count=%d)", out, split_manifest.count())
