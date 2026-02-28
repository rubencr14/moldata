"""Tests for upload_utils with LocalStorage."""

from pathlib import Path

import pytest

from moldata.core.storage import LocalStorage
from moldata.core.upload_utils import UploadItem, UploadOptions, parallel_upload


@pytest.fixture
def storage(tmp_path: Path) -> LocalStorage:
    return LocalStorage(root=tmp_path / "storage")


@pytest.fixture
def staging(tmp_path: Path) -> Path:
    d = tmp_path / "staging"
    d.mkdir()
    for i in range(10):
        (d / f"file_{i}.txt").write_text(f"content_{i}")
    return d


def _make_items(staging: Path, prefix: str = "test/") -> list[UploadItem]:
    return [
        UploadItem(
            key=f"{prefix}{p.name}",
            path=str(p),
            size_bytes=p.stat().st_size,
        )
        for p in sorted(staging.iterdir()) if p.is_file()
    ]


def test_parallel_upload_basic(storage: LocalStorage, staging: Path) -> None:
    items = _make_items(staging)
    opts = UploadOptions(max_workers=4, batch_size=5, skip_existing=False, use_checkpoint=False)
    uploaded, skipped = parallel_upload(storage, items, opts, prefix_label="test")
    assert uploaded == 10
    assert skipped == 0


def test_parallel_upload_skip_existing(storage: LocalStorage, staging: Path) -> None:
    items = _make_items(staging)
    opts = UploadOptions(max_workers=4, batch_size=5, skip_existing=True, use_checkpoint=False)
    parallel_upload(storage, items, opts, prefix_label="test")
    uploaded, skipped = parallel_upload(storage, items, opts, prefix_label="test")
    assert uploaded == 0
    assert skipped == 10


def test_parallel_upload_checkpoint_resume(storage: LocalStorage, staging: Path, tmp_path: Path) -> None:
    checkpoint_dir = str(tmp_path / "checkpoints")
    items = _make_items(staging)
    opts = UploadOptions(
        max_workers=2, batch_size=5,
        skip_existing=False, use_checkpoint=True,
        checkpoint_dir=checkpoint_dir,
    )
    parallel_upload(storage, items, opts, prefix_label="resume_test")
    uploaded, skipped = parallel_upload(storage, items, opts, prefix_label="resume_test")
    assert uploaded == 0
    assert skipped == 10


def test_parallel_upload_empty(storage: LocalStorage) -> None:
    opts = UploadOptions(max_workers=2, batch_size=5, use_checkpoint=False)
    uploaded, skipped = parallel_upload(storage, [], opts)
    assert uploaded == 0
    assert skipped == 0


def test_parallel_upload_retry_on_failure(tmp_path: Path, staging: Path) -> None:
    """Upload to a broken storage fails gracefully (no crash)."""
    class BrokenStorage:
        def put_file(self, key: str, path: str) -> None:
            raise ConnectionError("fake network error")
        def get_file(self, key: str, path: str) -> None:
            pass
        def head(self, key: str):
            return None
        def list_prefix(self, prefix: str):
            return []

    items = _make_items(staging)
    opts = UploadOptions(
        max_workers=2, batch_size=5,
        skip_existing=False, use_checkpoint=False,
        max_retries=2, retry_backoff=0.01,
    )
    uploaded, skipped = parallel_upload(BrokenStorage(), items, opts, prefix_label="broken")
    assert uploaded == 0
