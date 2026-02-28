"""Tests for download_utils."""

from pathlib import Path

import pytest

from moldata.core.download_utils import DownloadItem, DownloadOptions, parallel_download


def test_parallel_download_skip_existing(tmp_path: Path) -> None:
    """Pre-existing files are skipped."""
    dest = tmp_path / "out"
    dest.mkdir()
    f = dest / "existing.txt"
    f.write_text("already here")

    items = [DownloadItem(url="http://fake.invalid/existing.txt", dest=str(f))]
    opts = DownloadOptions(max_workers=1, batch_size=10, skip_existing=True, use_checkpoint=False, max_retries=1, retry_backoff=0.01)
    downloaded, skipped = parallel_download(items, opts, prefix_label="test")
    assert downloaded == 0
    assert skipped == 1


def test_parallel_download_empty() -> None:
    opts = DownloadOptions(max_workers=1, batch_size=10, use_checkpoint=False)
    downloaded, skipped = parallel_download([], opts)
    assert downloaded == 0
    assert skipped == 0


def test_parallel_download_failure_no_crash(tmp_path: Path) -> None:
    """Failed downloads don't crash the pipeline."""
    dest = tmp_path / "out" / "file.txt"
    items = [DownloadItem(url="http://192.0.2.1/unreachable.txt", dest=str(dest))]
    opts = DownloadOptions(
        max_workers=1, batch_size=10, timeout=1,
        skip_existing=False, use_checkpoint=False,
        max_retries=1, retry_backoff=0.01,
    )
    downloaded, skipped = parallel_download(items, opts, prefix_label="fail")
    assert downloaded == 0
