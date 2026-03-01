"""Parallel download utilities (HTTPS + S3) with progress, retry, and resume."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional
from urllib.request import Request, urlopen

from moldata.core.storage import Storage

logger = logging.getLogger(__name__)


@dataclass
class DownloadItem:
    """A single file to download."""

    url: str
    dest: str  # local path


@dataclass
class DownloadOptions:
    max_workers: int = 8
    batch_size: int = 200
    timeout: float = 60
    max_retries: int = 3
    retry_backoff: float = 1.0
    skip_existing: bool = True
    use_checkpoint: bool = True
    checkpoint_dir: Optional[str] = None

    def _checkpoint_path(self, prefix: str) -> Path:
        h = hashlib.sha256(prefix.encode()).hexdigest()[:16]
        base = Path(self.checkpoint_dir or "/moldata/checkpoints")
        base.mkdir(parents=True, exist_ok=True)
        return base / f"download_{h}.json"


def _download_one(
    item: DownloadItem,
    skip_existing: bool,
    timeout: float,
    max_retries: int,
    retry_backoff: float,
) -> tuple[str, bool, Optional[str]]:
    """Download a single file with retry. Returns (url, downloaded, error_msg)."""
    dest = Path(item.dest)
    if skip_existing and dest.exists() and dest.stat().st_size > 0:
        return (item.url, False, None)

    dest.parent.mkdir(parents=True, exist_ok=True)
    last_err: Optional[str] = None
    for attempt in range(1, max_retries + 1):
        try:
            req = Request(item.url, headers={"User-Agent": "moldata/1.0"})
            with urlopen(req, timeout=timeout) as resp:
                dest.write_bytes(resp.read())
            return (item.url, True, None)
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            if attempt < max_retries:
                time.sleep(retry_backoff * attempt)
    return (item.url, False, last_err)


def _flush_checkpoint(checkpoint_path: Optional[Path], done: set[str]) -> None:
    if not checkpoint_path:
        return
    try:
        checkpoint_path.write_text(json.dumps(
            {"urls": list(done)},
            separators=(",", ":"),
        ))
    except Exception:
        pass


def parallel_download(
    items: Iterable[DownloadItem],
    options: DownloadOptions,
    prefix_label: str = "download",
) -> tuple[int, int]:
    """Download files in parallel with progress, retry, and checkpoint.

    Returns (downloaded_count, skipped_count).
    """
    items_list = list(items)
    if not items_list:
        return (0, 0)

    checkpoint_path: Optional[Path] = None
    done_urls: set[str] = set()
    if options.use_checkpoint and options.checkpoint_dir:
        checkpoint_path = options._checkpoint_path(prefix_label)
        if checkpoint_path and checkpoint_path.exists():
            try:
                data = json.loads(checkpoint_path.read_text())
                done_urls = set(data.get("urls", []))
            except Exception:
                pass

    pending = [it for it in items_list if it.url not in done_urls]
    skipped_resume = len(items_list) - len(pending)
    if skipped_resume:
        logger.info("Resumed: skipping %d already-downloaded files", skipped_resume)

    downloaded = 0
    skipped = 0
    failed = 0

    try:
        from tqdm import tqdm
        pbar = tqdm(total=len(pending), unit="file", unit_scale=False, desc=prefix_label)
    except ImportError:
        pbar = None

    def _process_batch(batch: list[DownloadItem]) -> None:
        nonlocal downloaded, skipped, failed
        with ThreadPoolExecutor(max_workers=options.max_workers) as ex:
            futures = {
                ex.submit(
                    _download_one, it,
                    options.skip_existing, options.timeout,
                    options.max_retries, options.retry_backoff,
                ): it
                for it in batch
            }
            for fut in as_completed(futures):
                try:
                    url, was_downloaded, err = fut.result()
                except Exception as exc:
                    item = futures[fut]
                    logger.error("Unexpected error downloading %s: %s", item.url, exc)
                    failed += 1
                    if pbar:
                        pbar.update(1)
                    continue
                if err:
                    logger.warning("Failed after retries: %s — %s", url, err)
                    failed += 1
                elif was_downloaded:
                    downloaded += 1
                    done_urls.add(url)
                else:
                    skipped += 1
                    done_urls.add(url)
                if pbar:
                    pbar.update(1)
        _flush_checkpoint(checkpoint_path, done_urls)

    batch: list[DownloadItem] = []
    for item in pending:
        batch.append(item)
        if len(batch) >= options.batch_size:
            _process_batch(batch)
            batch = []
    if batch:
        _process_batch(batch)

    if pbar:
        pbar.close()

    if failed:
        logger.warning("Download finished with %d failures (downloaded=%d skipped=%d)", failed, downloaded, skipped + skipped_resume)

    return (downloaded, skipped + skipped_resume)


def parallel_s3_download(
    storage: Storage,
    keys: Iterable[str],
    dest_dir: Path,
    max_workers: int = 8,
    prefix_label: str = "s3_download",
) -> tuple[int, int]:
    """Download files from S3/MinIO in parallel.

    Returns (downloaded_count, skipped_count).
    """
    key_list = list(keys)
    if not key_list:
        return (0, 0)

    downloaded = 0
    skipped = 0

    try:
        from tqdm import tqdm
        pbar = tqdm(total=len(key_list), unit="file", desc=prefix_label)
    except ImportError:
        pbar = None

    def _get_one(key: str) -> tuple[str, bool]:
        local = dest_dir / key
        if local.exists() and local.stat().st_size > 0:
            return (key, False)
        local.parent.mkdir(parents=True, exist_ok=True)
        storage.get_file(key, str(local))
        return (key, True)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_get_one, k): k for k in key_list}
        for fut in as_completed(futures):
            try:
                _, was_downloaded = fut.result()
                if was_downloaded:
                    downloaded += 1
                else:
                    skipped += 1
            except Exception as exc:
                key = futures[fut]
                logger.error("Failed to download %s from S3: %s", key, exc)
            if pbar:
                pbar.update(1)

    if pbar:
        pbar.close()

    return (downloaded, skipped)
