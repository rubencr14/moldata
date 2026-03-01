"""Parallel upload utilities with chunking, progress, resume, and retry."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from moldata.core.storage import Storage

logger = logging.getLogger(__name__)


@dataclass
class UploadItem:
    """A single file to upload."""

    key: str
    path: str
    size_bytes: int = 0


@dataclass
class UploadOptions:
    """Options for parallel upload."""

    max_workers: int = 16
    batch_size: int = 500
    skip_existing: bool = True
    use_checkpoint: bool = True
    checkpoint_dir: Optional[str] = None
    max_retries: int = 3
    retry_backoff: float = 1.0  # seconds, multiplied by attempt number

    def _checkpoint_path(self, prefix: str) -> Path:
        h = hashlib.sha256(prefix.encode()).hexdigest()[:16]
        base = Path(self.checkpoint_dir or "/moldata/checkpoints")
        base.mkdir(parents=True, exist_ok=True)
        return base / f"upload_{h}.json"


def _upload_one(
    storage: Storage,
    item: UploadItem,
    skip_existing: bool,
    max_retries: int = 3,
    retry_backoff: float = 1.0,
) -> tuple[str, bool, Optional[str]]:
    """Upload a single file with retry. Returns (key, uploaded, error_msg)."""
    if skip_existing:
        try:
            head = storage.head(item.key)
            if head and int(head.get("ContentLength", -1)) == item.size_bytes:
                return (item.key, False, None)
        except Exception:
            pass

    last_err: Optional[str] = None
    for attempt in range(1, max_retries + 1):
        try:
            storage.put_file(item.key, item.path)
            return (item.key, True, None)
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            if attempt < max_retries:
                time.sleep(retry_backoff * attempt)
    return (item.key, False, last_err)


def _flush_checkpoint(
    checkpoint_path: Optional[Path],
    uploaded_keys: set[str],
) -> None:
    """Write checkpoint to disk."""
    if not checkpoint_path:
        return
    try:
        checkpoint_path.write_text(json.dumps(
            {"keys": list(uploaded_keys)},
            separators=(",", ":"),
        ))
    except Exception:
        pass


def parallel_upload(
    storage: Storage,
    items: Iterable[UploadItem],
    options: UploadOptions,
    prefix_label: str = "upload",
) -> tuple[int, int]:
    """Upload files in parallel with progress, checkpoint per batch, and retry.

    Returns (uploaded_count, skipped_count).
    """
    items_list = list(items)
    if not items_list:
        return (0, 0)

    checkpoint_path: Optional[Path] = None
    uploaded_keys: set[str] = set()
    if options.use_checkpoint and options.checkpoint_dir:
        checkpoint_path = options._checkpoint_path(prefix_label)
        if checkpoint_path and checkpoint_path.exists():
            try:
                data = json.loads(checkpoint_path.read_text())
                uploaded_keys = set(data.get("keys", []))
            except Exception:
                pass

    pending = [it for it in items_list if it.key not in uploaded_keys]
    skipped_resume = len(items_list) - len(pending)
    if skipped_resume:
        logger.info("Resumed: skipping %d already-uploaded files", skipped_resume)

    uploaded = 0
    skipped_size = 0
    failed = 0

    try:
        from tqdm import tqdm
        pbar = tqdm(total=len(pending), unit="file", unit_scale=False, desc=prefix_label)
    except ImportError:
        pbar = None

    def _process_batch(batch: list[UploadItem]) -> None:
        nonlocal uploaded, skipped_size, failed
        with ThreadPoolExecutor(max_workers=options.max_workers) as ex:
            futures = {
                ex.submit(
                    _upload_one,
                    storage,
                    it,
                    options.skip_existing,
                    options.max_retries,
                    options.retry_backoff,
                ): it
                for it in batch
            }
            for fut in as_completed(futures):
                try:
                    key, was_uploaded, err = fut.result()
                except Exception as exc:
                    item = futures[fut]
                    logger.error("Unexpected error uploading %s: %s", item.key, exc)
                    failed += 1
                    if pbar:
                        pbar.update(1)
                    continue

                if err:
                    logger.warning("Failed after retries: %s — %s", key, err)
                    failed += 1
                elif was_uploaded:
                    uploaded += 1
                    uploaded_keys.add(key)
                else:
                    skipped_size += 1
                if pbar:
                    pbar.update(1)

        _flush_checkpoint(checkpoint_path, uploaded_keys)

    batch: list[UploadItem] = []
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
        logger.warning("Upload finished with %d failures (uploaded=%d skipped=%d)", failed, uploaded, skipped_size + skipped_resume)

    return (uploaded, skipped_size + skipped_resume)
