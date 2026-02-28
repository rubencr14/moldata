"""Tests for LocalStorage."""

import tempfile
from pathlib import Path

import pytest

from moldata.core.storage import LocalStorage


@pytest.fixture
def local_store(tmp_path: Path) -> LocalStorage:
    return LocalStorage(root=tmp_path)


def test_put_and_get_file(local_store: LocalStorage, tmp_path: Path) -> None:
    src = tmp_path / "source.txt"
    src.write_text("hello world")

    local_store.put_file("test/file.txt", str(src))

    dst = tmp_path / "downloaded.txt"
    local_store.get_file("test/file.txt", str(dst))
    assert dst.read_text() == "hello world"


def test_head_existing(local_store: LocalStorage, tmp_path: Path) -> None:
    src = tmp_path / "source.bin"
    src.write_bytes(b"x" * 42)
    local_store.put_file("data/item.bin", str(src))

    result = local_store.head("data/item.bin")
    assert result is not None
    assert result["ContentLength"] == 42


def test_head_missing(local_store: LocalStorage) -> None:
    assert local_store.head("nonexistent/file.txt") is None


def test_list_prefix(local_store: LocalStorage, tmp_path: Path) -> None:
    for name in ["a.txt", "b.txt", "c.txt"]:
        src = tmp_path / f"src_{name}"
        src.write_text(name)
        local_store.put_file(f"prefix/{name}", str(src))

    keys = list(local_store.list_prefix("prefix/"))
    assert len(keys) == 3
    assert all(k.startswith("prefix/") for k in keys)


def test_put_creates_nested_dirs(local_store: LocalStorage, tmp_path: Path) -> None:
    src = tmp_path / "data.bin"
    src.write_bytes(b"data")
    local_store.put_file("deep/nested/path/file.bin", str(src))
    assert local_store.head("deep/nested/path/file.bin") is not None
