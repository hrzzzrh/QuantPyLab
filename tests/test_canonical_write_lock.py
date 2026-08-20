from pathlib import Path

import pytest

from utils.canonical_write_lock import (
    CanonicalWriteLock,
    CanonicalWriteLockError,
)


def test_canonical_write_lock_records_owner_and_releases(tmp_path):
    lock_path = tmp_path / ".locks" / "daily-kline-write.lock"

    with CanonicalWriteLock(tmp_path, operation="test-operation", run_id="test-run"):
        payload = lock_path.read_text()
        assert "operation=test-operation" in payload
        assert "run_id=test-run" in payload

    with CanonicalWriteLock(tmp_path, operation="second-operation"):
        assert lock_path.exists()


def test_canonical_write_lock_rejects_contention(tmp_path):
    first = CanonicalWriteLock(tmp_path, operation="first")
    second = CanonicalWriteLock(tmp_path, operation="second")

    with first:
        with pytest.raises(CanonicalWriteLockError, match="已被占用"):
            with second:
                pass


def test_canonical_write_lock_rejects_symlink_directory(tmp_path):
    lock_dir = tmp_path / ".locks"
    lock_dir.symlink_to(Path("/tmp"), target_is_directory=True)

    with pytest.raises(CanonicalWriteLockError, match="符号链接"):
        with CanonicalWriteLock(tmp_path, operation="unsafe"):
            pass
