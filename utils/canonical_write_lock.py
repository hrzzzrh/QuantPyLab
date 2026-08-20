"""Shared process lock for canonical Parquet writes."""

from __future__ import annotations

import errno
import fcntl
import os
from contextlib import AbstractContextManager
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path

from config.settings import WAREHOUSE_DIR

CANONICAL_WRITE_LOCK_RELATIVE_PATH = Path(".locks/daily-kline-write.lock")
_HELD_CANONICAL_LOCKS: ContextVar[frozenset[str]] = ContextVar(
    "held_canonical_locks", default=frozenset()
)


def canonical_write_lock_held(warehouse_dir: Path | str) -> bool:
    """Return whether the current execution context owns a warehouse lock."""

    key = str(Path(warehouse_dir).resolve(strict=False))
    return key in _HELD_CANONICAL_LOCKS.get()


class CanonicalWriteLockError(RuntimeError):
    """Raised when another process currently owns the canonical write lock."""


class CanonicalWriteLock(AbstractContextManager[None]):
    """Acquire an advisory lock shared by sync and canonical promotion."""

    def __init__(
        self,
        warehouse_dir: Path | str | None = None,
        *,
        operation: str,
        run_id: str = "",
    ) -> None:
        self.warehouse_dir = Path(warehouse_dir or WAREHOUSE_DIR)
        if any("\n" in value or "\r" in value for value in (operation, run_id)):
            raise ValueError("canonical 写锁字段不得包含换行")
        self.operation = operation
        self.run_id = run_id
        self.lock_path = self.warehouse_dir / CANONICAL_WRITE_LOCK_RELATIVE_PATH
        self._file = None
        self._held_token = None

    def _prepare_path(self) -> int:
        self.warehouse_dir.mkdir(parents=True, exist_ok=True)
        directory_flags = (
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            warehouse_fd = os.open(self.warehouse_dir, directory_flags)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise CanonicalWriteLockError(
                    "canonical 写锁仓库目录不得是符号链接"
                ) from exc
            raise
        try:
            try:
                os.mkdir(".locks", dir_fd=warehouse_fd)
            except FileExistsError:
                pass
            try:
                return os.open(".locks", directory_flags, dir_fd=warehouse_fd)
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                    raise CanonicalWriteLockError(
                        "canonical 写锁目录不得是符号链接"
                    ) from exc
                raise
        finally:
            os.close(warehouse_fd)

    def __enter__(self) -> None:
        lock_dir_fd = self._prepare_path()
        try:
            try:
                fd = os.open(
                    self.lock_path.name,
                    os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                    0o644,
                    dir_fd=lock_dir_fd,
                )
            except OSError as exc:
                if exc.errno == errno.ELOOP:
                    raise CanonicalWriteLockError(
                        "canonical 写锁文件不得是符号链接"
                    ) from exc
                raise
            self._file = os.fdopen(fd, "r+b")
            fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError) as exc:
            if self._file is not None:
                self._file.close()
                self._file = None
            if isinstance(exc, BlockingIOError) or getattr(exc, "errno", None) in {
                11,
                35,
            }:
                raise CanonicalWriteLockError(
                    f"canonical 写锁已被占用: {self.lock_path}"
                ) from exc
            raise
        finally:
            os.close(lock_dir_fd)

        try:
            payload = (
                f"operation={self.operation}\n"
                f"run_id={self.run_id}\n"
                f"pid={os.getpid()}\n"
                f"created_at={datetime.now(UTC).isoformat()}\n"
            ).encode()
            self._file.seek(0)
            self._file.truncate()
            self._file.write(payload)
            self._file.flush()
            os.fsync(self._file.fileno())
            held_locks = _HELD_CANONICAL_LOCKS.get()
            self._held_token = _HELD_CANONICAL_LOCKS.set(
                held_locks | {str(self.warehouse_dir.resolve(strict=False))}
            )
        except Exception:
            try:
                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
            finally:
                self._file.close()
                self._file = None
            raise
        return None

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self._file is None:
            return None
        try:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        finally:
            if self._held_token is not None:
                _HELD_CANONICAL_LOCKS.reset(self._held_token)
                self._held_token = None
            self._file.close()
            self._file = None
        return None
