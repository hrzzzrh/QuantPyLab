"""全量 Parquet 数据集的 staging、原子晋级和旧快照清理。"""

from __future__ import annotations

import os
import shutil
import uuid
from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from storage.file_store.atomic_partition_store import save_partitions_atomically
from utils.canonical_write_lock import CanonicalWriteLock
from utils.logger import logger


class SnapshotPromotionError(RuntimeError):
    """全量数据集快照晋级失败。"""


def _validate_category(category: str) -> Path:
    category_path = Path(category)
    if (
        not category_path.parts
        or category_path.is_absolute()
        or any(part in {"", ".", ".."} for part in category_path.parts)
    ):
        raise ValueError("Parquet 快照类别路径不安全")
    return category_path


def _fsync_directory(path: Path) -> None:
    directory_fd = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _remove_directory(path: Path) -> None:
    if not path.exists():
        return
    if path.is_symlink():
        raise SnapshotPromotionError(f"快照目录不得是符号链接: {path}")
    if not path.is_dir():
        raise SnapshotPromotionError(f"快照路径不是目录: {path}")
    shutil.rmtree(path)


def _ensure_directory(path: Path, description: str) -> None:
    if path.is_symlink():
        raise SnapshotPromotionError(f"{description}不得是符号链接: {path}")
    if path.exists() and not path.is_dir():
        raise SnapshotPromotionError(f"{description}不是目录: {path}")


def _promote_dataset(
    staged_dataset: Path,
    canonical_dataset: Path,
    warehouse_dir: Path,
) -> None:
    """在写锁内把完整 staging 目录切换为 canonical 目录。"""

    _ensure_directory(staged_dataset, "staging 快照目录")
    canonical_parent = canonical_dataset.parent
    canonical_parent.mkdir(parents=True, exist_ok=True)
    _ensure_directory(canonical_parent, "canonical 快照父目录")
    if canonical_dataset.is_symlink():
        raise SnapshotPromotionError(
            f"canonical 快照目录不得是符号链接: {canonical_dataset}"
        )
    if canonical_dataset.exists() and not canonical_dataset.is_dir():
        raise SnapshotPromotionError(f"canonical 快照路径不是目录: {canonical_dataset}")

    backup_dataset = canonical_parent / (
        f".{canonical_dataset.name}.backup-{uuid.uuid4().hex}"
    )
    moved_old = False
    promoted = False
    try:
        if canonical_dataset.exists():
            os.replace(canonical_dataset, backup_dataset)
            moved_old = True
        os.replace(staged_dataset, canonical_dataset)
        promoted = True
        _fsync_directory(canonical_parent)
        _fsync_directory(warehouse_dir)
    except Exception:
        if promoted:
            _remove_directory(canonical_dataset)
        if moved_old and backup_dataset.exists():
            os.replace(backup_dataset, canonical_dataset)
            _fsync_directory(canonical_parent)
            _fsync_directory(warehouse_dir)
        raise

    if moved_old:
        try:
            _remove_directory(backup_dataset)
            _fsync_directory(canonical_parent)
            _fsync_directory(warehouse_dir)
        except OSError as exc:
            logger.warning(
                "旧行业快照清理失败，canonical 已完成切换，保留备份待后续清理: %s (%s)",
                backup_dataset,
                exc,
            )


def save_snapshot_atomically(
    base_dir: str | Path,
    category: str,
    partitions: Sequence[tuple[pd.DataFrame, str]],
    *,
    operation: str,
    run_id: str = "",
) -> None:
    """将全量 symbol 分区写入 staging 后一次性晋级为 canonical 快照。

    `partitions` 中每项为 (DataFrame, symbol)。数据写入、完整校验和 fsync
    在 staging 完成后，才在共享 canonical 写锁内替换整个数据集目录；旧目录
    会在成功切换后清理。任一 staging 或晋级步骤失败，都不会留下半成品
    canonical 快照。
    """

    warehouse_dir = Path(base_dir)
    category_path = _validate_category(category)
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    _ensure_directory(warehouse_dir, "数据仓库目录")

    staged_parent = warehouse_dir / ".staging" / category_path
    _ensure_directory(staged_parent.parent, "staging 根目录")
    staged_parent.mkdir(parents=True, exist_ok=True)
    _ensure_directory(staged_parent, "staging 类别目录")

    staging_run = staged_parent / f"run-{uuid.uuid4().hex}"
    staging_run.mkdir()
    staged_dataset = staging_run / category_path
    canonical_dataset = warehouse_dir / category_path
    try:
        staged_partitions = [
            (frame, category, symbol) for frame, symbol in partitions if not frame.empty
        ]
        if not staged_partitions:
            raise ValueError("全量快照没有可写入的分区")
        save_partitions_atomically(staging_run, staged_partitions)
        _fsync_directory(staging_run)
        _fsync_directory(staged_parent)

        with CanonicalWriteLock(
            warehouse_dir,
            operation=operation,
            run_id=run_id or staging_run.name,
        ):
            _promote_dataset(staged_dataset, canonical_dataset, warehouse_dir)
    finally:
        _remove_directory(staging_run)
