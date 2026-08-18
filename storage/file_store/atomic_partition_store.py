"""支持多 Parquet 分区事务式替换的存储辅助函数。"""

from __future__ import annotations

import os
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from utils.logger import logger


@dataclass
class _PreparedPartition:
    """一个待提交的 Parquet 分区及其回滚状态。"""

    target_path: Path
    temp_path: Path
    partition_dir: Path
    backup_path: Path | None = None
    committed: bool = False


def _validate_partition_path(base_dir: Path, category: str, symbol: str) -> Path:
    category_path = Path(category)
    if (
        not category_path.parts
        or category_path.is_absolute()
        or any(part in {"", ".", ".."} for part in category_path.parts)
        or Path(symbol).name != symbol
        or symbol in {"", ".", ".."}
    ):
        raise ValueError("Parquet 分区路径不安全")

    resolved_base = base_dir.resolve()
    partition_dir = base_dir / category_path / f"symbol={symbol}"
    resolved_partition_dir = partition_dir.resolve()
    if resolved_base not in resolved_partition_dir.parents:
        raise ValueError("Parquet 分区路径越出数据仓库")
    return partition_dir


def _write_temp_partition(df: pd.DataFrame, temp_path: Path) -> None:
    columns = [column for column in df.columns if column != "symbol"]
    with temp_path.open("wb") as file:
        df[columns].to_parquet(
            file,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
        file.flush()
        os.fsync(file.fileno())


def _fsync_directory(path: Path) -> None:
    directory_fd = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def save_partitions_atomically(
    base_dir: str | Path,
    partitions: Sequence[tuple[pd.DataFrame, str, str]],
) -> None:
    """将多个 symbol 分区作为一个可回滚批次写入。

    所有临时文件先完整写入并 fsync，正式文件全部替换成功后才删除备份。
    任一替换或目录 fsync 失败都会恢复已经替换的分区，避免四源日期只写入部分表。
    """
    warehouse_dir = Path(base_dir)
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    prepared: list[_PreparedPartition] = []
    seen_targets: set[Path] = set()

    try:
        for df, category, symbol in partitions:
            if df.empty:
                continue
            partition_dir = _validate_partition_path(warehouse_dir, category, symbol)
            partition_dir.mkdir(parents=True, exist_ok=True)
            if partition_dir.is_symlink():
                raise ValueError("Parquet 分区目录不得是符号链接")

            target_path = partition_dir / "data.parquet"
            if target_path.is_symlink():
                raise ValueError("Parquet 数据文件不得是符号链接")
            if target_path in seen_targets:
                raise ValueError(f"同一批次包含重复 Parquet 分区: {category}/{symbol}")
            seen_targets.add(target_path)

            temp_path = partition_dir / f".tmp_{symbol}.{uuid.uuid4().hex}.parquet"
            try:
                _write_temp_partition(df, temp_path)
            except Exception:
                temp_path.unlink(missing_ok=True)
                raise
            prepared.append(
                _PreparedPartition(
                    target_path=target_path,
                    temp_path=temp_path,
                    partition_dir=partition_dir,
                )
            )

        for item in prepared:
            if item.target_path.exists():
                item.backup_path = item.target_path.with_name(
                    f".backup_{uuid.uuid4().hex}.parquet"
                )
                os.replace(item.target_path, item.backup_path)
            os.replace(item.temp_path, item.target_path)
            item.committed = True

        for item in prepared:
            _fsync_directory(item.partition_dir)
            _fsync_directory(item.partition_dir.parent)
            _fsync_directory(item.partition_dir.parent.parent)
            _fsync_directory(warehouse_dir)
    except Exception:
        for item in reversed(prepared):
            item.temp_path.unlink(missing_ok=True)
            if item.backup_path is not None and item.backup_path.exists():
                item.target_path.unlink(missing_ok=True)
                os.replace(item.backup_path, item.target_path)
            elif item.committed:
                item.target_path.unlink(missing_ok=True)
        raise
    finally:
        for item in prepared:
            item.temp_path.unlink(missing_ok=True)

    for item in prepared:
        if item.backup_path is not None:
            try:
                item.backup_path.unlink(missing_ok=True)
            except OSError as exc:
                logger.warning(
                    "Parquet 旧分区备份清理失败，保留备份待后续处理: %s (%s)",
                    item.backup_path,
                    exc,
                )
