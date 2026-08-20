import os
import uuid
from pathlib import Path

import pandas as pd

from config.settings import WAREHOUSE_DIR
from utils.canonical_write_lock import CanonicalWriteLock, canonical_write_lock_held
from utils.logger import logger


class ParquetStore:
    """
    Parquet 存储中心：负责数据的原子性写入与分片管理。
    支持 Hive-style 分区，确保并发读写不冲突。
    """

    def __init__(self):
        self.base_dir = Path(WAREHOUSE_DIR)

    @staticmethod
    def _fsync_file(path: Path) -> None:
        with path.open("rb") as file:
            os.fsync(file.fileno())

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        directory_fd = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def _open_partition_directory(self, category: str, symbol: str) -> int:
        category_parts = Path(category).parts
        if (
            not category_parts
            or Path(category).is_absolute()
            or any(part in {"", ".", ".."} for part in category_parts)
            or Path(symbol).name != symbol
            or symbol in {"", ".", ".."}
        ):
            raise ValueError("Parquet 分区路径不安全")

        self.base_dir.mkdir(parents=True, exist_ok=True)
        directory_fd = os.open(
            self.base_dir,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            for part in (*category_parts, f"symbol={symbol}"):
                try:
                    os.mkdir(part, dir_fd=directory_fd)
                except FileExistsError:
                    pass
                next_fd = os.open(
                    part,
                    os.O_RDONLY
                    | getattr(os, "O_DIRECTORY", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=directory_fd,
                )
                os.close(directory_fd)
                directory_fd = next_fd
            return directory_fd
        except Exception:
            os.close(directory_fd)
            raise

    def save_partition(self, df: pd.DataFrame, category: str, symbol: str):
        """
        原子性保存一个 symbol 的分区数据。
        :param df: 数据 Dataframe
        :param category: 类别路径 (例如: 'financial_statements/type=balance')
        :param symbol: 股票代码
        """
        if df.empty:
            return
        if category == "daily_kline" and not canonical_write_lock_held(self.base_dir):
            with CanonicalWriteLock(
                self.base_dir,
                operation="parquet-save-kline",
                run_id=symbol,
            ):
                return self._save_partition(df, category, symbol)
        return self._save_partition(df, category, symbol)

    def _save_partition(self, df: pd.DataFrame, category: str, symbol: str):
        """Persist a partition while the caller owns any required write lock."""

        # 1. 准备目录 (Hive-style: category/symbol=XXXXXX/)
        directory_fd = self._open_partition_directory(category, symbol)
        temp_name = f".tmp_{symbol}.{uuid.uuid4().hex}.parquet"
        temp_fd = None
        try:
            # 如果 symbol 列在 DF 中，导出时排除它（因为它已在目录名中）
            cols_to_save = [c for c in df.columns if c != "symbol"]
            temp_fd = os.open(
                temp_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o644,
                dir_fd=directory_fd,
            )
            with os.fdopen(temp_fd, "wb") as temp_file:
                temp_fd = None
                df[cols_to_save].to_parquet(
                    temp_file, engine="pyarrow", compression="snappy", index=False
                )
                temp_file.flush()
                os.fsync(temp_file.fileno())

            # 2. 原子替换，所有路径解析都固定在安全打开的目录 fd 内
            os.replace(
                temp_name,
                "data.parquet",
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
            os.fsync(directory_fd)
            category_path = self.base_dir
            for part in Path(category).parts:
                category_path /= part
            self._fsync_directory(category_path)
            self._fsync_directory(self.base_dir)

        except Exception:
            logger.exception(f"写入 Parquet 失败 [{symbol}]")
            if temp_fd is not None:
                os.close(temp_fd)
            try:
                os.unlink(temp_name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
            raise
        finally:
            os.close(directory_fd)

    def get_path(self, category: str) -> str:
        """获取某个类别的通配符路径，用于 DuckDB 读取"""
        return str(self.base_dir / category / "*/*.parquet")
