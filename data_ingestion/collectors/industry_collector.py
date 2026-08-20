"""Collect effective-dated Shenwan stock industry classifications."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path

import akshare as ak
import pandas as pd

from storage.database.sync_status import (
    DATASET_INDUSTRY_HISTORY,
    SYMBOL_SYNC_ALL,
    is_synced_today,
    record_sync_success,
)
from storage.file_store.atomic_snapshot_store import save_snapshot_atomically
from storage.file_store.parquet_store import ParquetStore
from utils.logger import logger

INDUSTRY_HISTORY_CATEGORY = "industry_classification_sw"
INDUSTRY_HISTORY_COLUMNS = (
    "effective_date",
    "industry_code",
    "source_updated_date",
)
_SOURCE_COLUMNS = {
    "symbol": "symbol",
    "start_date": "effective_date",
    "industry_code": "industry_code",
    "update_time": "source_updated_date",
}


def normalize_industry_history(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize and validate AkShare's effective-dated industry history."""

    if frame.empty:
        raise ValueError("申万行业历史接口返回为空")
    missing = set(_SOURCE_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError("申万行业历史缺少字段: " + ", ".join(sorted(missing)))

    normalized = frame.loc[:, list(_SOURCE_COLUMNS)].rename(columns=_SOURCE_COLUMNS)
    normalized = normalized.copy()
    normalized["symbol"] = (
        normalized["symbol"]
        .astype("string")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(6)
    )
    normalized["industry_code"] = (
        normalized["industry_code"]
        .astype("string")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(6)
    )
    normalized["effective_date"] = pd.to_datetime(
        normalized["effective_date"], format="mixed", errors="coerce"
    ).dt.date
    normalized["source_updated_date"] = pd.to_datetime(
        normalized["source_updated_date"], format="mixed", errors="coerce"
    ).dt.date

    invalid_symbol = normalized["symbol"].isna() | ~normalized["symbol"].str.fullmatch(
        r"\d{6}", na=False
    )
    invalid_industry = normalized["industry_code"].isna() | ~normalized[
        "industry_code"
    ].str.fullmatch(r"\d{6}", na=False)
    invalid_dates = (
        normalized[["effective_date", "source_updated_date"]].isna().any(axis=1)
    )
    if invalid_symbol.any():
        raise ValueError("申万行业历史包含非法股票代码")
    if invalid_industry.any():
        raise ValueError("申万行业历史包含非法行业代码")
    if invalid_dates.any():
        raise ValueError("申万行业历史包含无法解析的日期")

    if normalized.duplicated(["symbol", "effective_date"]).any():
        raise ValueError("申万行业历史包含重复的 symbol/effective_date")

    return normalized.sort_values(
        ["symbol", "effective_date", "industry_code"], kind="mergesort"
    ).reset_index(drop=True)


class IndustryHistoryCollector:
    """Download and persist the full Shenwan industry change history."""

    category = INDUSTRY_HISTORY_CATEGORY

    def __init__(
        self,
        store: ParquetStore | None = None,
        fetcher: Callable[[], pd.DataFrame] | None = None,
        warehouse_dir: str | Path | None = None,
    ) -> None:
        self.store = store or ParquetStore()
        self.fetcher = fetcher or ak.stock_industry_clf_hist_sw
        self.warehouse_dir = Path(warehouse_dir or self.store.base_dir)

    def fetch_history(self) -> pd.DataFrame:
        """Fetch and normalize the full source snapshot."""

        return normalize_industry_history(self.fetcher())

    def sync(self, *, force_refresh: bool = False) -> tuple[int, int]:
        """Persist all normalized partitions and return (rows, failed)."""

        if not force_refresh and is_synced_today(
            DATASET_INDUSTRY_HISTORY, SYMBOL_SYNC_ALL
        ):
            logger.info("申万行业历史今日已同步，跳过")
            return 0, 0

        frame = self.fetch_history()
        partitions = []
        for symbol, partition in frame.groupby("symbol", sort=True):
            partitions.append(
                (
                    partition.loc[:, [*INDUSTRY_HISTORY_COLUMNS]],
                    str(symbol),
                )
            )
        save_snapshot_atomically(
            self.warehouse_dir,
            self.category,
            partitions,
            operation="sync-industry-history",
            run_id=SYMBOL_SYNC_ALL,
        )
        record_sync_success(DATASET_INDUSTRY_HISTORY, SYMBOL_SYNC_ALL, date.today())
        logger.info(
            "申万行业历史同步完成: %s 行, %s 只股票分区",
            len(frame),
            len(partitions),
        )
        return len(frame), 0
