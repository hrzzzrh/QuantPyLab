from datetime import date

from storage.database.manager import db_manager
from utils.logger import logger

DATASET_SHARE_CAPITAL = "share_capital"
DATASET_KLINE = "kline"


def record_sync_success(dataset: str, symbol: str, sync_date: date) -> None:
    """记录数据集在指定日期的同步成功 (UPSERT, 幂等)"""
    conn = db_manager.get_sqlite_conn()
    conn.execute(
        """
        INSERT INTO sync_status (dataset, symbol, last_sync_date, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (dataset, symbol) DO UPDATE SET
            last_sync_date = excluded.last_sync_date,
            updated_at = CURRENT_TIMESTAMP
        """,
        (dataset, symbol, sync_date.isoformat()),
    )
    conn.commit()
    logger.debug(f"记录同步成功: {dataset}/{symbol} @ {sync_date}")


def get_last_sync_date(dataset: str, symbol: str) -> date | None:
    """查询数据集最近一次同步成功日期, 无记录返回 None"""
    conn = db_manager.get_sqlite_conn()
    row = conn.execute(
        "SELECT last_sync_date FROM sync_status WHERE dataset = ? AND symbol = ?",
        (dataset, symbol),
    ).fetchone()
    if row is None or not row[0]:
        return None
    return date.fromisoformat(row[0])


def is_synced_today(dataset: str, symbol: str, today: date | None = None) -> bool:
    """判断数据集当日是否已同步成功"""
    if today is None:
        today = date.today()
    last = get_last_sync_date(dataset, symbol)
    return last is not None and last >= today
