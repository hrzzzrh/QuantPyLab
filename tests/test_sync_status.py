"""单元测试: storage/database/sync_status.py 同步状态记录"""

import sqlite3
from datetime import date

import pytest

import storage.database.sync_status as sync_status_mod
from storage.database.sync_status import (
    DATASET_SYNC_ALL,
    SYMBOL_SYNC_ALL,
    clear_sync_status,
    get_last_sync_date,
    is_synced_today,
    record_sync_success,
)


@pytest.fixture(autouse=True)
def _isolate_db(tmp_path, monkeypatch):
    """每个测试使用独立的 SQLite 元数据库，避免污染真实 metadata.db"""
    from storage.database import manager as manager_mod

    sqlite_path = tmp_path / "test_metadata.db"
    monkeypatch.setattr(manager_mod, "SQLITE_DB_PATH", sqlite_path)
    monkeypatch.setattr(manager_mod, "WAREHOUSE_DIR", str(tmp_path))
    monkeypatch.setattr(sync_status_mod.db_manager, "sqlite_path", sqlite_path)
    sync_status_mod.db_manager._sqlite_conn = None
    sync_status_mod.db_manager.initialize_schema()
    yield
    sync_status_mod.db_manager._sqlite_conn = None


def _table_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sync_status'"
    ).fetchone()
    return row is not None


def test_initialize_schema_creates_sync_status():
    conn = sync_status_mod.db_manager.get_sqlite_conn()
    assert _table_exists(conn)


def test_record_and_get_sync_date():
    record_sync_success("share_capital", "600519", date(2026, 8, 8))
    assert get_last_sync_date("share_capital", "600519") == date(2026, 8, 8)
    assert get_last_sync_date("share_capital", "000001") is None


def test_record_is_idempotent_upsert():
    record_sync_success("share_capital", "600519", date(2026, 8, 7))
    record_sync_success("share_capital", "600519", date(2026, 8, 8))
    conn = sync_status_mod.db_manager.get_sqlite_conn()
    rows = conn.execute(
        "SELECT COUNT(*) FROM sync_status WHERE dataset='share_capital' AND symbol='600519'"
    ).fetchone()
    assert rows[0] == 1
    assert get_last_sync_date("share_capital", "600519") == date(2026, 8, 8)


def test_clear_sync_status_is_dataset_scoped():
    record_sync_success("kline_daily", "600519", date(2026, 8, 8))
    record_sync_success("kline_daily_no_data", "600519", date(2026, 8, 8))

    clear_sync_status("kline_daily_no_data", "600519")

    assert get_last_sync_date("kline_daily", "600519") == date(2026, 8, 8)
    assert get_last_sync_date("kline_daily_no_data", "600519") is None


def test_dataset_isolation():
    record_sync_success("share_capital", "600519", date(2026, 8, 8))
    record_sync_success("kline", "600519", date(2026, 8, 1))
    assert get_last_sync_date("share_capital", "600519") == date(2026, 8, 8)
    assert get_last_sync_date("kline", "600519") == date(2026, 8, 1)


def test_is_synced_today():
    today = date(2026, 8, 8)
    assert not is_synced_today("share_capital", "600519", today=today)
    record_sync_success("share_capital", "600519", today)
    assert is_synced_today("share_capital", "600519", today=today)
    assert not is_synced_today("share_capital", "600519", today=date(2026, 8, 9))
    assert not is_synced_today("kline", "600519", today=today)


def test_sync_all_roundtrip():
    """sync-all 全流程状态记录 (dataset=sync_all, symbol=ALL 固定占位符)"""
    assert get_last_sync_date(DATASET_SYNC_ALL, SYMBOL_SYNC_ALL) is None
    record_sync_success(DATASET_SYNC_ALL, SYMBOL_SYNC_ALL, date(2026, 8, 8))
    assert get_last_sync_date(DATASET_SYNC_ALL, SYMBOL_SYNC_ALL) == date(2026, 8, 8)
    record_sync_success(DATASET_SYNC_ALL, SYMBOL_SYNC_ALL, date(2026, 8, 9))
    conn = sync_status_mod.db_manager.get_sqlite_conn()
    rows = conn.execute(
        "SELECT COUNT(*) FROM sync_status"
        f" WHERE dataset='{DATASET_SYNC_ALL}' AND symbol='{SYMBOL_SYNC_ALL}'"
    ).fetchone()
    assert rows[0] == 1
