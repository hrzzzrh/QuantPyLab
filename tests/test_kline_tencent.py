"""单元测试: 退市股 K线 Sina 全量重建 (mock 网络与数据库, 不触真实网络)"""

from datetime import date

import pandas as pd
import pytest

from data_ingestion.collectors.kline_collector import DailyKlineCollector
from storage.database import manager as manager_mod
from storage.database.sync_status import (
    DATASET_KLINE,
    get_last_sync_date,
    record_sync_success,
)


@pytest.fixture(autouse=True)
def _isolate_env(tmp_path, monkeypatch):
    """隔离 SQLite 元数据库与 Parquet 仓库目录"""
    from storage.file_store import parquet_store as parquet_store_mod

    sqlite_path = tmp_path / "test_metadata.db"
    warehouse_dir = tmp_path / "warehouse"
    monkeypatch.setattr(manager_mod, "SQLITE_DB_PATH", sqlite_path)
    monkeypatch.setattr(manager_mod, "WAREHOUSE_DIR", str(warehouse_dir))
    monkeypatch.setattr(parquet_store_mod, "WAREHOUSE_DIR", str(warehouse_dir))
    from storage.database import sync_status as ss

    monkeypatch.setattr(ss.db_manager, "sqlite_path", sqlite_path)
    ss.db_manager._sqlite_conn = None
    ss.db_manager.initialize_schema()
    conn = ss.db_manager.get_sqlite_conn()
    conn.execute(
        "INSERT INTO stocks (symbol, code, name, is_active) VALUES ('600421', '600421', '*ST华嵘', 0)"
    )
    conn.execute(
        "INSERT INTO stocks (symbol, code, name, is_active) VALUES ('600519', '600519', '贵州茅台', 1)"
    )
    conn.commit()
    yield
    ss.db_manager._sqlite_conn = None


def test_is_delisted():
    collector = DailyKlineCollector()
    assert collector._is_delisted("600421") is True
    assert collector._is_delisted("600519") is False
    assert collector._is_delisted("000001") is False


def test_delisted_route_calls_sina_rebuild(monkeypatch):
    collector = DailyKlineCollector()
    called = {"sina": False}

    def fake_delisted(symbol):
        return symbol == "600421"

    def fake_sina(sina_symbol, start_date="19900101", end_date=None):
        called["sina"] = True
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_is_delisted", fake_delisted)
    monkeypatch.setattr(collector, "_fetch_sina_klc", fake_sina)
    collector.collect_kline("600421")
    assert called["sina"] is True


def test_delisted_rebuild_and_mark(monkeypatch):
    collector = DailyKlineCollector()
    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda sym, sd="19900101", ed=None: pd.DataFrame(
            {
                "date": ["2026-04-28", "2026-04-29"],
                "open": [4.29, 4.08],
                "high": [4.29, 4.08],
                "low": [4.29, 4.08],
                "close": [4.29, 4.08],
                "volume": [100.0, 200.0],
                "amount": [42900.0, 81600.0],
                "close_hfq": [4.29, 4.08],
                "adj_factor": [1.0, 1.0],
            }
        ),
    )
    saved = {}
    monkeypatch.setattr(
        collector.store,
        "save_partition",
        lambda df, cat, sym: saved.update({"cat": cat, "sym": sym, "n": len(df)}),
    )

    result = collector.collect_kline("600421")
    assert result is True
    assert saved == {"cat": "daily_kline", "sym": "600421", "n": 2}
    assert get_last_sync_date(DATASET_KLINE, "600421") == date.today()

    conn = manager_mod.db_manager.get_sqlite_conn()
    row = conn.execute(
        "SELECT last_trade_date FROM stocks WHERE symbol='600421'"
    ).fetchone()
    assert row[0] == "20260429"


def test_delisted_skips_when_rebuilt(monkeypatch):
    collector = DailyKlineCollector()
    record_sync_success(DATASET_KLINE, "600421", date.today())
    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda sym, sd="19900101", ed=None: (_ for _ in ()).throw(
            AssertionError("不应抓取")
        ),
    )
    result = collector.collect_kline("600421")
    assert result is False


def test_delisted_sina_empty_returns_false(monkeypatch):
    collector = DailyKlineCollector()
    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda sym, sd="19900101", ed=None: pd.DataFrame(),
    )
    result = collector.collect_kline("600421")
    assert result is False
    assert get_last_sync_date(DATASET_KLINE, "600421") is None


def test_last_trade_date_always_updated(monkeypatch):
    collector = DailyKlineCollector()
    conn = manager_mod.db_manager.get_sqlite_conn()
    conn.execute("UPDATE stocks SET last_trade_date='20260401' WHERE symbol='600421'")
    conn.commit()

    monkeypatch.setattr(
        collector,
        "_fetch_sina_klc",
        lambda sym, sd="19900101", ed=None: pd.DataFrame(
            {
                "date": ["2026-06-22"],
                "open": [0.24],
                "high": [0.24],
                "low": [0.24],
                "close": [0.24],
                "volume": [200.0],
                "amount": [1.0],
                "close_hfq": [0.24],
                "adj_factor": [1.0],
            }
        ),
    )
    monkeypatch.setattr(
        collector.store,
        "save_partition",
        lambda df, cat, sym: None,
    )
    collector.collect_kline("600421")
    row = conn.execute(
        "SELECT last_trade_date FROM stocks WHERE symbol='600421'"
    ).fetchone()
    assert row[0] == "20260622"
