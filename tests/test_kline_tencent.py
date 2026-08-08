"""单元测试: 退市股 K线腾讯全量重建 (mock 网络与数据库, 不触真实网络)"""

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


def test_delisted_route_uses_tencent_rebuild(monkeypatch):
    collector = DailyKlineCollector()
    called = {"collect_delisted": False}

    def fake_delisted(symbol):
        return symbol == "600421"

    def fake_rebuild(symbol):
        called["collect_delisted"] = True

    monkeypatch.setattr(collector, "_is_delisted", fake_delisted)
    monkeypatch.setattr(collector, "_collect_delisted", fake_rebuild)
    collector.collect_kline("600421")
    assert called["collect_delisted"] is True


def test_fetch_tencent_full_merges_adj_factor(monkeypatch):
    collector = DailyKlineCollector()
    day_rows = [
        ["2026-04-28", "4.29", "4.29", "4.29", "4.29", "100.000"],
        ["2026-04-29", "4.08", "4.08", "4.08", "4.08", "200.000"],
    ]
    hfq_rows = [
        ["2026-04-28", "8.24", "8.24", "8.24", "8.24", "100.000"],
        ["2026-04-29", "7.83", "7.83", "7.83", "7.83", "200.000"],
    ]

    def fake_page(symbol, end_date, fq):
        return day_rows if not fq else hfq_rows

    monkeypatch.setattr(collector, "_fetch_tencent_page", fake_page)
    df = collector._fetch_tencent_full("600421")
    assert len(df) == 2
    assert df["adj_factor"].tolist() == pytest.approx([8.24 / 4.29, 7.83 / 4.08])
    assert df["amount"].iloc[0] == pytest.approx(100.0 * 100 * 4.29)
    assert df["date"].iloc[-1] == date(2026, 4, 29)


def test_collect_delisted_rebuild_and_mark(monkeypatch):
    collector = DailyKlineCollector()
    rows = [
        ["2026-04-28", "4.29", "4.29", "4.29", "4.29", "100.000"],
        ["2026-04-29", "4.08", "4.08", "4.08", "4.08", "200.000"],
    ]

    def fake_full(symbol):
        df = pd.DataFrame(
            rows,
            columns=["date", "open", "close", "high", "low", "volume"],
        )
        df["adj_factor"] = 1.92
        df["amount"] = df["volume"].astype(float) * 100 * df["close"].astype(float)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["symbol"] = symbol
        return df

    monkeypatch.setattr(collector, "_fetch_tencent_full", fake_full)
    saved = {}
    monkeypatch.setattr(
        collector.store,
        "save_partition",
        lambda df, cat, sym: saved.update({"cat": cat, "sym": sym, "n": len(df)}),
    )

    collector._collect_delisted("600421")
    assert saved == {"cat": "daily_kline", "sym": "600421", "n": 2}
    assert get_last_sync_date(DATASET_KLINE, "600421") == date.today()

    conn = manager_mod.db_manager.get_sqlite_conn()
    row = conn.execute(
        "SELECT last_trade_date FROM stocks WHERE symbol='600421'"
    ).fetchone()
    assert row[0] == "20260429"


def test_collect_delisted_skips_when_rebuilt(monkeypatch):
    collector = DailyKlineCollector()
    record_sync_success(DATASET_KLINE, "600421", date.today())
    monkeypatch.setattr(
        collector,
        "_fetch_tencent_full",
        lambda sym: (_ for _ in ()).throw(AssertionError("不应抓取")),
    )
    collector._collect_delisted("600421")


def test_fetch_tencent_full_rejects_all_negative(monkeypatch):
    """腾讯 hfq 全为负 (极端历史个股) 时放弃重建"""
    collector = DailyKlineCollector()
    day_rows = [
        ["2026-07-10", "0.46", "0.46", "0.46", "0.46", "100.000"],
        ["2026-07-13", "0.51", "0.51", "0.51", "0.51", "200.000"],
    ]
    hfq_rows = [
        ["2026-07-10", "-6.12", "-6.04", "-6.04", "-6.12", "100.000"],
        ["2026-07-13", "-5.64", "-5.64", "-5.64", "-5.64", "200.000"],
    ]

    def fake_page(symbol, end_date, fq):
        return day_rows if not fq else hfq_rows

    monkeypatch.setattr(collector, "_fetch_tencent_page", fake_page)
    with pytest.raises(RuntimeError, match="全为负"):
        collector._fetch_tencent_full("000004")


def test_fetch_tencent_full_partial_negative_uses_seam(monkeypatch):
    """hfq 仅退市整理期段为负时: 尾段 adj_factor 按接缝因子延续"""
    collector = DailyKlineCollector()
    day_rows = [
        ["2026-06-20", "2.76", "2.76", "2.76", "2.76", "100.000"],
        ["2026-06-23", "0.31", "0.31", "0.31", "0.31", "200.000"],
        ["2026-06-24", "0.28", "0.28", "0.28", "0.28", "300.000"],
    ]
    hfq_rows = [
        ["2026-06-20", "12.345", "12.345", "12.345", "12.345", "100.000"],
        ["2026-06-23", "-7.241", "-7.241", "-7.241", "-7.241", "200.000"],
        ["2026-06-24", "-7.481", "-7.481", "-7.481", "-7.481", "300.000"],
    ]

    def fake_page(symbol, end_date, fq):
        return day_rows if not fq else hfq_rows

    monkeypatch.setattr(collector, "_fetch_tencent_page", fake_page)
    df = collector._fetch_tencent_full("000004")
    seam = 12.345 / 2.76
    assert len(df) == 3
    assert df["adj_factor"].iloc[0] == pytest.approx(seam)
    assert df["adj_factor"].iloc[1] == pytest.approx(seam)
    assert df["adj_factor"].iloc[2] == pytest.approx(seam)


def test_last_trade_date_always_updated(monkeypatch):
    collector = DailyKlineCollector()
    conn = manager_mod.db_manager.get_sqlite_conn()
    conn.execute("UPDATE stocks SET last_trade_date='20260401' WHERE symbol='600421'")
    conn.commit()

    def fake_full(symbol):
        df = pd.DataFrame(
            [["2026-06-22", "0.24", "0.24", "0.24", "0.24", "200.000"]],
            columns=["date", "open", "close", "high", "low", "volume"],
        )
        df["adj_factor"] = 1.92
        df["amount"] = 1.0
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["symbol"] = symbol
        return df

    monkeypatch.setattr(collector, "_fetch_tencent_full", fake_full)
    monkeypatch.setattr(
        collector.store,
        "save_partition",
        lambda df, cat, sym: None,
    )
    collector._collect_delisted("600421")
    row = conn.execute(
        "SELECT last_trade_date FROM stocks WHERE symbol='600421'"
    ).fetchone()
    assert row[0] == "20260622"
