"""单元测试: sync-stocks 退市股清单合并 (mock 接口与数据库, 不触真实网络)"""

import pandas as pd
import pytest

import main as main_mod
from data_ingestion.collectors.stock_list import StockListCollector
from storage.database import manager as manager_mod


@pytest.fixture(autouse=True)
def _isolate_db(tmp_path, monkeypatch):
    sqlite_path = tmp_path / "test_metadata.db"
    monkeypatch.setattr(manager_mod, "SQLITE_DB_PATH", sqlite_path)
    monkeypatch.setattr(manager_mod.db_manager, "sqlite_path", sqlite_path)
    manager_mod.db_manager._sqlite_conn = None
    manager_mod.db_manager.initialize_schema()
    conn = manager_mod.db_manager.get_sqlite_conn()
    conn.execute(
        "INSERT INTO stocks (symbol, code, name, is_active, last_trade_date) VALUES"
        " ('600519', '600519', '贵州茅台', 1, NULL)"
    )
    conn.execute(
        "INSERT INTO stocks (symbol, code, name, is_active, last_trade_date) VALUES"
        " ('600421', '600421', '*ST华嵘', 0, '20260622')"
    )
    conn.commit()
    yield
    manager_mod.db_manager._sqlite_conn = None


def _fake_ak(monkeypatch, sh_df=None, sz_df=None, raises=None):
    import akshare as ak

    def fake_sh():
        if raises:
            raise raises
        return sh_df

    def fake_sz():
        if raises:
            raise raises
        return sz_df

    monkeypatch.setattr(ak, "stock_info_sh_delist", fake_sh)
    monkeypatch.setattr(ak, "stock_info_sz_delist", fake_sz)


def test_fetch_all_stocks_propagates_api_failure(monkeypatch):
    """股票列表接口异常不能伪装成合法空列表"""
    monkeypatch.setattr(
        "akshare.stock_info_a_code_name",
        lambda: (_ for _ in ()).throw(RuntimeError("接口异常")),
    )
    with pytest.raises(RuntimeError, match="接口异常"):
        StockListCollector().fetch_all_stocks()


def test_merge_inserts_missing_delisted(monkeypatch):
    sh = pd.DataFrame(
        {
            "公司代码": [600002, 600001],
            "公司简称": ["齐鲁退市", "邯郸钢铁"],
            "上市日期": ["1998-04-08", "1998-01-22"],
            "暂停上市日期": ["2006-04-24", "2009-12-29"],
        }
    )
    sz = pd.DataFrame(
        {
            "证券代码": [3, 5],
            "证券简称": ["PT金田A", "ST星源"],
            "上市日期": ["1991-01-14", "1990-12-10"],
            "终止上市日期": ["2002-06-14", "2024-04-26"],
        }
    )
    _fake_ak(monkeypatch, sh, sz)
    main_mod.merge_delisted_stocks()

    conn = manager_mod.db_manager.get_sqlite_conn()
    rows = conn.execute(
        "SELECT code, name, is_active, last_trade_date FROM stocks ORDER BY code"
    ).fetchall()
    assert rows == [
        ("000003", "PT金田A", 0, "20020614"),
        ("000005", "ST星源", 0, "20240426"),
        ("600001", "邯郸钢铁", 0, "20091229"),
        ("600002", "齐鲁退市", 0, "20060424"),
        ("600421", "*ST华嵘", 0, "20260622"),
        ("600519", "贵州茅台", 1, None),
    ]


def test_merge_does_not_modify_existing(monkeypatch):
    """已存在的退市股 (含腾讯纠正的 last_trade_date) 不被清单覆盖"""
    sh = pd.DataFrame(
        {
            "公司代码": [600421],
            "公司简称": ["退市华嵘"],
            "上市日期": ["1997-06-30"],
            "暂停上市日期": ["2026-06-26"],
        }
    )
    sz = pd.DataFrame(
        {"证券代码": [], "证券简称": [], "上市日期": [], "终止上市日期": []}
    )
    _fake_ak(monkeypatch, sh, sz)
    main_mod.merge_delisted_stocks()

    conn = manager_mod.db_manager.get_sqlite_conn()
    row = conn.execute(
        "SELECT name, is_active, last_trade_date FROM stocks WHERE code='600421'"
    ).fetchone()
    assert row == ("*ST华嵘", 0, "20260622")


def test_merge_skips_on_api_failure(monkeypatch):
    _fake_ak(monkeypatch, raises=RuntimeError("接口异常"))
    main_mod.merge_delisted_stocks()
    conn = manager_mod.db_manager.get_sqlite_conn()
    total = conn.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
    assert total == 2


def test_merge_dedup_overlapping_codes(monkeypatch):
    """沪深清单重叠代码只插入一次"""
    sh = pd.DataFrame(
        {
            "公司代码": [600002],
            "公司简称": ["齐鲁退市"],
            "上市日期": ["1998-04-08"],
            "暂停上市日期": ["2006-04-24"],
        }
    )
    sz = pd.DataFrame(
        {
            "证券代码": [600002],
            "证券简称": ["齐鲁退市"],
            "上市日期": ["1998-04-08"],
            "终止上市日期": ["2006-04-24"],
        }
    )
    _fake_ak(monkeypatch, sh, sz)
    main_mod.merge_delisted_stocks()
    conn = manager_mod.db_manager.get_sqlite_conn()
    n = conn.execute("SELECT COUNT(*) FROM stocks WHERE code='600002'").fetchone()[0]
    assert n == 1
