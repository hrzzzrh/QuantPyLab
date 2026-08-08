"""单元测试: sync-metadata 行业雪球补全 (mock 雪球接口, 不触真实网络)"""

from unittest import mock

import pytest

import main as main_mod
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
        "INSERT INTO stocks (symbol, code, name, industry, is_active) VALUES"
        " ('600519', '600519', '贵州茅台', NULL, 1)"
    )
    conn.execute(
        "INSERT INTO stocks (symbol, code, name, industry, is_active) VALUES"
        " ('000001', '000001', '平安银行', '银行', 1)"
    )
    conn.execute(
        "INSERT INTO stocks (symbol, code, name, industry, is_active) VALUES"
        " ('600421', '600421', '*ST华嵘', NULL, 0)"
    )
    conn.commit()
    yield
    manager_mod.db_manager._sqlite_conn = None


def _fake_collector(monkeypatch, results):
    class FakeDetailCollector:
        def fetch_from_xueqiu(self, symbol):
            if symbol in results:
                return {"industry_xq": results[symbol]}
            return {}

    monkeypatch.setattr(main_mod, "StockDetailCollector", FakeDetailCollector)
    monkeypatch.setattr(main_mod, "time", mock.Mock())
    monkeypatch.setattr(main_mod.time, "sleep", lambda _: None)


def test_sync_industries_fills_missing_active_only(monkeypatch):
    """仅补全 industry 为 NULL 的活跃股, 跳过已有行业与退市股"""
    _fake_collector(monkeypatch, {"sh600519": "白酒"})
    conn = manager_mod.db_manager.get_sqlite_conn()
    main_mod._sync_industries_via_xueqiu(conn)

    rows = dict(conn.execute("SELECT symbol, industry FROM stocks").fetchall())
    assert rows["600519"] == "白酒"
    assert rows["000001"] == "银行"
    assert rows["600421"] is None


def test_sync_industries_skips_failed(monkeypatch):
    """雪球无资料 (退市股/北交所) 计入失败, 不写入"""
    _fake_collector(monkeypatch, {})
    conn = manager_mod.db_manager.get_sqlite_conn()
    main_mod._sync_industries_via_xueqiu(conn)
    rows = dict(conn.execute("SELECT symbol, industry FROM stocks").fetchall())
    assert rows["600519"] is None
