"""单元测试: sync_financial_statements 财务不完整标记 (孤儿补全防循环)"""

import pandas as pd
import pytest

import main as main_mod
from storage.database import manager as manager_mod
from storage.database.sync_status import (
    DATASET_FINANCIAL_INCOMPLETE,
    get_last_sync_date,
    record_sync_success,
)


@pytest.fixture(autouse=True)
def _isolate_db(tmp_path, monkeypatch):
    sqlite_path = tmp_path / "test_metadata.db"
    warehouse_dir = tmp_path / "warehouse"
    monkeypatch.setattr(manager_mod, "SQLITE_DB_PATH", sqlite_path)
    monkeypatch.setattr(manager_mod, "WAREHOUSE_DIR", str(warehouse_dir))
    monkeypatch.setattr(manager_mod.db_manager, "sqlite_path", sqlite_path)
    manager_mod.db_manager._sqlite_conn = None
    manager_mod.db_manager.initialize_schema()
    conn = manager_mod.db_manager.get_sqlite_conn()
    conn.execute(
        "INSERT INTO stocks (symbol, code, name, is_active) VALUES"
        " ('000508', '000508', '琼民源A', 0)"
    )
    conn.commit()
    yield
    manager_mod.db_manager._sqlite_conn = None


def _mock_env(monkeypatch, orphans, fetch_result=None):
    """mock 披露日历为空、孤儿列表指定、fetch 全空、sleep 无延时"""
    monkeypatch.setattr(main_mod, "get_target_report_dates", lambda: [])
    monkeypatch.setattr(main_mod, "get_orphan_codes", lambda cat, codes: orphans)
    monkeypatch.setattr(main_mod.time, "sleep", lambda _: None)

    calls = []

    def fake_fetch(self, code, st):
        calls.append((code, st))
        return pd.DataFrame() if fetch_result is None else fetch_result

    monkeypatch.setattr(main_mod.FinancialCollector, "fetch_statement", fake_fetch)

    class FakeStore:
        def __init__(self):
            self.saved = []

        def get_existing_report_dates(self):
            return set()

        def save_statement(self, df, table_name):
            self.saved.append(table_name)

    monkeypatch.setattr(main_mod, "FinancialStore", FakeStore)
    return calls


def test_orphan_backfill_records_incomplete_marker(monkeypatch):
    """孤儿补全处理完确证缺表股后记录 DATASET_FINANCIAL_INCOMPLETE"""
    from datetime import date

    _mock_env(monkeypatch, orphans=["000508"])
    main_mod.sync_financial_statements()

    assert get_last_sync_date(DATASET_FINANCIAL_INCOMPLETE, "000508") == date.today()


def test_marked_stock_excluded_from_orphan_pickup(monkeypatch):
    """已记录财务不完整标记的股不再被孤儿补全选中 (零请求)"""
    from datetime import date

    record_sync_success(DATASET_FINANCIAL_INCOMPLETE, "000508", date.today())
    calls = _mock_env(monkeypatch, orphans=["000508"])

    main_mod.sync_financial_statements()

    assert calls == [], f"已标记股不应再被孤儿补全请求, 实际请求: {calls}"


def test_marked_stock_not_skipped_on_explicit_symbol(monkeypatch):
    """单股模式 (--symbol) 不受标记影响, 仍执行抓取"""
    from datetime import date

    record_sync_success(DATASET_FINANCIAL_INCOMPLETE, "000508", date.today())
    calls = _mock_env(monkeypatch, orphans=[])

    main_mod.sync_financial_statements(symbol="000508")

    assert calls, "单股显式指定应正常执行抓取"
    assert len(calls) == 3  # balance/profit/cashflow 三张表
