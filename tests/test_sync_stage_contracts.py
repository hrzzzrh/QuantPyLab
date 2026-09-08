"""单元测试: sync-all 八个环节的 (processed, failed) 返回契约。"""

from datetime import date

import pandas as pd
import pytest

import main as main_mod


@pytest.fixture
def isolated_metadata_db(tmp_path, monkeypatch):
    from storage.database import manager as manager_mod

    sqlite_path = tmp_path / "metadata.db"
    monkeypatch.setattr(manager_mod, "SQLITE_DB_PATH", sqlite_path)
    monkeypatch.setattr(main_mod.db_manager, "sqlite_path", sqlite_path)
    main_mod.db_manager._sqlite_conn = None
    main_mod.db_manager.initialize_schema()
    yield
    main_mod.db_manager._sqlite_conn = None


def test_sync_stock_list_counts_delisted_merge_failure(
    monkeypatch, isolated_metadata_db
):
    class FakeStockListCollector:
        def fetch_all_stocks(self):
            return pd.DataFrame(
                {
                    "symbol": ["600519"],
                    "code": ["600519"],
                    "name": ["测试股票"],
                    "area": [None],
                    "industry": [None],
                    "list_date": [None],
                    "is_active": [1],
                    "updated_at": ["2026-08-09 00:00:00"],
                }
            )

    monkeypatch.setattr(main_mod, "StockListCollector", FakeStockListCollector)
    monkeypatch.setattr(main_mod, "merge_delisted_stocks", lambda: False)
    assert main_mod.sync_stock_list() == (1, 1)


def test_sync_stock_list_empty_response_counts_failure(
    monkeypatch, isolated_metadata_db
):
    """股票列表空响应必须失败, 且不得执行退市合并或写入数据库"""
    merge_calls = []

    class FakeStockListCollector:
        def fetch_all_stocks(self):
            return pd.DataFrame()

    monkeypatch.setattr(main_mod, "StockListCollector", FakeStockListCollector)
    monkeypatch.setattr(
        main_mod,
        "merge_delisted_stocks",
        lambda: merge_calls.append(True) or True,
    )

    assert main_mod.sync_stock_list() == (1, 1)
    assert merge_calls == []
    assert (
        main_mod.db_manager.get_sqlite_conn()
        .execute("SELECT COUNT(*) FROM stocks")
        .fetchone()[0]
        == 0
    )


def test_sync_financial_indicators_counts_failure(monkeypatch):
    class FakeIndicatorStore:
        pass

    class FakeFinancialCollector:
        def collect_indicators(self, *args, **kwargs):
            raise RuntimeError("指标接口异常")

    monkeypatch.setattr(main_mod, "get_all_stocks", lambda: [("600519", "测试")])
    monkeypatch.setattr(main_mod, "IndicatorStore", FakeIndicatorStore)
    monkeypatch.setattr(main_mod, "FinancialCollector", FakeFinancialCollector)
    monkeypatch.setattr(main_mod.time, "sleep", lambda _: None)
    assert main_mod.sync_financial_indicators(symbol="600519") == (1, 1)


def test_sync_financial_statements_counts_failure(monkeypatch):
    class FakeFinancialStore:
        pass

    class FakeFinancialCollector:
        def fetch_statement(self, *args, **kwargs):
            raise RuntimeError("报表接口异常")

    monkeypatch.setattr(main_mod, "get_all_stocks", lambda: [("600519", "测试")])
    monkeypatch.setattr(main_mod, "FinancialStore", FakeFinancialStore)
    monkeypatch.setattr(main_mod, "FinancialCollector", FakeFinancialCollector)
    monkeypatch.setattr(main_mod.time, "sleep", lambda _: None)
    assert main_mod.sync_financial_statements(symbol="600519") == (1, 1)


def test_calculate_ttm_single_symbol_counts_failure(monkeypatch):
    import analysis.processors.ttm_calculator as ttm_mod

    class FakeTTMCalculator:
        def calculate_for_symbol(self, symbol):
            raise RuntimeError("TTM 计算异常")

    monkeypatch.setattr(main_mod, "get_all_stocks", lambda: [("600519", "测试")])
    monkeypatch.setattr(main_mod, "get_financial_symbols", lambda: ["600519"])
    monkeypatch.setattr(ttm_mod, "TTMCalculator", FakeTTMCalculator)
    assert main_mod.calculate_ttm_metrics(symbol="600519") == (1, 1)


def test_sync_share_capital_counts_failure(monkeypatch):
    from data_ingestion.collectors import share_collector
    from utils import trade_date

    class FakeShareCollector:
        def collect_share_capital(self, *args, **kwargs):
            raise RuntimeError("股本接口异常")

    monkeypatch.setattr(main_mod, "get_all_stocks", lambda: [("600519", "测试")])
    monkeypatch.setattr(share_collector, "ShareCollector", FakeShareCollector)
    monkeypatch.setattr(trade_date, "get_latest_trade_date", lambda: date(2026, 8, 7))
    monkeypatch.setattr(main_mod.time, "sleep", lambda _: None)
    assert main_mod.sync_share_capital(symbol="600519") == (1, 1)


def test_sync_holder_number_counts_failure(monkeypatch):
    from data_ingestion.collectors import holder_collector

    class FakeHolderCollector:
        def collect_holder_number(self, *args, **kwargs):
            raise RuntimeError("户数接口异常")

    monkeypatch.setattr(main_mod, "get_all_stocks", lambda: [("600519", "测试")])
    monkeypatch.setattr(holder_collector, "HolderCollector", FakeHolderCollector)
    monkeypatch.setattr(main_mod.time, "sleep", lambda _: None)
    assert main_mod.sync_holder_number(symbol="600519") == (1, 1)


def test_sync_holder_number_batch_uses_active_stocks(monkeypatch):
    from data_ingestion.collectors import holder_collector
    from storage.database import sync_status as sync_status_mod

    seen = []

    class FakeHolderCollector:
        def collect_holder_number(self, code, **kwargs):
            seen.append(code)

    monkeypatch.setattr(
        main_mod,
        "get_active_stocks",
        lambda: [("600519", "测试")],
    )
    monkeypatch.setattr(
        main_mod,
        "get_all_stocks",
        lambda: [("600519", "测试"), ("000004", "退市")],
    )
    monkeypatch.setattr(holder_collector, "HolderCollector", FakeHolderCollector)
    monkeypatch.setattr(sync_status_mod, "is_synced_today", lambda *a, **k: False)
    monkeypatch.setattr(main_mod.time, "sleep", lambda _: None)
    assert main_mod.sync_holder_number() == (1, 0)
    assert seen == ["600519"]


def test_get_active_stocks_includes_null_flag(isolated_metadata_db):
    """is_active 为 NULL 的历史行不得被批量静默丢弃。"""
    conn = main_mod.db_manager.get_sqlite_conn()
    conn.execute(
        "INSERT INTO stocks (symbol, code, name, is_active) VALUES "
        "('600519', '600519', '测试', NULL), ('000001', '000001', '平安', 0)"
    )
    conn.commit()
    assert main_mod.get_active_stocks() == [("600519", "测试")]


def test_sync_holder_number_unknown_symbol_warns_empty(monkeypatch, caplog):
    from data_ingestion.collectors import holder_collector

    class FakeHolderCollector:
        def collect_holder_number(self, *args, **kwargs):  # pragma: no cover
            raise AssertionError("目标为空时不应调用采集器")

    monkeypatch.setattr(main_mod, "get_all_stocks", lambda: [("600519", "测试")])
    monkeypatch.setattr(holder_collector, "HolderCollector", FakeHolderCollector)
    with caplog.at_level("WARNING"):
        assert main_mod.sync_holder_number(symbol="999999") == (0, 0)
    assert "目标为空" in caplog.text


def test_sync_daily_kline_counts_failure(monkeypatch):
    from data_ingestion.collectors import kline_collector
    from utils import trade_date

    class FakeDailyKlineCollector:
        def __init__(self, source=None):
            self.source = source

        def collect_kline(self, *args, **kwargs):
            raise RuntimeError("K线接口异常")

    monkeypatch.setattr(main_mod, "get_all_stocks", lambda: [("600519", "测试")])
    monkeypatch.setattr(kline_collector, "DailyKlineCollector", FakeDailyKlineCollector)
    monkeypatch.setattr(trade_date, "get_latest_trade_date", lambda: date(2026, 8, 7))
    assert main_mod.sync_daily_kline(symbol="600519") == (1, 1)
