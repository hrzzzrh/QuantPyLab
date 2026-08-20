"""单元测试: sync_financial_statements 财务不完整标记 (孤儿补全防循环)"""

from datetime import date

import pandas as pd
import pytest

import main as main_mod
from storage.database import manager as manager_mod
from storage.database.sync_status import (
    DATASET_FINANCIAL_DATE_RECONCILIATION_PENDING,
    DATASET_FINANCIAL_INCOMPLETE,
    DATASET_FINANCIAL_OFFICIAL_PENDING,
    DATASET_FINANCIAL_TTM_PENDING,
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


def _mock_env(monkeypatch, orphans, fetch_result=None, save_result=None):
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
            return save_result

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


def test_official_date_correction_recalculates_ttm(monkeypatch):
    """官方日期回填后，当前股票应立即重算 TTM。"""
    import analysis.processors.ttm_calculator as ttm_mod

    _mock_env(
        monkeypatch,
        orphans=[],
        fetch_result=pd.DataFrame(
            {
                "symbol": ["000508"],
                "report_date": ["20240930"],
                "公告日期": ["20241102"],
            }
        ),
    )
    verification_calls = []
    monkeypatch.setattr(
        main_mod,
        "verify_overdue_financial_publish_dates_for_symbol",
        lambda code, resolver, *args, **kwargs: (
            verification_calls.append((code, resolver))
            or type(
                "Verification",
                (),
                {"changed_rows": {"income": 1}, "unresolved_report_dates": ()},
            )()
        ),
    )
    ttm_calls = []

    class FakeTTMCalculator:
        def __init__(self):
            pass

        def calculate_for_symbol(self, code):
            ttm_calls.append(code)

    monkeypatch.setattr(ttm_mod, "TTMCalculator", FakeTTMCalculator)

    assert main_mod.sync_financial_statements(symbol="000508") == (1, 0)
    assert [code for code, _resolver in verification_calls] == ["000508"]
    assert ttm_calls == ["000508"]


def test_source_date_correction_recalculates_ttm(monkeypatch):
    """来源日期统一发生变更时，即使官方核验无变更也要重算 TTM。"""
    import analysis.processors.ttm_calculator as ttm_mod

    _mock_env(
        monkeypatch,
        orphans=[],
        fetch_result=pd.DataFrame(
            {
                "symbol": ["000508"],
                "report_date": ["20240930"],
                "公告日期": ["20241102"],
            }
        ),
        save_result={"income": 1},
    )
    monkeypatch.setattr(
        main_mod,
        "verify_overdue_financial_publish_dates_for_symbol",
        lambda code, resolver, *args, **kwargs: type(
            "Verification",
            (),
            {"changed_rows": {}, "unresolved_report_dates": ()},
        )(),
    )
    ttm_calls = []

    class FakeTTMCalculator:
        def __init__(self):
            pass

        def calculate_for_symbol(self, code):
            ttm_calls.append(code)

    monkeypatch.setattr(ttm_mod, "TTMCalculator", FakeTTMCalculator)

    assert main_mod.sync_financial_statements(symbol="000508") == (1, 0)
    assert ttm_calls == ["000508"]


def test_official_date_verification_failure_is_retryable(monkeypatch):
    """官方核验未完成时计入失败并在下一轮重新处理该股票。"""
    _mock_env(monkeypatch, orphans=[])
    monkeypatch.setattr(
        main_mod,
        "verify_overdue_financial_publish_dates_for_symbol",
        lambda code, resolver, *args, **kwargs: type(
            "Verification",
            (),
            {"changed_rows": {}, "unresolved_report_dates": ("20240930",)},
        )(),
    )

    assert main_mod.sync_financial_statements(symbol="000508") == (1, 1)
    assert (
        get_last_sync_date(DATASET_FINANCIAL_OFFICIAL_PENDING, "000508") == date.today()
    )

    calls = _mock_env(monkeypatch, orphans=[])
    monkeypatch.setattr(
        main_mod,
        "verify_overdue_financial_publish_dates_for_symbol",
        lambda code, resolver, *args, **kwargs: type(
            "Verification",
            (),
            {"changed_rows": {}, "unresolved_report_dates": ()},
        )(),
    )
    assert main_mod.sync_financial_statements() == (1, 0)
    assert calls == [
        ("000508", "balance"),
        ("000508", "profit"),
        ("000508", "cashflow"),
    ]
    assert get_last_sync_date(DATASET_FINANCIAL_OFFICIAL_PENDING, "000508") is None


def test_ttm_failure_keeps_official_pending_status(monkeypatch):
    """官方日期已回填但 TTM 失败时，股票仍需在下一轮重试。"""
    import analysis.processors.ttm_calculator as ttm_mod

    _mock_env(monkeypatch, orphans=[])
    monkeypatch.setattr(
        main_mod,
        "verify_overdue_financial_publish_dates_for_symbol",
        lambda code, resolver, *args, **kwargs: type(
            "Verification",
            (),
            {"changed_rows": {"income": 1}, "unresolved_report_dates": ()},
        )(),
    )

    class FailingTTMCalculator:
        def __init__(self):
            pass

        def calculate_for_symbol(self, code):
            raise RuntimeError(f"TTM failed: {code}")

    monkeypatch.setattr(ttm_mod, "TTMCalculator", FailingTTMCalculator)

    assert main_mod.sync_financial_statements(symbol="000508") == (1, 1)
    assert (
        get_last_sync_date(DATASET_FINANCIAL_OFFICIAL_PENDING, "000508") == date.today()
    )
    assert get_last_sync_date(DATASET_FINANCIAL_TTM_PENDING, "000508") == date.today()

    calls = _mock_env(monkeypatch, orphans=[])
    monkeypatch.setattr(
        main_mod,
        "verify_overdue_financial_publish_dates_for_symbol",
        lambda code, resolver, *args, **kwargs: type(
            "Verification",
            (),
            {"changed_rows": {}, "unresolved_report_dates": ()},
        )(),
    )
    ttm_retry_calls = []

    class SucceedingTTMCalculator:
        def __init__(self):
            pass

        def calculate_for_symbol(self, code):
            ttm_retry_calls.append(code)

    monkeypatch.setattr(ttm_mod, "TTMCalculator", SucceedingTTMCalculator)

    assert main_mod.sync_financial_statements() == (1, 0)
    assert calls == [
        ("000508", "balance"),
        ("000508", "profit"),
        ("000508", "cashflow"),
    ]
    assert ttm_retry_calls == ["000508"]
    assert get_last_sync_date(DATASET_FINANCIAL_OFFICIAL_PENDING, "000508") is None
    assert get_last_sync_date(DATASET_FINANCIAL_TTM_PENDING, "000508") is None


def test_partial_official_correction_recalculates_ttm_and_stays_pending(monkeypatch):
    """部分官方日期命中时，已修正数据仍要重算且保留待重试状态。"""
    import analysis.processors.ttm_calculator as ttm_mod

    _mock_env(monkeypatch, orphans=[])
    monkeypatch.setattr(
        main_mod,
        "verify_overdue_financial_publish_dates_for_symbol",
        lambda code, resolver, *args, **kwargs: type(
            "Verification",
            (),
            {"changed_rows": {"income": 1}, "unresolved_report_dates": ("20240930",)},
        )(),
    )
    ttm_calls = []

    class FakeTTMCalculator:
        def __init__(self):
            pass

        def calculate_for_symbol(self, code):
            ttm_calls.append(code)

    monkeypatch.setattr(ttm_mod, "TTMCalculator", FakeTTMCalculator)

    assert main_mod.sync_financial_statements(symbol="000508") == (1, 1)
    assert ttm_calls == ["000508"]
    assert (
        get_last_sync_date(DATASET_FINANCIAL_OFFICIAL_PENDING, "000508") == date.today()
    )


def test_indicator_sync_retries_ttm_pending_symbol(monkeypatch):
    """指标增量同步也必须选中并完成 TTM 待处理股票。"""
    import analysis.processors.ttm_calculator as ttm_mod

    _mock_env(monkeypatch, orphans=[])
    record_sync_success(DATASET_FINANCIAL_TTM_PENDING, "000508", date.today())
    indicator_calls = []

    class FakeIndicatorStore:
        def __init__(self):
            pass

        def get_existing_report_dates(self):
            return set()

    class FakeFinancialCollector:
        def collect_indicators(self, code, market_symbol):
            indicator_calls.append((code, market_symbol))
            return {}

    monkeypatch.setattr(main_mod, "IndicatorStore", FakeIndicatorStore)
    monkeypatch.setattr(main_mod, "FinancialCollector", FakeFinancialCollector)
    ttm_calls = []

    class FakeTTMCalculator:
        def __init__(self):
            pass

        def calculate_for_symbol(self, code):
            ttm_calls.append(code)

    monkeypatch.setattr(ttm_mod, "TTMCalculator", FakeTTMCalculator)

    assert main_mod.sync_financial_indicators() == (1, 0)
    assert indicator_calls == [("000508", "000508.SZ")]
    assert ttm_calls == ["000508"]
    assert get_last_sync_date(DATASET_FINANCIAL_TTM_PENDING, "000508") is None


def test_batch_ttm_retries_pending_symbol(monkeypatch):
    """批量 TTM 计算不能因报告期最大值未变化而跳过待处理股票。"""
    import analysis.processors.ttm_calculator as ttm_mod

    record_sync_success(DATASET_FINANCIAL_TTM_PENDING, "000508", date.today())
    monkeypatch.setattr(main_mod, "get_financial_symbols", lambda: ["000508"])
    monkeypatch.setattr(main_mod, "get_all_stocks", lambda: [("000508", "琼民源A")])
    monkeypatch.setattr(main_mod.db_manager, "ensure_views", lambda *args: None)
    monkeypatch.setattr(
        main_mod.db_manager, "list_available_views", lambda: {"fin_ttm"}
    )

    class EmptyDuckDB:
        def execute(self, sql):
            return self

        def fetchall(self):
            return []

    monkeypatch.setattr(main_mod.db_manager, "get_duckdb_conn", lambda: EmptyDuckDB())
    ttm_calls = []

    class FakeTTMCalculator:
        def __init__(self):
            pass

        def calculate_for_symbol(self, code):
            ttm_calls.append(code)

    monkeypatch.setattr(ttm_mod, "TTMCalculator", FakeTTMCalculator)

    assert main_mod.calculate_ttm_metrics() == (1, 0)
    assert ttm_calls == ["000508"]
    assert get_last_sync_date(DATASET_FINANCIAL_TTM_PENDING, "000508") is None


def test_indicator_date_reconciliation_failure_is_retryable(monkeypatch):
    """指标日期协调失败后，下一轮指标同步必须重新选中该股票。"""
    _mock_env(monkeypatch, orphans=[])

    class FakeIndicatorStore:
        def __init__(self):
            pass

        def get_existing_report_dates(self):
            return set()

    class FailingFinancialCollector:
        def collect_indicators(self, code, market_symbol):
            raise RuntimeError("日期协调失败")

    monkeypatch.setattr(main_mod, "IndicatorStore", FakeIndicatorStore)
    monkeypatch.setattr(main_mod, "FinancialCollector", FailingFinancialCollector)

    assert main_mod.sync_financial_indicators(symbol="000508") == (1, 1)
    assert (
        get_last_sync_date(DATASET_FINANCIAL_DATE_RECONCILIATION_PENDING, "000508")
        == date.today()
    )

    indicator_calls = []

    class SucceedingFinancialCollector:
        def collect_indicators(self, code, market_symbol):
            indicator_calls.append((code, market_symbol))
            return {}

    monkeypatch.setattr(main_mod, "FinancialCollector", SucceedingFinancialCollector)

    assert main_mod.sync_financial_indicators() == (1, 0)
    assert indicator_calls == [("000508", "000508.SZ")]
    assert (
        get_last_sync_date(DATASET_FINANCIAL_DATE_RECONCILIATION_PENDING, "000508")
        is None
    )
