"""测试财务数据的安全点时日期选择。"""

import duckdb
import pandas as pd

from analysis.factors.engine import FactorEngine
from backtest.data_access import BacktestDataAccess, IndicatorField
from storage.database.views.analysis.v_daily_valuation import DailyValuationView
from storage.database.views.market.daily_kline import DailyKlineView
from storage.database.views.market.daily_kline_raw import DailyKlineRawView


def test_indicator_point_in_time_join_prefers_data_available_date():
    sql = BacktestDataAccess._build_indicator_history_cte(
        (IndicatorField("净资产收益率", "roe"),)
    )

    assert '"数据可用日期"' in sql
    assert '"公告日期"' in sql
    assert "deduplicated_indicators" in sql
    assert "arg_max" in sql
    assert "selection_key" in sql
    assert "LEAD(pub_date) OVER" in sql
    assert "indicator_intervals" in sql


def test_daily_valuation_deduplicates_same_effective_date():
    sql = DailyValuationView().get_sql("/tmp/warehouse")

    assert "arg_max" in sql
    assert "selection_key" in sql
    assert "数据可用日期" in sql
    assert "CAST(pub_date AS VARCHAR)" in sql
    assert "GROUP BY symbol, pub_date" in sql
    assert "GROUP BY symbol, change_date" in sql
    assert "LEAD(change_date) OVER" in sql
    assert "LEAD(pub_date) OVER" in sql


def test_daily_kline_deduplicates_same_symbol_date():
    sql = DailyKlineView().get_sql("/tmp/warehouse")

    assert "deduplicated_kline" in sql
    assert "PARTITION BY partition_symbol, CAST(date AS DATE)" in sql
    assert "ROW_NUMBER() OVER" in sql


def test_daily_kline_raw_view_skips_full_window_deduplication():
    sql = DailyKlineRawView().get_sql("/tmp/warehouse")

    assert "read_parquet" in sql
    assert "regexp_extract" in sql
    assert "ROW_NUMBER() OVER" not in sql


def test_daily_valuation_view_executes_with_net_assets_alias():
    """估值视图的净资产别名必须能在 DuckDB 中实际绑定执行。"""
    conn = duckdb.connect()
    try:
        conn.execute(
            "CREATE TABLE daily_kline ("
            "symbol VARCHAR, date DATE, close DOUBLE, adj_factor DOUBLE"
            ")"
        )
        conn.execute(
            "CREATE TABLE share_capital ("
            "symbol VARCHAR, change_date DATE, total_shares DOUBLE"
            ")"
        )
        conn.execute(
            "CREATE TABLE fin_ttm ("
            "symbol VARCHAR, pub_date VARCHAR, report_date VARCHAR, "
            "net_profit_ttm DOUBLE, deduct_net_profit_ttm DOUBLE, "
            "revenue_ttm DOUBLE, ocf_ttm DOUBLE, filename VARCHAR"
            ")"
        )
        conn.execute(
            "CREATE TABLE fin_balance_sheet ("
            'symbol VARCHAR, "数据可用日期" VARCHAR, "公告日期" VARCHAR, '
            'report_date VARCHAR, "归属于母公司股东权益合计" DOUBLE, filename VARCHAR'
            ")"
        )
        conn.execute("INSERT INTO daily_kline VALUES ('000001', '2024-11-16', 100, 1)")
        conn.execute("INSERT INTO share_capital VALUES ('000001', '2024-01-01', 10)")
        conn.execute(
            "INSERT INTO fin_ttm VALUES "
            "('000001', '20241115', '20240930', 100, 100, 1000, 100, 'test.parquet')"
        )
        conn.execute(
            "INSERT INTO fin_balance_sheet VALUES "
            "('000001', '20241115', '20241030', '20240930', 1000, 'test.parquet')"
        )

        conn.execute(DailyValuationView().get_sql("/tmp/warehouse"))
        assert conn.execute("SELECT pb FROM v_daily_valuation").fetchone() == (1.0,)
    finally:
        conn.close()


def test_ttm_pub_date_flows_to_valuation_factor_asof():
    """TTM 公告日应直接决定估值因子最早可见日期。"""
    conn = duckdb.connect()
    try:
        conn.execute(
            "CREATE TABLE daily_kline ("
            "symbol VARCHAR, date DATE, close DOUBLE, adj_factor DOUBLE"
            ")"
        )
        conn.execute(
            "CREATE TABLE share_capital ("
            "symbol VARCHAR, change_date DATE, total_shares DOUBLE"
            ")"
        )
        conn.execute(
            "CREATE TABLE fin_ttm ("
            "symbol VARCHAR, pub_date VARCHAR, report_date VARCHAR, "
            "net_profit_ttm DOUBLE, deduct_net_profit_ttm DOUBLE, "
            "revenue_ttm DOUBLE, ocf_ttm DOUBLE, filename VARCHAR"
            ")"
        )
        conn.execute(
            "CREATE TABLE fin_balance_sheet ("
            'symbol VARCHAR, "数据可用日期" VARCHAR, "公告日期" VARCHAR, '
            'report_date VARCHAR, "归属于母公司股东权益合计" DOUBLE, filename VARCHAR'
            ")"
        )
        conn.executemany(
            "INSERT INTO daily_kline VALUES (?, ?, ?, ?)",
            [
                ("000001", "2024-10-29", 100, 1),
                ("000001", "2024-10-30", 100, 1),
                ("000001", "2024-11-15", 100, 1),
            ],
        )
        conn.execute("INSERT INTO share_capital VALUES ('000001', '2024-01-01', 10)")
        conn.execute(
            "INSERT INTO fin_ttm VALUES "
            "('000001', '20241030', '20240930', 100, 100, 1000, 100, 'test.parquet')"
        )
        conn.execute(
            "INSERT INTO fin_balance_sheet VALUES "
            "('000001', '20241001', '20241001', '20240930', 1000, 'test.parquet')"
        )

        conn.execute(DailyValuationView().get_sql("/tmp/warehouse"))
        valuation = conn.execute(
            """
            SELECT date, symbol, pe_ttm
            FROM v_daily_valuation
            WHERE symbol = '000001'
            ORDER BY date
            """
        ).df()
        valuation["date"] = pd.to_datetime(valuation["date"])

        factor = FactorEngine().calculate(valuation, ("valuation_pe_ttm",))

        assert valuation["date"].tolist() == [
            pd.Timestamp("2024-10-30"),
            pd.Timestamp("2024-11-15"),
        ]
        assert factor["date"].min() == pd.Timestamp("2024-10-30")
        assert factor["valuation_pe_ttm"].tolist() == [10.0, 10.0]
    finally:
        conn.close()


def test_daily_valuation_canonicalizes_and_deduplicates_ttm_pub_dates():
    """同一自然日的不同字符串格式必须命中确定的最新报告记录。"""
    conn = duckdb.connect()
    try:
        conn.execute(
            "CREATE TABLE daily_kline ("
            "symbol VARCHAR, date DATE, close DOUBLE, adj_factor DOUBLE"
            ")"
        )
        conn.execute(
            "CREATE TABLE share_capital ("
            "symbol VARCHAR, change_date DATE, total_shares DOUBLE"
            ")"
        )
        conn.execute(
            "CREATE TABLE fin_ttm ("
            "symbol VARCHAR, pub_date VARCHAR, report_date VARCHAR, "
            "net_profit_ttm DOUBLE, deduct_net_profit_ttm DOUBLE, "
            "revenue_ttm DOUBLE, ocf_ttm DOUBLE, filename VARCHAR"
            ")"
        )
        conn.execute(
            "CREATE TABLE fin_balance_sheet ("
            'symbol VARCHAR, "数据可用日期" VARCHAR, "公告日期" VARCHAR, '
            'report_date VARCHAR, "归属于母公司股东权益合计" DOUBLE, filename VARCHAR'
            ")"
        )
        conn.execute("INSERT INTO daily_kline VALUES ('000001', '2024-11-16', 100, 1)")
        conn.execute("INSERT INTO share_capital VALUES ('000001', '2024-01-01', 10)")
        conn.execute(
            "INSERT INTO fin_ttm VALUES "
            "('000001', '20241115', '20240630', 10, 10, 1000, 10, 'old.parquet'), "
            "('000001', '2024-11-15', '20240930', 20, 20, 2000, 20, 'new.parquet')"
        )
        conn.execute(
            "INSERT INTO fin_balance_sheet VALUES "
            "('000001', '20241115', '20241115', '20240630', 1000, 'old.parquet'), "
            "('000001', '2024-11-15', '2024-11-15', '20240930', 2000, 'new.parquet')"
        )

        conn.execute(DailyValuationView().get_sql("/tmp/warehouse"))
        valuation = conn.execute("SELECT pe_ttm, pb FROM v_daily_valuation").fetchone()

        assert valuation == (50.0, 0.5)
    finally:
        conn.close()
