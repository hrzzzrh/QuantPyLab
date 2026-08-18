"""测试财务数据的安全 ASOF 日期选择。"""

import duckdb

from backtest.data_access import BacktestDataAccess, IndicatorField
from storage.database.views.analysis.v_daily_valuation import DailyValuationView


def test_indicator_asof_join_prefers_data_available_date():
    sql = BacktestDataAccess._build_indicator_join(
        (IndicatorField("净资产收益率", "roe"),)
    )

    assert '"数据可用日期"' in sql
    assert '"公告日期"' in sql
    assert "deduplicated_indicators" in sql
    assert "record_tie_breaker DESC" in sql


def test_daily_valuation_deduplicates_same_effective_date():
    sql = DailyValuationView().get_sql("/tmp/warehouse")

    assert "record_tie_breaker DESC" in sql
    assert "数据可用日期" in sql


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
            "revenue_ttm DOUBLE, ocf_ttm DOUBLE"
            ")"
        )
        conn.execute(
            "CREATE TABLE fin_balance_sheet ("
            'symbol VARCHAR, "数据可用日期" VARCHAR, "公告日期" VARCHAR, '
            'report_date VARCHAR, "归属于母公司股东权益合计" DOUBLE'
            ")"
        )
        conn.execute("INSERT INTO daily_kline VALUES ('000001', '2024-11-16', 100, 1)")
        conn.execute("INSERT INTO share_capital VALUES ('000001', '2024-01-01', 10)")
        conn.execute(
            "INSERT INTO fin_ttm VALUES "
            "('000001', '20241115', '20240930', 100, 100, 1000, 100)"
        )
        conn.execute(
            "INSERT INTO fin_balance_sheet VALUES "
            "('000001', '20241115', '20241030', '20240930', 1000)"
        )

        conn.execute(DailyValuationView().get_sql("/tmp/warehouse"))
        assert conn.execute("SELECT pb FROM v_daily_valuation").fetchone() == (1.0,)
    finally:
        conn.close()
