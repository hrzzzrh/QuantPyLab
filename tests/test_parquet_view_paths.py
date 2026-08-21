from storage.database.views.financial.fin_balance_sheet import BalanceSheetView
from storage.database.views.financial.fin_cashflow_statement import (
    CashFlowStatementView,
)
from storage.database.views.financial.fin_income_statement import IncomeStatementView
from storage.database.views.financial.fin_indicator import FinIndicatorView
from storage.database.views.financial.fin_ttm import FinTTMView
from storage.database.views.market.daily_kline import DailyKlineView
from storage.database.views.market.daily_kline_calendar import DailyKlineCalendarView
from storage.database.views.market.daily_kline_raw import DailyKlineRawView
from storage.database.views.market.etf_kline import ETFKlineView
from storage.database.views.market.industry_classification_sw import (
    IndustryClassificationShenwanView,
)
from storage.database.views.market.share_capital import ShareCapitalView


def test_parquet_views_read_only_atomic_data_files():
    view_classes = (
        BalanceSheetView,
        CashFlowStatementView,
        IncomeStatementView,
        FinIndicatorView,
        FinTTMView,
        DailyKlineView,
        DailyKlineCalendarView,
        DailyKlineRawView,
        ETFKlineView,
        IndustryClassificationShenwanView,
        ShareCapitalView,
    )

    for view_class in view_classes:
        sql = view_class().get_sql("/warehouse")
        assert "*/data.parquet" in sql
        assert "*/*.parquet" not in sql
