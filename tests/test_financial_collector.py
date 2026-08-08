"""单元测试: FinancialCollector.fetch_statement 清洗逻辑 (数据源 akshare 新浪)"""

import pandas as pd
import pytest

from data_ingestion.collectors.financial_collector import FinancialCollector


def fake_sina_report_df():
    return pd.DataFrame(
        {
            "报告日": ["20251231", "20250930"],
            "货币资金": [100.5, 90.0],
            "其中:现金": [50.0, 40.0],
            "数据源": ["新浪", "新浪"],
        }
    )


class TestFetchStatement:
    def test_parse_and_clean(self, monkeypatch):
        monkeypatch.setattr(
            "akshare.stock_financial_report_sina", lambda **kw: fake_sina_report_df()
        )
        df = FinancialCollector().fetch_statement("300507", "balance")
        assert len(df) == 2
        assert "report_date" in df.columns
        assert "报告日" not in df.columns
        assert "现金" in df.columns
        assert "其中:现金" not in df.columns
        assert list(df["report_date"]) == ["20250930", "20251231"]
        assert df["symbol"].tolist() == ["300507", "300507"]

    def test_passes_correct_args_to_akshare(self, monkeypatch):
        captured = {}

        def fake_func(**kw):
            captured.update(kw)
            return fake_sina_report_df()

        monkeypatch.setattr("akshare.stock_financial_report_sina", fake_func)
        FinancialCollector().fetch_statement("300507", "profit")
        assert captured["stock"] == "300507"
        assert captured["symbol"] == "利润表"

    def test_invalid_stat_type(self):
        with pytest.raises(ValueError, match="无效的报表类型"):
            FinancialCollector().fetch_statement("300507", "xxx")

    def test_empty_response_returns_empty(self, monkeypatch):
        monkeypatch.setattr(
            "akshare.stock_financial_report_sina", lambda **kw: pd.DataFrame()
        )
        df = FinancialCollector().fetch_statement("300507", "balance")
        assert df.empty

    def test_sina_blocked_raises_via_retry_fatal(self, monkeypatch):
        from utils.requests_protection import SinaBlockedError

        monkeypatch.setattr(
            "akshare.stock_financial_report_sina",
            lambda **kw: (_ for _ in ()).throw(SinaBlockedError("封禁")),
        )
        with pytest.raises(SinaBlockedError, match="封禁"):
            FinancialCollector().fetch_statement("300507", "balance")
