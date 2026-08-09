"""单元测试: FinancialCollector.fetch_statement 清洗逻辑 (数据源 akshare 新浪)"""

import pandas as pd
import pytest

from data_ingestion.collectors.financial_collector import (
    _SINA_NO_DATA_OVERRIDES,
    FinancialCollector,
)


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


class TestNoDataOverride:
    """白名单防御: 仅对确证无数据的 (code, stat_type) 短路, 其余完全不受影响"""

    def test_whitelist_hit_short_circuits_without_akshare(self, monkeypatch):
        """白名单命中 → 返回空 DataFrame, akshare 不被调用"""
        called = []

        def fake_akshare(**kw):
            called.append(kw)
            return fake_sina_report_df()

        monkeypatch.setattr("akshare.stock_financial_report_sina", fake_akshare)
        df = FinancialCollector().fetch_statement("000508", "cashflow")
        assert df.empty
        assert called == [], f"白名单命中不应调用 akshare, 实际调用: {called}"

    def test_same_code_other_stat_not_short_circuited(self, monkeypatch):
        """同代码非白名单报表 (balance) 不受影响, 正常抓取"""
        monkeypatch.setattr(
            "akshare.stock_financial_report_sina", lambda **kw: fake_sina_report_df()
        )
        df = FinancialCollector().fetch_statement("000508", "balance")
        assert not df.empty
        assert "report_date" in df.columns

    def test_other_code_same_stat_not_short_circuited(self, monkeypatch):
        """其他代码的 cashflow 不受白名单影响, 异常照常抛出"""
        import utils.retry as retry_mod

        monkeypatch.setattr(retry_mod.time, "sleep", lambda _: None)
        monkeypatch.setattr(
            "akshare.stock_financial_report_sina",
            lambda **kw: (_ for _ in ()).throw(TypeError("模拟其他问题")),
        )
        with pytest.raises(TypeError, match="模拟其他问题"):
            FinancialCollector().fetch_statement("600519", "cashflow")

    def test_whitelist_contains_expected_entry(self):
        """白名单确证只含 000508 的 cashflow (1998 年前退市唯一实例)"""
        assert _SINA_NO_DATA_OVERRIDES == {("000508", "cashflow")}


class TestGetDisclosurePlans:
    """披露计划获取: 双市场聚合 + 调用前 INFO 日志说明 (为 akshare 内部分页进度条提供上下文)"""

    def _fake_yysj(self, calls):
        def fake(symbol: str, date: str):
            calls.append((symbol, date))
            return pd.DataFrame(
                {
                    "股票代码": ["000001", "600519"]
                    if symbol == "沪深A股"
                    else ["830001"],
                    "实际披露时间": ["2026-08-10", "2026-08-11"]
                    if symbol == "沪深A股"
                    else ["2026-08-12"],
                }
            )

        return fake

    def test_aggregates_both_markets(self, monkeypatch):
        calls = []
        monkeypatch.setattr("akshare.stock_yysj_em", self._fake_yysj(calls))
        df = FinancialCollector().get_disclosure_plans("20260630")

        assert set(calls) == {("沪深A股", "20260630"), ("京市A股", "20260630")}
        assert set(df["code"]) == {"000001", "600519", "830001"}
        assert "actual_date" in df.columns
        assert "股票代码" not in df.columns

    def test_logs_info_before_fetch(self, monkeypatch, caplog):
        caplog.set_level("INFO", logger="QuantPyLab")
        calls = []
        monkeypatch.setattr("akshare.stock_yysj_em", self._fake_yysj(calls))
        FinancialCollector().get_disclosure_plans("20260630")

        infos = [r.message for r in caplog.records if r.levelname == "INFO"]
        assert any("20260630" in m and "沪深A股" in m for m in infos)
        assert any("20260630" in m and "京市A股" in m for m in infos)

    def test_empty_plan_returns_empty_df(self, monkeypatch):
        monkeypatch.setattr("akshare.stock_yysj_em", lambda **kw: pd.DataFrame())
        assert FinancialCollector().get_disclosure_plans("20260630").empty
