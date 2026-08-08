"""单元测试: utils/financial.py 财务报告期与市场标签工具"""

import pytest

from utils.financial import (
    MarketLabel,
    get_consecutive_reports,
    get_market_label,
    get_previous_report_date,
    to_sina_symbol,
)


class TestGetPreviousReportDate:
    """get_previous_report_date: 标准报告期回推"""

    @pytest.mark.parametrize(
        ("report_date", "expected"),
        [
            ("20250930", "20250630"),
            ("20250630", "20250331"),
            ("20250331", "20241231"),
            ("20241231", "20240930"),
            ("20240331", "20231231"),
        ],
    )
    def test_standard_periods(self, report_date, expected):
        assert get_previous_report_date(report_date) == expected

    def test_year_boundary(self):
        assert get_previous_report_date("20250101") == "20250101"

    def test_invalid_format_kept_as_is(self):
        assert get_previous_report_date("20250715") == "20250715"


class TestGetConsecutiveReports:
    """get_consecutive_reports: 连续 N 期报告期列表"""

    def test_default_five_periods(self):
        result = get_consecutive_reports("20250930")
        assert result == [
            "20250930",
            "20250630",
            "20250331",
            "20241231",
            "20240930",
        ]

    def test_single_period(self):
        assert get_consecutive_reports("20250930", n=1) == ["20250930"]

    def test_cross_year(self):
        result = get_consecutive_reports("20240331", 3)
        assert result == ["20240331", "20231231", "20230930"]

    def test_length_matches_n(self):
        assert len(get_consecutive_reports("20250630", 8)) == 8


class TestGetMarketLabel:
    """get_market_label: 交易所标识识别"""

    @pytest.mark.parametrize(
        ("code", "expected"),
        [
            ("600519", MarketLabel.SH),  # 沪市主板
            ("688981", MarketLabel.SH),  # 科创板
            ("900901", MarketLabel.SH),  # 沪 B 股
            ("510300", MarketLabel.SH),  # 沪市 ETF
            ("000001", MarketLabel.SZ),  # 深市主板
            ("300750", MarketLabel.SZ),  # 创业板
            ("200002", MarketLabel.SZ),  # 深 B 股
            ("159915", MarketLabel.SZ),  # 深市 ETF
            ("430047", MarketLabel.BJ),  # 北交所
            ("830799", MarketLabel.BJ),  # 北交所
            ("920002", MarketLabel.BJ),  # 北交所新号段
        ],
    )
    def test_market_labels(self, code, expected):
        assert get_market_label(code) is expected


class TestToSinaSymbol:
    """to_sina_symbol: 新浪前缀转换"""

    @pytest.mark.parametrize(
        ("code", "expected"),
        [
            ("600519", "sh600519"),
            ("000001", "sz000001"),
            ("430047", "bj430047"),
            ("300750", "sz300750"),
            ("688981", "sh688981"),
        ],
    )
    def test_conversion(self, code, expected):
        assert to_sina_symbol(code) == expected
