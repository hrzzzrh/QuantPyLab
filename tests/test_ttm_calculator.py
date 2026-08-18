"""单元测试: analysis/processors/ttm_calculator.py TTM 计算引擎

使用 tmp 数据仓库隔离，验证 TTM 公式正确性与"无穿越"特性。
TTM = 本期累计 + (上年年报 - 上年同期累计)
"""

from pathlib import Path

import pandas as pd
import pytest

import analysis.processors.ttm_calculator as ttm_mod
import storage.file_store.parquet_store as parquet_store_mod
from analysis.processors.ttm_calculator import TTMCalculator

INCOME_CATEGORY = "financial_statements/type=income"
CASHFLOW_CATEGORY = "financial_statements/type=cashflow"

# 11 个完整报告期 (2022Q1 ~ 2024Q3), 覆盖跨年与同期基期
FULL_REPORTS = [
    ("20220331", "2022-04-29", 60, 1000),
    ("20220630", "2022-08-30", 140, 2500),
    ("20220930", "2022-10-28", 280, 4200),
    ("20221231", "2023-04-28", 250, 5000),
    ("20230331", "2023-04-28", 70, 1200),
    ("20230630", "2023-08-30", 160, 2800),
    ("20230930", "2023-10-27", 300, 4500),
    ("20231231", "2024-04-30", 400, 6000),
    ("20240331", "2024-04-30", 80, 1300),
    ("20240630", "2024-08-30", 180, 3000),
    ("20240930", "2024-10-30", 300, 4700),
]

# 手算期望 (2022 年四期因上年同期缺失被剔除)
EXPECTED_TTM = {
    "20230331": {"net_profit_ttm": 260, "revenue_ttm": 5200},
    "20230630": {"net_profit_ttm": 270, "revenue_ttm": 5300},
    "20230930": {"net_profit_ttm": 270, "revenue_ttm": 5300},
    "20231231": {"net_profit_ttm": 400, "revenue_ttm": 6000},
    "20240331": {"net_profit_ttm": 410, "revenue_ttm": 6100},
    "20240630": {"net_profit_ttm": 420, "revenue_ttm": 6200},
    "20240930": {"net_profit_ttm": 400, "revenue_ttm": 6200},
}


@pytest.fixture
def isolated_warehouse(tmp_path, monkeypatch):
    """将全部数据仓路径指向临时目录"""
    monkeypatch.setattr(ttm_mod, "WAREHOUSE_DIR", tmp_path)
    monkeypatch.setattr(parquet_store_mod, "WAREHOUSE_DIR", tmp_path)
    return tmp_path


def _write_income(
    warehouse: Path, symbol: str, rows, columns=("net_profit", "revenue")
):
    df = pd.DataFrame(
        [
            {
                "report_date": rd,
                "公告日期": pub,
                "归属于母公司所有者的净利润": np,
                "营业总收入": rev,
            }
            for rd, pub, np, rev in rows
        ]
    )
    path = warehouse / INCOME_CATEGORY / f"symbol={symbol}" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return df


def _write_cashflow(warehouse: Path, symbol: str, rows):
    df = pd.DataFrame(
        [
            {
                "report_date": rd,
                "公告日期": pub,
                "经营活动产生的现金流量净额": ocf,
            }
            for rd, pub, ocf in rows
        ]
    )
    path = warehouse / CASHFLOW_CATEGORY / f"symbol={symbol}" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _read_result(warehouse: Path, symbol: str) -> pd.DataFrame:
    path = warehouse / "financial/ttm" / f"symbol={symbol}" / "data.parquet"
    assert path.exists(), f"TTM 结果文件未生成: {path}"
    return pd.read_parquet(path)


class TestNormalizePubDate:
    def test_datetime_string(self):
        calc = TTMCalculator.__new__(TTMCalculator)
        series = pd.Series(["2025-08-28 00:00:00"])
        assert calc._normalize_pub_date(series).iloc[0] == "20250828"

    def test_date_only(self):
        calc = TTMCalculator.__new__(TTMCalculator)
        series = pd.Series(["2025-08-28"])
        assert calc._normalize_pub_date(series).iloc[0] == "20250828"

    def test_already_normalized(self):
        calc = TTMCalculator.__new__(TTMCalculator)
        series = pd.Series(["20250828"])
        assert calc._normalize_pub_date(series).iloc[0] == "20250828"


class TestLoadData:
    def test_missing_file_returns_empty(self, isolated_warehouse):
        calc = TTMCalculator()
        df = calc._load_data(INCOME_CATEGORY, "999999")
        assert df.empty

    def test_load_existing_file(self, isolated_warehouse):
        _write_income(isolated_warehouse, "000001", FULL_REPORTS)
        calc = TTMCalculator()
        df = calc._load_data(INCOME_CATEGORY, "000001")
        assert len(df) == len(FULL_REPORTS)


class TestCalculateForSymbol:
    def test_full_data_ttm_math(self, isolated_warehouse):
        """完整 8 期数据下 TTM 公式与报告期集合正确"""
        _write_income(isolated_warehouse, "000001", FULL_REPORTS)
        calc = TTMCalculator()
        calc.calculate_for_symbol("000001")

        result = _read_result(isolated_warehouse, "000001")
        result = result.set_index("report_date")

        assert set(result.index) == set(EXPECTED_TTM.keys())
        for report_date, expected in EXPECTED_TTM.items():
            assert result.loc[report_date, "net_profit_ttm"] == pytest.approx(
                expected["net_profit_ttm"]
            )
            assert result.loc[report_date, "revenue_ttm"] == pytest.approx(
                expected["revenue_ttm"]
            )

    def test_first_period_excluded_no_forward_look(self, isolated_warehouse):
        """无上年同期数据的期间不得出现穿越性的 TTM 值"""
        _write_income(isolated_warehouse, "000001", FULL_REPORTS)
        calc = TTMCalculator()
        calc.calculate_for_symbol("000001")
        result = _read_result(isolated_warehouse, "000001")
        assert set(result["report_date"]) == set(EXPECTED_TTM.keys())

    def test_missing_period_breaks_series(self, isolated_warehouse):
        """缺失 20230630 时, 依赖其作为上年同期的 20240630 应被剔除 (无穿越)"""
        rows = [r for r in FULL_REPORTS if r[0] != "20230630"]
        _write_income(isolated_warehouse, "000001", rows)
        calc = TTMCalculator()
        calc.calculate_for_symbol("000001")
        result = _read_result(isolated_warehouse, "000001")
        report_dates = set(result["report_date"])
        assert "20240630" not in report_dates
        assert "20240930" in report_dates
        assert "20240331" in report_dates

    def test_mismatched_periods_between_indicators(self, isolated_warehouse):
        """income 与 cashflow 期间不一致时: 该期净利 TTM 正常, 缺失指标为 NaN 但行保留"""
        _write_income(isolated_warehouse, "000001", FULL_REPORTS)
        cashflow_rows = [
            (rd, pub, ocf)
            for rd, pub, ocf in [
                ("20221231", "2023-04-28", 600),
                ("20230331", "2023-04-28", 100),
                ("20230630", "2023-08-30", 350),
                ("20230930", "2023-10-27", 800),
                ("20231231", "2024-04-30", 1000),
                ("20240331", "2024-04-30", 150),
                ("20240930", "2024-10-30", 750),
            ]
        ]
        _write_cashflow(isolated_warehouse, "000001", cashflow_rows)
        calc = TTMCalculator()
        calc.calculate_for_symbol("000001")

        result = _read_result(isolated_warehouse, "000001").set_index("report_date")
        assert "20240630" in result.index
        assert result.loc["20240630", "net_profit_ttm"] == pytest.approx(420)
        assert pd.isna(result.loc["20240630", "ocf_ttm"])

    def test_duplicate_report_date_keeps_latest_pub(self, isolated_warehouse):
        """同一 report_date 多条公告时取公告日期最新的数据"""
        rows = [
            (rd, pub, np_, rev)
            for rd, pub, np_, rev in FULL_REPORTS
            if rd != "20240930"
        ] + [
            ("20240930", "2024-10-30", 300, 4700),  # 首报
            ("20240930", "2024-11-15", 999, 9999),  # 修订公告
        ]
        _write_income(isolated_warehouse, "000001", rows)
        calc = TTMCalculator()
        calc.calculate_for_symbol("000001")
        result = _read_result(isolated_warehouse, "000001").set_index("report_date")
        # 20240930 应使用修订值 999: TTM = 999 + (400 - 300)
        assert result.loc["20240930", "net_profit_ttm"] == pytest.approx(1099)

    def test_duplicate_report_date_with_same_safe_date_is_deterministic(
        self, isolated_warehouse
    ):
        """统一安全日期后，重复报告期的选择不依赖 Parquet 行顺序。"""
        base_rows = [
            {
                "report_date": rd,
                "公告日期": pub,
                "归属于母公司所有者的净利润": np_,
                "营业总收入": rev,
            }
            for rd, pub, np_, rev in FULL_REPORTS
            if rd != "20240930"
        ]
        duplicate_rows = [
            {
                "report_date": "20240930",
                "公告日期": "20241030",
                "数据可用日期": "20241115",
                "归属于母公司所有者的净利润": 300,
                "营业总收入": 4700,
            },
            {
                "report_date": "20240930",
                "公告日期": "20241030",
                "数据可用日期": "20241115",
                "归属于母公司所有者的净利润": 999,
                "营业总收入": 9999,
            },
        ]
        path = isolated_warehouse / INCOME_CATEGORY / "symbol=000001" / "data.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)

        def calculate_with_rows(rows):
            pd.DataFrame(base_rows + rows).to_parquet(path, index=False)
            calc = TTMCalculator()
            calc.calculate_for_symbol("000001")
            result = _read_result(isolated_warehouse, "000001")
            return tuple(
                result.loc[
                    result["report_date"] == "20240930",
                    ["net_profit_ttm", "revenue_ttm"],
                ].iloc[0]
            )

        first = calculate_with_rows(duplicate_rows)
        second = calculate_with_rows(list(reversed(duplicate_rows)))

        assert second == first
        assert first in {(400, 6200), (1099, 11499)}

    def test_uses_data_available_date_for_asof_publish_date(self, isolated_warehouse):
        """四源最小公告日期不能早于全部组件可用日。"""
        rows = [
            {
                "report_date": rd,
                "公告日期": pub,
                "数据可用日期": available,
                "归属于母公司所有者的净利润": np_,
                "营业总收入": rev,
            }
            for (rd, pub, np_, rev), available in zip(
                FULL_REPORTS,
                [
                    "2022-05-02",
                    "2022-09-02",
                    "2022-11-01",
                    "2023-05-02",
                    "2023-05-02",
                    "2023-09-02",
                    "2023-11-01",
                    "2024-05-02",
                    "2024-05-02",
                    "2024-09-02",
                    "2024-11-01",
                ],
            )
        ]
        path = isolated_warehouse / INCOME_CATEGORY / "symbol=000001" / "data.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(path, index=False)

        calc = TTMCalculator()
        calc.calculate_for_symbol("000001")

        result = _read_result(isolated_warehouse, "000001").set_index("report_date")
        assert result.loc["20240930", "pub_date"] == "20241101"

    def test_historical_input_date_is_included_in_asof_publish_date(
        self, isolated_warehouse
    ):
        """历史年末修订晚于当前期时，TTM 生效日必须覆盖该修订日期。"""
        rows = [
            (rd, "2025-01-01" if rd == "20231231" else pub, np_, rev)
            for rd, pub, np_, rev in FULL_REPORTS
        ]
        _write_income(isolated_warehouse, "000001", rows)

        calc = TTMCalculator()
        calc.calculate_for_symbol("000001")

        result = _read_result(isolated_warehouse, "000001").set_index("report_date")
        assert result.loc["20240331", "pub_date"] == "20250101"

    def test_insufficient_data_no_crash(self, isolated_warehouse):
        """无任何财务数据时静默跳过, 不报错"""
        calc = TTMCalculator()
        calc.calculate_for_symbol("000001")
        assert not (isolated_warehouse / "financial/ttm").exists()


class TestGetExistingReportDates:
    def test_empty_warehouse(self, isolated_warehouse):
        calc = TTMCalculator()
        assert calc.get_existing_report_dates() == set()
