import pandas as pd

from analysis.factors.base import FactorDefinition, FactorInput, FactorMetadata


def _copy_numeric_factor(data: pd.DataFrame, column: str) -> pd.DataFrame:
    result = data.loc[:, ["date", "symbol"]].copy()
    result["value"] = pd.to_numeric(data[column], errors="coerce")
    return result


def _copy_positive_factor(data: pd.DataFrame, column: str) -> pd.DataFrame:
    result = _copy_numeric_factor(data, column)
    result["value"] = result["value"].where(result["value"] > 0)
    return result


class ValuationPeTTM(FactorDefinition):
    metadata = FactorMetadata(
        name="valuation_pe_ttm",
        version="1",
        description="点时市盈率 TTM，非正值视为缺失。",
        inputs=(FactorInput("pe_ttm", "valuation"),),
        lookback_days=0,
        higher_is_better=False,
    )

    def compute(self, data, parameters=None) -> pd.DataFrame:
        return _copy_positive_factor(data, "pe_ttm")


class ValuationPb(FactorDefinition):
    metadata = FactorMetadata(
        name="valuation_pb",
        version="1",
        description="点时市净率，非正值视为缺失。",
        inputs=(FactorInput("pb", "valuation"),),
        lookback_days=0,
        higher_is_better=False,
    )

    def compute(self, data, parameters=None) -> pd.DataFrame:
        return _copy_positive_factor(data, "pb")


class QualityRoeWeighted(FactorDefinition):
    metadata = FactorMetadata(
        name="quality_roe_weighted",
        version="1",
        description="按公告日对齐的加权净资产收益率。",
        inputs=(
            FactorInput(
                "roe_weighted",
                "indicator",
                source_name="净资产收益率_加权",
            ),
        ),
        lookback_days=0,
        higher_is_better=True,
    )

    def compute(self, data, parameters=None) -> pd.DataFrame:
        return _copy_numeric_factor(data, "roe_weighted")


class QualityOperatingCashflowRatio(FactorDefinition):
    metadata = FactorMetadata(
        name="quality_operating_cashflow_ratio",
        version="1",
        description="按公告日对齐的经营现金流与营业收入比率。",
        inputs=(
            FactorInput(
                "operating_cashflow_to_revenue",
                "indicator",
                source_name="经营现金流/营业收入",
            ),
        ),
        lookback_days=0,
        higher_is_better=True,
    )

    def compute(self, data, parameters=None) -> pd.DataFrame:
        return _copy_numeric_factor(data, "operating_cashflow_to_revenue")
