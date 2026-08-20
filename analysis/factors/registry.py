from analysis.factors.base import FactorDefinition, FactorMetadata
from analysis.factors.fundamental import (
    GrowthDeductProfitYoy,
    GrowthRevenueYoy,
    QualityOperatingCashflowRatio,
    QualityRoeWeighted,
    QualityRoic,
    ValuationPb,
    ValuationPcfTTM,
    ValuationPeTTM,
    ValuationPsTTM,
)
from analysis.factors.market import (
    PriceMomentum120D,
    PriceReversal20D,
    PriceTrendAboveMA120D,
    PriceTrendGap120D,
    PriceVolatility60D,
)

FACTOR_REGISTRY: dict[str, FactorDefinition] = {
    "growth_deduct_profit_yoy": GrowthDeductProfitYoy(),
    "growth_revenue_yoy": GrowthRevenueYoy(),
    "price_momentum_120d": PriceMomentum120D(),
    "price_reversal_20d": PriceReversal20D(),
    "price_trend_above_ma_120d": PriceTrendAboveMA120D(),
    "price_trend_gap_120d": PriceTrendGap120D(),
    "price_volatility_60d": PriceVolatility60D(),
    "quality_roic": QualityRoic(),
    "quality_roe_weighted": QualityRoeWeighted(),
    "quality_operating_cashflow_ratio": QualityOperatingCashflowRatio(),
    "valuation_pcf_ttm": ValuationPcfTTM(),
    "valuation_pb": ValuationPb(),
    "valuation_pe_ttm": ValuationPeTTM(),
    "valuation_ps_ttm": ValuationPsTTM(),
}


def get_factor_definition(factor_name: str) -> FactorDefinition:
    try:
        return FACTOR_REGISTRY[factor_name]
    except KeyError as error:
        available = ", ".join(sorted(FACTOR_REGISTRY))
        raise ValueError(f"未注册的因子: {factor_name} (可选: {available})") from error


def list_factor_definitions() -> list[FactorMetadata]:
    return [FACTOR_REGISTRY[name].metadata for name in sorted(FACTOR_REGISTRY)]
