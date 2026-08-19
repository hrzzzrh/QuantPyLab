from analysis.factors.base import FactorDefinition, FactorMetadata
from analysis.factors.fundamental import (
    QualityOperatingCashflowRatio,
    QualityRoeWeighted,
    ValuationPb,
    ValuationPeTTM,
)
from analysis.factors.market import (
    PriceMomentum120D,
    PriceTrendAboveMA120D,
    PriceTrendGap120D,
    PriceVolatility60D,
)

FACTOR_REGISTRY: dict[str, FactorDefinition] = {
    "price_momentum_120d": PriceMomentum120D(),
    "price_trend_above_ma_120d": PriceTrendAboveMA120D(),
    "price_trend_gap_120d": PriceTrendGap120D(),
    "price_volatility_60d": PriceVolatility60D(),
    "valuation_pe_ttm": ValuationPeTTM(),
    "valuation_pb": ValuationPb(),
    "quality_roe_weighted": QualityRoeWeighted(),
    "quality_operating_cashflow_ratio": QualityOperatingCashflowRatio(),
}


def get_factor_definition(factor_name: str) -> FactorDefinition:
    try:
        return FACTOR_REGISTRY[factor_name]
    except KeyError as error:
        available = ", ".join(sorted(FACTOR_REGISTRY))
        raise ValueError(f"未注册的因子: {factor_name} (可选: {available})") from error


def list_factor_definitions() -> list[FactorMetadata]:
    return [FACTOR_REGISTRY[name].metadata for name in sorted(FACTOR_REGISTRY)]
