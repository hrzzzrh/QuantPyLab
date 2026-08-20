from collections.abc import Mapping

import pandas as pd

from analysis.factors.base import FactorDefinition, FactorInput, FactorMetadata


def _ordered_market_data(data: pd.DataFrame) -> pd.DataFrame:
    ordered = data.loc[:, ["date", "symbol", "close_hfq"]].copy()
    ordered["close_hfq"] = pd.to_numeric(ordered["close_hfq"], errors="coerce")
    return ordered.sort_values(["symbol", "date"]).reset_index(drop=True)


def _resolve_window_parameter(
    parameters: Mapping[str, object] | None,
    parameter_name: str,
    default: int,
    factor_name: str,
) -> int:
    provided = dict(parameters or {})
    unknown = set(provided) - {parameter_name}
    if unknown:
        raise ValueError(f"因子 {factor_name} 不支持参数: {', '.join(sorted(unknown))}")
    value = provided.get(parameter_name, default)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"因子 {factor_name} 的 {parameter_name} 必须是正整数")
    return value


class PriceMomentum120D(FactorDefinition):
    metadata = FactorMetadata(
        name="price_momentum_120d",
        version="1",
        description="后复权收盘价的 120 个交易日收益率。",
        inputs=(FactorInput("close_hfq", "valuation"),),
        lookback_days=120,
        higher_is_better=True,
    )

    def get_lookback_days(self, parameters=None) -> int:
        return _resolve_window_parameter(
            parameters, "lookback_days", 120, self.metadata.name
        )

    def compute(self, data, parameters=None) -> pd.DataFrame:
        ordered = _ordered_market_data(data)
        lookback_days = self.get_lookback_days(parameters)
        ordered["value"] = ordered.groupby("symbol", sort=False)["close_hfq"].transform(
            lambda values: values / values.shift(lookback_days) - 1
        )
        return ordered.loc[:, ["date", "symbol", "value"]]


class PriceTrendGap120D(FactorDefinition):
    metadata = FactorMetadata(
        name="price_trend_gap_120d",
        version="1",
        description="后复权收盘价相对 120 日均线的偏离幅度。",
        inputs=(FactorInput("close_hfq", "valuation"),),
        lookback_days=120,
        higher_is_better=True,
    )

    def get_lookback_days(self, parameters=None) -> int:
        return _resolve_window_parameter(
            parameters, "trend_window", 120, self.metadata.name
        )

    def compute(self, data, parameters=None) -> pd.DataFrame:
        ordered = _ordered_market_data(data)
        trend_window = self.get_lookback_days(parameters)
        moving_average = ordered.groupby("symbol", sort=False)["close_hfq"].transform(
            lambda values: values.rolling(trend_window, min_periods=trend_window).mean()
        )
        ordered["value"] = ordered["close_hfq"] / moving_average - 1
        return ordered.loc[:, ["date", "symbol", "value"]]


class PriceTrendAboveMA120D(FactorDefinition):
    metadata = FactorMetadata(
        name="price_trend_above_ma_120d",
        version="1",
        description="后复权收盘价是否高于 120 日均线的趋势确认信号。",
        inputs=(FactorInput("close_hfq", "valuation"),),
        lookback_days=120,
        higher_is_better=True,
    )

    def get_lookback_days(self, parameters=None) -> int:
        return _resolve_window_parameter(
            parameters, "trend_window", 120, self.metadata.name
        )

    def compute(self, data, parameters=None) -> pd.DataFrame:
        ordered = _ordered_market_data(data)
        trend_window = self.get_lookback_days(parameters)
        moving_average = ordered.groupby("symbol", sort=False)["close_hfq"].transform(
            lambda values: values.rolling(trend_window, min_periods=trend_window).mean()
        )
        ordered["value"] = (
            (ordered["close_hfq"] > moving_average)
            .where(moving_average.notna())
            .astype(float)
        )
        return ordered.loc[:, ["date", "symbol", "value"]]


class PriceVolatility60D(FactorDefinition):
    metadata = FactorMetadata(
        name="price_volatility_60d",
        version="1",
        description="后复权日收益率的 60 日滚动标准差。",
        inputs=(FactorInput("close_hfq", "valuation"),),
        lookback_days=60,
        higher_is_better=False,
    )

    def compute(self, data, parameters=None) -> pd.DataFrame:
        ordered = _ordered_market_data(data)
        returns = ordered.groupby("symbol", sort=False)["close_hfq"].pct_change(
            fill_method=None
        )
        ordered["value"] = returns.groupby(ordered["symbol"], sort=False).transform(
            lambda values: values.rolling(60, min_periods=60).std()
        )
        return ordered.loc[:, ["date", "symbol", "value"]]


class PriceReversal20D(FactorDefinition):
    metadata = FactorMetadata(
        name="price_reversal_20d",
        version="1",
        description="后复权收盘价 20 日收益率的反向信号。",
        inputs=(FactorInput("close_hfq", "valuation"),),
        lookback_days=20,
        higher_is_better=True,
    )

    def get_lookback_days(self, parameters=None) -> int:
        return _resolve_window_parameter(
            parameters, "lookback_days", 20, self.metadata.name
        )

    def compute(self, data, parameters=None) -> pd.DataFrame:
        ordered = _ordered_market_data(data)
        lookback_days = self.get_lookback_days(parameters)
        previous_close = ordered.groupby("symbol", sort=False)["close_hfq"].shift(
            lookback_days
        )
        ordered["value"] = -(ordered["close_hfq"] / previous_close - 1).where(
            ordered["close_hfq"].gt(0) & previous_close.gt(0)
        )
        return ordered.loc[:, ["date", "symbol", "value"]]
