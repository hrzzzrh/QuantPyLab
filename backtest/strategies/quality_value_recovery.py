import pandas as pd

from backtest.config import BacktestConfig
from backtest.data_access import BacktestDataAccess, IndicatorField
from backtest.strategy_base import BacktestStrategy, StrategyMetadata, get_month_end_dates, select_equal_weight_targets


class QualityValueRecoveryStrategy(BacktestStrategy):
    metadata = StrategyMetadata(
        name="quality-value-recovery",
        version="1",
        description="低估值、盈利质量与趋势确认的月度等权策略。",
        parameter_summary="holding_count, pe_pb_percentile, min_roe, min_operating_cashflow_to_revenue, trend_window, min_listing_days",
    )
    indicator_fields = (
        IndicatorField("净资产收益率_加权", "roe_weighted"),
        IndicatorField("经营现金流/营业收入", "operating_cashflow_to_revenue"),
    )

    def validate_parameters(self, parameters: dict) -> dict:
        defaults = {
            "holding_count": 20,
            "pe_pb_percentile": 0.4,
            "min_roe": 8.0,
            "min_operating_cashflow_to_revenue": 0.0,
            "trend_window": 120,
            "min_listing_days": 250,
        }
        return _validate_parameters(parameters, defaults, self.metadata.name)

    def load_signal_data(self, data_access, config, parameters) -> pd.DataFrame:
        lookback_days = max(parameters["trend_window"], parameters["min_listing_days"])
        return data_access.load_market_data(config, lookback_days, self.indicator_fields)

    def build_targets(self, signal_data, config, parameters) -> pd.DataFrame:
        data = signal_data.copy().sort_values(["symbol", "date"])
        data["listing_days"] = data.groupby("symbol").cumcount() + 1
        data["trend_average"] = data.groupby("symbol")["close_hfq"].transform(
            lambda values: values.rolling(parameters["trend_window"], min_periods=parameters["trend_window"]).mean()
        )
        rebalance_data = data[data["date"].isin(get_month_end_dates(data["date"]))].copy()
        rebalance_data = rebalance_data[rebalance_data["date"].dt.date >= config.start_date]
        rebalance_data = rebalance_data[
            (rebalance_data["listing_days"] >= parameters["min_listing_days"])
            & (rebalance_data["pe_ttm"] > 0)
            & (rebalance_data["pb"] > 0)
            & (rebalance_data["roe_weighted"] > parameters["min_roe"])
            & (rebalance_data["operating_cashflow_to_revenue"] > parameters["min_operating_cashflow_to_revenue"])
            & (rebalance_data["close_hfq"] > rebalance_data["trend_average"])
        ].copy()
        rebalance_data["pe_percentile"] = rebalance_data.groupby("date")["pe_ttm"].rank(pct=True)
        rebalance_data["pb_percentile"] = rebalance_data.groupby("date")["pb"].rank(pct=True)
        candidates = rebalance_data[
            (rebalance_data["pe_percentile"] <= parameters["pe_pb_percentile"])
            & (rebalance_data["pb_percentile"] <= parameters["pe_pb_percentile"])
        ].copy()
        candidates["score"] = candidates["pe_percentile"] + candidates["pb_percentile"]
        candidates["rank"] = candidates.groupby("date")["score"].rank(method="first")
        return select_equal_weight_targets(candidates, parameters["holding_count"])


def _validate_parameters(parameters: dict, defaults: dict, strategy_name: str) -> dict:
    unknown = set(parameters) - set(defaults)
    if unknown:
        raise ValueError(f"策略 {strategy_name} 不支持参数: {', '.join(sorted(unknown))}")
    resolved = {**defaults, **parameters}
    if not isinstance(resolved["holding_count"], int) or resolved["holding_count"] <= 0:
        raise ValueError("holding_count 必须是正整数")
    if not 0 < resolved["pe_pb_percentile"] <= 1:
        raise ValueError("pe_pb_percentile 必须在 (0, 1] 区间")
    if not isinstance(resolved["trend_window"], int) or resolved["trend_window"] <= 0:
        raise ValueError("trend_window 必须是正整数")
    if not isinstance(resolved["min_listing_days"], int) or resolved["min_listing_days"] <= 0:
        raise ValueError("min_listing_days 必须是正整数")
    return resolved
