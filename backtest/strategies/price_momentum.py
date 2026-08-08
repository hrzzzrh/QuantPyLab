import pandas as pd

from backtest.config import BacktestConfig
from backtest.data_access import BacktestDataAccess
from backtest.strategy_base import (
    BacktestStrategy,
    StrategyMetadata,
    get_month_end_dates,
    select_equal_weight_targets,
)


class PriceMomentumStrategy(BacktestStrategy):
    metadata = StrategyMetadata(
        name="price-momentum",
        version="1",
        description="后复权中期动量与趋势确认的月度等权策略。",
        parameter_summary="holding_count, lookback_days, trend_window, min_listing_days",
    )

    def validate_parameters(self, parameters: dict) -> dict:
        defaults = {
            "holding_count": 20,
            "lookback_days": 120,
            "trend_window": 120,
            "min_listing_days": 250,
        }
        unknown = set(parameters) - set(defaults)
        if unknown:
            raise ValueError(
                f"策略 {self.metadata.name} 不支持参数: {', '.join(sorted(unknown))}"
            )
        resolved = {**defaults, **parameters}
        for name in (
            "holding_count",
            "lookback_days",
            "trend_window",
            "min_listing_days",
        ):
            if not isinstance(resolved[name], int) or resolved[name] <= 0:
                raise ValueError(f"{name} 必须是正整数")
        return resolved

    def load_signal_data(
        self, data_access: BacktestDataAccess, config: BacktestConfig, parameters: dict
    ) -> pd.DataFrame:
        return data_access.load_market_data(
            config,
            max(
                parameters["lookback_days"],
                parameters["trend_window"],
                parameters["min_listing_days"],
            ),
        )

    def build_targets(self, signal_data, config, parameters) -> pd.DataFrame:
        data = signal_data.copy().sort_values(["symbol", "date"])
        data["listing_days"] = data.groupby("symbol").cumcount() + 1
        data["momentum"] = data.groupby("symbol")["close_hfq"].transform(
            lambda values: values / values.shift(parameters["lookback_days"]) - 1
        )
        data["trend_average"] = data.groupby("symbol")["close_hfq"].transform(
            lambda values: values.rolling(
                parameters["trend_window"], min_periods=parameters["trend_window"]
            ).mean()
        )
        candidates = data[data["date"].isin(get_month_end_dates(data["date"]))].copy()
        candidates = candidates[candidates["date"].dt.date >= config.start_date]
        candidates = candidates[
            (candidates["listing_days"] >= parameters["min_listing_days"])
            & candidates["momentum"].notna()
            & (candidates["close_hfq"] > candidates["trend_average"])
        ].copy()
        candidates["score"] = candidates["momentum"]
        candidates["rank"] = candidates.groupby("date")["score"].rank(
            method="first", ascending=False
        )
        return select_equal_weight_targets(candidates, parameters["holding_count"])
