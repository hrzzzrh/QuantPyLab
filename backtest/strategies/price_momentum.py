import pandas as pd

from analysis.factors import FactorEngine
from backtest.config import BacktestConfig
from backtest.data_access import BacktestDataAccess
from backtest.strategy_base import (
    BacktestStrategy,
    StrategyMetadata,
    get_month_end_dates,
    select_equal_weight_targets,
)


class PriceMomentumStrategy(BacktestStrategy):
    factor_names = ("price_momentum_120d", "price_trend_above_ma_120d")

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
        return data_access.load_factor_data(
            config,
            self.factor_names,
            factor_parameters=self._factor_parameters(parameters),
            minimum_history_days=max(
                parameters["lookback_days"],
                parameters["trend_window"],
                parameters["min_listing_days"],
            ),
        )

    def build_targets(self, signal_data, config, parameters) -> pd.DataFrame:
        factor_data = FactorEngine().calculate(
            signal_data,
            self.factor_names,
            self._factor_parameters(parameters),
        )
        ordered_input = signal_data.copy()
        ordered_input["date"] = pd.to_datetime(ordered_input["date"])
        ordered_input = ordered_input.sort_values(["symbol", "date"])
        listing_days = ordered_input[["date", "symbol"]].copy()
        listing_days["listing_days"] = (
            ordered_input.groupby("symbol", sort=False).cumcount() + 1
        ).to_numpy()
        data = factor_data.merge(
            listing_days,
            on=["date", "symbol"],
            how="left",
            validate="one_to_one",
        )
        candidates = data[data["date"].isin(get_month_end_dates(data["date"]))].copy()
        candidates = candidates[candidates["date"].dt.date >= config.start_date]
        candidates = candidates[
            (candidates["listing_days"] >= parameters["min_listing_days"])
            & candidates["price_momentum_120d"].notna()
            & candidates["price_trend_above_ma_120d"].gt(0)
        ].copy()
        candidates["score"] = candidates["price_momentum_120d"]
        candidates["rank"] = candidates.groupby("date")["score"].rank(
            method="first", ascending=False
        )
        return select_equal_weight_targets(candidates, parameters["holding_count"])

    @staticmethod
    def _factor_parameters(parameters: dict) -> dict:
        return {
            "price_momentum_120d": {"lookback_days": parameters["lookback_days"]},
            "price_trend_above_ma_120d": {"trend_window": parameters["trend_window"]},
        }
