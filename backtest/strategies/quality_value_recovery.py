import pandas as pd

from analysis.factors import FactorEngine
from backtest.strategy_base import (
    BacktestStrategy,
    StrategyMetadata,
    get_month_end_dates,
    rank_candidates_deterministically,
    select_equal_weight_targets,
)


class QualityValueRecoveryStrategy(BacktestStrategy):
    factor_names = (
        "price_trend_above_ma_120d",
        "valuation_pe_ttm",
        "valuation_pb",
        "quality_roe_weighted",
        "quality_operating_cashflow_ratio",
    )

    metadata = StrategyMetadata(
        name="quality-value-recovery",
        version="1",
        description="低估值、盈利质量与趋势确认的月度等权策略。",
        parameter_summary="holding_count, pe_pb_percentile, min_roe, min_operating_cashflow_to_revenue, trend_window, min_listing_days",
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
        return data_access.load_factor_data(
            config,
            self.factor_names,
            factor_parameters=self._factor_parameters(parameters),
            minimum_history_days=max(
                parameters["trend_window"], parameters["min_listing_days"]
            ),
            financial_signal_dates_only=True,
        )

    def build_targets(self, signal_data, config, parameters) -> pd.DataFrame:
        factor_data = FactorEngine().calculate_factors_on_dates(
            signal_data,
            self.factor_names,
            self._factor_parameters(parameters),
            get_month_end_dates(signal_data["date"]),
            symbol_batch_size=125,
        )
        ordered_input = signal_data.loc[:, ["date", "symbol"]].copy()
        ordered_input["date"] = pd.to_datetime(ordered_input["date"])
        ordered_input = ordered_input.sort_values(["symbol", "date"])
        ordered_input["listing_days"] = (
            ordered_input.groupby("symbol", sort=False).cumcount() + 1
        )
        signal_dates = get_month_end_dates(signal_data["date"])
        listing_days = ordered_input.loc[
            ordered_input["date"].isin(signal_dates)
            & (ordered_input["date"].dt.date >= config.start_date),
            ["date", "symbol", "listing_days"],
        ]
        data = factor_data.merge(
            listing_days,
            on=["date", "symbol"],
            how="left",
            validate="one_to_one",
        )
        rebalance_data = data[data["date"].isin(signal_dates)].copy()
        rebalance_data = rebalance_data[
            rebalance_data["date"].dt.date >= config.start_date
        ]
        rebalance_data = rebalance_data[
            (rebalance_data["listing_days"] >= parameters["min_listing_days"])
            & rebalance_data["valuation_pe_ttm"].notna()
            & rebalance_data["valuation_pb"].notna()
            & (rebalance_data["quality_roe_weighted"] > parameters["min_roe"])
            & (
                rebalance_data["quality_operating_cashflow_ratio"]
                > parameters["min_operating_cashflow_to_revenue"]
            )
            & rebalance_data["price_trend_above_ma_120d"].gt(0)
        ].copy()
        rebalance_data["pe_percentile"] = rebalance_data.groupby("date")[
            "valuation_pe_ttm"
        ].rank(pct=True)
        rebalance_data["pb_percentile"] = rebalance_data.groupby("date")[
            "valuation_pb"
        ].rank(pct=True)
        candidates = rebalance_data[
            (rebalance_data["pe_percentile"] <= parameters["pe_pb_percentile"])
            & (rebalance_data["pb_percentile"] <= parameters["pe_pb_percentile"])
        ].copy()
        candidates["score"] = candidates["pe_percentile"] + candidates["pb_percentile"]
        candidates = rank_candidates_deterministically(
            candidates,
            score_column="score",
            ascending=True,
        )
        return select_equal_weight_targets(candidates, parameters["holding_count"])

    @staticmethod
    def _factor_parameters(parameters: dict) -> dict:
        return {
            "price_trend_above_ma_120d": {"trend_window": parameters["trend_window"]}
        }


def _validate_parameters(parameters: dict, defaults: dict, strategy_name: str) -> dict:
    unknown = set(parameters) - set(defaults)
    if unknown:
        raise ValueError(
            f"策略 {strategy_name} 不支持参数: {', '.join(sorted(unknown))}"
        )
    resolved = {**defaults, **parameters}
    if not isinstance(resolved["holding_count"], int) or resolved["holding_count"] <= 0:
        raise ValueError("holding_count 必须是正整数")
    if not 0 < resolved["pe_pb_percentile"] <= 1:
        raise ValueError("pe_pb_percentile 必须在 (0, 1] 区间")
    if not isinstance(resolved["trend_window"], int) or resolved["trend_window"] <= 0:
        raise ValueError("trend_window 必须是正整数")
    if (
        not isinstance(resolved["min_listing_days"], int)
        or resolved["min_listing_days"] <= 0
    ):
        raise ValueError("min_listing_days 必须是正整数")
    return resolved
