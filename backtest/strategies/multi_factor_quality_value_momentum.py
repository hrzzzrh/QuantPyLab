import math
from collections.abc import Mapping

import pandas as pd

from analysis.factors import FactorEngine
from analysis.factors.registry import get_factor_definition
from analysis.factors.transforms import (
    combine_factor_scores,
    filter_valid_factor_rows,
    rank_factor_cross_sectionally,
    winsorize_factor_cross_sectionally,
)
from backtest.config import BacktestConfig
from backtest.data_access import BacktestDataAccess
from backtest.strategy_base import (
    BacktestStrategy,
    StrategyMetadata,
    get_month_end_dates,
    select_equal_weight_targets,
)

DEFAULT_FACTOR_WEIGHTS = {
    "price_momentum_120d": 0.20,
    "price_trend_gap_120d": 0.15,
    "price_volatility_60d": 0.10,
    "valuation_pe_ttm": 0.15,
    "valuation_pb": 0.15,
    "quality_roe_weighted": 0.125,
    "quality_operating_cashflow_ratio": 0.125,
}


class MultiFactorQualityValueMomentumStrategy(BacktestStrategy):
    metadata = StrategyMetadata(
        name="multi-factor-quality-value-momentum",
        version="1",
        description="价值、质量、动量、趋势与低波动因子合成的月度等权策略。",
        parameter_summary="holding_count, min_listing_days, winsorize_lower, winsorize_upper, factor_weights",
    )

    def validate_parameters(self, parameters: dict) -> dict:
        defaults = {
            "holding_count": 20,
            "min_listing_days": 250,
            "winsorize_lower": 0.05,
            "winsorize_upper": 0.95,
            "factor_weights": DEFAULT_FACTOR_WEIGHTS,
        }
        unknown = set(parameters) - set(defaults)
        if unknown:
            raise ValueError(
                f"策略 {self.metadata.name} 不支持参数: {', '.join(sorted(unknown))}"
            )

        resolved = {**defaults, **parameters}
        for name in ("holding_count", "min_listing_days"):
            if (
                isinstance(resolved[name], bool)
                or not isinstance(resolved[name], int)
                or resolved[name] <= 0
            ):
                raise ValueError(f"{name} 必须是正整数")

        lower = resolved["winsorize_lower"]
        upper = resolved["winsorize_upper"]
        if (
            isinstance(lower, bool)
            or isinstance(upper, bool)
            or not isinstance(lower, (int, float))
            or not isinstance(upper, (int, float))
            or not math.isfinite(lower)
            or not math.isfinite(upper)
            or not 0 <= lower < upper <= 1
        ):
            raise ValueError(
                "winsorize_lower 和 winsorize_upper 必须满足 0 <= lower < upper <= 1"
            )

        factor_weights = resolved["factor_weights"]
        if not isinstance(factor_weights, Mapping):
            raise ValueError("factor_weights 必须是映射")
        merged_weights = {**DEFAULT_FACTOR_WEIGHTS, **factor_weights}
        unknown_factors = set(merged_weights) - set(DEFAULT_FACTOR_WEIGHTS)
        if unknown_factors:
            raise ValueError(
                "策略使用了未纳入首期范围的因子: " + ", ".join(sorted(unknown_factors))
            )
        for factor_name, weight in merged_weights.items():
            if (
                isinstance(weight, bool)
                or not isinstance(weight, (int, float))
                or not math.isfinite(weight)
                or weight < 0
            ):
                raise ValueError(f"因子 {factor_name} 的权重必须是非负有限数字")
        total_weight = sum(merged_weights.values())
        if total_weight <= 0:
            raise ValueError("factor_weights 不能全部为零")
        normalized_weights = {
            name: weight / total_weight for name, weight in merged_weights.items()
        }

        resolved["factor_weights"] = normalized_weights
        resolved["factor_versions"] = {
            name: get_factor_definition(name).metadata.version
            for name in normalized_weights
        }
        return resolved

    def load_signal_data(
        self, data_access: BacktestDataAccess, config: BacktestConfig, parameters: dict
    ) -> pd.DataFrame:
        return data_access.load_factor_data(
            config,
            tuple(parameters["factor_weights"]),
            minimum_history_days=parameters["min_listing_days"],
        )

    def build_targets(
        self, signal_data: pd.DataFrame, config: BacktestConfig, parameters: dict
    ) -> pd.DataFrame:
        factor_names = tuple(parameters["factor_weights"])
        factor_frame = FactorEngine().calculate(signal_data, factor_names)

        ordered_input = signal_data.copy()
        ordered_input["date"] = pd.to_datetime(ordered_input["date"])
        ordered_input = ordered_input.sort_values(["symbol", "date"])
        listing_days = ordered_input[["date", "symbol"]].copy()
        listing_days["listing_days"] = (
            ordered_input.groupby("symbol", sort=False).cumcount() + 1
        ).to_numpy()
        factor_frame = factor_frame.merge(
            listing_days,
            on=["date", "symbol"],
            how="left",
            validate="one_to_one",
        )

        candidates = factor_frame[
            factor_frame["date"].isin(get_month_end_dates(factor_frame["date"]))
        ].copy()
        candidates = candidates[candidates["date"].dt.date >= config.start_date]
        candidates = candidates[
            candidates["listing_days"] >= parameters["min_listing_days"]
        ]
        candidates = filter_valid_factor_rows(candidates, factor_names)

        score_columns = {}
        for factor_name, weight in parameters["factor_weights"].items():
            transformed = candidates.loc[:, ["date", factor_name]].copy()
            transformed[factor_name] = winsorize_factor_cross_sectionally(
                transformed,
                factor_name,
                parameters["winsorize_lower"],
                parameters["winsorize_upper"],
            )
            score_column = f"{factor_name}_rank"
            candidates[score_column] = rank_factor_cross_sectionally(
                transformed,
                factor_name,
                get_factor_definition(factor_name).metadata.higher_is_better,
            )
            score_columns[score_column] = weight

        candidates["score"] = combine_factor_scores(candidates, score_columns)
        candidates["rank"] = candidates.groupby("date")["score"].rank(
            method="first", ascending=False
        )
        return select_equal_weight_targets(candidates, parameters["holding_count"])
