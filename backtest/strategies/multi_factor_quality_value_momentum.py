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
    rank_candidates_deterministically,
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
    supports_factor_training = True

    metadata = StrategyMetadata(
        name="multi-factor-quality-value-momentum",
        version="1",
        description="价值、质量、动量、趋势与低波动因子合成的月度等权策略。",
        parameter_summary="holding_count, min_listing_days, winsorize_lower, winsorize_upper, factor_weights, factor_parameters",
    )

    def validate_parameters(self, parameters: dict) -> dict:
        defaults = {
            "holding_count": 20,
            "min_listing_days": 250,
            "winsorize_lower": 0.05,
            "winsorize_upper": 0.95,
            "factor_weights": DEFAULT_FACTOR_WEIGHTS,
            "factor_parameters": {},
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
        factor_parameters = resolved["factor_parameters"]
        if not isinstance(factor_parameters, Mapping):
            raise ValueError("factor_parameters 必须是映射")
        unused_parameters = set(factor_parameters) - set(normalized_weights)
        if unused_parameters:
            raise ValueError(
                "factor_parameters 包含未使用的因子: "
                + ", ".join(sorted(unused_parameters))
            )
        normalized_parameters = {}
        for factor_name in normalized_weights:
            factor_specific_parameters = factor_parameters.get(factor_name, {})
            if not isinstance(factor_specific_parameters, Mapping):
                raise ValueError(f"因子 {factor_name} 的参数必须是映射")
            normalized_factor_parameters = dict(factor_specific_parameters)
            get_factor_definition(factor_name).get_lookback_days(
                normalized_factor_parameters
            )
            normalized_parameters[factor_name] = normalized_factor_parameters
        resolved["factor_parameters"] = normalized_parameters
        resolved["factor_versions"] = {
            name: get_factor_definition(name).metadata.version
            for name in normalized_weights
        }
        return resolved

    @staticmethod
    def apply_trained_factor_weights(
        parameters: dict, factor_weights: dict[str, float]
    ) -> dict[str, float]:
        """Return the complete factor map so fitted zeroes override defaults."""

        expected_factors = tuple(parameters["factor_weights"])
        if set(factor_weights) != set(expected_factors):
            raise ValueError("正式多因子策略训练结果必须覆盖全部七个因子")
        return {
            factor_name: float(factor_weights[factor_name])
            for factor_name in expected_factors
        }

    def load_signal_data(
        self, data_access: BacktestDataAccess, config: BacktestConfig, parameters: dict
    ) -> pd.DataFrame:
        return data_access.load_factor_data(
            config,
            tuple(parameters["factor_weights"]),
            factor_parameters=parameters["factor_parameters"],
            minimum_history_days=parameters["min_listing_days"],
            financial_signal_dates_only=True,
        )

    @staticmethod
    def calculate_factor_frame(
        signal_data: pd.DataFrame, parameters: dict
    ) -> pd.DataFrame:
        factor_names = tuple(parameters["factor_weights"])
        return FactorEngine().calculate_factors_on_dates(
            signal_data,
            factor_names,
            parameters["factor_parameters"],
            get_month_end_dates(signal_data["date"]),
            symbol_batch_size=125,
        )

    @staticmethod
    def prepare_target_candidates(
        signal_data: pd.DataFrame,
        factor_frame: pd.DataFrame,
        config: BacktestConfig,
        parameters: dict,
    ) -> pd.DataFrame:
        """Build the factor-valid monthly candidate universe before scoring."""

        factor_names = tuple(parameters["factor_weights"])
        signal_dates = get_month_end_dates(signal_data["date"])
        signal_date_mask = signal_data["date"].isin(signal_dates) & (
            signal_data["date"].dt.date >= config.start_date
        )
        candidates = signal_data.loc[signal_date_mask, ["date", "symbol"]].copy()

        ordered_input = signal_data.loc[:, ["date", "symbol"]].copy()
        ordered_input["date"] = pd.to_datetime(ordered_input["date"])
        ordered_input = ordered_input.sort_values(["symbol", "date"])
        ordered_input["listing_days"] = (
            ordered_input.groupby("symbol", sort=False).cumcount() + 1
        )
        listing_days = ordered_input.loc[
            ordered_input["date"].isin(signal_dates)
            & (ordered_input["date"].dt.date >= config.start_date),
            ["date", "symbol", "listing_days"],
        ]
        candidates = candidates.merge(
            listing_days,
            on=["date", "symbol"],
            how="left",
            validate="one_to_one",
        )
        candidates = candidates[
            candidates["listing_days"] >= parameters["min_listing_days"]
        ]
        factor_frame = factor_frame.loc[
            factor_frame["date"].isin(signal_dates)
            & (factor_frame["date"].dt.date >= config.start_date),
            ["date", "symbol", *factor_names],
        ]
        candidates = candidates.merge(
            factor_frame,
            on=["date", "symbol"],
            how="left",
            validate="one_to_one",
        )
        candidates = filter_valid_factor_rows(candidates, factor_names)
        return candidates

    @staticmethod
    def score_candidates(candidates: pd.DataFrame, parameters: dict) -> pd.DataFrame:
        """Apply trial-specific transforms and return ranked candidates."""

        candidates = candidates.copy()
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
        return rank_candidates_deterministically(
            candidates,
            score_column="score",
            ascending=False,
        )

    @staticmethod
    def build_targets_from_candidates(
        candidates: pd.DataFrame, parameters: dict
    ) -> pd.DataFrame:
        """Apply trial-specific transforms and build equal-weight targets."""

        candidates = MultiFactorQualityValueMomentumStrategy.score_candidates(
            candidates, parameters
        )
        return select_equal_weight_targets(candidates, parameters["holding_count"])

    def build_candidates(
        self, signal_data: pd.DataFrame, config: BacktestConfig, parameters: dict
    ) -> pd.DataFrame:
        """Build the scored monthly candidate universe once for reuse by reports."""

        candidates = self.prepare_target_candidates(
            signal_data,
            self.calculate_factor_frame(signal_data, parameters),
            config,
            parameters,
        )
        return self.score_candidates(candidates, parameters)

    def build_targets(
        self, signal_data: pd.DataFrame, config: BacktestConfig, parameters: dict
    ) -> pd.DataFrame:
        return self.build_targets_from_candidates(
            self.build_candidates(signal_data, config, parameters),
            parameters,
        )
