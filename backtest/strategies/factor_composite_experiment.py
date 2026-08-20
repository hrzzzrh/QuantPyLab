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

MAX_EXPERIMENT_FACTOR_COUNT = 6


class FactorCompositeExperimentStrategy(BacktestStrategy):
    """用于单因子和小组合研究的通用月度等权策略。"""

    metadata = StrategyMetadata(
        name="factor-composite-experiment",
        version="1",
        description="显式指定单因子或小组合、按方向排名后月度等权持仓的实验策略。",
        parameter_summary="factor_weights, factor_parameters, holding_count, min_listing_days, winsorize_lower, winsorize_upper",
    )

    def validate_parameters(self, parameters: dict) -> dict:
        defaults = {
            "factor_weights": {"price_momentum_120d": 1.0},
            "factor_parameters": {},
            "holding_count": 20,
            "min_listing_days": 250,
            "winsorize_lower": 0.05,
            "winsorize_upper": 0.95,
        }
        unknown = set(parameters) - set(defaults)
        if unknown:
            raise ValueError(
                f"策略 {self.metadata.name} 不支持参数: {', '.join(sorted(unknown))}"
            )

        resolved = {**defaults, **parameters}
        for name in ("holding_count", "min_listing_days"):
            value = resolved[name]
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
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
        if not isinstance(factor_weights, Mapping) or not factor_weights:
            raise ValueError("factor_weights 必须是非空映射")
        if len(factor_weights) > MAX_EXPERIMENT_FACTOR_COUNT:
            raise ValueError(
                f"实验策略最多同时使用 {MAX_EXPERIMENT_FACTOR_COUNT} 个因子"
            )

        validated_weights = {}
        for factor_name, weight in factor_weights.items():
            if not isinstance(factor_name, str) or not factor_name:
                raise ValueError("factor_weights 的因子名称必须是非空字符串")
            get_factor_definition(factor_name)
            if (
                isinstance(weight, bool)
                or not isinstance(weight, (int, float))
                or not math.isfinite(weight)
                or weight <= 0
            ):
                raise ValueError(f"因子 {factor_name} 的权重必须是正的有限数字")
            validated_weights[factor_name] = float(weight)

        total_weight = sum(validated_weights.values())
        resolved["factor_weights"] = {
            name: weight / total_weight for name, weight in validated_weights.items()
        }

        factor_parameters = resolved["factor_parameters"]
        if not isinstance(factor_parameters, Mapping):
            raise ValueError("factor_parameters 必须是映射")
        unused_parameters = set(factor_parameters) - set(validated_weights)
        if unused_parameters:
            raise ValueError(
                "factor_parameters 包含未选中的因子: "
                + ", ".join(sorted(unused_parameters))
            )

        resolved["factor_parameters"] = {}
        for factor_name in validated_weights:
            factor_specific_parameters = factor_parameters.get(factor_name, {})
            if not isinstance(factor_specific_parameters, Mapping):
                raise ValueError(f"因子 {factor_name} 的参数必须是映射")
            normalized_parameters = dict(factor_specific_parameters)
            get_factor_definition(factor_name).get_lookback_days(normalized_parameters)
            resolved["factor_parameters"][factor_name] = normalized_parameters

        resolved["factor_versions"] = {
            name: get_factor_definition(name).metadata.version
            for name in resolved["factor_weights"]
        }
        return resolved

    def load_signal_data(
        self,
        data_access: BacktestDataAccess,
        config: BacktestConfig,
        parameters: dict,
    ) -> pd.DataFrame:
        factor_names = tuple(parameters["factor_weights"])
        return data_access.load_factor_data(
            config,
            factor_names,
            factor_parameters=parameters["factor_parameters"],
            minimum_history_days=parameters["min_listing_days"],
        )

    def build_targets(
        self,
        signal_data: pd.DataFrame,
        config: BacktestConfig,
        parameters: dict,
    ) -> pd.DataFrame:
        factor_frame = self.calculate_factor_frame(signal_data, parameters)
        candidates = self.prepare_target_candidates(
            signal_data,
            factor_frame,
            config,
            parameters,
        )
        return self.build_targets_from_candidates(candidates, parameters)

    @staticmethod
    def calculate_factor_frame(
        signal_data: pd.DataFrame, parameters: dict
    ) -> pd.DataFrame:
        factor_names = tuple(parameters["factor_weights"])
        return FactorEngine().calculate(
            signal_data,
            factor_names,
            parameters["factor_parameters"],
        )

    @staticmethod
    def prepare_target_candidates(
        signal_data: pd.DataFrame,
        factor_frame: pd.DataFrame,
        config: BacktestConfig,
        parameters: dict,
    ) -> pd.DataFrame:
        factor_names = tuple(parameters["factor_weights"])
        ordered_input = signal_data.copy()
        ordered_input["date"] = pd.to_datetime(ordered_input["date"])
        ordered_input = ordered_input.sort_values(["symbol", "date"])
        listing_days = ordered_input[["date", "symbol"]].copy()
        listing_days["listing_days"] = (
            ordered_input.groupby("symbol", sort=False).cumcount() + 1
        ).to_numpy()
        candidates = factor_frame.merge(
            listing_days,
            on=["date", "symbol"],
            how="left",
            validate="one_to_one",
        )
        candidates = candidates[
            candidates["date"].isin(get_month_end_dates(candidates["date"]))
        ].copy()
        candidates = candidates[candidates["date"].dt.date >= config.start_date]
        candidates = candidates[
            candidates["listing_days"] >= parameters["min_listing_days"]
        ]
        return filter_valid_factor_rows(candidates, factor_names)

    @staticmethod
    def build_targets_from_candidates(
        candidates: pd.DataFrame, parameters: dict
    ) -> pd.DataFrame:
        scored_candidates = FactorCompositeExperimentStrategy.score_target_candidates(
            candidates, parameters
        )
        return select_equal_weight_targets(
            scored_candidates, parameters["holding_count"]
        )

    @staticmethod
    def score_target_candidates(
        candidates: pd.DataFrame, parameters: dict
    ) -> pd.DataFrame:
        """Score every valid candidate before holding-count selection."""

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
        candidates["rank"] = candidates.groupby("date")["score"].rank(
            method="first", ascending=False
        )
        return candidates
