"""Expand bounded, auditable factor-experiment hyperparameter grids."""

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from itertools import product

from backtest.config import BacktestConfig
from backtest.strategy_registry import get_backtest_strategy


@dataclass(frozen=True)
class HyperparameterSearchSpec:
    """Finite search space for the outer factor-experiment parameters."""

    enabled: bool = True
    max_combinations: int = 100
    holding_counts: tuple[int, ...] = (20,)
    winsorize_ranges: tuple[tuple[float, float], ...] = ((0.05, 0.95),)
    ridge_alphas: tuple[float, ...] = (0.1,)
    factor_parameter_values: dict[str, dict[str, tuple[object, ...]]] = field(
        default_factory=dict
    )

    def __post_init__(self):
        if not isinstance(self.enabled, bool):
            raise ValueError("[hyperparameter_search].enabled 必须是布尔值")
        if (
            isinstance(self.max_combinations, bool)
            or not isinstance(self.max_combinations, int)
            or self.max_combinations <= 0
        ):
            raise ValueError("[hyperparameter_search].max_combinations 必须是正整数")
        _validate_positive_integer_values(
            self.holding_counts,
            "[hyperparameter_search].holding_counts",
        )
        _validate_winsorize_ranges(self.winsorize_ranges)
        _validate_nonnegative_finite_values(
            self.ridge_alphas,
            "[hyperparameter_search].ridge_alphas",
        )
        if not isinstance(self.factor_parameter_values, Mapping):
            raise ValueError("[hyperparameter_search].factor_parameters 必须是 TOML 表")
        for factor_name, parameter_values in self.factor_parameter_values.items():
            if not isinstance(factor_name, str) or not factor_name:
                raise ValueError("搜索因子名称必须是非空字符串")
            if not isinstance(parameter_values, Mapping) or not parameter_values:
                raise ValueError(f"因子 {factor_name} 的搜索参数必须是非空 TOML 表")
            for parameter_name, values in parameter_values.items():
                if not isinstance(parameter_name, str) or not parameter_name:
                    raise ValueError("搜索因子参数名称必须是非空字符串")
                if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
                    raise ValueError(
                        f"因子 {factor_name}.{parameter_name} 的搜索值必须是数组"
                    )
                if not values:
                    raise ValueError(
                        f"因子 {factor_name}.{parameter_name} 的搜索值不能为空"
                    )
                _validate_unique_values(
                    values,
                    f"因子 {factor_name}.{parameter_name} 的搜索值",
                )

    def to_dict(self) -> dict:
        return {
            "enabled": self.enabled,
            "max_combinations": self.max_combinations,
            "holding_counts": list(self.holding_counts),
            "winsorize_ranges": [
                [lower, upper] for lower, upper in self.winsorize_ranges
            ],
            "ridge_alphas": list(self.ridge_alphas),
            "factor_parameters": {
                factor_name: {
                    parameter_name: list(values)
                    for parameter_name, values in parameter_values.items()
                }
                for factor_name, parameter_values in self.factor_parameter_values.items()
            },
        }


@dataclass(frozen=True)
class HyperparameterTrial:
    """One fully specified candidate configuration in the outer search."""

    trial_id: str
    candidate_id: str
    config: BacktestConfig
    parameters: dict[str, object]


def expand_hyperparameter_trials(
    candidate_configs: Sequence[tuple[str, BacktestConfig]],
    search: HyperparameterSearchSpec,
) -> tuple[HyperparameterTrial, ...]:
    """Expand candidate TOMLs and bounded parameter grids deterministically."""

    if not candidate_configs:
        raise ValueError("超参数搜索至少需要一个候选回测配置")
    factor_sets = {
        factor_name
        for _, candidate_config in candidate_configs
        for factor_name in _get_factor_names(candidate_config)
    }
    unknown_factors = set(search.factor_parameter_values) - factor_sets
    if unknown_factors:
        raise ValueError(
            "超参数搜索包含所有候选都未使用的因子: "
            + ", ".join(sorted(unknown_factors))
        )

    total_combinations = sum(
        _count_candidate_combinations(candidate_config, search)
        for _, candidate_config in candidate_configs
    )
    if total_combinations > search.max_combinations:
        raise ValueError(
            "超参数组合数量超过上限: "
            f"{total_combinations} > max_combinations={search.max_combinations}"
        )

    trials = []
    for candidate_id, candidate_config in candidate_configs:
        factor_names = _get_factor_names(candidate_config)
        factor_dimensions = [
            (factor_name, parameter_name, values)
            for factor_name in factor_names
            for parameter_name, values in search.factor_parameter_values.get(
                factor_name, {}
            ).items()
        ]
        dimensions = [
            search.holding_counts,
            search.winsorize_ranges,
            search.ridge_alphas,
            *(values for _, _, values in factor_dimensions),
        ]
        for index, values in enumerate(product(*dimensions), start=1):
            holding_count, winsorize_range, ridge_alpha, *factor_values = values
            lower, upper = winsorize_range
            strategy_parameters = dict(candidate_config.strategy_parameters)
            strategy_parameters.update(
                {
                    "holding_count": holding_count,
                    "winsorize_lower": lower,
                    "winsorize_upper": upper,
                }
            )
            base_factor_parameters = strategy_parameters.get("factor_parameters", {})
            if not isinstance(base_factor_parameters, Mapping):
                raise ValueError("候选配置的 factor_parameters 必须是映射")
            factor_parameters = {
                factor_name: dict(base_factor_parameters.get(factor_name, {}))
                for factor_name in factor_names
            }
            for (factor_name, parameter_name, _), factor_value in zip(
                factor_dimensions, factor_values, strict=True
            ):
                factor_parameters[factor_name][parameter_name] = factor_value
            strategy_parameters["factor_parameters"] = factor_parameters
            get_backtest_strategy(candidate_config.strategy_name).validate_parameters(
                strategy_parameters
            )
            trial_config = replace(
                candidate_config,
                strategy_parameters=strategy_parameters,
                strategy_version="",
            )
            trials.append(
                HyperparameterTrial(
                    trial_id=f"{candidate_id}__trial_{index:03d}",
                    candidate_id=candidate_id,
                    config=trial_config,
                    parameters={
                        "holding_count": holding_count,
                        "winsorize_lower": lower,
                        "winsorize_upper": upper,
                        "ridge_alpha": ridge_alpha,
                        "factor_parameters": factor_parameters,
                    },
                )
            )
    return tuple(trials)


def _count_candidate_combinations(
    candidate_config: BacktestConfig, search: HyperparameterSearchSpec
) -> int:
    count = len(search.holding_counts)
    count *= len(search.winsorize_ranges)
    count *= len(search.ridge_alphas)
    for factor_name in _get_factor_names(candidate_config):
        for values in search.factor_parameter_values.get(factor_name, {}).values():
            count *= len(values)
    return count


def _get_factor_names(candidate_config: BacktestConfig) -> tuple[str, ...]:
    factor_weights = candidate_config.strategy_parameters.get("factor_weights")
    if not isinstance(factor_weights, Mapping) or not factor_weights:
        raise ValueError(
            f"候选 {candidate_config.strategy_name} 必须声明非空 factor_weights"
        )
    return tuple(factor_weights)


def _validate_positive_integer_values(values, field_name: str) -> None:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise ValueError(f"{field_name} 必须是正整数数组")
    if not values:
        raise ValueError(f"{field_name} 不能为空")
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{field_name} 必须全部是正整数")
    if len(set(values)) != len(values):
        raise ValueError(f"{field_name} 不能包含重复值")


def _validate_winsorize_ranges(ranges) -> None:
    if isinstance(ranges, (str, bytes)) or not isinstance(ranges, Sequence):
        raise ValueError(
            "[hyperparameter_search].winsorize_ranges 必须是 [lower, upper] 数组"
        )
    if not ranges:
        raise ValueError("[hyperparameter_search].winsorize_ranges 不能为空")
    normalized = []
    for value in ranges:
        if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
            raise ValueError("每个缩尾范围必须是 [lower, upper] 数组")
        if len(value) != 2:
            raise ValueError("每个缩尾范围必须正好包含 lower 和 upper")
        lower, upper = value
        if (
            isinstance(lower, bool)
            or isinstance(upper, bool)
            or not isinstance(lower, (int, float))
            or not isinstance(upper, (int, float))
            or not math.isfinite(lower)
            or not math.isfinite(upper)
            or not 0 <= lower < upper <= 1
        ):
            raise ValueError("缩尾范围必须满足 0 <= lower < upper <= 1")
        normalized.append((float(lower), float(upper)))
    if len(set(normalized)) != len(normalized):
        raise ValueError("缩尾范围不能包含重复值")


def _validate_nonnegative_finite_values(values, field_name: str) -> None:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise ValueError(f"{field_name} 必须是非负有限数字数组")
    if not values:
        raise ValueError(f"{field_name} 不能为空")
    for value in values:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
        ):
            raise ValueError(f"{field_name} 必须全部是非负有限数字")
    if len(set(values)) != len(values):
        raise ValueError(f"{field_name} 不能包含重复值")


def _validate_unique_values(values, field_name: str) -> None:
    """Reject duplicate TOML scalar/array values without requiring hashability."""

    for index, value in enumerate(values):
        if any(value == previous for previous in values[:index]):
            raise ValueError(f"{field_name} 不能包含重复值")
