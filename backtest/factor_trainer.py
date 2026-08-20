"""Fit explainable factor-composite weights on point-in-time observations."""

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date

import pandas as pd

from analysis.factors import FactorEngine
from analysis.factors.diagnostics import calculate_forward_returns
from analysis.factors.registry import get_factor_definition
from analysis.factors.transforms import (
    filter_valid_factor_rows,
    rank_factor_cross_sectionally,
    winsorize_factor_cross_sectionally,
)
from backtest.strategy_base import get_month_end_dates


@dataclass(frozen=True)
class FactorTrainingResult:
    """The fitted weights and enough metadata to audit one training run."""

    factor_weights: dict[str, float]
    observation_count: int
    signal_date_count: int
    iterations: int
    converged: bool
    label_horizon_days: int
    ridge_alpha: float
    signal_date_start: str | None = None
    signal_date_end: str | None = None

    def to_dict(self) -> dict:
        return {
            "factor_weights": dict(self.factor_weights),
            "observation_count": self.observation_count,
            "signal_date_count": self.signal_date_count,
            "iterations": self.iterations,
            "converged": self.converged,
            "label_horizon_days": self.label_horizon_days,
            "ridge_alpha": self.ridge_alpha,
            "signal_date_start": self.signal_date_start,
            "signal_date_end": self.signal_date_end,
        }


def prepare_factor_training_data(
    data: pd.DataFrame,
    factor_names: Sequence[str],
    factor_parameters: Mapping[str, Mapping[str, object]] | None,
    train_start_date: date | pd.Timestamp | str,
    train_end_date: date | pd.Timestamp | str,
    *,
    minimum_history_days: int = 0,
    label_horizon_days: int = 20,
) -> pd.DataFrame:
    """Build reusable monthly factor rows and future-return labels.

    The returned frame intentionally does not apply winsorization or ranking.
    Those operations depend on the outer hyperparameter trial and are applied
    by :func:`fit_factor_weights` for each trial.
    """

    names = _validate_factor_names(factor_names)
    parameters = _validate_factor_parameters(factor_parameters, names)
    start_date = _normalize_date(train_start_date, "train_start_date")
    end_date = _normalize_date(train_end_date, "train_end_date")
    if start_date >= end_date:
        raise ValueError("训练开始日期必须早于训练结束日期")
    if (
        isinstance(minimum_history_days, bool)
        or not isinstance(minimum_history_days, int)
        or minimum_history_days < 0
    ):
        raise ValueError("minimum_history_days 必须是非负整数")
    if (
        isinstance(label_horizon_days, bool)
        or not isinstance(label_horizon_days, int)
        or label_horizon_days <= 0
    ):
        raise ValueError("label_horizon_days 必须是正整数")

    normalized = _normalize_training_input(data)
    factor_frame = FactorEngine().calculate(normalized, names, parameters)
    forward_returns = calculate_forward_returns(
        normalized, horizons=(label_horizon_days,)
    )
    label_column = f"forward_return_{label_horizon_days}d"
    label_exit_dates = normalized.loc[:, ["date", "symbol"]].copy()
    label_exit_dates = label_exit_dates.sort_values(["symbol", "date"])
    label_exit_dates["label_exit_date"] = label_exit_dates.groupby(
        "symbol", sort=False
    )["date"].shift(-label_horizon_days)
    candidates = factor_frame.merge(
        forward_returns.loc[:, ["date", "symbol", label_column]],
        on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    )
    candidates = candidates.merge(
        label_exit_dates,
        on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    )

    listing_days = normalized.loc[:, ["date", "symbol"]].copy()
    listing_days = listing_days.sort_values(["symbol", "date"])
    listing_days["listing_days"] = (
        listing_days.groupby("symbol", sort=False).cumcount() + 1
    )
    candidates = candidates.merge(
        listing_days,
        on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    )
    candidates = candidates[
        candidates["date"].isin(get_month_end_dates(candidates["date"]))
    ].copy()
    candidates = candidates[
        candidates["date"].between(start_date, end_date, inclusive="both")
    ]
    candidates = candidates[
        candidates["label_exit_date"].notna()
        & (candidates["label_exit_date"] <= end_date)
    ]
    candidates = candidates[candidates["listing_days"] >= minimum_history_days].copy()
    candidates = filter_valid_factor_rows(candidates, names)
    return candidates.loc[
        :, ["date", "symbol", *names, "label_exit_date", label_column]
    ].copy()


def fit_factor_weights(
    data: pd.DataFrame,
    factor_names: Sequence[str],
    factor_parameters: Mapping[str, Mapping[str, object]] | None,
    train_start_date: date | pd.Timestamp | str,
    train_end_date: date | pd.Timestamp | str,
    *,
    minimum_history_days: int = 0,
    winsorize_lower: float = 0.05,
    winsorize_upper: float = 0.95,
    label_horizon_days: int = 20,
    ridge_alpha: float = 0.1,
    max_iterations: int = 5000,
    minimum_training_observations: int = 200,
    minimum_training_dates: int = 24,
    prepared_data: pd.DataFrame | None = None,
) -> FactorTrainingResult:
    """Fit nonnegative Ridge weights from monthly point-in-time factor rows.

    Factors are transformed exactly as the factor-composite strategy transforms
    them: cross-sectional winsorization followed by a direction-aware
    percentile rank. The target is the return from the next trading-day open
    to the close ``label_horizon_days`` observations later. Both features and
    targets are demeaned within each signal date before the constrained Ridge
    fit, so the model learns cross-sectional factor efficacy rather than the
    market's common return.
    """

    names = _validate_factor_names(factor_names)
    parameters = _validate_factor_parameters(factor_parameters, names)
    start_date = _normalize_date(train_start_date, "train_start_date")
    end_date = _normalize_date(train_end_date, "train_end_date")
    if start_date >= end_date:
        raise ValueError("训练开始日期必须早于训练结束日期")
    _validate_training_parameters(
        minimum_history_days=minimum_history_days,
        winsorize_lower=winsorize_lower,
        winsorize_upper=winsorize_upper,
        label_horizon_days=label_horizon_days,
        ridge_alpha=ridge_alpha,
        max_iterations=max_iterations,
        minimum_training_observations=minimum_training_observations,
        minimum_training_dates=minimum_training_dates,
    )

    label_column = f"forward_return_{label_horizon_days}d"
    if prepared_data is None:
        candidates = prepare_factor_training_data(
            data,
            names,
            parameters,
            start_date,
            end_date,
            minimum_history_days=minimum_history_days,
            label_horizon_days=label_horizon_days,
        )
    else:
        candidates = _validate_prepared_training_data(
            prepared_data, names, label_column
        )
    candidates["label_exit_date"] = pd.to_datetime(
        candidates["label_exit_date"], errors="coerce"
    )
    candidates = candidates[
        candidates["label_exit_date"].notna()
        & (candidates["label_exit_date"] <= end_date)
    ].copy()
    candidates = filter_valid_factor_rows(candidates, names)

    rank_columns = []
    for factor_name in names:
        transformed = candidates.loc[:, ["date", factor_name]].copy()
        transformed[factor_name] = winsorize_factor_cross_sectionally(
            transformed,
            factor_name,
            winsorize_lower,
            winsorize_upper,
        )
        rank_column = f"{factor_name}__rank"
        candidates[rank_column] = rank_factor_cross_sectionally(
            transformed,
            factor_name,
            get_factor_definition(factor_name).metadata.higher_is_better,
        )
        rank_columns.append(rank_column)

    training_frame = candidates.loc[:, ["date", *rank_columns, label_column]].copy()
    training_frame[rank_columns] = training_frame[rank_columns].apply(
        pd.to_numeric, errors="coerce"
    )
    training_frame[label_column] = pd.to_numeric(
        training_frame[label_column], errors="coerce"
    )

    feature_frame = training_frame.loc[:, rank_columns].copy()
    feature_frame.columns = list(names)
    feature_frame = feature_frame - feature_frame.groupby(
        training_frame["date"], sort=False
    ).transform("mean")
    target = training_frame[label_column]
    target = target - target.groupby(training_frame["date"], sort=False).transform(
        "mean"
    )
    training_frame = pd.concat(
        [training_frame.loc[:, ["date"]], feature_frame, target.rename("target")],
        axis=1,
    ).dropna(subset=[*names, "target"])

    observation_count = len(training_frame)
    signal_date_count = int(training_frame["date"].nunique())
    if observation_count < minimum_training_observations:
        raise ValueError(
            f"训练样本不足: {observation_count} < "
            f"minimum_training_observations={minimum_training_observations}"
        )
    if signal_date_count < minimum_training_dates:
        raise ValueError(
            f"训练信号日不足: {signal_date_count} < "
            f"minimum_training_dates={minimum_training_dates}"
        )

    weights, iterations, converged = _fit_nonnegative_ridge(
        training_frame.loc[:, list(names)],
        training_frame["target"],
        names,
        ridge_alpha,
        max_iterations,
    )
    if not converged:
        raise ValueError(
            "非负 Ridge 训练未在 max_iterations 内收敛: "
            f"max_iterations={max_iterations}"
        )
    total_weight = float(weights.sum())
    if not math.isfinite(total_weight) or total_weight <= 1e-12:
        raise ValueError("训练得到的因子权重全部退化为零")
    normalized_weights = (weights / total_weight).to_dict()
    signal_dates = training_frame["date"].drop_duplicates().sort_values()
    return FactorTrainingResult(
        factor_weights={name: float(normalized_weights[name]) for name in names},
        observation_count=observation_count,
        signal_date_count=signal_date_count,
        iterations=iterations,
        converged=converged,
        label_horizon_days=label_horizon_days,
        ridge_alpha=float(ridge_alpha),
        signal_date_start=signal_dates.iloc[0].date().isoformat(),
        signal_date_end=signal_dates.iloc[-1].date().isoformat(),
    )


def _fit_nonnegative_ridge(
    features: pd.DataFrame,
    target: pd.Series,
    factor_names: tuple[str, ...],
    ridge_alpha: float,
    max_iterations: int,
) -> tuple[pd.Series, int, bool]:
    sample_count = len(features)
    gram = features.T.dot(features).astype(float) / sample_count
    covariance = features.T.dot(target.astype(float)) / sample_count
    lipschitz_bound = float(gram.abs().sum(axis=1).max()) + ridge_alpha
    step_size = 1 / max(lipschitz_bound, 1e-12)
    weights = pd.Series(0.0, index=factor_names, dtype="float64")
    converged = False
    iterations = max_iterations

    for iteration in range(1, max_iterations + 1):
        gradient = gram.dot(weights) - covariance + ridge_alpha * weights
        next_weights = (weights - step_size * gradient).clip(lower=0.0)
        if float((next_weights - weights).abs().max()) <= 1e-10:
            weights = next_weights
            iterations = iteration
            converged = True
            break
        weights = next_weights

    return weights, iterations, converged


def _validate_factor_names(factor_names: Sequence[str]) -> tuple[str, ...]:
    if isinstance(factor_names, (str, bytes)):
        raise ValueError("factor_names 必须是因子名称序列")
    names = tuple(dict.fromkeys(factor_names))
    if not names:
        raise ValueError("至少需要一个训练因子")
    for name in names:
        if not isinstance(name, str) or not name:
            raise ValueError("因子名称必须是非空字符串")
        get_factor_definition(name)
    return names


def _validate_factor_parameters(
    factor_parameters: Mapping[str, Mapping[str, object]] | None,
    factor_names: tuple[str, ...],
) -> dict[str, dict[str, object]]:
    parameters = dict(factor_parameters or {})
    unknown = set(parameters) - set(factor_names)
    if unknown:
        raise ValueError(
            "factor_parameters 包含未选中的因子: " + ", ".join(sorted(unknown))
        )
    normalized = {}
    for factor_name in factor_names:
        value = parameters.get(factor_name, {})
        if not isinstance(value, Mapping):
            raise ValueError(f"因子 {factor_name} 的参数必须是映射")
        get_factor_definition(factor_name).get_lookback_days(value)
        normalized[factor_name] = dict(value)
    return normalized


def _normalize_training_input(data: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "symbol", "open_hfq", "close_hfq"}
    missing = required - set(data.columns)
    if missing:
        raise ValueError(f"因子训练输入缺少字段: {', '.join(sorted(missing))}")
    normalized = data.copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    if normalized["date"].isna().any():
        raise ValueError("因子训练输入包含无效日期")
    if normalized[["date", "symbol"]].isna().any().any():
        raise ValueError("因子训练输入的 date 和 symbol 不能为空")
    if normalized.duplicated(["date", "symbol"]).any():
        raise ValueError("因子训练输入不能包含重复的 date/symbol")
    return normalized


def _validate_prepared_training_data(
    data: pd.DataFrame,
    factor_names: tuple[str, ...],
    label_column: str,
) -> pd.DataFrame:
    required = {"date", "symbol", *factor_names, "label_exit_date", label_column}
    missing = required - set(data.columns)
    if missing:
        raise ValueError("预计算因子训练数据缺少字段: " + ", ".join(sorted(missing)))
    prepared = data.loc[
        :, ["date", "symbol", *factor_names, "label_exit_date", label_column]
    ].copy()
    prepared["date"] = pd.to_datetime(prepared["date"], errors="coerce")
    if prepared["date"].isna().any():
        raise ValueError("预计算因子训练数据包含无效日期")
    prepared["label_exit_date"] = pd.to_datetime(
        prepared["label_exit_date"], errors="coerce"
    )
    if prepared["label_exit_date"].isna().any():
        raise ValueError("预计算因子训练数据包含无效标签退出日期")
    return prepared


def _normalize_date(value: date | pd.Timestamp | str, field_name: str) -> pd.Timestamp:
    normalized = pd.to_datetime(value, errors="coerce")
    if pd.isna(normalized):
        raise ValueError(f"{field_name} 必须是有效日期")
    return pd.Timestamp(normalized).normalize()


def _validate_training_parameters(
    *,
    minimum_history_days: int,
    winsorize_lower: float,
    winsorize_upper: float,
    label_horizon_days: int,
    ridge_alpha: float,
    max_iterations: int,
    minimum_training_observations: int,
    minimum_training_dates: int,
) -> None:
    if (
        isinstance(minimum_history_days, bool)
        or not isinstance(minimum_history_days, int)
        or minimum_history_days < 0
    ):
        raise ValueError("minimum_history_days 必须是非负整数")
    if (
        isinstance(winsorize_lower, bool)
        or isinstance(winsorize_upper, bool)
        or not isinstance(winsorize_lower, (int, float))
        or not isinstance(winsorize_upper, (int, float))
        or not math.isfinite(winsorize_lower)
        or not math.isfinite(winsorize_upper)
        or not 0 <= winsorize_lower < winsorize_upper <= 1
    ):
        raise ValueError(
            "winsorize_lower 和 winsorize_upper 必须满足 0 <= lower < upper <= 1"
        )
    if (
        isinstance(label_horizon_days, bool)
        or not isinstance(label_horizon_days, int)
        or label_horizon_days <= 0
    ):
        raise ValueError("label_horizon_days 必须是正整数")
    if (
        isinstance(ridge_alpha, bool)
        or not isinstance(ridge_alpha, (int, float))
        or not math.isfinite(ridge_alpha)
        or ridge_alpha < 0
    ):
        raise ValueError("ridge_alpha 必须是非负有限数字")
    for name, value in (
        ("max_iterations", max_iterations),
        ("minimum_training_observations", minimum_training_observations),
        ("minimum_training_dates", minimum_training_dates),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} 必须是正整数")
