"""Fit explainable factor-composite weights on point-in-time observations."""

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date

import pandas as pd

from analysis.factors import FactorEngine
from analysis.factors.registry import get_factor_definition
from analysis.factors.transforms import (
    filter_valid_factor_rows,
    rank_factor_cross_sectionally,
    winsorize_factor_cross_sectionally,
)
from backtest.strategy_base import get_month_end_dates

TRAINING_SYMBOL_BATCH_SIZE = 125


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
    target_transform: str = "cross_sectional_percentile_rank_demeaned"
    sample_weighting: str = "equal_signal_date"
    weight_constraint: str = "nonnegative_sum_to_one"
    prior_factor_weights: dict[str, float] | None = None

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
            "target_transform": self.target_transform,
            "sample_weighting": self.sample_weighting,
            "weight_constraint": self.weight_constraint,
            "prior_factor_weights": (
                dict(self.prior_factor_weights)
                if self.prior_factor_weights is not None
                else None
            ),
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

    normalized = _normalize_training_input(
        data,
        FactorEngine.get_required_columns(names),
    )
    label_column = f"forward_return_{label_horizon_days}d"
    signal_dates = get_month_end_dates(normalized["date"])
    signal_dates = signal_dates[
        (signal_dates >= start_date) & (signal_dates <= end_date)
    ]
    factor_frame = FactorEngine().calculate_factors_on_dates(
        normalized,
        names,
        parameters,
        signal_dates,
        symbol_batch_size=TRAINING_SYMBOL_BATCH_SIZE,
    )

    # Build labels and listing-day counts one symbol batch at a time. The old
    # path retained a full-market factor frame and a second full-market label
    # frame simultaneously, which caused avoidable multi-gigabyte RSS spikes.
    symbols = normalized["symbol"].drop_duplicates().tolist()
    candidate_frames = []
    for offset in range(0, len(symbols), TRAINING_SYMBOL_BATCH_SIZE):
        batch_symbols = symbols[offset : offset + TRAINING_SYMBOL_BATCH_SIZE]
        batch_mask = normalized["symbol"].isin(batch_symbols)
        label_data = normalized.loc[
            batch_mask,
            ["date", "symbol", "open_hfq", "close_hfq"],
        ].copy()
        label_data = label_data.sort_values(
            ["symbol", "date"], kind="mergesort"
        ).reset_index(drop=True)
        grouped_prices = label_data.groupby("symbol", sort=False)
        next_open = grouped_prices["open_hfq"].shift(-1)
        exit_close = grouped_prices["close_hfq"].shift(-label_horizon_days)
        label_data["label_exit_date"] = grouped_prices["date"].shift(
            -label_horizon_days
        )
        label_data["listing_days"] = grouped_prices.cumcount() + 1
        valid_prices = next_open.gt(0) & exit_close.gt(0)
        label_data[label_column] = (
            (exit_close / next_open - 1)
            .where(valid_prices)
            .replace([float("inf"), float("-inf")], pd.NA)
            .astype("Float64")
        )
        label_data = label_data.loc[
            label_data["date"].isin(signal_dates),
            [
                "date",
                "symbol",
                "label_exit_date",
                "listing_days",
                label_column,
            ],
        ]
        factor_batch = factor_frame.loc[factor_frame["symbol"].isin(batch_symbols)]
        batch_candidates = factor_batch.merge(
            label_data,
            on=["date", "symbol"],
            how="left",
            validate="one_to_one",
        )
        batch_candidates = batch_candidates[
            batch_candidates["label_exit_date"].notna()
            & (batch_candidates["label_exit_date"] <= end_date)
            & (batch_candidates["listing_days"] >= minimum_history_days)
        ]
        batch_candidates = filter_valid_factor_rows(batch_candidates, names)
        if not batch_candidates.empty:
            candidate_frames.append(
                batch_candidates.loc[
                    :, ["date", "symbol", *names, "label_exit_date", label_column]
                ].copy()
            )

    if not candidate_frames:
        return pd.DataFrame(
            columns=["date", "symbol", *names, "label_exit_date", label_column]
        )
    return (
        pd.concat(candidate_frames, ignore_index=True)
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )


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
    prior_factor_weights: Mapping[str, float] | None = None,
) -> FactorTrainingResult:
    """Fit prior-shrunk simplex weights from monthly point-in-time factor rows.

    Factors are transformed exactly as the factor-composite strategy transforms
    them: cross-sectional winsorization followed by a direction-aware
    percentile rank. The future-return target is independently ranked within
    each signal date and demeaned. Every signal date receives equal total
    objective weight. Fitted weights are constrained to the probability
    simplex and shrink toward the strategy's declared prior weights.
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
    normalized_prior = _normalize_prior_factor_weights(prior_factor_weights, names)

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
    target = training_frame.groupby("date", sort=False)[label_column].rank(
        method="average",
        pct=True,
    )
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

    observations_per_signal_date = training_frame.groupby("date", sort=False)[
        "date"
    ].transform("size")
    sample_weights = observations_per_signal_date.rdiv(1.0)
    weights, iterations, converged = _fit_prior_shrunk_simplex_ridge(
        training_frame.loc[:, list(names)],
        training_frame["target"],
        sample_weights,
        names,
        ridge_alpha,
        max_iterations,
        normalized_prior,
    )
    if not converged:
        raise ValueError(
            "单纯形 Ridge 训练未在 max_iterations 内收敛: "
            f"max_iterations={max_iterations}"
        )
    normalized_weights = weights.to_dict()
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
        prior_factor_weights=normalized_prior.to_dict(),
    )


def _fit_prior_shrunk_simplex_ridge(
    features: pd.DataFrame,
    target: pd.Series,
    sample_weights: pd.Series,
    factor_names: tuple[str, ...],
    ridge_alpha: float,
    max_iterations: int,
    prior_factor_weights: pd.Series,
) -> tuple[pd.Series, int, bool]:
    normalized_sample_weights = sample_weights.astype(float)
    total_sample_weight = float(normalized_sample_weights.sum())
    weighted_features = features.mul(normalized_sample_weights, axis=0)
    gram = features.T.dot(weighted_features).astype(float) / total_sample_weight
    covariance = (
        features.T.dot(target.astype(float) * normalized_sample_weights)
        / total_sample_weight
    )
    lipschitz_bound = float(gram.abs().sum(axis=1).max()) + ridge_alpha
    step_size = 1 / max(lipschitz_bound, 1e-12)
    weights = prior_factor_weights.astype("float64").copy()
    converged = False
    iterations = max_iterations

    for iteration in range(1, max_iterations + 1):
        gradient = (
            gram.dot(weights)
            - covariance
            + ridge_alpha * (weights - prior_factor_weights)
        )
        next_weights = pd.Series(
            _project_onto_probability_simplex(
                (weights - step_size * gradient).tolist()
            ),
            index=factor_names,
            dtype="float64",
        )
        if float((next_weights - weights).abs().max()) <= 1e-10:
            weights = next_weights
            iterations = iteration
            converged = True
            break
        weights = next_weights

    return weights, iterations, converged


def _project_onto_probability_simplex(values: Sequence[float]) -> list[float]:
    """Project finite values onto nonnegative weights that sum to one."""

    vector = [float(value) for value in values]
    if not vector or not all(math.isfinite(value) for value in vector):
        raise ValueError("单纯形投影输入必须是非空有限数字序列")
    sorted_values = sorted(vector, reverse=True)
    cumulative = 0.0
    threshold_index = 0
    for index, value in enumerate(sorted_values, start=1):
        cumulative += value
        threshold = (cumulative - 1.0) / index
        if value > threshold:
            threshold_index = index
    threshold = (sum(sorted_values[:threshold_index]) - 1.0) / threshold_index
    projected = [max(value - threshold, 0.0) for value in vector]
    total = sum(projected)
    return [value / total for value in projected]


def _normalize_prior_factor_weights(
    prior_factor_weights: Mapping[str, float] | None,
    factor_names: tuple[str, ...],
) -> pd.Series:
    if prior_factor_weights is None:
        return pd.Series(1.0 / len(factor_names), index=factor_names, dtype="float64")
    unknown = set(prior_factor_weights) - set(factor_names)
    missing = set(factor_names) - set(prior_factor_weights)
    if unknown or missing:
        details = []
        if missing:
            details.append("缺少 " + ", ".join(sorted(missing)))
        if unknown:
            details.append("未知 " + ", ".join(sorted(unknown)))
        raise ValueError(
            "prior_factor_weights 必须完整覆盖训练因子: " + "; ".join(details)
        )
    normalized = pd.Series(
        {name: prior_factor_weights[name] for name in factor_names}, dtype="float64"
    )
    if not normalized.map(lambda value: math.isfinite(float(value))).all():
        raise ValueError("prior_factor_weights 必须是有限数字")
    if normalized.lt(0).any():
        raise ValueError("prior_factor_weights 不能为负")
    total = float(normalized.sum())
    if total <= 1e-12:
        raise ValueError("prior_factor_weights 权重和必须为正")
    return normalized / total


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


def _normalize_training_input(
    data: pd.DataFrame,
    required_factor_columns: Sequence[str] = (),
) -> pd.DataFrame:
    required = {"date", "symbol", "open_hfq", "close_hfq"}
    required.update(required_factor_columns)
    missing = required - set(data.columns)
    if missing:
        raise ValueError(f"因子训练输入缺少字段: {', '.join(sorted(missing))}")
    normalized_dates = pd.to_datetime(data["date"], errors="coerce")
    if normalized_dates.isna().any():
        raise ValueError("因子训练输入包含无效日期")
    if data[["date", "symbol"]].isna().any().any():
        raise ValueError("因子训练输入的 date 和 symbol 不能为空")
    if data.duplicated(["date", "symbol"]).any():
        raise ValueError("因子训练输入不能包含重复的 date/symbol")
    if pd.api.types.is_datetime64_any_dtype(data["date"]):
        # DataAccess already returns an owned, canonical datetime column. Do
        # not make a second multi-million-row copy merely to normalize it;
        # all downstream operations select their own working columns.
        return data
    normalized = data.copy()
    normalized["date"] = normalized_dates
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
