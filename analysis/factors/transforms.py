import math
from collections.abc import Mapping

import pandas as pd


def _validate_factor_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if "date" not in frame.columns:
        raise ValueError("因子截面变换需要 date 字段")
    if column not in frame.columns:
        raise ValueError(f"因子截面变换缺少字段: {column}")
    return pd.to_numeric(frame[column], errors="coerce")


def rank_factor_cross_sectionally(
    frame: pd.DataFrame, column: str, higher_is_better: bool = True
) -> pd.Series:
    values = _validate_factor_column(frame, column)
    return values.groupby(frame["date"], sort=False).rank(
        method="average", pct=True, ascending=higher_is_better
    )


def winsorize_factor_cross_sectionally(
    frame: pd.DataFrame,
    column: str,
    lower_quantile: float = 0.05,
    upper_quantile: float = 0.95,
) -> pd.Series:
    if not 0 <= lower_quantile < upper_quantile <= 1:
        raise ValueError("缩尾分位数必须满足 0 <= lower < upper <= 1")
    values = _validate_factor_column(frame, column)
    grouped = values.groupby(frame["date"], sort=False)
    lower = grouped.transform(lambda group: group.quantile(lower_quantile))
    upper = grouped.transform(lambda group: group.quantile(upper_quantile))
    return values.clip(lower=lower, upper=upper)


def standardize_factor_cross_sectionally(frame: pd.DataFrame, column: str) -> pd.Series:
    values = _validate_factor_column(frame, column)
    grouped = values.groupby(frame["date"], sort=False)
    means = grouped.transform("mean")
    standard_deviations = grouped.transform(lambda group: group.std(ddof=0))
    standardized = (values - means) / standard_deviations
    return standardized.where(
        values.isna(), standardized.mask(standard_deviations.eq(0), 0.0)
    )


def combine_factor_scores(
    frame: pd.DataFrame, factor_weights: Mapping[str, float]
) -> pd.Series:
    if not factor_weights:
        raise ValueError("因子权重不能为空")

    score = pd.Series(0.0, index=frame.index)
    total_weight = 0.0
    for column, weight in factor_weights.items():
        if column not in frame.columns:
            raise ValueError(f"因子合成缺少字段: {column}")
        if isinstance(weight, bool) or not isinstance(weight, (int, float)):
            raise ValueError(f"因子 {column} 的权重必须是数字")
        if not math.isfinite(weight):
            raise ValueError(f"因子 {column} 的权重必须是有限数字")
        score = score + pd.to_numeric(frame[column], errors="coerce") * weight
        total_weight += abs(weight)
    if total_weight == 0:
        raise ValueError("因子权重不能全部为零")
    return score


def filter_valid_factor_rows(
    frame: pd.DataFrame, factor_columns: tuple[str, ...]
) -> pd.DataFrame:
    missing = set(factor_columns) - set(frame.columns)
    if missing:
        raise ValueError(f"因子过滤缺少字段: {', '.join(sorted(missing))}")
    return frame.dropna(subset=list(factor_columns)).copy()
