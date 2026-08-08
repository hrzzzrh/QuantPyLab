from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd

from backtest.config import BacktestConfig
from backtest.data_access import BacktestDataAccess

TARGET_COLUMNS = ("date", "symbol", "score", "rank", "target_weight")


@dataclass(frozen=True)
class StrategyMetadata:
    name: str
    version: str
    description: str
    parameter_summary: str


class BacktestStrategy(ABC):
    metadata: StrategyMetadata

    @abstractmethod
    def validate_parameters(self, parameters: dict) -> dict:
        """Return resolved defaults and reject unsupported strategy parameters."""

    @abstractmethod
    def load_signal_data(
        self,
        data_access: BacktestDataAccess,
        config: BacktestConfig,
        parameters: dict,
    ) -> pd.DataFrame:
        """Load only the point-in-time data required to produce strategy signals."""

    @abstractmethod
    def build_targets(
        self,
        signal_data: pd.DataFrame,
        config: BacktestConfig,
        parameters: dict,
    ) -> pd.DataFrame:
        """Return standard target weights dated at the close of each signal day."""


def validate_target_weights(targets: pd.DataFrame) -> pd.DataFrame:
    missing_columns = set(TARGET_COLUMNS) - set(targets.columns)
    if missing_columns:
        raise ValueError(f"策略目标缺少字段: {', '.join(sorted(missing_columns))}")
    validated = targets.loc[:, TARGET_COLUMNS].copy()
    if validated.empty:
        return validated
    if validated.duplicated(["date", "symbol"]).any():
        raise ValueError("同一信号日不能为同一证券生成多个目标权重")
    if (
        validated["target_weight"].isna().any()
        or (validated["target_weight"] <= 0).any()
    ):
        raise ValueError("目标权重必须为正数")
    if (validated.groupby("date")["target_weight"].sum() > 1.000001).any():
        raise ValueError("同一信号日的目标权重之和不能超过 1")
    return validated.sort_values(["date", "rank", "symbol"])


def get_month_end_dates(dates: pd.Series) -> pd.DatetimeIndex:
    unique_dates = pd.DatetimeIndex(
        pd.to_datetime(dates.drop_duplicates()).sort_values()
    )
    return unique_dates.to_series().groupby(unique_dates.to_period("M")).max().values


def select_equal_weight_targets(
    candidates: pd.DataFrame, holding_count: int
) -> pd.DataFrame:
    targets = candidates[candidates["rank"] <= holding_count].copy()
    targets["target_weight"] = 1 / targets.groupby("date")["symbol"].transform("count")
    return targets.loc[:, TARGET_COLUMNS]
