from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass

import pandas as pd

FACTOR_RESULT_COLUMNS = ("date", "symbol", "value")
FACTOR_INPUT_SOURCES = frozenset({"valuation", "kline", "indicator"})


@dataclass(frozen=True)
class FactorInput:
    """A source column required by a factor calculation."""

    alias: str
    source: str
    source_name: str | None = None

    def __post_init__(self):
        if not self.alias:
            raise ValueError("因子输入别名不能为空")
        if self.source not in FACTOR_INPUT_SOURCES:
            available = ", ".join(sorted(FACTOR_INPUT_SOURCES))
            raise ValueError(f"不支持的因子输入来源: {self.source} (可选: {available})")
        if self.source == "indicator" and not self.source_name:
            raise ValueError("财务指标因子输入必须声明 source_name")


@dataclass(frozen=True)
class FactorMetadata:
    name: str
    version: str
    description: str
    inputs: tuple[FactorInput, ...]
    lookback_days: int
    higher_is_better: bool

    def __post_init__(self):
        if not self.name:
            raise ValueError("因子名称不能为空")
        if not self.version:
            raise ValueError(f"因子 {self.name} 的版本不能为空")
        if self.lookback_days < 0:
            raise ValueError(f"因子 {self.name} 的 lookback_days 不能为负数")
        aliases = [field.alias for field in self.inputs]
        if len(aliases) != len(set(aliases)):
            raise ValueError(f"因子 {self.name} 的输入别名不能重复")

    @property
    def required_columns(self) -> tuple[str, ...]:
        return tuple(field.alias for field in self.inputs)


class FactorDefinition(ABC):
    metadata: FactorMetadata

    def get_lookback_days(self, parameters: Mapping[str, object] | None = None) -> int:
        if parameters:
            raise ValueError(
                f"因子 {self.metadata.name} 不支持参数: "
                + ", ".join(sorted(parameters))
            )
        return self.metadata.lookback_days

    @abstractmethod
    def compute(
        self,
        data: pd.DataFrame,
        parameters: Mapping[str, object] | None = None,
    ) -> pd.DataFrame:
        """Return date/symbol/value rows without accessing external state."""


def validate_factor_input(data: pd.DataFrame, required_columns: tuple[str, ...]):
    required = {"date", "symbol", *required_columns}
    missing = required - set(data.columns)
    if missing:
        raise ValueError(f"因子输入缺少字段: {', '.join(sorted(missing))}")

    normalized = data.copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    if normalized["date"].isna().any():
        raise ValueError("因子输入包含无效日期")
    if normalized[["date", "symbol"]].isna().any().any():
        raise ValueError("因子输入的 date 和 symbol 不能为空")
    if normalized.duplicated(["date", "symbol"]).any():
        raise ValueError("因子输入不能包含重复的 date/symbol")
    return normalized


def validate_factor_result(result: pd.DataFrame, factor_name: str) -> pd.DataFrame:
    missing = set(FACTOR_RESULT_COLUMNS) - set(result.columns)
    if missing:
        raise ValueError(
            f"因子 {factor_name} 输出缺少字段: {', '.join(sorted(missing))}"
        )

    validated = result.loc[:, FACTOR_RESULT_COLUMNS].copy()
    validated["date"] = pd.to_datetime(validated["date"], errors="coerce")
    if validated["date"].isna().any():
        raise ValueError(f"因子 {factor_name} 输出包含无效日期")
    if validated[["date", "symbol"]].isna().any().any():
        raise ValueError(f"因子 {factor_name} 输出的 date 和 symbol 不能为空")
    if validated.duplicated(["date", "symbol"]).any():
        raise ValueError(f"因子 {factor_name} 输出不能包含重复的 date/symbol")
    validated["value"] = pd.to_numeric(validated["value"], errors="coerce")
    return validated.sort_values(["date", "symbol"]).reset_index(drop=True)
