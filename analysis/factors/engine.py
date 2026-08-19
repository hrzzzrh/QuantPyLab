from collections.abc import Mapping, Sequence

import pandas as pd

from analysis.factors.base import validate_factor_input, validate_factor_result
from analysis.factors.registry import get_factor_definition


class FactorEngine:
    """Calculate registered factors from one point-in-time input frame."""

    @staticmethod
    def get_required_columns(factor_names: Sequence[str]) -> tuple[str, ...]:
        columns = set()
        for factor_name in factor_names:
            columns.update(get_factor_definition(factor_name).metadata.required_columns)
        return tuple(sorted(columns))

    @staticmethod
    def get_max_lookback_days(
        factor_names: Sequence[str],
        parameters: Mapping[str, Mapping[str, object]] | None = None,
    ) -> int:
        parameter_map = parameters or {}
        return max(
            (
                get_factor_definition(name).get_lookback_days(
                    parameter_map.get(name, {})
                )
                for name in factor_names
            ),
            default=0,
        )

    def calculate(
        self,
        data: pd.DataFrame,
        factor_names: Sequence[str],
        parameters: Mapping[str, Mapping[str, object]] | None = None,
    ) -> pd.DataFrame:
        names = tuple(dict.fromkeys(factor_names))
        if not names:
            raise ValueError("至少需要指定一个因子")

        normalized = validate_factor_input(data, self.get_required_columns(names))
        factor_frame = normalized.loc[:, ["date", "symbol"]].copy()
        parameter_map = parameters or {}

        for factor_name in names:
            definition = get_factor_definition(factor_name)
            factor_input = validate_factor_input(
                normalized, definition.metadata.required_columns
            )
            factor_parameters = parameter_map.get(factor_name, {})
            if not isinstance(factor_parameters, Mapping):
                raise ValueError(f"因子 {factor_name} 的参数必须是映射")
            result = validate_factor_result(
                definition.compute(factor_input, factor_parameters), factor_name
            )
            factor_frame = factor_frame.merge(
                result.rename(columns={"value": factor_name}),
                on=["date", "symbol"],
                how="left",
                validate="one_to_one",
            )

        return factor_frame.sort_values(["date", "symbol"]).reset_index(drop=True)
