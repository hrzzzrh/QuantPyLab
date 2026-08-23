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
        factor_keys = factor_frame.loc[:, ["date", "symbol"]]

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
            if result.loc[:, ["date", "symbol"]].equals(factor_keys):
                # All built-in factors preserve the input key set. Assigning
                # by position avoids a full-size merge for every factor.
                factor_frame[factor_name] = result["value"].to_numpy(copy=False)
            else:
                # Keep the defensive keyed path for third-party definitions
                # that return the same rows in a different deterministic order.
                factor_frame = factor_frame.merge(
                    result.rename(columns={"value": factor_name}),
                    on=["date", "symbol"],
                    how="left",
                    validate="one_to_one",
                )
                factor_keys = factor_frame.loc[:, ["date", "symbol"]]

        return factor_frame.sort_values(["date", "symbol"]).reset_index(drop=True)

    def calculate_factors_on_dates(
        self,
        data: pd.DataFrame,
        factor_names: Sequence[str],
        parameters: Mapping[str, Mapping[str, object]] | None,
        output_dates: Sequence[object],
        *,
        symbol_batch_size: int = 500,
    ) -> pd.DataFrame:
        """Calculate factors for selected dates without materializing a full wide frame.

        Rolling factors still receive each symbol's complete history, but symbols
        are processed in bounded batches and only the requested output dates are
        retained. This is safe for the registered symbol-local factors; a future
        cross-sectional factor can opt out through ``FactorMetadata.symbol_local``.
        """

        names = tuple(dict.fromkeys(factor_names))
        if not names:
            raise ValueError("至少需要指定一个因子")
        if (
            isinstance(symbol_batch_size, bool)
            or not isinstance(symbol_batch_size, int)
            or symbol_batch_size <= 0
        ):
            raise ValueError("symbol_batch_size 必须是正整数")

        normalized_dates = pd.to_datetime(
            pd.Series(list(output_dates)), errors="coerce"
        )
        if normalized_dates.isna().any():
            raise ValueError("output_dates 必须全部是有效日期")
        target_dates = pd.DatetimeIndex(normalized_dates.drop_duplicates())
        if target_dates.empty:
            return pd.DataFrame(columns=["date", "symbol", *names])

        if not all(get_factor_definition(name).metadata.symbol_local for name in names):
            full_frame = self.calculate(data, names, parameters)
            return full_frame.loc[full_frame["date"].isin(target_dates)].reset_index(
                drop=True
            )

        required_columns = self.get_required_columns(names)
        required_input_columns = [
            "date",
            "symbol",
            *required_columns,
        ]
        missing = set(required_input_columns) - set(data.columns)
        if missing:
            raise ValueError(f"因子输入缺少字段: {', '.join(sorted(missing))}")

        key_frame = data.loc[:, ["date", "symbol"]].copy()
        key_frame["date"] = pd.to_datetime(key_frame["date"], errors="coerce")
        if key_frame["date"].isna().any():
            raise ValueError("因子输入包含无效日期")
        if key_frame[["date", "symbol"]].isna().any().any():
            raise ValueError("因子输入的 date 和 symbol 不能为空")
        if key_frame.duplicated(["date", "symbol"]).any():
            raise ValueError("因子输入不能包含重复的 date/symbol")

        symbols = key_frame["symbol"].drop_duplicates().tolist()
        frames = []
        for offset in range(0, len(symbols), symbol_batch_size):
            batch_symbols = symbols[offset : offset + symbol_batch_size]
            batch_mask = key_frame["symbol"].isin(batch_symbols)
            batch_data = data.loc[batch_mask, required_input_columns]
            batch_frame = self.calculate(batch_data, names, parameters)
            batch_frame = batch_frame.loc[batch_frame["date"].isin(target_dates)].copy()
            if not batch_frame.empty:
                frames.append(batch_frame)

        if not frames:
            return pd.DataFrame(columns=["date", "symbol", *names])
        return (
            pd.concat(frames, ignore_index=True)
            .sort_values(["date", "symbol"])
            .reset_index(drop=True)
        )
