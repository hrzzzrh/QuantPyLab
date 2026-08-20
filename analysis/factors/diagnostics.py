"""Point-in-time factor diagnostics and report generation."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from itertools import combinations
from math import ceil, isfinite
from pathlib import Path

import pandas as pd

from analysis.factors.registry import get_factor_definition


@dataclass(frozen=True)
class FactorDiagnosticReport:
    """Tabular outputs produced by one factor diagnostic run."""

    summary: pd.DataFrame
    coverage: pd.DataFrame
    daily_rank_ic: pd.DataFrame
    quantile_returns: pd.DataFrame
    turnover: pd.DataFrame
    signal_autocorrelation: pd.DataFrame
    factor_correlation: pd.DataFrame


def calculate_forward_returns(
    data: pd.DataFrame, horizons: Sequence[int] = (1,)
) -> pd.DataFrame:
    """Calculate returns from the next trading-day open to a later close.

    A signal is generated after the current day's close. For a one-day horizon,
    the return is next_close / next_open - 1. For a longer horizon, the entry
    remains the next trading-day open and the exit is the close after horizon
    trading observations.
    """

    normalized = _normalize_price_input(data)
    validated_horizons = _validate_horizons(horizons)
    result = normalized.loc[:, ["date", "symbol"]].copy()
    grouped = normalized.groupby("symbol", sort=False)
    next_open = grouped["open_hfq"].shift(-1)

    for horizon in validated_horizons:
        exit_close = grouped["close_hfq"].shift(-horizon)
        forward_return = exit_close / next_open - 1
        valid_prices = next_open.gt(0) & exit_close.gt(0)
        result[f"forward_return_{horizon}d"] = (
            forward_return.where(valid_prices)
            .replace([float("inf"), float("-inf")], pd.NA)
            .astype("Float64")
        )
    return result


def calculate_factor_diagnostics(
    data: pd.DataFrame,
    factor_names: Sequence[str],
    horizons: Sequence[int] = (1, 5, 20),
    quantile_count: int = 5,
    signal_start_date: date | pd.Timestamp | str | None = None,
    signal_end_date: date | pd.Timestamp | str | None = None,
) -> FactorDiagnosticReport:
    """Calculate coverage, predictive, stability and correlation tables.

    The input may include lookback rows before signal_start_date and
    forward-price rows after signal_end_date. Those rows support turnover and
    forward-return calculations but are excluded from the reported window.
    """

    names = _validate_factor_names(factor_names)
    validated_horizons = _validate_horizons(horizons)
    quantile_count = _validate_quantile_count(quantile_count)
    normalized = _normalize_factor_input(data, names)
    start_date = _normalize_optional_date(signal_start_date, "signal_start_date")
    end_date = _normalize_optional_date(signal_end_date, "signal_end_date")
    signal_frame = _filter_signal_window(normalized, start_date, end_date)
    if signal_frame.empty:
        raise ValueError("指定信号区间没有可用的因子输入")

    with_forward_returns = normalized.merge(
        calculate_forward_returns(normalized, validated_horizons),
        on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    )
    signal_with_returns = _filter_signal_window(
        with_forward_returns, start_date, end_date
    )

    coverage = pd.concat(
        [_calculate_coverage(signal_frame, factor_name) for factor_name in names],
        ignore_index=True,
    )
    summary_rows = []
    rank_ic_tables = []
    quantile_tables = []
    turnover_tables = []
    autocorrelation_tables = []

    for factor_name in names:
        higher_is_better = get_factor_definition(factor_name).metadata.higher_is_better
        valid_factor_count = int(signal_frame[factor_name].notna().sum())
        total_observation_count = len(signal_frame)
        factor_coverage_rate = (
            valid_factor_count / total_observation_count
            if total_observation_count
            else None
        )

        turnover_tables.append(
            _calculate_turnover(
                normalized,
                factor_name,
                higher_is_better,
                top_fraction=1 / quantile_count,
                signal_start_date=start_date,
                signal_end_date=end_date,
            )
        )
        autocorrelation_tables.append(
            _calculate_signal_autocorrelation(
                normalized,
                factor_name,
                signal_start_date=start_date,
                signal_end_date=end_date,
            )
        )

        for horizon in validated_horizons:
            return_column = f"forward_return_{horizon}d"
            rank_ic = _calculate_daily_rank_ic(
                signal_with_returns, factor_name, return_column, horizon
            )
            quantile_returns = _calculate_quantile_returns(
                signal_with_returns,
                factor_name,
                return_column,
                horizon,
                quantile_count,
                higher_is_better,
            )
            rank_ic_tables.append(rank_ic)
            quantile_tables.append(quantile_returns)

            valid_forward_count = int(
                signal_with_returns[[factor_name, return_column]].dropna().shape[0]
            )
            forward_coverage_rate = (
                valid_forward_count / valid_factor_count if valid_factor_count else None
            )
            valid_rank_ic = rank_ic["rank_ic"].dropna()
            oriented_rank_ic = valid_rank_ic * (1 if higher_is_better else -1)
            rank_ic_mean = _optional_float(valid_rank_ic.mean())
            rank_ic_std = _optional_float(valid_rank_ic.std(ddof=0))
            rank_ic_ir = (
                rank_ic_mean / rank_ic_std
                if rank_ic_mean is not None
                and rank_ic_std is not None
                and rank_ic_std > 0
                else None
            )
            positive_ratio = _optional_float(
                (oriented_rank_ic > 0).mean() if not oriented_rank_ic.empty else None
            )
            weak_return, strong_return = _extract_preferred_quantile_returns(
                quantile_returns, quantile_count
            )
            summary_rows.append(
                {
                    "factor": factor_name,
                    "horizon_days": horizon,
                    "signal_date_count": int(signal_frame["date"].nunique()),
                    "total_observation_count": total_observation_count,
                    "valid_factor_observation_count": valid_factor_count,
                    "factor_coverage_rate": factor_coverage_rate,
                    "valid_forward_observation_count": valid_forward_count,
                    "forward_return_coverage_rate": forward_coverage_rate,
                    "rank_ic_observation_days": int(len(valid_rank_ic)),
                    "mean_rank_ic": rank_ic_mean,
                    "oriented_mean_rank_ic": _optional_float(oriented_rank_ic.mean()),
                    "rank_ic_std": rank_ic_std,
                    "rank_ic_ir": rank_ic_ir,
                    "positive_oriented_rank_ic_ratio": positive_ratio,
                    "weak_quantile_mean_return": weak_return,
                    "strong_quantile_mean_return": strong_return,
                    "preferred_quantile_spread": (
                        strong_return - weak_return
                        if weak_return is not None and strong_return is not None
                        else None
                    ),
                }
            )

    factor_correlation = _calculate_factor_correlation(signal_frame, names)
    return FactorDiagnosticReport(
        summary=pd.DataFrame(summary_rows),
        coverage=coverage,
        daily_rank_ic=_concat_or_empty(
            rank_ic_tables,
            ["factor", "horizon_days", "date", "rank_ic", "observation_count"],
        ),
        quantile_returns=_concat_or_empty(
            quantile_tables,
            [
                "factor",
                "horizon_days",
                "date",
                "quantile",
                "mean_return",
                "observation_count",
            ],
        ),
        turnover=_concat_or_empty(
            turnover_tables,
            [
                "factor",
                "date",
                "turnover",
                "selected_count",
                "universe_count",
            ],
        ),
        signal_autocorrelation=_concat_or_empty(
            autocorrelation_tables,
            ["factor", "date", "rank_autocorrelation", "observation_count"],
        ),
        factor_correlation=factor_correlation,
    )


def write_factor_diagnostic_report(
    report: FactorDiagnosticReport,
    output_dir: str | Path,
    parameters: dict | None = None,
) -> Path:
    """Write diagnostic tables and a concise Markdown summary."""

    import json

    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=False)
    tables = {
        "summary": report.summary,
        "coverage": report.coverage,
        "daily_rank_ic": report.daily_rank_ic,
        "quantile_returns": report.quantile_returns,
        "turnover": report.turnover,
        "signal_autocorrelation": report.signal_autocorrelation,
        "factor_correlation": report.factor_correlation,
    }
    for name, table in tables.items():
        table.to_csv(target_dir / f"{name}.csv", index=False)

    payload = dict(parameters or {})
    (target_dir / "parameters.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    (target_dir / "summary.md").write_text(
        _render_summary_markdown(report.summary, payload), encoding="utf-8"
    )
    return target_dir


def _validate_factor_names(factor_names: Sequence[str]) -> tuple[str, ...]:
    if isinstance(factor_names, str):
        raise ValueError("factor_names 必须是因子名称序列")
    names = tuple(dict.fromkeys(factor_names))
    if not names:
        raise ValueError("至少需要指定一个因子")
    for name in names:
        if not isinstance(name, str) or not name:
            raise ValueError("因子名称必须是非空字符串")
        get_factor_definition(name)
    return names


def _validate_horizons(horizons: Sequence[int]) -> tuple[int, ...]:
    if isinstance(horizons, (str, bytes)):
        raise ValueError("horizons 必须是正整数序列")
    values = tuple(dict.fromkeys(horizons))
    if not values:
        raise ValueError("至少需要指定一个持有期")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
        for value in values
    ):
        raise ValueError("持有期必须是正整数")
    return tuple(sorted(values))


def _validate_quantile_count(quantile_count: int) -> int:
    if (
        isinstance(quantile_count, bool)
        or not isinstance(quantile_count, int)
        or quantile_count < 2
    ):
        raise ValueError("quantile_count 必须是大于等于 2 的整数")
    return quantile_count


def _normalize_price_input(data: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "symbol", "open_hfq", "close_hfq"}
    missing = required - set(data.columns)
    if missing:
        raise ValueError(f"收益诊断输入缺少字段: {', '.join(sorted(missing))}")
    normalized = data.loc[:, ["date", "symbol", "open_hfq", "close_hfq"]].copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    if normalized["date"].isna().any():
        raise ValueError("收益诊断输入包含无效日期")
    if normalized[["date", "symbol"]].isna().any().any():
        raise ValueError("收益诊断输入的 date 和 symbol 不能为空")
    if normalized.duplicated(["date", "symbol"]).any():
        raise ValueError("收益诊断输入不能包含重复的 date/symbol")
    for column in ("open_hfq", "close_hfq"):
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    return normalized.sort_values(["symbol", "date"]).reset_index(drop=True)


def _normalize_factor_input(
    data: pd.DataFrame, factor_names: tuple[str, ...]
) -> pd.DataFrame:
    required = {"date", "symbol", "open_hfq", "close_hfq", *factor_names}
    missing = required - set(data.columns)
    if missing:
        raise ValueError(f"因子诊断输入缺少字段: {', '.join(sorted(missing))}")
    normalized = data.loc[
        :, ["date", "symbol", "open_hfq", "close_hfq", *factor_names]
    ].copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    if normalized["date"].isna().any():
        raise ValueError("因子诊断输入包含无效日期")
    if normalized[["date", "symbol"]].isna().any().any():
        raise ValueError("因子诊断输入的 date 和 symbol 不能为空")
    if normalized.duplicated(["date", "symbol"]).any():
        raise ValueError("因子诊断输入不能包含重复的 date/symbol")
    for column in ("open_hfq", "close_hfq", *factor_names):
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    return normalized.sort_values(["symbol", "date"]).reset_index(drop=True)


def _normalize_optional_date(value, name: str) -> pd.Timestamp | None:
    if value is None:
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"{name} 不是有效日期")
    return pd.Timestamp(parsed).normalize()


def _filter_signal_window(
    frame: pd.DataFrame,
    start_date: pd.Timestamp | None,
    end_date: pd.Timestamp | None,
) -> pd.DataFrame:
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("signal_start_date 不能晚于 signal_end_date")
    mask = pd.Series(True, index=frame.index)
    if start_date is not None:
        mask &= frame["date"] >= start_date
    if end_date is not None:
        mask &= frame["date"] <= end_date
    return frame.loc[mask].copy()


def _calculate_coverage(frame: pd.DataFrame, factor_name: str) -> pd.DataFrame:
    total = frame.groupby("date", sort=True).size().rename("total_observation_count")
    valid = (
        frame.dropna(subset=[factor_name])
        .groupby("date", sort=True)
        .size()
        .rename("valid_observation_count")
    )
    result = pd.concat([total, valid], axis=1).fillna(0).reset_index()
    result["valid_observation_count"] = result["valid_observation_count"].astype(int)
    result["coverage_rate"] = (
        result["valid_observation_count"] / result["total_observation_count"]
    ).where(result["total_observation_count"] > 0)
    result.insert(1, "factor", factor_name)
    return result.loc[
        :,
        [
            "factor",
            "date",
            "total_observation_count",
            "valid_observation_count",
            "coverage_rate",
        ],
    ]


def _calculate_daily_rank_ic(
    frame: pd.DataFrame,
    factor_name: str,
    return_column: str,
    horizon: int,
) -> pd.DataFrame:
    rows = []
    for current_date, group in frame.groupby("date", sort=True):
        valid = group[[factor_name, return_column]].dropna()
        rows.append(
            {
                "factor": factor_name,
                "horizon_days": horizon,
                "date": current_date,
                "rank_ic": _rank_correlation(valid[factor_name], valid[return_column]),
                "observation_count": int(len(valid)),
            }
        )
    return pd.DataFrame(
        rows,
        columns=["factor", "horizon_days", "date", "rank_ic", "observation_count"],
    )


def _calculate_quantile_returns(
    frame: pd.DataFrame,
    factor_name: str,
    return_column: str,
    horizon: int,
    quantile_count: int,
    higher_is_better: bool,
) -> pd.DataFrame:
    columns = [
        "factor",
        "horizon_days",
        "date",
        "quantile",
        "mean_return",
        "observation_count",
    ]
    valid = frame[["date", "symbol", factor_name, return_column]].dropna().copy()
    if valid.empty:
        return pd.DataFrame(columns=columns)
    valid = valid.sort_values(["date", "symbol"])
    factor_rank = valid.groupby("date", sort=False)[factor_name].rank(
        method="first", ascending=higher_is_better
    )
    group_size = valid.groupby("date", sort=False)["symbol"].transform("size")
    valid["quantile"] = (((factor_rank - 1) * quantile_count) // group_size + 1).astype(
        int
    )
    result = (
        valid.groupby(["date", "quantile"], sort=True)
        .agg(
            mean_return=(return_column, "mean"),
            observation_count=(return_column, "size"),
        )
        .reset_index()
    )
    result.insert(0, "horizon_days", horizon)
    result.insert(0, "factor", factor_name)
    return result.loc[:, columns]


def _calculate_turnover(
    frame: pd.DataFrame,
    factor_name: str,
    higher_is_better: bool,
    top_fraction: float,
    signal_start_date: pd.Timestamp | None,
    signal_end_date: pd.Timestamp | None,
) -> pd.DataFrame:
    rows = []
    previous_symbols: set[str] | None = None
    for current_date, group in frame.groupby("date", sort=True):
        valid = group.dropna(subset=[factor_name]).sort_values(
            [factor_name, "symbol"], ascending=[not higher_is_better, True]
        )
        if valid.empty:
            continue
        selected_count = max(1, ceil(len(valid) * top_fraction))
        selected_symbols = set(valid.head(selected_count)["symbol"])
        turnover = None
        if previous_symbols is not None:
            turnover = 1 - len(previous_symbols & selected_symbols) / max(
                len(previous_symbols), len(selected_symbols)
            )
        if (signal_start_date is None or current_date >= signal_start_date) and (
            signal_end_date is None or current_date <= signal_end_date
        ):
            rows.append(
                {
                    "factor": factor_name,
                    "date": current_date,
                    "turnover": turnover,
                    "selected_count": len(selected_symbols),
                    "universe_count": len(valid),
                }
            )
        previous_symbols = selected_symbols
    return pd.DataFrame(
        rows,
        columns=[
            "factor",
            "date",
            "turnover",
            "selected_count",
            "universe_count",
        ],
    )


def _calculate_signal_autocorrelation(
    frame: pd.DataFrame,
    factor_name: str,
    signal_start_date: pd.Timestamp | None,
    signal_end_date: pd.Timestamp | None,
) -> pd.DataFrame:
    rows = []
    dates = list(frame["date"].drop_duplicates().sort_values())
    for previous_date, current_date in zip(dates, dates[1:], strict=False):
        previous = frame.loc[
            frame["date"].eq(previous_date), ["symbol", factor_name]
        ].rename(columns={factor_name: "previous_value"})
        current = frame.loc[
            frame["date"].eq(current_date), ["symbol", factor_name]
        ].rename(columns={factor_name: "current_value"})
        merged = current.merge(previous, on="symbol", how="inner").dropna()
        if (signal_start_date is None or current_date >= signal_start_date) and (
            signal_end_date is None or current_date <= signal_end_date
        ):
            rows.append(
                {
                    "factor": factor_name,
                    "date": current_date,
                    "rank_autocorrelation": _rank_correlation(
                        merged["previous_value"], merged["current_value"]
                    ),
                    "observation_count": int(len(merged)),
                }
            )
    return pd.DataFrame(
        rows,
        columns=["factor", "date", "rank_autocorrelation", "observation_count"],
    )


def _calculate_factor_correlation(
    frame: pd.DataFrame, factor_names: tuple[str, ...]
) -> pd.DataFrame:
    columns = [
        "date",
        "factor_a",
        "factor_b",
        "spearman_correlation",
        "observation_count",
    ]
    rows = []
    for current_date, group in frame.groupby("date", sort=True):
        for factor_a, factor_b in combinations(factor_names, 2):
            valid = group[[factor_a, factor_b]].dropna()
            rows.append(
                {
                    "date": current_date,
                    "factor_a": factor_a,
                    "factor_b": factor_b,
                    "spearman_correlation": _rank_correlation(
                        valid[factor_a], valid[factor_b]
                    ),
                    "observation_count": int(len(valid)),
                }
            )
    return pd.DataFrame(rows, columns=columns)


def _rank_correlation(left: pd.Series, right: pd.Series) -> float | None:
    if len(left) < 2 or left.nunique() < 2 or right.nunique() < 2:
        return None
    correlation = left.rank(method="average").corr(right.rank(method="average"))
    if correlation is None or pd.isna(correlation) or not isfinite(float(correlation)):
        return None
    return float(correlation)


def _extract_preferred_quantile_returns(
    quantile_returns: pd.DataFrame, quantile_count: int
) -> tuple[float | None, float | None]:
    if quantile_returns.empty:
        return None, None
    grouped = quantile_returns.groupby("quantile")["mean_return"].mean()
    weak = grouped.get(1)
    strong = grouped.get(quantile_count)
    return _optional_float(weak), _optional_float(strong)


def _optional_float(value) -> float | None:
    if value is None or pd.isna(value):
        return None
    numeric = float(value)
    return numeric if isfinite(numeric) else None


def _concat_or_empty(frames: list[pd.DataFrame], columns: list[str]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True).loc[:, columns]


def _render_summary_markdown(
    summary: pd.DataFrame, parameters: Mapping[str, object]
) -> str:
    lines = ["# 因子诊断摘要", ""]
    if parameters:
        lines.extend(["## 运行参数", ""])
        import json

        lines.append(
            json.dumps(dict(parameters), ensure_ascii=False, indent=2, default=str)
        )
        lines.append("")
    if summary.empty:
        lines.extend(["没有可用的诊断结果。", ""])
        return "\n".join(lines)
    lines.extend(
        [
            "## 汇总",
            "",
            "| 因子 | 持有期 | 因子覆盖率 | 平均 Rank IC | 方向调整 Rank IC | Rank IC IR | 偏好分位差 |",
            "|:---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in summary.itertuples(index=False):
        lines.append(
            f"| {row.factor} | {row.horizon_days} | "
            f"{_format_metric(row.factor_coverage_rate)} | "
            f"{_format_metric(row.mean_rank_ic)} | "
            f"{_format_metric(row.oriented_mean_rank_ic)} | "
            f"{_format_metric(row.rank_ic_ir)} | "
            f"{_format_metric(row.preferred_quantile_spread)} |"
        )
    lines.extend(
        [
            "",
            "详细数据见同目录下的 coverage.csv、daily_rank_ic.csv、",
            "quantile_returns.csv、turnover.csv、",
            "signal_autocorrelation.csv 和 factor_correlation.csv。",
            "",
        ]
    )
    return "\n".join(lines)


def _format_metric(value) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):.4f}"
