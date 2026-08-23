"""Point-in-time liquidity and participation-capacity diagnostics."""

from __future__ import annotations

import gc
import json
import math
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from analysis.factors import FactorEngine
from analysis.factors.transforms import filter_valid_factor_rows
from backtest.config import BacktestConfig
from backtest.data_access import BacktestDataAccess
from backtest.engine import DailyBacktestEngine
from backtest.metrics import calculate_performance_metrics
from backtest.selection_comparison import summarize_trade_statistics
from backtest.strategy_base import (
    select_equal_weight_targets,
    validate_target_weights,
)
from backtest.strategy_registry import get_backtest_strategy
from backtest.trading_calendar import get_confirmed_month_end_trading_dates
from storage.database.manager import DBManager

DEFAULT_LIQUIDITY_LOOKBACK_DAYS = 20
DEFAULT_LIQUIDITY_BUCKET_COUNT = 5
DEFAULT_PARTICIPATION_LIMITS = (0.05, 0.10, 0.20)
FORMAL_STRATEGY_NAME = "multi-factor-quality-value-momentum"

LIQUIDITY_SNAPSHOT_COLUMNS = (
    "date",
    "symbol",
    "avg_amount",
    "market_cap",
    "amount_to_market_cap",
)
LIQUIDITY_SIGNAL_SUMMARY_COLUMNS = (
    "date",
    "candidate_count",
    "selected_count",
    "valid_amount_count",
    "amount_coverage_rate",
    "valid_market_cap_count",
    "market_cap_coverage_rate",
    "selected_valid_amount_count",
    "selected_amount_coverage_rate",
    "selected_valid_market_cap_count",
    "selected_market_cap_coverage_rate",
    "liquidity_bucket_eligible",
    "liquidity_bucket_valid_count",
    "avg_amount_median",
    "avg_amount_p10",
    "avg_amount_p90",
    "selected_avg_amount_median",
    "selected_avg_amount_p10",
    "selected_avg_amount_p90",
    "market_cap_median",
    "selected_market_cap_median",
    "amount_to_market_cap_median",
    "selected_amount_to_market_cap_median",
)
LIQUIDITY_BUCKET_COLUMNS = (
    "date",
    "liquidity_bucket",
    "bucket_count",
    "universe_count",
    "selected_count",
    "universe_share",
    "selected_share",
    "selection_lift",
    "universe_avg_amount_median",
    "selected_avg_amount_median",
    "valid_amount_count",
    "selected_valid_amount_count",
)
LIQUIDITY_TRADE_BASE_COLUMNS = (
    "date",
    "signal_date",
    "symbol",
    "side",
    "raw_open",
    "adjusted_open",
    "notional",
    "cost",
    "reason",
    "avg_amount",
    "market_cap",
    "amount_to_market_cap",
    "order_participation",
    "liquidity_status",
)
LIQUIDITY_TRADE_COLUMNS = (
    *LIQUIDITY_TRADE_BASE_COLUMNS,
    "capacity_capital_5pct",
    "capacity_capital_10pct",
    "capacity_capital_20pct",
)
LIQUIDITY_CAPACITY_SUMMARY_COLUMNS = (
    "participation_limit",
    "rebalance_order_count",
    "eligible_order_count",
    "missing_liquidity_order_count",
    "invalid_order_notional_count",
    "capacity_min",
    "capacity_p10",
    "capacity_median",
    "capacity_p90",
    "participation_median",
    "participation_p90",
    "max_order_participation",
    "status",
)
LIQUIDITY_BACKTEST_SUMMARY_COLUMNS = (
    "total_return",
    "annualized_return",
    "annualized_volatility",
    "sharpe_ratio",
    "max_drawdown",
    "trading_days",
    "cumulative_turnover",
    "total_transaction_cost",
    "executed_trade_count",
    "rebalance_day_count",
    "skipped_rebalance_count",
    "delist_count",
)


@dataclass(frozen=True)
class LiquidityCapacityReport:
    """Auditable tables for one formal strategy liquidity run."""

    signal_summary: pd.DataFrame
    buckets: pd.DataFrame
    trades: pd.DataFrame
    capacity_summary: pd.DataFrame
    backtest_summary: pd.DataFrame
    targets: pd.DataFrame
    daily_nav: pd.DataFrame


def calculate_trailing_liquidity(
    signal_data: pd.DataFrame,
    *,
    lookback_days: int = DEFAULT_LIQUIDITY_LOOKBACK_DAYS,
    output_dates: Sequence[object] | None = None,
) -> pd.DataFrame:
    """Calculate signal-date trailing amount and point-in-time market-cap fields.

    The rolling window is evaluated independently per symbol and includes the
    current row only.  It never reads an execution-date row.
    """

    lookback_days = _validate_positive_integer(lookback_days, "lookback_days")
    required = {"date", "symbol", "amount", "market_cap"}
    missing = required - set(signal_data.columns)
    if missing:
        raise ValueError("流动性快照数据缺少字段: " + ", ".join(sorted(missing)))
    normalized = signal_data.loc[:, ["date", "symbol", "amount", "market_cap"]].copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["symbol"] = normalized["symbol"].astype("string").str.strip()
    if (
        normalized["date"].isna().any()
        or normalized["symbol"].isna().any()
        or normalized["symbol"].eq("").any()
    ):
        raise ValueError("流动性快照的 date 和 symbol 必须有效")
    if normalized.duplicated(["date", "symbol"]).any():
        raise ValueError("流动性快照不能包含重复的 date/symbol")

    normalized = normalized.sort_values(
        ["symbol", "date"], kind="mergesort"
    ).reset_index(drop=True)
    amount = pd.to_numeric(normalized["amount"], errors="coerce")
    amount = amount.where(np.isfinite(amount) & amount.ge(0))
    normalized["amount"] = amount
    market_cap = pd.to_numeric(normalized["market_cap"], errors="coerce")
    normalized["market_cap"] = market_cap.where(
        np.isfinite(market_cap) & market_cap.gt(0)
    )
    rolling_amount = (
        normalized.groupby("symbol", sort=False)["amount"]
        .rolling(lookback_days, min_periods=lookback_days)
        .mean()
        .reset_index(level=0, drop=True)
    )
    normalized["avg_amount"] = rolling_amount
    normalized["amount_to_market_cap"] = (
        normalized["avg_amount"] / normalized["market_cap"]
    )
    normalized["amount_to_market_cap"] = normalized["amount_to_market_cap"].where(
        np.isfinite(normalized["amount_to_market_cap"])
    )
    result = (
        normalized.loc[:, LIQUIDITY_SNAPSHOT_COLUMNS]
        .sort_values(["date", "symbol"], kind="mergesort")
        .reset_index(drop=True)
    )
    if output_dates is not None:
        requested_dates = pd.DatetimeIndex(
            pd.to_datetime(list(output_dates), errors="coerce")
        )
        if requested_dates.isna().any():
            raise ValueError("流动性快照输出日期必须有效")
        result = result.loc[result["date"].isin(requested_dates)].reset_index(drop=True)
    return result


def build_formal_factor_candidates(
    signal_data: pd.DataFrame,
    factor_names: Sequence[str],
    config: BacktestConfig,
    parameters: Mapping[str, object],
) -> pd.DataFrame:
    """Rebuild the formal strategy's valid monthly candidate universe."""

    names = tuple(factor_names)
    if not names:
        raise ValueError("正式策略候选池至少需要一个因子")
    factor_frame = FactorEngine().calculate(
        signal_data,
        names,
        parameters.get("factor_parameters"),
    )
    ordered_input = signal_data.copy()
    ordered_input["date"] = pd.to_datetime(ordered_input["date"], errors="coerce")
    ordered_input = ordered_input.sort_values(["symbol", "date"], kind="mergesort")
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
        candidates["date"].isin(
            get_confirmed_month_end_trading_dates(candidates["date"])
        )
    ].copy()
    candidates = candidates[candidates["date"].dt.date >= config.start_date]
    candidates = candidates[
        candidates["listing_days"] >= int(parameters["min_listing_days"])
    ]
    candidates = filter_valid_factor_rows(candidates, names)
    return (
        candidates.loc[:, ["date", "symbol"]]
        .sort_values(["date", "symbol"], kind="mergesort")
        .reset_index(drop=True)
    )


def calculate_liquidity_signal_summary(
    candidates: pd.DataFrame,
    targets: pd.DataFrame,
    snapshots: pd.DataFrame,
    *,
    bucket_count: int = DEFAULT_LIQUIDITY_BUCKET_COUNT,
) -> pd.DataFrame:
    """Summarize point-in-time liquidity coverage for every signal date."""

    bucket_count = _validate_positive_integer(bucket_count, "bucket_count")
    candidate_keys = _normalize_keys(candidates, "流动性候选池")
    target_keys = _normalize_keys(targets, "流动性目标")
    snapshot = _normalize_snapshot(snapshots)
    _validate_target_membership(candidate_keys, target_keys)
    merged = candidate_keys.merge(
        snapshot,
        on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    )
    selected_keys = target_keys.assign(selected=True)
    merged = merged.merge(
        selected_keys,
        on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    )
    merged["selected"] = merged["selected"].fillna(False).astype(bool)
    candidate_counts = merged.groupby("date", sort=True).size()
    selected_counts = target_keys.groupby("date", sort=True).size()
    rows = []
    for signal_date, group in merged.groupby("date", sort=True):
        valid_amount = _valid_positive(group["avg_amount"])
        valid_market_cap = _valid_positive(group["market_cap"])
        selected_group = group[group["selected"]]
        selected_valid_amount = _valid_positive(selected_group["avg_amount"])
        selected_valid_market_cap = _valid_positive(selected_group["market_cap"])
        bucket_valid_count = int(valid_amount.sum())
        rows.append(
            {
                "date": signal_date.date().isoformat(),
                "candidate_count": int(candidate_counts.loc[signal_date]),
                "selected_count": int(selected_counts.get(signal_date, 0)),
                "valid_amount_count": int(valid_amount.sum()),
                "amount_coverage_rate": float(valid_amount.mean()),
                "valid_market_cap_count": int(valid_market_cap.sum()),
                "market_cap_coverage_rate": float(valid_market_cap.mean()),
                "selected_valid_amount_count": int(selected_valid_amount.sum()),
                "selected_amount_coverage_rate": _coverage_rate(
                    selected_valid_amount, len(selected_group)
                ),
                "selected_valid_market_cap_count": int(selected_valid_market_cap.sum()),
                "selected_market_cap_coverage_rate": _coverage_rate(
                    selected_valid_market_cap, len(selected_group)
                ),
                "liquidity_bucket_eligible": bucket_valid_count >= bucket_count,
                "liquidity_bucket_valid_count": bucket_valid_count,
                "avg_amount_median": _quantile(
                    group.loc[valid_amount, "avg_amount"], 0.5
                ),
                "avg_amount_p10": _quantile(group.loc[valid_amount, "avg_amount"], 0.1),
                "avg_amount_p90": _quantile(group.loc[valid_amount, "avg_amount"], 0.9),
                "selected_avg_amount_median": _quantile(
                    selected_group.loc[selected_valid_amount, "avg_amount"], 0.5
                ),
                "selected_avg_amount_p10": _quantile(
                    selected_group.loc[selected_valid_amount, "avg_amount"], 0.1
                ),
                "selected_avg_amount_p90": _quantile(
                    selected_group.loc[selected_valid_amount, "avg_amount"], 0.9
                ),
                "market_cap_median": _quantile(
                    group.loc[valid_market_cap, "market_cap"], 0.5
                ),
                "selected_market_cap_median": _quantile(
                    selected_group.loc[selected_valid_market_cap, "market_cap"], 0.5
                ),
                "amount_to_market_cap_median": _quantile(
                    group.loc[
                        _valid_positive(group["amount_to_market_cap"]),
                        "amount_to_market_cap",
                    ],
                    0.5,
                ),
                "selected_amount_to_market_cap_median": _quantile(
                    selected_group.loc[
                        _valid_positive(selected_group["amount_to_market_cap"]),
                        "amount_to_market_cap",
                    ],
                    0.5,
                ),
            }
        )
    return pd.DataFrame(rows, columns=LIQUIDITY_SIGNAL_SUMMARY_COLUMNS)


def calculate_liquidity_buckets(
    candidates: pd.DataFrame,
    targets: pd.DataFrame,
    snapshots: pd.DataFrame,
    *,
    bucket_count: int = DEFAULT_LIQUIDITY_BUCKET_COUNT,
) -> pd.DataFrame:
    """Compare target selection across signal-date trailing-amount buckets."""

    bucket_count = _validate_positive_integer(bucket_count, "bucket_count")
    candidate_keys = _normalize_keys(candidates, "流动性候选池")
    target_keys = _normalize_keys(targets, "流动性目标")
    snapshot = _normalize_snapshot(snapshots)
    _validate_target_membership(candidate_keys, target_keys)
    merged = candidate_keys.merge(
        snapshot,
        on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    ).merge(
        target_keys.assign(selected=True),
        on=["date", "symbol"],
        how="left",
        validate="one_to_one",
    )
    merged["selected"] = merged["selected"].fillna(False).astype(bool)
    rows = []
    for signal_date, group in merged.groupby("date", sort=True):
        valid = group.loc[_valid_positive(group["avg_amount"])].copy()
        if len(valid) < bucket_count:
            continue
        valid = valid.sort_values(
            ["avg_amount", "symbol"], kind="mergesort"
        ).reset_index(drop=True)
        valid["liquidity_bucket"] = (
            np.arange(len(valid)) * bucket_count // len(valid)
        ) + 1
        selected_valid_count = int(valid["selected"].sum())
        for bucket, bucket_group in valid.groupby("liquidity_bucket", sort=True):
            universe_count = len(bucket_group)
            selected_count = int(bucket_group["selected"].sum())
            universe_share = universe_count / len(valid)
            selected_share = (
                selected_count / selected_valid_count if selected_valid_count else None
            )
            rows.append(
                {
                    "date": signal_date.date().isoformat(),
                    "liquidity_bucket": int(bucket),
                    "bucket_count": bucket_count,
                    "universe_count": universe_count,
                    "selected_count": selected_count,
                    "universe_share": universe_share,
                    "selected_share": selected_share,
                    "selection_lift": (
                        selected_share / universe_share
                        if selected_share is not None
                        else None
                    ),
                    "universe_avg_amount_median": bucket_group["avg_amount"].median(),
                    "selected_avg_amount_median": _quantile(
                        bucket_group.loc[bucket_group["selected"], "avg_amount"],
                        0.5,
                    ),
                    "valid_amount_count": len(valid),
                    "selected_valid_amount_count": selected_valid_count,
                }
            )
    return pd.DataFrame(rows, columns=LIQUIDITY_BUCKET_COLUMNS)


def calculate_liquidity_trade_diagnostics(
    trades: pd.DataFrame,
    snapshots: pd.DataFrame,
    *,
    initial_capital: float,
    participation_limits: Sequence[float] = DEFAULT_PARTICIPATION_LIMITS,
) -> pd.DataFrame:
    """Join engine trades to signal-date liquidity and estimate capacity."""

    if (
        not isinstance(initial_capital, (int, float))
        or not math.isfinite(initial_capital)
        or initial_capital <= 0
    ):
        raise ValueError("initial_capital 必须是正的有限数字")
    limits = _validate_participation_limits(participation_limits)
    snapshot = _normalize_snapshot(snapshots)
    required = {"date", "signal_date", "symbol", "side", "notional"}
    missing = required - set(trades.columns)
    if missing:
        raise ValueError("流动性成交诊断缺少字段: " + ", ".join(sorted(missing)))
    normalized = trades.copy()
    for column in ("date", "signal_date"):
        normalized[column] = pd.to_datetime(normalized[column], errors="coerce")
    normalized["symbol"] = normalized["symbol"].astype("string").str.strip()
    normalized["notional"] = pd.to_numeric(normalized["notional"], errors="coerce")
    if normalized["date"].isna().any() or normalized["symbol"].isna().any():
        raise ValueError("流动性成交诊断的 date 和 symbol 必须有效")
    for column in ("raw_open", "adjusted_open", "cost", "reason"):
        if column not in normalized:
            normalized[column] = pd.NA
    joined = normalized.merge(
        snapshot.rename(columns={"date": "signal_date"}),
        on=["signal_date", "symbol"],
        how="left",
        validate="many_to_one",
        suffixes=("", "_snapshot"),
    )
    execution_mask = joined["side"].astype("string").isin(["BUY", "SELL"])
    valid_amount = _valid_positive(joined["avg_amount"])
    valid_notional = (
        joined["notional"].notna()
        & np.isfinite(joined["notional"])
        & joined["notional"].ge(0)
    )
    valid_order = execution_mask & valid_amount & valid_notional
    joined["order_participation"] = np.nan
    joined.loc[valid_order, "order_participation"] = (
        joined.loc[valid_order, "notional"].abs()
        / joined.loc[valid_order, "avg_amount"]
    )
    joined["liquidity_status"] = "not_rebalance_order"
    joined.loc[valid_order, "liquidity_status"] = "eligible_order"
    joined.loc[execution_mask & ~valid_amount, "liquidity_status"] = (
        "missing_signal_liquidity"
    )
    joined.loc[execution_mask & valid_amount & ~valid_notional, "liquidity_status"] = (
        "invalid_order_notional"
    )
    joined.loc[joined["side"].eq("DELIST"), "liquidity_status"] = "delist_liquidation"
    joined.loc[joined["side"].eq("SKIP_REBALANCE"), "liquidity_status"] = (
        "skipped_rebalance"
    )
    for limit in limits:
        suffix = _limit_suffix(limit)
        joined[f"capacity_capital_{suffix}"] = (
            initial_capital * limit / joined["order_participation"]
        )
    output_columns = tuple(
        [*LIQUIDITY_TRADE_BASE_COLUMNS]
        + [f"capacity_capital_{_limit_suffix(limit)}" for limit in limits]
    )
    return (
        joined.loc[:, output_columns]
        .sort_values(["date", "symbol", "side"], kind="mergesort")
        .reset_index(drop=True)
    )


def calculate_liquidity_capacity_summary(
    trades: pd.DataFrame,
    *,
    participation_limits: Sequence[float] = DEFAULT_PARTICIPATION_LIMITS,
) -> pd.DataFrame:
    """Aggregate order participation and capacity quantiles."""

    limits = _validate_participation_limits(participation_limits)
    if not set(LIQUIDITY_TRADE_BASE_COLUMNS).issubset(trades.columns):
        missing = set(LIQUIDITY_TRADE_BASE_COLUMNS) - set(trades.columns)
        raise ValueError("容量汇总成交数据缺少字段: " + ", ".join(sorted(missing)))
    execution = trades[trades["side"].astype("string").isin(["BUY", "SELL"])]
    eligible = execution[execution["liquidity_status"].eq("eligible_order")]
    missing_count = int(
        execution["liquidity_status"].eq("missing_signal_liquidity").sum()
    )
    invalid_notional_count = int(
        execution["liquidity_status"].eq("invalid_order_notional").sum()
    )
    rows = []
    for limit in limits:
        suffix = _limit_suffix(limit)
        capacity_column = f"capacity_capital_{suffix}"
        if capacity_column not in eligible:
            raise ValueError(f"容量汇总成交数据缺少字段: {capacity_column}")
        capacity = pd.to_numeric(eligible[capacity_column], errors="coerce").dropna()
        participation = pd.to_numeric(
            eligible["order_participation"], errors="coerce"
        ).dropna()
        rows.append(
            {
                "participation_limit": limit,
                "rebalance_order_count": int(len(execution)),
                "eligible_order_count": int(len(eligible)),
                "missing_liquidity_order_count": missing_count,
                "invalid_order_notional_count": invalid_notional_count,
                "capacity_min": _quantile(capacity, 0.0),
                "capacity_p10": _quantile(capacity, 0.1),
                "capacity_median": _quantile(capacity, 0.5),
                "capacity_p90": _quantile(capacity, 0.9),
                "participation_median": _quantile(participation, 0.5),
                "participation_p90": _quantile(participation, 0.9),
                "max_order_participation": _quantile(participation, 1.0),
                "status": "ok" if not capacity.empty else "no_valid_liquidity_order",
            }
        )
    return pd.DataFrame(rows, columns=LIQUIDITY_CAPACITY_SUMMARY_COLUMNS)


def run_factor_liquidity_capacity_diagnostic(
    config: BacktestConfig,
    database_manager: DBManager,
    *,
    lookback_days: int = DEFAULT_LIQUIDITY_LOOKBACK_DAYS,
    bucket_count: int = DEFAULT_LIQUIDITY_BUCKET_COUNT,
    participation_limits: Sequence[float] = DEFAULT_PARTICIPATION_LIMITS,
) -> tuple[LiquidityCapacityReport, dict[str, object]]:
    """Run the diagnostic while exclusively owning the shared DuckDB connection."""

    lock = getattr(database_manager, "duckdb_lock", None)
    guard = lock if lock is not None else nullcontext()
    close_duckdb = getattr(database_manager, "close_duckdb", None)
    with guard:
        try:
            return _run_factor_liquidity_capacity_diagnostic(
                config,
                database_manager,
                lookback_days=lookback_days,
                bucket_count=bucket_count,
                participation_limits=participation_limits,
            )
        finally:
            if close_duckdb is not None:
                close_duckdb()


def _run_factor_liquidity_capacity_diagnostic(
    config: BacktestConfig,
    database_manager: DBManager,
    *,
    lookback_days: int = DEFAULT_LIQUIDITY_LOOKBACK_DAYS,
    bucket_count: int = DEFAULT_LIQUIDITY_BUCKET_COUNT,
    participation_limits: Sequence[float] = DEFAULT_PARTICIPATION_LIMITS,
) -> tuple[LiquidityCapacityReport, dict[str, object]]:
    """Run liquidity diagnostics using the formal strategy's exact targets."""

    if config.strategy_name != FORMAL_STRATEGY_NAME:
        raise ValueError(
            "交易容量诊断当前只支持正式策略 "
            f"{FORMAL_STRATEGY_NAME}，不支持 {config.strategy_name}"
        )
    lookback_days = _validate_positive_integer(lookback_days, "lookback_days")
    bucket_count = _validate_positive_integer(bucket_count, "bucket_count")
    limits = _validate_participation_limits(participation_limits)
    strategy = get_backtest_strategy(config.strategy_name)
    parameters = strategy.validate_parameters(config.strategy_parameters)
    factor_names = tuple(parameters["factor_weights"])
    data_access = BacktestDataAccess(database_manager)
    signal_data = data_access.load_factor_data(
        config,
        factor_names,
        factor_parameters=parameters.get("factor_parameters"),
        minimum_history_days=max(parameters["min_listing_days"], lookback_days),
        include_market_cap=True,
        additional_kline_fields=("amount",),
    )
    # The signal frame owns all inputs needed by the remaining Pandas stages.
    # Drop DuckDB's Parquet scan/cache before factor scoring so the wide signal
    # frame is not retained alongside the query engine's buffer pool.
    database_manager.close_duckdb()
    gc.collect()
    scored_candidates = strategy.build_candidates(
        signal_data,
        config,
        parameters,
    )
    candidates = scored_candidates.loc[:, ["date", "symbol"]].copy()
    targets = validate_target_weights(
        select_equal_weight_targets(scored_candidates, parameters["holding_count"])
    )
    del scored_candidates
    gc.collect()
    candidate_dates = candidates["date"].drop_duplicates()
    snapshots = calculate_trailing_liquidity(
        signal_data,
        lookback_days=lookback_days,
        output_dates=candidate_dates,
    )
    signal_summary = calculate_liquidity_signal_summary(
        candidates,
        targets,
        snapshots,
        bucket_count=bucket_count,
    )
    buckets = calculate_liquidity_buckets(
        candidates,
        targets,
        snapshots,
        bucket_count=bucket_count,
    )
    market_columns = ["date", "symbol", "open", "open_hfq", "close_hfq"]
    market_date_mask = signal_data["date"].between(
        pd.Timestamp(config.start_date), pd.Timestamp(config.end_date)
    )
    market_data = signal_data.loc[market_date_mask, market_columns].copy()
    confirmed_delisting_dates = data_access.load_confirmed_delisting_dates(
        market_data["symbol"].drop_duplicates().tolist(),
        config.end_date,
    )
    del signal_data
    gc.collect()
    engine = DailyBacktestEngine(config)
    benchmark_prices = data_access.load_benchmark_prices(config)
    result = engine.run(
        market_data,
        targets,
        benchmark_prices,
        confirmed_delisting_dates=confirmed_delisting_dates,
    )
    del market_data
    gc.collect()
    trades = calculate_liquidity_trade_diagnostics(
        result.trades,
        snapshots,
        initial_capital=config.initial_capital,
        participation_limits=limits,
    )
    capacity_summary = calculate_liquidity_capacity_summary(
        trades,
        participation_limits=limits,
    )
    backtest_summary = pd.DataFrame(
        [
            {
                **calculate_performance_metrics(result.daily_nav),
                **summarize_trade_statistics(result.trades, config.initial_capital),
            }
        ],
        columns=LIQUIDITY_BACKTEST_SUMMARY_COLUMNS,
    )
    report = LiquidityCapacityReport(
        signal_summary=signal_summary,
        buckets=buckets,
        trades=trades,
        capacity_summary=capacity_summary,
        backtest_summary=backtest_summary,
        targets=targets,
        daily_nav=result.daily_nav,
    )
    metadata = {
        "strategy_name": config.strategy_name,
        "strategy_version": strategy.metadata.version,
        "factor_names": list(factor_names),
        "factor_versions": parameters["factor_versions"],
        "factor_weights": parameters["factor_weights"],
        "holding_count": parameters["holding_count"],
        "min_listing_days": parameters["min_listing_days"],
        "lookback_days": lookback_days,
        "bucket_count": bucket_count,
        "participation_limits": list(limits),
        "candidate_rows": int(len(candidates)),
        "candidate_signal_dates": int(candidates["date"].nunique()),
        "target_rows": int(len(targets)),
        "target_signal_dates": int(targets["date"].nunique()),
        "amount_source": "daily_kline.amount",
        "market_cap_source": "BacktestDataAccess point-in-time valuation",
        "liquidity_window_protocol": "signal date and prior trading days only",
        "execution_day_amount_used_for_capacity": False,
        "capacity_model": "initial capital scaled by per-order participation limit",
        "capacity_model_boundary": "not a market-impact, fill-probability or order-book model",
    }
    return report, metadata


def write_factor_liquidity_capacity_report(
    report: LiquidityCapacityReport,
    output_dir: str | Path,
    *,
    parameters: dict,
) -> Path:
    """Write the standard liquidity-capacity report with an atomic directory swap."""

    output_path = Path(output_dir)
    if output_path.exists():
        raise FileExistsError(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = Path(
        tempfile.mkdtemp(
            prefix=f".{output_path.name}.tmp-",
            dir=str(output_path.parent),
        )
    )
    try:
        (temporary_path / "parameters.json").write_text(
            json.dumps(parameters, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        report.signal_summary.to_csv(
            temporary_path / "liquidity_signal_summary.csv",
            index=False,
            columns=LIQUIDITY_SIGNAL_SUMMARY_COLUMNS,
        )
        report.buckets.to_csv(
            temporary_path / "liquidity_buckets.csv",
            index=False,
            columns=LIQUIDITY_BUCKET_COLUMNS,
        )
        report.trades.to_csv(
            temporary_path / "liquidity_trades.csv",
            index=False,
            columns=list(report.trades.columns),
        )
        report.capacity_summary.to_csv(
            temporary_path / "liquidity_capacity_summary.csv",
            index=False,
            columns=LIQUIDITY_CAPACITY_SUMMARY_COLUMNS,
        )
        report.backtest_summary.to_csv(
            temporary_path / "liquidity_backtest_summary.csv",
            index=False,
            columns=LIQUIDITY_BACKTEST_SUMMARY_COLUMNS,
        )
        report.targets.to_csv(
            temporary_path / "liquidity_targets.csv",
            index=False,
            columns=list(report.targets.columns),
        )
        report.daily_nav.to_csv(temporary_path / "liquidity_daily_nav.csv", index=False)
        (temporary_path / "summary.md").write_text(
            _build_summary(report, parameters),
            encoding="utf-8",
        )
        temporary_path.replace(output_path)
    except BaseException:
        shutil.rmtree(temporary_path, ignore_errors=True)
        raise
    return output_path


def _normalize_keys(frame: pd.DataFrame, label: str) -> pd.DataFrame:
    required = {"date", "symbol"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{label}缺少字段: " + ", ".join(sorted(missing)))
    normalized = frame.loc[:, ["date", "symbol"]].copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["symbol"] = normalized["symbol"].astype("string").str.strip()
    if (
        normalized["date"].isna().any()
        or normalized["symbol"].isna().any()
        or normalized["symbol"].eq("").any()
    ):
        raise ValueError(f"{label}的 date 和 symbol 必须有效")
    if normalized.duplicated(["date", "symbol"]).any():
        raise ValueError(f"{label}不能包含重复的 date/symbol")
    return normalized


def _normalize_snapshot(snapshots: pd.DataFrame) -> pd.DataFrame:
    missing = set(LIQUIDITY_SNAPSHOT_COLUMNS) - set(snapshots.columns)
    if missing:
        raise ValueError("流动性快照缺少字段: " + ", ".join(sorted(missing)))
    normalized = snapshots.loc[:, LIQUIDITY_SNAPSHOT_COLUMNS].copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["symbol"] = normalized["symbol"].astype("string").str.strip()
    if normalized["date"].isna().any() or normalized["symbol"].isna().any():
        raise ValueError("流动性快照的 date 和 symbol 必须有效")
    if normalized.duplicated(["date", "symbol"]).any():
        raise ValueError("流动性快照不能包含重复的 date/symbol")
    return normalized


def _validate_target_membership(
    candidates: pd.DataFrame, targets: pd.DataFrame
) -> None:
    membership = targets.merge(
        candidates,
        on=["date", "symbol"],
        how="left",
        indicator=True,
    )
    if (membership["_merge"] != "both").any():
        raise ValueError("流动性诊断目标必须属于对应信号日候选池")


def _valid_positive(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    return numeric.notna() & np.isfinite(numeric) & numeric.gt(0)


def _coverage_rate(mask: pd.Series, denominator: int) -> float | None:
    return float(mask.sum() / denominator) if denominator else None


def _quantile(values: pd.Series, quantile: float) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    numeric = numeric[np.isfinite(numeric)]
    if numeric.empty:
        return None
    return float(numeric.quantile(quantile))


def _validate_positive_integer(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} 必须是正整数")
    return value


def _validate_participation_limits(
    limits: Sequence[float],
) -> tuple[float, ...]:
    if isinstance(limits, (str, bytes)) or not isinstance(limits, Sequence):
        raise ValueError("participation_limits 必须是数值序列")
    normalized = []
    for limit in limits:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, (int, float))
            or not math.isfinite(limit)
            or not 0 < limit <= 1
        ):
            raise ValueError("参与率上限必须是 (0, 1] 内的有限数字")
        normalized.append(float(limit))
    if not normalized or len(set(normalized)) != len(normalized):
        raise ValueError("参与率上限不能为空且不能重复")
    return tuple(sorted(normalized))


def _limit_suffix(limit: float) -> str:
    percentage = f"{limit * 100:.10f}".rstrip("0").rstrip(".")
    return percentage.replace(".", "p") + "pct"


def _build_summary(report: LiquidityCapacityReport, parameters: dict) -> str:
    backtest = (
        report.backtest_summary.iloc[0] if not report.backtest_summary.empty else {}
    )
    capacity = report.capacity_summary
    lines = [
        "# 交易容量与流动性诊断",
        "",
        "本报告使用正式多因子策略的原始目标和同一 T+1 开盘回测引擎，评估信号日前可知的滚动成交额与简化订单参与率容量；它不修改正式策略。",
        "",
        "## 1. 评估信息与未来函数审计",
        "",
    ]
    _append_table(
        lines,
        ["项目", "内容"],
        [
            ("生成时间", datetime.now().astimezone().isoformat(timespec="seconds")),
            ("原始配置", parameters.get("backtest_config_path", "—")),
            ("实际评估区间", parameters.get("evaluation_scope", "—")),
            ("因子", ", ".join(parameters.get("factor_names", []))),
            (
                "信号日/候选行",
                f"{parameters.get('target_signal_dates', '—')} / {parameters.get('candidate_rows', '—')}",
            ),
            (
                "流动性窗口",
                f"{parameters.get('lookback_days', '—')} 个交易日，包含信号日及之前数据",
            ),
            ("成交额来源", parameters.get("amount_source", "—")),
            ("市值来源", parameters.get("market_cap_source", "—")),
            ("执行日成交额是否用于容量", "否"),
            ("容量模型边界", parameters.get("capacity_model_boundary", "—")),
        ],
    )
    lines.extend(["", "## 2. 正式策略回测基线", ""])
    _append_table(
        lines,
        [
            "总收益",
            "年化收益",
            "年化波动",
            "夏普",
            "最大回撤",
            "累计换手",
            "交易成本",
            "执行交易数",
        ],
        [
            (
                _format_percent(backtest.get("total_return")),
                _format_percent(backtest.get("annualized_return")),
                _format_percent(backtest.get("annualized_volatility")),
                _format_number(backtest.get("sharpe_ratio")),
                _format_percent(backtest.get("max_drawdown")),
                _format_number(backtest.get("cumulative_turnover")),
                _format_number(backtest.get("total_transaction_cost")),
                _format_integer(backtest.get("executed_trade_count")),
            )
        ],
    )
    lines.extend(["", "## 3. 参与率近似容量", ""])
    _append_table(
        lines,
        [
            "参与率上限",
            "有效订单",
            "缺少流动性订单",
            "无效名义金额订单",
            "容量最小值",
            "容量 P10",
            "容量中位数",
            "容量 P90",
            "状态",
        ],
        [
            (
                _format_percent(row["participation_limit"]),
                _format_integer(row["eligible_order_count"]),
                _format_integer(row["missing_liquidity_order_count"]),
                _format_integer(row["invalid_order_notional_count"]),
                _format_number(row["capacity_min"]),
                _format_number(row["capacity_p10"]),
                _format_number(row["capacity_median"]),
                _format_number(row["capacity_p90"]),
                row["status"],
            )
            for _, row in capacity.iterrows()
        ],
    )
    lines.extend(
        [
            "",
            "容量数值的单位是初始资金同单位（元）。它按单笔调仓订单计算 `initial_capital × 参与率上限 / 实际订单参与率`，只适用于筛查订单规模风险；没有模拟价格冲击、盘口深度、涨跌停、整手和多订单竞争。",
            "",
            "## 4. 审计文件",
            "",
        ]
    )
    _append_table(
        lines,
        ["文件", "内容"],
        [
            ("parameters.json", "配置、因子版本、窗口和点时协议"),
            ("liquidity_signal_summary.csv", "逐信号日成交额、市值覆盖和分布"),
            ("liquidity_buckets.csv", "按滚动平均成交额分组的候选/选中比例"),
            ("liquidity_trades.csv", "逐笔交易与信号日快照、参与率和容量"),
            ("liquidity_capacity_summary.csv", "不同参与率上限的容量分位数"),
            ("liquidity_backtest_summary.csv", "正式策略回测收益、风险、换手和成本"),
            ("liquidity_targets.csv", "正式策略逐信号日目标权重"),
            ("liquidity_daily_nav.csv", "正式策略逐交易日净值"),
        ],
    )
    return "\n".join(lines) + "\n"


def _append_table(lines: list[str], headers: Sequence[object], rows) -> None:
    lines.append("| " + " | ".join(str(header) for header in headers) + " |")
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for row in rows:
        lines.append("| " + " | ".join(_format_value(value) for value in row) + " |")


def _format_value(value) -> str:
    if value is None or value is pd.NA or pd.isna(value):
        return "—"
    return str(value).replace("|", "\\|").replace("\n", " ")


def _format_percent(value) -> str:
    if value is None or value is pd.NA or pd.isna(value):
        return "—"
    return f"{float(value):.2%}"


def _format_number(value) -> str:
    if value is None or value is pd.NA or pd.isna(value):
        return "—"
    return f"{float(value):.2f}"


def _format_integer(value) -> str:
    if value is None or value is pd.NA or pd.isna(value):
        return "—"
    return str(int(value))
