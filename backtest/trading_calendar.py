"""Pure trading-calendar helpers shared by backtest research components."""

from collections.abc import Sequence
from datetime import date

import pandas as pd

from backtest.config import (
    BIWEEKLY_REBALANCE_ANCHOR_DATE,
    BacktestConfig,
    validate_rebalance_schedule_parameters,
)

_BIWEEKLY_ANCHOR = pd.Timestamp(BIWEEKLY_REBALANCE_ANCHOR_DATE)


def get_configured_rebalance_signal_dates(
    dates: pd.Series | pd.DatetimeIndex | Sequence[object],
    config: BacktestConfig,
) -> pd.DatetimeIndex:
    """Resolve signal dates from one validated backtest configuration."""

    return get_confirmed_rebalance_signal_dates(
        dates,
        rebalance_frequency=config.rebalance_frequency,
        rebalance_interval_trading_days=config.rebalance_interval_trading_days,
        start_date=config.start_date,
        end_date=config.end_date,
    )


def get_confirmed_rebalance_signal_dates(
    dates: pd.Series | pd.DatetimeIndex | Sequence[object],
    *,
    rebalance_frequency: str,
    rebalance_interval_trading_days: int | None = None,
    start_date: date | pd.Timestamp | str | None = None,
    end_date: date | pd.Timestamp | str | None = None,
) -> pd.DatetimeIndex:
    """Return deterministic signal dates with an observed T+1 trading date."""

    validate_rebalance_schedule_parameters(
        rebalance_frequency,
        rebalance_interval_trading_days,
    )
    unique_dates = _normalize_trading_dates(dates)
    if len(unique_dates) < 2:
        return pd.DatetimeIndex([])

    normalized_start = _normalize_boundary(start_date, "start_date")
    normalized_end = _normalize_boundary(end_date, "end_date")
    if (
        normalized_start is not None
        and normalized_end is not None
        and normalized_start > normalized_end
    ):
        raise ValueError("调仓日程开始日期不能晚于结束日期")

    if rebalance_frequency == "every_n_trading_days":
        return _get_every_n_trading_day_signal_dates(
            unique_dates,
            rebalance_interval_trading_days,
            normalized_start,
            normalized_end,
        )

    if rebalance_frequency == "monthly":
        period_ids = unique_dates.to_period("M")
    elif rebalance_frequency == "weekly":
        period_ids = unique_dates.to_period("W-SUN")
    else:
        period_ids = pd.Index((unique_dates - _BIWEEKLY_ANCHOR).days // 14)

    period_changed = period_ids[:-1] != period_ids[1:]
    return _filter_confirmed_dates(
        unique_dates[:-1][period_changed],
        unique_dates[1:][period_changed],
        normalized_start,
        normalized_end,
    )


def get_confirmed_month_end_trading_dates(dates: pd.Series) -> pd.DatetimeIndex:
    """Return trading dates confirmed as month-end by the following date.

    The final observed date is intentionally excluded because a truncated
    series cannot prove that it is the month's final trading day. A confirmed
    month-end is an observed date whose next observed trading date belongs to
    a different calendar month.
    """

    return get_confirmed_rebalance_signal_dates(
        dates,
        rebalance_frequency="monthly",
    )


def _normalize_trading_dates(
    dates: pd.Series | pd.DatetimeIndex | Sequence[object],
) -> pd.DatetimeIndex:
    normalized = pd.DatetimeIndex(pd.to_datetime(dates, errors="coerce"))
    if normalized.isna().any():
        raise ValueError("交易日历包含无效日期")
    if normalized.tz is not None:
        normalized = normalized.tz_localize(None)
    return normalized.drop_duplicates().sort_values()


def _normalize_boundary(
    value: date | pd.Timestamp | str | None,
    field_name: str,
) -> pd.Timestamp | None:
    if value is None:
        return None
    normalized = pd.to_datetime(value, errors="coerce")
    if pd.isna(normalized):
        raise ValueError(f"{field_name} 必须是有效日期")
    timestamp = pd.Timestamp(normalized)
    if timestamp.tz is not None:
        timestamp = timestamp.tz_localize(None)
    return timestamp.normalize()


def _filter_confirmed_dates(
    signal_dates: pd.DatetimeIndex,
    following_dates: pd.DatetimeIndex,
    start_date: pd.Timestamp | None,
    end_date: pd.Timestamp | None,
) -> pd.DatetimeIndex:
    mask = pd.Series(True, index=range(len(signal_dates)))
    if start_date is not None:
        mask &= signal_dates >= start_date
    if end_date is not None:
        mask &= (signal_dates <= end_date) & (following_dates <= end_date)
    return signal_dates[mask.to_numpy()]


def _get_every_n_trading_day_signal_dates(
    unique_dates: pd.DatetimeIndex,
    interval_trading_days: int | None,
    start_date: pd.Timestamp | None,
    end_date: pd.Timestamp | None,
) -> pd.DatetimeIndex:
    interval = int(interval_trading_days)
    eligible_mask = pd.Series(True, index=range(len(unique_dates)))
    if start_date is not None:
        eligible_mask &= unique_dates >= start_date
    if end_date is not None:
        eligible_mask &= unique_dates <= end_date
    eligible_dates = unique_dates[eligible_mask.to_numpy()]
    if len(eligible_dates) < 2:
        return pd.DatetimeIndex([])
    return eligible_dates[:-1][interval - 1 :: interval]
