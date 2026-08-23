"""Pure trading-calendar helpers shared by backtest research components."""

import pandas as pd


def get_confirmed_month_end_trading_dates(dates: pd.Series) -> pd.DatetimeIndex:
    """Return trading dates confirmed as month-end by the following date.

    The final observed date is intentionally excluded because a truncated
    series cannot prove that it is the month's final trading day. A confirmed
    month-end is an observed date whose next observed trading date belongs to
    a different calendar month.
    """

    unique_dates = pd.DatetimeIndex(
        pd.to_datetime(dates.drop_duplicates()).sort_values()
    )
    if len(unique_dates) < 2:
        return pd.DatetimeIndex([])
    current_periods = unique_dates[:-1].to_period("M")
    following_periods = unique_dates[1:].to_period("M")
    return unique_dates[:-1][current_periods != following_periods]
