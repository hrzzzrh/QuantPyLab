import pandas as pd

from backtest.trading_calendar import get_confirmed_month_end_trading_dates


def test_confirmed_month_end_requires_following_month_observation():
    dates = pd.Series(
        pd.to_datetime(
            [
                "2026-07-30",
                "2026-07-31",
                "2026-08-03",
                "2026-08-20",
                "2026-08-21",
            ]
        )
    )

    result = get_confirmed_month_end_trading_dates(dates)

    assert result.tolist() == [pd.Timestamp("2026-07-31")]


def test_confirmed_month_end_handles_duplicates_and_unsorted_dates():
    dates = pd.Series(
        pd.to_datetime(
            [
                "2024-03-01",
                "2024-02-29",
                "2024-01-31",
                "2024-02-29",
                "2024-01-30",
            ]
        )
    )

    result = get_confirmed_month_end_trading_dates(dates)

    assert result.tolist() == [
        pd.Timestamp("2024-01-31"),
        pd.Timestamp("2024-02-29"),
    ]


def test_confirmed_month_end_returns_empty_without_following_date():
    result = get_confirmed_month_end_trading_dates(
        pd.Series(pd.to_datetime(["2026-08-21"]))
    )

    assert result.empty
