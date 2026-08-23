import pandas as pd
import pytest

from backtest.trading_calendar import (
    get_confirmed_month_end_trading_dates,
    get_confirmed_rebalance_signal_dates,
)


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


def test_weekly_schedule_confirms_completed_week_and_excludes_tail():
    dates = pd.to_datetime(
        [
            "2024-01-02",
            "2024-01-03",
            "2024-01-05",
            "2024-01-08",
            "2024-01-09",
            "2024-01-12",
        ]
    )

    result = get_confirmed_rebalance_signal_dates(
        dates,
        rebalance_frequency="weekly",
    )

    assert result.tolist() == [pd.Timestamp("2024-01-05")]


def test_biweekly_schedule_uses_fixed_anchor_across_input_windows():
    full_dates = pd.bdate_range("2023-12-25", "2024-01-22")
    later_dates = full_dates[full_dates >= pd.Timestamp("2024-01-02")]

    full_result = get_confirmed_rebalance_signal_dates(
        full_dates,
        rebalance_frequency="biweekly",
    )
    later_result = get_confirmed_rebalance_signal_dates(
        later_dates,
        rebalance_frequency="biweekly",
    )

    assert full_result[full_result >= pd.Timestamp("2024-01-02")].tolist() == (
        later_result.tolist()
    )
    assert later_result.tolist() == [
        pd.Timestamp("2024-01-05"),
        pd.Timestamp("2024-01-19"),
    ]


def test_every_n_trading_days_uses_start_date_anchor_and_excludes_tail():
    dates = pd.to_datetime(
        [
            "2024-01-02",
            "2024-01-03",
            "2024-01-05",
            "2024-01-08",
            "2024-01-09",
            "2024-01-10",
            "2024-01-11",
        ]
    )

    result = get_confirmed_rebalance_signal_dates(
        dates,
        rebalance_frequency="every_n_trading_days",
        rebalance_interval_trading_days=3,
        start_date="2024-01-03",
        end_date="2024-01-11",
    )

    assert result.tolist() == [pd.Timestamp("2024-01-08")]


def test_every_trading_day_excludes_only_final_unexecutable_date():
    dates = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])

    result = get_confirmed_rebalance_signal_dates(
        dates,
        rebalance_frequency="every_n_trading_days",
        rebalance_interval_trading_days=1,
    )

    assert result.tolist() == [
        pd.Timestamp("2024-01-02"),
        pd.Timestamp("2024-01-03"),
    ]


@pytest.mark.parametrize(
    ("frequency", "interval"),
    [
        ("unknown", None),
        ("weekly", 5),
        ("every_n_trading_days", None),
        ("every_n_trading_days", True),
    ],
)
def test_rebalance_schedule_rejects_invalid_configuration(frequency, interval):
    with pytest.raises(ValueError):
        get_confirmed_rebalance_signal_dates(
            pd.to_datetime(["2024-01-02", "2024-01-03"]),
            rebalance_frequency=frequency,
            rebalance_interval_trading_days=interval,
        )
