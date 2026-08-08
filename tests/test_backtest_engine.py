from datetime import date

import pandas as pd
import pytest

from backtest.config import BacktestConfig
from backtest.engine import DailyBacktestEngine


def _config():
    return BacktestConfig(
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 5),
        strategy_name="quality-value-recovery",
        initial_capital=100_000,
        commission_bps=5,
        slippage_bps=5,
        benchmark_symbol=None,
    )


def _prices(open_for_b=50.0):
    dates = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"])
    rows = []
    for current_date, a_open, a_close in zip(
        dates, [100, 100, 110, 121], [100, 110, 121, 121]
    ):
        rows.append(
            {
                "date": current_date,
                "symbol": "000001",
                "open": a_open,
                "open_hfq": a_open,
                "close_hfq": a_close,
            }
        )
    for current_date in dates:
        rows.append(
            {
                "date": current_date,
                "symbol": "000002",
                "open": open_for_b,
                "open_hfq": open_for_b,
                "close_hfq": open_for_b,
            }
        )
    return pd.DataFrame(rows)


def test_signal_executes_at_next_trading_day_open():
    targets = pd.DataFrame(
        [{"date": pd.Timestamp("2024-01-02"), "symbol": "000001", "target_weight": 1.0}]
    )

    result = DailyBacktestEngine(_config()).run(_prices(), targets)

    buy_trade = result.trades.loc[result.trades["side"] == "BUY"].iloc[0]
    assert buy_trade["date"] == pd.Timestamp("2024-01-03")
    assert buy_trade["signal_date"] == pd.Timestamp("2024-01-02")
    assert (
        result.daily_nav.loc[
            result.daily_nav["date"] == pd.Timestamp("2024-01-02"), "nav"
        ].iloc[0]
        == 100_000
    )


def test_missing_target_open_leaves_capital_as_cash():
    prices = _prices()
    prices.loc[
        (prices["date"] == pd.Timestamp("2024-01-03")) & (prices["symbol"] == "000002"),
        ["open", "open_hfq"],
    ] = float("nan")
    targets = pd.DataFrame(
        [{"date": pd.Timestamp("2024-01-02"), "symbol": "000002", "target_weight": 1.0}]
    )

    result = DailyBacktestEngine(_config()).run(prices, targets)

    assert result.trades.empty
    assert (
        result.daily_nav.loc[
            result.daily_nav["date"] == pd.Timestamp("2024-01-03"), "cash"
        ].iloc[0]
        == 100_000
    )


def test_rebalance_charges_commission_and_slippage_on_turnover():
    targets = pd.DataFrame(
        [{"date": pd.Timestamp("2024-01-02"), "symbol": "000001", "target_weight": 1.0}]
    )

    result = DailyBacktestEngine(_config()).run(_prices(), targets)

    buy_trade = result.trades.loc[result.trades["side"] == "BUY"].iloc[0]
    assert buy_trade["notional"] == 100_000
    assert buy_trade["cost"] == 100
    assert result.daily_nav.loc[
        result.daily_nav["date"] == pd.Timestamp("2024-01-03"), "nav"
    ].iloc[0] == pytest.approx(109_890)
