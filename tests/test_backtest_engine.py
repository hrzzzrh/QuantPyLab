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


@pytest.mark.parametrize(
    ("frequency", "interval", "expected_reason"),
    [
        ("monthly", None, "monthly_rebalance"),
        ("weekly", None, "weekly_rebalance"),
        ("biweekly", None, "biweekly_rebalance"),
        (
            "every_n_trading_days",
            2,
            "every_n_trading_days_rebalance",
        ),
    ],
)
def test_rebalance_trade_reason_matches_configured_frequency(
    frequency, interval, expected_reason
):
    config = BacktestConfig(
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 5),
        strategy_name="quality-value-recovery",
        benchmark_symbol=None,
        rebalance_frequency=frequency,
        rebalance_interval_trading_days=interval,
    )
    targets = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2024-01-02"),
                "symbol": "000001",
                "target_weight": 1.0,
            }
        ]
    )

    result = DailyBacktestEngine(config).run(_prices(), targets)

    assert result.trades["reason"].unique().tolist() == [expected_reason]


def test_prepared_market_data_preserves_backtest_result():
    targets = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2024-01-02"),
                "symbol": "000001",
                "target_weight": 1.0,
            }
        ]
    )
    engine = DailyBacktestEngine(_config())
    prepared_market_data = engine.prepare_market_data(_prices(), _config())

    uncached_result = engine.run(_prices(), targets)
    cached_result = engine.run(
        _prices(),
        targets,
        prepared_market_data=prepared_market_data,
    )

    pd.testing.assert_frame_equal(uncached_result.daily_nav, cached_result.daily_nav)
    pd.testing.assert_frame_equal(uncached_result.trades, cached_result.trades)


def test_prepared_market_data_keeps_compact_daily_frames():
    prices = _prices().assign(unused_signal_column=1.0)

    prepared = DailyBacktestEngine(_config()).prepare_market_data(prices, _config())

    assert prepared.price_data.columns.tolist() == [
        "date",
        "symbol",
        "open",
        "open_hfq",
        "close_hfq",
    ]
    assert all(isinstance(frame, pd.DataFrame) for frame in prepared.price_map.values())
    assert all(
        frame.columns.tolist() == ["open", "open_hfq", "close_hfq"]
        for frame in prepared.price_map.values()
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


def test_rebalance_is_independent_of_target_row_order():
    targets = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2024-01-02"),
                "symbol": "000002",
                "target_weight": 0.5,
            },
            {
                "date": pd.Timestamp("2024-01-02"),
                "symbol": "000001",
                "target_weight": 0.5,
            },
        ]
    )
    engine = DailyBacktestEngine(_config())

    first = engine.run(_prices(), targets)
    second = engine.run(_prices(), targets.iloc[::-1].reset_index(drop=True))

    pd.testing.assert_frame_equal(first.daily_nav, second.daily_nav)
    pd.testing.assert_frame_equal(first.trades, second.trades)
    assert first.trades.loc[first.trades["side"] == "BUY", "symbol"].tolist() == [
        "000001",
        "000002",
    ]


def test_delisted_position_is_liquidated_at_last_close():
    dates = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])
    rows = []
    # 000001 最后交易日为 2024-01-03, 之后行情终结 (退市)。
    for current_date, a_open, a_close in zip(dates[:2], [100, 100], [100, 110]):
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
                "open": 50.0,
                "open_hfq": 50.0,
                "close_hfq": 50.0,
            }
        )
    prices = pd.DataFrame(rows)
    targets = pd.DataFrame(
        [{"date": pd.Timestamp("2024-01-02"), "symbol": "000001", "target_weight": 1.0}]
    )

    result = DailyBacktestEngine(_config()).run(
        prices,
        targets,
        confirmed_delisting_dates={"000001": pd.Timestamp("2024-01-03")},
    )

    delist_trades = result.trades.loc[result.trades["side"] == "DELIST"]
    assert len(delist_trades) == 1
    trade = delist_trades.iloc[0]
    assert trade["date"] == pd.Timestamp("2024-01-03")
    assert trade["symbol"] == "000001"
    assert trade["adjusted_open"] == 110
    assert trade["notional"] == pytest.approx(109_890)
    assert trade["cost"] == 0
    assert not result.trades["side"].isin(["SKIP_REBALANCE"]).any()
    # 清算后不再持仓: 净值冻结为清算价值, 不被后续行情终结日影响。
    assert result.daily_nav.loc[
        result.daily_nav["date"] == pd.Timestamp("2024-01-04"), "nav"
    ].iloc[0] == pytest.approx(109_890)


def test_delisted_position_does_not_block_rebalance():
    dates = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])
    rows = []
    # 000001 最后交易日为 2024-01-03; 000002 全程在市。
    for current_date, a_open, a_close in zip(dates[:2], [100, 100], [100, 110]):
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
                "open": 50.0,
                "open_hfq": 50.0,
                "close_hfq": 50.0,
            }
        )
    prices = pd.DataFrame(rows)
    # 01-02 信号: 全仓 000001; 01-03 信号: 全仓 000002 (000001 已清算, 不应阻塞调仓)。
    targets = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2024-01-02"),
                "symbol": "000001",
                "target_weight": 1.0,
            },
            {
                "date": pd.Timestamp("2024-01-03"),
                "symbol": "000002",
                "target_weight": 1.0,
            },
        ]
    )

    result = DailyBacktestEngine(_config()).run(
        prices,
        targets,
        confirmed_delisting_dates={"000001": pd.Timestamp("2024-01-03")},
    )

    assert not result.trades["side"].isin(["SKIP_REBALANCE"]).any()
    buy_b = result.trades.loc[
        (result.trades["side"] == "BUY") & (result.trades["symbol"] == "000002")
    ]
    assert len(buy_b) == 1
    assert buy_b.iloc[0]["date"] == pd.Timestamp("2024-01-04")


def test_truncated_active_position_is_not_misclassified_as_delisted():
    targets = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2024-01-02"),
                "symbol": "000001",
                "target_weight": 1.0,
            }
        ]
    )

    result = DailyBacktestEngine(_config()).run(_prices(), targets)

    assert not result.trades["side"].eq("DELIST").any()
    final_nav = result.daily_nav.iloc[-1]
    assert final_nav["cash"] == pytest.approx(0.0)
    assert final_nav["positions_value"] == pytest.approx(final_nav["nav"])
