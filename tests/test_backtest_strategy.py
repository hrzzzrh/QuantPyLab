from datetime import date

import pandas as pd

from backtest.config import BacktestConfig
from backtest.strategies.multi_factor_quality_value_momentum import (
    MultiFactorQualityValueMomentumStrategy,
)
from backtest.strategies.price_momentum import PriceMomentumStrategy
from backtest.strategies.quality_value_recovery import QualityValueRecoveryStrategy


def _config(end_date, strategy_name):
    return BacktestConfig(
        start_date=date(2023, 1, 2),
        end_date=end_date,
        strategy_name=strategy_name,
        benchmark_symbol=None,
    )


def test_quality_value_recovery_selects_lowest_value_scores():
    dates = pd.bdate_range("2023-01-02", periods=251)
    rows = []
    for symbol, pe_ttm, pb in [
        ("000001", 10.0, 1.0),
        ("000002", 20.0, 2.0),
        ("000003", 30.0, 3.0),
        ("000004", 40.0, 4.0),
        ("000005", 50.0, 5.0),
    ]:
        for index, current_date in enumerate(dates):
            rows.append(
                {
                    "date": current_date,
                    "symbol": symbol,
                    "close_hfq": 100 + index,
                    "pe_ttm": pe_ttm,
                    "pb": pb,
                    "roe_weighted": 10.0,
                    "operating_cashflow_to_revenue": 0.1,
                }
            )
    config = _config(dates[-1].date(), "quality-value-recovery")
    strategy = QualityValueRecoveryStrategy()

    targets = strategy.build_targets(
        pd.DataFrame(rows), config, strategy.validate_parameters({"holding_count": 2})
    )

    latest_targets = targets[targets["date"] == targets["date"].max()]
    assert latest_targets["symbol"].tolist() == ["000001", "000002"]
    assert latest_targets["target_weight"].tolist() == [0.5, 0.5]


def test_price_momentum_selects_highest_returns():
    dates = pd.bdate_range("2023-01-02", periods=251)
    rows = []
    for symbol, daily_gain in [
        ("000001", 0.50),
        ("000002", 0.40),
        ("000003", 0.30),
        ("000004", 0.20),
        ("000005", 0.10),
    ]:
        for index, current_date in enumerate(dates):
            rows.append(
                {
                    "date": current_date,
                    "symbol": symbol,
                    "close_hfq": 100 + index * daily_gain,
                }
            )
    config = _config(dates[-1].date(), "price-momentum")
    strategy = PriceMomentumStrategy()

    targets = strategy.build_targets(
        pd.DataFrame(rows), config, strategy.validate_parameters({"holding_count": 2})
    )

    latest_targets = targets[targets["date"] == targets["date"].max()]
    assert latest_targets["symbol"].tolist() == ["000001", "000002"]
    assert latest_targets["target_weight"].tolist() == [0.5, 0.5]


def test_price_momentum_preserves_custom_factor_windows():
    dates = pd.bdate_range("2023-01-02", periods=251)
    rows = []
    for symbol, daily_gain in [("000001", 0.50), ("000002", 0.40)]:
        for index, current_date in enumerate(dates):
            rows.append(
                {
                    "date": current_date,
                    "symbol": symbol,
                    "close_hfq": 100 + index * daily_gain,
                }
            )
    config = _config(dates[-1].date(), "price-momentum")
    strategy = PriceMomentumStrategy()
    parameters = strategy.validate_parameters(
        {"holding_count": 1, "lookback_days": 20, "trend_window": 30}
    )

    targets = strategy.build_targets(pd.DataFrame(rows), config, parameters)

    assert not targets.empty
    assert targets["symbol"].unique().tolist() == ["000001"]


def test_multi_factor_strategy_builds_equal_weight_targets_from_registered_factors():
    dates = pd.bdate_range("2023-01-02", periods=251)
    rows = []
    for symbol, pe_ttm, pb, daily_gain in [
        ("000001", 10.0, 1.0, 0.50),
        ("000002", 20.0, 2.0, 0.40),
        ("000003", 30.0, 3.0, 0.30),
        ("000004", 40.0, 4.0, 0.20),
        ("000005", 50.0, 5.0, 0.10),
    ]:
        for index, current_date in enumerate(dates):
            rows.append(
                {
                    "date": current_date,
                    "symbol": symbol,
                    "close_hfq": 100 + index * daily_gain,
                    "pe_ttm": pe_ttm,
                    "pb": pb,
                    "roe_weighted": 10.0,
                    "operating_cashflow_to_revenue": 0.1,
                }
            )
    config = _config(dates[-1].date(), "multi-factor-quality-value-momentum")
    strategy = MultiFactorQualityValueMomentumStrategy()
    parameters = strategy.validate_parameters({"holding_count": 2})

    targets = strategy.build_targets(pd.DataFrame(rows), config, parameters)

    latest_targets = targets[targets["date"] == targets["date"].max()]
    assert len(latest_targets) == 2
    assert latest_targets["target_weight"].tolist() == [0.5, 0.5]
    assert latest_targets["score"].notna().all()


def test_quality_value_recovery_preserves_custom_trend_window():
    dates = pd.bdate_range("2023-01-02", periods=251)
    rows = []
    for symbol, pe_ttm in [
        ("000001", 10.0),
        ("000002", 20.0),
        ("000003", 30.0),
        ("000004", 40.0),
        ("000005", 50.0),
    ]:
        for index, current_date in enumerate(dates):
            rows.append(
                {
                    "date": current_date,
                    "symbol": symbol,
                    "close_hfq": 100 + index,
                    "pe_ttm": pe_ttm,
                    "pb": pe_ttm / 10,
                    "roe_weighted": 10.0,
                    "operating_cashflow_to_revenue": 0.1,
                }
            )
    config = _config(dates[-1].date(), "quality-value-recovery")
    strategy = QualityValueRecoveryStrategy()
    parameters = strategy.validate_parameters({"holding_count": 2, "trend_window": 30})

    targets = strategy.build_targets(pd.DataFrame(rows), config, parameters)

    assert not targets.empty
    assert targets["symbol"].tolist() == ["000001", "000002"]
