from datetime import date

import pandas as pd

import backtest.runner as runner_module
from backtest.config import BacktestConfig
from backtest.engine import BacktestResult
from backtest.strategy_base import StrategyMetadata


def test_execute_backtest_resolves_strategy_and_skips_benchmark_when_requested(
    monkeypatch,
):
    config = BacktestConfig(
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 5),
        strategy_name="fake-strategy",
        strategy_parameters={"holding_count": 2},
        benchmark_symbol="510300",
    )
    prices = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2024-01-02"),
                "symbol": "000001",
                "open": 100.0,
                "open_hfq": 100.0,
                "close_hfq": 100.0,
            }
        ]
    )
    targets = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2024-01-02"),
                "symbol": "000001",
                "score": 1.0,
                "rank": 1.0,
                "target_weight": 1.0,
            }
        ]
    )

    class FakeStrategy:
        metadata = StrategyMetadata("fake-strategy", "7", "fake", "")

        def validate_parameters(self, parameters):
            return {**parameters, "resolved": True}

        def load_signal_data(self, data_access, config, parameters):
            return prices

        def build_targets(self, signal_data, config, parameters):
            return targets

    class FakeDataAccess:
        def __init__(self, database_manager):
            self.database_manager = database_manager

        def load_benchmark_prices(self, config):
            raise AssertionError("include_benchmark=False 时不应加载基准")

    class FakeEngine:
        def __init__(self, config):
            self.config = config

        def run(self, signal_data, resolved_targets, benchmark_prices):
            assert benchmark_prices is None
            return BacktestResult(
                daily_nav=pd.DataFrame(
                    [{"date": pd.Timestamp("2024-01-02"), "nav": 1.0}]
                ),
                trades=pd.DataFrame(),
            )

    monkeypatch.setattr(
        runner_module, "get_backtest_strategy", lambda name: FakeStrategy()
    )
    monkeypatch.setattr(runner_module, "BacktestDataAccess", FakeDataAccess)
    monkeypatch.setattr(runner_module, "DailyBacktestEngine", FakeEngine)

    result = runner_module.execute_backtest(config, object(), include_benchmark=False)

    assert result.config.strategy_version == "7"
    assert result.config.strategy_parameters["resolved"] is True
    assert result.targets.equals(targets)
