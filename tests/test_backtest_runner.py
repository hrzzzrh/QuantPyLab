from dataclasses import replace
from datetime import date

import pandas as pd

import backtest.runner as runner_module
from backtest.config import BacktestConfig
from backtest.engine import BacktestResult
from backtest.strategies.factor_composite_experiment import (
    FactorCompositeExperimentStrategy,
)
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


def test_execute_backtest_reuses_factor_candidates_within_cache(monkeypatch):
    config = BacktestConfig(
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 5),
        strategy_name="fake-factor-strategy",
        strategy_parameters={
            "factor_weights": {"valuation_pb": 1.0},
            "factor_parameters": {},
            "min_listing_days": 1,
        },
        benchmark_symbol=None,
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
    calls = {"load": 0, "calculate": 0, "prepare": 0, "build": 0}

    class FakeStrategy:
        metadata = StrategyMetadata("fake-factor-strategy", "1", "fake", "")

        def validate_parameters(self, parameters):
            return dict(parameters)

        def load_signal_data(self, data_access, config, parameters):
            calls["load"] += 1
            return prices

        def build_targets(self, signal_data, config, parameters):
            raise AssertionError("缓存能力存在时应走拆分后的目标生成路径")

        def calculate_factor_frame(self, signal_data, parameters):
            calls["calculate"] += 1
            return pd.DataFrame(
                [
                    {
                        "date": pd.Timestamp("2024-01-02"),
                        "symbol": "000001",
                        "valuation_pb": 1.0,
                    }
                ]
            )

        def prepare_target_candidates(
            self, signal_data, factor_frame, config, parameters
        ):
            calls["prepare"] += 1
            return factor_frame

        def build_targets_from_candidates(self, candidates, parameters):
            calls["build"] += 1
            return targets

    class FakeDataAccess:
        def __init__(self, database_manager):
            pass

        def load_factor_data(self, config, factor_names, **kwargs):
            raise AssertionError("runner 应通过 FakeStrategy.load_signal_data 加载")

        def load_benchmark_prices(self, config):
            raise AssertionError("benchmark_symbol=None 时不应加载基准")

    class FakeEngine:
        def __init__(self, config):
            pass

        def run(self, signal_data, resolved_targets, benchmark_prices):
            assert benchmark_prices is None
            assert resolved_targets.equals(targets)
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

    assert runner_module.BacktestExecutionCache().max_entries == 4
    cache = runner_module.BacktestExecutionCache(max_entries=2)
    first = runner_module.execute_backtest(
        config, object(), include_benchmark=False, execution_cache=cache
    )
    second = runner_module.execute_backtest(
        config, object(), include_benchmark=False, execution_cache=cache
    )
    changed_period = replace(config, end_date=date(2024, 1, 6))
    runner_module.execute_backtest(
        changed_period, object(), include_benchmark=False, execution_cache=cache
    )
    runner_module.execute_backtest(
        config, object(), include_benchmark=False, execution_cache=cache
    )
    second_changed_period = replace(config, end_date=date(2024, 1, 7))
    runner_module.execute_backtest(
        second_changed_period,
        object(),
        include_benchmark=False,
        execution_cache=cache,
    )
    runner_module.execute_backtest(
        changed_period, object(), include_benchmark=False, execution_cache=cache
    )

    assert first.targets.equals(second.targets)
    assert calls == {"load": 4, "calculate": 4, "prepare": 4, "build": 6}
    assert cache.stats == {"cache_hits": 2, "cache_misses": 4}


def test_factor_execution_cache_matches_real_strategy_and_isolates_keys():
    dates = pd.bdate_range("2023-01-02", periods=251)
    rows = []
    for symbol, pb, ps in [("000001", 1.0, 2.0), ("000002", 2.0, 1.0)]:
        for index, current_date in enumerate(dates):
            rows.append(
                {
                    "date": current_date,
                    "symbol": symbol,
                    "close_hfq": 100.0 + index,
                    "pb": pb,
                    "ps_ttm": ps,
                }
            )
    signal_data = pd.DataFrame(rows)
    config = BacktestConfig(
        start_date=dates[0].date(),
        end_date=dates[-1].date(),
        strategy_name="factor-composite-experiment",
        benchmark_symbol=None,
    )
    strategy = FactorCompositeExperimentStrategy()
    load_calls = []

    class FakeDataAccess:
        def load_factor_data(self, config, factor_names, **kwargs):
            load_calls.append((factor_names, kwargs))
            return signal_data.copy()

    data_access = FakeDataAccess()
    cache = runner_module.BacktestExecutionCache(max_entries=10)

    two_factor_parameters = strategy.validate_parameters(
        {
            "factor_weights": {"valuation_pb": 1.0, "valuation_ps_ttm": 1.0},
            "min_listing_days": 20,
        }
    )
    first_signal_data, first_targets = cache.prepare_inputs(
        strategy,
        data_access,
        config,
        two_factor_parameters,
    )
    expected_first_targets = strategy.build_targets(
        signal_data, config, two_factor_parameters
    )

    reweighted_parameters = strategy.validate_parameters(
        {
            "factor_weights": {"valuation_pb": 2.0, "valuation_ps_ttm": 1.0},
            "min_listing_days": 20,
        }
    )
    _, reweighted_targets = cache.prepare_inputs(
        strategy,
        data_access,
        config,
        reweighted_parameters,
    )
    expected_reweighted_targets = strategy.build_targets(
        signal_data, config, reweighted_parameters
    )

    single_factor_parameters = strategy.validate_parameters(
        {"factor_weights": {"valuation_pb": 1.0}, "min_listing_days": 20}
    )
    cache.prepare_inputs(strategy, data_access, config, single_factor_parameters)
    changed_listing_parameters = strategy.validate_parameters(
        {"factor_weights": {"valuation_pb": 1.0}, "min_listing_days": 21}
    )
    cache.prepare_inputs(strategy, data_access, config, changed_listing_parameters)

    long_reversal_parameters = strategy.validate_parameters(
        {
            "factor_weights": {"price_reversal_20d": 1.0},
            "factor_parameters": {"price_reversal_20d": {"lookback_days": 20}},
            "min_listing_days": 20,
        }
    )
    cache.prepare_inputs(strategy, data_access, config, long_reversal_parameters)
    short_reversal_parameters = strategy.validate_parameters(
        {
            "factor_weights": {"price_reversal_20d": 1.0},
            "factor_parameters": {"price_reversal_20d": {"lookback_days": 10}},
            "min_listing_days": 20,
        }
    )
    cache.prepare_inputs(strategy, data_access, config, short_reversal_parameters)
    cache.prepare_inputs(strategy, data_access, config, short_reversal_parameters)

    assert first_signal_data.equals(signal_data)
    pd.testing.assert_frame_equal(
        first_targets.reset_index(drop=True),
        expected_first_targets.reset_index(drop=True),
    )
    pd.testing.assert_frame_equal(
        reweighted_targets.reset_index(drop=True),
        expected_reweighted_targets.reset_index(drop=True),
    )
    assert len(load_calls) == 5
    assert cache.stats == {"cache_hits": 2, "cache_misses": 5}

    cache.clear()
    cache.prepare_inputs(strategy, data_access, config, two_factor_parameters)
    assert len(load_calls) == 6
    assert cache.stats == {"cache_hits": 2, "cache_misses": 6}
