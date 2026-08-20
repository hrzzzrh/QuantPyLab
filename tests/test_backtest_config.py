from pathlib import Path

import pytest

from backtest.config import load_backtest_config
from backtest.strategy_registry import get_backtest_strategy, list_backtest_strategies


def test_loads_toml_backtest_config():
    config = load_backtest_config(Path("config/backtest/price_momentum.toml"))

    assert config.strategy_name == "price-momentum"
    assert config.strategy_parameters["lookback_days"] == 120
    assert config.start_date.isoformat() == "2018-01-01"


def test_registry_exposes_four_explicit_strategies():
    names = [metadata.name for metadata in list_backtest_strategies()]

    assert names == [
        "factor-composite-experiment",
        "multi-factor-quality-value-momentum",
        "price-momentum",
        "quality-value-recovery",
    ]
    assert get_backtest_strategy("price-momentum").metadata.version == "1"


def test_loads_multi_factor_strategy_parameters():
    config = load_backtest_config(
        Path("config/backtest/multi_factor_quality_value_momentum.toml")
    )

    assert config.strategy_name == "multi-factor-quality-value-momentum"
    assert config.strategy_parameters["factor_weights"]["valuation_pb"] == 0.15


def test_loads_factor_experiment_parameters():
    config = load_backtest_config(
        Path("config/backtest/factor_experiment_reversal.toml")
    )

    assert config.strategy_name == "factor-composite-experiment"
    assert config.strategy_parameters["factor_weights"] == {"price_reversal_20d": 1.0}
    assert config.strategy_parameters["factor_parameters"] == {
        "price_reversal_20d": {"lookback_days": 20}
    }


def test_registry_rejects_unknown_strategy():
    with pytest.raises(ValueError, match="未注册的回测策略"):
        get_backtest_strategy("unknown-strategy")
