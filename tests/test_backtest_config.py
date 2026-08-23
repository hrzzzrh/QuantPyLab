from datetime import date
from pathlib import Path

import pytest

from backtest.config import BacktestConfig, load_backtest_config
from backtest.strategy_registry import get_backtest_strategy, list_backtest_strategies


def test_loads_toml_backtest_config():
    config = load_backtest_config(Path("config/backtest/price_momentum.toml"))

    assert config.strategy_name == "price-momentum"
    assert config.strategy_parameters["lookback_days"] == 120
    assert config.start_date.isoformat() == "2018-01-01"
    assert config.rebalance_frequency == "monthly"
    assert config.rebalance_interval_trading_days is None


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


@pytest.mark.parametrize(
    ("frequency", "anchor_kind", "anchor_date"),
    [
        ("monthly", "calendar_month", None),
        ("weekly", "calendar_week_monday_to_sunday", None),
        ("biweekly", "fixed_biweekly_calendar", "1970-01-05"),
    ],
)
def test_backtest_config_serializes_calendar_rebalance_anchor(
    frequency, anchor_kind, anchor_date
):
    config = BacktestConfig(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 2, 1),
        strategy_name="price-momentum",
        rebalance_frequency=frequency,
    )

    payload = config.to_dict()
    assert payload["rebalance_frequency"] == frequency
    assert payload["rebalance_anchor_kind"] == anchor_kind
    assert payload["rebalance_anchor_date"] == anchor_date
    assert config.rebalance_interval_trading_days is None


def test_backtest_config_accepts_every_n_trading_days():
    config = BacktestConfig(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 2, 1),
        strategy_name="price-momentum",
        rebalance_frequency="every_n_trading_days",
        rebalance_interval_trading_days=10,
    )

    assert config.rebalance_interval_trading_days == 10
    assert config.to_dict()["rebalance_anchor_kind"] == "start_date"
    assert config.to_dict()["rebalance_anchor_date"] == "2024-01-01"


def test_loads_every_n_trading_days_from_toml(tmp_path):
    config_path = tmp_path / "every_n.toml"
    config_path.write_text(
        """
[run]
start_date = "2024-01-01"
end_date = "2024-12-31"
rebalance_frequency = "every_n_trading_days"
rebalance_interval_trading_days = 10

[strategy]
name = "price-momentum"
""".strip(),
        encoding="utf-8",
    )

    config = load_backtest_config(config_path)

    assert config.rebalance_frequency == "every_n_trading_days"
    assert config.rebalance_interval_trading_days == 10


@pytest.mark.parametrize(
    ("frequency", "interval", "message"),
    [
        ("daily", None, "调仓频率"),
        ("every_n_trading_days", None, "必须设置正整数"),
        ("every_n_trading_days", True, "必须设置正整数"),
        ("every_n_trading_days", 0, "必须设置正整数"),
        ("weekly", 5, "仅用于 every_n_trading_days"),
    ],
)
def test_backtest_config_rejects_invalid_rebalance_schedule(
    frequency, interval, message
):
    with pytest.raises(ValueError, match=message):
        BacktestConfig(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 2, 1),
            strategy_name="price-momentum",
            rebalance_frequency=frequency,
            rebalance_interval_trading_days=interval,
        )
