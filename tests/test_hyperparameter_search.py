from datetime import date

import pytest

from backtest.config import BacktestConfig
from backtest.hyperparameter_search import (
    HyperparameterSearchSpec,
    expand_hyperparameter_trials,
)


def _candidate_config() -> BacktestConfig:
    return BacktestConfig(
        start_date=date(2018, 1, 1),
        end_date=date(2026, 1, 1),
        strategy_name="factor-composite-experiment",
        strategy_parameters={
            "factor_weights": {"price_reversal_20d": 1.0},
            "factor_parameters": {
                "price_reversal_20d": {"lookback_days": 20},
            },
        },
        benchmark_symbol=None,
    )


def test_hyperparameter_search_expands_complete_parameter_groups():
    spec = HyperparameterSearchSpec(
        max_combinations=20,
        holding_counts=(10, 20),
        winsorize_ranges=((0.05, 0.95), (0.1, 0.9)),
        ridge_alphas=(0.1, 1.0),
        factor_parameter_values={
            "price_reversal_20d": {"lookback_days": (10, 20)},
        },
    )

    trials = expand_hyperparameter_trials(
        (("candidate", _candidate_config()),),
        spec,
    )

    assert len(trials) == 16
    assert trials[0].trial_id == "candidate__trial_001"
    assert trials[0].parameters["holding_count"] == 10
    assert trials[0].parameters["winsorize_lower"] == 0.05
    assert trials[0].parameters["ridge_alpha"] == 0.1
    assert trials[0].config.strategy_parameters["factor_parameters"][
        "price_reversal_20d"
    ]["lookback_days"] in {10, 20}
    assert trials[-1].config.strategy_parameters["holding_count"] == 20


def test_hyperparameter_search_rejects_combination_count_over_limit():
    spec = HyperparameterSearchSpec(
        max_combinations=15,
        holding_counts=(10, 20),
        winsorize_ranges=((0.05, 0.95), (0.1, 0.9)),
        ridge_alphas=(0.1, 1.0),
        factor_parameter_values={
            "price_reversal_20d": {"lookback_days": (10, 20)},
        },
    )

    with pytest.raises(ValueError, match="超过上限"):
        expand_hyperparameter_trials(
            (("candidate", _candidate_config()),),
            spec,
        )


def test_hyperparameter_search_rejects_unused_factor_grid():
    spec = HyperparameterSearchSpec(
        factor_parameter_values={
            "price_momentum_120d": {"lookback_days": (60, 120)},
        }
    )

    with pytest.raises(ValueError, match="所有候选都未使用"):
        expand_hyperparameter_trials(
            (("candidate", _candidate_config()),),
            spec,
        )


def test_hyperparameter_search_rejects_duplicate_factor_values():
    with pytest.raises(ValueError, match="不能包含重复值"):
        HyperparameterSearchSpec(
            factor_parameter_values={
                "price_reversal_20d": {"lookback_days": (10, 10)},
            }
        )
