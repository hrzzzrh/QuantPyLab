from datetime import date

import pandas as pd
import pytest

import backtest.factor_trainer as trainer_module
from backtest.factor_trainer import fit_factor_weights, prepare_factor_training_data


def _training_data(periods: int = 90) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=periods)
    rows = []
    symbols = (
        ("000001", 1.0, 10.0, 0.0020),
        ("000002", 2.0, 4.0, 0.0012),
        ("000003", 3.0, 8.0, 0.0007),
        ("000004", 4.0, 2.0, 0.0002),
    )
    for symbol, pb, roic, daily_return in symbols:
        for index, current_date in enumerate(dates):
            open_price = 100 * (1.0005**index)
            rows.append(
                {
                    "date": current_date,
                    "symbol": symbol,
                    "open_hfq": open_price,
                    "close_hfq": open_price * (1 + daily_return),
                    "pb": pb,
                    "roic": roic,
                }
            )
    return pd.DataFrame(rows)


def test_fit_factor_weights_returns_normalized_nonnegative_fitted_weights():
    result = fit_factor_weights(
        _training_data(),
        ("valuation_pb", "quality_roic"),
        {},
        date(2024, 1, 1),
        date(2024, 4, 30),
        label_horizon_days=2,
        ridge_alpha=0.01,
        minimum_training_observations=8,
        minimum_training_dates=3,
    )

    assert set(result.factor_weights) == {"valuation_pb", "quality_roic"}
    assert all(weight >= 0 for weight in result.factor_weights.values())
    assert sum(result.factor_weights.values()) == pytest.approx(1.0)
    assert result.observation_count >= 8
    assert result.signal_date_count >= 3
    assert result.iterations > 0
    assert result.label_horizon_days == 2
    assert result.signal_date_start == "2024-01-31"
    assert result.signal_date_end == "2024-03-29"


def test_prepare_factor_training_data_drops_labels_after_train_end():
    prepared = prepare_factor_training_data(
        _training_data(),
        ("valuation_pb", "quality_roic"),
        {},
        date(2024, 1, 1),
        date(2024, 4, 30),
        label_horizon_days=2,
    )

    assert prepared["date"].max() == pd.Timestamp("2024-03-29")
    assert (prepared["label_exit_date"] <= pd.Timestamp("2024-04-30")).all()


def test_fit_factor_weights_requires_enough_training_observations():
    with pytest.raises(ValueError, match="训练样本不足"):
        fit_factor_weights(
            _training_data(),
            ("valuation_pb",),
            {},
            date(2024, 1, 1),
            date(2024, 4, 30),
            label_horizon_days=2,
            minimum_training_observations=1000,
            minimum_training_dates=3,
        )


def test_fit_factor_weights_rejects_future_duplicate_input():
    data = _training_data()
    duplicate = pd.concat([data, data.iloc[[0]]], ignore_index=True)

    with pytest.raises(ValueError, match="重复的 date/symbol"):
        fit_factor_weights(
            duplicate,
            ("valuation_pb",),
            {},
            date(2024, 1, 1),
            date(2024, 4, 30),
            label_horizon_days=2,
            minimum_training_observations=8,
            minimum_training_dates=3,
        )


def test_fit_factor_weights_reuses_prepared_training_data_without_changing_result():
    data = _training_data()
    factor_names = ("valuation_pb", "quality_roic")
    kwargs = {
        "label_horizon_days": 2,
        "ridge_alpha": 0.01,
        "minimum_training_observations": 8,
        "minimum_training_dates": 3,
    }
    prepared = prepare_factor_training_data(
        data,
        factor_names,
        {},
        date(2024, 1, 1),
        date(2024, 4, 30),
        label_horizon_days=2,
    )

    direct = fit_factor_weights(
        data,
        factor_names,
        {},
        date(2024, 1, 1),
        date(2024, 4, 30),
        **kwargs,
    )
    reused = fit_factor_weights(
        data,
        factor_names,
        {},
        date(2024, 1, 1),
        date(2024, 4, 30),
        prepared_data=prepared,
        **kwargs,
    )

    assert reused == direct


def test_fit_factor_weights_rejects_nonconverged_ridge(monkeypatch):
    def fake_fit(features, target, factor_names, ridge_alpha, max_iterations):
        return pd.Series(1.0, index=factor_names), 1, False

    monkeypatch.setattr(trainer_module, "_fit_nonnegative_ridge", fake_fit)

    with pytest.raises(ValueError, match="未在 max_iterations 内收敛"):
        fit_factor_weights(
            _training_data(),
            ("valuation_pb",),
            {},
            date(2024, 1, 1),
            date(2024, 4, 30),
            label_horizon_days=2,
            minimum_training_observations=8,
            minimum_training_dates=3,
        )
