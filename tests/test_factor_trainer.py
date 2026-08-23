from datetime import date

import pandas as pd
import pytest

import backtest.factor_trainer as trainer_module
from backtest.factor_trainer import (
    _fit_prior_shrunk_simplex_ridge,
    _project_onto_probability_simplex,
    fit_factor_weights,
    prepare_factor_training_data,
)


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
    assert result.target_transform == "cross_sectional_percentile_rank_demeaned"
    assert result.sample_weighting == "equal_signal_date"
    assert result.weight_constraint == "nonnegative_sum_to_one"
    assert result.prior_factor_weights == pytest.approx(
        {"valuation_pb": 0.5, "quality_roic": 0.5}
    )


def test_probability_simplex_projection_is_nonnegative_and_sums_to_one():
    projected = _project_onto_probability_simplex([3.0, -2.0, 0.5])

    assert projected == pytest.approx([1.0, 0.0, 0.0])
    assert sum(projected) == pytest.approx(1.0)


def test_prior_shrunk_simplex_fit_cannot_collapse_to_all_zero_weights():
    features = pd.DataFrame(
        {
            "valuation_pb": [-0.5, 0.5, -0.5, 0.5],
            "quality_roic": [0.5, -0.5, 0.5, -0.5],
        }
    )
    target = pd.Series([0.5, -0.5, 0.5, -0.5])
    sample_weights = pd.Series([0.5, 0.5, 0.5, 0.5])
    prior = pd.Series({"valuation_pb": 0.6, "quality_roic": 0.4})

    weights, _, converged = _fit_prior_shrunk_simplex_ridge(
        features,
        target,
        sample_weights,
        ("valuation_pb", "quality_roic"),
        ridge_alpha=0.1,
        max_iterations=5000,
        prior_factor_weights=prior,
    )

    assert converged is True
    assert weights.ge(0).all()
    assert weights.sum() == pytest.approx(1.0)


def test_equal_signal_date_weighting_is_unchanged_by_repeating_one_month_rows():
    features = pd.DataFrame(
        {
            "valuation_pb": [-0.5, 0.5, 0.5, -0.5],
            "quality_roic": [0.5, -0.5, -0.5, 0.5],
        }
    )
    target = pd.Series([-0.5, 0.5, 0.5, -0.5])
    sample_weights = pd.Series([0.5, 0.5, 0.5, 0.5])
    prior = pd.Series({"valuation_pb": 0.5, "quality_roic": 0.5})
    baseline, _, _ = _fit_prior_shrunk_simplex_ridge(
        features,
        target,
        sample_weights,
        ("valuation_pb", "quality_roic"),
        ridge_alpha=0.1,
        max_iterations=5000,
        prior_factor_weights=prior,
    )
    repeated_features = pd.concat([features.iloc[:2]] * 3 + [features.iloc[2:]])
    repeated_target = pd.concat([target.iloc[:2]] * 3 + [target.iloc[2:]])
    repeated_sample_weights = pd.Series(
        [1 / 6] * 6 + [1 / 2] * 2,
        index=repeated_features.index,
    )

    repeated, _, _ = _fit_prior_shrunk_simplex_ridge(
        repeated_features.reset_index(drop=True),
        repeated_target.reset_index(drop=True),
        repeated_sample_weights.reset_index(drop=True),
        ("valuation_pb", "quality_roic"),
        ridge_alpha=0.1,
        max_iterations=5000,
        prior_factor_weights=prior,
    )

    pd.testing.assert_series_equal(repeated, baseline)


def test_public_fit_passes_ranked_targets_and_equal_date_weights_to_optimizer(
    monkeypatch,
):
    captured = {}

    def fake_fit(
        features,
        target,
        sample_weights,
        factor_names,
        ridge_alpha,
        max_iterations,
        prior_factor_weights,
    ):
        del ridge_alpha, max_iterations
        captured["features"] = features.copy()
        captured["target"] = target.copy()
        captured["sample_weights"] = sample_weights.copy()
        return prior_factor_weights.copy(), 1, True

    monkeypatch.setattr(trainer_module, "_fit_prior_shrunk_simplex_ridge", fake_fit)
    prepared = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31", "2024-01-31", *(["2024-02-29"] * 4)]),
            "symbol": ["000001", "000002", "000001", "000002", "000003", "000004"],
            "valuation_pb": [1.0, 2.0, 1.0, 2.0, 3.0, 4.0],
            "quality_roic": [2.0, 1.0, 4.0, 3.0, 2.0, 1.0],
            "label_exit_date": pd.to_datetime(
                ["2024-02-02", "2024-02-02", *(["2024-03-04"] * 4)]
            ),
            "forward_return_20d": [10.0, 20.0, 1.0, 2.0, 100.0, 200.0],
        }
    )

    fit_factor_weights(
        pd.DataFrame(),
        ("valuation_pb", "quality_roic"),
        {},
        date(2024, 1, 1),
        date(2024, 3, 31),
        minimum_training_observations=6,
        minimum_training_dates=2,
        prepared_data=prepared,
    )

    assert captured["target"].tolist() == pytest.approx(
        [-0.25, 0.25, -0.375, -0.125, 0.125, 0.375]
    )
    assert captured["sample_weights"].tolist() == pytest.approx(
        [0.5, 0.5, 0.25, 0.25, 0.25, 0.25]
    )
    assert sum(captured["sample_weights"].iloc[:2]) == pytest.approx(1.0)
    assert sum(captured["sample_weights"].iloc[2:]) == pytest.approx(1.0)


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
    def fake_fit(
        features,
        target,
        sample_weights,
        factor_names,
        ridge_alpha,
        max_iterations,
        prior_factor_weights,
    ):
        del features, target, sample_weights, ridge_alpha, max_iterations
        del prior_factor_weights
        return pd.Series(1.0, index=factor_names), 1, False

    monkeypatch.setattr(trainer_module, "_fit_prior_shrunk_simplex_ridge", fake_fit)

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
