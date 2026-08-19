import pandas as pd
import pytest

from analysis.factors.engine import FactorEngine
from analysis.factors.registry import list_factor_definitions
from analysis.factors.transforms import (
    combine_factor_scores,
    rank_factor_cross_sectionally,
    standardize_factor_cross_sectionally,
    winsorize_factor_cross_sectionally,
)


def _factor_data(periods=122):
    dates = pd.bdate_range("2023-01-02", periods=periods)
    rows = []
    for symbol, offset in [("000001", 0), ("000002", 10)]:
        for index, current_date in enumerate(dates):
            rows.append(
                {
                    "date": current_date,
                    "symbol": symbol,
                    "close_hfq": 100 + index + offset,
                    "pe_ttm": 10 + offset,
                    "pb": 1 + offset / 10,
                    "roe_weighted": 10 + offset,
                    "operating_cashflow_to_revenue": 0.1 + offset / 100,
                }
            )
    return pd.DataFrame(rows)


def test_factor_registry_exposes_eight_initial_factors():
    names = [metadata.name for metadata in list_factor_definitions()]

    assert names == [
        "price_momentum_120d",
        "price_trend_above_ma_120d",
        "price_trend_gap_120d",
        "price_volatility_60d",
        "quality_operating_cashflow_ratio",
        "quality_roe_weighted",
        "valuation_pb",
        "valuation_pe_ttm",
    ]


def test_factor_engine_calculates_wide_point_in_time_frame():
    data = _factor_data()
    factor_names = (
        "price_momentum_120d",
        "price_trend_above_ma_120d",
        "price_trend_gap_120d",
        "price_volatility_60d",
        "valuation_pe_ttm",
        "valuation_pb",
        "quality_roe_weighted",
        "quality_operating_cashflow_ratio",
    )

    result = FactorEngine().calculate(data, factor_names)

    assert result.columns.tolist() == ["date", "symbol", *factor_names]
    latest = result.loc[lambda frame: frame["date"] == frame["date"].max()]
    assert latest["price_momentum_120d"].notna().all()
    assert latest["price_trend_above_ma_120d"].eq(1.0).all()
    assert latest["price_trend_gap_120d"].notna().all()
    assert latest["price_volatility_60d"].notna().all()
    assert latest["valuation_pe_ttm"].tolist() == [10.0, 20.0]


def test_factor_engine_rejects_duplicate_input_rows():
    data = _factor_data().iloc[[0, 0]]

    with pytest.raises(ValueError, match="重复的 date/symbol"):
        FactorEngine().calculate(data, ("price_momentum_120d",))


def test_factor_engine_does_not_use_future_price_for_previous_value():
    data = _factor_data()
    original = FactorEngine().calculate(data, ("price_momentum_120d",))
    changed = data.copy()
    changed.loc[changed.index[-1], "close_hfq"] = 10_000
    recalculated = FactorEngine().calculate(changed, ("price_momentum_120d",))

    cutoff = data["date"].iloc[-2]
    original_previous = original[original["date"] < cutoff]
    recalculated_previous = recalculated[recalculated["date"] < cutoff]
    pd.testing.assert_frame_equal(original_previous, recalculated_previous)


def test_price_factors_accept_strategy_window_parameters():
    data = _factor_data(periods=10)
    parameters = {
        "price_momentum_120d": {"lookback_days": 3},
        "price_trend_above_ma_120d": {"trend_window": 3},
        "price_trend_gap_120d": {"trend_window": 3},
    }

    result = FactorEngine().calculate(
        data, ("price_momentum_120d", "price_trend_gap_120d"), parameters
    )

    latest = result.loc[result["date"] == result["date"].max()].iloc[0]
    assert latest["price_momentum_120d"] == pytest.approx(109 / 106 - 1)
    assert latest["price_trend_gap_120d"] == pytest.approx(109 / 108 - 1)
    assert (
        FactorEngine().get_max_lookback_days(
            (
                "price_momentum_120d",
                "price_trend_above_ma_120d",
                "price_trend_gap_120d",
            ),
            parameters,
        )
        == 3
    )


def test_trend_confirmation_factor_preserves_strict_above_average_rule():
    dates = pd.bdate_range("2024-01-02", periods=4)
    data = pd.DataFrame(
        {
            "date": dates,
            "symbol": ["000001"] * len(dates),
            "close_hfq": [1.0, 3.0, 2.0, 4.0],
        }
    )

    result = FactorEngine().calculate(
        data,
        ("price_trend_above_ma_120d",),
        {"price_trend_above_ma_120d": {"trend_window": 3}},
    )

    assert result["price_trend_above_ma_120d"].iloc[:2].isna().all()
    assert result["price_trend_above_ma_120d"].tolist()[2:] == [0.0, 1.0]


def test_valuation_factors_reject_non_positive_values():
    data = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-02"]),
            "symbol": ["000001", "000002"],
            "pe_ttm": [10.0, -1.0],
            "pb": [1.0, 0.0],
        }
    )

    result = FactorEngine().calculate(data, ("valuation_pe_ttm", "valuation_pb"))

    assert result.loc[result["symbol"] == "000001", "valuation_pe_ttm"].iloc[0] == 10
    assert pd.isna(result.loc[result["symbol"] == "000002", "valuation_pe_ttm"].iloc[0])
    assert pd.isna(result.loc[result["symbol"] == "000002", "valuation_pb"].iloc[0])


def test_cross_sectional_transforms_respect_factor_direction():
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02"] * 3),
            "symbol": ["000001", "000002", "000003"],
            "factor": [1.0, 2.0, 100.0],
        }
    )

    higher_rank = rank_factor_cross_sectionally(frame, "factor", True)
    lower_rank = rank_factor_cross_sectionally(frame, "factor", False)
    clipped = winsorize_factor_cross_sectionally(frame, "factor", 0.0, 0.5)
    standardized = standardize_factor_cross_sectionally(frame, "factor")
    frame["component"] = higher_rank
    combined = combine_factor_scores(frame, {"component": 2.0})

    assert higher_rank.tolist() == [pytest.approx(1 / 3), pytest.approx(2 / 3), 1.0]
    assert lower_rank.tolist() == [1.0, pytest.approx(2 / 3), pytest.approx(1 / 3)]
    assert clipped.tolist() == [1.0, 2.0, 2.0]
    assert standardized.iloc[0] < standardized.iloc[1] < standardized.iloc[2]
    assert combined.tolist() == [pytest.approx(value * 2) for value in higher_rank]


def test_factor_input_requires_date_and_symbol():
    data = pd.DataFrame({"close_hfq": [100.0]})

    with pytest.raises(ValueError, match="因子输入缺少字段"):
        FactorEngine().calculate(data, ("price_momentum_120d",))
