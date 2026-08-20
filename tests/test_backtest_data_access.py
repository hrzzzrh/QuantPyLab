from datetime import date

import pandas as pd

from backtest.config import BacktestConfig
from backtest.data_access import BacktestDataAccess, IndicatorField


def _config():
    return BacktestConfig(
        start_date=date(2024, 1, 2),
        end_date=date(2024, 2, 1),
        strategy_name="multi-factor-quality-value-momentum",
        benchmark_symbol=None,
    )


def test_load_factor_data_collects_registered_input_requirements(monkeypatch):
    access = BacktestDataAccess(object())
    captured = {}

    def fake_load_market_data(
        config,
        lookback_days,
        indicator_fields=(),
        kline_fields=(),
        data_end_date=None,
        valuation_fields=(),
    ):
        captured["config"] = config
        captured["lookback_days"] = lookback_days
        captured["indicator_fields"] = indicator_fields
        captured["kline_fields"] = kline_fields
        captured["valuation_fields"] = valuation_fields
        return pd.DataFrame()

    monkeypatch.setattr(access, "load_market_data", fake_load_market_data)

    result = access.load_factor_data(
        _config(),
        (
            "price_momentum_120d",
            "quality_roe_weighted",
            "quality_operating_cashflow_ratio",
        ),
        factor_parameters={"price_momentum_120d": {"lookback_days": 300}},
        minimum_history_days=250,
    )

    assert result.empty
    assert captured["lookback_days"] == 300
    assert captured["kline_fields"] == ()
    assert captured["valuation_fields"] == ()
    assert captured["indicator_fields"] == (
        IndicatorField("净资产收益率_加权", "roe_weighted"),
        IndicatorField("经营现金流/营业收入", "operating_cashflow_to_revenue"),
    )


def test_load_factor_data_collects_new_factor_input_requirements(monkeypatch):
    access = BacktestDataAccess(object())
    captured = {}

    def fake_load_market_data(
        config,
        lookback_days,
        indicator_fields=(),
        kline_fields=(),
        data_end_date=None,
        valuation_fields=(),
    ):
        captured["lookback_days"] = lookback_days
        captured["indicator_fields"] = indicator_fields
        captured["valuation_fields"] = valuation_fields
        return pd.DataFrame()

    monkeypatch.setattr(access, "load_market_data", fake_load_market_data)

    access.load_factor_data(
        _config(),
        (
            "valuation_ps_ttm",
            "valuation_pcf_ttm",
            "growth_revenue_yoy",
            "growth_deduct_profit_yoy",
            "quality_roic",
            "price_reversal_20d",
        ),
        factor_parameters={"price_reversal_20d": {"lookback_days": 20}},
    )

    assert captured["lookback_days"] == 20
    assert captured["valuation_fields"] == ("pcf_ttm", "ps_ttm")
    assert captured["indicator_fields"] == (
        IndicatorField("营业总收入同比增长", "revenue_yoy"),
        IndicatorField("扣非净利润同比增长", "deduct_profit_yoy"),
        IndicatorField("投入资本回报率", "roic"),
    )


def test_load_factor_data_can_include_forward_price_rows(monkeypatch):
    access = BacktestDataAccess(object())
    captured = {}

    def fake_load_market_data(
        config,
        lookback_days,
        indicator_fields=(),
        kline_fields=(),
        data_end_date=None,
        valuation_fields=(),
    ):
        captured["data_end_date"] = data_end_date
        return pd.DataFrame()

    monkeypatch.setattr(access, "load_market_data", fake_load_market_data)

    access.load_factor_data(
        _config(),
        ("price_momentum_120d",),
        data_end_date=date(2024, 3, 1),
    )

    assert captured["data_end_date"] == date(2024, 3, 1)


def test_load_factor_data_rejects_empty_factor_list():
    access = BacktestDataAccess(object())

    try:
        access.load_factor_data(_config(), ())
    except ValueError as error:
        assert "至少需要指定一个因子" in str(error)
    else:
        raise AssertionError("空因子列表应该被拒绝")


def test_build_kline_projection_quotes_requested_columns():
    projection = BacktestDataAccess._build_kline_projection(("volume", "amount"))

    assert projection == ', kline."volume" AS "volume", kline."amount" AS "amount"'


def test_build_valuation_projection_includes_only_requested_extensions():
    projection = BacktestDataAccess._build_valuation_projection(("pcf_ttm", "ps_ttm"))

    assert "valuation.pe_ttm" in projection
    assert "valuation.pb" in projection
    assert "valuation.pcf_ttm AS pcf_ttm" in projection
    assert "valuation.ps_ttm AS ps_ttm" in projection

    try:
        BacktestDataAccess._build_valuation_projection(("unknown",))
    except ValueError as error:
        assert "不支持的估值信号字段" in str(error)
    else:
        raise AssertionError("未知估值字段应该被拒绝")
