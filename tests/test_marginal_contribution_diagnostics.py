import json
from datetime import date

import pandas as pd
import pytest

import main as main_module
from backtest.config import BacktestConfig
from backtest.engine import DailyBacktestEngine
from backtest.marginal_contribution_diagnostics import (
    FULL_COMBINATION_VARIANT,
    MARGINAL_SUMMARY_COLUMNS,
    build_marginal_targets,
    build_marginal_variant_specs,
    compare_marginal_contributions,
    write_marginal_contribution_report,
)


def _formal_weights():
    return {
        "valuation_pb": 0.5,
        "valuation_ps_ttm": 0.3,
        "quality_roic": 0.2,
    }


def _candidates():
    candidates = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31"] * 4 + ["2024-02-29"] * 4),
            "symbol": ["000001", "000002", "000003", "000004"] * 2,
            "valuation_pb": [1.0, 2.0, 3.0, 4.0] * 2,
            "valuation_ps_ttm": [4.0, 3.0, 2.0, 1.0] * 2,
            "quality_roic": [0.1, 0.2, 0.4, 0.3] * 2,
        }
    )
    return candidates


def _signal_data():
    dates = pd.date_range("2024-01-29", "2024-03-05", freq="B")
    rows = []
    for day_index, current_date in enumerate(dates):
        for symbol_index in range(1, 5):
            price = 50.0 + symbol_index + day_index * 0.2
            rows.append(
                {
                    "date": current_date,
                    "symbol": f"00000{symbol_index}",
                    "open": price,
                    "open_hfq": price,
                    "close_hfq": price + 0.1,
                }
            )
    return pd.DataFrame(rows)


def _config():
    return BacktestConfig(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 3, 5),
        strategy_name="multi-factor-quality-value-momentum",
        initial_capital=100_000,
        commission_bps=5,
        slippage_bps=5,
        benchmark_symbol=None,
    )


def test_builds_full_single_and_leave_one_out_weight_protocol():
    specs = build_marginal_variant_specs(_formal_weights())

    assert len(specs) == 7
    assert specs[0].variant == FULL_COMBINATION_VARIANT
    assert specs[0].factor_weights == _formal_weights()
    assert [spec.variant_type for spec in specs[1:4]] == [
        "single_factor",
        "single_factor",
        "single_factor",
    ]
    for spec in specs:
        assert sum(spec.factor_weights.values()) == pytest.approx(1.0)
    leave_one_out = specs[4:]
    assert all(spec.variant_type == "leave_one_out" for spec in leave_one_out)
    assert leave_one_out[0].factor_name == "valuation_pb"
    assert "valuation_pb" not in leave_one_out[0].factor_weights


def test_rejects_zero_weight_factor_in_fixed_protocol():
    with pytest.raises(ValueError, match="正的有限数字"):
        build_marginal_variant_specs(
            {
                "valuation_pb": 1.0,
                "valuation_ps_ttm": 0.0,
                "quality_roic": 1.0,
            }
        )


def test_builds_targets_on_common_candidate_pool():
    specs = build_marginal_variant_specs(_formal_weights())
    targets = build_marginal_targets(
        _candidates(),
        specs,
        holding_count=2,
        winsorize_lower=0.05,
        winsorize_upper=0.95,
    )

    assert list(targets) == [spec.variant for spec in specs]
    assert all(len(targets[spec.variant]) == 4 for spec in specs)
    assert all(
        targets[spec.variant].groupby("date")["target_weight"].sum().eq(1).all()
        for spec in specs
    )


def test_marginal_comparison_reuses_one_prepared_market_data(monkeypatch):
    candidates = _candidates()
    specs = build_marginal_variant_specs(_formal_weights())
    targets = build_marginal_targets(
        candidates,
        specs,
        holding_count=2,
        winsorize_lower=0.05,
        winsorize_upper=0.95,
    )
    prepared_ids = []
    original_run = DailyBacktestEngine.run

    def wrapped_run(self, *args, **kwargs):
        prepared_ids.append(id(kwargs["prepared_market_data"]))
        return original_run(self, *args, **kwargs)

    monkeypatch.setattr(DailyBacktestEngine, "run", wrapped_run)
    report = compare_marginal_contributions(
        _config(),
        _signal_data(),
        candidates,
        specs,
        targets,
        holding_count=2,
    )

    assert len(prepared_ids) == len(specs)
    assert len(set(prepared_ids)) == 1
    assert report.summary["variant"].tolist() == [spec.variant for spec in specs] + [
        "benchmark"
    ]
    assert report.coverage["status"].eq("success").all()
    assert list(report.summary.columns) == list(MARGINAL_SUMMARY_COLUMNS)


def test_marginal_report_writes_audit_files(tmp_path):
    candidates = _candidates()
    specs = build_marginal_variant_specs(_formal_weights())
    targets = build_marginal_targets(
        candidates,
        specs,
        holding_count=2,
        winsorize_lower=0.05,
        winsorize_upper=0.95,
    )
    report = compare_marginal_contributions(
        _config(),
        _signal_data(),
        candidates,
        specs,
        targets,
        holding_count=2,
    )
    output_dir = write_marginal_contribution_report(
        report,
        tmp_path / "marginal",
        parameters={"evaluation_scope": "locked"},
    )

    for filename in (
        "summary.md",
        "parameters.json",
        "marginal_contribution.csv",
        "marginal_daily_nav.csv",
        "marginal_trades.csv",
        "marginal_targets.csv",
        "marginal_coverage.csv",
        "marginal_target_overlap.csv",
    ):
        assert output_dir.joinpath(filename).exists()
    payload = json.loads(
        output_dir.joinpath("parameters.json").read_text(encoding="utf-8")
    )
    assert payload["evaluation_scope"] == "locked"
    summary = output_dir.joinpath("summary.md").read_text(encoding="utf-8")
    assert "leave-one-out" in summary
    with pytest.raises(FileExistsError):
        write_marginal_contribution_report(report, output_dir, parameters={})


def test_main_requires_marginal_evaluation_dates_as_a_pair():
    with pytest.raises(ValueError, match="必须同时指定"):
        main_module.evaluate_factor_marginal_contributions(
            "candidate.toml",
            evaluation_start_date="2022-07-01",
        )
