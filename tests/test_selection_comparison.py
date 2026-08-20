import json
from datetime import date

import pandas as pd
import pytest

import main as main_module
from backtest.config import BacktestConfig
from backtest.engine import DailyBacktestEngine
from backtest.selection_comparison import (
    SELECTION_SUMMARY_COLUMNS,
    SELECTION_VARIANTS,
    build_selection_target_overlap,
    build_selection_variant_targets,
    compare_selection_variants,
    write_selection_comparison_report,
)


def _candidates():
    candidates = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31"] * 6 + ["2024-02-29"] * 6),
            "symbol": [f"00000{index}" for index in range(1, 7)] * 2,
            "score": [
                0.9,
                0.8,
                0.7,
                0.6,
                0.5,
                0.4,
                0.85,
                0.75,
                0.65,
                0.55,
                0.45,
                0.35,
            ],
            "industry_code": (["480101"] * 3 + ["480301"] * 3) * 2,
            "market_cap": [1.0, 2.0, 3.0, 100.0, 200.0, 300.0] * 2,
        }
    )
    candidates["rank"] = candidates.groupby("date")["score"].rank(
        method="first", ascending=False
    )
    return candidates


def _signal_data():
    dates = pd.date_range("2024-01-29", "2024-03-05", freq="B")
    rows = []
    for day_index, current_date in enumerate(dates):
        for symbol_index in range(1, 7):
            base = 50.0 + symbol_index + day_index * 0.1
            rows.append(
                {
                    "date": current_date,
                    "symbol": f"00000{symbol_index}",
                    "open": base,
                    "open_hfq": base,
                    "close_hfq": base + 0.2,
                }
            )
    return pd.DataFrame(rows)


def _config():
    return BacktestConfig(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 3, 5),
        strategy_name="factor-composite-experiment",
        initial_capital=100_000,
        commission_bps=5,
        slippage_bps=5,
        benchmark_symbol=None,
    )


def test_builds_all_selection_variants_from_one_scored_universe():
    candidates = _candidates()
    targets = build_selection_variant_targets(
        candidates,
        holding_count=2,
        quantile_count=2,
    )

    assert list(targets) == list(SELECTION_VARIANTS)
    assert all(len(targets[variant]) == 4 for variant in SELECTION_VARIANTS)
    assert all(
        targets[variant].groupby("date")["target_weight"].sum().eq(1).all()
        for variant in SELECTION_VARIANTS
    )
    overlap = build_selection_target_overlap(targets)
    assert len(overlap) == len(SELECTION_VARIANTS) * 2
    assert overlap.loc[overlap["variant"] == "baseline", "overlap_rate"].tolist() == [
        1.0,
        1.0,
    ]


def test_selection_comparison_reuses_one_prepared_market_data_for_all_variants(
    monkeypatch,
):
    candidates = _candidates()
    targets = build_selection_variant_targets(
        candidates,
        holding_count=2,
        quantile_count=2,
    )
    prepared_ids = []
    original_run = DailyBacktestEngine.run

    def wrapped_run(self, *args, **kwargs):
        prepared_ids.append(id(kwargs["prepared_market_data"]))
        return original_run(self, *args, **kwargs)

    monkeypatch.setattr(DailyBacktestEngine, "run", wrapped_run)
    report = compare_selection_variants(
        _config(),
        _signal_data(),
        targets,
        holding_count=2,
        candidates=candidates,
    )

    assert len(prepared_ids) == len(SELECTION_VARIANTS)
    assert len(set(prepared_ids)) == 1
    assert report.summary["variant"].tolist() == [*SELECTION_VARIANTS, "benchmark"]
    assert report.coverage["status"].eq("success").all()
    assert report.trades["variant"].nunique() == len(SELECTION_VARIANTS)


def test_selection_comparison_records_missing_control_failures_without_fallback():
    candidates = _candidates()
    candidates.loc[:, "industry_code"] = None
    candidates.loc[:, "market_cap"] = float("nan")
    targets = build_selection_variant_targets(
        candidates,
        holding_count=2,
        quantile_count=2,
    )

    coverage = compare_selection_variants(
        _config(),
        _signal_data(),
        targets,
        holding_count=2,
        candidates=candidates,
    ).coverage
    for variant in (
        "neutralized_industry",
        "neutralized_size",
        "neutralized_industry_size",
        "industry_quota",
        "size_quota",
        "industry_size_quota",
    ):
        failed = coverage[coverage["variant"] == variant]
        assert failed["status"].eq("failed").all()
        assert failed["valid_control_count"].eq(0).all()
        assert failed["control_coverage_rate"].eq(0).all()
        assert failed["selected_count"].eq(0).all()
        assert failed["failure_reason"].notna().all()
    baseline = coverage[coverage["variant"] == "baseline"]
    assert baseline["status"].eq("success").all()


def test_selection_comparison_summarizes_cost_turnover_and_writes_report(tmp_path):
    candidates = _candidates()
    targets = build_selection_variant_targets(
        candidates,
        holding_count=2,
        quantile_count=2,
    )
    report = compare_selection_variants(
        _config(),
        _signal_data(),
        targets,
        holding_count=2,
        candidates=candidates,
    )

    assert list(report.summary.columns) == list(SELECTION_SUMMARY_COLUMNS)
    baseline = report.summary.loc[report.summary["variant"] == "baseline"].iloc[0]
    assert baseline["cumulative_turnover"] > 0
    assert baseline["total_transaction_cost"] > 0
    assert baseline["executed_trade_count"] > 0
    assert report.daily_nav["variant"].nunique() == len(SELECTION_VARIANTS)

    output_dir = write_selection_comparison_report(
        report,
        tmp_path / "comparison",
        parameters={
            "evaluation_scope": "locked",
            "benchmark_data_status": "不可用：510300 在评估区间没有行情",
        },
    )
    for filename in (
        "summary.md",
        "parameters.json",
        "selection_comparison.csv",
        "selection_daily_nav.csv",
        "selection_trades.csv",
        "selection_targets.csv",
        "selection_coverage.csv",
        "selection_target_overlap.csv",
    ):
        assert output_dir.joinpath(filename).exists()
    payload = json.loads(
        output_dir.joinpath("parameters.json").read_text(encoding="utf-8")
    )
    assert payload["evaluation_scope"] == "locked"
    summary = output_dir.joinpath("summary.md").read_text(encoding="utf-8")
    assert "同一 T+1 开盘" in summary
    assert "基准 ETF 行情" in summary
    assert "年化波动" in summary
    assert "夏普" in summary
    with pytest.raises(FileExistsError):
        write_selection_comparison_report(report, output_dir, parameters={})


def test_selection_comparison_rejects_incomplete_variant_set():
    targets = build_selection_variant_targets(
        _candidates(),
        holding_count=2,
        quantile_count=2,
    )
    targets.pop("size_quota")

    with pytest.raises(ValueError, match="不完整"):
        compare_selection_variants(
            _config(),
            _signal_data(),
            targets,
            holding_count=2,
        )


def test_selection_comparison_rejects_target_outside_candidate_pool():
    candidates = _candidates()
    targets = build_selection_variant_targets(
        candidates,
        holding_count=2,
        quantile_count=2,
    )
    invalid_targets = {variant: frame.copy() for variant, frame in targets.items()}
    invalid_targets["industry_quota"].loc[0, "symbol"] = "999999"

    with pytest.raises(ValueError, match="必须属于对应信号日候选池"):
        compare_selection_variants(
            _config(),
            _signal_data(),
            invalid_targets,
            holding_count=2,
            candidates=candidates,
        )


def test_main_requires_evaluation_dates_as_a_pair():
    with pytest.raises(ValueError, match="必须同时指定"):
        main_module.evaluate_factor_selection_variants(
            "candidate.toml",
            evaluation_start_date="2022-07-01",
        )
