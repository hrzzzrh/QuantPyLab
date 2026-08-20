import json
import sys
from datetime import date

import pandas as pd
import pytest

import main as main_module
from backtest.config import BacktestConfig
from backtest.data_access import BacktestDataAccess
from backtest.neutralization_diagnostics import (
    NEUTRALIZATION_SUMMARY_COLUMNS,
    _build_neutralized_targets,
    calculate_neutralization_diagnostics,
    write_neutralization_diagnostic_report,
)


def _candidates():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31"] * 4 + ["2024-02-29"] * 4),
            "symbol": ["000001", "000002", "000003", "000004"] * 2,
            "score": [0.9, 0.8, 0.7, 0.6, 0.85, 0.75, 0.65, 0.55],
            "industry_code": ["480101", "480101", "480301", "480301"] * 2,
            "market_cap": [1.0, 2.0, 100.0, 200.0] * 2,
        }
    )


def _baseline_targets():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2024-01-31", "2024-01-31", "2024-02-29", "2024-02-29"]
            ),
            "symbol": ["000001", "000002", "000001", "000002"],
            "score": [0.9, 0.8, 0.85, 0.75],
            "rank": [1, 2, 1, 2],
            "target_weight": [0.5, 0.5, 0.5, 0.5],
        }
    )


def test_neutralization_reduces_industry_exposure_and_reports_overlap():
    report = calculate_neutralization_diagnostics(
        _candidates(),
        _baseline_targets(),
        holding_count=2,
        quantile_count=2,
    )

    assert list(report.summary.columns) == list(NEUTRALIZATION_SUMMARY_COLUMNS)
    assert report.summary["mode"].tolist() == [
        "baseline",
        "industry",
        "size",
        "industry_size",
    ]
    baseline = report.summary.iloc[0]
    industry = report.summary[report.summary["mode"] == "industry"].iloc[0]
    assert baseline["mean_abs_industry_share_difference"] > 0
    assert industry["mean_abs_industry_share_difference"] == pytest.approx(0.0)
    industry_overlap = report.target_overlap[
        report.target_overlap["mode"] == "industry"
    ]
    assert industry_overlap["overlap_rate"].tolist() == pytest.approx([0.5, 0.5])
    assert report.coverage["status"].eq("success").all()
    assert set(report.industry_exposure["mode"]) == {
        "baseline",
        "industry",
        "size",
        "industry_size",
    }
    assert set(report.size_exposure["mode"]) == {
        "baseline",
        "industry",
        "size",
        "industry_size",
    }


@pytest.mark.parametrize("mode", ["size", "industry_size"])
def test_neutralization_modes_are_deterministic_and_select_targets(mode):
    first_targets, first_coverage = _build_neutralized_targets(
        _candidates(),
        mode,
        holding_count=2,
    )
    second_targets, second_coverage = _build_neutralized_targets(
        _candidates(),
        mode,
        holding_count=2,
    )

    pd.testing.assert_frame_equal(first_targets, second_targets)
    assert len(first_targets) == 4
    assert first_targets["score"].notna().all()
    assert {row["status"] for row in first_coverage} == {"success"}


def test_neutralization_joint_modes_fail_without_valid_size_controls():
    candidates = _candidates().copy()
    candidates.loc[0, "market_cap"] = 0
    candidates.loc[1, "market_cap"] = None

    report = calculate_neutralization_diagnostics(
        candidates,
        _baseline_targets(),
        holding_count=3,
        quantile_count=2,
    )

    for mode in ("size", "industry_size"):
        failed = report.coverage[report.coverage["mode"] == mode]
        assert failed.iloc[0]["status"] == "failed"
        assert failed.iloc[0]["valid_control_count"] == 2
        overlap = report.target_overlap[report.target_overlap["mode"] == mode]
        assert overlap.iloc[0]["neutralized_target_count"] == 0
        assert pd.isna(overlap.iloc[0]["overlap_rate"])


def test_neutralization_does_not_fallback_when_controls_are_insufficient():
    candidates = _candidates().iloc[:2].copy()
    candidates.loc[:, "industry_code"] = ["480101", None]
    baseline = _baseline_targets().iloc[:2].copy()

    report = calculate_neutralization_diagnostics(
        candidates,
        baseline,
        holding_count=2,
        quantile_count=2,
    )

    industry = report.summary[report.summary["mode"] == "industry"].iloc[0]
    assert industry["failed_signal_date_count"] == 1
    assert industry["successful_signal_date_count"] == 0
    failed = report.coverage[report.coverage["mode"] == "industry"].iloc[0]
    assert failed["status"] == "failed"
    assert "少于持仓数" in failed["failure_reason"]
    overlap = report.target_overlap[report.target_overlap["mode"] == "industry"]
    assert overlap["neutralized_target_count"].tolist() == [0]
    assert overlap["overlap_rate"].isna().all()


def test_neutralization_rejects_invalid_mode_and_membership():
    with pytest.raises(ValueError, match="中性化模式"):
        from backtest.neutralization_diagnostics import _validate_mode

        _validate_mode("unknown")
    with pytest.raises(ValueError, match="必须属于"):
        calculate_neutralization_diagnostics(
            _candidates(),
            _baseline_targets().assign(symbol=["999999", "000002", "000001", "000002"]),
            holding_count=2,
            quantile_count=2,
        )


def test_neutralization_report_writes_audit_files(tmp_path):
    report = calculate_neutralization_diagnostics(
        _candidates(),
        _baseline_targets(),
        holding_count=2,
        quantile_count=2,
    )
    output_dir = write_neutralization_diagnostic_report(
        report,
        tmp_path / "neutralization",
        parameters={"industry_snapshot": {"snapshot_sha256": "abc"}},
    )

    assert output_dir.joinpath("neutralization_summary.csv").exists()
    assert output_dir.joinpath("neutralization_coverage.csv").exists()
    assert output_dir.joinpath("neutralization_target_overlap.csv").exists()
    assert output_dir.joinpath("neutralization_industry_exposure.csv").exists()
    assert output_dir.joinpath("neutralization_size_exposure.csv").exists()
    payload = json.loads(
        output_dir.joinpath("parameters.json").read_text(encoding="utf-8")
    )
    assert payload["industry_snapshot"]["snapshot_sha256"] == "abc"
    summary = output_dir.joinpath("summary.md").read_text(encoding="utf-8")
    assert "不等于样本外收益改善" in summary
    with pytest.raises(FileExistsError):
        write_neutralization_diagnostic_report(report, output_dir, parameters={})


def test_neutralization_rejects_non_finite_scores():
    candidates = _candidates().copy()
    candidates.loc[0, "score"] = float("inf")

    with pytest.raises(ValueError, match="score"):
        calculate_neutralization_diagnostics(
            candidates,
            _baseline_targets(),
            holding_count=2,
            quantile_count=2,
        )


def test_neutralization_rejects_invalid_quantile_count():
    with pytest.raises(ValueError, match="quantile_count"):
        calculate_neutralization_diagnostics(
            _candidates(),
            _baseline_targets(),
            holding_count=2,
            quantile_count=0,
        )


def test_run_factor_neutralization_diagnostics_builds_research_report(
    monkeypatch, tmp_path
):
    config = BacktestConfig(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 3, 1),
        strategy_name="factor-composite-experiment",
        strategy_parameters={
            "factor_weights": {"valuation_pb": 1.0},
            "holding_count": 2,
            "min_listing_days": 1,
        },
        benchmark_symbol=None,
    )
    signal_data = _candidates().assign(
        open_hfq=100.0,
        close_hfq=101.0,
        pb=[4.0, 3.0, 2.0, 1.0] * 2,
    )
    monkeypatch.setattr("backtest.config.load_backtest_config", lambda _: config)
    monkeypatch.setattr(
        BacktestDataAccess,
        "load_factor_data",
        lambda self, *args, **kwargs: signal_data,
    )
    monkeypatch.setattr(
        BacktestDataAccess,
        "load_point_in_time_industry",
        lambda self, points: points.assign(industry_code="480101"),
    )
    monkeypatch.setattr(
        BacktestDataAccess,
        "get_industry_snapshot_metadata",
        lambda self: {
            "row_count": 8,
            "symbol_count": 4,
            "snapshot_sha256": "b" * 64,
            "source_updated_date_min": "2025-12-15",
            "source_updated_date_max": "2026-08-21",
        },
    )

    output_dir = main_module.run_factor_neutralization_diagnostics(
        "candidate.toml",
        quantile_count=2,
        output_path=str(tmp_path / "cli-output"),
    )

    assert output_dir.joinpath("summary.md").exists()
    assert len(pd.read_csv(output_dir / "neutralization_summary.csv")) == 4
    parameters = json.loads(
        output_dir.joinpath("parameters.json").read_text(encoding="utf-8")
    )
    assert "全候选池" in parameters["exposure_universe_scope"]


def test_run_factor_neutralization_diagnostics_rejects_invalid_quantile_before_load(
    monkeypatch,
):
    def fail_load(*_args, **_kwargs):
        raise AssertionError("非法 quantile_count 不应触发数据加载")

    monkeypatch.setattr("backtest.config.load_backtest_config", fail_load)

    with pytest.raises(ValueError, match="quantile_count"):
        main_module.run_factor_neutralization_diagnostics(
            "candidate.toml",
            quantile_count=0,
        )


def test_run_factor_neutralization_diagnostics_rejects_other_strategy(monkeypatch):
    config = BacktestConfig(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 3, 1),
        strategy_name="price-momentum",
        benchmark_symbol=None,
    )
    monkeypatch.setattr("backtest.config.load_backtest_config", lambda _: config)

    with pytest.raises(ValueError, match="只支持 factor-composite-experiment"):
        main_module.run_factor_neutralization_diagnostics("candidate.toml")


def test_cli_neutralization_exception_exits_1(monkeypatch):
    import utils.requests_protection as requests_protection

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py",
            "diagnose-factor-neutralization",
            "--backtest-config",
            "candidate.toml",
        ],
    )
    monkeypatch.setattr(
        requests_protection, "install_requests_protection", lambda: None
    )

    def fail(**_kwargs):
        raise ValueError("测试错误路径")

    monkeypatch.setattr(main_module, "run_factor_neutralization_diagnostics", fail)
    with pytest.raises(SystemExit) as exc:
        main_module.main()
    assert exc.value.code == 1
