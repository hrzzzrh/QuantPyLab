import json
import sys
from datetime import date

import pandas as pd
import pytest

import main as main_module
from backtest.config import BacktestConfig
from backtest.data_access import BacktestDataAccess
from backtest.industry_exposure_diagnostics import (
    INDUSTRY_EXPOSURE_COVERAGE_COLUMNS,
    INDUSTRY_EXPOSURE_SUMMARY_COLUMNS,
    calculate_industry_exposure_diagnostics,
    write_industry_exposure_diagnostic_report,
)


def _candidates():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31"] * 4 + ["2024-02-29"] * 4),
            "symbol": ["000001", "000002", "000003", "000004"] * 2,
            "industry_code": ["480101", "480101", "480301", None] * 2,
        }
    )


def _targets():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2024-01-31", "2024-01-31", "2024-02-29", "2024-02-29"]
            ),
            "symbol": ["000001", "000003", "000003", "000004"],
        }
    )


def test_industry_exposure_reports_coverage_and_selection_lift():
    report = calculate_industry_exposure_diagnostics(_candidates(), _targets())

    assert report.coverage["industry_coverage_rate"].tolist() == [0.75, 0.75]
    assert report.coverage["selected_industry_coverage_rate"].tolist() == [1.0, 0.5]
    first_date = report.exposure[report.exposure["date"] == "2024-01-31"]
    first_480101 = first_date[first_date["industry_code"] == "480101"].iloc[0]
    assert first_480101["universe_share"] == pytest.approx(2 / 3)
    assert first_480101["selected_share"] == pytest.approx(0.5)
    assert first_480101["selection_lift"] == pytest.approx(0.75)
    assert list(report.summary.columns) == list(INDUSTRY_EXPOSURE_SUMMARY_COLUMNS)
    assert list(report.coverage.columns) == list(INDUSTRY_EXPOSURE_COVERAGE_COLUMNS)


def test_industry_exposure_handles_all_missing_classifications():
    candidates = _candidates().assign(industry_code=None)
    report = calculate_industry_exposure_diagnostics(candidates, _targets())

    assert report.exposure.empty
    assert report.summary.empty
    assert report.coverage["classified_candidate_count"].tolist() == [0, 0]
    assert report.coverage["missing_candidate_count"].tolist() == [4, 4]


def test_industry_exposure_rejects_invalid_inputs():
    with pytest.raises(ValueError, match="非法行业代码"):
        calculate_industry_exposure_diagnostics(
            _candidates().assign(industry_code="bad"), _targets()
        )
    with pytest.raises(ValueError, match="必须属于"):
        calculate_industry_exposure_diagnostics(
            _candidates(),
            pd.DataFrame(
                {"date": pd.to_datetime(["2024-01-31"]), "symbol": ["999999"]}
            ),
        )
    with pytest.raises(ValueError, match="重复"):
        calculate_industry_exposure_diagnostics(
            pd.concat([_candidates(), _candidates().iloc[[0]]], ignore_index=True),
            _targets(),
        )


def test_industry_exposure_report_writes_audit_files(tmp_path):
    report = calculate_industry_exposure_diagnostics(_candidates(), _targets())

    output_dir = write_industry_exposure_diagnostic_report(
        report,
        tmp_path / "industry-exposure",
        parameters={
            "backtest_config_path": "candidate.toml",
            "industry_data_source": "industry_classification_sw",
        },
    )

    assert output_dir.joinpath("parameters.json").exists()
    assert output_dir.joinpath("industry_exposure.csv").exists()
    assert output_dir.joinpath("industry_exposure_summary.csv").exists()
    assert output_dir.joinpath("industry_exposure_coverage.csv").exists()
    payload = json.loads(
        output_dir.joinpath("parameters.json").read_text(encoding="utf-8")
    )
    assert payload["industry_data_source"] == "industry_classification_sw"
    summary = output_dir.joinpath("summary.md").read_text(encoding="utf-8")
    assert "effective_date" in summary
    assert "不实施行业中性化" in summary
    with pytest.raises(FileExistsError):
        write_industry_exposure_diagnostic_report(report, output_dir, parameters={})


def test_run_factor_industry_exposure_diagnostics_uses_point_in_time_industry(
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
    captured = {}

    monkeypatch.setattr("backtest.config.load_backtest_config", lambda _: config)

    def fake_load_factor_data(
        self,
        config,
        factor_names,
        factor_parameters=None,
        minimum_history_days=0,
        data_end_date=None,
        include_market_cap=False,
    ):
        return signal_data

    def fake_load_industry(self, points):
        captured["point_count"] = len(points)
        return points.assign(industry_code="480101")

    def fake_snapshot_metadata(self):
        return {
            "row_count": 8,
            "symbol_count": 4,
            "snapshot_sha256": "a" * 64,
            "source_updated_date_min": "2025-12-15",
            "source_updated_date_max": "2026-08-21",
        }

    monkeypatch.setattr(BacktestDataAccess, "load_factor_data", fake_load_factor_data)
    monkeypatch.setattr(
        BacktestDataAccess,
        "load_point_in_time_industry",
        fake_load_industry,
    )
    monkeypatch.setattr(
        BacktestDataAccess,
        "get_industry_snapshot_metadata",
        fake_snapshot_metadata,
    )

    output_dir = main_module.run_factor_industry_exposure_diagnostics(
        "candidate.toml",
        output_path=str(tmp_path / "cli-output"),
    )

    assert captured["point_count"] == 8
    assert output_dir.joinpath("summary.md").exists()
    assert len(pd.read_csv(output_dir / "industry_exposure.csv")) == 2
    parameters = json.loads(
        output_dir.joinpath("parameters.json").read_text(encoding="utf-8")
    )
    assert parameters["industry_snapshot"]["row_count"] == 8
    assert len(parameters["industry_snapshot"]["snapshot_sha256"]) == 64


def test_run_factor_industry_exposure_diagnostics_rejects_other_strategy(monkeypatch):
    config = BacktestConfig(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 3, 1),
        strategy_name="price-momentum",
        benchmark_symbol=None,
    )
    monkeypatch.setattr("backtest.config.load_backtest_config", lambda _: config)

    with pytest.raises(ValueError, match="只支持 factor-composite-experiment"):
        main_module.run_factor_industry_exposure_diagnostics("candidate.toml")


def test_cli_industry_exposure_exception_exits_1(monkeypatch):
    import utils.requests_protection as requests_protection

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py",
            "diagnose-factor-industry-exposures",
            "--backtest-config",
            "candidate.toml",
        ],
    )
    monkeypatch.setattr(
        requests_protection, "install_requests_protection", lambda: None
    )

    def fail(**_kwargs):
        raise ValueError("测试错误路径")

    monkeypatch.setattr(main_module, "run_factor_industry_exposure_diagnostics", fail)
    with pytest.raises(SystemExit) as exc:
        main_module.main()
    assert exc.value.code == 1
