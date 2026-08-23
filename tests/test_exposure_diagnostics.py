import json
from datetime import date

import pandas as pd
import pytest

import main as main_module
from backtest.config import BacktestConfig
from backtest.data_access import BacktestDataAccess
from backtest.exposure_diagnostics import (
    SIZE_EXPOSURE_COVERAGE_COLUMNS,
    SIZE_EXPOSURE_SUMMARY_COLUMNS,
    calculate_size_exposure_diagnostics,
    write_size_exposure_diagnostic_report,
)


def _candidates():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31"] * 4 + ["2024-02-29"] * 4),
            "symbol": [
                "000001",
                "000002",
                "000003",
                "000004",
            ]
            * 2,
            "market_cap": [1.0, 2.0, 3.0, 4.0] * 2,
        }
    )


def _targets():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2024-01-31", "2024-01-31", "2024-02-29", "2024-02-29"]
            ),
            "symbol": ["000003", "000004", "000003", "000004"],
        }
    )


def test_size_exposure_buckets_are_calculated_per_signal_date():
    report = calculate_size_exposure_diagnostics(
        _candidates(), _targets(), quantile_count=2
    )

    assert report.coverage["market_cap_coverage_rate"].tolist() == [1.0, 1.0]
    assert report.coverage["selected_market_cap_coverage_rate"].tolist() == [
        1.0,
        1.0,
    ]
    first_date = report.exposure[report.exposure["date"] == "2024-01-31"]
    assert first_date["universe_share"].tolist() == pytest.approx([0.5, 0.5])
    assert first_date["selected_share"].tolist() == pytest.approx([0.0, 1.0])
    assert first_date["selection_lift"].tolist() == pytest.approx([0.0, 2.0])
    assert report.summary["size_bucket"].tolist() == [1, 2]
    assert list(report.summary.columns) == list(SIZE_EXPOSURE_SUMMARY_COLUMNS)
    assert list(report.coverage.columns) == list(SIZE_EXPOSURE_COVERAGE_COLUMNS)


def test_size_exposure_keeps_missing_market_cap_in_coverage_and_avoids_zero_lift():
    candidates = _candidates().iloc[:4].copy()
    candidates.loc[candidates["symbol"] == "000004", "market_cap"] = None
    targets = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31"]),
            "symbol": ["000004"],
        }
    )

    report = calculate_size_exposure_diagnostics(candidates, targets, quantile_count=2)

    assert report.coverage.loc[0, "candidate_count"] == 4
    assert report.coverage.loc[0, "valid_market_cap_count"] == 3
    assert report.coverage.loc[0, "market_cap_coverage_rate"] == pytest.approx(0.75)
    assert report.coverage.loc[0, "selected_count"] == 1
    assert report.coverage.loc[0, "selected_valid_market_cap_count"] == 0
    assert report.coverage.loc[0, "selected_market_cap_coverage_rate"] == pytest.approx(
        0.0
    )
    assert report.exposure["selected_share"].isna().all()
    assert report.exposure["selection_lift"].isna().all()


def test_size_exposure_rejects_invalid_inputs():
    with pytest.raises(ValueError, match="quantile_count"):
        calculate_size_exposure_diagnostics(_candidates(), _targets(), quantile_count=1)
    with pytest.raises(ValueError, match="至少需要 5 个有效市值候选"):
        calculate_size_exposure_diagnostics(
            _candidates().iloc[:4], _targets().iloc[:2], quantile_count=5
        )

    with pytest.raises(ValueError, match="必须属于"):
        calculate_size_exposure_diagnostics(
            _candidates(),
            pd.DataFrame(
                {
                    "date": pd.to_datetime(["2024-01-31"]),
                    "symbol": ["999999"],
                }
            ),
        )
    with pytest.raises(ValueError, match="重复"):
        calculate_size_exposure_diagnostics(
            pd.concat([_candidates(), _candidates().iloc[[0]]], ignore_index=True),
            _targets(),
        )


def test_size_exposure_handles_empty_targets_without_zero_fabrication():
    empty_targets = pd.DataFrame(
        {
            "date": pd.to_datetime([]),
            "symbol": pd.Series(dtype="string"),
        }
    )

    report = calculate_size_exposure_diagnostics(
        _candidates().iloc[:4], empty_targets, quantile_count=2
    )

    assert report.coverage["selected_count"].tolist() == [0]
    assert report.coverage["selected_market_cap_coverage_rate"].isna().all()
    assert report.exposure["selected_share"].isna().all()
    assert report.exposure["selection_lift"].isna().all()


def test_size_exposure_report_writes_audit_files(tmp_path):
    report = calculate_size_exposure_diagnostics(
        _candidates(), _targets(), quantile_count=2
    )

    output_dir = write_size_exposure_diagnostic_report(
        report,
        tmp_path / "size-exposure",
        parameters={
            "backtest_config_path": "candidate.toml",
            "quantile_count": 2,
            "industry_data_status": "unavailable_historical_point_in_time",
        },
    )

    assert output_dir.joinpath("parameters.json").exists()
    assert output_dir.joinpath("size_exposure.csv").exists()
    assert output_dir.joinpath("size_exposure_summary.csv").exists()
    assert output_dir.joinpath("size_exposure_coverage.csv").exists()
    summary = output_dir.joinpath("summary.md").read_text(encoding="utf-8")
    assert "v_daily_valuation.market_cap" in summary
    assert "没有历史行业分类数据" not in summary
    assert "stocks.industry" in summary
    with pytest.raises(FileExistsError):
        write_size_exposure_diagnostic_report(
            report,
            output_dir,
            parameters={"quantile_count": 2},
        )


def test_load_factor_data_can_include_point_in_time_market_cap(monkeypatch):
    access = BacktestDataAccess(object())
    captured = {}

    def fake_load_market_data(
        config,
        lookback_days,
        indicator_fields=(),
        kline_fields=(),
        data_end_date=None,
        valuation_fields=(),
        financial_signal_dates_only=False,
    ):
        captured["valuation_fields"] = valuation_fields
        return pd.DataFrame()

    monkeypatch.setattr(access, "load_market_data", fake_load_market_data)

    access.load_factor_data(
        BacktestConfig(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 2, 1),
            strategy_name="factor-composite-experiment",
            benchmark_symbol=None,
        ),
        ("valuation_pb",),
        include_market_cap=True,
    )

    assert captured["valuation_fields"] == ("market_cap",)


def test_run_factor_exposure_diagnostics_builds_targets_and_writes_report(
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
    signal_data = pd.concat(
        [
            _candidates(),
            _candidates().iloc[:4].assign(date=pd.Timestamp("2024-03-01")),
        ],
        ignore_index=True,
    ).assign(
        open_hfq=100.0,
        close_hfq=101.0,
        pb=[4.0, 3.0, 2.0, 1.0] * 3,
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
        captured["include_market_cap"] = include_market_cap
        return signal_data

    monkeypatch.setattr(BacktestDataAccess, "load_factor_data", fake_load_factor_data)

    output_dir = main_module.run_factor_exposure_diagnostics(
        "candidate.toml",
        quantile_count=2,
        output_path=str(tmp_path / "cli-output"),
    )

    assert captured["include_market_cap"] is True
    assert output_dir.joinpath("summary.md").exists()
    exposure = pd.read_csv(output_dir / "size_exposure.csv")
    assert len(exposure) == 4
    assert exposure["selected_count"].tolist() == [0, 2, 0, 2]
    assert exposure["selection_lift"].tolist() == pytest.approx([0.0, 2.0, 0.0, 2.0])
    parameters = json.loads(
        output_dir.joinpath("parameters.json").read_text(encoding="utf-8")
    )
    assert parameters["quantile_count"] == 2
    assert parameters["market_cap_source"] == "v_daily_valuation.market_cap"
